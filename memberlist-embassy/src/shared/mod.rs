//! State shared between the [`Memberlist`](crate::Memberlist) handle and the
//! [`Runner`](crate::Runner) run loop.
//!
//! Single-executor (`!Send`) cooperative sharing, exactly like hick's
//! `MdnsState`: the engine lives behind a [`RefCell`] and the two sides
//! coordinate through `embassy-sync` [`Signal`]s. The handle borrows the engine
//! to enqueue work and parks on a per-request signal; the Runner (the only `pump`
//! caller) drains the machine's events each loop and resolves the parked waiters.
//!
//! Because [`Engine::pump`](memberlist_embedded::Engine::pump) is synchronous,
//! every `RefCell` borrow either side takes completes before the next `.await`,
//! so no borrow ever spans a suspension point.

use core::{cell::RefCell, net::SocketAddr, time::Duration};

use alloc::{collections::VecDeque, rc::Rc, vec::Vec};

use embassy_sync::{blocking_mutex::raw::NoopRawMutex, signal::Signal};
use memberlist_embedded::Engine;
use memberlist_proto::{
  SmallRng,
  event::{Event, PingId, StreamId},
};

use crate::{error::OpError, stream_io::SlotId};

/// A one-shot result channel for a parked handle op, fired by the Runner.
pub(crate) type OpSignal<T> = Rc<Signal<NoopRawMutex, Result<T, OpError>>>;

/// An outstanding [`ping`](crate::Memberlist::ping) call, resolved on the
/// matching `PingId` (`PingCompleted` → `Ok(rtt)`, `PingFailed` → `Err`).
pub(crate) struct PendingPing {
  pub(crate) ping_id: PingId,
  pub(crate) reply: OpSignal<Duration>,
}

/// An outstanding [`send_reliable`](crate::Memberlist::send_reliable) call,
/// resolved on the terminal `ExchangeCompleted { kind: UserMessage }` whose
/// exchange the engine correlates back to this send's [`StreamId`].
///
/// `Engine::send_reliable` returns the `StreamId` it dispatched; the engine maps
/// that to the bridge `ExchangeId` at the `Connect`, so the Runner resolves the
/// EXACT waiter keyed by `StreamId` — overlapping or out-of-order completions
/// (e.g. to different peers, or one failing while another succeeds) never
/// cross-resolve.
pub(crate) struct PendingSend {
  /// The `StreamId` `Engine::send_reliable` returned for this exchange.
  pub(crate) key: StreamId,
  pub(crate) reply: OpSignal<()>,
}

/// The waiter registries the Runner resolves from drained machine events.
#[derive(Default)]
pub(crate) struct Waiters {
  /// Outstanding application pings, matched by `PingId`.
  pub(crate) pings: Vec<PendingPing>,
  /// Outstanding reliable sends, resolved by `StreamId` on the matching
  /// `ExchangeCompleted(UserMessage)`.
  pub(crate) sends: Vec<PendingSend>,
}

/// The distinct seed addresses of every live [`join`](crate::Memberlist::join)
/// future, each counted by how many of those futures offer it.
///
/// A join future re-offers its resolved seed list until the node is joined, and the
/// engine's over-cap seed admission rotates over the addresses offered TOGETHER —
/// one engine-wide rotation, which a call can only advance past the entries it
/// actually saw. Two futures offering disjoint lists longer than the seed queue can
/// hold would therefore share that rotation while neither ever names the other's
/// addresses, and could delay each other without bound. Registering every live
/// future's seeds here lets each offer carry their union, so the one rotation
/// sweeps all of them.
#[derive(Default)]
pub(crate) struct JoinOffers {
  /// `(address, number of live join futures offering it)`. A flat vector rather
  /// than a map: the entry count is capped at [`JOIN_OFFER_ADDRS_CAP`], and every
  /// use walks the whole thing anyway to build the union.
  entries: Vec<(SocketAddr, usize)>,
}

impl JoinOffers {
  /// Record one more live offer of `addr`, reporting whether it is now registered.
  ///
  /// `false` only when `addr` is new and the cap is already reached.
  fn register(&mut self, addr: SocketAddr) -> bool {
    if let Some(entry) = self.entries.iter_mut().find(|(a, _)| *a == addr) {
      // Saturating: the count is how many join futures are alive naming this one
      // address, which cannot approach `usize::MAX` (each is a live future holding
      // its own seed list), and saturating keeps the arithmetic total rather than
      // resting on that.
      entry.1 = entry.1.saturating_add(1);
      return true;
    }
    if self.entries.len() >= JOIN_OFFER_ADDRS_CAP {
      return false;
    }
    self.entries.push((addr, 1));
    true
  }

  /// Drop one live offer of `addr`, removing the entry when the last one goes.
  fn unregister(&mut self, addr: SocketAddr) {
    let Some(pos) = self.entries.iter().position(|(a, _)| *a == addr) else {
      return;
    };
    let count = &mut self.entries[pos].1;
    *count = count.saturating_sub(1);
    if *count == 0 {
      self.entries.swap_remove(pos);
    }
  }
}

/// One live [`join`](crate::Memberlist::join) future's registration in
/// [`JoinOffers`], which it holds for as long as it is offering.
///
/// The guard is what makes the registry cancellation-safe: a `join` future parks on
/// an await the caller can drop at any moment — its own join deadline, a `select`
/// that lost, a task teardown — and `Drop` runs on every one of those paths as well
/// as on a normal return, so a future that is no longer offering never leaves its
/// seeds in the union the running joins carry.
pub(crate) struct JoinOffer<'a> {
  offers: &'a RefCell<JoinOffers>,
  /// This future's own resolved seeds, deduplicated, in the order it offered them.
  addrs: Vec<SocketAddr>,
  /// How many leading entries of `addrs` are registered in `offers`, so `Drop`
  /// releases exactly what this guard took. Short of the full list only when the
  /// registry hit [`JOIN_OFFER_ADDRS_CAP`].
  registered: usize,
}

impl JoinOffer<'_> {
  /// Fill `out` with the addresses this future's next offer should carry: its own
  /// seeds first, then every address any other live join future is offering.
  ///
  /// Own-seeds-first so an offer always carries the caller's whole list even on the
  /// path where the registry cap had no room for part of it — the merging is then
  /// all that is lost, never the caller's own discovery intent. `out` is the
  /// caller's reused buffer, so a steady re-offer loop allocates nothing.
  pub(crate) fn fill_union(&self, out: &mut Vec<SocketAddr>) {
    out.clear();
    out.extend_from_slice(&self.addrs);
    for (addr, _) in self.offers.borrow().entries.iter() {
      if !out.contains(addr) {
        out.push(*addr);
      }
    }
  }
}

impl Drop for JoinOffer<'_> {
  fn drop(&mut self) {
    let mut offers = self.offers.borrow_mut();
    for addr in self.addrs.iter().take(self.registered) {
      offers.unregister(*addr);
    }
  }
}

/// The state both the handle and the run loop reach.
///
/// Shared via [`Rc`] (single-core cooperative). `I` is the node id type; `R` is
/// the gossip RNG (defaulting to [`SmallRng`]).
pub(crate) struct Shared<I, R = SmallRng> {
  /// The transport-agnostic driving core (SWIM machine, reliable-plane state +
  /// pool, gossip codec, join-seed queue), behind interior mutability.
  pub(crate) engine: RefCell<Engine<I, SlotId, R>>,
  /// The pump loop's single wake. Producers: the handle (when it enqueues
  /// `join` / `leave` / `ping` / `send*` work) AND every worker (when it advances
  /// its mailbox — inbound bytes, drained outbound, a FIN/reset). Sole consumer:
  /// the pump loop. A single-consumer, many-producer [`Signal`] is sound here
  /// because the pump drains EVERY mailbox each tick, so one pulse re-pumps all
  /// pending work regardless of which producer fired it.
  pub(crate) pump_wake: Signal<NoopRawMutex, ()>,
  /// Pulsed by the Runner after it drains machine events (membership may have
  /// changed), so a parked `join` re-checks `is_joined()`. A parked `join` also
  /// races a short timer, so a missed pulse (this `Signal` wakes only one of
  /// several concurrent joiners) costs at most that interval, never a hang.
  pub(crate) join_wake: Signal<NoopRawMutex, ()>,
  /// Application-facing events the Runner drained from the machine, buffered for
  /// the handle's [`Memberlist::poll_event`](crate::Memberlist::poll_event).
  ///
  /// The Runner is the sole `poll_event` caller on the engine (it drains events
  /// each pump to resolve parked ping/send waiters); draining is destructive, so
  /// the events the application still wants — `UserPacket`, `NodeJoined`, … — are
  /// re-buffered here rather than discarded. Bounded so a peer flooding events a
  /// never-polling application cannot grow it without limit.
  pub(crate) app_events: RefCell<VecDeque<Event<I, SocketAddr>>>,
  /// The parked-op waiter tables.
  pub(crate) waiters: RefCell<Waiters>,
  /// The seeds every live `join` future is offering, so each of them can offer
  /// their union rather than its own list alone. See [`JoinOffers`].
  pub(crate) join_offers: RefCell<JoinOffers>,
}

/// Cap on the buffered application-event queue. A never-draining application then
/// drops the OLDEST surplus events (best-effort, like the std drivers' bounded
/// observation channel) rather than growing memory without bound.
const APP_EVENTS_CAP: usize = 1024;

/// Cap on the DISTINCT addresses the live-join registry holds.
///
/// The registry merges concurrent joins into one offer; it is not a second copy of
/// the application's discovery intent. Bounding it keeps the union each offer
/// carries — which the engine scans once per free seed-queue slot — independent of
/// how many join futures an application happens to spawn. Past the cap a further
/// future's seeds simply go unregistered: its own offers still carry its whole list,
/// so only the merging with the other loops' seeds is lost. Well above any plausible
/// number of concurrent joins on one embedded node, so the degraded path is a
/// backstop rather than a working mode.
const JOIN_OFFER_ADDRS_CAP: usize = 64;

impl<I, R> Shared<I, R>
where
  I: memberlist_proto::Id,
{
  /// Wrap a constructed engine as shared state with empty signals/waiters.
  pub(crate) fn new(engine: Engine<I, SlotId, R>) -> Self {
    Self {
      engine: RefCell::new(engine),
      pump_wake: Signal::new(),
      join_wake: Signal::new(),
      app_events: RefCell::new(VecDeque::new()),
      waiters: RefCell::new(Waiters::default()),
      join_offers: RefCell::new(JoinOffers::default()),
    }
  }

  /// Register `seeds` as one live join future's offer and hand back the guard that
  /// releases them again when that future ends — returned, cancelled or dropped.
  ///
  /// Registration stops at the first address the cap has no room for, so the guard
  /// holds a prefix it can release exactly.
  pub(crate) fn register_join_offer(&self, seeds: &[SocketAddr]) -> JoinOffer<'_> {
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(seeds.len());
    for seed in seeds {
      if !addrs.contains(seed) {
        addrs.push(*seed);
      }
    }

    let mut registered = 0;
    {
      let mut offers = self.join_offers.borrow_mut();
      for addr in addrs.iter() {
        if !offers.register(*addr) {
          break;
        }
        registered += 1;
      }
    }

    JoinOffer {
      offers: &self.join_offers,
      addrs,
      registered,
    }
  }

  /// Number of distinct addresses the live join futures are offering between them.
  pub(crate) fn join_offer_addr_count(&self) -> usize {
    self.join_offers.borrow().entries.len()
  }

  /// Pop one buffered application event for the handle's `poll_event`.
  pub(crate) fn pop_app_event(&self) -> Option<Event<I, SocketAddr>> {
    self.app_events.borrow_mut().pop_front()
  }

  /// Wake the pump loop (a handle op enqueued work).
  #[inline]
  pub(crate) fn wake_pump(&self) {
    self.pump_wake.signal(());
  }

  /// Register a pending ping waiter and return its reply signal.
  pub(crate) fn register_ping(&self, ping_id: PingId) -> OpSignal<Duration> {
    let reply: OpSignal<Duration> = Rc::new(Signal::new());
    self.waiters.borrow_mut().pings.push(PendingPing {
      ping_id,
      reply: reply.clone(),
    });
    reply
  }

  /// Register a pending reliable-send waiter keyed by its `StreamId` and return
  /// its reply signal.
  pub(crate) fn register_send(&self, key: StreamId) -> OpSignal<()> {
    let reply: OpSignal<()> = Rc::new(Signal::new());
    self.waiters.borrow_mut().sends.push(PendingSend {
      key,
      reply: reply.clone(),
    });
    reply
  }

  /// Drain the machine's pending events, resolving any matched ping/send waiters,
  /// buffering every event for the handle's `poll_event`, and pulsing `join_wake`
  /// so parked `join`s re-check membership.
  ///
  /// Called by the Runner once per loop, AFTER `pump` (so it sees this tick's
  /// freshly-emitted events). The Runner is the sole `poll_event` caller on the
  /// engine, so it must re-buffer the application-facing events it drains rather
  /// than discard them. Takes only brief `RefCell` borrows; never awaits.
  pub(crate) fn drain_events(&self) {
    use memberlist_proto::event::ExchangeKind;

    let mut any = false;
    loop {
      let ev = self.engine.borrow_mut().poll_event();
      let Some(ev) = ev else { break };
      any = true;

      // Resolve any waiter this event terminates (correlation is additive — the
      // event is still buffered for the application below).
      match &ev {
        Event::PingCompleted(p) => self.resolve_ping(p.ping_id(), Ok(p.rtt())),
        Event::PingFailed(p) => self.resolve_ping(p.ping_id(), Err(OpError::PingTimeout)),
        // `poll_event` (above) already pruned EVERY completed exchange's correlation
        // entry from the engine map (so it cannot leak under any consumer) and
        // stashed the user-message ones' originating StreamId. Resolve the exact send
        // waiter by that StreamId — never by arrival order, so overlapping /
        // out-of-order completions cannot cross-resolve.
        Event::ExchangeCompleted(ec) if ec.kind() == ExchangeKind::UserMessage => {
          if let Some(sid) = self.engine.borrow().last_completed_send() {
            let result = if ec.outcome().is_succeeded() {
              Ok(())
            } else {
              Err(OpError::SendFailed)
            };
            self.resolve_send(sid, result);
          }
        }
        _ => {}
      }

      // Buffer the event for `poll_event`, dropping the OLDEST when at the cap so a
      // never-draining application bounds memory (best-effort, like the std
      // drivers' bounded observation channel).
      let mut q = self.app_events.borrow_mut();
      if q.len() >= APP_EVENTS_CAP {
        q.pop_front();
      }
      q.push_back(ev);
    }
    if any {
      self.join_wake.signal(());
    }
  }

  /// Resolve (and remove) the ping waiter matching `id`, if any.
  fn resolve_ping(&self, id: PingId, result: Result<Duration, OpError>) {
    let mut w = self.waiters.borrow_mut();
    if let Some(pos) = w.pings.iter().position(|p| p.ping_id == id) {
      let pending = w.pings.swap_remove(pos);
      // Drop the borrow before signalling so a woken waiter that immediately
      // re-borrows `waiters` does not alias this guard.
      drop(w);
      pending.reply.signal(result);
    }
  }

  /// Resolve (and remove) the pending reliable send whose `StreamId` matches
  /// `key`, if any.
  fn resolve_send(&self, key: StreamId, result: Result<(), OpError>) {
    let mut w = self.waiters.borrow_mut();
    if let Some(pos) = w.sends.iter().position(|s| s.key == key) {
      let pending = w.sends.swap_remove(pos);
      // Drop the borrow before signalling so a woken waiter that immediately
      // re-borrows `waiters` does not alias this guard.
      drop(w);
      pending.reply.signal(result);
    }
  }

  /// Fail every parked waiter with `NotRunning` (used when the run loop stops, so
  /// no awaiting handle op hangs forever after teardown).
  pub(crate) fn fail_all_waiters(&self) {
    let mut w = self.waiters.borrow_mut();
    let pings = core::mem::take(&mut w.pings);
    let sends = core::mem::take(&mut w.sends);
    drop(w);
    for p in pings {
      p.reply.signal(Err(OpError::NotRunning));
    }
    for s in sends {
      s.reply.signal(Err(OpError::NotRunning));
    }
  }
}

/// A free-standing helper a `join` uses to test convergence: whether the node has
/// learned at least one peer. Kept here so the handle and any future caller share
/// one definition of "joined".
#[inline]
pub(crate) fn is_joined<I, R>(shared: &Shared<I, R>) -> bool
where
  I: memberlist_proto::Id,
{
  shared.engine.borrow().is_joined()
}

/// The local advertised address — used by the handle's convenience forwards.
#[inline]
pub(crate) fn advertise_address<I, R>(shared: &Shared<I, R>) -> SocketAddr
where
  I: memberlist_proto::Id,
{
  shared.engine.borrow().advertise_address()
}

#[cfg(test)]
mod tests;
