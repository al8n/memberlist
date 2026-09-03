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

use core::{
  cell::{Cell, RefCell},
  net::SocketAddr,
  time::Duration,
};

use alloc::{collections::VecDeque, rc::Rc, vec::Vec};

use embassy_sync::{blocking_mutex::raw::NoopRawMutex, signal::Signal};
use memberlist_embedded::Engine;
use memberlist_proto::{
  Instant, SmallRng,
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

/// The node-wide floor on how often the live joins' seeds are offered to the
/// engine, and the interval a parked join re-checks on.
///
/// Both uses are the same bound seen from the two ends. Offering is a NODE-wide
/// activity — one offer carries every live join's seeds — so this paces how fast a
/// seed whose exchange just failed can be dialed again: a peer whose single reliable
/// listener is occupied RSTs each dial at link speed, and an unpaced re-offer would
/// spin dial → RST → event → dial with no floor at all. A quarter second holds that
/// to one SYN per seed per interval, negligible next to a node's other traffic and
/// far inside the seconds-long deadline a caller puts around a join.
///
/// It is also what a parked join waits on when no event wakes it. `join_wake` has a
/// single consumer, so a pulse can go to another concurrent joiner; re-checking on
/// this interval costs a missed waiter one interval rather than the caller's whole
/// deadline. Latency in the common case comes from the wake, not from here — the
/// Runner pulses it whenever it drained machine events.
pub(crate) const JOIN_OFFER_INTERVAL_MILLIS: u64 = 250;

/// [`JOIN_OFFER_INTERVAL_MILLIS`] as the machine-clock duration the offer pacing
/// measures with. The driver's runtime-clock counterpart lives with the join future
/// that waits on it, derived from the same constant so the two cannot drift.
pub(crate) const JOIN_OFFER_INTERVAL: Duration = Duration::from_millis(JOIN_OFFER_INTERVAL_MILLIS);

/// The distinct seed addresses of every live [`join`](crate::Memberlist::join)
/// future, each counted by how many of those futures offer it.
///
/// The node offers these seeds to the engine once per [`JOIN_OFFER_INTERVAL`] while
/// any join is live. Offering the UNION is what the registry exists for: the
/// engine's over-cap seed admission rotates over the addresses offered TOGETHER —
/// one engine-wide rotation, which an offer can only advance past the entries it
/// actually named. Separate offers of disjoint lists longer than the seed queue can
/// hold would share that rotation while neither ever named the other's addresses,
/// and could delay each other without bound.
///
/// Every address a live future offers is registered. Refusing one would put that
/// future's seeds outside the offer the others ride in, which is precisely the
/// unmerged interleaving the union exists to remove, so no constant bounds this
/// registry. What bounds it instead is the SUM OF THE LIVE JOIN FUTURES' OWN
/// RESOLVED SEED LISTS — memory the application already holds, since each live
/// future owns its resolved list for as long as it is offering — and [`JoinOffer`]
/// releases exactly what it registered on every exit path, so a future that is no
/// longer offering leaves nothing behind.
#[derive(Default)]
pub(crate) struct JoinOffers {
  /// The distinct offered addresses. This IS the union the node offers: the offer
  /// hands the engine this slice, so no call copies or rebuilds it.
  addrs: Vec<SocketAddr>,
  /// `counts[i]` is how many live join futures are offering `addrs[i]`. Parallel to
  /// `addrs` — same length, same indices — so an address and its refcount are added
  /// and removed together.
  counts: Vec<usize>,
}

impl JoinOffers {
  /// Record one more live offer of `addr`.
  fn register(&mut self, addr: SocketAddr) {
    if let Some(pos) = self.addrs.iter().position(|a| *a == addr) {
      // Saturating: the count is how many join futures are alive naming this one
      // address, which cannot approach `usize::MAX` (each is a live future holding
      // its own seed list), and saturating keeps the arithmetic total rather than
      // resting on that.
      self.counts[pos] = self.counts[pos].saturating_add(1);
      return;
    }
    self.addrs.push(addr);
    self.counts.push(1);
  }

  /// Drop one live offer of `addr`, removing the entry when the last one goes.
  fn unregister(&mut self, addr: SocketAddr) {
    let Some(pos) = self.addrs.iter().position(|a| *a == addr) else {
      return;
    };
    let count = &mut self.counts[pos];
    *count = count.saturating_sub(1);
    if *count == 0 {
      // The two vectors are indexed alike, so they are removed alike — a swap on one
      // without the other would re-label every surviving count.
      self.addrs.swap_remove(pos);
      self.counts.swap_remove(pos);
    }
    if self.addrs.is_empty() {
      // No join is live, so nothing needs the capacity the busiest moment grew:
      // release it rather than hold a node's peak seed count for the rest of its
      // uptime. Fresh empty vectors allocate nothing, so this is a free reset.
      self.addrs = Vec::new();
      self.counts = Vec::new();
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
  /// Every one of them is registered in `offers`, so `Drop` releases exactly the
  /// set this guard took.
  addrs: Vec<SocketAddr>,
}

impl Drop for JoinOffer<'_> {
  fn drop(&mut self) {
    let mut offers = self.offers.borrow_mut();
    for addr in self.addrs.iter() {
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
  /// The seeds every live `join` future is offering. The Runner offers their union
  /// as ONE list, so a join future registers here and waits rather than offering
  /// anything itself. Holds every address those futures name, bounded by the
  /// resolved seed lists they already hold — see [`JoinOffers`].
  pub(crate) join_offers: RefCell<JoinOffers>,
  /// When the node may make its next join offer, in the machine clock domain the
  /// Runner pumps with. `None` means no offer is pending — either no join is live,
  /// or none has been made yet — so the next registration is offered at once
  /// instead of waiting out an interval measured against a join that has ended.
  ///
  /// Private to this module: it is the offer step's own bookkeeping, and every
  /// caller reaches it through [`Shared::offer_join_seeds_at`].
  next_join_offer: Cell<Option<Instant>>,
}

/// Cap on the buffered application-event queue. A never-draining application then
/// drops the OLDEST surplus events (best-effort, like the std drivers' bounded
/// observation channel) rather than growing memory without bound.
const APP_EVENTS_CAP: usize = 1024;

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
      next_join_offer: Cell::new(None),
    }
  }

  /// Register `seeds` as one live join future's offer and hand back the guard that
  /// releases them again when that future ends — returned, cancelled or dropped.
  ///
  /// The seed list is deduplicated first, so one address repeated within it takes a
  /// single reference and the guard's release balances its registration exactly.
  pub(crate) fn register_join_offer(&self, seeds: &[SocketAddr]) -> JoinOffer<'_> {
    let mut addrs: Vec<SocketAddr> = Vec::with_capacity(seeds.len());
    for seed in seeds {
      if !addrs.contains(seed) {
        addrs.push(*seed);
      }
    }

    {
      let mut offers = self.join_offers.borrow_mut();
      for addr in addrs.iter() {
        offers.register(*addr);
      }
    }

    JoinOffer {
      offers: &self.join_offers,
      addrs,
    }
  }

  /// Number of distinct addresses the live join futures are offering between them.
  pub(crate) fn join_offer_addr_count(&self) -> usize {
    self.join_offers.borrow().addrs.len()
  }

  /// Make the node's one join offer if it is due, and report both whether this call
  /// offered and when the next offer is due.
  ///
  /// Offering is a NODE-wide activity, not a per-join one: one offer carries the
  /// union of every live join's seeds, so F concurrent joins cost one offer and one
  /// dial per seed per interval rather than F of each. The Runner calls this once
  /// per pump iteration, immediately before `pump`, so an admitted seed gets its
  /// `Connect` in the very same pump.
  ///
  /// Returns `(false, None)` — and forgets the pacing — when there is nothing to
  /// offer: no live join, or a node that has left (which refuses offers anyway).
  /// Otherwise returns when the next offer is due, which the Runner folds into its
  /// sleep deadline so a node with live joins wakes to re-offer even when the
  /// machine has no earlier work of its own.
  ///
  /// Borrows only; never awaits. `join_offers` and `engine` are separate cells, so
  /// holding the registry across the engine call aliases nothing.
  pub(crate) fn offer_join_seeds_at(&self, now: Instant) -> (bool, Option<Instant>) {
    let offers = self.join_offers.borrow();
    if offers.addrs.is_empty() || self.engine.borrow().ensure_running().is_err() {
      self.next_join_offer.set(None);
      return (false, None);
    }

    // Paced: at most one offer per interval, however many joins are live and however
    // often they wake the pump.
    if let Some(next) = self.next_join_offer.get()
      && now < next
    {
      return (false, Some(next));
    }

    // Ignoring Err: `join` refuses only a node that is not running, which the guard
    // above just ruled out with no suspension point in between; a join future
    // observes the lifecycle itself either way.
    let _ = self.engine.borrow_mut().join(&offers.addrs);
    let next = now + JOIN_OFFER_INTERVAL;
    self.next_join_offer.set(Some(next));
    (true, Some(next))
  }

  /// The Runner's view of [`offer_join_seeds_at`](Self::offer_join_seeds_at): make
  /// the offer if it is due and report when the next one is.
  #[inline]
  pub(crate) fn offer_join_seeds(&self, now: Instant) -> Option<Instant> {
    self.offer_join_seeds_at(now).1
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

  /// Wake a parked `join` (membership or the node's lifecycle may have changed).
  ///
  /// Single-consumer, so this reaches one of several concurrent joiners; the others
  /// re-check on their own interval backstop.
  #[inline]
  pub(crate) fn wake_joins(&self) {
    self.join_wake.signal(());
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
      self.wake_joins();
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
