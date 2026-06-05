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
use memberlist_proto::event::{Event, PingId, StreamId};

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

/// The state both the handle and the run loop reach.
///
/// Shared via [`Rc`] (single-core cooperative). `I` is the node id type.
pub(crate) struct Shared<I>
where
  I: memberlist_proto::Id,
{
  /// The transport-agnostic driving core (SWIM machine, reliable-plane state +
  /// pool, gossip codec, join-seed queue), behind interior mutability.
  pub(crate) engine: RefCell<Engine<I, SlotId>>,
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
}

/// Cap on the buffered application-event queue. A never-draining application then
/// drops the OLDEST surplus events (best-effort, like the std drivers' bounded
/// observation channel) rather than growing memory without bound.
const APP_EVENTS_CAP: usize = 1024;

impl<I> Shared<I>
where
  I: memberlist_proto::Id,
{
  /// Wrap a constructed engine as shared state with empty signals/waiters.
  pub(crate) fn new(engine: Engine<I, SlotId>) -> Self {
    Self {
      engine: RefCell::new(engine),
      pump_wake: Signal::new(),
      join_wake: Signal::new(),
      app_events: RefCell::new(VecDeque::new()),
      waiters: RefCell::new(Waiters::default()),
    }
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
pub(crate) fn is_joined<I>(shared: &Shared<I>) -> bool
where
  I: memberlist_proto::Id,
{
  shared.engine.borrow().is_joined()
}

/// The local advertised address — used by the handle's convenience forwards.
#[inline]
pub(crate) fn advertise_address<I>(shared: &Shared<I>) -> SocketAddr
where
  I: memberlist_proto::Id,
{
  shared.engine.borrow().advertise_address()
}

#[cfg(test)]
mod tests {
  use super::{Shared, advertise_address, is_joined};
  use crate::{error::OpError, stream_io::SlotId};
  use core::{net::SocketAddr, time::Duration};
  use memberlist_embedded::{Engine, Options as EngineConfig, TransformOptions};
  use memberlist_proto::{EndpointOptions, Instant, event::Event};
  use smol_str::SmolStr;

  fn sa(last: u8) -> SocketAddr {
    SocketAddr::from(([169, 254, 0, last], 7946))
  }

  /// Build a single-node engine wrapped as `Shared` for the waiter/buffer tests.
  fn shared_node(id: &str, last: u8) -> Shared<SmolStr> {
    let now = Instant::from_origin(Duration::from_secs(1));
    let engine: Engine<SmolStr, SlotId> = Engine::new_at(
      EngineConfig::new().with_port(7946),
      TransformOptions::default(),
      EndpointOptions::new(SmolStr::new(id), sa(last)).with_rng_seed(7),
      now,
    );
    Shared::new(engine)
  }

  /// Two overlapping reliable sends get distinct `StreamId`s; resolving the SECOND
  /// one first (out of issue order) must resolve ONLY the second waiter, and the
  /// first must still resolve to its OWN independent result. Completions are
  /// matched by `StreamId`, never by arrival order — the old FIFO resolution
  /// would have handed the second's outcome to the first.
  #[test]
  fn out_of_order_reliable_completions_resolve_their_own_waiter() {
    let now = Instant::from_origin(Duration::from_secs(1));
    let engine: Engine<SmolStr, SlotId> = Engine::new_at(
      EngineConfig::new().with_port(7946),
      TransformOptions::default(),
      EndpointOptions::new(SmolStr::new("t"), sa(1)).with_rng_seed(7),
      now,
    );
    let shared: Shared<SmolStr> = Shared::new(engine);

    let sid1 =
      shared
        .engine
        .borrow_mut()
        .send_reliable(sa(2), bytes::Bytes::from_static(b"one"), now);
    let sid2 =
      shared
        .engine
        .borrow_mut()
        .send_reliable(sa(3), bytes::Bytes::from_static(b"two"), now);
    assert_ne!(sid1, sid2, "distinct sends mint distinct StreamIds");

    let reply1 = shared.register_send(sid1);
    let reply2 = shared.register_send(sid2);

    // Resolve the SECOND send first (out of issue order): only its waiter fires.
    shared.resolve_send(sid2, Ok(()));
    assert!(reply2.signaled(), "the matching (second) waiter resolved");
    assert!(
      !reply1.signaled(),
      "the first waiter is untouched by the second send's completion"
    );

    // The first send fails: only its own waiter sees the failure.
    shared.resolve_send(sid1, Err(OpError::SendFailed));
    assert!(
      reply1.signaled(),
      "the first waiter resolved on its own completion"
    );

    assert!(
      matches!(reply2.try_take(), Some(Ok(()))),
      "second waiter got Ok"
    );
    assert!(
      matches!(reply1.try_take(), Some(Err(OpError::SendFailed))),
      "first waiter got its OWN Err, not the second's Ok"
    );
  }

  /// `pop_app_event` is FIFO over the buffered application events and yields
  /// `None` once drained — the order `poll_event` hands them to the application.
  #[test]
  fn pop_app_event_is_fifo_and_drains_to_none() {
    let shared = shared_node("t", 1);
    assert!(
      shared.pop_app_event().is_none(),
      "a fresh buffer yields None"
    );

    // Buffer two distinguishable events directly (the Runner is the only producer
    // in production; here we stand in for one drain to test ordering).
    {
      let mut q = shared.app_events.borrow_mut();
      q.push_back(Event::LeftCluster);
      q.push_back(Event::DecodeError(
        memberlist_proto::event::DecodeError::new(sa(2), "boom".into()),
      ));
    }

    assert!(
      matches!(shared.pop_app_event(), Some(Event::LeftCluster)),
      "the oldest event comes out first"
    );
    assert!(
      matches!(shared.pop_app_event(), Some(Event::DecodeError(_))),
      "the second-oldest event comes out next"
    );
    assert!(shared.pop_app_event().is_none(), "the buffer is drained");
  }

  /// A registered ping resolves exactly when `resolve_ping` is called with its
  /// matching `PingId`; an unrelated `PingId` leaves it parked.
  #[test]
  fn ping_waiter_resolves_only_on_its_own_id() {
    let mut shared = shared_node("t", 1);
    let now = Instant::from_origin(Duration::from_secs(1));

    // Mint two distinct PingIds from the engine (the only legitimate producer).
    let id1 = shared
      .engine
      .get_mut()
      .ping(memberlist_proto::Node::new(SmolStr::new("p1"), sa(2)), now);
    let id2 = shared
      .engine
      .get_mut()
      .ping(memberlist_proto::Node::new(SmolStr::new("p2"), sa(3)), now);
    assert_ne!(id1, id2, "distinct pings mint distinct PingIds");

    let reply = shared.register_ping(id1);

    // A foreign id does not fire this waiter.
    shared.resolve_ping(id2, Ok(Duration::from_millis(1)));
    assert!(
      !reply.signaled(),
      "a foreign PingId leaves the waiter parked"
    );

    // The matching id resolves it with its own result.
    shared.resolve_ping(id1, Ok(Duration::from_millis(7)));
    assert!(
      matches!(reply.try_take(), Some(Ok(d)) if d == Duration::from_millis(7)),
      "the matching PingId resolves the waiter with its own RTT"
    );
  }

  /// `fail_all_waiters` resolves every parked ping AND send with `NotRunning`,
  /// so no awaiting handle op hangs after the run loop stops.
  #[test]
  fn fail_all_waiters_resolves_every_parked_op_not_running() {
    let mut shared = shared_node("t", 1);
    let now = Instant::from_origin(Duration::from_secs(1));

    let ping_id = shared
      .engine
      .get_mut()
      .ping(memberlist_proto::Node::new(SmolStr::new("p"), sa(2)), now);
    let sid = shared
      .engine
      .get_mut()
      .send_reliable(sa(3), bytes::Bytes::from_static(b"x"), now);

    let ping_reply = shared.register_ping(ping_id);
    let send_reply = shared.register_send(sid);

    shared.fail_all_waiters();

    assert!(
      matches!(ping_reply.try_take(), Some(Err(OpError::NotRunning))),
      "the parked ping fails with NotRunning"
    );
    assert!(
      matches!(send_reply.try_take(), Some(Err(OpError::NotRunning))),
      "the parked send fails with NotRunning"
    );
    // The waiter tables are now empty — a second sweep is a no-op.
    let w = shared.waiters.borrow();
    assert!(w.pings.is_empty() && w.sends.is_empty());
  }

  /// The `is_joined` / `advertise_address` free helpers forward the engine's
  /// view: a fresh single-node engine is not joined and reports its advertise
  /// address.
  #[test]
  fn free_helpers_forward_the_engine_view() {
    let shared = shared_node("t", 9);
    assert!(
      !is_joined(&shared),
      "a fresh single-node engine has only itself, so it is not joined"
    );
    assert_eq!(
      advertise_address(&shared),
      sa(9),
      "the advertise address is the configured one"
    );
  }
}
