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
  future::Future,
  net::SocketAddr,
  pin::Pin,
  task::{Context, Poll, Waker},
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

/// The node-wide floor on how often the live joins' seeds are offered to the engine.
///
/// Offering is a NODE-wide activity — one offer carries every live join's seeds — so
/// this paces how fast a seed whose exchange just failed can be dialed again: a peer
/// whose single reliable listener is occupied RSTs each dial at link speed, and an
/// unpaced re-offer would spin dial → RST → event → dial with no floor at all. A
/// quarter second holds that to one SYN per seed per interval, negligible next to a
/// node's other traffic and far inside the seconds-long deadline a caller puts around
/// a join.
///
/// # Contract
///
/// The node makes AT MOST ONE union offer per interval, measured on a node-wide
/// last-offer clock that keeps running across registrations: it is not reset when the
/// registry empties, nor when a node leaves. A join registered within an interval of
/// the last offer is therefore offered at the END of that interval — a first-offer
/// delay of at most one interval — and never sooner.
///
/// Pacing on the offers alone, rather than on the registry's history, is what makes
/// the floor deterministic. A clock forgotten whenever no join happened to be live
/// would let a seed one join dropped and another re-offered inside the same interval
/// be dialed twice, and would let a caller drive the dial rate as fast as it can
/// start and cancel joins. What the bound has to hold down is what the node puts on
/// the wire, so it is measured from the last thing the node put there.
pub(crate) const JOIN_OFFER_INTERVAL: Duration = Duration::from_millis(250);

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
/// registry. What bounds it instead is the DISTINCT addresses held here plus the live
/// join futures' OWN RESOLVED SEED LISTS — memory the application already holds,
/// since each live future owns its resolved list for as long as it is offering, and
/// [`JoinOffer`] borrows that list rather than copying it. [`JoinOffer`] releases
/// exactly what it registered on every exit path, so a future that is no longer
/// offering leaves nothing behind.
///
/// What it holds therefore scales with the joins alive NOW, not with any past peak:
/// released entries give their storage back as they go (see [`JoinOffers::shrink`]),
/// so a burst of joins that is cancelled leaves nothing pinned behind the one small
/// join that survived it.
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
      self.shrink();
    }
  }

  /// Give back storage a burst of joins grew and their cancellation left behind, so
  /// what the registry holds tracks the joins alive NOW rather than the busiest
  /// moment the node ever saw.
  ///
  /// Releasing only at empty is not enough for that: one surviving join — one address
  /// out of a hundred — would pin the whole peak for as long as it lives, and a node
  /// whose joins never all end at once would never release any of it.
  ///
  /// Shrinking at a QUARTER full, down to twice the length, is what keeps this
  /// amortized O(1) per removal. The gap it leaves is the hysteresis: after a shrink
  /// the registry has room for its length again in pushes before it has to grow, and
  /// must lose half its length again before it shrinks again, so no
  /// register/unregister pair at the boundary can be made to reallocate on every
  /// call. Shrinking to fit at half full has no such gap and is exactly that
  /// pathology.
  fn shrink(&mut self) {
    if self.addrs.is_empty() {
      // No join is live, so nothing needs the capacity at all: release it rather than
      // hold a node's peak seed count for the rest of its uptime. Fresh empty vectors
      // allocate nothing, so this is a free reset.
      self.addrs = Vec::new();
      self.counts = Vec::new();
      return;
    }
    let len = self.addrs.len();
    if len <= self.addrs.capacity() / 4 {
      self.addrs.shrink_to(len * 2);
    }
    if len <= self.counts.capacity() / 4 {
      self.counts.shrink_to(len * 2);
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
  /// The join future's OWN resolved seed list, borrowed for as long as the guard
  /// lives — never copied. The future owns that list for the whole time it is
  /// offering, so a copy here would hold every live join's seeds twice: F joins over
  /// the same U addresses would cost `F × U` on top of the registry's `U`.
  ///
  /// Every occurrence in it was registered in `offers`, so `Drop` releases the same
  /// number of references: an address repeated within one list is counted twice and
  /// decremented twice, which balances without deduplicating anything.
  addrs: &'a [SocketAddr],
  /// The node-wide join notify this future parks on, and the id of the waiter entry
  /// it owns there. The entry is removed with the registration, so a join that has
  /// ended leaves no waker behind for a notify to wake and none of its storage
  /// behind for a notify to walk.
  notify: &'a JoinNotify,
  id: JoinId,
}

impl JoinOffer<'_> {
  /// The id of the waiter entry this join's waits park in.
  #[inline]
  pub(crate) fn id(&self) -> JoinId {
    self.id
  }
}

impl Drop for JoinOffer<'_> {
  fn drop(&mut self) {
    {
      let mut offers = self.offers.borrow_mut();
      for addr in self.addrs {
        offers.unregister(*addr);
      }
    }
    self.notify.release(self.id);
  }
}

/// The identity of one live join's entry in [`JoinNotify`], minted when it takes the
/// entry and spent when its [`JoinOffer`] drops.
///
/// An id rather than a position because the entries are REMOVED as joins end, so a
/// position would name a different join after any removal that moved the vector's
/// tail. The counter behind it is monotone: two live entries could only share an id
/// after 2^64 joins had been registered without this one ending, which no uptime
/// reaches.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct JoinId(u64);

/// The node-wide join notify: one wake epoch plus one waker entry per live join.
///
/// Every live [`join`](crate::Memberlist::join) owns an entry for as long as it is
/// offering, so a wake reaches EVERY parked join rather than one of them. A
/// single-consumer signal cannot do that: storing a waker there REPLACES the one
/// already held and wakes the displaced task, so two joins parked on distinct tasks
/// wake each other forever with nothing behind it — a spin that starves the very
/// Runner they are both waiting on, and one that a test polling both from a single
/// task cannot see.
///
/// The epoch is what makes a wake impossible to lose. A join reads it BEFORE it
/// checks the node's lifecycle and membership; its wait then resolves immediately if
/// the epoch has moved since, so a notify landing between those checks and the park
/// is observed rather than slept through.
///
/// Entries are removed as joins end rather than blanked in place, so both the
/// storage and the walk a notify makes are Θ(joins alive NOW). A join that has ended
/// leaves nothing for a later notify to step over, and one small surviving join does
/// not pin the vector a cancelled burst grew.
///
/// Wake sources, and the whole set of them: a drained [`Event::NodeJoined`] — the one
/// event that can add a member, and so the only one that can change the `is_joined()`
/// a parked join is waiting on — [`leave`](crate::Memberlist::leave), which changes
/// the lifecycle answer the same join checks first, and the run future going away
/// through [`Shared::stop_runner`] (it returned, or was dropped by a `select` that
/// lost or a task teardown). The third does not change either of the first two
/// answers; it is a source because it removes both, and it gives the woken join a
/// third, terminal answer of its own — the stopped flag `stop_runner` sets before it
/// wakes anyone — so the join returns rather than parking again. Ordinary traffic (user
/// packets, pings, sends, exchange completions) wakes nobody: it cannot change either
/// answer, and the seed re-offer that a failed exchange leads to is paced by the
/// Runner's own offer clock, not by a parked join re-polling.
#[derive(Default)]
pub(crate) struct JoinNotify {
  /// Bumped by every notify. A parked join compares it against the value it read
  /// before its checks; any difference means something those checks may care about
  /// has happened, so the wait resolves and the loop re-runs them.
  epoch: Cell<u64>,
  /// Mints the next [`JoinId`]. Monotone, so no live entry is ever confused with
  /// another.
  next_id: Cell<u64>,
  /// One entry per LIVE join, keyed by the [`JoinId`] its [`JoinOffer`] holds and
  /// removed when that guard drops, so the vector is exactly as long as the joins
  /// alive now. The waker is seeded no-op when the join takes its entry and replaced
  /// by the real one at its first park, so an entry that exists is always wakeable
  /// and a notify never has to tell "registered but not yet parked" from "gone".
  waiters: RefCell<Vec<(JoinId, Option<Waker>)>>,
}

impl JoinNotify {
  /// The current wake epoch.
  #[inline]
  fn epoch(&self) -> u64 {
    self.epoch.get()
  }

  /// Take an entry for one live join and return the id that names it.
  fn acquire(&self) -> JoinId {
    let id = JoinId(self.next_id.get());
    // Wrapping keeps the arithmetic total rather than resting on the counter never
    // reaching the end; see [`JoinId`] for why a repeat cannot collide with a live
    // entry.
    self.next_id.set(self.next_id.get().wrapping_add(1));
    // A no-op waker marks the entry live before its join has ever parked. Waking it
    // does nothing, and the join's first park replaces it with the real waker.
    self
      .waiters
      .borrow_mut()
      .push((id, Some(Waker::noop().clone())));
    id
  }

  /// Remove one join's entry, giving back the storage a burst of joins grew.
  fn release(&self, id: JoinId) {
    let mut waiters = self.waiters.borrow_mut();
    let Some(pos) = waiters.iter().position(|(held, _)| *held == id) else {
      return;
    };
    waiters.swap_remove(pos);
    if waiters.is_empty() {
      // No join is live, so nothing needs the capacity the busiest moment grew:
      // release it beside the registry's rather than hold a node's peak concurrent
      // join count for the rest of its uptime. A fresh empty vector allocates nothing.
      *waiters = Vec::new();
      return;
    }
    // Otherwise shrink on the same quarter-full / down-to-twice-the-length rule the
    // registry uses, for the same reason and with the same amortized O(1) cost per
    // removal: one surviving join must not pin the capacity a cancelled burst grew.
    let len = waiters.len();
    if len <= waiters.capacity() / 4 {
      waiters.shrink_to(len * 2);
    }
  }

  /// Store `waker` in the entry `id` names for the next notify, skipping the clone
  /// when that entry already holds a waker that wakes the same task.
  fn park(&self, id: JoinId, waker: &Waker) {
    let mut waiters = self.waiters.borrow_mut();
    // The guard that owns this id lives for the whole wait, so the entry is always
    // present. A missing one means the join has already ended and has nothing left to
    // wake.
    if let Some((_, entry)) = waiters.iter_mut().find(|(held, _)| *held == id) {
      match entry {
        Some(held) if held.will_wake(waker) => {}
        other => *other = Some(waker.clone()),
      }
    }
  }

  /// Bump the epoch and wake every parked join.
  fn notify(&self) {
    // Wrapping: a parked join compares epochs for INEQUALITY, so the counter only has
    // to change. Wrapping keeps the arithmetic total instead of resting on a node
    // never draining 2^64 event batches.
    self.epoch.set(self.epoch.get().wrapping_add(1));

    // Take a clone of each waker rather than holding the borrow across the wake: a
    // waker runs code this module does not own, and one that re-entered the registry
    // would alias the guard. Walking from the END DOWN is what keeps that re-entrancy
    // safe now that a release REMOVES its entry: `swap_remove` fills the hole with the
    // LAST entry, and every index above the cursor has already been woken, so a
    // release running inside a wake can only move an already-woken entry into the part
    // still to walk — never carry an unwoken one out of it.
    let mut idx = self.waiters.borrow().len();
    while idx > 0 {
      idx -= 1;
      let waker = {
        let waiters = self.waiters.borrow();
        match waiters.get(idx) {
          // The vector shrank under a re-entrant release; the entries below this one
          // are still to walk.
          None => continue,
          Some((_, entry)) => entry.clone(),
        }
      };
      if let Some(waker) = waker {
        waker.wake();
      }
    }
  }
}

/// A parked [`join`](crate::Memberlist::join)'s wait for the next join notify.
///
/// Resolves as soon as the epoch differs from the one its join read before its
/// checks — so a notify that landed in between is observed, not slept through — and
/// otherwise parks the polling task's waker in the entry that join's [`JoinOffer`]
/// owns. Every parked join has an entry of its own, so one notify wakes all of them.
pub(crate) struct JoinWait<'a> {
  notify: &'a JoinNotify,
  id: JoinId,
  seen: u64,
}

impl Future for JoinWait<'_> {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.notify.epoch() != self.seen {
      return Poll::Ready(());
    }
    self.notify.park(self.id, cx.waker());
    Poll::Pending
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
  /// The node-wide join notify: bumped and fanned out to EVERY parked `join` when a
  /// drained machine event ADDED a member, and on `leave`. Each live join owns a
  /// waker entry, so no joiner depends on another one being woken in its place.
  pub(crate) join_notify: JoinNotify,
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
  /// Runner pumps with. `None` means the node has never offered; otherwise this is
  /// one [`JOIN_OFFER_INTERVAL`] past the last offer it made.
  ///
  /// The clock is node-wide and survives the registry emptying, so the interval
  /// bounds the node's OFFERS rather than any one join's lifetime — see
  /// [`JOIN_OFFER_INTERVAL`] for the contract that follows from it.
  ///
  /// Private to this module: it is the offer step's own bookkeeping, and every
  /// caller reaches it through [`Shared::offer_join_seeds_at`].
  next_join_offer: Cell<Option<Instant>>,
  /// Whether the [`Runner`](crate::Runner) driving this node is gone: its `run`
  /// future returned, or — the way out its `-> !` signature cannot express — was
  /// DROPPED, by a `select` that lost or by a task teardown.
  ///
  /// Terminal and one-way. The pump is the only thing that completes a parked
  /// handle op, so once it is gone nothing behind a park can ever resolve: every
  /// op parked at the transition is failed, and every later one is refused with
  /// [`OpError::RunnerStopped`] rather than parked on a wake that cannot come.
  ///
  /// Distinct from the engine's own lifecycle, which the handle ops check
  /// separately: a node that has LEFT answers [`OpError::NotRunning`], and a node
  /// whose runner is gone may never have left at all.
  ///
  /// Private to this module: [`Shared::stop_runner`] is the only way to set it, so
  /// the flag and the release of everything parked on the runner always happen
  /// together and in that order.
  runner_stopped: Cell<bool>,
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
      join_notify: JoinNotify::default(),
      app_events: RefCell::new(VecDeque::new()),
      waiters: RefCell::new(Waiters::default()),
      join_offers: RefCell::new(JoinOffers::default()),
      next_join_offer: Cell::new(None),
      runner_stopped: Cell::new(false),
    }
  }

  /// Whether the run loop driving this node is gone — see
  /// [`runner_stopped`](Self::runner_stopped) the field for what that means for a
  /// handle op.
  #[inline]
  pub(crate) fn runner_stopped(&self) -> bool {
    self.runner_stopped.get()
  }

  /// Mark the run loop terminally gone and release everything parked on it.
  ///
  /// The ORDER is load-bearing: the flag is set BEFORE the waiters are failed, so
  /// every op this call reaches — a parked ping/send resolving its signal, a parked
  /// join re-running its checks on the wake — observes the terminal state and
  /// reports it, instead of parking again on a wake that can no longer arrive.
  pub(crate) fn stop_runner(&self) {
    self.runner_stopped.set(true);
    self.fail_all_waiters();
  }

  /// Register `seeds` as one live join future's offer and hand back the guard that
  /// releases them again when that future ends — returned, cancelled or dropped.
  ///
  /// The guard BORROWS the caller's list: the join future owns its resolved seeds for
  /// as long as it is offering, so the node's join memory is exactly those lists plus
  /// the registry's distinct addresses, with no per-guard copy on top.
  ///
  /// Each occurrence in the list takes a reference and the guard releases the same
  /// number, so a repeat within one list needs no deduplication here — the registry
  /// holds one entry per distinct address either way, and the refcount balances.
  pub(crate) fn register_join_offer<'a>(&'a self, seeds: &'a [SocketAddr]) -> JoinOffer<'a> {
    {
      let mut offers = self.join_offers.borrow_mut();
      for seed in seeds {
        offers.register(*seed);
      }
    }

    JoinOffer {
      offers: &self.join_offers,
      addrs: seeds,
      notify: &self.join_notify,
      id: self.join_notify.acquire(),
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
  /// Returns `(false, None)` when there is nothing to offer: no live join, or a node
  /// that has left (which refuses offers anyway). That case leaves the last-offer
  /// clock alone — the interval paces the node's offers, not a join's lifetime, so a
  /// join registered just after another one ended still waits out the rest of the
  /// interval rather than being dialed inside it (see [`JOIN_OFFER_INTERVAL`]).
  /// Otherwise returns when the next offer is due, which the Runner folds into its
  /// sleep deadline so a node with live joins wakes to re-offer even when the
  /// machine has no earlier work of its own.
  ///
  /// Borrows only; never awaits. `join_offers` and `engine` are separate cells, so
  /// holding the registry across the engine call aliases nothing.
  pub(crate) fn offer_join_seeds_at(&self, now: Instant) -> (bool, Option<Instant>) {
    let offers = self.join_offers.borrow();
    if offers.addrs.is_empty() || self.engine.borrow().ensure_running().is_err() {
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

  /// Wake EVERY parked `join`: a member was added, or the node's lifecycle changed.
  ///
  /// Each live join owns a waker entry, so this reaches all of them — no joiner waits
  /// on another one having been woken in its place. Call it only for those two
  /// changes; a parked join re-checks exactly the lifecycle and the member count, so
  /// a wake for anything else is a fan-out over every live join that can only park
  /// them again (see [`JoinNotify`]).
  ///
  /// [`fail_all_waiters`](Self::fail_all_waiters) wakes the same joins without going
  /// through here, because it is the one wake that is NOT about a changed answer: it
  /// fires when the run loop ends and the two sources above can no longer arrive.
  /// The join it wakes then reads the terminal state
  /// [`stop_runner`](Self::stop_runner) recorded first, and returns.
  #[inline]
  pub(crate) fn notify_join_waiters(&self) {
    self.join_notify.notify();
  }

  /// The join notify's current epoch, which a join reads BEFORE its lifecycle and
  /// membership checks so a notify racing them cannot be slept through.
  #[inline]
  pub(crate) fn join_epoch(&self) -> u64 {
    self.join_notify.epoch()
  }

  /// Park the join owning `id` until the notify moves past the epoch it read.
  #[inline]
  pub(crate) fn join_wait(&self, id: JoinId, seen: u64) -> JoinWait<'_> {
    JoinWait {
      notify: &self.join_notify,
      id,
      seen,
    }
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
  /// buffering every event for the handle's `poll_event`, and — only when one of them
  /// ADDED a member — notifying every parked `join` so they re-check membership.
  ///
  /// Called by the Runner once per loop, AFTER `pump` (so it sees this tick's
  /// freshly-emitted events). The Runner is the sole `poll_event` caller on the
  /// engine, so it must re-buffer the application-facing events it drains rather
  /// than discard them. Takes only brief `RefCell` borrows; never awaits.
  pub(crate) fn drain_events(&self) {
    use memberlist_proto::event::ExchangeKind;

    // Only an event that can change a parked `join`'s answer wakes them, and one
    // class can: a member being ADDED. `is_joined()` is a member count, and a
    // membership insertion always surfaces as `NodeJoined` — every other event acts
    // on a record the node already holds, or carries no membership at all. Waking on
    // the rest (user packets, pings, sends, exchange completions) would fan a wake out
    // to every live join for a re-check whose two answers cannot have moved. The
    // lifecycle half of that pair is `leave`, which notifies at its own call site.
    let mut member_added = false;
    loop {
      let ev = self.engine.borrow_mut().poll_event();
      let Some(ev) = ev else { break };

      // Resolve any waiter this event terminates (correlation is additive — the
      // event is still buffered for the application below).
      match &ev {
        // A member was added, so the count a parked join waits on may now satisfy it.
        // A resurrection (`Dead`/`Left` → `Alive`) emits this too and leaves the count
        // alone; one extra re-check of two cheap conditions is the right side to be
        // wrong on, where a missed insertion would be a join parked until its caller's
        // deadline.
        Event::NodeJoined(_) => member_added = true,
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
    if member_added {
      self.notify_join_waiters();
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

  /// Fail every parked waiter and wake every parked join, so no awaiting handle op
  /// is left on a wake that can no longer arrive.
  ///
  /// The reason follows the runner's state. Once [`stop_runner`](Self::stop_runner)
  /// has recorded the run loop as gone, it is [`OpError::RunnerStopped`]: this node
  /// has no driver left, so nothing the caller does can turn the op into a success.
  /// Otherwise it is [`OpError::NotRunning`], the engine's own lifecycle answer.
  ///
  /// A parked [`join`](crate::Memberlist::join) is not in these tables — it owns a
  /// waker entry in [`JoinNotify`] instead — so it is WOKEN here rather than
  /// resolved. Without that wake it would have no wake source left at all: its two
  /// ordinary ones are a drained `NodeJoined` and `leave`, and both are reached
  /// through the loop that has just ended. Because the stopped flag is set before
  /// this call, the woken join re-runs its checks, sees the terminal state and
  /// returns `RunnerStopped` rather than finding both of its ordinary answers
  /// unchanged and parking again on the new epoch.
  pub(crate) fn fail_all_waiters(&self) {
    // One read for the whole call: the flag is set before the waiters are released,
    // so every op failed here gets the same, already-settled answer.
    let stopped = self.runner_stopped.get();
    let reason = || {
      if stopped {
        OpError::RunnerStopped
      } else {
        OpError::NotRunning
      }
    };
    let mut w = self.waiters.borrow_mut();
    let pings = core::mem::take(&mut w.pings);
    let sends = core::mem::take(&mut w.sends);
    drop(w);
    for p in pings {
      p.reply.signal(Err(reason()));
    }
    for s in sends {
      s.reply.signal(Err(reason()));
    }
    // Joins park on the node-wide notify rather than in `waiters`, so signalling the
    // tables above reaches none of them.
    self.join_notify.notify();
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
