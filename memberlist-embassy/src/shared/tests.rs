use super::{JOIN_OFFER_INTERVAL, Shared, advertise_address, is_joined};
use crate::{error::OpError, stream_io::SlotId};
use alloc::{vec, vec::Vec};
use core::{net::SocketAddr, time::Duration};
use memberlist_embedded::{
  Engine, GOSSIP_READ_CAP, GossipIo, Options as EngineConfig, TransformOptions,
};
use memberlist_proto::{EndpointOptions, Instant, SeedableRng, SmallRng, event::Event};
use smol_str::SmolStr;

fn sa(last: u8) -> SocketAddr {
  SocketAddr::from(([169, 254, 0, last], 7946))
}

/// A gossip view that carries no traffic: these tests exercise the waiter and
/// event plumbing, never a pump. It exists so the engine constructor can screen a
/// receive ring, and declares the largest conforming one.
struct NoGossip;

impl GossipIo for NoGossip {
  fn recv(&mut self, _buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    None
  }

  fn send(&mut self, _bytes: &[u8], _dest: SocketAddr) {}

  fn recv_capacity(&self) -> usize {
    GOSSIP_READ_CAP - 1
  }
}

/// The union the node would offer: the registry's address slice itself, which the
/// offer hands the engine without copying.
fn union(shared: &Shared<SmolStr>) -> Vec<SocketAddr> {
  shared.join_offers.borrow().addrs.clone()
}

/// Build a single-node engine wrapped as `Shared` for the waiter/buffer tests.
fn shared_node(id: &str, last: u8) -> Shared<SmolStr> {
  let now = Instant::from_origin(Duration::from_secs(1));
  let engine: Engine<SmolStr, SlotId> = Engine::new_at(
    EngineConfig::new().with_port(7946),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new(id), sa(last)),
    now,
    SmallRng::seed_from_u64(7),
    &NoGossip,
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
    EndpointOptions::new(SmolStr::new("t"), sa(1)),
    now,
    SmallRng::seed_from_u64(7),
    &NoGossip,
  );
  let shared: Shared<SmolStr> = Shared::new(engine);

  let sid1 = shared
    .engine
    .borrow_mut()
    .send_reliable(sa(2), bytes::Bytes::from_static(b"one"), now)
    .expect("issued while running");
  let sid2 = shared
    .engine
    .borrow_mut()
    .send_reliable(sa(3), bytes::Bytes::from_static(b"two"), now)
    .expect("issued while running");
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
    .ping(memberlist_proto::Node::new(SmolStr::new("p1"), sa(2)), now)
    .expect("issued while running");
  let id2 = shared
    .engine
    .get_mut()
    .ping(memberlist_proto::Node::new(SmolStr::new("p2"), sa(3)), now)
    .expect("issued while running");
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
    .ping(memberlist_proto::Node::new(SmolStr::new("p"), sa(2)), now)
    .expect("issued while running");
  let sid = shared
    .engine
    .get_mut()
    .send_reliable(sa(3), bytes::Bytes::from_static(b"x"), now)
    .expect("issued while running");

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

/// Counts the wakes one parked future receives.
///
/// `Waker::from` needs `Send + Sync`, which rules out a `Cell`; the count is only
/// ever read after the wake it is checking, so the ordering is immaterial.
#[derive(Default)]
struct WakeCounter {
  wakes: core::sync::atomic::AtomicUsize,
}

impl WakeCounter {
  fn count(&self) -> usize {
    self.wakes.load(core::sync::atomic::Ordering::Relaxed)
  }
}

impl alloc::task::Wake for WakeCounter {
  fn wake(self: alloc::sync::Arc<Self>) {
    self.wake_by_ref();
  }

  fn wake_by_ref(self: &alloc::sync::Arc<Self>) {
    self
      .wakes
      .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
  }
}

/// A join parked when the run loop ends must be WOKEN by `fail_all_waiters`.
///
/// Parked joins are not in the waiter tables that call resolves — each owns a waker
/// entry in the node-wide join notify — and with a per-join waker there is no other
/// joiner whose wake could stand in for this one. Its two ordinary wake sources, a
/// drained `NodeJoined` and `leave`, both run on the loop that has just ended, so
/// without the notify here the join has no wake source at all and parks forever.
///
/// The wake is what is asserted, not a verdict: this call resolves no lifecycle, so
/// what the woken join decides is the engine's answer to `ensure_running` — here the
/// engine is still running, which is exactly the case where the join re-checks and
/// parks again rather than returning `NotRunning`.
#[test]
fn fail_all_waiters_wakes_a_parked_join() {
  let shared = shared_node("t", 1);
  let seeds = [sa(9)];
  let offer = shared.register_join_offer(&seeds);

  let counter = alloc::sync::Arc::new(WakeCounter::default());
  let waker = core::task::Waker::from(counter.clone());
  let mut cx = core::task::Context::from_waker(&waker);

  // Park exactly as the join loop does: read the epoch, then poll the wait.
  let seen = shared.join_epoch();
  let mut wait = core::pin::pin!(shared.join_wait(offer.id(), seen));
  assert!(
    core::future::Future::poll(wait.as_mut(), &mut cx).is_pending(),
    "a join with nothing to converge on parks"
  );
  assert_eq!(counter.count(), 0, "parking is not itself a wake");

  shared.fail_all_waiters();

  assert_eq!(
    counter.count(),
    1,
    "the join parked when the run loop ended was not woken, so it had no wake \
     source left at all"
  );
  assert!(
    core::future::Future::poll(wait.as_mut(), &mut cx).is_ready(),
    "the epoch moved, so the wait resolves and the join re-runs its lifecycle and \
     membership checks"
  );
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

/// One address named by two live joins is held by a refcount, not a flag: the union
/// keeps it while either of them is still offering, and it leaves only when the last
/// one does.
///
/// A registry that stored membership alone would drop the shared address as soon as
/// the first guard went, taking a seed the surviving join is still trying to reach
/// out of its own offers.
#[test]
fn a_shared_seed_survives_until_the_last_join_holding_it_ends() {
  let shared = shared_node("refcount", 1);

  let first_seeds = [sa(10), sa(11)];
  let second_seeds = [sa(11), sa(12)];
  let first = shared.register_join_offer(&first_seeds);
  let second = shared.register_join_offer(&second_seeds);
  assert_eq!(
    shared.join_offer_addr_count(),
    3,
    "the shared address is one entry, not two"
  );
  assert_eq!(
    union(&shared),
    vec![sa(10), sa(11), sa(12)],
    "the offer names all three distinct addresses, once each"
  );

  drop(first);
  assert_eq!(
    shared.join_offer_addr_count(),
    2,
    "the ended join's exclusive seed leaves, while the shared one stays for the join \
     still offering it"
  );
  let after_first = union(&shared);
  assert!(
    after_first.contains(&sa(11)),
    "so the node keeps offering it"
  );
  assert!(
    !after_first.contains(&sa(10)),
    "while the ended join's exclusive seed is gone"
  );

  drop(second);
  assert_eq!(
    shared.join_offer_addr_count(),
    0,
    "the last guard releases everything it held"
  );
}

/// A repeated address within one join's seed list takes ONE registry entry and one
/// reference per occurrence, and the guard releases one per occurrence too — so the
/// registration balances exactly without the guard having to deduplicate (and so
/// copy) the caller's list.
#[test]
fn a_repeated_seed_within_one_join_takes_one_entry_and_a_count_per_occurrence() {
  let shared = shared_node("repeat", 1);

  let seeds = [sa(10), sa(10), sa(11)];
  let offer = shared.register_join_offer(&seeds);
  assert_eq!(
    shared.join_offer_addr_count(),
    2,
    "the repeat is not a second entry"
  );
  {
    let offers = shared.join_offers.borrow();
    let repeated = offers
      .addrs
      .iter()
      .position(|a| *a == sa(10))
      .expect("the repeated address is registered");
    assert_eq!(
      offers.counts[repeated], 2,
      "both occurrences took a reference, since the guard counts what it was given"
    );
  }

  assert_eq!(
    union(&shared),
    vec![sa(10), sa(11)],
    "and an offer carries each address once"
  );

  drop(offer);
  assert_eq!(
    shared.join_offer_addr_count(),
    0,
    "the guard releases one reference per occurrence, so both are given back"
  );
}

/// The guard holds a BORROW of the join future's own resolved list, not a copy of it.
///
/// The future owns that list for as long as it is offering, so a copy would hold
/// every live join's seeds twice: F joins naming the same U addresses would cost
/// `F × U` on top of the registry's `U` — on a device whose whole heap may be a few
/// kilobytes.
#[test]
fn guard_holds_no_copy_of_its_list() {
  let shared = shared_node("borrow", 1);
  let seeds = [sa(10), sa(11), sa(12)];

  let offer = shared.register_join_offer(&seeds);
  assert!(
    core::ptr::eq(offer.addrs.as_ptr(), seeds.as_ptr()),
    "the guard names the caller's own list rather than a copy of it"
  );
  assert_eq!(offer.addrs.len(), seeds.len(), "and all of it");
  assert_eq!(
    shared.join_offer_addr_count(),
    3,
    "while the registry holds the distinct addresses, as it always did"
  );

  drop(offer);
  assert_eq!(
    shared.join_offer_addr_count(),
    0,
    "and the borrow releases exactly what it registered"
  );
}

/// The registry holds every address the live joins offer, however many that is. One
/// that silently refused an address would put a later join's seeds outside the
/// earlier joins' offers — exactly the unmerged interleaving the union exists to
/// remove — so it has no cap, and its size is instead bounded by what the live
/// futures already hold: their own resolved seed lists, released in full when each
/// of them ends.
#[test]
fn union_registry_has_no_silent_cap() {
  let shared = shared_node("union", 1);

  // Two disjoint lists, 70 distinct addresses between them, from two separate joins.
  let first_addrs: Vec<SocketAddr> = (10u8..50).map(sa).collect();
  let second_addrs: Vec<SocketAddr> = (50u8..80).map(sa).collect();
  assert_eq!(
    first_addrs.len() + second_addrs.len(),
    70,
    "the two joins name 70 distinct addresses between them"
  );

  let first = shared.register_join_offer(&first_addrs);
  let second = shared.register_join_offer(&second_addrs);
  assert_eq!(
    shared.join_offer_addr_count(),
    70,
    "every offered address is registered, none silently refused"
  );

  // The one offer carries the whole union.
  let offered = union(&shared);
  assert_eq!(
    offered.len(),
    70,
    "the offer carries every registered address, once each"
  );
  for addr in first_addrs.iter().chain(second_addrs.iter()) {
    assert!(
      offered.contains(addr),
      "and names every address either join registered"
    );
  }

  drop(first);
  assert_eq!(
    shared.join_offer_addr_count(),
    second_addrs.len(),
    "an ended join releases exactly the seeds it registered, leaving the other's"
  );
  drop(second);
  assert_eq!(
    shared.join_offer_addr_count(),
    0,
    "and the last guard leaves nothing behind"
  );
}

/// The registry gives its memory back when the last join ends, rather than holding a
/// node's busiest moment for the rest of its uptime.
///
/// The union is the registry's own address vector, so its capacity is whatever the
/// largest concurrent set of joins grew it to — 70 addresses here. Emptying it by
/// removing entries would leave that buffer allocated with nothing in it, on a
/// device whose whole heap may be a few kilobytes.
#[test]
fn registry_releases_capacity_when_the_last_join_ends() {
  let shared = shared_node("release", 1);

  let first: Vec<SocketAddr> = (10u8..50).map(sa).collect();
  let second: Vec<SocketAddr> = (50u8..80).map(sa).collect();
  let first = shared.register_join_offer(&first);
  let second = shared.register_join_offer(&second);
  assert_eq!(
    shared.join_offer_addr_count(),
    70,
    "the two joins name 70 distinct addresses between them"
  );
  assert!(
    shared.join_offers.borrow().addrs.capacity() >= 70,
    "the registry grew to hold them"
  );

  drop(first);
  drop(second);
  assert_eq!(
    shared.join_offer_addr_count(),
    0,
    "the last guard leaves nothing registered"
  );
  let offers = shared.join_offers.borrow();
  assert_eq!(
    offers.addrs.capacity(),
    0,
    "and the address buffer is released, not merely emptied"
  );
  assert_eq!(
    offers.counts.capacity(),
    0,
    "as is the refcount buffer beside it"
  );
}

/// The node makes ONE offer per interval, and offers nothing at all when there is
/// nothing to offer — because no join is live, or because the node has left.
///
/// A left node must offer nothing whatever is registered, since the engine refuses a
/// left node's seeds and the join futures resolve on the lifecycle themselves. Either
/// way the last-offer clock is left alone: it paces what the node PUT ON THE WIRE, so
/// it is not rewound by a registry that happens to be empty.
#[test]
fn one_offer_per_interval_and_none_at_all_once_left() {
  let shared = shared_node("pacing", 1);
  let t0 = Instant::from_origin(Duration::from_secs(1));

  assert_eq!(
    shared.offer_join_seeds_at(t0),
    (false, None),
    "with no join live there is nothing to offer"
  );

  let first_seeds = [sa(10)];
  let offer = shared.register_join_offer(&first_seeds);
  let (offered, next) = shared.offer_join_seeds_at(t0);
  assert!(offered, "the first registration is offered at once");
  assert_eq!(
    next,
    Some(t0 + JOIN_OFFER_INTERVAL),
    "and the next offer is due one interval later"
  );
  assert!(
    !shared.offer_join_seeds_at(t0).0,
    "a second call inside the interval is paced, not a second offer"
  );
  assert!(
    shared.offer_join_seeds_at(t0 + JOIN_OFFER_INTERVAL).0,
    "and the interval's end is due, not still paced"
  );

  // Two offers have been made, the second at `t0 + JOIN_OFFER_INTERVAL`.
  let last_offer_due = t0 + JOIN_OFFER_INTERVAL * 2;

  drop(offer);
  assert_eq!(
    shared.offer_join_seeds_at(t0 + JOIN_OFFER_INTERVAL),
    (false, None),
    "the last join ending leaves nothing to offer"
  );
  assert_eq!(
    shared.next_join_offer.get(),
    Some(last_offer_due),
    "and the empty registry leaves the last-offer clock intact"
  );

  // A left node: registered seeds, but no offer the engine would accept.
  let second_seeds = [sa(11)];
  let _offer = shared.register_join_offer(&second_seeds);
  shared
    .engine
    .borrow_mut()
    .leave(t0 + JOIN_OFFER_INTERVAL)
    .expect("leave a running node");
  assert_eq!(
    shared.offer_join_seeds_at(t0 + JOIN_OFFER_INTERVAL),
    (false, None),
    "a left node offers nothing, however many joins are still registered"
  );
  assert_eq!(
    shared.next_join_offer.get(),
    Some(last_offer_due),
    "and leaves the clock alone there too"
  );
}

/// A join registered inside the interval of the node's last offer is offered at the
/// END of that interval, never sooner — even when the registry emptied in between.
///
/// The pacing clock is node-wide and continuous. Were it forgotten whenever no join
/// happened to be live, the floor would depend on the registry's history rather than
/// on time: a failing seed one join drops and another re-offers inside the same
/// interval would be dialed twice, and a caller could drive the dial rate as fast as
/// it can start and cancel joins.
#[test]
fn a_join_registering_after_another_ends_still_waits_out_the_interval() {
  let shared = shared_node("pacing-epoch", 1);
  let t0 = Instant::from_origin(Duration::from_secs(1));

  let a_seeds = [sa(10)];
  let a = shared.register_join_offer(&a_seeds);
  assert!(
    shared.offer_join_seeds_at(t0).0,
    "the first join is offered at once"
  );

  // A ends and B registers, both inside the interval of that offer.
  drop(a);
  let inside = t0 + JOIN_OFFER_INTERVAL / 2;
  assert_eq!(
    shared.offer_join_seeds_at(inside),
    (false, None),
    "an empty registry has nothing to offer"
  );

  let b_seeds = [sa(11)];
  let _b = shared.register_join_offer(&b_seeds);
  assert_eq!(
    shared.offer_join_seeds_at(inside),
    (false, Some(t0 + JOIN_OFFER_INTERVAL)),
    "B registered inside the interval, so it is not offered inside it — and the call \
     reports when it will be"
  );
  assert!(
    shared.offer_join_seeds_at(t0 + JOIN_OFFER_INTERVAL).0,
    "and it is offered at the end of that interval"
  );
}

/// A burst of joins that is cancelled must not leave its peak pinned behind the one
/// small join that outlived it: both the seed registry and the waiter vector give
/// their storage back as the burst's guards drop, so what the node holds — and what a
/// notify walks — tracks the joins alive NOW.
///
/// Releasing only when the last join ends cannot do that. One surviving join, a
/// single address of sixty-five, would hold the whole burst's capacity and its Θ(N)
/// notify walk for as long as it lives, and a node whose joins never all end at once
/// would never release any of it.
#[test]
fn a_survivor_does_not_pin_a_cancelled_bursts_capacity() {
  const BURST: usize = 64;

  let shared = shared_node("survivor", 1);

  // The join that outlives the burst: one address of its own.
  let survivor_seeds = [sa(200)];
  let _survivor = shared.register_join_offer(&survivor_seeds);

  // The burst, each join naming a distinct seed so the registry holds one entry per
  // join and both its vectors grow to the peak.
  let burst_seeds: Vec<[SocketAddr; 1]> = (0..BURST).map(|i| [sa(i as u8)]).collect();
  let burst: Vec<_> = burst_seeds
    .iter()
    .map(|seeds| shared.register_join_offer(seeds))
    .collect();

  assert_eq!(
    shared.join_offer_addr_count(),
    BURST + 1,
    "the burst and the survivor are all registered"
  );
  assert_eq!(
    shared.join_notify.waiters.borrow().len(),
    BURST + 1,
    "and each of them owns a waiter entry"
  );
  let peak = shared.join_offers.borrow().addrs.capacity();
  assert!(
    peak > BURST,
    "the burst grew the registry to its peak, got a capacity of {peak}"
  );

  // Every one of them is cancelled; only the survivor is still offering.
  drop(burst);

  assert_eq!(
    shared.join_offer_addr_count(),
    1,
    "only the survivor's address is still offered"
  );
  let waiters = shared.join_notify.waiters.borrow().len();
  assert_eq!(
    waiters, 1,
    "a notify walks the live joins, not the burst's high-water mark, got {waiters} \
     entries"
  );

  // The shrink rule (a quarter full, down to twice the length) settles at twice the
  // live length; the assertion allows one allocator step above that and is still an
  // order of magnitude below the peak it must not hold.
  let offers = shared.join_offers.borrow();
  let live = offers.addrs.len();
  assert_eq!(live, 1, "one live address");
  assert!(
    offers.addrs.capacity() <= 4 * live && offers.counts.capacity() <= 4 * live,
    "the survivor holds storage for the joins alive now, not for the cancelled \
     burst: addrs {}/{live}, counts {}/{live} (peak was {peak})",
    offers.addrs.capacity(),
    offers.counts.capacity()
  );
  assert!(
    shared.join_notify.waiters.borrow().capacity() <= 4,
    "and so does the waiter vector, got a capacity of {}",
    shared.join_notify.waiters.borrow().capacity()
  );
}
