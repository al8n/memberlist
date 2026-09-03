use super::{Shared, advertise_address, is_joined};
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
  let mut union = Vec::new();

  let first = shared.register_join_offer(&[sa(10), sa(11)]);
  let second = shared.register_join_offer(&[sa(11), sa(12)]);
  assert_eq!(
    shared.join_offer_addr_count(),
    3,
    "the shared address is one entry, not two"
  );

  first.fill_union(&mut union);
  assert_eq!(
    union.len(),
    3,
    "each offer names all three distinct addresses"
  );
  assert_eq!(
    &union[..2],
    &[sa(10), sa(11)],
    "an offer leads with its own seeds, in its own order"
  );
  assert!(union.contains(&sa(12)), "and carries the other join's seed");

  drop(first);
  assert_eq!(
    shared.join_offer_addr_count(),
    2,
    "the ended join's exclusive seed leaves, while the shared one stays for the join \
     still offering it"
  );
  second.fill_union(&mut union);
  assert!(
    union.contains(&sa(11)),
    "so the surviving join keeps offering it"
  );
  assert!(
    !union.contains(&sa(10)),
    "while the ended join's exclusive seed is gone"
  );

  drop(second);
  assert_eq!(
    shared.join_offer_addr_count(),
    0,
    "the last guard releases everything it held"
  );
}

/// A repeated address within one join's seed list takes ONE registry entry and ONE
/// reference, so the guard's release balances its registration exactly rather than
/// leaving the address pinned in the union forever.
#[test]
fn a_repeated_seed_within_one_join_registers_once() {
  let shared = shared_node("repeat", 1);

  let offer = shared.register_join_offer(&[sa(10), sa(10), sa(11)]);
  assert_eq!(
    shared.join_offer_addr_count(),
    2,
    "the repeat is not a second entry"
  );

  let mut union = Vec::new();
  offer.fill_union(&mut union);
  assert_eq!(
    union,
    vec![sa(10), sa(11)],
    "and an offer carries each address once"
  );

  drop(offer);
  assert_eq!(
    shared.join_offer_addr_count(),
    0,
    "one registration, one release"
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

  // Either join's offer carries the whole union: same set, each leading with its own
  // seeds.
  let mut union = Vec::new();
  for (offer, own) in [(&first, &first_addrs), (&second, &second_addrs)] {
    offer.fill_union(&mut union);
    assert_eq!(
      union.len(),
      70,
      "an offer carries every registered address, once each"
    );
    assert_eq!(
      &union[..own.len()],
      &own[..],
      "leading with its own seeds, in its own order"
    );
    for addr in first_addrs.iter().chain(second_addrs.iter()) {
      assert!(
        union.contains(addr),
        "and naming every address the other join registered"
      );
    }
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
