//! Scenario coverage for the SWIM Alive-handling state transitions, mirroring
//! the behavioral oracle from the original gossip implementation.
//!
//! Each test drives the synchronous [`Endpoint`] directly: inject an `Alive`
//! via [`Endpoint::process_alive`] (no `AliveDelegate` installed, so every
//! `Alive` is admitted), then observe via the public accessors
//! ([`member_liveness`](Endpoint::member_liveness),
//! [`node_incarnation`](Endpoint::node_incarnation),
//! [`member`](Endpoint::member), [`node_state_change`](Endpoint::node_state_change))
//! and the drained event / broadcast queues.
//!
//! Liveness assertions use [`member_liveness`](Endpoint::member_liveness): the
//! wire `NodeState` returned by [`member`](Endpoint::member) fixes its `state`
//! field at insertion, while the gossip-maintained liveness lives on the
//! `LocalNodeState`. Metadata / address assertions read the wire `NodeState`,
//! which is replaced wholesale on each accepted update.

use super::*;
use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};
use smol_str::SmolStr;

use crate::typed::{DelegateVersion, ProtocolVersion};
use Alive;
use Dead;
use Message;
use Meta;
use Node;
use State;
use Suspect;

fn cfg() -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  )
  .with_rng_seed(0xdeadbeef)
}

fn node(id: &str, port: u16) -> Node<SmolStr, SocketAddr> {
  Node::new(
    SmolStr::new(id),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
  )
}

fn alive_of(n: &Node<SmolStr, SocketAddr>, inc: u32) -> Alive<SmolStr, SocketAddr> {
  Alive::new(inc, n.cheap_clone())
    .with_meta(Meta::empty())
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1)
}

/// A `Dead` for `target` accused by `from` at incarnation `inc`. When
/// `target == from` this is the self-marked-departure sentinel that resolves to
/// `State::Left` rather than `State::Dead`.
fn dead_of(target: &SmolStr, from: &SmolStr, inc: u32) -> Dead<SmolStr> {
  Dead::new(inc, target.cheap_clone(), from.cheap_clone())
}

/// A `Suspect` for `target` accused by `from` at incarnation `inc`.
fn suspect_of(target: &SmolStr, from: &SmolStr, inc: u32) -> Suspect<SmolStr> {
  Suspect::new(inc, target.cheap_clone(), from.cheap_clone())
}

/// Drain the gossip queue and collect the incarnations of every queued `Alive`
/// broadcast targeting `id`.
///
/// `Endpoint::new` seeds a self-`Alive` so peers learn about us immediately, so
/// the global queue length is construction-dependent; inspecting the broadcast
/// targeting a specific node is the construction-independent way to assert that
/// a given transition did (or did not) gossip about that node.
fn drained_alive_incarnations(e: &mut Endpoint<SmolStr, SocketAddr>, id: &SmolStr) -> Vec<u32> {
  e.drain_broadcasts()
    .into_iter()
    .filter_map(|m| match m {
      Message::Alive(a) if a.node_ref().id_ref() == id => Some(a.incarnation()),
      _ => None,
    })
    .collect()
}

/// A first Alive for an unknown peer inserts it as `Alive` at the advertised
/// incarnation, emits a single `NodeJoined`, stamps the state-change at "now",
/// and enqueues exactly one gossip broadcast.
#[test]
fn alive_node_new_node() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  // Drain the construction-time events for the local node first.
  while e.poll_event().is_some() {}

  let test = node("test", 8000);
  let now = Instant::now();
  e.process_alive(alive_of(&test, 1), false, now);

  // Exactly the local node plus the new peer.
  assert_eq!(e.num_members(), 2);

  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Alive));
  assert_eq!(e.node_incarnation(test.id_ref()), Some(1));

  // The state-change is stamped at the instant we processed the Alive.
  assert_eq!(e.node_state_change(test.id_ref()), Some(now));

  let ev = e.poll_event().expect("expected NodeJoined");
  match ev {
    Event::NodeJoined(n) => assert_eq!(n.id_ref(), test.id_ref()),
    other => panic!("expected NodeJoined, got {other:?}"),
  }
  assert!(e.poll_event().is_none(), "exactly one event expected");

  // The new node's Alive is gossiped.
  assert_eq!(
    drained_alive_incarnations(&mut e, test.id_ref()),
    vec![1],
    "the new node's Alive should be queued for gossip"
  );
}

/// Re-applying an Alive at a strictly higher incarnation to an already-`Alive`
/// peer with unchanged metadata keeps it `Alive`, does not restamp the
/// state-change, and emits no event.
#[test]
fn alive_node_idempotent() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  e.process_alive(alive_of(&test, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  let change = e
    .node_state_change(test.id_ref())
    .expect("state-change recorded");

  // Higher incarnation, same (empty) meta on an already-Alive node.
  e.process_alive(alive_of(&test, 2), false, Instant::now());

  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Alive));
  assert_eq!(
    e.node_state_change(test.id_ref()),
    Some(change),
    "an already-Alive node must not restamp its state-change"
  );
  assert!(
    e.poll_event().is_none(),
    "no event on an idempotent re-Alive"
  );
  // The node is represented once in the gossip queue, at its latest incarnation.
  assert_eq!(
    drained_alive_incarnations(&mut e, test.id_ref()),
    vec![2],
    "the node should be gossiped once, at its latest incarnation"
  );
}

/// A peer's metadata change at a higher incarnation updates the tracked meta
/// and emits `NodeUpdated`.
#[test]
fn alive_node_change_meta() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  let meta1 = Meta::try_from(Bytes::from_static(b"val1")).unwrap();
  e.process_alive(alive_of(&test, 1).with_meta(meta1), false, Instant::now());
  while e.poll_event().is_some() {}

  let meta2 = Meta::try_from(Bytes::from_static(b"val2")).unwrap();
  e.process_alive(
    alive_of(&test, 2).with_meta(meta2.cheap_clone()),
    false,
    Instant::now(),
  );

  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Alive));
  assert_eq!(
    e.member(test.id_ref()).unwrap().meta_ref().as_bytes(),
    meta2.as_bytes(),
    "meta should be updated"
  );

  let ev = e.poll_event().expect("expected NodeUpdated");
  match ev {
    Event::NodeUpdated(n) => {
      assert_eq!(n.id_ref(), test.id_ref());
      assert_eq!(n.meta_ref().as_bytes(), meta2.as_bytes());
    }
    other => panic!("expected NodeUpdated, got {other:?}"),
  }
}

/// A conflicting Alive about ourselves (claiming a higher incarnation and new
/// metadata) is refuted: we stay `Alive`, our metadata is unchanged, and a
/// single self-`Alive` is broadcast to defend the local node.
#[test]
fn alive_node_refute() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  while e.poll_event().is_some() {}

  let local = Node::new(e.local_id_ref().cheap_clone(), *e.advertise_ref());

  // Bootstrap self-Alive at our starting incarnation.
  e.process_alive(alive_of(&local, 1), true, Instant::now());

  // A higher-incarnation Alive about us carrying foreign metadata.
  let foreign = Meta::try_from(Bytes::from_static(b"foo")).unwrap();
  e.process_alive(
    alive_of(&local, 2).with_meta(foreign),
    false,
    Instant::now(),
  );

  assert_eq!(
    e.member_liveness(e.local_id_ref()),
    Some(State::Alive),
    "should still be alive"
  );
  assert!(
    e.member(e.local_id_ref())
      .unwrap()
      .meta_ref()
      .as_bytes()
      .is_empty(),
    "meta should still be empty after refute"
  );

  // Refute broadcasts a fresh self-Alive at an incarnation past the accusation
  // (the bumped incarnation also replaces the construction-seeded self-Alive,
  // so exactly one self-Alive remains queued).
  let local_id = e.local_id_ref().cheap_clone();
  let incs = drained_alive_incarnations(&mut e, &local_id);
  assert_eq!(incs.len(), 1, "exactly one self-Alive should be queued");
  assert!(
    incs[0] > 2,
    "refute should broadcast an Alive past the accused incarnation, got {}",
    incs[0]
  );
}

/// A conflicting Alive for a known id at a different address does not overwrite
/// the tracked address/meta; it emits `NodeConflict` and enqueues nothing.
/// Once the original entry is `Dead` and the reclaim window has elapsed, a
/// fresh Alive at the new address is adopted.
#[test]
fn alive_node_conflict() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(cfg().with_dead_node_reclaim_time(Duration::from_millis(10)));
  while e.poll_event().is_some() {}

  let id = SmolStr::new("test");
  let node1 = Node::new(id.cheap_clone(), "127.0.0.1:8000".parse().unwrap());
  let t0 = Instant::now();
  e.process_alive(alive_of(&node1, 1), true, t0);
  while e.poll_event().is_some() {}

  // Conflicting Alive: same id, different address, higher incarnation, meta.
  // The conflict path returns before queueing anything, so the queue length is
  // unchanged across the call.
  let node2 = Node::new(id.cheap_clone(), "127.0.0.2:9000".parse().unwrap());
  let foo = Meta::try_from(Bytes::from_static(b"foo")).unwrap();
  let queued_before = e.broadcast_queue_len();
  e.process_alive(alive_of(&node2, 2).with_meta(foo.cheap_clone()), false, t0);

  // The original entry is untouched.
  assert_eq!(
    e.member_liveness(&id),
    Some(State::Alive),
    "should still be alive"
  );
  {
    let n = e.member(&id).unwrap();
    assert!(
      n.meta_ref().as_bytes().is_empty(),
      "meta should still be empty"
    );
    assert_eq!(n.id_ref(), &id, "id should not be updated");
    assert_eq!(
      n.address_ref(),
      node1.addr_ref(),
      "addr should not be updated"
    );
  }
  let ev = e.poll_event().expect("expected NodeConflict");
  match ev {
    Event::NodeConflict(p) => {
      assert_eq!(p.existing_ref().address_ref(), node1.addr_ref());
      assert_eq!(p.other_ref().address_ref(), node2.addr_ref());
    }
    other => panic!("expected NodeConflict, got {other:?}"),
  }
  // A pure conflict enqueues no broadcast.
  assert_eq!(
    e.broadcast_queue_len(),
    queued_before,
    "a pure conflict must not enqueue a broadcast"
  );

  // Mark the node Dead, then let the reclaim window elapse.
  e.process_dead(
    Dead::new(2, id.cheap_clone(), e.local_id_ref().cheap_clone()),
    t0,
  );
  assert_eq!(e.member_liveness(&id), Some(State::Dead), "should be dead");
  while e.poll_event().is_some() {}

  // After the reclaim window, the new address is adopted.
  let later = t0 + Duration::from_millis(11);
  e.process_alive(
    alive_of(&node2, 3).with_meta(foo.cheap_clone()),
    false,
    later,
  );

  assert_eq!(
    e.member_liveness(&id),
    Some(State::Alive),
    "should be alive again"
  );
  {
    let n = e.member(&id).unwrap();
    assert_eq!(n.meta_ref().as_bytes(), b"foo", "meta should be updated");
    assert_eq!(n.address_ref(), node2.addr_ref(), "addr should be updated");
  }
  // The adopted node is gossiped at its new incarnation.
  assert_eq!(
    drained_alive_incarnations(&mut e, &id),
    vec![3],
    "the reclaimed node's Alive should be queued for gossip"
  );
}

/// A `Suspect` node that receives an Alive at the same (old) incarnation stays
/// `Suspect`; a higher-incarnation Alive clears suspicion and returns it to
/// `Alive`, restamping the state-change. The resurrection emits no membership
/// event (the node was already known and its meta is unchanged) but enqueues a
/// gossip broadcast.
#[test]
fn alive_node_suspect_node() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  while e.poll_event().is_some() {}

  // Drive the node into Suspect via an incoming Suspect at its incarnation,
  // then age the suspicion well past any reset window.
  e.process_suspect(
    Suspect::new(
      1,
      test.id_ref().cheap_clone(),
      e.local_id_ref().cheap_clone(),
    ),
    t0,
  );
  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Suspect));
  e.age_member(test.id_ref(), Duration::from_secs(3600));
  while e.poll_event().is_some() {}

  // Old incarnation: no change, stays Suspect.
  e.process_alive(alive_of(&test, 1), false, Instant::now());
  assert_eq!(
    e.member_liveness(test.id_ref()),
    Some(State::Suspect),
    "update with old incarnation must not resurrect"
  );

  // Higher incarnation: clears suspicion and returns to Alive.
  let resurrect_at = Instant::now();
  e.process_alive(alive_of(&test, 2), false, resurrect_at);
  assert_eq!(
    e.member_liveness(test.id_ref()),
    Some(State::Alive),
    "higher incarnation must resurrect to Alive"
  );
  assert_eq!(
    e.node_state_change(test.id_ref()),
    Some(resurrect_at),
    "resurrection restamps the state-change"
  );

  assert!(
    e.poll_event().is_none(),
    "resurrecting a known node with unchanged meta emits no event"
  );
  // The resurrecting Alive is gossiped at the new incarnation (replacing the
  // earlier Suspect/Alive broadcast for the same node).
  assert_eq!(
    drained_alive_incarnations(&mut e, test.id_ref()),
    vec![2],
    "the resurrecting Alive should be queued for gossip"
  );
}

/// Collect the incarnations of every queued `Dead` broadcast targeting `id`.
fn drained_dead_incarnations(e: &mut Endpoint<SmolStr, SocketAddr>, id: &SmolStr) -> Vec<u32> {
  e.drain_broadcasts()
    .into_iter()
    .filter_map(|m| match m {
      Message::Dead(d) if d.node_ref() == id => Some(d.incarnation()),
      _ => None,
    })
    .collect()
}

/// Collect the incarnations of every queued `Suspect` broadcast targeting `id`.
fn drained_suspect_incarnations(e: &mut Endpoint<SmolStr, SocketAddr>, id: &SmolStr) -> Vec<u32> {
  e.drain_broadcasts()
    .into_iter()
    .filter_map(|m| match m {
      Message::Suspect(s) if s.node_ref() == id => Some(s.incarnation()),
      _ => None,
    })
    .collect()
}

// ─────────────────────────────── Dead-node family ───────────────────────────

/// Marking a known `Alive` peer `Dead` at its current incarnation transitions
/// it to `Dead`, restamps the state-change at "now", emits a single `NodeLeft`,
/// and enqueues a `Dead` gossip broadcast.
#[test]
fn dead_node() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  while e.poll_event().is_some() {}

  // Age the join past any reset window, then mark it Dead at a later instant.
  e.age_member(test.id_ref(), Duration::from_secs(3600));
  let dead_at = Instant::now();
  e.process_dead(dead_of(test.id_ref(), e.local_id_ref(), 1), dead_at);

  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Dead));
  assert_eq!(
    e.node_state_change(test.id_ref()),
    Some(dead_at),
    "the Dead transition restamps the state-change"
  );

  let ev = e.poll_event().expect("expected NodeLeft");
  match ev {
    Event::NodeLeft(n) => assert_eq!(n.id_ref(), test.id_ref()),
    other => panic!("expected NodeLeft, got {other:?}"),
  }
  assert!(e.poll_event().is_none(), "exactly one event expected");

  // The Dead is gossiped at the node's incarnation.
  assert_eq!(
    drained_dead_incarnations(&mut e, test.id_ref()),
    vec![1],
    "the dead node's Dead should be queued for gossip"
  );
}

/// A second `Dead` for an already-`Dead` peer is a no-op: even at a higher
/// incarnation it leaves the state, incarnation, and stamp untouched, emits no
/// event, and enqueues no broadcast.
#[test]
fn dead_node_double() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  e.age_member(test.id_ref(), Duration::from_secs(3600));
  e.process_dead(dead_of(test.id_ref(), e.local_id_ref(), 1), t0);
  while e.poll_event().is_some() {}
  let inc_before = e.node_incarnation(test.id_ref());
  let change_before = e.node_state_change(test.id_ref());

  // Drain so the queue-length delta isolates the second Dead's effect.
  e.drain_broadcasts();
  let queued_before = e.broadcast_queue_len();

  // A second Dead at a strictly higher incarnation must do nothing.
  e.process_dead(dead_of(test.id_ref(), e.local_id_ref(), 2), Instant::now());

  assert_eq!(
    e.member_liveness(test.id_ref()),
    Some(State::Dead),
    "a double Dead must leave the node Dead"
  );
  assert_eq!(
    e.node_incarnation(test.id_ref()),
    inc_before,
    "a double Dead must not advance the incarnation"
  );
  assert_eq!(
    e.node_state_change(test.id_ref()),
    change_before,
    "a double Dead must not restamp the state-change"
  );
  assert!(e.poll_event().is_none(), "a double Dead emits no event");
  assert_eq!(
    e.broadcast_queue_len(),
    queued_before,
    "a double Dead must not enqueue a broadcast"
  );
}

/// A `Dead` carrying an incarnation older than the tracked one is ignored: a
/// peer learned `Alive` at a high incarnation stays `Alive` when a stale `Dead`
/// at a lower incarnation arrives, with no event and no broadcast.
#[test]
fn dead_node_old_dead() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 10), false, t0);
  e.age_member(test.id_ref(), Duration::from_secs(3600));
  while e.poll_event().is_some() {}
  e.drain_broadcasts();
  let queued_before = e.broadcast_queue_len();

  // Dead at incarnation 1 < tracked 10 ⇒ ignored.
  e.process_dead(dead_of(test.id_ref(), e.local_id_ref(), 1), t0);

  assert_eq!(
    e.member_liveness(test.id_ref()),
    Some(State::Alive),
    "a stale Dead must not kill a higher-incarnation Alive"
  );
  assert!(
    e.poll_event().is_none(),
    "an ignored stale Dead emits no event"
  );
  assert_eq!(
    e.broadcast_queue_len(),
    queued_before,
    "an ignored stale Dead must not enqueue a broadcast"
  );
}

/// The `node == from` self-marked-departure sentinel resolves to `State::Left`
/// (not `State::Dead`), emits `NodeLeft`, and gossips a `Dead`. After the
/// reclaim window a fresh `Alive` at a new address re-adopts the id.
#[test]
fn dead_node_left() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(cfg().with_dead_node_reclaim_time(Duration::from_millis(10)));
  let id = SmolStr::new("test");
  let node1 = Node::new(id.cheap_clone(), "127.0.0.1:8000".parse().unwrap());

  let t0 = Instant::now();
  e.process_alive(alive_of(&node1, 1), false, t0);
  while e.poll_event().is_some() {}

  // Self-marked departure: the node announces its own leave (node == from).
  e.process_dead(dead_of(&id, &id, 1), t0);
  assert_eq!(
    e.member_liveness(&id),
    Some(State::Left),
    "a self-marked Dead resolves to Left, not Dead"
  );

  let ev = e.poll_event().expect("expected NodeLeft");
  match ev {
    Event::NodeLeft(n) => assert_eq!(n.id_ref(), &id),
    other => panic!("expected NodeLeft, got {other:?}"),
  }
  // A Left transition still gossips the Dead.
  assert_eq!(
    drained_dead_incarnations(&mut e, &id),
    vec![1],
    "the left node's Dead should be queued for gossip"
  );

  // After the reclaim window, a fresh Alive at a new address re-adopts the id.
  let node2 = Node::new(id.cheap_clone(), "127.0.0.2:9000".parse().unwrap());
  let foo = Meta::try_from(Bytes::from_static(b"foo")).unwrap();
  let later = t0 + Duration::from_millis(11);
  e.process_alive(
    alive_of(&node2, 3).with_meta(foo.cheap_clone()),
    false,
    later,
  );

  assert_eq!(
    e.member_liveness(&id),
    Some(State::Alive),
    "should be alive again after reclaim"
  );
  {
    let n = e.member(&id).unwrap();
    assert_eq!(n.meta_ref().as_bytes(), b"foo", "meta should be updated");
    assert_eq!(n.address_ref(), node2.addr_ref(), "addr should be updated");
  }
  let ev = e.poll_event().expect("expected NodeJoined");
  match ev {
    Event::NodeJoined(n) => assert_eq!(n.id_ref(), &id),
    other => panic!("expected NodeJoined, got {other:?}"),
  }
}

/// A `Dead` accusing the local node while `Running` is refuted: we stay `Alive`,
/// broadcast a fresh self-`Alive` past the accusation, and ding our health.
#[test]
fn dead_node_refute() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  while e.poll_event().is_some() {}

  let local = Node::new(e.local_id_ref().cheap_clone(), *e.advertise_ref());
  e.process_alive(alive_of(&local, 1), true, Instant::now());
  while e.poll_event().is_some() {}

  // Health starts fully healthy.
  assert_eq!(e.health_score(), 0, "health should start at 0");

  // A self-Dead at our current incarnation reaches the refute path.
  let local_id = e.local_id_ref().cheap_clone();
  let accused = e.node_incarnation(&local_id).unwrap();
  e.process_dead(dead_of(&local_id, &local_id, accused), Instant::now());

  assert_eq!(
    e.member_liveness(&local_id),
    Some(State::Alive),
    "self-Dead while Running must refute and stay Alive"
  );

  // Refute broadcasts a single self-Alive past the accused incarnation (the
  // bump replaces the construction-seeded self-Alive).
  let incs = drained_alive_incarnations(&mut e, &local_id);
  assert_eq!(incs.len(), 1, "exactly one self-Alive should be queued");
  assert!(
    incs[0] > accused,
    "refute should broadcast an Alive past the accused incarnation, got {} <= {accused}",
    incs[0]
  );

  // Refute dings the local health score.
  assert_eq!(e.health_score(), 1, "refute should ding health");
}

/// Replaying the original `Alive` at the same incarnation after a peer was
/// marked `Dead` does not resurrect it: `process_alive` requires a strictly
/// higher incarnation to revive a `Dead` node, so it stays `Dead`.
#[test]
fn dead_node_alive_replay() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 10), false, t0);
  e.process_dead(dead_of(test.id_ref(), e.local_id_ref(), 10), t0);
  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Dead));
  while e.poll_event().is_some() {}

  // Replay the same Alive at the same incarnation ⇒ stays Dead.
  e.process_alive(alive_of(&test, 10), false, Instant::now());
  assert_eq!(
    e.member_liveness(test.id_ref()),
    Some(State::Dead),
    "an Alive replay at the same incarnation must not resurrect a Dead node"
  );
}

// ─────────────────────────────── Suspect-node family ────────────────────────

/// A known `Alive` peer suspected at its current incarnation transitions to
/// `Suspect`, restamps the state-change, and gossips a `Suspect`. When the
/// suspicion timer fires it transitions to `Dead`, restamps again, and gossips
/// a `Dead`.
#[test]
fn suspect_node() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
    cfg()
      .with_probe_interval(Duration::from_millis(1))
      .with_suspicion_mult(1)
      .with_suspicion_max_timeout_mult(1),
  );
  let test = node("test", 8000);

  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  e.age_member(test.id_ref(), Duration::from_secs(3600));
  while e.poll_event().is_some() {}

  e.process_suspect(suspect_of(test.id_ref(), e.local_id_ref(), 1), t0);

  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Suspect));
  let suspect_change = e
    .node_state_change(test.id_ref())
    .expect("state-change recorded");
  assert_eq!(suspect_change, t0, "the Suspect transition stamps at now");

  // The Suspect is gossiped at the node's incarnation.
  assert_eq!(
    drained_suspect_incarnations(&mut e, test.id_ref()),
    vec![1],
    "the suspected node's Suspect should be queued for gossip"
  );

  // Fire the suspicion timer: the node transitions to Dead.
  let deadline = e.poll_timeout().expect("suspicion deadline expected");
  let fired_at = deadline + Duration::from_millis(1);
  e.handle_timeout(fired_at);

  assert_eq!(
    e.member_liveness(test.id_ref()),
    Some(State::Dead),
    "an expired suspicion must mark the node Dead"
  );
  let dead_change = e
    .node_state_change(test.id_ref())
    .expect("state-change recorded");
  assert!(
    dead_change > suspect_change,
    "the Dead transition must advance the state-change past the Suspect stamp"
  );

  // The timeout-driven Dead is gossiped at the node's incarnation.
  assert_eq!(
    drained_dead_incarnations(&mut e, test.id_ref()),
    vec![1],
    "the timed-out node's Dead should be queued for gossip"
  );
}

/// A `Suspect` from the SAME accuser repeated against an already-`Suspect` peer
/// is a no-op: it does not restamp the state-change and enqueues no new
/// broadcast (the existing suspicion timer is merely re-confirmed, with no new
/// confirming source).
#[test]
fn suspect_node_double_suspect() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let test = node("test", 8000);

  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  e.age_member(test.id_ref(), Duration::from_secs(3600));
  while e.poll_event().is_some() {}

  let from = e.local_id_ref().cheap_clone();
  e.process_suspect(suspect_of(test.id_ref(), &from, 1), t0);
  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Suspect));
  let change = e
    .node_state_change(test.id_ref())
    .expect("state-change recorded");

  // Drain so the queue-length delta isolates the second Suspect's effect.
  e.drain_broadcasts();
  let queued_before = e.broadcast_queue_len();

  // Suspect again from the same source ⇒ no new confirmation, no restamp.
  e.process_suspect(suspect_of(test.id_ref(), &from, 1), t0);

  assert_eq!(
    e.node_state_change(test.id_ref()),
    Some(change),
    "a same-source repeat Suspect must not restamp the state-change"
  );
  assert_eq!(
    e.broadcast_queue_len(),
    queued_before,
    "a same-source repeat Suspect must not enqueue a broadcast"
  );
}

/// A `Suspect` accusing the local node is refuted: we stay `Alive`, broadcast a
/// fresh self-`Alive`, and ding our health.
#[test]
fn suspect_node_refute() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  while e.poll_event().is_some() {}

  let local = Node::new(e.local_id_ref().cheap_clone(), *e.advertise_ref());
  e.process_alive(alive_of(&local, 1), true, Instant::now());
  while e.poll_event().is_some() {}

  // Health starts fully healthy.
  assert_eq!(e.health_score(), 0, "health should start at 0");

  let local_id = e.local_id_ref().cheap_clone();
  let accused = e.node_incarnation(&local_id).unwrap();
  e.process_suspect(suspect_of(&local_id, &local_id, accused), Instant::now());

  assert_eq!(
    e.member_liveness(&local_id),
    Some(State::Alive),
    "a self-Suspect must refute and stay Alive"
  );

  // Refute broadcasts a single self-Alive past the accusation.
  let incs = drained_alive_incarnations(&mut e, &local_id);
  assert_eq!(incs.len(), 1, "exactly one self-Alive should be queued");
  assert!(
    incs[0] > accused,
    "refute should broadcast an Alive past the accused incarnation, got {} <= {accused}",
    incs[0]
  );

  // Refute dings the local health score.
  assert_eq!(e.health_score(), 1, "refute should ding health");
}
