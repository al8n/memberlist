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

// ─────────────────────────────── Probe family ───────────────────────────────
//
// These mirror the failure-detection probe scenarios from the behavioral
// oracle. The Sans-I/O `Endpoint` runs the probe FSM synchronously:
//
//   * `start_probe(now)` round-robins to the next eligible peer and enqueues a
//     `Ping` (plus a buddy `Suspect` when the target is already Suspect). The
//     concrete target is the FSM's choice — peers are inserted at random
//     positions — so each test reads the chosen target/seq back out of the
//     emitted [`Transmit`] rather than assuming an identity, exactly as the
//     mechanical probe tests in `endpoint::tests` do.
//   * a direct `handle_ack` from the target completes the probe (peer stays
//     Alive, health improves by one).
//   * with no direct ack, `handle_timeout(direct_deadline + ε)` escalates the
//     probe to `AwaitingIndirect`: it fans `IndirectPing`s out to up to
//     `indirect_checks` Alive peers AND opens the reliable-ping fallback
//     concurrently (one `DialRequested`), both bounded by the single
//     `sent + scale_timeout(probe_interval)` cumulative deadline.
//   * `handle_nack(peer, …)` records that an indirect peer reached the target
//     but got no ack; `handle_timeout(cumulative_deadline + ε)` then terminates
//     the probe as failure, marking the target Suspect and applying the
//     Lifeguard awareness delta (`expected_nacks - nacked` missing nacks, or a
//     flat `+1` when no indirect peers existed).
//
// There is no driver, so the concurrent reliable-ping fallback emits its
// `DialRequested` but never receives a response — it simply expires at the
// cumulative deadline, just as the oracle's bad/never-started node never
// answers the TCP fallback either.

/// Start a probe and read the chosen target back out of the emitted Ping.
///
/// `start_probe` may emit the Ping as a lone `Packet` or, when the target is
/// already Suspect, as a `Compound([Ping, Suspect])` (the buddy piggyback);
/// both forms are handled. Returns `(seq, target_id, target_addr)`. Any
/// remaining queued transmits are left in place for the caller to inspect.
fn start_probe_capture(
  e: &mut Endpoint<SmolStr, SocketAddr>,
  now: Instant,
) -> (u32, SmolStr, SocketAddr) {
  assert!(e.start_probe(now), "expected an eligible probe target");
  let tx = e.poll_transmit().expect("probe must emit a Ping");
  let ping = match tx {
    Transmit::Packet(p) => match p.into_parts() {
      (_, Message::Ping(ping)) => ping,
      (_, other) => panic!("expected a lone Ping packet, got {other:?}"),
    },
    Transmit::Compound(c) => {
      let (_, msgs) = c.into_parts();
      match msgs.into_iter().next() {
        Some(Message::Ping(ping)) => ping,
        other => panic!("expected Ping as the first compound part, got {other:?}"),
      }
    }
  };
  (
    ping.sequence_number(),
    ping.target_ref().id_ref().cheap_clone(),
    *ping.target_ref().addr_ref(),
  )
}

/// Escalate a stalled direct probe to the indirect phase and return the
/// addresses of every peer that received an `IndirectPing`.
///
/// Fires `handle_timeout` once past the direct sub-deadline, then drains the
/// queues: `DialRequested` events (the concurrent reliable-ping fallback,
/// which has no driver to answer it) are discarded, and each emitted
/// `IndirectPing` destination is collected. The returned set is exactly the
/// FSM's Nack allowlist for this probe.
fn escalate_to_indirect(e: &mut Endpoint<SmolStr, SocketAddr>, at: Instant) -> Vec<SocketAddr> {
  e.handle_timeout(at);
  // Drain the concurrent reliable-fallback DialRequested (no driver answers).
  while e.poll_event().is_some() {}
  let mut indirect_peers = Vec::new();
  while let Some(tx) = e.poll_transmit() {
    if let Transmit::Packet(p) = tx {
      if let Message::IndirectPing(_) = p.message_ref() {
        indirect_peers.push(*p.to_ref());
      }
    }
  }
  indirect_peers
}

/// `probe` / `probe_node`: probing a peer that answers the direct Ping keeps it
/// Alive and produces no suspicion. The oracle's `probe`/`probe_node` differ
/// only in entrypoint (round-robin `probe()` vs direct `probe_node()`); the
/// Sans-I/O machine has a single `start_probe`, so both collapse to this one
/// faithful scenario. A successful probe also improves health (oracle:
/// `awareness_delta = -1`), which the dedicated `probe_node_awareness_improved`
/// test below pins explicitly.
#[test]
fn probe_node_responsive_peer_stays_alive() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
    cfg()
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(1000)),
  );
  let test = node("test", 8000);
  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Only `test` is an eligible peer, so the probe targets it.
  let (seq, target_id, target_addr) = start_probe_capture(&mut e, t0);
  assert_eq!(target_id, *test.id_ref(), "the lone peer is probed");

  // The target answers the direct Ping within the window.
  e.handle_ack(target_addr, Ack::new(seq), t0 + Duration::from_millis(10));

  // Should NOT be marked suspect; the probe is resolved.
  assert_eq!(
    e.member_liveness(test.id_ref()),
    Some(State::Alive),
    "a peer that acks the direct ping stays Alive"
  );
  assert!(
    !e.probes.contains_key(&seq),
    "the probe is resolved on the direct ack"
  );
}

/// `probe_node_suspect`: a peer that never answers a direct probe escalates to
/// indirect checks (k Alive peers receive an `IndirectPing`) and is marked
/// `Suspect` once the cumulative deadline elapses with no ack. The oracle
/// asserts both the resulting Suspect state and that the indirect peers were
/// actually pinged (their seqno advanced).
#[test]
fn probe_node_suspect() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
    cfg()
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10))
      .with_indirect_checks(3),
  );
  // local + three reachable peers + one unreachable target. The probe FSM
  // picks ONE peer to probe directly; whichever it picks, none answer here,
  // so it escalates and the OTHER Alive peers serve as indirect checkers.
  let p1 = node("p1", 8001);
  let p2 = node("p2", 8002);
  let p3 = node("p3", 8003);
  let target = node("target", 8004);
  let t0 = Instant::now();
  for n in [&p1, &p2, &p3, &target] {
    e.process_alive(alive_of(n, 1), false, t0);
  }
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let (seq, target_id, _target_addr) = start_probe_capture(&mut e, t0);

  // No direct ack. Past the direct sub-deadline (probe_timeout = 1ms) the
  // probe fans out IndirectPings to the OTHER Alive peers.
  let direct_deadline = t0 + Duration::from_millis(1);
  let indirect_peers = escalate_to_indirect(&mut e, direct_deadline + Duration::from_millis(1));

  // At least one peer was asked to probe indirectly (oracle: peers' seqnos
  // advanced). With three other Alive peers and k=3, all three are chosen.
  assert!(
    !indirect_peers.is_empty(),
    "a failed direct probe must fan out at least one IndirectPing"
  );

  // No indirect ack/nack arrives. The single cumulative deadline
  // (sent + scale_timeout(probe_interval) = t0 + 10ms, health 0) elapses ⇒
  // the target is suspected.
  let cumulative_deadline = t0 + Duration::from_millis(10);
  e.handle_timeout(cumulative_deadline + Duration::from_millis(1));

  assert_eq!(
    e.member_liveness(&target_id),
    Some(State::Suspect),
    "a peer that answers neither direct nor indirect probes is suspected"
  );
  assert!(!e.probes.contains_key(&seq), "the failed probe is retired");
  // The suspicion is gossiped at the target's incarnation.
  assert_eq!(
    drained_suspect_incarnations(&mut e, &target_id),
    vec![1],
    "the suspected target's Suspect is queued for gossip"
  );
}

/// `probe_node_buddy`: when the probe target is already `Suspect`, the outgoing
/// Ping carries a buddy `Suspect` (piggybacked) so the target can refute on
/// receipt. The oracle forces the target Suspect, probes, and checks a ping was
/// sent and the Suspect accompanies it. Here we assert the machine co-emits the
/// Ping and the buddy `Suspect` for the target (as one compound datagram when
/// it fits the MTU, which it does for these short ids).
#[test]
fn probe_node_buddy() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
    cfg()
      .with_probe_timeout(Duration::from_millis(1))
      .with_probe_interval(Duration::from_millis(10)),
  );
  let test = node("test", 8000);
  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  // Drive the target into Suspect so the probe piggybacks a Suspect with the
  // Ping (the oracle force-sets the state to Suspect for the same reason).
  e.process_suspect(suspect_of(test.id_ref(), e.local_id_ref(), 1), t0);
  assert_eq!(e.member_liveness(test.id_ref()), Some(State::Suspect));
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The lone peer is Suspect, so the probe emits Ping + buddy Suspect.
  assert!(e.start_probe(t0), "the Suspect peer is still probed");
  let tx = e.poll_transmit().expect("probe must emit traffic");
  let (ping_seq, suspect_target) = match tx {
    // Short ids ⇒ the Ping+Suspect compound fits the MTU and is co-sent.
    Transmit::Compound(c) => {
      let (to, msgs) = c.into_parts();
      assert_eq!(to, *test.addr_ref(), "probe is addressed to the target");
      assert_eq!(msgs.len(), 2, "buddy piggyback is Ping + Suspect");
      let ping_seq = match &msgs[0] {
        Message::Ping(p) => {
          assert_eq!(p.target_ref().id_ref(), test.id_ref());
          p.sequence_number()
        }
        other => panic!("expected Ping first, got {other:?}"),
      };
      let suspect_target = match &msgs[1] {
        Message::Suspect(s) => s.node_ref().cheap_clone(),
        other => panic!("expected buddy Suspect second, got {other:?}"),
      };
      (ping_seq, suspect_target)
    }
    other => panic!("expected a Ping+Suspect compound, got {other:?}"),
  };
  assert_eq!(
    suspect_target,
    *test.id_ref(),
    "the buddy Suspect accuses the probed target so it can refute"
  );

  // A ping was sent (oracle: seqno advanced to a fresh probe sequence).
  assert_ne!(ping_seq, 0, "the probe allocated a ping sequence number");
  assert!(
    e.probes.contains_key(&ping_seq),
    "the buddy probe is in flight under its ping seq"
  );
}

struct DogpileCase {
  name: &'static str,
  /// Total cluster size N (local + peers + the suspected node) the suspicion
  /// timeout is computed against.
  num_nodes: usize,
  confirmations: usize,
  expected: Duration,
}

/// `probe_node_dogpile`: the suspicion-timer table. A suspected node's timeout
/// starts at `max` and is pulled toward `min` as independent confirmations
/// arrive, per the Lifeguard formula. With `suspicion_mult = 5`,
/// `suspicion_max_timeout_mult = 2`, `probe_interval = 100ms`:
///   `min = 100ms * 5 * max(log10(N), 1)` and `max = 2 * min`,
///   `k = max(0, 5 - 2) = 3` once `N >= 5` (else `k = 0`, fixed at `min`).
///
/// The oracle reaches Suspect via a failed probe of a bogus node; the Sans-I/O
/// machine produces the identical `Suspect(inc, target, local)` and Suspicion
/// when that probe fails, so we install it directly via `process_suspect` and
/// then exercise the same confirmation/timeout dynamics this scenario targets.
/// Confirmations come from distinct peers (the original suspector is excluded).
#[test]
fn probe_node_dogpile() {
  const CASES: &[DogpileCase] = &[
    DogpileCase {
      name: "n=2, k=3 (max timeout disabled)",
      num_nodes: 2,
      confirmations: 0,
      expected: Duration::from_millis(500),
    },
    DogpileCase {
      name: "n=3, k=3",
      num_nodes: 3,
      confirmations: 0,
      expected: Duration::from_millis(500),
    },
    DogpileCase {
      name: "n=4, k=3",
      num_nodes: 4,
      confirmations: 0,
      expected: Duration::from_millis(500),
    },
    DogpileCase {
      name: "n=5, k=3 (max timeout starts to take effect)",
      num_nodes: 5,
      confirmations: 0,
      expected: Duration::from_millis(1000),
    },
    DogpileCase {
      name: "n=6, k=3 (zero confirmations)",
      num_nodes: 6,
      confirmations: 0,
      expected: Duration::from_millis(1000),
    },
    DogpileCase {
      name: "n=6, k=3 (confirmations start to lower timeout)",
      num_nodes: 6,
      confirmations: 1,
      expected: Duration::from_millis(750),
    },
    DogpileCase {
      name: "n=6, k=3 (two confirmations)",
      num_nodes: 6,
      confirmations: 2,
      expected: Duration::from_millis(604),
    },
    DogpileCase {
      name: "n=6, k=3 (timeout driven to nominal value)",
      num_nodes: 6,
      confirmations: 3,
      expected: Duration::from_millis(500),
    },
    DogpileCase {
      name: "n=6, k=3 (extra confirmation past k is ignored)",
      num_nodes: 6,
      confirmations: 4,
      expected: Duration::from_millis(500),
    },
  ];

  for c in CASES {
    let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
      cfg()
        .with_probe_timeout(Duration::from_millis(1))
        .with_probe_interval(Duration::from_millis(100))
        .with_suspicion_mult(5)
        .with_suspicion_max_timeout_mult(2),
    );

    // Build the cluster to exactly N members: local is already present, the
    // last is the bad node we suspect, the rest are confirmation sources.
    let t0 = Instant::now();
    let bad = node("bad", 9000);
    let num_peers = c.num_nodes - 2; // exclude local and the bad node
    let mut peers = Vec::new();
    for j in 0..num_peers {
      let peer = node(&format!("peer{j}"), 9100 + j as u16);
      e.process_alive(alive_of(&peer, 1), false, t0);
      peers.push(peer);
    }
    e.process_alive(alive_of(&bad, 1), false, t0);
    assert_eq!(
      e.num_members(),
      c.num_nodes,
      "case {}: cluster size",
      c.name
    );
    while e.poll_event().is_some() {}

    // Suspect the bad node — the exact Suspect a failed self-probe emits.
    e.process_suspect(suspect_of(bad.id_ref(), e.local_id_ref(), 1), t0);
    assert_eq!(
      e.member_liveness(bad.id_ref()),
      Some(State::Suspect),
      "case {}: node should be suspect",
      c.name
    );

    // Apply the requested confirmations from distinct peers.
    for peer in peers.iter().take(c.confirmations) {
      e.process_suspect(suspect_of(bad.id_ref(), peer.id_ref(), 1), t0);
    }

    // Just before the expected timeout the node must still be Suspect.
    let fudge = Duration::from_millis(25);
    e.handle_timeout(t0 + c.expected - fudge);
    assert_eq!(
      e.member_liveness(bad.id_ref()),
      Some(State::Suspect),
      "case {}: node should still be suspect just before the timeout",
      c.name
    );

    // Through the timeout the suspicion fires and the node becomes Dead.
    e.handle_timeout(t0 + c.expected + Duration::from_millis(1));
    assert_eq!(
      e.member_liveness(bad.id_ref()),
      Some(State::Dead),
      "case {}: node should be dead after the timeout",
      c.name
    );
  }
}

/// `probe_node_awareness_degraded`: starting in a degraded health state
/// (score 1), a failed probe whose indirect checkers all NACK (they reached the
/// target, so our own network is fine) leaves the score unchanged. The
/// Lifeguard delta is `expected_nacks - nacked = 0` because every indirect peer
/// nacked, so the failed probe applies no additional penalty — but the prior
/// failure detail (no successful probe) means it does not improve either.
#[test]
fn probe_node_awareness_degraded() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
    cfg()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200))
      .with_indirect_checks(3),
  );
  let p1 = node("p1", 8001);
  let p2 = node("p2", 8002);
  let target = node("target", 8004);
  let t0 = Instant::now();
  for n in [&p1, &p2, &target] {
    e.process_alive(alive_of(n, 1), false, t0);
  }
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Start the health in a degraded state (oracle: awareness.apply_delta(1)).
  e.degrade_health(1);
  assert_eq!(e.health_score(), 1, "health starts degraded");

  let (seq, target_id, _addr) = start_probe_capture(&mut e, t0);

  // No direct ack. The scaled probe_interval is the cumulative deadline:
  // health 1 ⇒ scale_timeout(200ms) = 400ms.
  let direct_deadline = t0 + Duration::from_millis(10);
  let indirect_peers = escalate_to_indirect(&mut e, direct_deadline + Duration::from_millis(1));
  assert!(
    !indirect_peers.is_empty(),
    "the failed probe fans out indirect checks"
  );

  // EVERY indirect checker nacks (they reached the target; our network is
  // healthy). Feed the nacks before the cumulative deadline.
  let nack_at = t0 + Duration::from_millis(20);
  for peer in &indirect_peers {
    e.handle_nack(*peer, Nack::new(seq), nack_at);
  }

  // The cumulative deadline (sent + scale_timeout(200ms) = t0 + 400ms)
  // elapses ⇒ the target is suspected.
  let cumulative_deadline = t0 + Duration::from_millis(400);
  e.handle_timeout(cumulative_deadline + Duration::from_millis(1));

  assert_eq!(
    e.member_liveness(&target_id),
    Some(State::Suspect),
    "the unreachable target is suspected"
  );

  // We got all the nacks, so the score is unchanged — no successful probe,
  // but no missing-nack penalty either (delta = expected - nacked = 0).
  assert_eq!(
    e.health_score(),
    1,
    "all indirect nacks received ⇒ no additional health penalty"
  );
}

/// `probe_node_awareness_improved`: starting degraded (score 1), a successful
/// direct probe improves health by one (oracle: `awareness_delta = -1` on a
/// good probe), and the peer stays Alive.
#[test]
fn probe_node_awareness_improved() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
    cfg()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200)),
  );
  let test = node("test", 8000);
  let t0 = Instant::now();
  e.process_alive(alive_of(&test, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Start the health in a degraded state.
  e.degrade_health(1);
  assert_eq!(e.health_score(), 1, "health starts degraded");

  let (seq, target_id, target_addr) = start_probe_capture(&mut e, t0);

  // A good direct probe response.
  e.handle_ack(target_addr, Ack::new(seq), t0 + Duration::from_millis(5));

  assert_eq!(
    e.member_liveness(&target_id),
    Some(State::Alive),
    "the responsive peer stays Alive"
  );
  // The good probe improved our score.
  assert_eq!(
    e.health_score(),
    0,
    "a successful probe improves health by one"
  );
}

/// `probe_node_awareness_missed_nack`: starting healthy (score 0), a failed
/// probe whose indirect checkers never respond (no nacks) decrements health —
/// missing nacks are read as our own degradation. The Lifeguard delta is
/// `expected_nacks - 0 > 0`, so the score rises to 1.
#[test]
fn probe_node_awareness_missed_nack() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
    cfg()
      .with_probe_timeout(Duration::from_millis(10))
      .with_probe_interval(Duration::from_millis(200))
      .with_indirect_checks(3),
  );
  // local + one responsive peer (a valid indirect checker that stays silent)
  // + an unreachable target. The oracle's node3/node4 "never get started".
  let p1 = node("p1", 8001);
  let target = node("target", 8004);
  let t0 = Instant::now();
  for n in [&p1, &target] {
    e.process_alive(alive_of(n, 1), false, t0);
  }
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Health looks good.
  assert_eq!(e.health_score(), 0, "health starts healthy");

  let (seq, target_id, _addr) = start_probe_capture(&mut e, t0);

  // No direct ack ⇒ escalate to indirect. At least one peer is asked.
  let direct_deadline = t0 + Duration::from_millis(10);
  let indirect_peers = escalate_to_indirect(&mut e, direct_deadline + Duration::from_millis(1));
  assert!(
    !indirect_peers.is_empty(),
    "the failed probe fans out at least one indirect check"
  );

  // NO indirect peer nacks (they never respond). The cumulative deadline
  // (health 0 ⇒ scale_timeout(200ms) = 200ms) elapses ⇒ failure.
  let cumulative_deadline = t0 + Duration::from_millis(200);
  e.handle_timeout(cumulative_deadline + Duration::from_millis(1));

  assert_eq!(
    e.member_liveness(&target_id),
    Some(State::Suspect),
    "the unreachable target is suspected"
  );
  assert!(!e.probes.contains_key(&seq), "the failed probe is retired");

  // Missing every expected nack dings our health (delta = expected - 0 > 0).
  assert_eq!(
    e.health_score(),
    1,
    "missing all indirect nacks decrements health to 1"
  );
}
