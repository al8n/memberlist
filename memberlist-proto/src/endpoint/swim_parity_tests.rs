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
