//! Scenario: deterministic gossip propagation in a datagram-only simulation.
//!
//! Two tests:
//! - `two_node_alive_propagation`: node1 learns about node2 via gossip
//!   broadcast; converges so both nodes see each other as Alive.
//! - `three_node_transitive_gossip`: node1 knows all three; gossip propagates
//!   transitively so every node eventually knows every other node.
//!
//! These correspond to the legacy `probe` and `gossip` scenarios in
//! `memberlist-core/src/state/tests.rs`.

use memberlist_simulation::Cluster;
use smol_str::SmolStr;
use std::net::SocketAddr;

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

/// After creating two nodes and running the simulation until quiescent,
/// both endpoints should know about each other as Alive via gossip.
///
/// Corresponds to legacy `probe` in `memberlist-core/src/state/tests.rs:57`.
#[test]
fn two_node_alive_propagation() {
  let mut c = Cluster::new();
  let a1 = addr(17001);
  let a2 = addr(17002);
  c.add_node(SmolStr::new("node1"), a1);
  c.add_node(SmolStr::new("node2"), a2);

  // Seed knowledge: tell node1 about node2's Alive state so the probe
  // scheduler has a peer to probe and gossip about.
  c.inject_alive(a1, SmolStr::new("node2"), a2, 1);

  // Run simulation long enough for gossip to propagate (gossip_interval=200ms).
  for _ in 0..500 {
    c.step();
  }

  // node1 should see node2 (directly injected).
  assert!(
    c.member(a1, &SmolStr::new("node2")).is_some(),
    "node1 should know node2 (directly injected)"
  );

  // node2 should now know about node1 via gossip.
  assert!(
    c.member(a2, &SmolStr::new("node1")).is_some(),
    "node2 should have learned about node1 via gossip"
  );
}

/// Three nodes: node1 gossips its knowledge (node2, node3) to node2, which
/// then gossips to node3. All three should eventually know all three peers.
///
/// Corresponds to legacy `gossip` in `memberlist-core/src/state/tests.rs:2167`.
#[test]
fn three_node_transitive_gossip() {
  let mut c = Cluster::new();
  let a1 = addr(17101);
  let a2 = addr(17102);
  let a3 = addr(17103);
  c.add_node(SmolStr::new("n1"), a1);
  c.add_node(SmolStr::new("n2"), a2);
  c.add_node(SmolStr::new("n3"), a3);

  // Bootstrap node1 with knowledge of n2 and n3.
  c.inject_alive(a1, SmolStr::new("n2"), a2, 1);
  c.inject_alive(a1, SmolStr::new("n3"), a3, 1);

  for _ in 0..1000 {
    c.step();
  }

  // All three nodes should eventually see each other.
  let peers: &[(&str, SocketAddr)] = &[("n1", a1), ("n2", a2), ("n3", a3)];
  for &(host_id, host_addr) in peers {
    for &(peer_id, _) in peers {
      if host_id == peer_id {
        continue;
      }
      assert!(
        c.member(host_addr, &SmolStr::new(peer_id)).is_some(),
        "{host_id} should know about {peer_id}"
      );
    }
  }
}
