//! Scenario: two-node push/pull convergence.
//!
//! node1 knows about itself and node2. node2 only knows itself. After
//! a push/pull exchange initiated by node1, node2 learns about node1
//! (and vice versa).
//!
//! Corresponds to legacy `push_pull` in `memberlist-core/src/state/tests.rs:2301`.

use memberlist_simulation::Cluster;
use smol_str::SmolStr;
use std::net::SocketAddr;

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

/// After node1 triggers a push/pull to node2, node2 should learn about node1.
#[test]
fn two_node_push_pull_convergence() {
  let mut c = Cluster::new();
  let a1 = addr(18001);
  let a2 = addr(18002);
  c.add_node(SmolStr::new("n1"), a1);
  c.add_node(SmolStr::new("n2"), a2);

  // Bootstrap: tell node1 about node2 via out-of-band inject_alive.
  // node2 does not yet know node1.
  c.inject_alive(a1, SmolStr::new("n2"), a2, 1);

  // Trigger a push/pull from node1 → node2 deterministically, bypassing the
  // 30-second periodic scheduler.
  c.trigger_push_pull(a1, a2);

  // Step the simulation enough to complete the stream exchange.
  for _ in 0..100 {
    c.step();
  }

  // After push/pull, node2 should know node1.
  assert!(
    c.member(a2, &SmolStr::new("n1")).is_some(),
    "node2 should have learned n1 via push/pull"
  );

  // node1 should still know node2.
  assert!(
    c.member(a1, &SmolStr::new("n2")).is_some(),
    "node1 should still know n2"
  );
}
