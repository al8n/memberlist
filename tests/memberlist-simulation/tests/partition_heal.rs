//! Scenario: network partition + heal → membership reconciles via push/pull.
//!
//! Three nodes: n1 and n2 form group A; n3 is isolated in group B.
//! During the partition, n3's gossip cannot reach n1/n2. After healing,
//! a deterministic push/pull from n1 to n3 brings n3 up to date with the
//! full membership.
//!
//! Exercises `Cluster::partition`, `Cluster::heal`, and
//! `Cluster::trigger_push_pull`.

use memberlist_simulation::Cluster;
use smol_str::SmolStr;
use std::time::Duration;

fn addr(port: u16) -> std::net::SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

/// After partitioning {n1, n2} from {n3}, healing, and triggering a
/// push/pull from n1 to n3, n3 should learn about n1 and n2.
#[test]
fn partition_then_heal_reconciles_membership() {
  let mut c = Cluster::new();
  let a1 = addr(19201);
  let a2 = addr(19202);
  let a3 = addr(19203);
  c.add_node(SmolStr::new("n1"), a1);
  c.add_node(SmolStr::new("n2"), a2);
  c.add_node(SmolStr::new("n3"), a3);

  // Bootstrap n1 with knowledge of n2 and n3.
  c.inject_alive(a1, SmolStr::new("n2"), a2, 1);
  c.inject_alive(a1, SmolStr::new("n3"), a3, 1);

  // Let gossip converge briefly before partition.
  for _ in 0..200 {
    c.step();
  }

  // Partition: n3 isolated from n1 and n2.
  c.partition(&[a1, a2], &[a3]);

  // Advance 2 s while partitioned.
  c.advance(Duration::from_secs(2));
  for _ in 0..100 {
    c.step();
  }

  // Heal the partition.
  c.heal();

  // Trigger a push/pull from n1 to n3 to accelerate reconciliation.
  c.trigger_push_pull(a1, a3);

  // Step enough for the stream exchange to complete.
  for _ in 0..100 {
    c.step();
  }

  // After healing + push/pull, n3 should know n1 and n2.
  for peer in ["n1", "n2"] {
    assert!(
      c.member(a3, &SmolStr::new(peer)).is_some(),
      "n3 should know {peer} after partition heal"
    );
  }
}
