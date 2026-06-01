//! Gossip propagation scenario helpers.

use std::net::SocketAddr;

use memberlist_proto::typed::{Alive, Node};
use smol_str::SmolStr;

use crate::Cluster;

/// Bootstrap n1 with knowledge of n2 and n3, run gossip for `steps` ticks.
/// Returns the cluster for assertions.
///
/// Corresponds to legacy `gossip` in
/// `memberlist-core/src/state/tests.rs:2167`.
pub fn three_node_gossip(a1: SocketAddr, a2: SocketAddr, a3: SocketAddr, steps: usize) -> Cluster {
  let mut c = Cluster::new();
  c.add_node(SmolStr::new("n1"), a1);
  c.add_node(SmolStr::new("n2"), a2);
  c.add_node(SmolStr::new("n3"), a3);

  let now = c.clock.now();
  {
    let ep = c.net.endpoints.get_mut(&a1).unwrap();
    for (id, addr) in [("n2", a2), ("n3", a3)] {
      let a = Alive::new(1, Node::new(SmolStr::new(id), addr));
      ep.handle_alive(a1, a, now);
    }
  }

  for _ in 0..steps {
    c.step();
  }

  c
}
