//! Push/pull scenario helpers.

use std::net::SocketAddr;

use memberlist_machine::PushPullKind;
use memberlist_wire::typed::{Alive, Node};
use smol_str::SmolStr;

use crate::Cluster;

/// Set up a two-node cluster and trigger a push/pull from `initiator` to
/// `acceptor`. Returns the cluster after convergence.
///
/// Corresponds to legacy `push_pull` in
/// `memberlist-core/src/state/tests.rs:2301`.
pub fn two_node_push_pull(initiator: SocketAddr, acceptor: SocketAddr) -> Cluster {
  let mut c = Cluster::new();
  c.add_node(SmolStr::new("initiator"), initiator);
  c.add_node(SmolStr::new("acceptor"), acceptor);

  let now = c.clock.now();
  // Seed initiator's knowledge of acceptor.
  {
    let ep = c.net.endpoints.get_mut(&initiator).unwrap();
    let a = Alive::new(1, Node::new(SmolStr::new("acceptor"), acceptor));
    ep.handle_alive(initiator, a, now);
    ep.start_push_pull(acceptor, PushPullKind::Refresh, now);
  }

  for _ in 0..80 {
    c.step();
  }

  c
}
