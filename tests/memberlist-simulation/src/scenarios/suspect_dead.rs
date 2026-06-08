//! Suspect and dead state scenario helpers.

use std::{net::SocketAddr, time::Duration};

use memberlist_proto::typed::{Alive, Message, Node, Suspect};
use smol_str::SmolStr;

use crate::Cluster;

/// Injects an Alive then a Suspect for `target_id` at `observer`, advances
/// past the suspicion timeout, and returns the cluster.
///
/// Corresponds to legacy `suspect_node` in
/// `memberlist-core/src/state/tests.rs:1464`.
pub fn suspect_then_dead(
  observer: SocketAddr,
  target_id: SmolStr,
  target_addr: SocketAddr,
) -> Cluster {
  let mut c = Cluster::new();
  c.add_node(SmolStr::new("observer"), observer);

  let now = c.clock.now();
  {
    let ep = c.net.endpoints.get_mut(&observer).unwrap();
    let a = Alive::new(1, Node::new(target_id.clone(), target_addr));
    ep.handle_packet(observer, Message::Alive(a), now);
    let s = Suspect::new(1, target_id.clone(), SmolStr::new("observer"));
    ep.handle_packet(observer, Message::Suspect(s), now);
  }

  // Advance well past suspicion timeout.
  c.advance(Duration::from_secs(5));
  for _ in 0..50 {
    c.step();
  }

  c
}
