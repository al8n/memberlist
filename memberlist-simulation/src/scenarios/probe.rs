//! Probe scenario helpers.

use std::{net::SocketAddr, time::Duration};

use memberlist_wire::typed::{Alive, Node};
use smol_str::SmolStr;

use crate::Cluster;

/// Set up a two-node cluster where `prober` knows about `target` and run
/// a direct probe. Returns the cluster after the probe completes (success path).
///
/// Corresponds to legacy `probe` in `memberlist-core/src/state/tests.rs:57`.
pub fn probe_success(prober: SocketAddr, target: SocketAddr) -> Cluster {
  let mut c = Cluster::new();
  c.add_node(SmolStr::new("prober"), prober);
  c.add_node(SmolStr::new("target"), target);

  // Seed prober's knowledge of target.
  let now = c.clock.now();
  {
    let ep = c.net.endpoints.get_mut(&prober).unwrap();
    let a = Alive::new(1, Node::new(SmolStr::new("target"), target));
    ep.handle_alive(prober, a, now);
  }

  // Trigger direct probe.
  {
    let ep = c.net.endpoints.get_mut(&prober).unwrap();
    ep.start_probe(now);
  }

  // Advance past probe_timeout to allow ack to arrive and be processed.
  c.advance(Duration::from_millis(100));
  for _ in 0..50 {
    c.step();
  }

  c
}

/// Set up a three-node cluster where `prober` suspects `target` (dead node)
/// and uses indirect probes via `helper`. Returns the cluster for assertion.
///
/// Corresponds to legacy `probe_node_suspect` in
/// `memberlist-core/src/state/tests.rs:99`.
pub fn indirect_probe_dead_target(
  prober: SocketAddr,
  helper: SocketAddr,
  target: SocketAddr,
) -> Cluster {
  let mut c = Cluster::new();
  c.add_node(SmolStr::new("prober"), prober);
  c.add_node(SmolStr::new("helper"), helper);
  // target is NOT added — it has no real endpoint; prober will get no ack.

  let now = c.clock.now();
  {
    let ep = c.net.endpoints.get_mut(&prober).unwrap();
    for (id, addr) in [("helper", helper), ("target", target)] {
      let a = Alive::new(1, Node::new(SmolStr::new(id), addr));
      ep.handle_alive(prober, a, now);
    }
  }

  // Start probe; target will not reply; prober will escalate to indirect.
  {
    c.net.endpoints.get_mut(&prober).unwrap().start_probe(now);
  }

  // Advance past probe_timeout + indirect timeout.
  c.advance(Duration::from_millis(600));
  for _ in 0..200 {
    c.step();
  }

  c
}
