//! Property-based test: eventual consistency in a connected cluster.
//!
//! Property: "In a connected cluster of N nodes (2–5) with bounded
//! latency (0–100 ms), every Alive node eventually appears in every
//! other node's member view after sufficient simulation steps."

use memberlist_simulation::Cluster;
use proptest::prelude::*;
use smol_str::{SmolStr, format_smolstr};
use std::{net::SocketAddr, time::Duration};

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

proptest! {
  #![proptest_config(ProptestConfig {
    cases: 20,
    max_shrink_iters: 50,
    ..Default::default()
  })]

  /// For N nodes (2–5) with latency 0–100 ms, every node's Alive state
  /// eventually propagates to every other node in the cluster.
  ///
  /// Uses a flat step loop (not run_until_quiescent) because the simulation's
  /// 3-idle quiescence threshold can be hit before gossip propagates when the
  /// gossip stagger places the first fire beyond the initial idle steps.
  /// 5000 steps covers many gossip + probe + push/pull rounds for any
  /// (n, latency) pair in the generated range.
  #[test]
  fn every_node_learns_all_peers(
    n in 2_u16..=5_u16,
    latency_ms in 0_u64..=100_u64,
  ) {
    let base_port: u16 = 20000;
    let addrs: Vec<SocketAddr> = (0..n).map(|i| addr(base_port + i)).collect();
    let ids: Vec<SmolStr> = (0..n).map(|i| format_smolstr!("node{i}")).collect();

    let mut c = Cluster::new();
    c.set_latency(Duration::from_millis(latency_ms));

    for (id, &a) in ids.iter().zip(addrs.iter()) {
      c.add_node(id.clone(), a);
    }

    // Bootstrap node0 with knowledge of all other nodes via public API.
    for i in 1..n as usize {
      c.inject_alive(addrs[0], ids[i].clone(), addrs[i], 1);
    }

    // Run simulation (gossip + probe + push/pull).
    // 5000 steps covers many gossip/probe/push-pull rounds even for n=5
    // with latency=100ms. The step() call advances simulated time to the
    // next scheduler deadline on each idle step, so wall-clock cost is low.
    for _ in 0..5000 {
      c.step();
    }

    // Assert: every node knows every other node.
    for (i, &host_addr) in addrs.iter().enumerate() {
      for (j, peer_id) in ids.iter().enumerate() {
        if i == j {
          continue;
        }
        let found = c.member(host_addr, peer_id).is_some();
        prop_assert!(
          found,
          "node{i} should know {peer_id} (latency={latency_ms}ms, n={n})"
        );
      }
    }
  }
}
