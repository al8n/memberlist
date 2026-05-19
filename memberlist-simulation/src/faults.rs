//! Fault injection configuration for the virtual network.

use std::{
  collections::{HashMap, HashSet},
  net::SocketAddr,
  time::Duration,
};

/// Controls which packets are dropped, delayed, or partitioned.
///
/// Fields are `pub(crate)` so `Cluster` and tests can mutate them
/// directly without a public setter API.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
  /// One-shot drop set: the next datagram FROM this address is dropped.
  pub(crate) drop_next: HashSet<SocketAddr>,
  /// Global artificial latency added to every enqueued datagram.
  pub(crate) latency: Duration,
  /// Partition groups: if `partitions[a] != partitions[b]` datagrams are
  /// silently dropped. `None` means no partition is active.
  pub(crate) partitions: Option<HashMap<SocketAddr, usize>>,
}

impl FaultConfig {
  /// No faults; zero latency.
  pub fn none() -> Self {
    Self::default()
  }

  /// Returns `true` if a datagram from `from` to `to` should be delivered.
  /// Side-effect: consumes a pending one-shot drop.
  #[allow(dead_code)]
  pub(crate) fn should_deliver(&mut self, from: SocketAddr, to: SocketAddr) -> bool {
    // One-shot drop.
    if self.drop_next.remove(&from) {
      return false;
    }
    // Partition check.
    if let Some(ref groups) = self.partitions {
      let gf = groups.get(&from).copied();
      let gt = groups.get(&to).copied();
      if gf != gt {
        return false;
      }
    }
    true
  }
}
