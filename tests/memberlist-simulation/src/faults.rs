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
  /// Probabilistic drop: each datagram is dropped with probability
  /// drop_per_mille/1000.
  pub(crate) drop_per_mille: u32,
  /// Per-direction probabilistic drop: a datagram from `a` to `b` is dropped
  /// with probability `directional_drop_per_mille[(a, b)] / 1000`, applied ON
  /// TOP of the global `drop_per_mille`. Unlike the symmetric global rate this
  /// models a ONE-WAY (asymmetric) lossy or cut link — `(a, b)` set without
  /// `(b, a)` means a hears b but b never hears a, the classic
  /// half-open-failure case SWIM must still resolve.
  pub(crate) directional_drop_per_mille: HashMap<(SocketAddr, SocketAddr), u32>,
  /// Probabilistic duplicate: each datagram is duplicated with probability
  /// duplicate_per_mille/1000.
  pub(crate) duplicate_per_mille: u32,
  /// Max extra random delivery delay added per datagram (reorder source).
  pub(crate) jitter: Duration,
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
    if self.drop_next.remove(&from) {
      return false;
    }
    if self.partitioned(from, to) {
      return false;
    }
    true
  }

  /// Non-consuming partition predicate: returns `true` iff `a` and `b` are
  /// assigned to DIFFERENT partition groups (so a datagram between them would
  /// be dropped). Unlike [`should_deliver`](Self::should_deliver) this does
  /// NOT touch the one-shot `drop_next` set, so it is safe to call on an
  /// already-established virtual stream each tick to decide whether the link
  /// is currently cut.
  #[allow(dead_code)]
  pub(crate) fn partitioned(&self, a: SocketAddr, b: SocketAddr) -> bool {
    match self.partitions {
      Some(ref groups) => {
        let ga = groups.get(&a).copied();
        let gb = groups.get(&b).copied();
        ga != gb
      }
      None => false,
    }
  }
}
