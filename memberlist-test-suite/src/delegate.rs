//! Driver-agnostic test fixtures shared by every driver's suite adapter.
//!
//! The observation delegate that records inbound hooks stays per-driver — the
//! drivers expose different delegate trait shapes (compio splits it across four
//! sub-traits, reactor uses one flat trait) — but it writes into the shared
//! [`Captures`] here, and the adapter reads that back through the `TestCluster`
//! capture accessors. The admission predicates ([`RejectMerge`] /
//! [`VetoForeignAlive`]) ARE driver-agnostic (plain `memberlist-proto`
//! `MergeDelegate` / `AliveDelegate` impls), so they live here and are installed
//! through each driver's `Options::with_merge_delegate` / `with_alive_delegate`.

use std::{
  net::SocketAddr,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::Duration,
};

use bytes::Bytes;
use memberlist_proto::{AliveDelegate, MergeDelegate, typed::NodeState};
use smol_str::SmolStr;

/// One observed ping completion: which peer answered, the measured round trip,
/// and the ack payload that peer's ping delegate attached.
#[derive(Debug, Clone)]
pub struct PingObservation {
  /// The peer that answered the probe.
  pub peer: SmolStr,
  /// The measured round-trip time.
  pub rtt: Duration,
  /// The ack payload the peer attached to its reply.
  pub payload: Bytes,
}

/// The shared buffers a per-driver observation delegate records into. The
/// adapter holds a clone and surfaces them through the `TestCluster` capture
/// accessors. Each buffer locks independently so one hook records without
/// blocking another.
#[derive(Clone, Default)]
pub struct Captures {
  /// Inbound user messages (gossip- or stream-plane), in arrival order.
  pub msgs: Arc<Mutex<Vec<Bytes>>>,
  /// Remote push/pull states received via the node delegate's
  /// `merge_remote_state` hook, in arrival order.
  pub remote_states: Arc<Mutex<Vec<Bytes>>>,
  /// Observed node conflicts as `(existing_id, conflicting_id)` pairs.
  pub conflicts: Arc<Mutex<Vec<(SmolStr, SmolStr)>>>,
  /// Observed ping completions.
  pub pings: Arc<Mutex<Vec<PingObservation>>>,
}

impl Captures {
  /// A snapshot of the captured inbound user messages, in arrival order.
  pub fn messages(&self) -> Vec<Bytes> {
    self.msgs.lock().unwrap().clone()
  }

  /// A snapshot of the captured remote push/pull states.
  pub fn remote_states(&self) -> Vec<Bytes> {
    self.remote_states.lock().unwrap().clone()
  }

  /// A snapshot of the captured node conflicts.
  pub fn conflicts(&self) -> Vec<(SmolStr, SmolStr)> {
    self.conflicts.lock().unwrap().clone()
  }

  /// A snapshot of the captured ping completions.
  pub fn pings(&self) -> Vec<PingObservation> {
    self.pings.lock().unwrap().clone()
  }
}

/// A [`MergeDelegate`] that cancels every join merge and records that it ran.
///
/// Installed by the `join_cancel` scenario: a peer whose merge the delegate
/// vetoes is never incorporated, so a join that consults it leaves membership
/// at one. (`false` is the Sans-I/O analog of the legacy delegate returning an
/// error — it cancels the join merge.)
#[derive(Clone, Default)]
pub struct RejectMerge {
  invoked: Arc<AtomicBool>,
}

impl RejectMerge {
  /// A fresh rejecting predicate, paired with the shared flag the adapter reads
  /// to confirm the merge hook fired.
  pub fn new() -> (Self, Arc<AtomicBool>) {
    let invoked = Arc::new(AtomicBool::new(false));
    (
      Self {
        invoked: invoked.clone(),
      },
      invoked,
    )
  }
}

impl MergeDelegate<SmolStr, SocketAddr> for RejectMerge {
  fn notify_merge(&self, _peers: &[NodeState<SmolStr, SocketAddr>]) -> bool {
    self.invoked.store(true, Ordering::SeqCst);
    false
  }
}

/// An [`AliveDelegate`] that admits only its own node and vetoes every foreign
/// alive, counting each invocation.
///
/// Installed by the `join_cancel_passive` scenario: the join handshake itself
/// succeeds, but because every foreign alive is vetoed no peer is ever admitted,
/// so membership stays at one while the delegate's invocation count climbs.
#[derive(Clone)]
pub struct VetoForeignAlive {
  own_id: SmolStr,
  count: Arc<AtomicUsize>,
}

impl VetoForeignAlive {
  /// A fresh predicate that admits `own_id` and vetoes every other peer, paired
  /// with the shared invocation counter the adapter reads.
  pub fn new(own_id: SmolStr) -> (Self, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    (
      Self {
        own_id,
        count: count.clone(),
      },
      count,
    )
  }
}

impl AliveDelegate<SmolStr, SocketAddr> for VetoForeignAlive {
  fn notify_alive(&self, peer: &NodeState<SmolStr, SocketAddr>) -> bool {
    self.count.fetch_add(1, Ordering::SeqCst);
    peer.id_ref() == &self.own_id
  }
}
