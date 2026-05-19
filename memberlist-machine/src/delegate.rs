//! Synchronous membership-admission delegates.
//!
//! Sans-I/O port of memberlist-core's (and HashiCorp Go memberlist's)
//! `AliveDelegate` / `MergeDelegate`. Unlike the original `async fn`
//! delegates, these are **synchronous, pure, non-blocking, deterministic**
//! filters the [`Endpoint`](crate::endpoint::Endpoint) calls *inline* while
//! processing an inbound Alive / join push-pull — exactly where Go's
//! `aliveNode` / push-pull invoked the delegates (`state.go:988` /
//! `net.go:1297`). They MUST NOT perform I/O, block, `await`, or read the
//! clock: that would break the Sans-I/O guarantee and the deterministic
//! simulation. If an admission decision needs async work, do it in the
//! driver before feeding the packet to the machine, or have the delegate
//! consult a pre-computed allow/deny set.
//!
//! No delegate (`None`) accepts everything, mirroring Go's optional
//! `config.Alive` / `config.Merge`.

use memberlist_wire::typed::NodeState;

/// Filters inbound Alive messages. Returning `false` ignores the alive so
/// the node is not considered a peer — the Sans-I/O analog of Go
/// `AliveDelegate.NotifyAlive` returning a non-nil error (`state.go:988`,
/// whose error is only logged then dropped). Invoked inline for **every**
/// alive (gossip or push/pull), matching `aliveNode`.
pub trait AliveDelegate<I, A>: Send + Sync + 'static {
  /// `true` to admit the peer, `false` to ignore this alive message.
  fn notify_alive(&self, peer: &NodeState<I, A>) -> bool;
}

/// Filters a **join** push/pull merge. Returning `false` cancels the merge
/// — the Sans-I/O analog of Go `MergeDelegate.NotifyMerge` returning a
/// non-nil error (`net.go:1297`). Invoked inline ONLY for a join push/pull,
/// never for periodic anti-entropy refresh (`net.go:1280`, gated on
/// `if join`).
pub trait MergeDelegate<I, A>: Send + Sync + 'static {
  /// `true` to proceed with the merge, `false` to cancel the join merge.
  fn notify_merge(&self, peers: &[NodeState<I, A>]) -> bool;
}
