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

use std::boxed::Box;

use crate::{maybe_owned::MaybeOwned, typed::NodeState};

/// Filters inbound Alive messages. Returning `false` ignores the alive so
/// the node is not considered a peer — the Sans-I/O analog of Go
/// `AliveDelegate.NotifyAlive` returning a non-nil error (`state.go:988`,
/// whose error is only logged then dropped). Invoked inline for **every**
/// alive (gossip or push/pull), matching `aliveNode`.
pub trait AliveDelegate<I, A>: Send + Sync + 'static {
  /// `true` to admit the peer, `false` to ignore this alive message.
  fn notify_alive(&self, peer: &NodeState<I, A>) -> bool;
}

/// A boxed alive delegate is itself an [`AliveDelegate`], so a driver can store
/// or compose delegates as trait objects.
impl<I, A> AliveDelegate<I, A> for Box<dyn AliveDelegate<I, A>>
where
  I: 'static,
  A: 'static,
{
  #[inline]
  fn notify_alive(&self, peer: &NodeState<I, A>) -> bool {
    (**self).notify_alive(peer)
  }
}

/// Filters a push/pull merge. Returning `false` cancels the merge — the
/// Sans-I/O analog of Go `MergeDelegate.NotifyMerge` returning a non-nil error
/// (`net.go:1297`). Invoked inline for EVERY push/pull — a join AND a periodic
/// anti-entropy refresh. This deliberately tightens Go memberlist, which gates
/// `NotifyMerge` on the join flag only (`net.go:1280`, `if join`) and so still
/// merges a rejected peer's state on anti-entropy.
pub trait MergeDelegate<I, A>: Send + Sync + 'static {
  /// `true` to proceed with the merge, `false` to cancel it. `peers` is a
  /// [`MaybeOwned`] slice the caller already owns: borrow it (`&peers` /
  /// `peers.iter()`) for a predicate, or take it for free with
  /// [`into_owned`](MaybeOwned::into_owned) if the delegate keeps the peer view.
  fn notify_merge(&self, peers: MaybeOwned<'_, [NodeState<I, A>]>) -> bool;
}

/// A boxed merge delegate is itself a [`MergeDelegate`], so a driver can store
/// or compose delegates as trait objects.
impl<I, A> MergeDelegate<I, A> for Box<dyn MergeDelegate<I, A>>
where
  I: 'static,
  A: 'static,
{
  #[inline]
  fn notify_merge(&self, peers: MaybeOwned<'_, [NodeState<I, A>]>) -> bool {
    (**self).notify_merge(peers)
  }
}
