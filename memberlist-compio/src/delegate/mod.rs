//! `Delegate` composite — compio's per-driver observation hook surface.
//!
//! Composes the 4 driver-fired observation hooks (`EventDelegate` /
//! `ConflictDelegate` / `PingDelegate` / `NodeDelegate`). `!Send`-tolerant
//! — the driver loop owns it and fires the AFIT hooks on the (single,
//! `!Send`) compio thread.
//!
//! Admission predicates are separate: they are the machine's
//! `AliveDelegate` / `MergeDelegate` (`Send + Sync`, re-exported below),
//! supplied via [`Options`](crate::Options) and installed into the
//! `Endpoint`. Admission runs inline inside the FSM; observation runs in
//! the driver loop — distinct concerns, distinct traits.

mod conflict;
mod event;
mod node;
mod ping;
mod void;

pub use conflict::ConflictDelegate;
pub use event::EventDelegate;
pub use node::NodeDelegate;
pub use ping::PingDelegate;
pub use void::VoidDelegate;

// Admission predicates are the machine's Sans-I/O traits — re-exported so
// compio users name them from one place when supplying them to `Options`.
pub use memberlist_proto::{AliveDelegate, MergeDelegate};

/// compio's per-driver observation hook surface (NOT admission — that's
/// the machine's `AliveDelegate`/`MergeDelegate` via `Options`).
///
/// `!Send`-tolerant: the driver loop owns the delegate and fires the AFIT
/// hooks on one thread.
pub trait Delegate:
  NodeDelegate
  + PingDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + EventDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + ConflictDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
{
  /// Node identifier type.
  type Id;
  /// Address type — always `SocketAddr` in compio.
  type Address;
}

/// Forwards a boxed machine `AliveDelegate` so it satisfies the
/// `impl AliveDelegate` bound of `Endpoint::set_alive_delegate`, which boxes
/// its argument again. `Send + Sync` flows from the boxed dyn's supertrait,
/// so the machine needs no `Box<dyn AliveDelegate>` forwarding impl of its
/// own and stays untouched.
pub(crate) struct BoxedAlive<I, A>(pub(crate) Box<dyn AliveDelegate<I, A>>);

impl<I: 'static, A: 'static> AliveDelegate<I, A> for BoxedAlive<I, A> {
  fn notify_alive(&self, peer: &memberlist_proto::typed::NodeState<I, A>) -> bool {
    self.0.notify_alive(peer)
  }
}

/// Forwards a boxed machine `MergeDelegate`; same rationale as
/// [`BoxedAlive`].
pub(crate) struct BoxedMerge<I, A>(pub(crate) Box<dyn MergeDelegate<I, A>>);

impl<I: 'static, A: 'static> MergeDelegate<I, A> for BoxedMerge<I, A> {
  fn notify_merge(&self, peers: &[memberlist_proto::typed::NodeState<I, A>]) -> bool {
    self.0.notify_merge(peers)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  #[test]
  fn void_delegate_satisfies_observation_composite() {
    fn assert_delegate<D: Delegate<Id = SmolStr, Address = SocketAddr>>(_d: &D) {}
    let v: VoidDelegate<SmolStr, SocketAddr> = VoidDelegate::default();
    assert_delegate(&v);
  }
}
