//! CIDR-based peer-admission policy.
//!
//! A [`CidrPolicy`] is an IP allow-list that plugs into the membership FSM as an
//! [`AliveDelegate`]: an inbound Alive whose advertised address falls outside
//! the policy is ignored, so the peer is not admitted to the membership table.
//! It is the Sans-I/O port of memberlist's `CIDRsPolicy`, which gated the same
//! `aliveNode` admission point. Because the FSM runs every alive — gossip and
//! push/pull alike — through [`AliveDelegate::notify_alive`], one policy gates
//! membership admission on both the unreliable and reliable planes.
//!
//! # Scope: advertised address, not packet origin
//!
//! The decision uses the peer's **self-advertised** address — the node address
//! carried inside the Alive — NOT the observed transport source. So this is a
//! membership-admission filter, **not** a spoof-proof network-origin boundary: a
//! node can advertise an in-policy address from any source and be admitted. To
//! restrict by the observed packet/connection source, filter at the
//! transport/driver layer, where the remote socket address is available; the
//! machine intentionally does not surface it to the delegate (a delegate is a
//! pure predicate over membership state, not over transport metadata).
//!
//! # Join reports contact, not admission
//!
//! Rejecting a peer drops its Alive; it does NOT fail the transport exchange
//! that carried it. A join toward an out-of-policy seed therefore still
//! completes its exchange and counts that seed as contacted, while the seed is
//! not admitted to membership. The join "contacted" count reflects completed
//! exchanges, not admission — inspect membership to confirm a peer was admitted.

use core::{
  net::{IpAddr, SocketAddr},
  str::FromStr,
};

use ipnet::IpNet;

use crate::{FxHashSet, delegate::AliveDelegate, typed::NodeState};

/// A Classless Inter-Domain Routing (CIDR) peer-admission allow-list.
///
/// The inner `Option` encodes three states:
/// - `None` — allow every address (no restriction).
/// - `Some(empty)` — block every address.
/// - `Some(non-empty)` — allow only addresses contained in one of the networks.
///
/// Allow-all is reached ONLY through the explicit [`allow_all`](Self::allow_all)
/// / [`new`](Self::new) / [`Default`] constructors. Building from a collection
/// ([`FromIterator`] / [`TryFrom`]) — even an empty one — yields a policy that is
/// in effect, and removing every entry leaves a block-all policy. So an
/// explicitly empty allow-list stays fail-closed (deny) rather than silently
/// becoming allow-all.
///
/// Install it as an [`AliveDelegate`] (e.g. via a driver's `with_alive_delegate`):
/// [`notify_alive`](AliveDelegate::notify_alive) admits a peer iff its advertised
/// IP [`is_allowed`](CidrPolicy::is_allowed).
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct CidrPolicy {
  allowed_cidrs: Option<FxHashSet<IpNet>>,
}

impl Default for CidrPolicy {
  #[inline]
  fn default() -> Self {
    Self::allow_all()
  }
}

impl CidrPolicy {
  /// A policy with no restriction — every address is admitted (the default).
  #[inline]
  pub const fn allow_all() -> Self {
    Self {
      allowed_cidrs: None,
    }
  }

  /// A policy that admits no address — every address is denied.
  #[inline]
  pub fn block_all() -> Self {
    Self {
      allowed_cidrs: Some(FxHashSet::default()),
    }
  }

  /// A default (allow-all) policy.
  #[inline]
  pub const fn new() -> Self {
    Self::allow_all()
  }

  /// Add an allowed network.
  pub fn add(&mut self, net: IpNet) {
    self
      .allowed_cidrs
      .get_or_insert_with(FxHashSet::default)
      .insert(net);
  }

  /// Remove an allowed network. Removing the last one leaves a block-all policy
  /// (an in-effect policy that admits nothing); call [`allow_all`](Self::allow_all)
  /// to lift the restriction entirely.
  pub fn remove(&mut self, net: &IpNet) {
    if let Some(allowed) = self.allowed_cidrs.as_mut() {
      allowed.remove(net);
    }
  }

  /// Remove every allowed network that contains `ip`. Removing the last one
  /// leaves a block-all policy; call [`allow_all`](Self::allow_all) to lift the
  /// restriction entirely.
  pub fn remove_by_ip(&mut self, ip: &IpAddr) {
    if let Some(allowed) = self.allowed_cidrs.as_mut() {
      allowed.retain(|net| !net.contains(ip));
    }
  }

  /// An iterator over the allowed networks (empty when allow-all or block-all).
  pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
    self.allowed_cidrs.as_ref().into_iter().flatten()
  }

  /// Whether the exact `net` is in the allow-list (allow-all admits everything).
  pub fn is_allowed_net(&self, net: &IpNet) -> bool {
    self.allowed_cidrs.as_ref().is_none_or(|x| x.contains(net))
  }

  /// Whether the exact `net` is blocked.
  pub fn is_blocked_net(&self, net: &IpNet) -> bool {
    !self.is_allowed_net(net)
  }

  /// Whether `ip` is allowed (allow-all admits every address).
  pub fn is_allowed(&self, ip: &IpAddr) -> bool {
    self
      .allowed_cidrs
      .as_ref()
      .is_none_or(|nets| nets.iter().any(|net| net.contains(ip)))
  }

  /// Whether `ip` is blocked.
  pub fn is_blocked(&self, ip: &IpAddr) -> bool {
    !self.is_allowed(ip)
  }

  /// Whether every address is blocked.
  pub fn is_block_all(&self) -> bool {
    self
      .allowed_cidrs
      .as_ref()
      .is_some_and(|nets| nets.is_empty())
  }

  /// Whether every address is allowed.
  pub fn is_allow_all(&self) -> bool {
    self.allowed_cidrs.is_none()
  }
}

/// An explicit set of allowed networks. An EMPTY set is block-all, not allow-all
/// — only [`allow_all`](CidrPolicy::allow_all) / [`new`](CidrPolicy::new) /
/// [`Default`] produce allow-all. (std only; no_std builds use [`FromIterator`]
/// or [`TryFrom`].)
#[cfg(feature = "std")]
impl From<std::collections::HashSet<IpNet>> for CidrPolicy {
  fn from(allowed_cidrs: std::collections::HashSet<IpNet>) -> Self {
    Self {
      allowed_cidrs: Some(allowed_cidrs.into_iter().collect()),
    }
  }
}

impl<A: Into<IpNet>> FromIterator<A> for CidrPolicy {
  /// Collect allowed networks. An empty iterator yields a block-all policy.
  fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
    Self {
      allowed_cidrs: Some(iter.into_iter().map(Into::into).collect()),
    }
  }
}

impl<A: AsRef<str>> TryFrom<&[A]> for CidrPolicy {
  type Error = ipnet::AddrParseError;

  /// Parse a slice of CIDR strings (e.g. `["10.0.0.0/8", "127.0.0.1/32"]`).
  /// Whitespace around each entry is trimmed. An empty slice yields a block-all
  /// policy.
  fn try_from(iter: &[A]) -> Result<Self, Self::Error> {
    let mut allowed_cidrs = FxHashSet::with_capacity_and_hasher(iter.len(), Default::default());
    for cidr in iter {
      allowed_cidrs.insert(IpNet::from_str(cidr.as_ref().trim())?);
    }
    Ok(Self {
      allowed_cidrs: Some(allowed_cidrs),
    })
  }
}

/// Admits a peer iff its **self-advertised** IP is allowed by the policy — the
/// address carried in the Alive, not the observed transport source (see the
/// module docs: this is membership admission, not a spoof-proof origin
/// boundary). An ignored alive keeps the peer out of the membership table, so
/// two clusters whose policies exclude each other's advertised networks never
/// merge.
impl<I> AliveDelegate<I, SocketAddr> for CidrPolicy {
  #[inline]
  fn notify_alive(&self, peer: &NodeState<I, SocketAddr>) -> bool {
    self.is_allowed(&peer.address_ref().ip())
  }
}

/// Composes a [`CidrPolicy`] with another [`AliveDelegate`]: a peer is admitted
/// only when BOTH the policy and the inner delegate admit it. Lets a driver
/// enforce a CIDR policy alongside a user-supplied alive delegate from a single
/// `with_cidr_policy` setting.
#[derive(Debug, Clone)]
pub struct CidrAnd<D> {
  policy: CidrPolicy,
  inner: D,
}

impl<D> CidrAnd<D> {
  /// Compose `policy` with `inner` — a peer must pass BOTH to be admitted.
  #[inline]
  pub fn new(policy: CidrPolicy, inner: D) -> Self {
    Self { policy, inner }
  }
}

impl<I, D: AliveDelegate<I, SocketAddr>> AliveDelegate<I, SocketAddr> for CidrAnd<D> {
  #[inline]
  fn notify_alive(&self, peer: &NodeState<I, SocketAddr>) -> bool {
    self.policy.is_allowed(&peer.address_ref().ip()) && self.inner.notify_alive(peer)
  }
}

#[cfg(test)]
mod tests;
