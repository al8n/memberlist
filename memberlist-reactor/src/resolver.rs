//! Address resolution: the [`AddressResolver`] trait, the [`MaybeResolved`]
//! seed/advertise form, and the trivial [`SocketAddrResolver`].
//!
//! Resolution happens only at the boundary — the bootstrap constructor (the
//! local advertise address) and `join` (the seeds) — so a resolver is passed to
//! those methods rather than stored on the `Memberlist`. Everything past the
//! boundary is a wire [`SocketAddr`]; the cluster only ever gossips resolved
//! addresses.

use std::{future::Future, net::SocketAddr};

/// Resolves an unresolved address (e.g. a `host:port` domain name) into wire
/// [`SocketAddr`]s.
///
/// Invoked only at the boundary (bootstrap + `join`). `resolve` returns zero or
/// more candidates — a name may map to several A/AAAA records. The returned
/// future is `Send` so resolution can run on a multi-threaded runtime; it is
/// written `-> impl Future + Send` rather than `async fn` so the `Send` bound is
/// part of the trait contract.
pub trait AddressResolver: Send + Sync + 'static {
  /// The unresolved address this resolver accepts.
  type Address: Send + Sync + 'static;

  /// The error returned when resolution fails.
  type Error: core::error::Error + Send + Sync + 'static;

  /// Resolves `address` into zero or more candidate wire addresses.
  fn resolve(
    &self,
    address: &Self::Address,
  ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_;
}

/// A seed or advertise address that is either an already-resolved wire
/// [`SocketAddr`] or an unresolved address to pass through an
/// [`AddressResolver`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MaybeResolved<A> {
  /// An already-resolved wire address — used verbatim, no resolver needed.
  Resolved(SocketAddr),
  /// An unresolved address, resolved at the boundary.
  Unresolved(A),
}

/// An [`AddressResolver`] for callers that already hold wire [`SocketAddr`]s: it
/// passes each address through unchanged and never fails.
#[derive(Debug, Default, Clone, Copy)]
pub struct SocketAddrResolver;

impl AddressResolver for SocketAddrResolver {
  type Address = SocketAddr;
  type Error = core::convert::Infallible;

  fn resolve(
    &self,
    address: &SocketAddr,
  ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
    let addr = *address;
    async move { Ok(vec![addr]) }
  }
}

#[cfg(test)]
mod tests {
  use std::{
    pin::pin,
    task::{Context, Poll, Waker},
  };

  use super::*;

  fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
  }

  /// A dependency-free single-poll driver for the trivially-ready resolver
  /// futures (each completes on first poll). Avoids pulling a runtime into the
  /// always-on (feature-bare) resolver module.
  fn block_on<F: Future>(fut: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(fut);
    loop {
      if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
        return out;
      }
    }
  }

  /// `SocketAddrResolver` passes a wire address through unchanged, yielding
  /// exactly that one candidate.
  #[test]
  fn socket_addr_resolver_passes_through() {
    let addr = sock(7777);
    let out =
      block_on(SocketAddrResolver.resolve(&addr)).expect("SocketAddrResolver is infallible");
    assert_eq!(
      out,
      vec![addr],
      "the resolved address is the input verbatim"
    );
  }

  /// The resolver is `Default` / `Copy`, and a copy resolves identically.
  #[test]
  fn socket_addr_resolver_is_default_and_copy() {
    let r = SocketAddrResolver;
    let copy = r;
    // Exercise the derived `Default` via the explicit trait path (the bare
    // `SocketAddrResolver::default()` form trips clippy on a unit struct).
    let _default = <SocketAddrResolver as Default>::default();
    let addr = sock(8888);
    let out = block_on(copy.resolve(&addr)).expect("infallible");
    assert_eq!(out, vec![addr]);
  }

  /// `MaybeResolved` derives equality and hashing; the two variants are
  /// distinct, and `Resolved`/`Unresolved` round-trip their payloads.
  #[test]
  fn maybe_resolved_variants_are_distinct() {
    let resolved: MaybeResolved<SocketAddr> = MaybeResolved::Resolved(sock(1));
    let unresolved: MaybeResolved<SocketAddr> = MaybeResolved::Unresolved(sock(1));
    assert_ne!(
      resolved, unresolved,
      "a resolved wire address differs from an unresolved one even at the same socket"
    );
    assert_eq!(resolved.clone(), MaybeResolved::Resolved(sock(1)));
    match unresolved {
      MaybeResolved::Unresolved(a) => assert_eq!(a, sock(1)),
      MaybeResolved::Resolved(_) => panic!("expected the Unresolved variant"),
    }

    // Debug + Hash are derived; exercise them so the derives stay covered.
    use std::collections::HashSet;
    let mut set: HashSet<MaybeResolved<SocketAddr>> = HashSet::new();
    set.insert(MaybeResolved::Resolved(sock(2)));
    assert!(set.contains(&MaybeResolved::Resolved(sock(2))));
    assert!(!format!("{resolved:?}").is_empty());
  }

  /// A custom `AddressResolver` may return several candidates (a name mapping
  /// to multiple A/AAAA records) and may report a typed error.
  #[test]
  fn custom_resolver_multi_candidate_and_error() {
    struct MultiResolver;

    #[derive(Debug, thiserror::Error)]
    #[error("resolve failed: {0}")]
    struct ResolveErr(&'static str);

    impl AddressResolver for MultiResolver {
      type Address = &'static str;
      type Error = ResolveErr;

      fn resolve(
        &self,
        address: &&'static str,
      ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
        let address = *address;
        async move {
          match address {
            "multi" => Ok(vec![sock(1), sock(2), sock(3)]),
            "empty" => Ok(vec![]),
            other => Err(ResolveErr(other)),
          }
        }
      }
    }

    let multi = block_on(MultiResolver.resolve(&"multi")).expect("ok");
    assert_eq!(multi.len(), 3, "a name may map to several candidates");

    let empty = block_on(MultiResolver.resolve(&"empty")).expect("ok");
    assert!(empty.is_empty(), "zero candidates is a valid resolution");

    let err = block_on(MultiResolver.resolve(&"bad"));
    assert!(
      matches!(err, Err(ResolveErr("bad"))),
      "the typed error surfaces"
    );
  }
}
