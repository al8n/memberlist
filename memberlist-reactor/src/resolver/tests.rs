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
  let out = block_on(SocketAddrResolver.resolve(&addr)).expect("SocketAddrResolver is infallible");
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

#[cfg(feature = "getifs")]
#[test]
fn private_scope_is_rfc1918_and_ula_only() {
  use LocalAddrScope::Private;
  for ip in [
    "10.0.0.1",
    "172.16.5.4",
    "192.168.1.1",
    "fc00::1",
    "fd12::3",
  ] {
    assert!(
      Private.accepts(ip.parse().unwrap()),
      "{ip} should be private"
    );
  }
  // RFC 6890 special-purpose that getifs::private_addrs would wrongly include:
  for ip in [
    "100.64.0.1",
    "198.18.0.1",
    "192.0.2.1",
    "8.8.8.8",
    "169.254.0.1",
    "2001:db8::1",
    "64:ff9b::1",
    "2606:4700::1",
    "fe80::1",
  ] {
    assert!(
      !Private.accepts(ip.parse().unwrap()),
      "{ip} must NOT be private"
    );
  }
}

#[cfg(feature = "getifs")]
#[test]
fn public_scope_excludes_special_purpose() {
  use LocalAddrScope::Public;
  assert!(Public.accepts("8.8.8.8".parse().unwrap()));
  assert!(Public.accepts("2606:4700::1".parse().unwrap()));
  // Real global unicast inside 2001::/16 — must be public (not the whole
  // /16 special-purpose block; only 2001::/23 + 2001:db8::/32 are).
  assert!(Public.accepts("2001:4860:4860::8888".parse().unwrap()));
  for ip in [
    "10.0.0.1",
    "100.64.0.1", // CGNAT
    "198.18.0.1", // benchmarking
    "192.0.2.1",  // documentation
    "192.0.0.1",  // IETF protocol assignments
    "240.0.0.1",  // reserved / future use
    "127.0.0.1",
    "169.254.0.1",
    "fc00::1",
    "2001:db8::1", // documentation v6
    "2001::1",     // 2001::/23 (Teredo)
    "2001:2::1",   // benchmarking v6
    "2001:10::1",  // ORCHID
    "fe80::1",
    "::1",
  ] {
    assert!(
      !Public.accepts(ip.parse().unwrap()),
      "{ip} must NOT be public"
    );
  }
}

#[cfg(feature = "getifs")]
#[test]
fn all_scope_excludes_loopback_unspecified_linklocal() {
  use LocalAddrScope::All;
  assert!(All.accepts("10.0.0.1".parse().unwrap()));
  assert!(All.accepts("8.8.8.8".parse().unwrap()));
  for ip in [
    "127.0.0.1",
    "0.0.0.0",
    "169.254.0.1",
    "::1",
    "::",
    "fe80::1",
  ] {
    assert!(!All.accepts(ip.parse().unwrap()), "{ip} must NOT be in All");
  }
}

#[cfg(feature = "getifs")]
#[test]
fn wildcard_respects_address_family() {
  let r = LocalAddrResolver::all();
  // `0.0.0.0` must never substitute an IPv6 address, and `[::]` never an IPv4
  // one — the resolved bind+advertise address keeps the requested family.
  let v4 = block_on(r.resolve(&"0.0.0.0:7946".parse().unwrap())).unwrap();
  assert!(
    v4.iter().all(|s| s.is_ipv4()),
    "0.0.0.0 must yield only IPv4"
  );
  let v6 = block_on(r.resolve(&"[::]:7946".parse().unwrap())).unwrap();
  assert!(v6.iter().all(|s| s.is_ipv6()), "[::] must yield only IPv6");
}
