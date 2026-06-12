use super::*;

#[compio::test]
async fn concrete_address_passes_through() {
  let r = LocalAddrResolver::all();
  let addr: SocketAddr = "10.0.0.5:7946".parse().unwrap();
  assert_eq!(r.resolve(&addr).await.unwrap(), vec![addr]);
}

#[compio::test]
async fn wildcard_enumerates_interface_addrs() {
  let r = LocalAddrResolver::all();
  let wild: SocketAddr = "0.0.0.0:7946".parse().unwrap();
  let out = r.resolve(&wild).await.unwrap();
  // The wildcard was replaced by real interface addresses, all on the asked
  // port; none is the wildcard itself.
  assert!(!out.contains(&wild));
  assert!(
    out
      .iter()
      .all(|s| s.port() == 7946 && !s.ip().is_unspecified())
  );
}

#[compio::test]
async fn wildcard_respects_address_family() {
  let r = LocalAddrResolver::all();
  // `0.0.0.0` must never substitute an IPv6 address, and `[::]` never an IPv4
  // one — the resolved bind+advertise address keeps the requested family.
  let v4 = r.resolve(&"0.0.0.0:7946".parse().unwrap()).await.unwrap();
  assert!(
    v4.iter().all(|s| s.is_ipv4()),
    "0.0.0.0 must yield only IPv4"
  );
  let v6 = r.resolve(&"[::]:7946".parse().unwrap()).await.unwrap();
  assert!(v6.iter().all(|s| s.is_ipv6()), "[::] must yield only IPv6");
}

#[test]
fn local_advertise_attaches_port() {
  // Best-effort: a host may have no address in a given scope, but when one is
  // found the helper attaches the requested port.
  if let Ok(addr) = local_advertise(LocalAddrScope::All, 7946) {
    assert_eq!(addr.port(), 7946);
  }
}

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
    "100.64.0.1",   // CGNAT
    "198.18.0.1",   // benchmarking
    "192.0.2.1",    // documentation
    "8.8.8.8",      // public
    "169.254.0.1",  // link-local
    "2001:db8::1",  // documentation v6
    "64:ff9b::1",   // NAT64
    "2606:4700::1", // public v6
    "fe80::1",      // link-local v6
  ] {
    assert!(
      !Private.accepts(ip.parse().unwrap()),
      "{ip} must NOT be private"
    );
  }
}

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
