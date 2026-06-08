//! getifs-backed advertise auto-detection (feature `getifs`).
//!
//! [`LocalAddrResolver`] enumerates the host's own interface addresses when the
//! configured advertise address is a wildcard (`0.0.0.0` / `[::]`) and
//! substitutes a concrete one. This driver uses a single address for both the
//! socket bind and the advertised contact — and rejects an unspecified advertise
//! at construction — so the substituted address becomes the bind address too.
//! Unlike HashiCorp memberlist (which binds `0.0.0.0` across every interface and
//! advertises a separately-detected IP), the node binds and advertises only the
//! chosen interface address.

use std::{
  io,
  net::{IpAddr, SocketAddr},
};

use crate::resolver::Resolver;

/// Which of the host's own interface addresses to enumerate when resolving a
/// wildcard (`0.0.0.0` / `[::]`) advertise address via [`LocalAddrResolver`].
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAddrScope {
  /// Private (RFC 1918 / RFC 4193) addresses — the usual choice for a LAN cluster.
  Private,
  /// Globally-routable public addresses.
  Public,
  /// Every interface address except loopback, unspecified, and link-local.
  All,
}

/// A [`Resolver`] that auto-detects the host's advertise address from its
/// network interfaces, via [`getifs`](https://crates.io/crates/getifs).
///
/// `resolve` enumerates the host's own interface addresses — filtered by the
/// configured [`LocalAddrScope`], IPv4 first — **only when the input address is
/// a wildcard** (`0.0.0.0` / `[::]`). A concrete address is passed through
/// unchanged, so the same resolver also resolves `join` seeds.
///
/// The chosen address becomes the node's single bind **and** advertise address;
/// on a multi-homed host the node binds only that one interface, not all of
/// them (this driver has no separate all-interfaces bind).
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
#[derive(Debug, Clone, Copy)]
pub struct LocalAddrResolver {
  scope: LocalAddrScope,
}

impl LocalAddrResolver {
  /// A resolver for the given scope.
  pub const fn new(scope: LocalAddrScope) -> Self {
    Self { scope }
  }

  /// Private addresses (the default).
  pub const fn private() -> Self {
    Self::new(LocalAddrScope::Private)
  }

  /// Public addresses.
  pub const fn public() -> Self {
    Self::new(LocalAddrScope::Public)
  }

  /// Every interface address except loopback, unspecified, and link-local.
  pub const fn all() -> Self {
    Self::new(LocalAddrScope::All)
  }
}

impl Default for LocalAddrResolver {
  fn default() -> Self {
    Self::private()
  }
}

impl Resolver for LocalAddrResolver {
  type Address = SocketAddr;
  type Error = io::Error;

  async fn resolve(&self, address: &SocketAddr) -> Result<Vec<SocketAddr>, io::Error> {
    let addr = *address;
    if addr.ip().is_unspecified() {
      // Honor the wildcard's family: `0.0.0.0` yields only IPv4 candidates,
      // `[::]` only IPv6 — never substitute the other family for the address
      // the caller asked to bind + advertise.
      local_socket_addrs(self.scope, addr.port(), Some(addr.is_ipv6()))
    } else {
      Ok(vec![addr])
    }
  }
}

/// Detect a single advertise [`SocketAddr`] directly — the first interface
/// address matching `scope` (IPv4 first), with `port` attached — for callers
/// who would rather compute the address up front and pass
/// [`MaybeResolved::Resolved`](crate::MaybeResolved::Resolved).
///
/// Family-agnostic: prefers IPv4. To pin a family, drive [`LocalAddrResolver`]
/// with a `0.0.0.0` / `[::]` wildcard instead.
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
pub fn local_advertise(scope: LocalAddrScope, port: u16) -> io::Result<SocketAddr> {
  local_socket_addrs(scope, port, None)?
    .into_iter()
    .next()
    .ok_or_else(|| {
      io::Error::new(
        io::ErrorKind::AddrNotAvailable,
        "no local interface address found",
      )
    })
}

/// Enumerate the host's interface addresses matching `scope`, IPv4 first, each
/// with `port` attached. `only_ipv6` filters to one family (`Some(true)` = IPv6,
/// `Some(false)` = IPv4); `None` keeps both, IPv4 first.
fn local_socket_addrs(
  scope: LocalAddrScope,
  port: u16,
  only_ipv6: Option<bool>,
) -> io::Result<Vec<SocketAddr>> {
  // Enumerate ALL interface addresses and classify them here. getifs's own
  // `private_addrs` / `public_addrs` use the broad RFC 6890 special-purpose
  // registry (CGNAT, benchmarking, NAT64, ...), not the RFC 1918 / RFC 4193
  // "reachable LAN contact" set we want to advertise.
  let mut ips: Vec<IpAddr> = getifs::local_addrs()?
    .iter()
    .map(|n| n.addr())
    .filter(|ip| scope.accepts(*ip))
    .filter(|ip| only_ipv6.is_none_or(|v6| ip.is_ipv6() == v6))
    .collect();
  // IPv4 first — Go memberlist advertises an IPv4 private address by default.
  ips.sort_by_key(|ip| ip.is_ipv6());
  Ok(
    ips
      .into_iter()
      .map(|ip| SocketAddr::new(ip, port))
      .collect(),
  )
}

// Classification via the `iprfc` RFC registry getifs itself uses (depended on
// directly so the version floor — iprfc 0.2.2, which fixes the RFC6890 IPv6
// table — is enforced in our own dependency graph). Mirrored in
// memberlist-reactor's resolver.rs; keep the two in sync (the std drivers carry
// self-contained getifs resolvers).
impl LocalAddrScope {
  /// Whether `ip` is an acceptable advertise candidate for this scope.
  fn accepts(self, ip: IpAddr) -> bool {
    use iprfc::{RFC1918, RFC4193, RFC6890};
    match self {
      // RFC 1918 (IPv4) + RFC 4193 unique-local (IPv6).
      LocalAddrScope::Private => RFC1918.contains(&ip) || RFC4193.contains(&ip),
      // Globally-routable = not in the RFC 6890 special-purpose registry — the
      // exact definition getifs uses for its own `public_addrs`.
      LocalAddrScope::Public => !RFC6890.contains(&ip),
      LocalAddrScope::All => !ip.is_loopback() && !ip.is_unspecified() && !is_link_local(ip),
    }
  }
}

/// Link-local (IPv4 169.254/16, IPv6 fe80::/10) — usable only on the local link.
fn is_link_local(ip: IpAddr) -> bool {
  match ip {
    IpAddr::V4(a) => a.is_link_local(),
    IpAddr::V6(a) => (a.segments()[0] & 0xffc0) == 0xfe80,
  }
}

#[cfg(test)]
mod tests {
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
}
