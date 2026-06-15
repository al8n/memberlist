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

/// Which of the host's own interface addresses to enumerate when resolving a
/// wildcard (`0.0.0.0` / `[::]`) advertise address via [`LocalAddrResolver`].
#[cfg(feature = "getifs")]
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

/// An [`AddressResolver`] that auto-detects the host's advertise address from
/// its network interfaces, via [`getifs`](https://crates.io/crates/getifs).
///
/// `resolve` enumerates the host's own interface addresses — filtered by the
/// configured [`LocalAddrScope`], IPv4 first — **only when the input address is
/// a wildcard** (`0.0.0.0` / `[::]`). A concrete address is passed through
/// unchanged, so the same resolver also resolves `join` seeds.
///
/// The chosen address becomes the node's single bind **and** advertise address;
/// this driver binds and advertises one concrete address (an unspecified
/// advertise is rejected at construction) rather than binding `0.0.0.0` across
/// every interface the way HashiCorp memberlist does. On a multi-homed host the
/// node binds only the chosen interface.
#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
#[derive(Debug, Clone, Copy)]
pub struct LocalAddrResolver {
  scope: LocalAddrScope,
}

#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
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

#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
impl Default for LocalAddrResolver {
  fn default() -> Self {
    Self::private()
  }
}

#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
impl AddressResolver for LocalAddrResolver {
  type Address = SocketAddr;
  type Error = std::io::Error;

  fn resolve(
    &self,
    address: &SocketAddr,
  ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>> + Send + '_ {
    let addr = *address;
    let scope = self.scope;
    async move {
      if addr.ip().is_unspecified() {
        // Honor the wildcard's family: `0.0.0.0` yields only IPv4 candidates,
        // `[::]` only IPv6 — never substitute the other family for the address
        // the caller asked to bind + advertise.
        local_socket_addrs(scope, addr.port(), Some(addr.is_ipv6()))
      } else {
        Ok(vec![addr])
      }
    }
  }
}

/// Detect a single advertise [`SocketAddr`] directly — the first interface
/// address matching `scope` (IPv4 first), with `port` attached — for callers
/// who would rather compute the address up front and pass
/// [`MaybeResolved::Resolved`].
///
/// Family-agnostic: prefers IPv4. To pin a family, drive [`LocalAddrResolver`]
/// with a `0.0.0.0` / `[::]` wildcard instead.
#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
pub fn local_advertise(scope: LocalAddrScope, port: u16) -> std::io::Result<SocketAddr> {
  local_socket_addrs(scope, port, None)?
    .into_iter()
    .next()
    .ok_or_else(|| {
      std::io::Error::new(
        std::io::ErrorKind::AddrNotAvailable,
        "no local interface address found",
      )
    })
}

/// Enumerate the host's interface addresses matching `scope`, IPv4 first, each
/// with `port` attached. `only_ipv6` filters to one family (`Some(true)` = IPv6,
/// `Some(false)` = IPv4); `None` keeps both, IPv4 first.
#[cfg(feature = "getifs")]
fn local_socket_addrs(
  scope: LocalAddrScope,
  port: u16,
  only_ipv6: Option<bool>,
) -> std::io::Result<Vec<SocketAddr>> {
  // Enumerate ALL interface addresses and classify them here. getifs's own
  // `private_addrs` / `public_addrs` use the broad RFC 6890 special-purpose
  // registry (CGNAT, benchmarking, NAT64, ...), not the RFC 1918 / RFC 4193
  // "reachable LAN contact" set we want to advertise.
  let mut ips: Vec<std::net::IpAddr> = getifs::local_addrs()?
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
// memberlist-compio's resolver/getifs.rs; keep the two in sync (the std drivers
// carry self-contained getifs resolvers).
#[cfg(feature = "getifs")]
#[cfg_attr(docsrs, doc(cfg(feature = "getifs")))]
impl LocalAddrScope {
  /// Whether `ip` is an acceptable advertise candidate for this scope.
  fn accepts(self, ip: std::net::IpAddr) -> bool {
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
#[cfg(feature = "getifs")]
fn is_link_local(ip: std::net::IpAddr) -> bool {
  match ip {
    std::net::IpAddr::V4(a) => a.is_link_local(),
    std::net::IpAddr::V6(a) => (a.segments()[0] & 0xffc0) == 0xfe80,
  }
}

#[cfg(test)]
mod tests;
