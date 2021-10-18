pub use is_global_ip::IsGlobalIp;

#[cfg(feature = "ipnet")]
pub use cidr::*;

#[cfg(feature = "ipnet")]
mod cidr {
  pub use ipnet::*;
  use std::{collections::HashSet, net::IpAddr, str::FromStr};
  /// Classless Inter-Domain Routing (CIDR) policy.
  #[derive(Debug, Clone, PartialEq, Eq)]
  #[repr(transparent)]
  #[cfg(feature = "ipnet")]
  #[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  pub struct CIDRsPolicy {
    allowed_cidrs: Option<HashSet<IpNet>>,
  }

  #[cfg(feature = "ipnet")]
  impl Default for CIDRsPolicy {
    fn default() -> Self {
      Self::allow_all()
    }
  }

  #[cfg(feature = "ipnet")]
  impl CIDRsPolicy {
    /// Allow connection from any [`IpNet`].
    #[inline(always)]
    pub const fn allow_all() -> Self {
      Self {
        allowed_cidrs: None,
      }
    }

    /// Block connection from any [`IpNet`].
    #[inline]
    pub fn block_all() -> Self {
      Self {
        allowed_cidrs: Some(HashSet::new()),
      }
    }

    /// Create a default [`CIDRsPolicy`].
    #[inline(always)]
    pub const fn new() -> Self {
      Self::allow_all()
    }

    /// Add a new [`IpNet`] to the list of allowed [`IpNet`].
    pub fn add(&mut self, ip: IpNet) {
      self
        .allowed_cidrs
        .get_or_insert_with(HashSet::new)
        .insert(ip);
    }

    /// Remove an [`IpNet`] from the list of allowed [`IpNet`].
    pub fn remove(&mut self, ip: &IpNet) {
      if let Some(allowed_cidrs) = self.allowed_cidrs.as_mut() {
        allowed_cidrs.remove(ip);
      }
    }

    /// Remove an [`IpNet`]s from the list of allowed [`IpNet`] by [`IpAddr`].
    pub fn remove_by_ip(&mut self, ip: &IpAddr) {
      if let Some(allowed_cidrs) = self.allowed_cidrs.as_mut() {
        allowed_cidrs.retain(|cidr| !cidr.contains(ip));
      }
    }

    /// Returns an iterator over the allowed [`IpNet`]s.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
      self
        .allowed_cidrs
        .as_ref()
        .into_iter()
        .flat_map(|x| x.iter())
    }

    /// Reports whether the network includes ip.
    pub fn contains(&self, ip: &IpAddr) -> bool {
      self
        .allowed_cidrs
        .as_ref()
        .map_or(true, |x| x.contains(&IpNet::from(*ip)))
    }

    /// Returns `true` if the [`IpAddr`] is allowed.
    pub fn is_allowed(&self, ip: &IpAddr) -> bool {
      self
        .allowed_cidrs
        .as_ref()
        .map_or(true, |x| x.iter().any(|cidr| cidr.contains(ip)))
    }

    /// Returns `true` if the [`IpAddr`] is blocked.
    pub fn is_blocked(&self, ip: &IpAddr) -> bool {
      !self.is_allowed(ip)
    }

    /// Returns `true` connection from any IP is blocked.
    pub fn is_block_all(&self) -> bool {
      self.allowed_cidrs.as_ref().map_or(false, |x| x.is_empty())
    }

    /// Returns `true` connection from any IP is allowed.
    pub fn is_allow_all(&self) -> bool {
      self.allowed_cidrs.is_none()
    }

    /// Try to parse a [`CIDRsPolicy`] from an iter.
    pub fn try_from_iter<A: TryInto<IpNet, Error = AddrParseError>, I: Iterator<Item = A>>(
      iter: I,
    ) -> Result<Self, AddrParseError> {
      let mut allowed_cidrs = HashSet::new();
      for ip in iter {
        allowed_cidrs.insert(ip.try_into()?);
      }
      Ok(Self {
        allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
      })
    }
  }

  #[cfg(feature = "ipnet")]
  impl From<HashSet<IpNet>> for CIDRsPolicy {
    fn from(allowed_cidrs: HashSet<IpNet>) -> Self {
      Self {
        allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
      }
    }
  }

  #[cfg(feature = "ipnet")]
  impl<A: Into<IpNet>> FromIterator<A> for CIDRsPolicy {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
      let mut allowed_cidrs = HashSet::new();
      for ip in iter {
        allowed_cidrs.insert(ip.into());
      }
      Self {
        allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
      }
    }
  }

  #[cfg(feature = "ipnet")]
  impl<'a, A: AsRef<str>> TryFrom<&'a [A]> for CIDRsPolicy {
    type Error = AddrParseError;

    fn try_from(iter: &'a [A]) -> Result<Self, Self::Error> {
      let mut allowed_cidrs = HashSet::new();
      for ip in iter {
        allowed_cidrs.insert(IpNet::from_str(ip.as_ref().trim())?);
      }
      Ok(Self {
        allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
      })
    }
  }

  #[cfg(feature = "ipnet")]
  impl<A: TryInto<IpNet, Error = AddrParseError>> TryFrom<Vec<A>> for CIDRsPolicy {
    type Error = AddrParseError;

    fn try_from(iter: Vec<A>) -> Result<Self, Self::Error> {
      let mut allowed_cidrs = HashSet::new();
      for ip in iter {
        allowed_cidrs.insert(ip.try_into()?);
      }
      Ok(Self {
        allowed_cidrs: (!allowed_cidrs.is_empty()).then_some(allowed_cidrs),
      })
    }
  }
}

/// The code in this mod is copied from [libp2p]
///
/// [libp2p]: https://github.com/tcoratger/rust-libp2p/blob/master/core/src/transport/global_only.rs.
mod is_global_ip {
  /// A trait for checking if an IP address is globally reachable.
  pub trait IsGlobalIp {
    /// Returns [`true`] if the address appears to be globally reachable
    fn is_global_ip(&self) -> bool;
  }

  impl IsGlobalIp for std::net::Ipv4Addr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      ipv4_global::is_global(*self)
    }
  }

  impl IsGlobalIp for std::net::Ipv6Addr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      ipv6_global::is_global(*self)
    }
  }

  impl IsGlobalIp for std::net::IpAddr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      match self {
        std::net::IpAddr::V4(addr) => addr.is_global_ip(),
        std::net::IpAddr::V6(addr) => addr.is_global_ip(),
      }
    }
  }

  impl IsGlobalIp for std::net::SocketAddrV4 {
    #[inline]
    fn is_global_ip(&self) -> bool {
      self.ip().is_global_ip()
    }
  }

  impl IsGlobalIp for std::net::SocketAddrV6 {
    #[inline]
    fn is_global_ip(&self) -> bool {
      self.ip().is_global_ip()
    }
  }

  impl IsGlobalIp for std::net::SocketAddr {
    #[inline]
    fn is_global_ip(&self) -> bool {
      self.ip().is_global_ip()
    }
  }

  /// This module contains an implementation of the `is_global` IPv4 address space.
  ///
  /// Credit for this implementation goes to the Rust standard library team.
  ///
  /// Unstable tracking issue: [#27709](https://github.com/rust-lang/rust/issues/27709)
  mod ipv4_global {
    use std::net::Ipv4Addr;

    /// Returns [`true`] if this address is reserved by IANA for future use. [IETF RFC 1112]
    /// defines the block of reserved addresses as `240.0.0.0/4`. This range normally includes the
    /// broadcast address `255.255.255.255`, but this implementation explicitly excludes it, since
    /// it is obviously not reserved for future use.
    ///
    /// [IETF RFC 1112]: https://tools.ietf.org/html/rfc1112
    ///
    /// # Warning
    ///
    /// As IANA assigns new addresses, this method will be
    /// updated. This may result in non-reserved addresses being
    /// treated as reserved in code that relies on an outdated version
    /// of this method.
    #[must_use]
    #[inline]
    const fn is_reserved(a: Ipv4Addr) -> bool {
      a.octets()[0] & 240 == 240 && !a.is_broadcast()
    }

    /// Returns [`true`] if this address part of the `198.18.0.0/15` range, which is reserved for
    /// network devices benchmarking. This range is defined in [IETF RFC 2544] as `192.18.0.0`
    /// through `198.19.255.255` but [errata 423] corrects it to `198.18.0.0/15`.
    ///
    /// [IETF RFC 2544]: https://tools.ietf.org/html/rfc2544
    /// [errata 423]: https://www.rfc-editor.org/errata/eid423
    #[must_use]
    #[inline]
    const fn is_benchmarking(a: Ipv4Addr) -> bool {
      a.octets()[0] == 198 && (a.octets()[1] & 0xfe) == 18
    }

    /// Returns [`true`] if this address is part of the Shared Address Space defined in
    /// [IETF RFC 6598] (`100.64.0.0/10`).
    ///
    /// [IETF RFC 6598]: https://tools.ietf.org/html/rfc6598
    #[must_use]
    #[inline]
    const fn is_shared(a: Ipv4Addr) -> bool {
      a.octets()[0] == 100 && (a.octets()[1] & 0b1100_0000 == 0b0100_0000)
    }

    /// Returns [`true`] if this is a private address.
    ///
    /// The private address ranges are defined in [IETF RFC 1918] and include:
    ///
    ///  - `10.0.0.0/8`
    ///  - `172.16.0.0/12`
    ///  - `192.168.0.0/16`
    ///
    /// [IETF RFC 1918]: https://tools.ietf.org/html/rfc1918
    #[must_use]
    #[inline]
    const fn is_private(a: Ipv4Addr) -> bool {
      match a.octets() {
        [10, ..] => true,
        [172, b, ..] if b >= 16 && b <= 31 => true,
        [192, 168, ..] => true,
        _ => false,
      }
    }

    /// Returns [`true`] if the address appears to be globally reachable
    /// as specified by the [IANA IPv4 Special-Purpose Address Registry].
    /// Whether or not an address is practically reachable will depend on your network configuration.
    ///
    /// Most IPv4 addresses are globally reachable;
    /// unless they are specifically defined as *not* globally reachable.
    ///
    /// Non-exhaustive list of notable addresses that are not globally reachable:
    ///
    /// - The [unspecified address] ([`is_unspecified`](Ipv4Addr::is_unspecified))
    /// - Addresses reserved for private use ([`is_private`](Ipv4Addr::is_private))
    /// - Addresses in the shared address space ([`is_shared`](Ipv4Addr::is_shared))
    /// - Loopback addresses ([`is_loopback`](Ipv4Addr::is_loopback))
    /// - Link-local addresses ([`is_link_local`](Ipv4Addr::is_link_local))
    /// - Addresses reserved for documentation ([`is_documentation`](Ipv4Addr::is_documentation))
    /// - Addresses reserved for benchmarking ([`is_benchmarking`](Ipv4Addr::is_benchmarking))
    /// - Reserved addresses ([`is_reserved`](Ipv4Addr::is_reserved))
    /// - The [broadcast address] ([`is_broadcast`](Ipv4Addr::is_broadcast))
    ///
    /// For the complete overview of which addresses are globally reachable, see the table at the [IANA IPv4 Special-Purpose Address Registry].
    ///
    /// [IANA IPv4 Special-Purpose Address Registry]: https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml
    /// [unspecified address]: Ipv4Addr::UNSPECIFIED
    /// [broadcast address]: Ipv4Addr::BROADCAST
    #[must_use]
    #[inline]
    pub(crate) const fn is_global(a: Ipv4Addr) -> bool {
      !(a.octets()[0] == 0 // "This network"
          || is_private(a)
          || is_shared(a)
          || a.is_loopback()
          || a.is_link_local()
          // addresses reserved for future protocols (`192.0.0.0/24`)
          ||(a.octets()[0] == 192 && a.octets()[1] == 0 && a.octets()[2] == 0)
          || a.is_documentation()
          || is_benchmarking(a)
          || is_reserved(a)
          || a.is_broadcast())
    }
  }

  /// This module contains an implementation of the `is_global` IPv6 address space.
  ///
  /// Credit for this implementation goes to the Rust standard library team.
  ///
  /// Unstable tracking issue: [#27709](https://github.com/rust-lang/rust/issues/27709)
  mod ipv6_global {
    use std::net::Ipv6Addr;

    /// Returns `true` if the address is a unicast address with link-local scope,
    /// as defined in [RFC 4291].
    ///
    /// A unicast address has link-local scope if it has the prefix `fe80::/10`, as per [RFC 4291 section 2.4].
    /// Note that this encompasses more addresses than those defined in [RFC 4291 section 2.5.6],
    /// which describes "Link-Local IPv6 Unicast Addresses" as having the following stricter format:
    ///
    /// ```text
    /// | 10 bits  |         54 bits         |          64 bits           |
    /// +----------+-------------------------+----------------------------+
    /// |1111111010|           0             |       interface ID         |
    /// +----------+-------------------------+----------------------------+
    /// ```
    /// So while currently the only addresses with link-local scope an application will encounter are all in `fe80::/64`,
    /// this might change in the future with the publication of new standards. More addresses in `fe80::/10` could be allocated,
    /// and those addresses will have link-local scope.
    ///
    /// Also note that while [RFC 4291 section 2.5.3] mentions about the [loopback address] (`::1`) that "it is treated as having Link-Local scope",
    /// this does not mean that the loopback address actually has link-local scope and this method will return `false` on it.
    ///
    /// [RFC 4291]: https://tools.ietf.org/html/rfc4291
    /// [RFC 4291 section 2.4]: https://tools.ietf.org/html/rfc4291#section-2.4
    /// [RFC 4291 section 2.5.3]: https://tools.ietf.org/html/rfc4291#section-2.5.3
    /// [RFC 4291 section 2.5.6]: https://tools.ietf.org/html/rfc4291#section-2.5.6
    /// [loopback address]: Ipv6Addr::LOCALHOST
    #[must_use]
    #[inline]
    const fn is_unicast_link_local(a: Ipv6Addr) -> bool {
      (a.segments()[0] & 0xffc0) == 0xfe80
    }

    /// Returns [`true`] if this is a unique local address (`fc00::/7`).
    ///
    /// This property is defined in [IETF RFC 4193].
    ///
    /// [IETF RFC 4193]: https://tools.ietf.org/html/rfc4193
    #[must_use]
    #[inline]
    const fn is_unique_local(a: Ipv6Addr) -> bool {
      (a.segments()[0] & 0xfe00) == 0xfc00
    }

    /// Returns [`true`] if this is an address reserved for documentation
    /// (`2001:db8::/32`).
    ///
    /// This property is defined in [IETF RFC 3849].
    ///
    /// [IETF RFC 3849]: https://tools.ietf.org/html/rfc3849
    #[must_use]
    #[inline]
    const fn is_documentation(a: Ipv6Addr) -> bool {
      (a.segments()[0] == 0x2001) && (a.segments()[1] == 0xdb8)
    }

    /// Returns [`true`] if the address appears to be globally reachable
    /// as specified by the [IANA IPv6 Special-Purpose Address Registry].
    /// Whether or not an address is practically reachable will depend on your network configuration.
    ///
    /// Most IPv6 addresses are globally reachable;
    /// unless they are specifically defined as *not* globally reachable.
    ///
    /// Non-exhaustive list of notable addresses that are not globally reachable:
    /// - The [unspecified address] ([`is_unspecified`](Ipv6Addr::is_unspecified))
    /// - The [loopback address] ([`is_loopback`](Ipv6Addr::is_loopback))
    /// - IPv4-mapped addresses
    /// - Addresses reserved for benchmarking
    /// - Addresses reserved for documentation ([`is_documentation`](Ipv6Addr::is_documentation))
    /// - Unique local addresses ([`is_unique_local`](Ipv6Addr::is_unique_local))
    /// - Unicast addresses with link-local scope ([`is_unicast_link_local`](Ipv6Addr::is_unicast_link_local))
    ///
    /// For the complete overview of which addresses are globally reachable, see the table at the [IANA IPv6 Special-Purpose Address Registry].
    ///
    /// Note that an address having global scope is not the same as being globally reachable,
    /// and there is no direct relation between the two concepts: There exist addresses with global scope
    /// that are not globally reachable (for example unique local addresses),
    /// and addresses that are globally reachable without having global scope
    /// (multicast addresses with non-global scope).
    ///
    /// [IANA IPv6 Special-Purpose Address Registry]: https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml
    /// [unspecified address]: Ipv6Addr::UNSPECIFIED
    /// [loopback address]: Ipv6Addr::LOCALHOST
    #[must_use]
    #[inline]
    pub(crate) const fn is_global(a: Ipv6Addr) -> bool {
      !(a.is_unspecified()
          || a.is_loopback()
          // IPv4-mapped Address (`::ffff:0:0/96`)
          || matches!(a.segments(), [0, 0, 0, 0, 0, 0xffff, _, _])
          // IPv4-IPv6 Translat. (`64:ff9b:1::/48`)
          || matches!(a.segments(), [0x64, 0xff9b, 1, _, _, _, _, _])
          // Discard-Only Address Block (`100::/64`)
          || matches!(a.segments(), [0x100, 0, 0, 0, _, _, _, _])
          // IETF Protocol Assignments (`2001::/23`)
          || (matches!(a.segments(), [0x2001, b, _, _, _, _, _, _] if b < 0x200)
              && !(
                  // Port Control Protocol Anycast (`2001:1::1`)
                  u128::from_be_bytes(a.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0001
                  // Traversal Using Relays around NAT Anycast (`2001:1::2`)
                  || u128::from_be_bytes(a.octets()) == 0x2001_0001_0000_0000_0000_0000_0000_0002
                  // AMT (`2001:3::/32`)
                  || matches!(a.segments(), [0x2001, 3, _, _, _, _, _, _])
                  // AS112-v6 (`2001:4:112::/48`)
                  || matches!(a.segments(), [0x2001, 4, 0x112, _, _, _, _, _])
                  // ORCHIDv2 (`2001:20::/28`)
                  || matches!(a.segments(), [0x2001, b, _, _, _, _, _, _] if b >= 0x20 && b <= 0x2F)
              ))
          || is_documentation(a)
          || is_unique_local(a)
          || is_unicast_link_local(a))
    }
  }
}
