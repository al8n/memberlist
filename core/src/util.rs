use crate::{
  checksum::Checksumer,
  types::{encoded_u32_len, Compress, MessageType},
};

use super::types::CompressionAlgo;

pub(crate) fn retransmit_limit(retransmit_mult: usize, n: usize) -> usize {
  let node_scale = ((n + 1) as f64).log10().ceil() as usize;
  retransmit_mult * node_scale
}

const LZW_LIT_WIDTH: u8 = 8;

#[derive(Debug, thiserror::Error)]
pub enum CompressError {
  #[error("{0}")]
  Lzw(#[from] weezl::LzwError),
}

#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
  #[error("{0}")]
  Lzw(#[from] weezl::LzwError),
}

pub(crate) fn compress_to_msg<C: Checksumer>(
  algo: CompressionAlgo,
  data: Bytes,
) -> Result<Bytes, CompressError> {
  let b = compress_payload(algo, data.as_ref())?;
  let compress = Compress {
    algo,
    buf: b.into(),
  };
  let basic_size = compress.encoded_len();
  let total_size = MessageType::SIZE + basic_size + encoded_u32_len(basic_size as u32);
  let mut b = BytesMut::with_capacity(total_size);
  b.put_u8(MessageType::Compress as u8);
  compress.encode_to::<C>(&mut b);
  Ok(b.freeze())
}

#[inline]
pub(crate) fn compress_payload(cmp: CompressionAlgo, inp: &[u8]) -> Result<Vec<u8>, CompressError> {
  match cmp {
    CompressionAlgo::Lzw => weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .encode(inp)
      .map_err(Into::into),
    CompressionAlgo::None => unreachable!(),
  }
}

#[inline]
pub(crate) fn decompress_payload(
  cmp: CompressionAlgo,
  inp: &[u8],
) -> Result<Vec<u8>, DecompressError> {
  match cmp {
    CompressionAlgo::Lzw => weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
      .decode(inp)
      .map_err(Into::into),
    CompressionAlgo::None => unreachable!(),
  }
}

#[cfg(feature = "metrics")]
pub(crate) mod label_serde {
  use std::{collections::HashMap, sync::Arc};

  use metrics::Label;
  use serde::{
    de::Deserializer,
    ser::{SerializeMap, Serializer},
    Deserialize,
  };

  pub fn serialize<S>(labels: &Arc<Vec<Label>>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    let mut ser = serializer.serialize_map(Some(labels.len()))?;
    for label in labels.iter() {
      ser.serialize_entry(label.key(), label.value())?;
    }
    ser.end()
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Vec<Label>>, D::Error>
  where
    D: Deserializer<'de>,
  {
    HashMap::<String, String>::deserialize(deserializer)
      .map(|map| Arc::new(map.into_iter().map(|(k, v)| Label::new(k, v)).collect()))
  }
}

use bytes::{BufMut, Bytes, BytesMut};
pub(crate) use is_global_ip::IsGlobalIp;

/// The code in this mod is copied from [libp2p]
///
/// [libp2p]: https://github.com/tcoratger/rust-libp2p/blob/master/core/src/transport/global_only.rs.
mod is_global_ip {
  pub(crate) trait IsGlobalIp {
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

pub(crate) use dns_util::read_resolv_conf;

#[cfg(not(target_family = "wasm"))]
mod dns_util {
  use std::{io, path::Path};

  use trust_dns_resolver::config::{ResolverConfig, ResolverOpts};

  pub(crate) fn read_resolv_conf<P: AsRef<Path>>(
    path: P,
  ) -> io::Result<(ResolverConfig, ResolverOpts)> {
    std::fs::read_to_string(path).and_then(trust_dns_resolver::system_conf::parse_resolv_conf)
  }
}

#[cfg(target_family = "wasm")]
mod dns_util {
  use std::{
    fs::File,
    io::{self, Read},
    net::SocketAddr,
    path::Path,
    time::Duration,
  };
  use trust_dns_resolver::{
    config::{NameServerConfig, Protocol, ResolverConfig, ResolverOpts},
    Name,
  };

  const DEFAULT_PORT: u16 = 53;

  pub(crate) fn read_resolv_conf<P: AsRef<Path>>(
    path: P,
  ) -> io::Result<(ResolverConfig, ResolverOpts)> {
    let mut data = String::new();
    let mut file = File::open(path)?;
    file.read_to_string(&mut data)?;
    parse_resolv_conf(&data)
  }

  fn parse_resolv_conf<T: AsRef<[u8]>>(data: T) -> io::Result<(ResolverConfig, ResolverOpts)> {
    let parsed_conf = resolv_conf::Config::parse(&data).map_err(|e| {
      io::Error::new(
        io::ErrorKind::Other,
        format!("Error parsing resolv.conf: {e}"),
      )
    })?;
    into_resolver_config(parsed_conf)
  }

  fn into_resolver_config(
    parsed_config: resolv_conf::Config,
  ) -> io::Result<(ResolverConfig, ResolverOpts)> {
    let domain = None;

    // nameservers
    let mut nameservers = Vec::<NameServerConfig>::with_capacity(parsed_config.nameservers.len());
    for ip in &parsed_config.nameservers {
      nameservers.push(NameServerConfig {
        socket_addr: SocketAddr::new(ip.into(), DEFAULT_PORT),
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_negative_responses: false,
        #[cfg(feature = "dns-over-rustls")]
        tls_config: None,
        bind_addr: None,
      });
      nameservers.push(NameServerConfig {
        socket_addr: SocketAddr::new(ip.into(), DEFAULT_PORT),
        protocol: Protocol::Tcp,
        tls_dns_name: None,
        trust_negative_responses: false,
        #[cfg(feature = "dns-over-rustls")]
        tls_config: None,
        bind_addr: None,
      });
    }
    if nameservers.is_empty() {
      tracing::warn!("no nameservers found in config");
    }

    // search
    let mut search = vec![];
    for search_domain in parsed_config.get_last_search_or_domain() {
      search.push(Name::from_str_relaxed(search_domain).map_err(|e| {
        io::Error::new(
          io::ErrorKind::Other,
          format!("Error parsing resolv.conf: {e}"),
        )
      })?);
    }

    let config = ResolverConfig::from_parts(domain, search, nameservers);

    let mut options = ResolverOpts::default();
    options.timeout = Duration::from_secs(parsed_config.timeout as u64);
    options.attempts = parsed_config.attempts as usize;
    options.ndots = parsed_config.ndots as usize;

    Ok((config, options))
  }
}
