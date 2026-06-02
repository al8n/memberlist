//! Errors from constructing a [`Memberlist`](crate::Memberlist).

use core::fmt;

use memberlist_proto::EndpointInitError;

use crate::interface::{HardwareAddress, IpCidr, Medium, Route};

/// Why constructing a [`Memberlist`](crate::Memberlist) failed.
///
/// Every variant is a misconfiguration or environment fault reported in place
/// of a panic, so a caller assembling an [`InterfaceConfig`](crate::InterfaceConfig)
/// from untrusted or runtime values can recover.
#[derive(Debug)]
#[non_exhaustive]
pub enum InitError {
  /// The configured hardware address's medium does not match the bound device.
  ///
  /// smoltcp's `Interface::new` would otherwise panic; the driver checks first
  /// and returns this instead.
  MediumMismatch(MediumMismatch),
  /// The configured hardware address selects a medium this driver does not
  /// support (smoltcp's `Ieee802154`).
  ///
  /// The driver enables only `medium-ip` and `medium-ethernet`; an
  /// `Ieee802154` hardware address — constructible only when a downstream crate
  /// unifies smoltcp's `medium-ieee802154` feature on — would otherwise reach an
  /// `unreachable!()` while deriving the medium. The driver rejects it here.
  UnsupportedMedium,
  /// The configured Ethernet hardware address is not unicast.
  ///
  /// smoltcp's `Interface::new` stores the configured hardware address without
  /// validating it (it never calls `check_hardware_addr`), so a
  /// broadcast/multicast MAC would not panic but would install an invalid L2
  /// source/acceptance identity. The driver rejects it instead. The offending
  /// address is carried for diagnostics.
  NonUnicastHardwareAddress(HardwareAddress),
  /// A configured IP CIDR's address is neither unicast nor unspecified.
  ///
  /// smoltcp's `update_ip_addrs` calls `check_ip_addrs`, which `panic!`s on an
  /// address that is not unicast and not unspecified (e.g. multicast or the
  /// limited broadcast address). The driver mirrors that exact condition and
  /// returns this instead of panicking. The offending CIDR is carried for
  /// diagnostics.
  NonUnicastIpAddress(IpCidr),
  /// The configured advertise address
  /// ([`EndpointConfig::advertise_addr_ref`](memberlist_proto::EndpointConfig::advertise_addr_ref))
  /// is not a routable destination.
  ///
  /// A node must advertise an address its peers can route a reply to. An
  /// unspecified/multicast/broadcast IP or port 0 would be gossiped to the
  /// cluster and then be useless to every peer that selected it as an egress
  /// destination — smoltcp's socket layer rejects the unspecified address and
  /// port 0 (`Unaddressable`) and the route / neighbor lookup asserts the address
  /// is unicast. The offending address is carried for diagnostics.
  NonRoutableAdvertiseAddr(core::net::SocketAddr),
  /// [`InterfaceConfig::ip_addrs`](crate::InterfaceConfig::ip_addrs) was empty,
  /// so the interface would accept no packets.
  MissingIpAddress,
  /// More IP addresses were configured than smoltcp's interface capacity
  /// (`IFACE_MAX_ADDR_COUNT`).
  TooManyIpAddresses,
  /// More routes were configured than smoltcp's route-table capacity
  /// (`IFACE_MAX_ROUTE_COUNT`).
  TooManyRoutes,
  /// [`Config::port`](crate::Config::port) is zero.
  ///
  /// smoltcp's `udp::Socket::bind` and `tcp::Socket::listen` reject port 0
  /// (`Unaddressable`), which would otherwise panic inside the fallible
  /// constructor; the driver rejects it up front instead.
  ZeroPort,
  /// The advertised port does not match the bound port.
  ///
  /// The node binds one [`Config::port`](crate::Config::port) for both the
  /// gossip UDP socket and the reliable TCP listener (the single-port memberlist
  /// model). On a direct smoltcp interface (no NAT) a node is reachable only at
  /// the port it binds, so its advertised port
  /// ([`EndpointConfig::advertise_addr_ref`](memberlist_proto::EndpointConfig::advertise_addr_ref))
  /// must equal it. Otherwise every peer routes to a port nothing is listening on.
  AdvertisePortMismatch,
  /// The advertised IP is not one assigned to the interface.
  ///
  /// smoltcp drops any inbound packet whose destination is not an assigned
  /// interface address (the `has_ip_addr` gate in `process_ipv4`/`process_ipv6`),
  /// so a node advertising an IP absent from
  /// [`InterfaceConfig::ip_addrs`](crate::InterfaceConfig::ip_addrs) is
  /// unreachable on both planes — peers gossip and dial an address its own
  /// interface discards. The advertised IP must exactly match a configured
  /// interface address. The offending advertise address is carried for
  /// diagnostics.
  AdvertiseAddrNotLocal(core::net::SocketAddr),
  /// A configured route's gateway (`via_router`) is not unicast.
  ///
  /// On Ethernet egress smoltcp resolves an off-link next hop through its
  /// neighbor cache, whose `lookup` asserts the protocol address is unicast (a
  /// release-mode `assert!`). A multicast/broadcast/unspecified `via_router`
  /// would pass construction and then crash the running node at first off-link
  /// egress. The offending route is carried for diagnostics.
  NonUnicastRouteGateway(Route),
  /// A configured route's prefix and gateway are different IP families.
  ///
  /// A route to an IPv4 prefix via an IPv6 gateway (or the reverse) can never
  /// resolve a next hop; it is rejected here rather than silently failing at
  /// egress. The offending route is carried for diagnostics.
  RouteFamilyMismatch(Route),
  /// Drawing a random seed from the system entropy source failed (only when no
  /// seed was pinned via
  /// [`InterfaceConfig::with_random_seed`](crate::InterfaceConfig::with_random_seed)).
  Entropy,
  /// The SWIM machine endpoint failed to initialize.
  Endpoint(EndpointInitError),
  /// The configured encryption keyring cannot be used by this build.
  ///
  /// Construction probes every configured key (primary then secondaries) by
  /// encrypting an empty frame. A key whose AEAD backend was not compiled into
  /// this binary surfaces here as
  /// [`EncryptionError::UnsupportedAlgorithm`](memberlist_proto::EncryptionError::UnsupportedAlgorithm),
  /// turning what would otherwise be a silent runtime drop of every encrypted
  /// gossip datagram into a typed construction error.
  Encryption(memberlist_proto::EncryptionError),
  /// The configured gossip MTU's on-wire datagram cannot fit a UDP packet.
  ///
  /// The driver sizes its UDP arenas from `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`
  /// (the largest on-wire datagram the machine can emit). A `gossip_mtu` whose
  /// on-wire size exceeds the 65507-byte UDP payload limit could never be sent,
  /// and the unchecked arena arithmetic would overflow (panic in a checked build,
  /// wrap to an undersized arena in release). The configured value and the
  /// effective ceiling are carried for diagnostics.
  GossipMtuTooLarge(GossipMtuTooLarge),
  /// A UDP arena byte count (`udp_*_packets × max on-wire datagram`) overflows
  /// `usize`.
  ///
  /// Reachable on a 32-bit target with an astronomically large UDP packet-slot
  /// count: the product would wrap to an undersized arena. Rejected here rather
  /// than allocating a silently-too-small buffer.
  UdpArenaTooLarge,
  /// [`Config::tcp_pool_size`](crate::Config::tcp_pool_size) is below 2.
  ///
  /// Construction dedicates one pooled TCP socket to the inbound listener and
  /// uses the rest for dials and accepts. With 0 there is no listener (no
  /// reliable plane at all); with 1 the listener consumes the only socket,
  /// leaving none to dial — so the node can never dial a seed to join and can
  /// accept at most one inbound at a time. The functional minimum is 2 (a
  /// listener plus one dial/accept socket).
  TcpPoolTooSmall,
  /// [`Config::tcp_socket_rx_bytes`](crate::Config::tcp_socket_rx_bytes) or
  /// [`Config::tcp_socket_tx_bytes`](crate::Config::tcp_socket_tx_bytes) is zero.
  ///
  /// smoltcp's `RingBuffer::new` does not panic on zero-length storage — it
  /// builds a permanently-empty ring — so a 0-byte rx (or tx) buffer is a socket
  /// that can never receive (or send): a silently-dead reliable plane. Both must
  /// be non-zero.
  ZeroTcpSocketBuffer,
  /// [`Config::tcp_socket_rx_bytes`](crate::Config::tcp_socket_rx_bytes) exceeds
  /// smoltcp's 1 GiB receive-buffer limit.
  ///
  /// smoltcp's `tcp::Socket::new` `panic!`s when the receive-buffer capacity
  /// exceeds 1 GiB (`> 1 << 30`); a caller-supplied `tcp_socket_rx_bytes` past
  /// that limit would panic inside this fallible constructor. The transmit
  /// buffer has no such limit. Rejected here instead.
  TcpRxBufferTooLarge,
  /// [`Config::udp_rx_packets`](crate::Config::udp_rx_packets) or
  /// [`Config::udp_tx_packets`](crate::Config::udp_tx_packets) is zero.
  ///
  /// The gossip `udp::PacketBuffer` is built with that many per-packet metadata
  /// slots. Zero slots is a ring that can never enqueue or dequeue a datagram,
  /// so gossip can never be received (or sent): a silently-dead gossip plane.
  /// Both must be non-zero.
  ZeroUdpPackets,
  /// [`Config::close_timeout`](crate::Config::close_timeout) is zero.
  ///
  /// `close_timeout` bounds the graceful reliable-close drain: a connection
  /// still `Closing` past `now + close_timeout` is force-aborted. A zero timeout
  /// sets that deadline to `now`, so every graceful close is force-aborted
  /// immediately — the drain never runs and an in-flight push/pull response is
  /// truncated. Must be non-zero.
  ZeroCloseTimeout,
}

/// The configured gossip MTU exceeds the largest plaintext payload whose on-wire
/// datagram still fits a UDP packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GossipMtuTooLarge {
  /// The configured `gossip_mtu` that was rejected.
  pub gossip_mtu: usize,
  /// The largest acceptable `gossip_mtu`: `65507 - ENCRYPTED_WRAPPER_OVERHEAD`.
  pub ceiling: usize,
}

impl fmt::Display for GossipMtuTooLarge {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "gossip_mtu {} exceeds the maximum sendable plaintext payload of {} bytes \
       (the on-wire datagram must fit the 65507-byte UDP payload limit)",
      self.gossip_mtu, self.ceiling
    )
  }
}

/// The configured hardware-address medium did not match the bound device's
/// medium.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediumMismatch {
  /// The medium implied by the configured [`HardwareAddress`](crate::HardwareAddress).
  pub expected: Medium,
  /// The medium the bound device actually reports.
  pub actual: Medium,
}

impl fmt::Display for MediumMismatch {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "hardware address medium {:?} does not match device medium {:?}",
      self.expected, self.actual
    )
  }
}

impl fmt::Display for InitError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      InitError::MediumMismatch(m) => write!(f, "{m}"),
      InitError::UnsupportedMedium => f.write_str(
        "configured hardware address selects a medium this driver does not support \
         (only IP and Ethernet are supported)",
      ),
      InitError::NonUnicastHardwareAddress(addr) => {
        write!(f, "hardware address {addr} is not unicast")
      }
      InitError::NonUnicastIpAddress(cidr) => {
        write!(f, "IP address {} is not unicast", cidr.address())
      }
      InitError::NonRoutableAdvertiseAddr(addr) => {
        write!(f, "advertise address {addr} is not a routable destination")
      }
      InitError::MissingIpAddress => {
        f.write_str("no IP address configured; the interface would accept no packets")
      }
      InitError::TooManyIpAddresses => {
        f.write_str("more IP addresses configured than the smoltcp interface can hold")
      }
      InitError::TooManyRoutes => {
        f.write_str("more routes configured than the smoltcp route table can hold")
      }
      InitError::ZeroPort => f.write_str("port is zero (smoltcp rejects port 0)"),
      InitError::AdvertisePortMismatch => {
        f.write_str("advertised port does not match the bound port")
      }
      InitError::AdvertiseAddrNotLocal(addr) => {
        write!(
          f,
          "advertised IP {} is not assigned to the interface",
          addr.ip()
        )
      }
      InitError::NonUnicastRouteGateway(route) => {
        write!(f, "route gateway {} is not unicast", route.via_router)
      }
      InitError::RouteFamilyMismatch(route) => write!(
        f,
        "route prefix {} and gateway {} are different IP families",
        route.cidr, route.via_router
      ),
      InitError::Entropy => f.write_str("system entropy source failed while drawing a random seed"),
      InitError::Endpoint(e) => write!(f, "SWIM endpoint initialization failed: {e}"),
      InitError::Encryption(e) => write!(f, "encryption configuration is unusable: {e}"),
      InitError::GossipMtuTooLarge(m) => write!(f, "{m}"),
      InitError::UdpArenaTooLarge => {
        f.write_str("UDP arena byte count (packets × max datagram) overflows usize")
      }
      InitError::TcpPoolTooSmall => {
        f.write_str("tcp_pool_size must be at least 2 (a listener plus one dial/accept socket)")
      }
      InitError::ZeroTcpSocketBuffer => {
        f.write_str("tcp_socket_rx_bytes and tcp_socket_tx_bytes must be non-zero")
      }
      InitError::TcpRxBufferTooLarge => {
        f.write_str("tcp_socket_rx_bytes exceeds smoltcp's 1 GiB receive-buffer limit")
      }
      InitError::ZeroUdpPackets => {
        f.write_str("udp_rx_packets and udp_tx_packets must be non-zero")
      }
      InitError::ZeroCloseTimeout => f.write_str("close_timeout must be non-zero"),
    }
  }
}

impl InitError {
  /// Map an [`Engine`](memberlist_embedded::Engine) construction error into the
  /// driver's [`InitError`].
  ///
  /// The driver pre-validates the port, gossip MTU, close timeout, and advertise
  /// address before building the engine, so in practice the engine fails only with
  /// [`Endpoint`](memberlist_embedded::InitError::Endpoint) (machine init) or
  /// [`Encryption`](memberlist_embedded::InitError::Encryption) (an unusable
  /// keyring). The remaining variants are mapped to their driver equivalents
  /// anyway so the conversion is total and stays correct if the driver's
  /// pre-checks are ever reordered or relaxed.
  pub(crate) fn from_embedded(e: memberlist_embedded::InitError) -> Self {
    use memberlist_embedded::InitError as E;
    match e {
      E::NonRoutableAdvertiseAddr(addr) => InitError::NonRoutableAdvertiseAddr(addr),
      E::AdvertisePortMismatch => InitError::AdvertisePortMismatch,
      E::ZeroPort => InitError::ZeroPort,
      E::ZeroCloseTimeout => InitError::ZeroCloseTimeout,
      E::GossipMtuTooLarge(m) => InitError::GossipMtuTooLarge(GossipMtuTooLarge {
        gossip_mtu: m.gossip_mtu,
        ceiling: m.ceiling,
      }),
      E::Endpoint(inner) => InitError::Endpoint(inner),
      E::Encryption(inner) => InitError::Encryption(inner),
      // `memberlist_embedded::InitError` is `#[non_exhaustive]`, so a wildcard is
      // required even though every variant it defines today is handled above. A
      // future engine-only failure mode reaching here surfaces as a generic endpoint
      // init failure (the sole `EndpointInitError` variant) and would warrant its own
      // driver variant when added.
      _ => InitError::Endpoint(EndpointInitError::Entropy),
    }
  }
}

impl From<EndpointInitError> for InitError {
  fn from(e: EndpointInitError) -> Self {
    InitError::Endpoint(e)
  }
}

impl From<memberlist_proto::EncryptionError> for InitError {
  fn from(e: memberlist_proto::EncryptionError) -> Self {
    InitError::Encryption(e)
  }
}

#[cfg(feature = "std")]
impl std::error::Error for InitError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      InitError::Endpoint(e) => Some(e),
      InitError::Encryption(e) => Some(e),
      _ => None,
    }
  }
}
