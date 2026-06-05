//! Errors from constructing an [`Engine`](crate::Engine).

use core::fmt;

use memberlist_proto::EndpointInitError;

/// Why constructing an [`Engine`](crate::Engine) failed.
///
/// Every variant is a misconfiguration or environment fault reported in place
/// of a panic, so a caller assembling a [`Options`](crate::Options) and an
/// [`EndpointOptions`](memberlist_proto::EndpointOptions) from untrusted or
/// runtime values can recover. A concrete driver layers its own link-layer
/// construction errors (medium, interface addresses, socket buffers, …) on top
/// of these.
#[derive(Debug)]
#[non_exhaustive]
pub enum InitError {
  /// The configured advertise address
  /// ([`EndpointOptions::advertise_addr_ref`](memberlist_proto::EndpointOptions::advertise_addr_ref))
  /// is not a routable destination.
  ///
  /// A node must advertise an address its peers can route a reply to. An
  /// unspecified/multicast/broadcast IP or port 0 would be gossiped to the
  /// cluster and then be useless to every peer that selected it as an egress
  /// destination — the link layer rejects the unspecified address and port 0,
  /// and on stacks that assert during routing it can panic. The offending
  /// address is carried for diagnostics.
  NonRoutableAdvertiseAddr(core::net::SocketAddr),
  /// The advertised port does not match the bound port.
  ///
  /// The node binds one [`Options::port`](crate::Options::port) for both the
  /// gossip plane and the reliable listener (the single-port memberlist model).
  /// A direct embedded interface has no NAT, so a node is reachable only at the
  /// port it binds; its advertised port
  /// ([`EndpointOptions::advertise_addr_ref`](memberlist_proto::EndpointOptions::advertise_addr_ref))
  /// must equal it. Otherwise every peer routes to a port nothing is listening
  /// on.
  AdvertisePortMismatch,
  /// [`Options::port`](crate::Options::port) is zero.
  ///
  /// A link layer such as smoltcp rejects binding/listening on port 0, and no
  /// peer can dial it; the engine rejects it up front.
  ZeroPort,
  /// [`Options::close_timeout`](crate::Options::close_timeout) is zero.
  ///
  /// `close_timeout` bounds the graceful reliable-close drain: a connection
  /// still draining past `now + close_timeout` is force-aborted. A zero timeout
  /// sets that deadline to `now`, so every graceful close is force-aborted
  /// immediately — the drain never runs and an in-flight push/pull response is
  /// truncated. Must be non-zero.
  ZeroCloseTimeout,
  /// The configured gossip MTU's on-wire datagram cannot fit a UDP packet.
  ///
  /// A driver sizes its gossip arenas from `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`
  /// (the largest on-wire datagram the machine can emit). A `gossip_mtu` whose
  /// on-wire size exceeds the 65507-byte UDP payload limit could never be sent,
  /// and the unchecked arena arithmetic would overflow. The configured value
  /// and the effective ceiling are carried for diagnostics.
  GossipMtuTooLarge(GossipMtuTooLarge),
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

impl fmt::Display for InitError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      InitError::NonRoutableAdvertiseAddr(addr) => {
        write!(f, "advertise address {addr} is not a routable destination")
      }
      InitError::AdvertisePortMismatch => {
        f.write_str("advertised port does not match the bound port")
      }
      InitError::ZeroPort => f.write_str("port is zero"),
      InitError::ZeroCloseTimeout => f.write_str("close_timeout must be non-zero"),
      InitError::GossipMtuTooLarge(m) => write!(f, "{m}"),
      InitError::Endpoint(e) => write!(f, "SWIM endpoint initialization failed: {e}"),
      InitError::Encryption(e) => write!(f, "encryption configuration is unusable: {e}"),
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

#[cfg(all(test, feature = "std"))]
mod tests {
  use core::net::{IpAddr, Ipv4Addr, SocketAddr};
  use std::error::Error;

  use super::{GossipMtuTooLarge, InitError};

  #[test]
  fn every_init_error_variant_displays_and_reports_its_source() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    // (variant, whether `source()` should be `Some`).
    let cases: [(InitError, bool); 7] = [
      (InitError::NonRoutableAdvertiseAddr(addr), false),
      (InitError::AdvertisePortMismatch, false),
      (InitError::ZeroPort, false),
      (InitError::ZeroCloseTimeout, false),
      (
        InitError::GossipMtuTooLarge(GossipMtuTooLarge {
          gossip_mtu: 70_000,
          ceiling: 65_000,
        }),
        false,
      ),
      (memberlist_proto::EndpointInitError::Entropy.into(), true),
      (memberlist_proto::EncryptionError::AuthFailed.into(), true),
    ];
    for (err, has_source) in cases {
      assert!(!err.to_string().is_empty(), "Display non-empty for {err:?}");
      assert!(!format!("{err:?}").is_empty(), "Debug non-empty");
      assert_eq!(
        err.source().is_some(),
        has_source,
        "source presence for {err:?}"
      );
    }
  }

  #[test]
  fn gossip_mtu_too_large_display_carries_both_values() {
    let m = GossipMtuTooLarge {
      gossip_mtu: 70_000,
      ceiling: 65_000,
    };
    let shown = m.to_string();
    assert!(
      shown.contains("70000") && shown.contains("65000"),
      "got {shown}"
    );
    assert_eq!(
      m,
      GossipMtuTooLarge {
        gossip_mtu: 70_000,
        ceiling: 65_000
      }
    );
  }
}
