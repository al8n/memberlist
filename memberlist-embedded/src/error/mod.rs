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
  /// A driver sizes its gossip arenas from
  /// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD`
  /// (the largest on-wire datagram the machine can emit). A `gossip_mtu` whose
  /// on-wire size exceeds the 65507-byte UDP payload limit could never be sent,
  /// and the unchecked arena arithmetic would overflow. The configured value
  /// and the effective ceiling are carried for diagnostics.
  GossipMtuTooLarge(GossipMtuTooLarge),
  /// The SWIM machine endpoint failed to initialize.
  Endpoint(EndpointInitError),
  /// The configured encryption keyring cannot be used by this build.
  ///
  /// Construction and runtime rotation probe every configured key (primary then
  /// secondaries) entropy-free. A key whose AEAD backend was not compiled into
  /// this binary surfaces here as
  /// [`EncryptionError::UnsupportedAlgorithm`](memberlist_proto::EncryptionError::UnsupportedAlgorithm),
  /// and a key whose cipher variant disagrees with its algorithm tag as
  /// [`KeyMismatch`](memberlist_proto::EncryptionError::KeyMismatch), turning what
  /// would otherwise be a silent runtime drop of every encrypted gossip datagram
  /// into a typed construction error.
  ///
  /// The probe validates only this PERMANENT usability — it does not validate the
  /// per-send nonce source. Encryption is cross-transport (gossip datagrams and the
  /// plaintext reliable plane), and every encrypted frame draws a fresh nonce from
  /// `getrandom` at send time, so on a target whose backend is missing or failing
  /// the node still constructs and then cannot encrypt outbound traffic — gossip
  /// datagrams and reliable exchanges alike fail as they are sent.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  Encryption(memberlist_proto::EncryptionError),
  /// The configured gossip checksum algorithm cannot be used by this build.
  ///
  /// A checksum algorithm whose backend feature was not compiled into this
  /// binary is accepted by the options builder, but every later
  /// `checksum_gossip` would return a
  /// [`ChecksumError`](memberlist_proto::ChecksumError) and the driver would drop
  /// the datagram — so a "successfully" configured checksum would silently
  /// disable ALL gossip. Construction probes the configured algorithm and
  /// surfaces this typed error instead.
  #[cfg(checksum)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3"
    )))
  )]
  Checksum(memberlist_proto::ChecksumError),
}

/// The configured gossip MTU exceeds the largest plaintext payload whose on-wire
/// datagram still fits a UDP packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GossipMtuTooLarge {
  /// The configured `gossip_mtu` that was rejected.
  pub gossip_mtu: usize,
  /// The largest acceptable `gossip_mtu`:
  /// `65507 - ENCRYPTED_WRAPPER_OVERHEAD - CHECKSUMED_WRAPPER_OVERHEAD`.
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
      #[cfg(encryption)]
      InitError::Encryption(e) => write!(f, "encryption configuration is unusable: {e}"),
      #[cfg(checksum)]
      InitError::Checksum(e) => write!(f, "checksum configuration is unusable: {e}"),
    }
  }
}

impl From<EndpointInitError> for InitError {
  fn from(e: EndpointInitError) -> Self {
    InitError::Endpoint(e)
  }
}

#[cfg(encryption)]
impl From<memberlist_proto::EncryptionError> for InitError {
  fn from(e: memberlist_proto::EncryptionError) -> Self {
    InitError::Encryption(e)
  }
}

#[cfg(checksum)]
impl From<memberlist_proto::ChecksumError> for InitError {
  fn from(e: memberlist_proto::ChecksumError) -> Self {
    InitError::Checksum(e)
  }
}

#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
impl std::error::Error for InitError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      InitError::Endpoint(e) => Some(e),
      #[cfg(encryption)]
      InitError::Encryption(e) => Some(e),
      #[cfg(checksum)]
      InitError::Checksum(e) => Some(e),
      _ => None,
    }
  }
}

/// Why a runtime control operation on a running [`Engine`](crate::Engine) was
/// rejected.
///
/// Only [`set_encryption_options`](crate::Engine::set_encryption_options) needs
/// this type: it can fail BOTH on lifecycle and on an unusable keyring. The
/// other runtime setters have a single failure domain and report through the
/// machine's [`memberlist_proto::Error`] (lifecycle + size).
#[cfg(encryption)]
#[cfg_attr(
  docsrs,
  doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
)]
#[derive(Debug)]
#[non_exhaustive]
pub enum ControlError {
  /// The node has left the cluster (or never started): its schedulers are
  /// stopped, so the change could never reach the wire.
  NotRunning,
  /// A key in the supplied keyring uses an AEAD backend not built into this
  /// binary; the live encryption policy is left unchanged.
  Encryption(memberlist_proto::EncryptionError),
}

#[cfg(encryption)]
impl fmt::Display for ControlError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ControlError::NotRunning => {
        f.write_str("endpoint is not running (already left or shut down)")
      }
      ControlError::Encryption(e) => write!(f, "encryption configuration is unusable: {e}"),
    }
  }
}

#[cfg(encryption)]
impl From<memberlist_proto::EncryptionError> for ControlError {
  fn from(e: memberlist_proto::EncryptionError) -> Self {
    ControlError::Encryption(e)
  }
}

#[cfg(all(encryption, feature = "std"))]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
impl std::error::Error for ControlError {
  fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
    match self {
      ControlError::Encryption(e) => Some(e),
      ControlError::NotRunning => None,
    }
  }
}

#[cfg(all(test, feature = "std"))]
mod tests;
