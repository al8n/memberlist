//! Errors surfaced by `memberlist-reactor`.

use std::net::SocketAddr;

/// The largest the encrypted wrapper can inflate a gossip datagram, or `0` when
/// no encryption backend is built in. The proto const exists only under an
/// encryption backend; with none the gossip frame goes out unencrypted, so the
/// wrapper adds nothing to the on-wire ceiling reported below.
#[cfg(encryption)]
const ENCRYPTED_WRAPPER_OVERHEAD: usize = memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD;
#[cfg(not(encryption))]
const ENCRYPTED_WRAPPER_OVERHEAD: usize = 0;

/// The largest the checksum wrapper can inflate a gossip datagram, or `0` when
/// no checksum backend is built in. The proto const exists only under a checksum
/// backend; with none the gossip frame carries no checksum, so the wrapper adds
/// nothing to the on-wire ceiling reported below.
#[cfg(checksum)]
const CHECKSUMED_WRAPPER_OVERHEAD: usize = memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD;
#[cfg(not(checksum))]
const CHECKSUMED_WRAPPER_OVERHEAD: usize = 0;

/// Payload for [`Error::InvalidGossipMtu`]: the configured `gossip_mtu` exceeds
/// the largest plaintext gossip payload whose on-wire datagram still fits a
/// single UDP packet once the checksum and encryption wrappers are added.
/// Carries the configured value and the effective ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidGossipMtu {
  configured: usize,
  ceiling: usize,
}

impl InvalidGossipMtu {
  /// Build a new payload from the configured `gossip_mtu` and the ceiling.
  #[inline]
  pub(crate) fn new(configured: usize, ceiling: usize) -> Self {
    Self {
      configured,
      ceiling,
    }
  }

  /// The configured `gossip_mtu` that was rejected.
  #[inline]
  pub fn configured(&self) -> usize {
    self.configured
  }

  /// The effective ceiling — the largest plaintext `gossip_mtu` whose wire
  /// datagram (after the checksum and encryption wrappers) still fits a single
  /// UDP packet.
  #[inline]
  pub fn ceiling(&self) -> usize {
    self.ceiling
  }
}

impl std::fmt::Display for InvalidGossipMtu {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "gossip_mtu {} exceeds the maximum sendable plaintext gossip payload of {} bytes \
       (a gossip packet is one UDP datagram, capped at {} bytes on the wire after the \
       {}-byte checksum and encryption wrappers); a larger gossip_mtu would make \
       near-MTU gossip packets deterministically unsendable",
      self.configured,
      self.ceiling,
      self.ceiling + ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD,
      ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD,
    )
  }
}

/// An error from a `memberlist-reactor` operation.
///
/// Grows as the driver is built out (transport / machine variants are added
/// where those operations are wired). Kept `#[non_exhaustive]` so additions are
/// not breaking.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
  /// The memberlist has shut down; the operation cannot proceed.
  #[error("memberlist has shut down")]
  Shutdown,

  /// The node has left or never started; the operation cannot proceed.
  #[error("memberlist is not running")]
  NotRunning,

  /// The address resolver returned an error. The resolver's error type is a
  /// generic `Res::Error` (so it can't be a typed `#[from]` variant); it is
  /// boxed to preserve the `source()` chain — a caller that knows its concrete
  /// resolver can downcast.
  #[error("address resolution failed: {0}")]
  Resolve(#[source] Box<dyn core::error::Error + Send + Sync + 'static>),

  /// Address resolution succeeded but yielded no usable addresses.
  #[error("address resolution returned no addresses")]
  NoAddresses,

  /// A join dispatched push/pulls to seeds but contacted none of them.
  #[error("join contacted none of {0} seed(s)")]
  JoinFailed(usize),

  /// The resolved advertise address is not a usable unicast contact
  /// (unspecified, multicast, or broadcast); peers could not reach this node.
  #[error("invalid advertise address {0}: must be a concrete unicast address")]
  InvalidAdvertise(SocketAddr),

  /// The configured `close_timeout` is zero. The stream driver bounds each
  /// per-bridge graceful-close drain `write` with this timeout; a zero timeout
  /// fires immediately, so a graceful close abandons (RSTs) its queued push/pull
  /// response bytes instead of draining them, truncating reliable exchanges.
  /// Rejected at construction (stream backends only; QUIC has no bridges),
  /// mirroring the smoltcp driver's `ZeroCloseTimeout` rejection.
  #[error(
    "close_timeout must be nonzero: a zero timeout immediately RSTs a graceful reliable close, truncating queued push/pull bytes"
  )]
  ZeroCloseTimeout,

  /// A directed ping timed out (no ack received within the probe deadline).
  #[error("ping timed out")]
  PingTimeout,

  /// A reliable directed send failed: one or more stream exchanges did not
  /// complete successfully.
  #[error("reliable send failed")]
  SendFailed,

  /// A coordinator operation failed — most commonly a directed user-message or
  /// metadata payload exceeding its wire limit, which the machine reports as a
  /// structured size error. Carries the typed [`memberlist_proto::Error`] so
  /// callers can dispatch on the specific cause.
  #[error(transparent)]
  Proto(#[from] memberlist_proto::Error),

  /// The supplied node metadata exceeds the wire ceiling
  /// ([`Meta::MAX_SIZE`](memberlist_proto::typed::Meta::MAX_SIZE)) and was
  /// rejected before any coordinator mutation.
  #[error(transparent)]
  MetaTooLarge(#[from] memberlist_proto::typed::LargeMeta),

  /// An I/O error.
  #[error(transparent)]
  Io(#[from] std::io::Error),

  /// The operating system entropy source failed while seeding the gossip RNG.
  /// On a std target this is an unrecoverable platform fault and essentially
  /// never occurs; it is surfaced rather than panicked because the backend
  /// constructors that seed the machine already return a `Result`.
  #[error("OS entropy source failed while seeding the gossip RNG")]
  Entropy(#[source] std::io::Error),

  /// An encryption keyring was rejected (a key's algorithm backend is
  /// missing), so the node refuses to start rather than run plaintext on an
  /// encrypted-cluster configuration.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  #[error(transparent)]
  Encryption(#[from] memberlist_proto::EncryptionError),

  /// A gossip checksum algorithm was rejected because its backend feature is
  /// not compiled into this build. The options builder accepts the algorithm,
  /// but every later `checksum_gossip` would fail and the driver would drop the
  /// datagram — so a "successful" checksum config silently disables ALL gossip.
  /// Caught at construction and at the runtime
  /// [`set_checksum_options`](crate::Memberlist::set_checksum_options) setter so
  /// the misconfiguration is rejected rather than disabling gossip after a false
  /// `Ok`.
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
  #[error(transparent)]
  Checksum(#[from] memberlist_proto::ChecksumError),

  /// The configured `gossip_mtu` exceeds the largest plaintext gossip payload
  /// whose on-wire datagram still fits one UDP packet (after the checksum and
  /// encryption wrappers). A gossip packet is one UDP datagram, so a near-MTU
  /// packet built above that ceiling would be deterministically unsendable.
  /// Rejected at construction, mirroring the compio / embedded / smoltcp drivers.
  #[error("{0}")]
  InvalidGossipMtu(InvalidGossipMtu),

  /// The cluster label supplied to
  /// [`MemberlistOptions::with_label`](crate::MemberlistOptions::with_label)
  /// violates the wire constraints: it exceeds the 253-byte maximum or is not
  /// valid UTF-8. Rejected at the setter so the error surfaces at configuration
  /// time rather than at construction.
  #[error(transparent)]
  InvalidLabel(#[from] memberlist_proto::LabelError),
}
