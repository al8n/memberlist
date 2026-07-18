//! Errors surfaced by `memberlist-reactor`.

use std::net::SocketAddr;

pub use memberlist_driver::error::{GossipMtuTooSmall, InvalidOption, JoinFailed};

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

/// Payload for [`Error::UserDialBacklogFull`]: the peer whose per-peer reliable
/// user-message dial backlog is full and the configured per-peer
/// outstanding-dial limit it reached.
#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserDialBacklogFull {
  peer: SocketAddr,
  limit: usize,
}

#[cfg(feature = "quic")]
impl UserDialBacklogFull {
  /// Build from the target peer and the per-peer outstanding-dial limit.
  #[inline]
  pub(crate) fn new(peer: SocketAddr, limit: usize) -> Self {
    Self { peer, limit }
  }

  /// The peer whose reliable user-message dial backlog is full.
  #[inline]
  pub fn peer(&self) -> SocketAddr {
    self.peer
  }

  /// The configured per-peer outstanding-dial limit that was reached.
  #[inline]
  pub fn limit(&self) -> usize {
    self.limit
  }
}

#[cfg(feature = "quic")]
impl std::fmt::Display for UserDialBacklogFull {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "reliable user-message dial backlog to {} is full ({} outstanding intents); \
       backpressure, not failure — retry once the peer drains",
      self.peer, self.limit,
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

  /// A join dispatched push/pulls to seeds but contacted none of them. Carries
  /// the requested-seed count and the contacted count (always `0` for this
  /// error — a non-zero contact count resolves `Ok` with the reached address
  /// set).
  #[error("{0}")]
  JoinFailed(JoinFailed),

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

  /// A reliable directed send over QUIC was refused because the per-peer
  /// reliable user-message dial backlog to the target is already at its
  /// configured ceiling
  /// ([`QuicOptions::max_pending_user_dials_per_peer`](memberlist_proto::QuicOptions::max_pending_user_dials_per_peer)).
  /// This is admission control on this node's OWN reliable-send load —
  /// backpressure, not a delivery failure. The whole batch is admitted
  /// atomically, so a refused send started no exchange at all; retry once the
  /// peer establishes or grants stream credit and the backlog drains. Carries
  /// the peer and the limit.
  #[cfg(feature = "quic")]
  #[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
  #[error("{0}")]
  UserDialBacklogFull(UserDialBacklogFull),

  /// A coordinator operation failed — most commonly a directed user-message or
  /// metadata payload exceeding its wire limit, which the machine reports as a
  /// structured size error. Carries the typed [`memberlist_proto::Error`] so
  /// callers can dispatch on the specific cause.
  #[error(transparent)]
  Proto(#[from] memberlist_proto::Error),

  /// The Sans-I/O endpoint rejected the resolved configuration at construction —
  /// an out-of-range gossip MTU or reliable-frame size, an oversized initial
  /// meta or local-state snapshot, or a wire-unencodable local identity.
  /// Surfaced from the constructor instead of unwinding from it.
  #[error(transparent)]
  EndpointInit(#[from] memberlist_proto::EndpointInitError),

  /// The QUIC coordinator rejected the supplied [`QuicOptions`](memberlist_proto::QuicOptions)
  /// at construction — currently a zero per-peer reliable user-message dial
  /// ceiling, which would disable reliable user messages entirely. Surfaced from
  /// the constructor instead of silently shipping the misconfiguration.
  #[cfg(feature = "quic")]
  #[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
  #[error(transparent)]
  QuicOptionsInit(#[from] memberlist_proto::QuicOptionsError),

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

  /// The configured `gossip_mtu` is below the floor needed to carry the
  /// mandatory single-datagram control packets (probe Ping / Ack / a minimal
  /// self-Alive) the SWIM protocol always emits. A `gossip_mtu` smaller than the
  /// largest such packet would make normal probes exceed the plaintext gossip
  /// ceiling on the receive side, so peers would reject them and falsely suspect
  /// this node. Returned by the constructor (fail-fast, before any driver task
  /// is spawned) so the misconfiguration is surfaced rather than producing
  /// silently-rejected probes. Mirrors the compio / embedded / smoltcp drivers.
  #[error("{0}")]
  GossipMtuTooSmall(GossipMtuTooSmall),

  /// An operator-set tuning knob was given a value that would
  /// DETERMINISTICALLY break the node rather than merely degrade it — an
  /// accept-then-silently-fail configuration. Currently rejects a
  /// `max_stream_frame_size` of zero (rejects every reliable frame, so the node
  /// could never complete a push/pull or receive a reliable user message) or one
  /// above the `u32` wire limit (unreachable as a receive gate; a locally-built
  /// frame above it would fail to encode). Rejected fail-fast at construction so
  /// the misconfiguration is surfaced rather than producing a silently-broken
  /// node. Mirrors the compio / embedded / smoltcp drivers.
  #[error("{0}")]
  InvalidOption(InvalidOption),

  /// The cluster label supplied to
  /// [`MemberlistOptions::with_label`](crate::MemberlistOptions::with_label)
  /// violates the wire constraints: it exceeds the 253-byte maximum or is not
  /// valid UTF-8. Rejected at the setter so the error surfaces at configuration
  /// time rather than at construction.
  #[error(transparent)]
  InvalidLabel(#[from] memberlist_proto::LabelError),
}
