//! Error types for memberlist-compio.

use core::fmt;
use std::{io, net::SocketAddr};

/// The largest the encrypted wrapper can inflate a gossip datagram, or `0` when
/// no encryption backend is built in. The proto const exists only under an
/// encryption backend; with none the gossip frame goes out unencrypted, so the
/// wrapper adds nothing and the ceiling arithmetic that sizes from it is the
/// plaintext size.
#[cfg(encryption)]
const ENCRYPTED_WRAPPER_OVERHEAD: usize = memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD;
#[cfg(not(encryption))]
const ENCRYPTED_WRAPPER_OVERHEAD: usize = 0;

/// The largest the checksum wrapper can inflate a gossip datagram, or `0` when
/// no checksum backend is built in. The proto const exists only under a checksum
/// backend; with none the gossip frame carries no checksum, so the wrapper adds
/// nothing.
#[cfg(checksum)]
const CHECKSUMED_WRAPPER_OVERHEAD: usize = memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD;
#[cfg(not(checksum))]
const CHECKSUMED_WRAPPER_OVERHEAD: usize = 0;

/// Payload for [`MemberlistError::JoinAllFailed`]: every dispatched
/// outbound push/pull exchange from a synchronous
/// [`join`](crate::Memberlist::join) terminated without
/// `ExchangeStatus::Succeeded`, OR the per-call deadline elapsed
/// before any exchange could succeed.
#[derive(Debug)]
pub struct JoinAllFailed {
  requested: usize,
  contacted: usize,
}

impl JoinAllFailed {
  /// Build a new payload from the per-call seed counts.
  #[inline]
  pub(crate) fn new(requested: usize, contacted: usize) -> Self {
    Self {
      requested,
      contacted,
    }
  }

  /// Number of seed addresses the call requested (post-resolution).
  #[inline]
  pub fn requested(&self) -> usize {
    self.requested
  }

  /// Number of seeds actually contacted before the call resolved.
  /// Always `0` when this payload is observed inside a
  /// [`MemberlistError::JoinAllFailed`] — the variant fires precisely
  /// when no seed was contacted.
  #[inline]
  pub fn contacted(&self) -> usize {
    self.contacted
  }
}

impl fmt::Display for JoinAllFailed {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "join failed: contacted {} of {} seed(s)",
      self.contacted, self.requested
    )
  }
}

/// Payload for [`MemberlistError::InvalidGossipMtu`]: the configured
/// `gossip_mtu` exceeds the largest plaintext gossip payload that can still
/// fit a single UDP datagram once the encryption wrapper is added. Carries
/// the configured value and the effective ceiling.
#[derive(Debug)]
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
  /// datagram (after the encryption wrapper) still fits a single UDP packet.
  #[inline]
  pub fn ceiling(&self) -> usize {
    self.ceiling
  }
}

impl fmt::Display for InvalidGossipMtu {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// Payload for [`MemberlistError::GossipMtuTooSmall`]: the configured
/// `gossip_mtu` is below the floor needed to carry the mandatory
/// single-datagram control packets (probe Ping / Ack / minimal self-Alive)
/// the SWIM protocol always emits. Carries the configured value and the
/// required minimum.
#[derive(Debug)]
pub struct GossipMtuTooSmall {
  configured: usize,
  minimum: usize,
}

impl GossipMtuTooSmall {
  /// Build a new payload from the configured `gossip_mtu` and the minimum.
  #[inline]
  pub(crate) fn new(configured: usize, minimum: usize) -> Self {
    Self {
      configured,
      minimum,
    }
  }

  /// The configured `gossip_mtu` that was rejected.
  #[inline]
  pub fn configured(&self) -> usize {
    self.configured
  }

  /// The required minimum `gossip_mtu` — the floor that fits the mandatory
  /// single-datagram control packets the protocol always emits.
  #[inline]
  pub fn minimum(&self) -> usize {
    self.minimum
  }
}

impl fmt::Display for GossipMtuTooSmall {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "gossip_mtu {} is below the minimum of {} bytes required to carry the mandatory \
       single-datagram control packets (probe Ping / Ack / a minimal self-Alive); a \
       smaller gossip_mtu would make normal probes exceed the plaintext gossip ceiling, \
       so peers would reject them and falsely suspect this node",
      self.configured, self.minimum,
    )
  }
}

/// Payload for [`MemberlistError::InvalidAdvertiseAddr`]: the resolved
/// advertise address cannot serve as the local node's reachable contact
/// identity, so peers that learn it would be unable to route membership
/// traffic to this node. Two independent classes are rejected, both carried
/// here with a typed `reason`:
///
/// - NOT A USABLE UNICAST CONTACT — an unspecified IP (`0.0.0.0` / `::`, the
///   wildcard-bind a node gets when it binds `0.0.0.0:0` and forgets to set a
///   concrete advertise), a multicast IP, an IPv4 broadcast IP
///   (`255.255.255.255`), or a zero port. Such an address encodes fine but is
///   undialable: every UDP probe and reliable dial a peer aims at it would go
///   nowhere.
/// - NOT REPRESENTABLE ON THE WIRE — a scoped/flow-labelled IPv6 `SocketAddr`
///   (`SocketAddrV6` with a nonzero `scope_id` or `flowinfo`, e.g. a link-local
///   `fe80::/10` address): the compact wire layout is `[16B IP][2B port]` and
///   carries neither field, so every local-node-bearing control packet
///   (self-`Alive`, push/pull state, `Ping`) would fail to encode at runtime.
///
/// Carries the offending address and a human-readable reason describing which
/// class it fell into.
#[derive(Debug)]
pub struct InvalidAdvertiseAddr {
  addr: SocketAddr,
  reason: String,
}

impl InvalidAdvertiseAddr {
  /// Build a new payload from the rejected advertise address and the reason
  /// it cannot serve as the local node's contact identity.
  #[inline]
  pub(crate) fn new(addr: SocketAddr, reason: String) -> Self {
    Self { addr, reason }
  }

  /// The advertise address that was rejected.
  #[inline]
  pub fn addr(&self) -> SocketAddr {
    self.addr
  }

  /// The reason the address cannot serve as the local node's contact
  /// identity (undialable unicast contact, or not wire-encodable).
  #[inline]
  pub fn reason(&self) -> &str {
    &self.reason
  }
}

impl fmt::Display for InvalidAdvertiseAddr {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "advertise address {} cannot serve as this node's reachable contact \
       identity, so peers that learn it could not route membership traffic \
       to this node: {}",
      self.addr, self.reason,
    )
  }
}

/// Payload for [`MemberlistError::InvalidOption`]: an operator-set tuning knob
/// ([`RuntimeOptions`](crate::RuntimeOptions) /
/// [`StreamTransportOptions`](crate::StreamTransportOptions)) was given a value
/// that would DETERMINISTICALLY break the node — an accept-then-silently-fail
/// configuration the constructor rejects rather than honoring. Carries the
/// knob name and a human-readable reason describing why the value is invalid.
#[derive(Debug)]
pub struct InvalidOption {
  option: &'static str,
  reason: String,
}

impl InvalidOption {
  /// Build a new payload from the rejected knob name and the reason.
  #[inline]
  pub(crate) fn new(option: &'static str, reason: String) -> Self {
    Self { option, reason }
  }

  /// The name of the tuning knob whose value was rejected.
  #[inline]
  pub fn option(&self) -> &'static str {
    self.option
  }

  /// The reason the value would deterministically break the node.
  #[inline]
  pub fn reason(&self) -> &str {
    &self.reason
  }
}

impl fmt::Display for InvalidOption {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "invalid {} option: {}", self.option, self.reason)
  }
}

/// Errors returned by [`Memberlist`](crate::Memberlist) operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MemberlistError {
  /// I/O error from the OS, socket, or compio runtime.
  #[error(transparent)]
  Io(#[from] io::Error),

  /// The operating system entropy source failed while seeding the gossip RNG.
  /// On a std target this is an unrecoverable platform fault and essentially
  /// never occurs; it is drawn in the node constructor (which returns this
  /// `Result`) before the driver task is spawned, so a failure is surfaced here
  /// rather than panicking in the spawned task.
  #[error("OS entropy source failed while seeding the gossip RNG")]
  Entropy(#[source] io::Error),

  /// Encryption codec error from memberlist-wire.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  #[error(transparent)]
  Encryption(#[from] memberlist_proto::EncryptionError),

  /// Checksum codec error from memberlist-wire. Surfaced when a configured
  /// gossip checksum algorithm's backend feature is not compiled into this
  /// build: the algorithm is accepted by the options builder but every later
  /// `checksum_gossip` would fail, silently dropping all gossip. Caught at
  /// construction and at the runtime
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

  /// Frame decode error from memberlist-wire.
  #[error(transparent)]
  Frame(#[from] memberlist_proto::FrameError),

  /// Address resolution failed (DNS error, etc.).
  #[error("address resolution: {0}")]
  Resolve(io::Error),

  /// The synchronous [`join`](crate::Memberlist::join) attempt
  /// resolved without any dispatched outbound push/pull exchange
  /// terminating with `ExchangeStatus::Succeeded` — either every
  /// exchange terminated `Failed` (dial failure, frame/record-layer
  /// rejection, peer hung up before the response was decoded), or
  /// the per-call deadline elapsed before any could succeed.
  #[error("{0}")]
  JoinAllFailed(JoinAllFailed),

  /// A graceful [`leave`](crate::Memberlist::leave) did not complete
  /// within the driver's `leave_timeout`: the machine's
  /// `Event::LeftCluster` completion signal — which fires only after the
  /// direct `Dead`-self notices queued for live peers have been flushed
  /// to the wire — was not observed before the deadline elapsed. The
  /// leave was initiated but the driver cannot confirm peers were
  /// notified; the local node still transitioned out of the cluster.
  #[error("leave did not complete within the configured leave_timeout")]
  LeaveTimeout,

  /// The driver task has shut down and is no longer accepting commands.
  #[error("driver shut down")]
  Shutdown,

  /// The local node has left the cluster, so the operation cannot be
  /// applied. Data-plane and metadata mutations
  /// ([`queue_user_broadcast`](crate::Memberlist::queue_user_broadcast),
  /// [`set_local_state`](crate::Memberlist::set_local_state),
  /// [`set_ack_payload`](crate::Memberlist::set_ack_payload),
  /// [`update_node_metadata`](crate::Memberlist::update_node_metadata))
  /// require an actively-participating node: once the node has left, the
  /// periodic gossip/probe/push-pull schedulers are stopped, so the
  /// mutation could never be disseminated. Returned instead of a false
  /// success.
  #[error("the local node has left the cluster; the operation requires a running node")]
  NotRunning,

  /// A coordinator operation failed — most commonly a data-plane payload that,
  /// once framed, would not fit a single gossip datagram (e.g. an over-budget
  /// ack payload from [`set_ack_payload`](crate::Memberlist::set_ack_payload),
  /// which the machine rejects as a structured size error rather than emit a
  /// silently-failing datagram). Carries the typed [`memberlist_proto::Error`]
  /// so callers can dispatch on the specific cause.
  #[error(transparent)]
  Proto(#[from] memberlist_proto::Error),

  /// The configured `gossip_mtu`
  /// ([`MemberlistOptions::with_gossip_mtu`](crate::MemberlistOptions::with_gossip_mtu))
  /// is larger than the largest plaintext gossip payload that can still fit a
  /// single UDP datagram once the encryption wrapper is added. A gossip packet
  /// (probe ack, gossip-disseminated Alive/user broadcast, …) is sent as ONE
  /// UDP datagram, hard-capped at 65507 bytes on the wire; both drivers drop
  /// `send_to` errors under the lossy-gossip policy, so a near-`gossip_mtu`
  /// packet built above this ceiling would be silently unsendable and peers
  /// would falsely suspect this node. Returned by `Memberlist::new` (fail-fast,
  /// before any socket is bound) so the misconfiguration is surfaced rather
  /// than producing deterministically dropped gossip.
  #[error("{0}")]
  InvalidGossipMtu(InvalidGossipMtu),

  /// The configured `gossip_mtu`
  /// ([`MemberlistOptions::with_gossip_mtu`](crate::MemberlistOptions::with_gossip_mtu))
  /// is below the floor needed to carry the mandatory single-datagram control
  /// packets (probe Ping / Ack / a minimal self-Alive) the SWIM protocol always
  /// emits. A `gossip_mtu` smaller than the largest such packet would make
  /// normal probes exceed the plaintext gossip ceiling on the receive side, so
  /// peers would reject them and falsely suspect this node. Returned by
  /// `Memberlist::new` (fail-fast, before any socket is bound) so the
  /// misconfiguration is surfaced rather than producing silently-rejected
  /// probes.
  #[error("{0}")]
  GossipMtuTooSmall(GossipMtuTooSmall),

  /// The resolved advertise address cannot serve as the local node's
  /// reachable contact identity, so a node constructed with it would publish
  /// an address peers cannot route membership traffic to. Rejected (rather
  /// than constructing `Ok` then silently failing every probe/dial) for any of:
  /// an unspecified IP (`0.0.0.0` / `::` — the wildcard-bind a node gets when
  /// it binds `0.0.0.0:0` and forgets to set a concrete advertise), a
  /// multicast IP, an IPv4 broadcast IP, a zero port (all undialable contacts),
  /// or a scoped/flow-labelled IPv6 `SocketAddr` (nonzero `scope_id`/`flowinfo`,
  /// e.g. a link-local `fe80::/10` address) the compact `[16B IP][2B port]`
  /// wire layout cannot carry — which would make every local-node-bearing
  /// control packet (self-`Alive`, push/pull state, `Ping`) fail to encode.
  /// Loopback, private, and global unicast addresses are accepted. Returned by
  /// `Memberlist::new` after the transport resolves its advertise address but
  /// before any driver task is spawned (the just-built transport is dropped on
  /// `Err`, closing its socket), so the misconfiguration is surfaced rather
  /// than producing a node that joins a cluster as an unreachable member.
  #[error("{0}")]
  InvalidAdvertiseAddr(InvalidAdvertiseAddr),

  /// An operator-set driver / stream-transport tuning knob
  /// ([`RuntimeOptions`](crate::RuntimeOptions) /
  /// [`StreamTransportOptions`](crate::StreamTransportOptions)) was given a
  /// value that would DETERMINISTICALLY break the node rather than merely
  /// degrade it — an accept-then-silently-fail configuration. Two such knobs
  /// are rejected:
  /// - `bridge_recv_buf_len == 0` — the per-bridge reliable-stream read buffer
  ///   would be zero-length, so every read returns `Ok(0)` (false EOF) and all
  ///   TCP/TLS join / push-pull stream exchanges break with no error surfaced;
  /// - `idle_wake_interval == 0` — the driver-loop fallback sleep would be
  ///   zero, so a quiescent endpoint (one with no nearer scheduled deadline)
  ///   busy-spins the loop, pegging a CPU core with no error surfaced.
  ///
  /// Rejected fail-fast at construction (the stream knob in `Transport::new`,
  /// the driver knob in `Memberlist::new`) so the misconfiguration is surfaced
  /// rather than producing a silently-broken or CPU-pegged node. Values that
  /// merely degrade-but-function (e.g. `iter_drain_cap == 0`,
  /// `cmd_fairness_budget == 0`) or are loud (`join_deadline == 0`) are NOT
  /// rejected.
  #[error("{0}")]
  InvalidOption(InvalidOption),

  /// Sending a command to the driver failed because the channel is closed.
  #[error("send to driver failed (channel closed)")]
  CommandSend,

  /// The driver's reply channel was dropped before a reply arrived.
  #[error("driver reply channel closed")]
  ReplyClosed,

  /// An application ping (`Memberlist::ping`) did not receive an Ack within
  /// the endpoint's configured `probe_timeout`. The probe was initiated but
  /// the peer did not respond before the deadline elapsed.
  #[error("ping timed out: no Ack received within probe_timeout")]
  PingTimeout,

  /// A reliable directed send (`send_reliable` / `send_many_reliable`) failed
  /// because one or more outbound reliable exchanges did not complete
  /// successfully. The peer may have been unreachable, rejected the connection,
  /// or the stream-level exchange timed out.
  #[error("reliable send failed: one or more outbound exchanges failed")]
  SendFailed,

  /// The cluster label supplied to
  /// [`MemberlistOptions::with_label`](crate::MemberlistOptions::with_label)
  /// violates the wire constraints: it exceeds the 253-byte maximum or is not
  /// valid UTF-8. Rejected at the setter so the error surfaces at configuration
  /// time rather than at construction.
  #[error(transparent)]
  InvalidLabel(#[from] memberlist_proto::LabelError),
}

/// Convenience [`Result`] for [`MemberlistError`].
pub type Result<T, E = MemberlistError> = core::result::Result<T, E>;

#[cfg(test)]
mod tests;
