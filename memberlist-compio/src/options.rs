//! `Options<T: Transport>` umbrella — bundles per-backend transport options,
//! SWIM-level memberlist options, and driver tuning options.

use std::net::SocketAddr;

use bytes::Bytes;
use memberlist_proto::{
  CheapClone, CompressionOptions, EncryptionOptions, config::EndpointOptions,
  label::validate_label, typed::Meta,
};

use crate::{
  delegate::{AliveDelegate, MergeDelegate},
  driver_options::DriverOptions,
  transport::Transport,
};

/// SWIM-protocol-level options applied to the machine-layer
/// [`EndpointOptions`](memberlist_proto::config::EndpointOptions) inside each
/// `Transport::run`.
///
/// Each field is an override layered over the `EndpointOptions` default:
/// a `None` scalar / an empty [`Meta`] leaves the corresponding
/// `EndpointOptions` knob at its own default. The size knobs surfaced here
/// tune the gossip and reliable-stream paths:
///
/// - `gossip_mtu` — the plaintext outbound-datagram cap (and the per-iter
///   recv-buffer sizing that tracks it); raise it to carry large Alive
///   broadcasts that exceed the default 1400-byte path-MTU budget.
/// - `meta_max_size` — the LOCAL node's `Meta` byte ceiling (a
///   broadcast-size cap, NOT a peer-rejection filter).
/// - `max_stream_frame_size` — the reliable-stream frame ceiling; bounds the
///   per-event size of reliable user / push-pull payloads (and thus the
///   delegate-queue memory a peer can drive when a delegate falls behind).
/// - `initial_meta` — the local node's initial metadata payload.
/// - `initial_local_state` — the local node's initial push/pull
///   application-state snapshot.
/// - `compression` — the initial gossip and reliable-stream compression policy
///   (disabled by default). Applied at endpoint construction; the runtime
///   [`set_compression_options`](crate::Memberlist::set_compression_options)
///   command allows reconfiguration after the node is running.
/// - `encryption` — the initial gossip and reliable-stream encryption policy
///   (disabled / no keyring by default). Applied at endpoint construction; a
///   keyring naming an unsupported AEAD algorithm is caught at
///   [`Memberlist::new`](crate::Memberlist::new) rather than silently starting
///   plaintext. The runtime
///   [`set_encryption_options`](crate::Memberlist::set_encryption_options)
///   command allows key rotation after the node is running.
/// - `label` — cluster label applied to both the gossip and reliable planes
///   (no label by default). Validated at the setter: must be ≤253 bytes and
///   valid UTF-8. Feeds the reliable-plane `LabelOptions` (TCP/TLS) and the
///   gossip codec `EncodeOptions` from a single source, so the two planes
///   cannot diverge. QUIC clusters use the SNI hostname for isolation and do
///   not consult this field.
/// - `skip_inbound_label_check` — when `true`, an inbound stream that presents
///   no label header is accepted rather than rejected. Defaults to `false`.
#[derive(Debug, Clone, Default)]
pub struct MemberlistOptions {
  gossip_mtu: Option<usize>,
  meta_max_size: Option<usize>,
  max_stream_frame_size: Option<usize>,
  initial_meta: Option<Meta>,
  initial_local_state: Option<Bytes>,
  compression: CompressionOptions,
  encryption: EncryptionOptions,
  label: Option<Bytes>,
  skip_inbound_label_check: bool,
}

impl MemberlistOptions {
  /// Construct with SWIM-protocol defaults — all fields unset (`None`),
  /// letting the machine-layer `EndpointOptions` defaults apply.
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  /// Builder: override the plaintext gossip-datagram MTU. `None` (the
  /// default) keeps the `EndpointOptions` default
  /// ([`DEFAULT_GOSSIP_MTU`](memberlist_proto::config::DEFAULT_GOSSIP_MTU),
  /// 1400 bytes).
  ///
  /// A gossip packet is emitted as a single UDP datagram, so `mtu` must leave
  /// room for the encryption wrapper within the 65507-byte UDP payload limit:
  /// `Memberlist::new` rejects an `mtu` above `65507 - ENCRYPTED_WRAPPER_OVERHEAD`
  /// (65477 bytes) with [`MemberlistError::InvalidGossipMtu`](crate::MemberlistError::InvalidGossipMtu),
  /// since a near-MTU packet built above that would be deterministically
  /// unsendable. It also rejects an `mtu` below the floor needed to carry the
  /// mandatory single-datagram control packets (probe Ping / Ack / minimal
  /// self-Alive) with
  /// [`MemberlistError::GossipMtuTooSmall`](crate::MemberlistError::GossipMtuTooSmall),
  /// since a smaller value would make normal probes deterministically rejected
  /// by peers. The default (1400) sits comfortably between the two bounds.
  #[must_use]
  #[inline]
  pub fn with_gossip_mtu(mut self, mtu: usize) -> Self {
    self.gossip_mtu = Some(mtu);
    self
  }

  /// Builder: override the LOCAL node's `Meta` byte ceiling. `None` (the
  /// default) keeps the `EndpointOptions` default
  /// ([`DEFAULT_META_MAX_SIZE`](memberlist_proto::config::DEFAULT_META_MAX_SIZE),
  /// 512 bytes). This is a local-broadcast cap, not a peer-rejection filter.
  #[must_use]
  #[inline]
  pub fn with_meta_max_size(mut self, size: usize) -> Self {
    self.meta_max_size = Some(size);
    self
  }

  /// Builder: override the reliable-stream frame ceiling — the largest
  /// push/pull or reliable user-message payload accepted on a reliable stream.
  /// `None` (the default) keeps the `EndpointOptions` default
  /// ([`DEFAULT_MAX_STREAM_FRAME_SIZE`](memberlist_proto::config::DEFAULT_MAX_STREAM_FRAME_SIZE),
  /// 64 MiB). This bounds the per-event size of `RemoteStateReceived` /
  /// reliable `UserPacket` payloads, and so — together with the
  /// observation-channel byte backstop — the memory a peer can drive through
  /// the delegate queue when a delegate falls behind.
  #[must_use]
  #[inline]
  pub fn with_max_stream_frame_size(mut self, size: usize) -> Self {
    self.max_stream_frame_size = Some(size);
    self
  }

  /// Builder: set the local node's initial metadata. An unset value (the
  /// default) leaves the node's initial `Meta` empty.
  #[must_use]
  #[inline]
  pub fn with_initial_meta(mut self, meta: Meta) -> Self {
    self.initial_meta = Some(meta);
    self
  }

  /// Builder: set the local node's initial push/pull application-state
  /// snapshot. An unset value (the default) leaves the initial local state
  /// empty. The snapshot rides every push/pull exchange (including the
  /// initial join) until replaced via
  /// [`Memberlist::set_local_state`](crate::Memberlist::set_local_state).
  #[must_use]
  #[inline]
  pub fn with_initial_local_state(mut self, state: Bytes) -> Self {
    self.initial_local_state = Some(state);
    self
  }

  /// Builder: set the initial gossip and reliable-stream compression policy.
  /// The default (disabled) leaves all datagrams and stream frames
  /// uncompressed until a runtime
  /// [`set_compression_options`](crate::Memberlist::set_compression_options)
  /// call is made.
  #[must_use]
  #[inline]
  pub fn with_compression(mut self, compression: CompressionOptions) -> Self {
    self.compression = compression;
    self
  }

  /// Builder: set the initial gossip and reliable-stream encryption policy.
  /// The default (no keyring) leaves all traffic unencrypted until a runtime
  /// [`set_encryption_options`](crate::Memberlist::set_encryption_options)
  /// call is made. A keyring naming an unsupported AEAD algorithm is rejected
  /// at [`Memberlist::new`](crate::Memberlist::new) before any socket is
  /// bound.
  #[must_use]
  #[inline]
  pub fn with_encryption(mut self, encryption: EncryptionOptions) -> Self {
    self.encryption = encryption;
    self
  }

  /// Builder: set the cluster label for both the gossip and reliable planes.
  ///
  /// The label is validated immediately: it must be ≤253 bytes and valid
  /// UTF-8. An empty slice normalizes to `None` (no label). Returns
  /// `Err(MemberlistError::InvalidLabel(_))` when either constraint is
  /// violated.
  ///
  /// The validated label is the single source for both the reliable-plane
  /// `LabelOptions` (plain TCP or TLS) and the gossip codec `EncodeOptions`,
  /// so the two planes cannot diverge.
  #[inline]
  pub fn with_label(
    mut self,
    label: Option<Vec<u8>>,
  ) -> Result<Self, crate::error::MemberlistError> {
    self.label = match label {
      None => None,
      Some(v) if v.is_empty() => None,
      Some(v) => {
        validate_label(&v).map_err(crate::error::MemberlistError::InvalidLabel)?;
        Some(Bytes::from(v))
      }
    };
    Ok(self)
  }

  /// Builder: suppress the inbound reliable-plane label check.
  ///
  /// When set, an inbound TCP/TLS stream that presents no label header is
  /// accepted rather than rejected. Defaults to `false`. Faithful to
  /// memberlist-core `Options::skip_inbound_label_check`.
  #[must_use]
  #[inline]
  pub fn with_skip_inbound_label_check(mut self, skip: bool) -> Self {
    self.skip_inbound_label_check = skip;
    self
  }

  /// The configured gossip-MTU override, if any.
  #[inline]
  pub const fn gossip_mtu(&self) -> Option<usize> {
    self.gossip_mtu
  }

  /// The configured local `Meta` byte ceiling override, if any.
  #[inline]
  pub const fn meta_max_size(&self) -> Option<usize> {
    self.meta_max_size
  }

  /// The configured reliable-stream frame ceiling override, if any.
  #[inline]
  pub const fn max_stream_frame_size(&self) -> Option<usize> {
    self.max_stream_frame_size
  }

  /// The configured initial `Meta`, if any.
  #[inline]
  pub const fn initial_meta(&self) -> Option<&Meta> {
    self.initial_meta.as_ref()
  }

  /// The configured initial push/pull local-state snapshot, if any.
  #[inline]
  pub const fn initial_local_state(&self) -> Option<&Bytes> {
    self.initial_local_state.as_ref()
  }

  /// The initial gossip and reliable-stream compression policy.
  #[inline]
  pub const fn compression(&self) -> &CompressionOptions {
    &self.compression
  }

  /// The initial gossip and reliable-stream encryption policy.
  #[inline]
  pub const fn encryption(&self) -> &EncryptionOptions {
    &self.encryption
  }

  /// The cluster label, if set.
  #[inline]
  pub fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// Whether the inbound reliable-plane label check is suppressed.
  #[inline]
  pub const fn skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }
}

/// The IP-layer maximum UDP payload: 65535 (the 16-bit UDP length field)
/// minus the 8-byte UDP header minus the 20-byte IPv4 header. A gossip packet
/// is emitted as one UDP datagram, so its on-wire size can never exceed this.
/// Mirrors the `GOSSIP_RECV_BUF_MAX` recv-buffer clamp in the stream and QUIC
/// drivers (both `65507`).
const UDP_PAYLOAD_MAX: usize = 65507;

/// The largest plaintext `gossip_mtu` whose wire datagram still fits a single
/// UDP packet. A gossip packet's plaintext budget is `gossip_mtu`; the wire
/// datagram after the encryption wrapper is at most
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD` (the same model the drivers' recv
/// buffers are sized to — compression only shrinks, the binding inflation is
/// the encryption wrapper), and that must be `<= UDP_PAYLOAD_MAX`. So the
/// valid maximum plaintext `gossip_mtu` is
/// `UDP_PAYLOAD_MAX - ENCRYPTED_WRAPPER_OVERHEAD`.
pub(crate) const GOSSIP_MTU_MAX: usize =
  UDP_PAYLOAD_MAX - memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD;

/// The lower bound on the plaintext `gossip_mtu`. A gossip packet is the
/// transport for the SWIM protocol's mandatory single-datagram control
/// messages — the probe `Ping` (sequence + source `Node` + target `Node`),
/// its `Ack` reply, and a minimal self-`Alive` (the node's own membership
/// broadcast at join). Each is emitted as ONE UDP datagram with no split
/// point, and when compression OR encryption is enabled the receive side caps
/// the decompressed/decrypted plaintext at `gossip_mtu`
/// ([`memberlist_proto::unwrap_transforms_with_encryption`]'s `max_orig_len`),
/// so a `gossip_mtu` below the largest mandatory control packet makes normal
/// probes deterministically rejected → false suspicion. On the send side a
/// self-`Alive` smaller than `gossip_mtu` is required for the gossip scheduler
/// to select it at all.
///
/// 512 is a documented conservative floor: for the pinned `(SmolStr,
/// SocketAddr)` types the framed mandatory packets measure ~8 B (`Ack`), ~28 B
/// (minimal `Alive`), and ~70 B (`Ping` over IPv6), so 512 covers them with
/// generous headroom for larger node-id / address encodings, matches the
/// codebase's established small-but-functional size anchor
/// ([`memberlist_proto::config::DEFAULT_META_MAX_SIZE`] / the legacy
/// `META_MAX_SIZE`, both 512), and sits far below the
/// [`DEFAULT_GOSSIP_MTU`](memberlist_proto::config::DEFAULT_GOSSIP_MTU)
/// (1400), so the default and any sane value pass. Reject (don't clamp) so the
/// operator learns and fixes the misconfiguration.
pub(crate) const GOSSIP_MTU_MIN: usize = 512;

/// Validate the configured `gossip_mtu` against the hard UDP datagram ceiling
/// and the mandatory-control-packet floor.
///
/// A gossip packet (probe ack, gossip-disseminated Alive / user broadcast) is
/// sent as ONE UDP datagram and both drivers drop `send_to` errors under the
/// lossy-gossip policy, so a `gossip_mtu` whose near-MTU wire datagram cannot
/// fit a UDP packet is an impossible configuration: such packets would be
/// silently dropped and peers would falsely suspect this node. Symmetrically, a
/// `gossip_mtu` below [`GOSSIP_MTU_MIN`] cannot carry the mandatory
/// single-datagram control packets (probe Ping / Ack / minimal self-Alive) the
/// protocol always emits — normal probes would be rejected on the receive side
/// (and a too-small self-Alive would never be selected for gossip), again
/// producing false suspicion. Reject both (rather than silently clamping) so
/// the operator learns and fixes it — the default and any sane value sit
/// comfortably between the floor and the ceiling.
///
/// Called from `Memberlist::new` before the transport is constructed so the
/// misconfiguration fails fast, before any socket is bound; every backend
/// (TCP/TLS/QUIC) routes through that single `Memberlist::new` path, so the
/// check is enforced uniformly without per-backend duplication.
pub(crate) fn validate_gossip_mtu(
  opts: &MemberlistOptions,
) -> Result<(), crate::error::MemberlistError> {
  if let Some(mtu) = opts.gossip_mtu() {
    if mtu > GOSSIP_MTU_MAX {
      return Err(crate::error::MemberlistError::InvalidGossipMtu(
        crate::error::InvalidGossipMtu::new(mtu, GOSSIP_MTU_MAX),
      ));
    }
    if mtu < GOSSIP_MTU_MIN {
      return Err(crate::error::MemberlistError::GossipMtuTooSmall(
        crate::error::GossipMtuTooSmall::new(mtu, GOSSIP_MTU_MIN),
      ));
    }
  }
  Ok(())
}

/// Validate the generic-free [`DriverOptions`] knobs that would
/// DETERMINISTICALLY break (not merely degrade) the driver loop.
///
/// Rejected fail-fast (mirroring the reject-not-clamp `gossip_mtu` doctrine)
/// rather than constructing `Ok` over a silently-broken node:
///
/// - `idle_wake_interval == 0`: the driver loop's fallback sleep when the
///   coordinator has no nearer pending deadline. Zero makes a quiescent
///   endpoint wake with a zero-duration timer every pass — a busy-spin that
///   pegs a CPU core, silently.
/// - `cmd_fairness_budget == 0`: the iter-top command fairness drain. The
///   main `select_biased!` polls the recv arm ahead of the command arm, so
///   under a continuous inbound flood the command arm is never reached; the
///   fairness drain (`try_recv` up to the budget every pass) is the ONLY
///   mechanism that keeps commands progressing under that load. Zero disables
///   it, so `shutdown` / `leave` / joins can hang indefinitely.
/// - `peek_budget == 0`: the past-due recv-preemption peek window. On
///   completion-based io_uring a freshly-submitted recv is always pending on
///   first poll, so a zero-duration peek timer wins the select immediately,
///   cancels the recv, and drops a kernel-buffered ack — falsely suspecting
///   the peer. Zero defeats the peek's entire purpose.
/// - `observation_channel == Channel::Bounded(0)`: a zero-capacity rendezvous
///   the driver's non-blocking `try_send` can never deposit into, so the
///   delegate would observe nothing.
///
/// The remaining knobs degrade-but-function or are loud at zero and are NOT
/// rejected: `iter_drain_cap == 0` (the per-iteration batch cap — the `select`
/// arms still process one inbound event per pass), `event_queue_cap == 0` (a
/// valid flume rendezvous EventStream channel), and `join_deadline == 0` (a
/// loud immediate `JoinAllFailed`).
///
/// Called from `Memberlist::new` before the transport is constructed so the
/// misconfiguration fails fast, before any socket is bound; every backend
/// (TCP/TLS/QUIC) routes through that single path.
pub(crate) fn validate_driver_options(
  opts: &DriverOptions,
) -> Result<(), crate::error::MemberlistError> {
  if opts.idle_wake_interval().is_zero() {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "idle_wake_interval",
        "the driver-loop fallback sleep must be nonzero: a zero idle_wake_interval makes a \
         quiescent endpoint busy-spin the driver loop and peg a CPU core"
          .to_string(),
      ),
    ));
  }
  if opts.cmd_fairness_budget() == 0 {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "cmd_fairness_budget",
        "the iter-top command fairness drain must pull at least one command per pass: the main \
         select biases the recv arm ahead of commands, so under a continuous inbound flood the \
         command arm is starved indefinitely and shutdown / leave / joins would never be \
         serviced; a zero budget disables the only drain that guarantees command progress"
          .to_string(),
      ),
    ));
  }
  if opts.peek_budget().is_zero() {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "peek_budget",
        "the past-due preemption peek must give the recv a nonzero window: on completion-based \
         io_uring a freshly-submitted recv is always pending on first poll, so a zero peek \
         budget makes the timer arm win immediately, cancelling the recv and dropping a \
         kernel-buffered ack — the peer would be falsely suspected"
          .to_string(),
      ),
    ));
  }
  if let crate::Channel::Bounded(0) = opts.observation_channel() {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "observation_channel",
        "a Bounded(0) observation channel is a zero-capacity rendezvous: the driver's \
         non-blocking try_send can never deposit an event into it, so the delegate would \
         observe nothing; use Channel::Unbounded or Bounded(n) with n >= 1"
          .to_string(),
      ),
    ));
  }
  Ok(())
}

/// Validate that the resolved advertise address is a USABLE UNICAST CONTACT —
/// an address peers can actually route membership traffic to.
///
/// The advertise address is the local node's published identity: every peer
/// that learns this node aims its UDP probes and reliable dials at it. An
/// address that encodes fine but is undialable would let the node construct
/// `Ok` and join a cluster as a member no one can reach — every probe a peer
/// sends it goes nowhere, so peers eventually suspect and reap it. The classic
/// trap is binding the wildcard `0.0.0.0:0` (or `[::]:0`) and forgetting to set
/// a concrete advertise: the gossip socket's `local_addr()` read back in
/// `T::new` is then an UNSPECIFIED IP, which the wire codec happily encodes.
///
/// Rejected (not clamped — the operator must supply a concrete reachable
/// address; auto-resolving one from the interface list is a deliberately
/// out-of-scope larger feature) for any of:
/// - `ip().is_unspecified()` — `0.0.0.0` / `::`, the wildcard bind;
/// - `ip().is_multicast()` — a group address, never a single peer's contact;
/// - an IPv4 broadcast IP (`255.255.255.255`) — not a unicast contact;
/// - `port() == 0` — undialable. Defensive: the `local_addr()` readback that
///   feeds this should already carry the concrete OS-assigned port, but a zero
///   port is rejected if it ever surfaces.
///
/// ACCEPTED: loopback (`127.0.0.0/8`, `::1` — valid for single-machine and
/// test deployments), private, and global unicast addresses. Wire-encodability
/// (the scoped/flow-labelled IPv6 case) is validated separately by
/// [`validate_gossip_mtu_for_identity`], which needs the actual local id to
/// trial-encode; both rejections surface as
/// [`MemberlistError::InvalidAdvertiseAddr`].
///
/// Called from `Memberlist::new` on the post-readback resolved advertise
/// address (`transport.advertise_address()`), before any driver task is
/// spawned (the just-built transport is dropped on `Err`, closing its socket).
/// Every backend (TCP/TLS/QUIC) is an alias over the one generic `Memberlist`,
/// so this single call covers all three without per-backend duplication.
pub(crate) fn validate_advertise_addr(
  advertise_addr: &SocketAddr,
) -> Result<(), crate::error::MemberlistError> {
  let reject = |reason: &str| {
    Err(crate::error::MemberlistError::InvalidAdvertiseAddr(
      crate::error::InvalidAdvertiseAddr::new(*advertise_addr, reason.to_string()),
    ))
  };

  let ip = advertise_addr.ip();
  if ip.is_unspecified() {
    return reject(
      "an unspecified IP (0.0.0.0 / ::) is the wildcard-bind address, not a routable contact \
       — peers cannot dial it (set a concrete advertise address when binding the wildcard)",
    );
  }
  if ip.is_multicast() {
    return reject("a multicast IP is a group address, not a single peer's unicast contact");
  }
  // IPv4 broadcast (255.255.255.255) is a v4-only concept; match the variant.
  if let SocketAddr::V4(v4) = advertise_addr
    && v4.ip().is_broadcast()
  {
    return reject("an IPv4 broadcast IP (255.255.255.255) is not a unicast contact");
  }
  if advertise_addr.port() == 0 {
    return reject(
      "a zero port is undialable — the bound socket's local_addr() readback must carry a \
       concrete port",
    );
  }
  Ok(())
}

/// Validate the effective `gossip_mtu` against the IDENTITY-AWARE
/// mandatory-control-packet floor — the fixed [`GOSSIP_MTU_MIN`] floor in
/// [`validate_gossip_mtu`] ignores the local id size, but `I` is unbounded
/// (`SmolStr`/`String`), so a node whose own mandatory single-datagram control
/// packets — built from its ACTUAL local id — exceed the gossip budget would
/// have those packets silently unsendable / never gossiped and peers would
/// falsely suspect it.
///
/// The receive side caps the decompressed/decrypted plaintext at `gossip_mtu`
/// ([`memberlist_proto::unwrap_transforms_with_encryption`]'s `max_orig_len`),
/// and the send-side gossip scheduler only selects an Alive whose plain frame
/// is `<= gossip_mtu` (mirrored by the machine's own
/// [`set_ack_payload`](memberlist_proto::endpoint) cap, which charges the
/// plain `encode_message` length directly against `gossip_mtu`). So each
/// mandatory packet's PLAINTEXT framed length must fit `gossip_mtu`; the
/// encryption wrapper is charged separately against the UDP ceiling
/// ([`GOSSIP_MTU_MAX`]) and is not double-counted here.
///
/// The mandatory single-datagram control packets are encoded with the actual
/// `local_id`, the ACTUAL resolved `advertise_addr` for the LOCAL node, and the
/// widest sequence/incarnation varint (`u32::MAX`):
/// - a direct `Ping` carrying two `Node`s — the LOCAL node (the actual local id
///   with the actual advertise address, the source of an outbound probe) and a
///   worst-case max-size PEER target (an IPv6 `SocketAddr`, the largest the
///   `Data` codec emits; its `flowinfo`/`scope_id` are zero, the only form the
///   compact wire layout accepts). The peer is held at the worst case so the
///   size bound stays conservative for the largest possible target address,
///   while the local source uses the node's real config. This is the largest
///   id-driven mandatory packet, and the one that makes the floor identity-aware;
/// - a self-`Alive` carrying the LOCAL node + the ACTUAL configured
///   [`initial_meta`](MemberlistOptions::with_initial_meta) (empty when unset) —
///   the Alive the node actually broadcasts about itself at join;
/// - an empty-payload `Ack` (a non-empty ack payload is validated against the
///   same budget by the machine's `set_ack_payload`).
///
/// The self-Alive charges the ACTUAL `initial_meta`, NOT the `meta_max_size`
/// cap: `meta_max_size` is an independent operator knob (a `meta_max_size ==
/// gossip_mtu` config is sensible — the node carries a meta well under the
/// budget), and the machine itself validates the ACTUAL value against
/// `gossip_mtu` (`set_ack_payload` charges the real payload; `update_meta`
/// gates a runtime meta change against `meta_max_size`, where any meta-vs-
/// gossip_mtu interaction belongs) rather than the cap. Charging the cap here
/// would falsely reject those configs. This floor is about the unbounded
/// IDENTITY (the id, dominating the `Ping`); the meta is included only at its
/// real configured size so an over-budget `initial_meta` is still caught.
///
/// Each is encoded through the PUBLIC codec
/// ([`memberlist::codec::encode_outgoing`]) — the same plain-frame bytes the
/// machine charges against `gossip_mtu`. Because the LOCAL node carries the
/// ACTUAL advertise address, this also validates that address's
/// wire-encodability: the compact `SocketAddrV6` encoder REJECTS a nonzero
/// `scope_id`/`flowinfo` (a scoped link-local `fe80::/10` advertise address is
/// a realistic deployment), so a node advertising such an address would
/// otherwise construct `Ok` and then fail to encode EVERY local-node-bearing
/// packet (self-`Alive`, push/pull state, `Ping`) at runtime — silently unable
/// to emit membership traffic. The worst-case peer target is always encodable
/// (zero `scope_id`/`flowinfo`), so the only possible encode failure here is
/// the local advertise address; that case is rejected (not clamped) with
/// [`MemberlistError::InvalidAdvertiseAddr`] carrying the address and the codec
/// reason. The routability of the advertise address (it must be a usable
/// unicast contact, not an unspecified/multicast/broadcast IP or a zero port)
/// is checked separately by [`validate_advertise_addr`].
///
/// When encoding succeeds, the largest required plaintext is compared against
/// the effective `gossip_mtu` (the override, or
/// [`DEFAULT_GOSSIP_MTU`](memberlist_proto::config::DEFAULT_GOSSIP_MTU) when
/// unset — an oversized id is rejected even at the default). Over-budget is
/// rejected (not clamped) with [`MemberlistError::GossipMtuTooSmall`] carrying
/// the effective `gossip_mtu` and the required minimum.
///
/// Called from `Memberlist::new` immediately after `T::new` (the first point
/// the resolved local id AND advertise address are available through the
/// generic `Transport` trait); the just-built transport is dropped on `Err`,
/// before any driver task is spawned (its socket closes). Every backend
/// (TCP/TLS/QUIC) routes through the single `Memberlist::new` path, so the
/// check is enforced uniformly without per-backend duplication.
pub(crate) fn validate_gossip_mtu_for_identity<I>(
  local_id: &I,
  advertise_addr: &SocketAddr,
  opts: &MemberlistOptions,
) -> Result<(), crate::error::MemberlistError>
where
  I: memberlist_proto::Data + CheapClone,
{
  use memberlist::codec::{EncodeOptions, encode_outgoing};
  use memberlist_proto::{
    Node,
    typed::{Ack, Alive, Message, Ping},
  };

  let budget = opts
    .gossip_mtu()
    .unwrap_or(memberlist_proto::config::DEFAULT_GOSSIP_MTU);

  // The LOCAL node carries the node's ACTUAL resolved advertise address: every
  // mandatory packet below is one the node really emits about itself, so its
  // size — and its wire-encodability — is validated against the real config.
  let local_node = Node::new(local_id.cheap_clone(), *advertise_addr);

  // Worst-case PEER target for the probe Ping: the largest address the `Data`
  // codec emits (an IPv6 `SocketAddr`, 19 wire bytes vs 7 for IPv4).
  // `flowinfo`/`scope_id` are zero — the only form the compact wire encoder
  // accepts — so this peer is always encodable and never the source of an
  // encode failure here; it only keeps the size bound conservative for the
  // largest possible target address.
  let worst_peer_addr = SocketAddr::V6(std::net::SocketAddrV6::new(
    std::net::Ipv6Addr::new(
      0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
    ),
    u16::MAX,
    0,
    0,
  ));
  let worst_peer = Node::new(local_id.cheap_clone(), worst_peer_addr);

  // The self-Alive carries the ACTUAL configured initial meta (empty when
  // unset) — the meta the node actually broadcasts at join.
  let alive_meta = opts.initial_meta().cloned().unwrap_or_default();

  let mandatory: [Message<I, SocketAddr>; 3] = [
    Message::Ping(Ping::new(
      u32::MAX,
      local_node.cheap_clone(),
      worst_peer.cheap_clone(),
    )),
    Message::Alive(Alive::new(u32::MAX, local_node.cheap_clone()).with_meta(alive_meta)),
    Message::Ack(Ack::new(u32::MAX)),
  ];

  let mut required = 0usize;
  for msg in &mandatory {
    // The peer target is always wire-encodable; the only address that can make
    // a local-node-bearing packet fail to encode is the LOCAL advertise
    // address (e.g. a scoped/flow-labelled IPv6 the compact `SocketAddrV6`
    // encoder rejects). Surface that as a clear advertise-address rejection
    // rather than the generic mtu floor, so the operator sees the real cause.
    let len = match encode_outgoing(msg, &EncodeOptions::default()) {
      Ok(bytes) => bytes.len(),
      Err(e) => {
        return Err(crate::error::MemberlistError::InvalidAdvertiseAddr(
          crate::error::InvalidAdvertiseAddr::new(
            *advertise_addr,
            format!(
              "not representable on the compact `[16B IP][2B port]` wire layout \
               (a scoped/flow-labelled IPv6 address — nonzero scope_id or flowinfo — \
               carries neither field), so every local-node-bearing control packet \
               (self-Alive / push-pull state / Ping) would fail to encode at runtime: {e}"
            ),
          ),
        ));
      }
    };
    required = required.max(len);
  }

  if required > budget {
    return Err(crate::error::MemberlistError::GossipMtuTooSmall(
      crate::error::GossipMtuTooSmall::new(budget, required),
    ));
  }
  Ok(())
}

/// Validate that the local node can frame its own minimal reliable push/pull
/// within the configured `max_stream_frame_size`.
///
/// Every join and periodic anti-entropy exchange is a reliable-stream PushPull
/// frame carrying at least the local node's own `PushNodeState`. The receiver
/// rejects any reliable frame whose declared length exceeds
/// `max_stream_frame_size`, so a cap below the local node's minimal PushPull
/// frame makes every membership exchange undeliverable — yet `Memberlist::new`
/// would otherwise return `Ok` and the node would appear to run while never
/// completing a reliable exchange. Reject it fail-fast, after transport
/// resolution (so the ACTUAL advertise address is known), mirroring
/// [`validate_gossip_mtu_for_identity`].
///
/// The floor is dynamic, not a fixed magic-number: it is the encoded size of a
/// PushPull carrying the ACTUAL local id / advertise address, sized for the
/// WORST-CASE meta the node could broadcast — its `meta_max_size` ceiling, since
/// `update_meta` admits any later metadata up to that cap. So it accepts any cap
/// that genuinely fits the node (e.g. a small no-snapshot 256 KiB deployment),
/// and a cap that passes is guaranteed to fit every future `update_node_metadata`
/// — closing the construction-only gap where a later meta growth could push the
/// node's own PushPull past the cap.
/// Zero and the u32 upper bound are covered by
/// [`validate_max_stream_frame_size`]; the advertise address's wire-encodability
/// is covered by [`validate_gossip_mtu_for_identity`] (which runs first), so an
/// encode failure here is treated defensively as "fits" — that validator reports
/// the real cause.
pub(crate) fn validate_stream_frame_for_identity<I>(
  local_id: &I,
  advertise_addr: &SocketAddr,
  opts: &MemberlistOptions,
) -> Result<(), crate::error::MemberlistError>
where
  I: memberlist_proto::Data + CheapClone,
{
  use memberlist::codec::{EncodeOptions, encode_outgoing};
  use memberlist_proto::typed::{Message, PushNodeState, PushPull, State};

  let max_frame = opts
    .max_stream_frame_size()
    .unwrap_or(memberlist_proto::config::DEFAULT_MAX_STREAM_FRAME_SIZE);

  // The node's own state is the minimum any join / anti-entropy PushPull carries.
  // Size it for the WORST-CASE meta the node could ever broadcast — its
  // `meta_max_size` ceiling (capped at the wire `Meta::MAX_SIZE`), not just the
  // initial meta — because `Endpoint::update_meta` admits any later metadata up
  // to that cap (rejecting larger with `MetaExceedsCap`). A frame that fits a
  // meta_max_size-sized meta therefore fits every future `update_node_metadata`,
  // so this construction check alone closes the runtime-growth gap. Worst-case
  // incarnation varint (`u32::MAX`) too.
  let meta_cap = opts
    .meta_max_size()
    .unwrap_or(memberlist_proto::config::DEFAULT_META_MAX_SIZE)
    .min(Meta::MAX_SIZE);
  let worst_case_meta = Meta::try_from(Bytes::from(vec![0u8; meta_cap])).unwrap_or_default();
  let local_state = PushNodeState::new(
    u32::MAX,
    local_id.cheap_clone(),
    *advertise_addr,
    State::Alive,
  )
  .with_meta(worst_case_meta);
  let minimal: Message<I, SocketAddr> =
    Message::PushPull(PushPull::new(true, core::iter::once(local_state)));

  let Ok(bytes) = encode_outgoing(&minimal, &EncodeOptions::default()) else {
    return Ok(());
  };
  if bytes.len() > max_frame {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "max_stream_frame_size",
        format!(
          "the reliable-stream frame ceiling ({max_frame}) is too small to carry the local \
           node's minimal push/pull frame ({} bytes): every join / anti-entropy exchange would \
           be rejected by the receiver's frame-length gate",
          bytes.len()
        ),
      ),
    ));
  }
  Ok(())
}

/// Trial-validate an [`EncryptionOptions`](memberlist_proto::EncryptionOptions)
/// for usability in THIS build BEFORE it is applied as the live policy.
///
/// A keyring naming an AEAD algorithm whose backend feature is not compiled in
/// (e.g. a `SecretKey::Aes128` key in a build without the `aes-gcm` feature) is
/// constructible, but every later `encrypt_gossip` would return
/// [`UnsupportedAlgorithm`](memberlist_proto::EncryptionError::UnsupportedAlgorithm)
/// (gossip datagram dropped) and every reliable-stream encode would fail — the
/// cluster silently breaks after "successfully" enabling encryption.
///
/// EVERY key in the ring is probed — the primary AND all secondaries — not just
/// the primary: the primary is used to encrypt, but inbound frames are decrypted
/// by trial-matching the variant of EVERY key in the ring, so a supported primary
/// paired with an unsupported-algorithm secondary (a common key-rotation state)
/// would silently drop every frame a peer encrypted under that secondary. Each
/// key is probed with a trial encryption of a tiny payload through the EXISTING
/// wire API ([`memberlist_proto::encode_encrypted_frame`]); a backend not built
/// into this binary surfaces `UnsupportedAlgorithm` exactly as the live
/// `encrypt_gossip` / reliable-stream encode / decrypt-trial would. The result
/// bytes are discarded — this is a usability probe, not a real send. The whole
/// policy is rejected ATOMICALLY if ANY key is unsupported (the caller leaves
/// the live policy unchanged); a disabled (no keyring) policy is always usable.
/// Returns the wire [`EncryptionError`](memberlist_proto::EncryptionError) when
/// the policy is unusable so the caller can reject the reconfiguration instead
/// of acking a false `Ok`.
pub(crate) fn validate_encryption_options(
  opts: &memberlist_proto::EncryptionOptions,
) -> Result<(), memberlist_proto::EncryptionError> {
  let Some(keyring) = opts.keyring() else {
    // No keyring ⇒ encryption disabled ⇒ always usable (identity codec).
    return Ok(());
  };
  // Probe the primary followed by every secondary, in decrypt-trial order. The
  // first key whose algorithm's backend is absent surfaces `UnsupportedAlgorithm`
  // and aborts the whole validation — the live policy is never swapped.
  for key in core::iter::once(keyring.primary_ref()).chain(keyring.secondaries()) {
    memberlist_proto::encode_encrypted_frame(key.algorithm(), key, b"")?;
  }
  Ok(())
}

/// Reject an `initial_meta` larger than the effective meta cap
/// (`min(meta_max_size, Meta::MAX_SIZE)`).
///
/// The machine's `Endpoint::new` only `debug_assert`s `initial_meta <=
/// meta_max_size`, so a release build would otherwise start with a local meta
/// the cluster's agreed cap forbids — and one the reliable-frame floor
/// ([`validate_stream_frame_for_identity`]) under-counts, since that floor sizes
/// the node's minimal PushPull for a `meta_max_size`-sized meta. An oversized
/// initial meta frames a larger PushPull than the floor reserves for, so a
/// `max_stream_frame_size` that passes construction could still have the first
/// join / anti-entropy frame rejected at the receiver's frame-length gate.
/// Reject it fail-fast, before any driver is spawned.
pub(crate) fn validate_initial_meta(
  opts: &MemberlistOptions,
) -> Result<(), crate::error::MemberlistError> {
  let Some(meta) = opts.initial_meta() else {
    return Ok(());
  };
  let cap = opts
    .meta_max_size()
    .unwrap_or(memberlist_proto::config::DEFAULT_META_MAX_SIZE)
    .min(Meta::MAX_SIZE);
  if meta.as_bytes().len() > cap {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "initial_meta",
        format!(
          "initial_meta ({} bytes) exceeds the effective meta cap ({cap} bytes = \
           min(meta_max_size, Meta::MAX_SIZE)): raise meta_max_size or shrink the initial meta",
          meta.as_bytes().len()
        ),
      ),
    ));
  }
  Ok(())
}

/// Validate the configured `initial_local_state` against the reliable-stream
/// frame budget.
///
/// The initial local-state snapshot rides every push/pull exchange (including
/// the initial join) as the PushPull `user_data`, and receivers reject any
/// reliable-stream frame whose declared length exceeds
/// [`max_stream_frame_size`](memberlist_proto::config::EndpointOptions::max_stream_frame_size).
/// A snapshot whose framed PushPull would exceed that cap is
/// deterministically untransmittable — every push/pull carrying it would be
/// rejected and the application state would never reach a peer. Reject it at
/// `Memberlist::new` (fail-fast, before any socket is bound) rather than
/// accept-then-silently-fail, mirroring the runtime
/// [`set_local_state`](crate::Memberlist::set_local_state) setter's check.
///
/// The snapshot is validated against the EFFECTIVE cap: the configured
/// [`max_stream_frame_size`](MemberlistOptions::max_stream_frame_size) override
/// when set, else the machine default
/// ([`DEFAULT_MAX_STREAM_FRAME_SIZE`](memberlist_proto::config::DEFAULT_MAX_STREAM_FRAME_SIZE)).
/// That is exactly the ceiling `apply_memberlist_options` installs into every
/// `Transport::run`'s `EndpointOptions`, so the up-front check matches the
/// runtime frame-length gate.
pub(crate) fn validate_initial_local_state<I, A>(
  opts: &MemberlistOptions,
) -> Result<(), crate::error::MemberlistError>
where
  I: memberlist_proto::Data,
  A: memberlist_proto::Data,
{
  if let Some(state) = opts.initial_local_state() {
    let cap = opts
      .max_stream_frame_size()
      .unwrap_or(memberlist_proto::config::DEFAULT_MAX_STREAM_FRAME_SIZE);
    memberlist_proto::endpoint::validate_local_state_snapshot::<I, A>(state, cap)
      .map_err(|e| crate::error::MemberlistError::PayloadTooLarge(e.to_string()))?;
  }
  Ok(())
}

/// Validate a configured `max_stream_frame_size` against the wire envelope.
///
/// The reliable-stream frame ceiling bounds the declared length of every
/// push/pull and large-user-message frame a receiver will accept. Two bounds
/// are enforced fail-fast at `Memberlist::new` (mirroring the reject-not-clamp
/// `gossip_mtu` doctrine) rather than constructing an `Ok` node that later
/// silently fails:
///
/// - Zero rejects EVERY reliable frame, so the node could never complete a
///   push/pull (join, periodic anti-entropy) nor receive any reliable user
///   message — a deterministically broken node.
/// - Above `u32::MAX` is rejected: reliable frame lengths are `u32`-encoded on
///   the wire (the framing layer decodes the length as a `u32` varint and
///   rejects a body beyond `u32::MAX`; the receive path rejects any declared
///   length over `u32::MAX` REGARDLESS of this cap). A ceiling above the wire
///   envelope is unreachable as a receive gate and lets a locally-built frame
///   whose body lands in `(u32::MAX, cap]` fail to encode at runtime. Capping at
///   `u32::MAX` also keeps the saturating `4 * cap` observation byte budget well
///   within `u64`, so the byte backstop stays effective.
///
/// No fixed FLOOR above zero is imposed. A snapshot-bearing node is already
/// bounded by [`validate_initial_local_state`] (the machine reserves
/// ~1 MiB, `LOCAL_STATE_FRAME_BUDGET`, for the membership states the snapshot's
/// PushPull also carries, so a too-small cap with a snapshot is rejected). A
/// node WITHOUT a snapshot legitimately runs with small caps (a constrained
/// deployment), and the only hard constraint — the live membership fitting one
/// push/pull frame — grows dynamically with the cluster, so any fixed floor
/// large enough to be useful would reject valid small-frame configurations.
pub(crate) fn validate_max_stream_frame_size(
  opts: &MemberlistOptions,
) -> Result<(), crate::error::MemberlistError> {
  let Some(size) = opts.max_stream_frame_size() else {
    return Ok(());
  };
  if size == 0 {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "max_stream_frame_size",
        "the reliable-stream frame ceiling must be nonzero: a zero ceiling rejects every \
         reliable frame, so the node can never complete a push/pull (join / anti-entropy) or \
         receive a reliable user message"
          .to_string(),
      ),
    ));
  }
  // Reliable frame lengths are u32 on the wire; a cap above that is unreachable
  // as a receive gate and would let a local frame above u32::MAX fail to encode.
  if size > u32::MAX as usize {
    return Err(crate::error::MemberlistError::InvalidOption(
      crate::error::InvalidOption::new(
        "max_stream_frame_size",
        format!(
          "the reliable-stream frame ceiling must not exceed the u32 wire limit ({}): reliable \
           frame lengths are u32-encoded, so a larger cap is unreachable as a receive gate and a \
           locally-built frame above it would fail to encode",
          u32::MAX
        ),
      ),
    ));
  }
  Ok(())
}

/// Layer the [`MemberlistOptions`] overrides onto a freshly-built
/// machine-layer [`EndpointOptions`]. Each unset knob leaves the
/// corresponding `EndpointOptions` default untouched. Called from every
/// `Transport::run` body after it builds its `EndpointOptions::new(id, addr)`.
/// The `gossip_mtu` override is validated up-front in `Memberlist::new` (see
/// [`validate_gossip_mtu`]); by the time it reaches here it is known sendable.
pub(crate) fn apply_memberlist_options<I, A>(
  mut cfg: EndpointOptions<I, A>,
  opts: &MemberlistOptions,
) -> EndpointOptions<I, A> {
  if let Some(mtu) = opts.gossip_mtu() {
    cfg = cfg.with_gossip_mtu(mtu);
  }
  if let Some(size) = opts.meta_max_size() {
    cfg = cfg.with_meta_max_size(size);
  }
  if let Some(size) = opts.max_stream_frame_size() {
    cfg = cfg.with_max_stream_frame_size(size);
  }
  if let Some(meta) = opts.initial_meta() {
    cfg = cfg.with_initial_meta(meta.cheap_clone());
  }
  if let Some(state) = opts.initial_local_state() {
    cfg = cfg.with_initial_local_state(state.clone());
  }
  cfg
}

/// Decomposed parts of an [`Options<T>`] bundle, as returned by
/// [`Options::into_parts`].
pub type OptionsParts<T> = (
  <T as Transport>::Options,
  MemberlistOptions,
  DriverOptions,
  Option<Box<dyn AliveDelegate<<T as Transport>::Id, SocketAddr>>>,
  Option<Box<dyn MergeDelegate<<T as Transport>::Id, SocketAddr>>>,
);

/// Per-`Memberlist` umbrella options bundle.
///
/// Holds the per-backend transport options (`T::Options`), the SWIM-protocol
/// options (`MemberlistOptions`), the per-driver tuning knobs
/// (`DriverOptions`), and the optional machine admission predicates
/// (`AliveDelegate` / `MergeDelegate`).
///
/// Admission predicates are the machine's `Send + Sync` Sans-I/O traits,
/// distinct from the driver-fired observation [`Delegate`](crate::Delegate)
/// passed to `Memberlist::new`. An unset predicate (the default) admits
/// every peer / accepts every join merge; `Memberlist::new` installs the
/// supplied predicates into the machine `Endpoint`.
pub struct Options<T: Transport> {
  transport: T::Options,
  memberlist: MemberlistOptions,
  driver: DriverOptions,
  alive_delegate: Option<Box<dyn AliveDelegate<T::Id, SocketAddr>>>,
  merge_delegate: Option<Box<dyn MergeDelegate<T::Id, SocketAddr>>>,
}

impl<T: Transport> Options<T> {
  /// Construct from per-backend transport options + SWIM + driver defaults.
  /// Both admission predicates default to unset (admit all).
  #[inline]
  pub fn new(transport: T::Options) -> Self {
    Self {
      transport,
      memberlist: MemberlistOptions::default(),
      driver: DriverOptions::default(),
      alive_delegate: None,
      merge_delegate: None,
    }
  }

  /// Builder: replace the SWIM-protocol options.
  #[must_use]
  #[inline]
  pub fn with_memberlist(mut self, opts: MemberlistOptions) -> Self {
    self.memberlist = opts;
    self
  }

  /// Builder: replace the driver tuning options.
  #[must_use]
  #[inline]
  pub fn with_driver(mut self, opts: DriverOptions) -> Self {
    self.driver = opts;
    self
  }

  /// Builder: install a custom alive-admission predicate (machine
  /// [`AliveDelegate`]). Default (unset) admits every peer.
  #[must_use]
  #[inline]
  pub fn with_alive_delegate(mut self, d: impl AliveDelegate<T::Id, SocketAddr>) -> Self {
    self.alive_delegate = Some(Box::new(d));
    self
  }

  /// Builder: install a custom merge-admission predicate (machine
  /// [`MergeDelegate`]). Default (unset) accepts every join merge.
  #[must_use]
  #[inline]
  pub fn with_merge_delegate(mut self, d: impl MergeDelegate<T::Id, SocketAddr>) -> Self {
    self.merge_delegate = Some(Box::new(d));
    self
  }

  /// Borrow the per-backend transport options.
  #[inline]
  pub const fn transport(&self) -> &T::Options {
    &self.transport
  }

  /// Borrow the SWIM-protocol options.
  #[inline]
  pub const fn memberlist(&self) -> &MemberlistOptions {
    &self.memberlist
  }

  /// Borrow the driver tuning options.
  #[inline]
  pub const fn driver(&self) -> &DriverOptions {
    &self.driver
  }

  /// Destructure into the parts. Used by `Memberlist::new` to fan out
  /// construction: the transport / SWIM / driver options plus the optional
  /// machine admission predicates the runtime carries into each `run`.
  #[inline]
  pub fn into_parts(self) -> OptionsParts<T> {
    (
      self.transport,
      self.memberlist,
      self.driver,
      self.alive_delegate,
      self.merge_delegate,
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{
    AdvertiseAddrResolver, Delegate, MaybeResolved, Resolver, Transport, TransportRuntime,
  };
  use std::net::SocketAddr;

  struct MockTransport;

  impl Transport for MockTransport {
    type Error = std::io::Error;
    type Id = smol_str::SmolStr;
    type Address = String;
    type Options = ();

    async fn new<RES, AR>(
      _options: Self::Options,
      _resolver: &RES,
      _advertise_resolver: &AR,
    ) -> Result<Self, Self::Error>
    where
      RES: Resolver<Address = Self::Address>,
      AR: AdvertiseAddrResolver,
    {
      unimplemented!("mock — only exercises Options<T> shape")
    }

    fn local_id(&self) -> &Self::Id {
      unimplemented!()
    }

    fn local_address(&self) -> &MaybeResolved<Self::Address, SocketAddr> {
      unimplemented!()
    }

    fn advertise_address(&self) -> &SocketAddr {
      unimplemented!()
    }

    async fn run<D>(self, _runtime: TransportRuntime<Self, D>)
    where
      D: Delegate<Id = Self::Id, Address = SocketAddr>,
    {
      unimplemented!()
    }
  }

  #[test]
  fn options_construction_and_accessors() {
    let opts: Options<MockTransport> = Options::new(());
    let _t: &() = opts.transport();
    let _d: &DriverOptions = opts.driver();
    let _m: &MemberlistOptions = opts.memberlist();
  }

  #[test]
  fn options_builder_chain() {
    let opts: Options<MockTransport> = Options::new(())
      .with_driver(DriverOptions::new())
      .with_memberlist(MemberlistOptions::new());
    let _ = opts.transport(); // Unused: binding only to suppress unused-value lint
  }

  #[test]
  fn validate_driver_options_rejects_deterministic_break_knobs() {
    use core::time::Duration;
    // Baseline (all defaults) is accepted.
    assert!(validate_driver_options(&DriverOptions::new()).is_ok());

    // The four deterministic-break knobs are each rejected at zero.
    let breaks = [
      DriverOptions::new().with_idle_wake_interval(Duration::ZERO),
      DriverOptions::new().with_cmd_fairness_budget(0),
      DriverOptions::new().with_peek_budget(Duration::ZERO),
      DriverOptions::new().with_observation_channel(crate::Channel::Bounded(0)),
    ];
    for opts in breaks {
      assert!(
        matches!(
          validate_driver_options(&opts),
          Err(crate::error::MemberlistError::InvalidOption(_))
        ),
        "a deterministic-break knob at zero must be rejected: {opts:?}"
      );
    }

    // Degrade-but-function knobs at zero, and a positive bounded channel,
    // are accepted.
    assert!(validate_driver_options(&DriverOptions::new().with_iter_drain_cap(0)).is_ok());
    assert!(validate_driver_options(&DriverOptions::new().with_event_queue_cap(0)).is_ok());
    assert!(
      validate_driver_options(
        &DriverOptions::new().with_observation_channel(crate::Channel::Bounded(16))
      )
      .is_ok()
    );
  }

  #[test]
  fn observation_channel_round_trips() {
    // Bounded by default (safe against remote-driven OOM).
    assert_eq!(
      DriverOptions::new().observation_channel(),
      crate::Channel::Bounded(1024)
    );
    // Explicit opt-in to never-drop, and an explicit smaller bound, round-trip.
    assert_eq!(
      DriverOptions::new()
        .with_observation_channel(crate::Channel::Unbounded)
        .observation_channel(),
      crate::Channel::Unbounded
    );
    assert_eq!(
      DriverOptions::new()
        .with_observation_channel(crate::Channel::Bounded(8))
        .observation_channel(),
      crate::Channel::Bounded(8)
    );
  }

  // A scoped/flow-labelled IPv6 advertise address (nonzero `scope_id` or
  // `flowinfo`) is not representable on the compact memberlist wire layout, so
  // every local-node-bearing control packet would fail to encode at runtime.
  // `validate_gossip_mtu_for_identity` builds those packets with the ACTUAL
  // advertise address for the local node, so it must reject such an address at
  // construction with `InvalidAdvertiseAddr` rather than letting the node
  // construct `Ok` and then silently fail every send.
  //
  // The validator is exercised directly (it takes `&local_id`, `&SocketAddr`,
  // `&MemberlistOptions`) rather than end-to-end through `Memberlist::new`:
  // binding a real link-local scoped IPv6 socket is system-dependent and would
  // fail at the OS bind before reaching this check, making an end-to-end test
  // flaky. Calling the validator directly with a constructed scoped address is
  // deterministic and isolates exactly the encodability gate under test.
  #[test]
  fn identity_floor_rejects_scoped_ipv6_advertise_addr() {
    use std::net::{Ipv6Addr, SocketAddrV6};

    let id = smol_str::SmolStr::new("node-a");
    let opts = MemberlistOptions::new();

    // Nonzero scope_id (a link-local `fe80::1%scope` advertise address).
    let scoped_scope = SocketAddr::V6(SocketAddrV6::new(
      Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
      7946,
      0,
      3,
    ));
    match validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &scoped_scope, &opts) {
      Err(crate::error::MemberlistError::InvalidAdvertiseAddr(e)) => {
        assert_eq!(
          e.addr(),
          scoped_scope,
          "carries the rejected advertise addr"
        );
        assert!(
          !e.reason().is_empty(),
          "carries the underlying wire-codec reason"
        );
      }
      other => panic!(
        "a scoped (nonzero scope_id) IPv6 advertise address must be rejected with InvalidAdvertiseAddr, got {other:?}"
      ),
    }

    // Nonzero flowinfo is equally non-encodable.
    let scoped_flow = SocketAddr::V6(SocketAddrV6::new(
      Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
      7946,
      1,
      0,
    ));
    assert!(
      matches!(
        validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &scoped_flow, &opts),
        Err(crate::error::MemberlistError::InvalidAdvertiseAddr(_))
      ),
      "a nonzero-flowinfo IPv6 advertise address must also be rejected"
    );

    // A plain IPv4 advertise address is wire-encodable ⇒ Ok.
    let v4: SocketAddr = "127.0.0.1:7946".parse().unwrap();
    assert!(
      validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &v4, &opts).is_ok(),
      "a normal IPv4 advertise address must construct Ok"
    );

    // An UNSCOPED IPv6 advertise address (flowinfo = scope_id = 0) is the form
    // the compact wire encoder accepts ⇒ Ok.
    let v6_unscoped = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 7946, 0, 0));
    assert!(
      validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &v6_unscoped, &opts).is_ok(),
      "an unscoped IPv6 advertise address must construct Ok"
    );
  }

  // The resolved advertise address is the local node's published contact: an
  // unspecified / multicast / IPv4-broadcast IP or a zero port encodes fine but
  // is undialable, so `validate_advertise_addr` must reject it (the node would
  // otherwise join as an unreachable member). Loopback / private / global
  // unicast must stay valid. Exercised directly — it is the same function
  // `Memberlist::new` calls on `transport.advertise_address()`, with no socket
  // bind, so a zero port (which the post-readback address never carries) and a
  // multicast/broadcast IP (which the OS would refuse to bind) can both be
  // tested deterministically.
  #[test]
  fn validate_advertise_addr_rejects_unroutable_contacts() {
    use crate::error::MemberlistError::InvalidAdvertiseAddr;

    // Unspecified (the wildcard-bind footgun): 0.0.0.0 and [::].
    for addr in ["0.0.0.0:7946", "[::]:7946"] {
      let a: SocketAddr = addr.parse().unwrap();
      assert!(
        matches!(validate_advertise_addr(&a), Err(InvalidAdvertiseAddr(e)) if e.addr() == a),
        "unspecified advertise {addr} must be rejected"
      );
    }

    // Multicast: an IPv4 group and an IPv6 link-local-all-nodes group.
    for addr in ["224.0.0.1:7946", "[ff02::1]:7946"] {
      let a: SocketAddr = addr.parse().unwrap();
      assert!(
        matches!(validate_advertise_addr(&a), Err(InvalidAdvertiseAddr(_))),
        "multicast advertise {addr} must be rejected"
      );
    }

    // IPv4 broadcast.
    let bcast: SocketAddr = "255.255.255.255:7946".parse().unwrap();
    assert!(
      matches!(
        validate_advertise_addr(&bcast),
        Err(InvalidAdvertiseAddr(_))
      ),
      "broadcast advertise must be rejected"
    );

    // Zero port (a resolved advertise should never carry it, but reject if it
    // ever surfaces) on an otherwise-valid loopback IP.
    let port0: SocketAddr = "127.0.0.1:0".parse().unwrap();
    assert!(
      matches!(
        validate_advertise_addr(&port0),
        Err(InvalidAdvertiseAddr(_))
      ),
      "a zero port must be rejected even on a routable IP"
    );

    // ACCEPTED: loopback (v4 + v6), private, and global unicast.
    for addr in [
      "127.0.0.1:7946",   // IPv4 loopback
      "10.0.0.5:7946",    // private (10/8)
      "192.168.1.7:7946", // private (192.168/16)
      "203.0.113.10:443", // global unicast (TEST-NET-3)
      "[::1]:7946",       // IPv6 loopback (unscoped)
    ] {
      let a: SocketAddr = addr.parse().unwrap();
      assert!(
        validate_advertise_addr(&a).is_ok(),
        "a usable unicast advertise {addr} must be accepted"
      );
    }
  }

  #[test]
  fn validate_max_stream_frame_size_bounds() {
    // Unset is accepted (the machine default applies at `T::run`).
    assert!(validate_max_stream_frame_size(&MemberlistOptions::new()).is_ok());
    // Positive caps within the u32 wire envelope are accepted, INCLUDING small
    // caps — a no-snapshot node legitimately runs with small reliable frames
    // (e.g. the byte-backstop test uses 256 KiB).
    for ok in [256usize, 64 * 1024, u32::MAX as usize] {
      assert!(
        validate_max_stream_frame_size(&MemberlistOptions::new().with_max_stream_frame_size(ok))
          .is_ok(),
        "cap {ok} within the u32 wire envelope must be accepted"
      );
    }
    // Zero rejects every reliable frame — rejected fail-fast.
    assert!(matches!(
      validate_max_stream_frame_size(&MemberlistOptions::new().with_max_stream_frame_size(0)),
      Err(crate::error::MemberlistError::InvalidOption(_))
    ));
    // Above the u32 wire envelope is rejected (unencodable body / unreachable
    // receive gate). Only representable where usize exceeds u32 (64-bit).
    #[cfg(target_pointer_width = "64")]
    {
      for bad in [(u32::MAX as usize) + 1, usize::MAX] {
        assert!(
          matches!(
            validate_max_stream_frame_size(
              &MemberlistOptions::new().with_max_stream_frame_size(bad)
            ),
            Err(crate::error::MemberlistError::InvalidOption(_))
          ),
          "cap {bad} above the u32 wire envelope must be rejected"
        );
      }
    }
  }

  #[test]
  fn validate_initial_local_state_honors_configured_cap() {
    // A ~4 KiB snapshot frames far under the 64 MiB machine default, so with no
    // override it validates.
    let snapshot = Bytes::from(vec![0u8; 4096]);
    assert!(
      validate_initial_local_state::<smol_str::SmolStr, SocketAddr>(
        &MemberlistOptions::new().with_initial_local_state(snapshot.clone())
      )
      .is_ok()
    );
    // The SAME snapshot is rejected against a 256-byte configured ceiling —
    // proving the check honors the configured `max_stream_frame_size`, not the
    // machine default (under which 4 KiB would pass). This is the regression
    // guard for the validator that previously always checked the default.
    assert!(matches!(
      validate_initial_local_state::<smol_str::SmolStr, SocketAddr>(
        &MemberlistOptions::new()
          .with_max_stream_frame_size(256)
          .with_initial_local_state(snapshot)
      ),
      Err(crate::error::MemberlistError::PayloadTooLarge(_))
    ));
  }

  #[test]
  fn validate_stream_frame_for_identity_rejects_tiny_caps() {
    let id = smol_str::SmolStr::new("sf-node");
    let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap();
    // A cap of 1 cannot carry the local node's minimal push/pull frame — the
    // node would construct Ok yet never complete a reliable membership exchange.
    assert!(matches!(
      validate_stream_frame_for_identity(
        &id,
        &addr,
        &MemberlistOptions::new().with_max_stream_frame_size(1)
      ),
      Err(crate::error::MemberlistError::InvalidOption(_))
    ));
    // A small-but-realistic cap (256 KiB — the no-snapshot byte-backstop config)
    // and the machine default both genuinely fit the node, so both are accepted:
    // the floor is dynamic, not a fixed magic-number that would reject 256 KiB.
    assert!(
      validate_stream_frame_for_identity(
        &id,
        &addr,
        &MemberlistOptions::new().with_max_stream_frame_size(256 * 1024)
      )
      .is_ok()
    );
    assert!(validate_stream_frame_for_identity(&id, &addr, &MemberlistOptions::new()).is_ok());

    // The floor reserves room for a `meta_max_size`-sized meta (worst case), so
    // a cap that fits the node's CURRENT (empty) meta but NOT its meta_max_size
    // ceiling is rejected — closing the bypass where a later `update_node_metadata`
    // grows the meta past the cap. A 60 KiB ceiling needs a ~60 KiB frame even
    // with an empty initial meta, so a 50 KiB cap is rejected while a 128 KiB cap
    // (which fits the worst-case meta) is accepted.
    assert!(matches!(
      validate_stream_frame_for_identity(
        &id,
        &addr,
        &MemberlistOptions::new()
          .with_meta_max_size(60 * 1024)
          .with_max_stream_frame_size(50 * 1024)
      ),
      Err(crate::error::MemberlistError::InvalidOption(_))
    ));
    assert!(
      validate_stream_frame_for_identity(
        &id,
        &addr,
        &MemberlistOptions::new()
          .with_meta_max_size(60 * 1024)
          .with_max_stream_frame_size(128 * 1024)
      )
      .is_ok()
    );
  }

  #[test]
  fn validate_initial_meta_rejects_oversized() {
    // Unset, and an initial meta within meta_max_size, are accepted.
    assert!(validate_initial_meta(&MemberlistOptions::new()).is_ok());
    assert!(
      validate_initial_meta(
        &MemberlistOptions::new()
          .with_meta_max_size(64)
          .with_initial_meta(Meta::try_from(Bytes::from(vec![0u8; 64])).unwrap())
      )
      .is_ok()
    );
    // An initial meta larger than meta_max_size is rejected fail-fast — the
    // machine only debug-asserts this, so the release check is what keeps the
    // reliable-frame floor (sized for meta_max_size) from under-counting the
    // actual local PushPull.
    assert!(matches!(
      validate_initial_meta(
        &MemberlistOptions::new()
          .with_meta_max_size(64)
          .with_initial_meta(Meta::try_from(Bytes::from(vec![0u8; 128])).unwrap())
      ),
      Err(crate::error::MemberlistError::InvalidOption(_))
    ));
  }
}
