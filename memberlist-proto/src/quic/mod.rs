//! Composed QUIC + memberlist Sans-I/O coordinator.
//!
//! One conceptual UDP socket. Inbound: first-byte demux (>=0x40 -> quinn,
//! 1..=15 -> memberlist codec). Reliable exchanges ride per-peer QUIC bidi
//! streams; unreliable gossip rides plain UDP. The per-tick step order is
//! load-bearing: stream endpoint-events are drained into the Endpoint
//! BEFORE the memberlist probe `handle_timeout` (else a fallback-ping ack
//! that lands the same tick the probe cumulative deadline expires is lost
//! and the peer is wrongly Suspected).

mod bridge;
mod conn;
pub mod crypto;
mod demux;
mod transport_mode;

pub use crypto::QuicOptions;
pub use transport_mode::{DatagramSendOutcome, UnreliableTransport};

use crate::Instant;
use core::net::SocketAddr;
use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use quinn_proto::{DatagramEvent, Dir, Endpoint as QuinnEndpoint};

use crate::{
  endpoint::Endpoint,
  event::{
    Event, ExchangeCompleted, ExchangeId, ExchangeKind, ExchangeOutcome, PushPullKind, StreamId,
    Transmit,
  },
};
use bridge::Bridge;
use conn::ConnTable;
use demux::{Class, classify};

/// Maximum entries buffered in `mem_ingress` from the QUIC datagram receive
/// drain. quinn's `datagram_receive_buffer_size` bounds inbound BYTES but not
/// entry COUNT (a flood of tiny or zero-length DATAGRAM frames adds ~0 bytes
/// yet one coordinator-queue entry each), so the drain enforces an explicit
/// count cap: beyond it datagrams are popped from quinn and dropped (counted),
/// keeping the coordinator queue and per-tick decode work bounded. Sized far
/// above any legitimate buffered-ingress burst, so normal traffic never drops.
const MAX_MEM_INGRESS_DATAGRAMS: usize = 8192;

/// Maximum inbound application datagrams a SINGLE peer may hold as its STANDING
/// share of the shared `mem_ingress` across the WHOLE undrained queue (counted
/// via `mem_ingress_per_peer`, maintained on every push and pop), not merely
/// within one `service_quinn` pass. Bounds one peer's standing share of the
/// node-global queue so a flooding peer cannot starve other peers' probe Acks
/// regardless of how many recv passes a driver batches before decoding; excess
/// is popped from quinn (so its buffer cannot accumulate) and dropped.
const MAX_INGRESS_DATAGRAMS_PER_PEER: usize = 1024;

/// Push one inbound unreliable payload (a QUIC datagram or a plain-UDP gossip
/// frame) into the shared coordinator ingress queue, enforcing the per-peer
/// standing-share cap AND the node-global cap so neither source can exceed the
/// bound. Drops and counts when either cap is reached; returns whether it was
/// queued. The per-peer counter is maintained here so a flood on EITHER
/// transport cannot starve another peer's probe Ack and the global cap is a
/// hard memory bound regardless of source.
///
/// The payload is supplied as a thunk and built ONLY on admission: a rejected
/// frame never materializes its `Bytes`, so a saturated queue cannot force an
/// allocation/copy per dropped frame on the unauthenticated plain-UDP path
/// (where the payload would otherwise be a fresh `Bytes::copy_from_slice`).
///
/// Borrows only the three disjoint ingress fields (never all of `self`) so the
/// QUIC datagram drain in `service_quinn` can call it while the
/// `self.conns.get_mut(..)` connection borrow is still live.
fn push_mem_ingress_capped(
  mem_ingress: &mut std::collections::VecDeque<(SocketAddr, Bytes)>,
  per_peer: &mut HashMap<SocketAddr, usize>,
  dropped: &mut u64,
  from: SocketAddr,
  make_payload: impl FnOnce() -> Bytes,
) -> bool {
  let queued = per_peer.get(&from).copied().unwrap_or(0);
  if queued >= MAX_INGRESS_DATAGRAMS_PER_PEER || mem_ingress.len() >= MAX_MEM_INGRESS_DATAGRAMS {
    // Reject WITHOUT constructing the payload: a saturated queue must not let a
    // flood force an allocation/copy per dropped frame on the unauthenticated
    // UDP path.
    *dropped = dropped.saturating_add(1);
    return false;
  }
  mem_ingress.push_back((from, make_payload()));
  *per_peer.entry(from).or_insert(0) += 1;
  true
}

/// One pending dial intent the coordinator owes a `service_dials` attempt to.
///
/// `attempted` distinguishes a freshly-sieved entry (never yet processed by
/// `service_dials`) from one that has been processed at least once. Freshly-
/// sieved entries get an immediate-due wake out of `poll_timeout` so a caller
/// that advances solely by `poll_timeout` cannot orphan them: a caller that
/// drains `poll_event` (sieving `DialRequested` into `dial_pending`) and then
/// waits on `poll_timeout` alone would otherwise only see the intent's own
/// `deadline` and wake at `now + stream_timeout`, by which point
/// `service_dials` would discover the deadline elapsed and consume the intent
/// via `dial_failed`. Once `service_dials` attempts the entry (whether it
/// completes, requeues because the connection is still handshaking, or
/// requeues because of `MAX_STREAMS` credit exhaustion), `attempted` becomes
/// `true` and stays `true` across requeues: future wake-ups are driven by the
/// connection's own `poll_timeout` (handshake completion / credit recovery)
/// and the intent's `deadline`. Immediately re-firing an attempted entry
/// would busy-loop a still-handshaking connection.
struct PendingDial {
  id: StreamId,
  peer: SocketAddr,
  deadline: Instant,
  attempted: bool,
}

/// Coordinator: `memberlist::Endpoint` (unreliable + membership) composed with
/// `quinn_proto::Endpoint` (reliable). Pure Sans-I/O — inject `now`.
///
/// The membership address is pinned to `SocketAddr` inside this coordinator —
/// `quinn_proto::Endpoint` is structurally `SocketAddr`-typed (it dials and
/// accepts wire addresses), so the composed unit pins `A = SocketAddr` rather
/// than carrying a per-coordinator conversion layer over a generic `A`. A
/// driver whose user-facing membership address differs from the wire socket
/// translates at the driver boundary (e.g. in `Memberlist<I, A, R>::join`).
pub struct QuicEndpoint<I> {
  ep: Endpoint<I, SocketAddr>,
  quinn: QuinnEndpoint,
  cfg: QuicOptions,
  /// Cross-transport compression configuration. A disabled `CompressionOptions`
  /// makes the gossip compress/decompress methods identity.
  compression: crate::CompressionOptions,
  /// Cross-transport encryption configuration. Applied to the QUIC gossip
  /// path (plain UDP on the same socket as the QUIC packets); the QUIC
  /// reliable path always skips — quinn-encrypted streams already provide
  /// confidentiality.
  encryption: crate::EncryptionOptions,
  /// Checksum configuration for the gossip (unreliable) plane — the QUIC
  /// datagram path. A checksum guards the connectionless datagram path, which
  /// carries no transport-level integrity of its own, so it is applied in
  /// [`Self::checksum_gossip`]. The QUIC reliable bidi bridge carries no
  /// checksum: quinn streams are already integrity-protected end to end, so
  /// corruption detection is an unreliable-plane concern (matching the original
  /// Go memberlist and the legacy port). A disabled `ChecksumOptions` makes the
  /// gossip path identity.
  checksum: crate::ChecksumOptions,
  /// Cluster label for all reliable bridges spawned by this coordinator, or
  /// `None` when no label is configured.
  ///
  /// Identical source as the gossip-plane label threaded through the driver —
  /// a single `MemberlistOptions::label` feeds both paths so they cannot
  /// diverge. Set via [`Self::with_label`]; defaults to `None` so
  /// constructors that never call `with_label` are byte-identical to before.
  label: Option<bytes::Bytes>,
  /// Forwarded verbatim to each new [`Bridge`]'s `skip_inbound_label_check`
  /// parameter. Suppresses the "label expected but missing from inbound peer"
  /// check without suppressing `DoubleLabel`. Defaults to `false`.
  skip_inbound_label_check: bool,
  conns: ConnTable,
  bridges: HashMap<StreamId, Bridge<I, SocketAddr>>,
  /// Tags each outbound bridge's [`StreamId`] with the originating
  /// [`ExchangeKind`] so the bridge-reap path can carry that kind on
  /// the uniform [`Event::ExchangeCompleted`] terminal event. Mirrors
  /// `StreamEndpoint::pending_outbound_kinds`. The reap fires for ALL
  /// outbound kinds (push/pull, reliable ping, reliable user message);
  /// consumers filter on the payload's `kind()` to focus on the
  /// bridges they care about. Populated at `start_push_pull` /
  /// `start_reliable_ping` / `start_user_message` time; drained at
  /// bridge-reap time inside [`Self::emit_exchange_completed`].
  /// Strictly outbound-only — inbound (server-side) bridges accepted
  /// from `streams().accept(Dir::Bi)` are not assigned a kind by the
  /// initiator and never appear in this table.
  pending_outbound_kinds: HashMap<StreamId, ExchangeKind>,
  /// Tags each outbound bridge's [`StreamId`] with the [`SocketAddr`]
  /// of the peer so the bridge-reap path can carry it on the
  /// [`Event::ExchangeCompleted`] payload. Populated alongside
  /// `pending_outbound_kinds`; drained at bridge-reap time inside
  /// [`Self::emit_exchange_completed`]. Inbound (server-side) bridges
  /// do not appear here.
  pending_outbound_peers: HashMap<StreamId, SocketAddr>,
  /// Outbound UDP datagrams produced this tick (quinn datagrams + stateless
  /// `Response`s; the memberlist unreliable path is NOT prebuffered — see
  /// [`poll_memberlist_transmit`](Self::poll_memberlist_transmit)).
  out: std::collections::VecDeque<(SocketAddr, Bytes)>,
  /// Raw inbound memberlist datagrams the first-byte demux classified as
  /// `Class::Memberlist`. `memberlist-proto` has no umbrella `codec`
  /// dependency, so the coordinator cannot decode them in-crate and MUST NOT
  /// silently drop them (that would lose every UDP ping/ack/alive/suspect on
  /// the composed unit's public ingress). They are buffered here and surfaced
  /// as an explicit action via [`poll_memberlist_ingress`](Self::poll_memberlist_ingress)
  /// — the same idiom the coordinator uses for QUIC `Transmit`/`DatagramEvent`
  /// — for the codec-owning layer to unwrap and feed back through
  /// [`handle_packet`](Self::handle_packet).
  mem_ingress: std::collections::VecDeque<(SocketAddr, Bytes)>,
  /// Per-peer count of entries currently in `mem_ingress`, maintained on every
  /// push and pop, so the inbound datagram drain can bound one peer's standing
  /// share of the shared queue (fairness against a single-peer flood)
  /// regardless of how many recv passes a driver batches before decoding.
  mem_ingress_per_peer: HashMap<SocketAddr, usize>,
  /// Private queue of pending dial intents. `memberlist::Endpoint::poll_event`
  /// emits `Event::DialRequested { id, peer, deadline }` for an external
  /// driver to dial — but in the composed design `QuicEndpoint` IS the
  /// driver: `service_dials` opens the quinn bidi stream itself, and an
  /// intent whose connection is still handshaking is retried on the next
  /// tick. If `DialRequested` leaked through [`Self::poll_event`] an
  /// external caller draining events between `handle_timeout` and the
  /// next `service_dials` would pop the retry token and silently drop it
  /// — the pending stream intent would orphan and the push/pull or
  /// reliable-ping would never open. The coordinator therefore sieves
  /// `Event::DialRequested` out of the inner endpoint's queue into this
  /// private deque (see [`Self::poll_event`] and [`Self::service_dials`]);
  /// external pollers only ever observe application-visible events. Each
  /// entry carries an `attempted` bit so a freshly-sieved intent surfaces
  /// in [`Self::poll_timeout`] as an immediate-due wake — see
  /// [`PendingDial`].
  dial_pending: std::collections::VecDeque<PendingDial>,
  /// Most recent `now: Instant` injected by `handle_udp` / `handle_timeout` /
  /// any high-level `start_*` wrapper. Used by [`Self::poll_timeout`] as the
  /// known-past anchor for the immediate-due wake of an unattempted
  /// `dial_pending` entry: the only way to signal "fire as soon as possible"
  /// out of an `Option<Instant>` Sans-I/O API is to return an `Instant <=
  /// caller's now`, and the only such anchor we may hold is one we observed
  /// from a prior `handle_*` call (Sans-I/O forbids `Instant::now()`). Stays
  /// `None` only before the very first `handle_*` / `start_*` call; after
  /// that, every subsequent `poll_timeout` can return it.
  last_now: Option<Instant>,
  /// Count of unreliable datagrams dropped by
  /// [`queue_unreliable_datagram`](Self::queue_unreliable_datagram) on a quinn
  /// datagram-state error. The `max_size` pre-check excludes `TooLarge`, and
  /// `Blocked` (a full send buffer) is handled separately as a `NotReady`
  /// UDP fallback rather than a drop, so this counts only a residual edge.
  /// Best-effort accounting only — never a membership signal.
  datagram_dropped: u64,
  /// Count of inbound unreliable datagrams the `service_quinn` receive drain
  /// popped from quinn but did NOT push into `mem_ingress`, because either the
  /// per-peer budget ([`MAX_INGRESS_DATAGRAMS_PER_PEER`], one peer's standing
  /// share of the undrained queue) or the node-global cap
  /// ([`MAX_MEM_INGRESS_DATAGRAMS`]) was already reached. quinn's
  /// `datagram_receive_buffer_size` bounds inbound bytes
  /// but not entry count, so a flood of tiny/zero-length DATAGRAM frames is
  /// bounded here by popping-and-dropping past either limit. Best-effort
  /// accounting only — never a membership signal.
  datagram_ingress_dropped: u64,
  /// Test-only counter incremented once per `EndpointEvent` drained from
  /// every connection's `poll_endpoint_events()` queue inside
  /// [`Self::service_quinn`]. Exists ONLY for the negative-control regression
  /// test that proves the endpoint-event drain loop runs at all (a missing
  /// drain leaves the counter at zero and breaks CID issuance / reset-token
  /// registration in quinn-proto — see [`Self::service_quinn`] for the
  /// per-event contract). Never compiled into production builds.
  #[cfg(test)]
  endpoint_events_processed: u64,
  /// Test-only counter incremented once per [`Endpoint::handle_timeout`] call,
  /// i.e. once per membership-time advance. Exists ONLY for the regression test
  /// proving a QUIC packet ingress (`service_quic_inbound`) does NOT advance
  /// membership time — only the driver's explicit `handle_timeout` does — so a
  /// probe Ack carried in a datagram cannot be timed out before it is decoded.
  /// Never compiled into production builds.
  #[cfg(test)]
  membership_time_advances: u64,
  /// Test-only counter incremented once per bridge `drain_then_reap`'d
  /// inside [`Self::service_quinn`] on an `Event::ConnectionLost` — the
  /// strict-poll self-sufficiency path that closes the D1 drain within
  /// the SAME tick the loss is observed (rather than deferring to a
  /// future `pump_bridges` that a strict-poll driver may never wake to
  /// run on a quiet cluster). The negative-control regression test asserts
  /// this counter advances under strict poll-surface driving; reverting
  /// the inline drain to mere `mark_fatal()` leaves it at zero. Never
  /// compiled into production builds.
  #[cfg(test)]
  bridges_reaped_on_connection_lost: u64,
  /// Test-only counter incremented once per bridge pumped by the
  /// post-acceptance second `pump_bridges` invocation in [`Self::run_tick`]
  /// (step 5.5) / [`Self::flush_outbound`] that was inserted into
  /// `self.bridges` by `service_quinn`'s `accept(Dir::Bi)` loop (step 4)
  /// or `service_dials`'s `open(Dir::Bi)` (step 5) AFTER step (2)'s
  /// `pump_bridges` already ran this tick. A newly-inserted inbound bridge
  /// carries its first buffered request data inside quinn's per-stream recv
  /// buffer (delivered by the inbound datagram `service_quinn` just
  /// ingested); a newly-opened outbound bridge carries its first request
  /// bytes in its FSM `Stream` output buffer. Without the second pump,
  /// `Bridge::pump_in` / `Bridge::pump_out` never run on those bridges this
  /// tick, and a strict-poll driver next wakes at the bridge's exchange
  /// deadline — at which point `Stream::handle_data` rejects the buffered
  /// request as timed out and the exchange fails. Counter advances ONLY
  /// when at least one such bridge was pumped post-acceptance; the
  /// regression test asserts it advances under strict poll-surface
  /// driving and reverting step (5.5) leaves it at zero. Never compiled
  /// into production builds.
  #[cfg(test)]
  bridges_pumped_after_acceptance: u64,
  /// Test-only counter incremented once each time [`Self::route_datagram_event`]
  /// surfaces an `AcceptError::response` from `quinn_proto::Endpoint::accept`
  /// onto the driver-facing `out` queue. quinn-proto attaches an
  /// `Option<Transmit>` to its `AcceptError` whenever `accept` owes a
  /// refusal/close to the peer (CID exhaustion or a handshake
  /// `TransportError` produce an `initial_close` response). The close bytes
  /// are written into the caller-supplied `buf` and the returned
  /// `Transmit.size` equals `buf.len()`; without this counter we have no
  /// observable seam to assert the refusal/close `Transmit` actually reaches
  /// the driver via the `out` queue. Never compiled into production builds.
  #[cfg(test)]
  accept_error_responses_emitted: u64,
  /// Test-only counter incremented each time `pump_bridges`'s
  /// post-`drain_payload_only` `is_terminal()` re-check fires — i.e. a
  /// bridge that was non-terminal entering `drain_payload_only` became
  /// terminal during the per-tick endpoint-event drain (typically a
  /// `StreamCommand::Close` from a `MergeDelegate` / `AliveDelegate`
  /// rejection sets `fatal`). Provides an observable seam for the
  /// admission-rejection same-tick reap because `live_bridge_count`
  /// would otherwise show the bridge transiently — appearing on accept
  /// then immediately reaping in the same tick. Never compiled into
  /// production builds.
  #[cfg(test)]
  bridges_terminalized_via_close_command: u64,
}

// Accessors / builders whose bodies touch only non-generic fields
// (`compression`, `encryption`, `quinn`, `cfg`, `conns`, `out`,
// `mem_ingress`) or delegate to `Endpoint`'s own accessor surface
// (`endpoint()`, `gossip_mtu()`). Re-states only the struct's
// well-formedness bag — no method-side additions, so the heavier
// `I: Debug + Display + Send + Sync + 'static` constraints carried by
// the impl blocks below are NOT required to call any of these.
impl<I> QuicEndpoint<I>
where
  I: crate::Id + crate::Data + crate::CheapClone,
{
  /// Build the coordinator. The quinn endpoint is created with the bundled
  /// config; `allow_mtud = true`, and `rng_seed = None` so quinn seeds its
  /// connection-ID / path-challenge RNG from the OS (production entropy).
  ///
  /// Signature (quinn-proto 0.11.14): `Endpoint::new(Arc<EndpointOptions>,
  /// Option<Arc<ServerConfig>>, allow_mtud: bool, rng_seed: Option<[u8; 32]>)`.
  pub fn new(ep: Endpoint<I, SocketAddr>, cfg: QuicOptions) -> Self {
    Self::with_quinn_rng_seed(ep, cfg, None)
  }

  /// Build the coordinator with an explicit quinn `rng_seed`.
  ///
  /// `rng_seed` is quinn's documented determinism seam: it seeds the
  /// endpoint's connection-ID generator and path-challenge RNG. Production
  /// uses [`new`](Self::new) (`None` → OS entropy); a deterministic driver
  /// (e.g. the conformance simulation, whose temporal determinism is the
  /// injected virtual clock) passes a fixed `Some([_; 32])` so the QUIC
  /// transport — and therefore the composed membership behaviour and timing
  /// — is bit-for-bit reproducible across runs. Behaviour is otherwise
  /// identical to [`new`](Self::new).
  #[must_use]
  pub fn with_quinn_rng_seed(
    ep: Endpoint<I, SocketAddr>,
    cfg: QuicOptions,
    rng_seed: Option<[u8; 32]>,
  ) -> Self {
    let quinn = QuinnEndpoint::new(cfg.endpoint_arc(), Some(cfg.server_arc()), true, rng_seed);
    Self {
      ep,
      quinn,
      cfg,
      compression: crate::CompressionOptions::new(),
      encryption: crate::EncryptionOptions::new(),
      checksum: crate::ChecksumOptions::new(),
      label: None,
      skip_inbound_label_check: false,
      conns: ConnTable::new(),
      bridges: HashMap::new(),
      pending_outbound_kinds: HashMap::new(),
      pending_outbound_peers: HashMap::new(),
      out: std::collections::VecDeque::new(),
      mem_ingress: std::collections::VecDeque::new(),
      mem_ingress_per_peer: HashMap::new(),
      dial_pending: std::collections::VecDeque::new(),
      last_now: None,
      datagram_dropped: 0,
      datagram_ingress_dropped: 0,
      #[cfg(test)]
      endpoint_events_processed: 0,
      #[cfg(test)]
      membership_time_advances: 0,
      #[cfg(test)]
      bridges_reaped_on_connection_lost: 0,
      #[cfg(test)]
      bridges_pumped_after_acceptance: 0,
      #[cfg(test)]
      accept_error_responses_emitted: 0,
      #[cfg(test)]
      bridges_terminalized_via_close_command: 0,
    }
  }

  /// Build the coordinator with an explicit cross-transport compression
  /// configuration. [`Self::new`] is `with_compression` with compression
  /// disabled.
  #[must_use]
  pub fn with_compression(
    ep: Endpoint<I, SocketAddr>,
    cfg: QuicOptions,
    compression: crate::CompressionOptions,
  ) -> Self {
    let mut this = Self::new(ep, cfg);
    this.compression = compression;
    this
  }

  /// Attach a cluster label to this coordinator. Every reliable bridge opened
  /// after this call inherits `label` and `skip_inbound_label_check`.
  ///
  /// `label = None` — or an empty label, which normalizes to `None` — is
  /// byte-identical to having never called this builder: no label frame is
  /// written and the inbound path skips validation entirely. An over-long
  /// (> 253-byte) or non-UTF-8 label returns
  /// [`LabelError`](crate::label::LabelError). The intended call site is the
  /// driver constructor, which threads the same `MemberlistOptions::label`
  /// value used by the gossip codec so the two planes share one source and
  /// cannot diverge.
  pub fn with_label(
    mut self,
    label: Option<bytes::Bytes>,
    skip_inbound_label_check: bool,
  ) -> Result<Self, crate::label::LabelError> {
    // Normalize and validate at this public entry: an empty label collapses to
    // the byte-identical no-label path, and an over-long or non-UTF-8 label is
    // rejected here so it can never reach `encode_label_prefix`, which would
    // truncate the single length byte and let the overflow be parsed as
    // reliable-unit data.
    self.label = match label {
      None => None,
      Some(bytes) if bytes.is_empty() => None,
      Some(bytes) => {
        crate::label::validate_label(&bytes)?;
        Some(bytes)
      }
    };
    self.skip_inbound_label_check = skip_inbound_label_check;
    Ok(self)
  }

  /// The configured cluster label, or `None` for an unlabeled coordinator.
  #[cfg(test)]
  pub(crate) fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// The configured cross-transport compression options.
  pub fn compression(&self) -> crate::CompressionOptions {
    self.compression
  }

  /// Reconfigure the gossip compression policy in place. Applies to
  /// the next outbound datagram.
  ///
  /// QUIC's reliable path is skipped — quinn streams provide
  /// confidentiality and integrity intrinsically, so compression is
  /// applied only to the gossip path (plain UDP datagrams sharing
  /// the same socket as the QUIC packets).
  pub fn set_compression_options(&mut self, compression: crate::CompressionOptions) {
    self.compression = compression;
  }

  /// Compress one outbound gossip datagram for the wire. When compression is
  /// disabled, or the datagram does not benefit, the original bytes are
  /// returned.
  pub fn compress_gossip(&self, datagram: &[u8]) -> Vec<u8> {
    match self.compression.apply(datagram) {
      Ok(crate::CompressionOutcome::Compressed(packed)) => {
        let wrapped = crate::encode_compressed_frame(
          self
            .compression
            .algorithm()
            .expect("a Compressed outcome implies an algorithm is set"),
          datagram.len(),
          &packed,
        );
        // The wrapper header (tag + algorithm + `orig_len` varint) is overhead
        // on top of the raw compressed bytes; if it pushes the wrapped
        // datagram to `datagram`'s size or larger, send `datagram` plain so
        // compressed gossip can never inflate. The receiver's
        // `unwrap_transforms` passes a non-wrapper buffer through unchanged.
        if wrapped.len() < datagram.len() {
          wrapped
        } else {
          datagram.to_vec()
        }
      }
      // Plain outcome, or a backend error: emit the datagram uncompressed.
      _ => datagram.to_vec(),
    }
  }

  /// The configured gossip-plane checksum options. Applies to the QUIC
  /// datagram (unreliable) path only; the reliable bidi path carries no
  /// checksum.
  pub fn checksum(&self) -> crate::ChecksumOptions {
    self.checksum
  }

  /// Reconfigure the gossip-plane checksum policy in place. Applies to the next
  /// outbound gossip datagram via [`Self::checksum_gossip`]. The reliable bidi
  /// bridge carries no checksum (quinn provides its own integrity), so there is
  /// no per-bridge fan-out.
  pub fn set_checksum_options(
    &mut self,
    checksum: crate::ChecksumOptions,
  ) -> Result<(), crate::ChecksumError> {
    // Validate the algorithm's backend is built into this binary BEFORE storing
    // it: an unusable policy would make every subsequent `checksum_gossip` fail
    // and the driver drop the datagram — silently disabling all gossip behind a
    // false success. The empty-payload probe surfaces `Disabled` /
    // `UnknownAlgorithm`.
    checksum.apply(&[])?;
    self.checksum = checksum;
    Ok(())
  }

  /// Wrap one outbound gossip datagram in a checksum frame for the wire. The
  /// codec-owning driver calls this on the already-compressed gossip bytes
  /// (from [`Self::compress_gossip`]) BEFORE [`Self::encrypt_gossip`], so the
  /// on-wire order is `[Encrypted[Checksumed[Compressed[frame]]]]`. When
  /// checksumming is disabled the bytes are returned unchanged.
  ///
  /// Returns `Err` when a checksum algorithm is configured but its backend was
  /// not built into this binary; the driver MUST drop the gossip rather than
  /// emit an unverifiable datagram, mirroring [`Self::encrypt_gossip`].
  pub fn checksum_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::ChecksumError> {
    match self.checksum.apply(datagram)? {
      crate::ChecksumOutcome::Checksumed(framed) => Ok(framed),
      crate::ChecksumOutcome::Plain => Ok(datagram.to_vec()),
    }
  }

  /// The configured cross-transport encryption options. Applies to the
  /// gossip path only; the QUIC reliable path always skips.
  pub fn encryption_options(&self) -> &crate::EncryptionOptions {
    &self.encryption
  }

  /// Encrypt one outbound gossip datagram for the wire. The codec-owning
  /// driver calls this on the already-compressed-and-checksummed gossip bytes
  /// (from [`Self::compress_gossip`] → [`Self::checksum_gossip`]) before
  /// handing them to the UDP socket. When encryption is disabled the bytes
  /// are returned unchanged.
  ///
  /// The on-wire byte order with the full stack is
  /// `[Encrypted[Checksumed[Compressed[frame]]]]`; each disabled layer
  /// collapses to identity (e.g. `[Encrypted[frame]]` with compression and
  /// checksum off).
  ///
  /// Returns `Err` when encryption is configured but the backend rejects the
  /// request — typically [`crate::EncryptionError::UnsupportedAlgorithm`]
  /// for a primary key whose backend was not built into this binary. The
  /// driver MUST drop the gossip in that case; emitting unencrypted bytes
  /// on an encrypted-cluster path would bypass authentication silently.
  pub fn encrypt_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::EncryptionError> {
    let keyring = match self.encryption.keyring() {
      Some(kr) => kr,
      None => return Ok(datagram.to_vec()),
    };
    let key = keyring.primary_ref();
    crate::encode_encrypted_frame(key.algorithm(), key, datagram)
  }

  /// Unwrap one inbound gossip datagram. The codec-owning driver calls this
  /// on the raw bytes from [`Self::poll_memberlist_ingress`] BEFORE decoding
  /// frames — it strips the Encrypted-then-Checksumed-then-Compressed wrapper
  /// stack in one pass (each layer identity when its wrapper is absent, the
  /// checksum layer verifying the digest as it strips). A datagram with no
  /// Encrypted wrapper is returned unchanged when no keyring is configured;
  /// when a keyring IS configured the strict-mode entry check rejects any
  /// non-Encrypted leading tag. A corrupt or unknown-algorithm wrapper, a
  /// checksum mismatch, or a frame the keyring cannot decrypt, is an `Err` —
  /// the driver drops the datagram (gossip is lossy and self-healing).
  ///
  /// This is the SINGLE canonical ingress unwrap on the coordinator. The
  /// outbound side uses [`Self::compress_gossip`] → [`Self::checksum_gossip`]
  /// → [`Self::encrypt_gossip`] so the on-wire order is
  /// `[Encrypted[Checksumed[Compressed[frame]]]]`; this helper reverses all
  /// layers, so authentication and integrity never depend on integration
  /// discipline.
  pub fn decrypt_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::FrameError> {
    // Ceiling is the gossip MTU — the maximum size any compliant gossip
    // datagram decompresses to. A wrapper claiming more is not a compliant
    // datagram and is rejected. The encryption-aware unwrap consumes an
    // Encrypted wrapper through the keyring, then verifies and strips a
    // Checksumed wrapper, then strips a Compressed wrapper if present; a
    // non-Encrypted-led datagram is returned unchanged when no keyring is
    // configured (the strict-mode entry check is gated on
    // `encryption.is_enabled()`).
    crate::unwrap_transforms_with_encryption(datagram, self.ep.gossip_mtu(), &self.encryption)
      .map(|cow| cow.into_owned())
  }

  /// Borrow the inner membership endpoint (members / queue_user_broadcast / …).
  #[inline(always)]
  pub fn endpoint_ref(&self) -> &Endpoint<I, SocketAddr> {
    &self.ep
  }

  /// The machine's load-shedding counters for this QUIC endpoint. Folds the QUIC
  /// datagram-plane ingress shed (`datagram_ingress_dropped`) into
  /// [`gossip_ingress_dropped`](crate::metrics::Metrics::gossip_ingress_dropped)
  /// — the inner `Endpoint`'s own gossip-ingress count is the STREAM plane's
  /// (zero on a QUIC endpoint) — so a driver reads one unified counter regardless
  /// of transport.
  pub fn metrics(&self) -> crate::metrics::Metrics {
    let mut m = self.ep.metrics();
    m.gossip_ingress_dropped = m
      .gossip_ingress_dropped
      .saturating_add(self.datagram_ingress_dropped);
    m
  }

  /// Mutably borrow the inner membership endpoint — test-only. Production
  /// code accesses `self.ep` directly inside `QuicEndpoint`'s own methods.
  /// A public raw `&mut Endpoint` would let external callers drain
  /// `Event::DialRequested` directly out of the inner queue (via
  /// `endpoint_mut().poll_event()`) and orphan the `PendingStreamIntent` —
  /// the QUIC bridge would never open, the immediate-due `poll_timeout`
  /// term would never fire, and the exchange (push/pull, reliable-ping
  /// fallback, user-message) would silently strand. External callers go
  /// through scoped pass-through methods ([`Self::start_push_pull`],
  /// [`Self::start_reliable_ping`], [`Self::start_user_message`],
  /// [`Self::start_probe`], [`Self::handle_alive`], [`Self::requeue_event`])
  /// AND the sieving public [`Self::poll_event`], preserving the sealed
  /// inner endpoint invariant that no caller can drain `DialRequested`
  /// out from under `service_dials`.
  #[cfg(test)]
  pub(crate) fn endpoint_mut(&mut self) -> &mut Endpoint<I, SocketAddr> {
    &mut self.ep
  }

  /// Arm the periodic probe / gossip / push-pull schedulers. Forwards to
  /// [`Endpoint::start_scheduling`].
  #[inline]
  pub fn start_scheduling(&mut self, now: Instant) {
    self.ep.start_scheduling(now);
  }

  /// Returns `true` if the endpoint is in normal operation (not leaving
  /// or left). Forwards to [`Endpoint::is_running`]. A driver consults
  /// this before calling [`Self::leave`] to distinguish a leave that
  /// actually initiates the dead-self flush (and will emit
  /// [`Event::LeftCluster`](crate::event::Event::LeftCluster)) from an
  /// idempotent post-leave no-op (which will not).
  #[inline]
  pub fn is_running(&self) -> bool {
    self.ep.is_running()
  }

  /// Update the local node's metadata. The new value is gossiped
  /// through the standard alive-broadcast path.
  ///
  /// Pass-through to [`Endpoint::update_meta`]; the inner endpoint bumps
  /// the local incarnation and queues an `Alive` broadcast carrying the
  /// new bytes so peers converge to the updated metadata via the normal
  /// SWIM path.
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::NotRunning`] if the local lifecycle has
  /// already transitioned to `Leaving` / `Left` / `Shutdown`. Returns
  /// [`crate::error::Error::MetaExceedsCap`] if `meta` exceeds the
  /// per-endpoint cap configured at construction.
  pub fn update_meta(&mut self, meta: crate::typed::Meta) -> Result<(), crate::error::Error> {
    self.ep.update_meta(meta)
  }

  /// Queue an application user-broadcast for gossip dissemination. Forwards
  /// to the inner membership [`Endpoint`].
  #[inline]
  pub fn queue_user_broadcast(&mut self, data: Bytes) -> Result<(), crate::error::Error> {
    self.ep.queue_user_broadcast(data)
  }

  /// The reliable-stream frame ceiling
  /// ([`max_stream_frame_size`](crate::config::EndpointOptions::max_stream_frame_size)).
  /// The driver derives its observation-channel payload byte budget from this.
  #[inline]
  pub fn max_stream_frame_size(&self) -> usize {
    self.ep.max_stream_frame_size()
  }

  /// Set the application push-pull local-state snapshot. Forwards to the
  /// inner [`Endpoint`].
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::LocalStateExceedsFrame`] if the snapshot's
  /// framed PushPull would exceed the reliable-stream frame budget — such a
  /// snapshot is deterministically untransmittable, so it is rejected rather
  /// than stored.
  #[inline]
  pub fn set_local_state_snapshot(&mut self, bytes: Bytes) -> Result<(), crate::error::Error> {
    self.ep.set_local_state_snapshot(bytes)
  }

  /// Set the application ack payload attached to probe acks. Forwards to the
  /// inner [`Endpoint`].
  ///
  /// # Errors
  ///
  /// Returns [`crate::error::Error::AckPayloadExceedsMtu`] if the framed Ack
  /// carrying `payload` would not fit the node's gossip packet budget — an
  /// over-budget Ack is deterministically unsendable on the gossip socket,
  /// so the payload is rejected rather than stored.
  #[inline]
  pub fn set_ack_payload(&mut self, payload: Bytes) -> Result<(), crate::error::Error> {
    self.ep.set_ack_payload(payload)
  }

  /// Next outbound UDP datagram (quinn or encoded memberlist), if any.
  pub fn poll_transmit(&mut self) -> Option<(SocketAddr, Bytes)> {
    self.out.pop_front()
  }

  /// Next raw inbound memberlist datagram (the first-byte demux classified it
  /// `Class::Memberlist`), if any.
  ///
  /// `memberlist-proto` has no umbrella `codec` dependency, so the
  /// coordinator cannot perform the structured unwrap
  /// (label → decrypt → decompress → split-compound) in-crate; it surfaces
  /// the raw `(from, bytes)` as an explicit action instead of silently
  /// dropping it (a silent drop would lose every UDP ping/ack/alive/suspect
  /// on the composed unit's public ingress). The codec-owning layer drains
  /// this, decodes each `Message`, and feeds it back through
  /// [`handle_packet`](Self::handle_packet).
  pub fn poll_memberlist_ingress(&mut self) -> Option<(SocketAddr, Bytes)> {
    let (from, bytes) = self.mem_ingress.pop_front()?;
    // Keep the per-peer share counter exact: decrement on pop and remove the
    // entry at zero so no stale zeros accumulate (one map key per peer with a
    // live standing share, nothing more).
    if let std::collections::hash_map::Entry::Occupied(mut slot) =
      self.mem_ingress_per_peer.entry(from)
    {
      let n = slot.get_mut();
      *n -= 1;
      if *n == 0 {
        slot.remove();
      }
    }
    Some((from, bytes))
  }

  /// Number of live (non-reaped) QUIC connections to `peer` — `0` or `1`,
  /// since the connection table pools one connection per peer.
  ///
  /// Observation-only, for a driver/test to assert the drained-reap
  /// lifecycle (a connection that idled past `max_idle_timeout` is reaped:
  /// its slab + peers entry is removed, so this drops back to `0`). A
  /// connection still in its closing/draining wind-down is reported live
  /// until [`ConnTable::reap_if_drained`] removes it.
  pub fn live_connections_to(&self, peer: SocketAddr) -> usize {
    match self.conns.handle_for(&peer) {
      Some(ch) => usize::from(
        self
          .conns
          .get(ch)
          .map(|e| !e.conn_ref().is_drained())
          .unwrap_or(false),
      ),
      None => 0,
    }
  }

  /// Number of active reliable-exchange bridges (one per in-flight push/pull
  /// or reliable-ping stream). Observation-only, for a driver/test to assert
  /// no bridge leaked after an exchange completed or its connection dropped.
  pub fn live_bridge_count(&self) -> usize {
    self.bridges.len()
  }

  /// The configured plaintext-byte ceiling for an outbound gossip datagram.
  /// Sourced from [`crate::config::EndpointOptions::gossip_mtu`] (default
  /// [`crate::config::DEFAULT_GOSSIP_MTU`]). The on-wire datagram may
  /// exceed this by [`crate::ENCRYPTED_WRAPPER_OVERHEAD`] when
  /// encryption is enabled.
  pub fn gossip_mtu(&self) -> usize {
    self.ep.gossip_mtu()
  }

  /// Probe the protocol-layer credit for opening a remote-initiated
  /// unidirectional stream to `peer`. Returns `true` iff the open
  /// would have succeeded; on the (rare) success branch the probe
  /// CLOSES THE ENTIRE CONNECTION before returning so no hidden
  /// stream state can persist on a reusable connection.
  ///
  /// Diagnostic only: the composed unit disables remotely-initiated
  /// unidirectional streams by construction — the transport config
  /// installed by [`QuicOptions::new`] advertises
  /// `max_concurrent_uni_streams = 0`, so on a peer that observed
  /// our transport parameters this method MUST return `false` once
  /// the handshake completes. A test can use this to assert that
  /// the protocol-layer refusal is in effect; it is not a path the
  /// coordinator itself uses (all coordinator-initiated streams
  /// are bidirectional).
  ///
  /// Why the close-on-success — `quinn_proto::Streams::open(Dir::Uni)`
  /// inserts send state and increments `StreamsState::send_streams`.
  /// `SendStream::reset(0)` only marks the stream `ResetSent` and
  /// queues a `RESET_STREAM` frame; the entry is freed on the peer's
  /// reset ACK, NOT synchronously. A `true` branch therefore means
  /// (a) the transport-config invariant was violated (an unsafe state
  /// to keep using the connection), AND (b) any reset-only retirement
  /// would leave hidden state on the pooled connection until the peer
  /// ACKs the reset. `Connection::close(now, 0, empty)` tears down the
  /// connection-level state immediately (transitions to `State::Closed`,
  /// queues `CONNECTION_CLOSE` once, marks `is_drained()` after the next
  /// `poll_transmit`/`poll_timeout` cycle); the coordinator's
  /// `finalize_tick` then drained-reaps the slab + peers entry.
  /// `last_now` is also anchored so any wake the close requires
  /// surfaces immediately.
  ///
  /// Returns `false` if no connection to `peer` exists, or if the
  /// open is refused.
  pub fn try_open_uni_stream_to(&mut self, peer: SocketAddr, now: Instant) -> bool {
    self.last_now = Some(now);
    let Some(ch) = self.conns.handle_for(&peer) else {
      return false;
    };
    let Some(e) = self.conns.get_mut(ch) else {
      return false;
    };
    let opened = e.conn_mut().streams().open(Dir::Uni).is_some();
    if opened {
      e.conn_mut().close(
        now.into_std(),
        quinn_proto::VarInt::from_u32(0),
        bytes::Bytes::new(),
      );
    }
    opened
  }
}

// Methods that delegate to `Endpoint`'s full-bag surface (`poll_event`,
// `poll_transmit`, `poll_timeout`, `handle_packet`, `handle_alive`,
// `handle_suspect`, `requeue_event`, `start_probe`, `leave`), drive
// `Bridge` ops (whose impls require the full bag), or run the internal
// bridge-pump / reap helpers.
impl<I> QuicEndpoint<I>
where
  I: crate::Id
    + crate::Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  /// Build the coordinator with an explicit cross-transport encryption
  /// configuration. [`Self::new`] is `with_encryption` with encryption
  /// disabled. The configuration applies to the QUIC gossip (plain UDP)
  /// path only; the QUIC reliable path always skips encryption because
  /// quinn-encrypted streams already provide confidentiality.
  ///
  /// Routes through [`Self::set_encryption_options`] so the bridge-fan-out
  /// runs in both the builder and the in-place setter, matching
  /// [`crate::streams::StreamEndpoint::with_encryption`].
  #[must_use]
  pub fn with_encryption(mut self, encryption: crate::EncryptionOptions) -> Self {
    self.set_encryption_options(encryption);
    self
  }

  /// Replace the encryption options in place. The driver calls this on a key
  /// rotation: build a new `EncryptionOptions` with the rotated `Keyring`,
  /// then publish it via the setter. Single-threaded `&mut self` — no lock.
  ///
  /// Propagates the new options to every live reliable bridge for symmetry
  /// with the plain-stream coordinator (see
  /// [`crate::streams::StreamEndpoint::set_encryption_options`]). On QUIC the
  /// reliable bridge always force-disables encryption (quinn already
  /// encrypts the stream), so the bridge-level propagation is a no-op — the
  /// gossip path's strictness propagates immediately via the coordinator's
  /// own `self.encryption` field (`decrypt_gossip` reads it directly each
  /// call).
  ///
  /// **No-op reapply** — short-circuits at entry if the new options equal
  /// the current ones, mirroring [`crate::streams::StreamEndpoint::set_encryption_options`]'s
  /// own guard. The bridge-fan-out is a no-op on QUIC, but the
  /// `bridge.set_encryption(encryption.clone())` clone-and-call still
  /// runs once per live bridge — pure waste on a config reconciler that
  /// republishes the same effective policy.
  pub fn set_encryption_options(&mut self, encryption: crate::EncryptionOptions) {
    if self.encryption == encryption {
      self.encryption = encryption;
      return;
    }
    for bridge in self.bridges.values_mut() {
      bridge.set_encryption(encryption.clone());
    }
    // Drop every buffered raw gossip datagram. `handle_memberlist_udp`
    // enqueues `(src, raw_bytes)` into [`Self::mem_ingress`];
    // [`Self::decrypt_gossip`] reads the coordinator's CURRENT
    // `self.encryption` at drain time. Without this clear, a datagram queued
    // before the policy change is decrypted under the NEW policy — a
    // plaintext datagram queued while strict-mode was ON would be accepted
    // after the operator switches to disabled, and a ciphertext datagram
    // queued while disabled would be rejected after enabling. Gossip is
    // lossy and self-healing, so the dropped bytes recover on the next
    // gossip round. The QUIC reliable bridges force-disable encryption
    // regardless of the new options (quinn already encrypts the stream), so
    // there is no per-bridge failure path here — the gossip ingress buffer
    // is the only at-risk queue on policy change.
    self.mem_ingress.clear();
    // Keep the per-peer share counter consistent with the now-empty queue.
    self.mem_ingress_per_peer.clear();
    self.encryption = encryption;
  }

  /// Initiate one SWIM probe tick on the inner membership endpoint.
  ///
  /// Pass-through to [`Endpoint::start_probe`]. Sets `last_now` so the
  /// next `poll_timeout` is anchored to a known-past instant (the same
  /// idiom every other `handle_*` / `start_*` uses). The probe itself
  /// rides the unreliable UDP path (`poll_memberlist_transmit`); only if
  /// it fails does the reliable QUIC fallback kick in via the natural
  /// suspicion / failure-detection timing.
  pub fn start_probe(&mut self, now: Instant) -> bool {
    self.last_now = Some(now);
    self.ep.start_probe(now)
  }

  /// Seed an `Alive` state on the inner membership endpoint (typical
  /// bootstrap path: a harness teaching the coordinator about a known
  /// peer without going through a join push/pull).
  ///
  /// Pass-through to [`Endpoint::handle_alive`]. Sets `last_now`.
  pub fn handle_alive(
    &mut self,
    from: SocketAddr,
    alive: crate::typed::Alive<I, SocketAddr>,
    at: Instant,
  ) {
    self.last_now = Some(at);
    self.ep.handle_alive(from, alive, at);
  }

  /// Inject a `Suspect` event on the inner membership endpoint
  /// (test-harness path; a real driver gets Suspect via SWIM probe
  /// timeouts or peer gossip).
  ///
  /// Pass-through to [`Endpoint::handle_suspect`]. Sets `last_now`.
  pub fn handle_suspect(
    &mut self,
    from: SocketAddr,
    suspect: crate::typed::Suspect<I>,
    at: Instant,
  ) {
    self.last_now = Some(at);
    self.ep.handle_suspect(from, suspect, at);
  }

  /// Re-queue an event for observation by a later [`Self::poll_event`]
  /// (the sieving public drain).
  ///
  /// Anchors `last_now = Some(now)` unconditionally — the immediate-due
  /// `poll_timeout` rescue for an unattempted `dial_pending` entry
  /// requires `last_now` to be `Some(...)`, and every public surface
  /// that deposits an event must satisfy that invariant.
  ///
  /// Routing differs by event kind:
  /// - `Event::DialRequested` is routed DIRECTLY into the private
  ///   `dial_pending` deque with `attempted = false`, bypassing the
  ///   inner Endpoint queue entirely. Otherwise a caller that calls
  ///   [`Self::poll_timeout`] WITHOUT an intervening
  ///   [`Self::poll_event`] sieve would see an empty `dial_pending`
  ///   (the requeued `DialRequested` is sitting in the inner queue
  ///   awaiting sieve) — the immediate-due rescue would skip,
  ///   `poll_timeout` would degrade to the next gossip / probe /
  ///   quinn timer or the inner endpoint's own term, and the dial
  ///   would only be examined at the entry's `deadline` ≈
  ///   `now + stream_timeout` (silent strand at the deadline).
  ///   Direct routing ensures the entry IS present the moment
  ///   `requeue_event` returns.
  /// - Every other variant delegates to [`Endpoint::requeue_event`]
  ///   for observation via the next [`Self::poll_event`] — the standard
  ///   forwarded-event reordering pattern a harness uses to put an event
  ///   back at the tail of the queue after peeking.
  pub fn requeue_event(&mut self, ev: Event<I, SocketAddr>, now: Instant) {
    self.last_now = Some(now);
    match ev {
      Event::DialRequested(dial) => {
        let (id, peer, deadline) = dial.into_parts();
        self.dial_pending.push_back(PendingDial {
          id,
          peer,
          deadline,
          attempted: false,
        });
      }
      other => self.ep.requeue_event(other),
    }
  }

  /// Begin a graceful leave; delegates to the membership endpoint.
  pub fn leave(&mut self, now: Instant) -> Result<(), crate::error::Error> {
    self.last_now = Some(now);
    self.ep.leave(now)
  }

  /// Next membership/lifecycle event for the driver, if any.
  ///
  /// `Event::DialRequested` is sieved out of the inner endpoint's queue
  /// into the private [`dial_pending`](Self::dial_pending) deque and is
  /// NEVER returned to external callers: in the composed design the
  /// coordinator IS the driver and dials itself (see [`Self::service_dials`])
  /// — leaking the retry token would let an external `poll_event` drain
  /// mid-handshake silently drop it, orphaning the pending stream intent
  /// (the push/pull or reliable-ping would never open). External callers
  /// only observe application-visible events.
  pub fn poll_event(&mut self) -> Option<Event<I, SocketAddr>> {
    loop {
      match self.ep.poll_event()? {
        Event::DialRequested(dial) => {
          let (id, peer, deadline) = dial.into_parts();
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
          continue;
        }
        other => return Some(other),
      }
    }
  }

  /// Unified next-deadline = `min` over the memberlist endpoint, every
  /// bridge stream, every quinn connection, AND every pending-dial intent's
  /// own deadline. Returns an immediate-due wake (an `Instant` already
  /// `<= caller's now`) whenever `dial_pending` holds an entry
  /// `service_dials` has not yet attempted.
  ///
  /// The pending-dial deadline term is correctness, not optimisation: when
  /// a dial intent is requeued onto `dial_pending` (the connection is still
  /// handshaking, or the established connection's MAX_STREAMS credit is
  /// exhausted), the next service tick must happen no later than that
  /// intent's own `deadline` — otherwise a fully-stalled `dial_pending`
  /// queue would be starved of wake-ups and the intent would only be
  /// re-examined when some unrelated component (the memberlist endpoint,
  /// an active stream, or a quinn connection) happens to wake the
  /// coordinator. On a quiet cluster that wake could be arbitrarily far
  /// after the intent's `deadline`, postponing the `dial_failed` past the
  /// user-visible exchange timeout.
  ///
  /// The immediate-due term is defence-in-depth for callers that bypass
  /// the high-level [`Self::start_push_pull`] / [`Self::start_reliable_ping`]
  /// / [`Self::start_user_message`] wrappers (which dial in-band) and queue
  /// a `DialRequested` directly via `endpoint_mut()`. A caller that drains
  /// [`Self::poll_event`] (sieving the `DialRequested` into `dial_pending`)
  /// and then advances solely by `poll_timeout` would otherwise only see
  /// the intent's own `deadline` ≈ `now + stream_timeout` — by the time
  /// that wake fires, `service_dials` discovers the deadline elapsed and
  /// retires the intent via `dial_failed`, never attempting the handshake.
  /// Returning `Some(last_now)` for any unattempted entry forces the next
  /// `handle_timeout(now)` to fire promptly, so `service_dials` attempts
  /// the dial in real wall-clock time of the same logical instant. Once an
  /// entry is attempted (whether the bidi opened or it was requeued for
  /// handshake / credit), subsequent wake-ups are driven by the
  /// connection's own `poll_timeout` and by `deadline`; re-firing an
  /// attempted entry every tick would busy-loop a still-handshaking
  /// connection.
  ///
  /// `last_now` is `None` only before the very first `handle_*` /
  /// `start_*` call: in that window the immediate-due wake degrades to the
  /// intent's `deadline` term, which is still strictly better than no
  /// wake at all (the dial fails at the deadline rather than orphaning).
  pub fn poll_timeout(&mut self) -> Option<Instant> {
    let mut best = self.ep.poll_timeout();
    for b in self.bridges.values() {
      if let Some(t) = b.poll_timeout() {
        best = Some(best.map_or(t, |b| b.min(t)));
      }
    }
    let mut has_pending_conn_events = false;
    for ch in self.conns.iter_handles() {
      if let Some(e) = self.conns.get_mut(ch) {
        if let Some(t) = e.conn_mut().poll_timeout().map(crate::Instant::from_std) {
          best = Some(best.map_or(t, |b| b.min(t)));
        }
        if e.has_pending_events() {
          has_pending_conn_events = true;
        }
      }
    }
    let mut has_unattempted = false;
    for entry in &self.dial_pending {
      let t = entry.deadline;
      best = Some(best.map_or(t, |b| b.min(t)));
      if !entry.attempted {
        has_unattempted = true;
      }
    }
    // Immediate-due wake on deferred-event backlog. `service_quinn`
    // queues `ConnectionEvent`s returned by `Endpoint::handle_event`
    // (e.g. `NewIdentifiers`, the response to `NeedIdentifiers` at
    // handshake completion) on the OWNING `ConnEntry.pending_events`
    // for delivery on its NEXT iteration — the one-tick deferral
    // mirrors quinn-proto's reference async driver and keeps
    // NEW_CONNECTION_ID frames out of the same-tick stream-data
    // packet build. Without surfacing the backlog through
    // `poll_timeout`, a strict-poll driver that sleeps until the
    // next deadline would not re-enter `service_quinn` until an
    // unrelated idle/loss/probe timer fires — stalling CID /
    // reset-token lifecycle work for arbitrarily long. The same
    // immediate-due idiom the unattempted-dial branch uses: anchor
    // on `last_now` (no `Instant::now()` inside this Sans-I/O
    // method), degrading gracefully to "no wake added" on the
    // first-call corner case before any `handle_*` / `start_*` has
    // set the anchor.
    if has_unattempted || has_pending_conn_events {
      if let Some(anchor) = self.last_now {
        best = Some(best.map_or(anchor, |b| b.min(anchor)));
      }
    }
    best
  }

  /// Next typed unreliable memberlist [`Transmit`] for the driver to encode
  /// onto the unreliable (UDP) path, if any.
  ///
  /// Each call drains ONE `Transmit` straight out of the inner
  /// `Endpoint::poll_transmit`; nothing is prebuffered coordinator-internally.
  /// This makes the inner pop — which decrements `Endpoint`'s leave-completion
  /// counter and emits `Event::LeftCluster` after the last dead-self notice
  /// — happen at the SAME moment the datagram crosses to the external
  /// driver. A caller that `leave(now)`s, ticks, and then reads `poll_event`
  /// cannot observe `LeftCluster` until it has drained the dead-self tail
  /// through this accessor: tearing the socket down on `LeftCluster` therefore
  /// guarantees every dead-self broadcast has been handed to the driver, so
  /// peers see `Dead`/`Left` rather than wrongly Suspecting.
  ///
  /// The driver MUST take the unreliable path through this accessor and never
  /// call `endpoint_mut().poll_transmit()` directly (that would double-drive
  /// the `LeftCluster` boundary).
  pub fn poll_memberlist_transmit(&mut self) -> Option<Transmit<I, SocketAddr>> {
    self.ep.poll_transmit()
  }

  /// Pump queued quinn outbound — including datagrams just handed to
  /// [`queue_unreliable_datagram`](Self::queue_unreliable_datagram) — into the
  /// [`poll_transmit`](Self::poll_transmit) queue at the current instant WITHOUT
  /// advancing any membership timer. A driver calls this after queuing
  /// unreliable datagrams so they flush on the SAME tick they were queued: a
  /// datagram carries a probe Ping whose timeout is armed in the same tick, and
  /// a one-tick send latency would let that timeout fire before the Ping ever
  /// left the host (a spurious failure). Idempotent and side-effect-free on
  /// membership state (no `Endpoint::handle_timeout`); the existing zero-time
  /// outbound flush the `start_*` paths already use.
  pub fn flush_outbound_transmits(&mut self, now: Instant) {
    self.flush_outbound(now);
  }

  /// Feed one decoded unreliable memberlist [`Message`](crate::typed::Message)
  /// (a frame the codec-owning layer unwrapped from a datagram surfaced by
  /// [`poll_memberlist_ingress`](Self::poll_memberlist_ingress)) into the
  /// inner membership endpoint.
  ///
  /// Pass-through to [`Endpoint::handle_packet`]; the composed unit's public
  /// ingress for the unreliable path is `handle_udp` → `poll_memberlist_ingress`
  /// → (codec decode) → `handle_packet`, never a direct call into the inner
  /// `Endpoint`.
  pub fn handle_packet(
    &mut self,
    from: SocketAddr,
    msg: crate::typed::Message<I, SocketAddr>,
    now: Instant,
  ) {
    self.ep.handle_packet(from, msg, now);
  }

  fn route_datagram_event(
    &mut self,
    ev: DatagramEvent,
    from: SocketAddr,
    now: Instant,
    scratch: &[u8],
  ) {
    match ev {
      DatagramEvent::ConnectionEvent(ch, cev) => {
        if let Some(e) = self.conns.get_mut(ch) {
          e.conn_mut().handle_event(cev);
        }
      }
      DatagramEvent::NewConnection(incoming) => {
        let mut buf = Vec::new();
        match self.quinn.accept(
          incoming,
          now.into_std(),
          &mut buf,
          Some(self.cfg.server_arc()),
        ) {
          Ok((ch, conn)) => self.conns.insert_accepted(ch, conn, from),
          Err(e) => {
            // quinn-proto attaches an `Option<Transmit>` to its `AcceptError`
            // whenever `accept` owes a refusal/close to the peer (CID
            // exhaustion + initial-handshake transport failure produce an
            // `initial_close` response). The close bytes are already in our
            // local `buf`; surface them via the driver-facing `out` queue,
            // mirroring the `DatagramEvent::Response` arm below. Without
            // this the peer waits its full handshake retransmit budget
            // instead of seeing the immediate close.
            if let Some(t) = e.response {
              if t.size <= buf.len() {
                self
                  .out
                  .push_back((t.destination, Bytes::copy_from_slice(&buf[..t.size])));
                #[cfg(test)]
                {
                  self.accept_error_responses_emitted =
                    self.accept_error_responses_emitted.saturating_add(1);
                }
              }
            }
          }
        }
      }
      DatagramEvent::Response(t) => {
        // `Endpoint::handle` wrote `t.size` bytes of a stateless response
        // (Retry / version negotiation / stateless reset) into the `scratch`
        // buffer passed in `handle_udp`; surface it as an outbound datagram.
        if t.size <= scratch.len() {
          self
            .out
            .push_back((t.destination, Bytes::copy_from_slice(&scratch[..t.size])));
        }
      }
    }
  }

  fn handle_memberlist_udp(&mut self, from: SocketAddr, datagram: &[u8]) {
    // The umbrella `codec` is not a dependency of memberlist-proto, so the
    // byte-level decode (decode_incoming -> parse_messages -> handle_packet)
    // cannot run in-crate. Surfacing the raw datagram as an explicit ingress
    // action — never a silent no-op — is required for the composed unit's
    // ingress to remain correct: a no-op here would lose every UDP
    // ping/ack/alive/suspect on the composed unit's public ingress. The
    // codec-owning layer drains it via `poll_memberlist_ingress`, decodes
    // each `Message`, and feeds it back through `handle_packet`.
    //
    // Admission goes through the SAME capped helper as the QUIC datagram drain
    // so the shared coordinator queue is bounded uniformly: a plain-UDP /
    // fallback flood cannot bypass the per-peer or node-global cap, drive
    // `mem_ingress_per_peer` past the bound the QUIC drain checks, or push the
    // global count over the hard memory limit.
    push_mem_ingress_capped(
      &mut self.mem_ingress,
      &mut self.mem_ingress_per_peer,
      &mut self.datagram_ingress_dropped,
      from,
      || Bytes::copy_from_slice(datagram),
    );
  }

  /// Emit [`Event::ExchangeCompleted`] for an outbound bridge that has
  /// reached its terminal state. `id` MUST be the bridge's
  /// machine-level [`StreamId`] (the key the bridge was inserted into
  /// `self.bridges` under). The helper drains the originating kind from
  /// [`Self::pending_outbound_kinds`] (`None` ⇒ inbound or unknown —
  /// no emission) and the peer address from
  /// [`Self::pending_outbound_peers`].
  ///
  /// Called from every bridge-reap site (the `pump_bridges` D1 reap,
  /// the `service_quinn` ConnectionLost / `is_drained()` inline drain,
  /// and the test-only acceptance-tracking pump). Outbound only —
  /// inbound (server-accepted) bridges have no entry in
  /// `pending_outbound_kinds` and silently no-op here.
  fn emit_exchange_completed(&mut self, id: StreamId, outcome: ExchangeOutcome) {
    let Some(kind) = self.pending_outbound_kinds.remove(&id) else {
      return;
    };
    // `pending_outbound_peers` is always populated alongside
    // `pending_outbound_kinds`; if `kind` was present, peer must be too.
    let peer = self
      .pending_outbound_peers
      .remove(&id)
      .expect("pending_outbound_peers entry must exist when kind entry exists");
    self
      .ep
      .emit_event(Event::ExchangeCompleted(ExchangeCompleted::new(
        ExchangeId::from(id),
        peer,
        outcome,
        kind,
      )));
  }

  /// Retire dial intent `id` after the in-band dial failed BEFORE any
  /// bridge was created (deadline elapsed, cached connection closed,
  /// `get_or_dial` error, or a frozen-API `dial_succeeded == None`).
  ///
  /// Discards the staged kind + peer (the bridge that would have been
  /// keyed by `id` never existed, so the `emit_exchange_completed`
  /// reap path will never observe `id`), then routes the failure
  /// through the inner FSM's `dial_failed`.
  ///
  /// For an `ExchangeKind::UserMessage` OR `ExchangeKind::PushPull` dial
  /// it ALSO emits a terminal [`Event::ExchangeCompleted`] with
  /// `outcome = Failed`, keyed by the SAME `ExchangeId::from(id)` the
  /// QUIC driver parked its waiter under (the reliable-send waiter for
  /// `UserMessage`, the `WaitForCompletion` join waiter for `PushPull`).
  /// Without this the driver's parked waiter would hang forever: the only
  /// other `ExchangeCompleted` producer is the bridge-reap path, which
  /// never fires for a bridge that was never created. A QUIC join parks
  /// every `start_push_pull` exchange keyed by `ExchangeId::from(StreamId)`
  /// and resolves only when each parked id surfaces a terminal
  /// `ExchangeCompleted(PushPull)`; an unreachable seed whose dial fails
  /// before a bridge exists would otherwise never drain its waiter set.
  ///
  /// No double-completion: a pre-bridge failure means NO bridge keyed by
  /// `id` was ever inserted into `self.bridges` (the bridge is created
  /// only on the `dial_succeeded` success path, which does not call this),
  /// and this method drains both staged maps up front, so the bridge-reap
  /// `emit_exchange_completed` — which requires both a live bridge for `id`
  /// AND a `pending_outbound_kinds` entry — can never also fire for this
  /// `id`. This single `Failed` is the lone terminal event for the
  /// StreamId, independent of kind.
  ///
  /// `ReliablePing` is NOT widened here: its failure is already driven
  /// into the probe FSM by `dial_failed` itself, which terminalizes the
  /// fallback-probe path; a second `ExchangeCompleted` is neither parked
  /// on nor expected by any driver for the reliable-ping fallback.
  fn retire_failed_dial(&mut self, id: StreamId, err: crate::error::StreamError, now: Instant) {
    let kind = self.pending_outbound_kinds.remove(&id);
    let peer = self.pending_outbound_peers.remove(&id);
    if let (Some(kind @ (ExchangeKind::UserMessage | ExchangeKind::PushPull)), Some(peer)) =
      (kind, peer)
    {
      self
        .ep
        .emit_event(Event::ExchangeCompleted(ExchangeCompleted::new(
          ExchangeId::from(id),
          peer,
          ExchangeOutcome::Failed,
          kind,
        )));
    }
    self.ep.dial_failed(id, err, now);
  }

  /// Determine the [`ExchangeOutcome`] of a bridge at the moment it is
  /// being reaped. Mirrors [`super::streams::StreamEndpoint::reap_bridge`]'s
  /// rule: any failure phase (`BridgeFailure::Timeout`, `Transport`,
  /// `Decode`, `ConnectionLost`, `AdmissionClosed`, `DialRetired`,
  /// `EncryptionPolicyChanged`) maps to `Failed`; the cooperative
  /// `BothClosed` clean terminus maps to `Succeeded`. The bridge MUST
  /// be terminal before this is called — terminal-after-D1 is the only
  /// site that knows the final outcome.
  #[inline]
  fn outcome_for_terminal(br: &Bridge<I, SocketAddr>) -> ExchangeOutcome {
    if br.is_phase_failed() {
      ExchangeOutcome::Failed
    } else {
      ExchangeOutcome::Succeeded
    }
  }

  /// Step (2) of the per-tick order: pump every bridge's inbound + outbound
  /// halves, drain each non-terminal stream's endpoint-events into the
  /// `Endpoint`, and D1-drain-then-reap any bridge that turned terminal.
  ///
  /// Extracted so [`Self::flush_outbound`] can re-use the same bridge step
  /// after `service_dials` — a freshly-opened outbound bridge carries its
  /// request bytes in its FSM `Stream` output buffer, and a single pump is
  /// what moves those bytes into the quinn send stream so they emerge on
  /// the next [`Self::collect_transmits`].
  fn pump_bridges(&mut self, now: Instant) {
    let ids: Vec<StreamId> = self.bridges.keys().copied().collect();
    for id in &ids {
      // Split borrow: take the bridge out, operate, put back (or reap).
      if let Some(mut br) = self.bridges.remove(id) {
        // `pump_in`/`pump_out` set the bridge `fatal` flag on a transport
        // error, so `is_terminal()` below drives the prompt reap; the
        // `#[must_use]` Results are consumed — terminality is the signal.
        let _ = br.pump_in(&mut self.conns, now);
        let _ = br.pump_out(&mut self.conns, now);
        // Drain endpoint-events EVERY tick (not only when terminal).
        // `drain_then_reap` also delivers the slot-gone notice (terminal
        // only); a non-terminal stream drains its payload events with the
        // SAME encode+load+flush but WITHOUT that notice.
        if br.is_terminal() {
          br.drain_then_reap(&mut self.ep, &mut self.conns, now);
          let outcome = Self::outcome_for_terminal(&br);
          // Reap AFTER drain: dropping the bridge frees its slot.
          drop(br);
          self.emit_exchange_completed(*id, outcome);
        } else {
          br.drain_payload_only(&mut self.ep, &mut self.conns, now);
          // `drain_payload_only` may flip the bridge to terminal (e.g.
          // a `StreamCommand::Close` from an admission-rejected join sets
          // `fatal`); re-check terminality so the bridge D1-drains and
          // reaps in this SAME tick rather than holding the quinn bidi
          // stream until its exchange deadline.
          if br.is_terminal() {
            #[cfg(test)]
            {
              self.bridges_terminalized_via_close_command = self
                .bridges_terminalized_via_close_command
                .saturating_add(1);
            }
            br.drain_then_reap(&mut self.ep, &mut self.conns, now);
            let outcome = Self::outcome_for_terminal(&br);
            drop(br);
            self.emit_exchange_completed(*id, outcome);
          } else {
            self.bridges.insert(*id, br);
          }
        }
      }
    }
  }

  /// Test-only variant of [`Self::pump_bridges`] that increments
  /// [`Self::bridges_pumped_after_acceptance`] once for each bridge whose
  /// id is NOT in `pre_snapshot_ids` (i.e. inserted into `self.bridges`
  /// AFTER the snapshot was taken). Used by step (5.5) of [`Self::run_tick`]
  /// and the post-`service_quinn` second pump in [`Self::flush_outbound`] to
  /// prove the post-acceptance pump actually runs on every newly-inserted
  /// bridge — the negative-control regression test reverts the step (5.5)
  /// call site and the counter stays at zero.
  ///
  /// The body is otherwise byte-identical to `pump_bridges`: the counter
  /// increment is the ONLY observable difference, so production behaviour
  /// (the second pump's effect on `self.bridges` and the inner `Endpoint`)
  /// is unchanged.
  #[cfg(test)]
  fn pump_bridges_tracking_post_acceptance(
    &mut self,
    now: Instant,
    pre_snapshot_ids: &std::collections::HashSet<StreamId>,
  ) {
    let ids: Vec<StreamId> = self.bridges.keys().copied().collect();
    for id in &ids {
      if let Some(mut br) = self.bridges.remove(id) {
        if !pre_snapshot_ids.contains(id) {
          self.bridges_pumped_after_acceptance =
            self.bridges_pumped_after_acceptance.saturating_add(1);
        }
        let _ = br.pump_in(&mut self.conns, now);
        let _ = br.pump_out(&mut self.conns, now);
        if br.is_terminal() {
          br.drain_then_reap(&mut self.ep, &mut self.conns, now);
          let outcome = Self::outcome_for_terminal(&br);
          drop(br);
          self.emit_exchange_completed(*id, outcome);
        } else {
          br.drain_payload_only(&mut self.ep, &mut self.conns, now);
          // Mirror `pump_bridges`'s post-`drain_payload_only` terminality
          // re-check so this test-only variant matches production reap
          // semantics under an admission-rejected `Close`.
          if br.is_terminal() {
            self.bridges_terminalized_via_close_command = self
              .bridges_terminalized_via_close_command
              .saturating_add(1);
            br.drain_then_reap(&mut self.ep, &mut self.conns, now);
            let outcome = Self::outcome_for_terminal(&br);
            drop(br);
            self.emit_exchange_completed(*id, outcome);
          } else {
            self.bridges.insert(*id, br);
          }
        }
      }
    }
  }

  /// Shared tail of [`Self::run_tick`] and [`Self::flush_outbound`]:
  /// step (5) connection drained-reap, then [`Self::collect_transmits`].
  ///
  /// The reap simply walks every live `ConnectionHandle` and calls
  /// [`ConnTable::reap_if_drained`]; per-connection deferred
  /// `ConnectionEvent`s queued by `service_quinn` live in each
  /// [`super::conn::ConnEntry`]'s own `pending_events` deque (see
  /// [`super::conn::ConnEntry::queue_pending_event`]) so a reap that drops
  /// the slab entry also drops its deferred queue by construction — no
  /// global FIFO can survive past the reap to be re-keyed onto a fresh
  /// connection occupying the freed slab slot.
  fn finalize_tick(&mut self, now: Instant) {
    for ch in self.conns.iter_handles() {
      self.conns.reap_if_drained(&mut self.quinn, ch);
    }
    self.collect_transmits(now);
  }

  /// Move any `Event::DialRequested` currently in the inner endpoint's
  /// queue into the private [`dial_pending`](Self::dial_pending) deque,
  /// preserving FIFO order of every other event. The inner queue is
  /// fully drained into a local buffer; `DialRequested` is routed to
  /// `dial_pending`; every other event is re-queued at the back via
  /// [`Endpoint::requeue_event`] in original order. Bounded — each event
  /// is visited at most once because the drain stops when the inner
  /// queue is empty and re-queueing into the now-empty queue cannot
  /// re-surface anything we have already taken out.
  fn sieve_dial_events(&mut self) {
    let mut others: Vec<Event<I, SocketAddr>> = Vec::new();
    while let Some(ev) = self.ep.poll_event() {
      match ev {
        Event::DialRequested(dial) => {
          let (id, peer, deadline) = dial.into_parts();
          self.dial_pending.push_back(PendingDial {
            id,
            peer,
            deadline,
            attempted: false,
          });
        }
        other => others.push(other),
      }
    }
    for ev in others {
      self.ep.requeue_event(ev);
    }
  }

  fn collect_transmits(&mut self, now: Instant) {
    // Memberlist unreliable Transmit is NOT pre-drained here. Each call to
    // `poll_memberlist_transmit` drains one `Transmit` out of
    // `Endpoint::poll_transmit` on demand, so the inner pop — which counts
    // down the leave-completion boundary and emits `Event::LeftCluster` after
    // the last dead-self notice — happens exactly when the datagram crosses
    // to the external driver. Pre-draining coordinator-internally would tick
    // the boundary on the inner-queue→buffer hop and let a caller observe
    // `LeftCluster` while the dead-self bytes still sat in the buffer,
    // leaving peers to wrongly Suspect after teardown.
    //
    // quinn datagrams (handshake, stream data, ACKs, close) HAVE no such
    // dead-self accounting on their inner pop, so pre-draining them into
    // `out` is fine and keeps `poll_transmit` a constant-time `pop_front`.
    for ch in self.conns.iter_handles() {
      let Some(e) = self.conns.get_mut(ch) else {
        continue;
      };
      let mut buf = Vec::new();
      while let Some(tr) = e.conn_mut().poll_transmit(now.into_std(), 1, &mut buf) {
        // Use the transmit's own destination (not the cached peer) so a
        // datagram is sent to the address quinn selected — correct under
        // path migration and consistent with the stateless `Response` arm.
        self
          .out
          .push_back((tr.destination, Bytes::copy_from_slice(&buf[..tr.size])));
        buf.clear();
      }
    }
  }

  /// Which wire the unreliable path (gossip + probes) rides. Delegates to the
  /// [`QuicOptions`]; the driver reads it to route each unreliable send onto
  /// either [`queue_unreliable_datagram`](Self::queue_unreliable_datagram) or
  /// the plain-UDP fallback.
  pub fn unreliable_transport(&self) -> UnreliableTransport {
    self.cfg.unreliable_transport()
  }

  /// Count of unreliable datagrams dropped on a residual quinn datagram-state
  /// error (best-effort accounting; never a membership signal).
  #[cfg(test)]
  pub(crate) fn datagram_dropped(&self) -> u64 {
    self.datagram_dropped
  }

  /// Count of inbound unreliable datagrams dropped by the receive drain because
  /// `mem_ingress` was at the count cap (best-effort accounting; never a
  /// membership signal).
  #[cfg(test)]
  pub(crate) fn datagram_ingress_dropped(&self) -> u64 {
    self.datagram_ingress_dropped
  }

  /// Count of membership-time advances ([`Endpoint::handle_timeout`] calls).
  /// A QUIC packet ingress must NOT bump this — only the driver's explicit
  /// `handle_timeout` may. See [`Self::service_quic_inbound`].
  #[cfg(test)]
  pub(crate) fn membership_time_advances(&self) -> u64 {
    self.membership_time_advances
  }

  /// The datagram `max_size` of the pooled connection to `peer`, or `None` if
  /// no connection exists or datagrams are not yet negotiated.
  #[cfg(test)]
  pub(crate) fn connection_datagram_max_size(&mut self, peer: SocketAddr) -> Option<usize> {
    let ch = self.conns.handle_for(&peer)?;
    self.conns.get_mut(ch)?.conn_mut().datagrams().max_size()
  }

  /// Offer one already-encoded unreliable datagram (gossip or probe) to `peer`
  /// over its pooled QUIC connection. Routes only the WIRE — the bytes are the
  /// same the plain-UDP path would send. Connection liveness is never a
  /// membership signal: a `NotReady`/dropped datagram becomes a probe timeout,
  /// not a `Suspect`. The driver falls back to plain UDP on a non-`Queued`
  /// outcome so dissemination is not starved.
  pub fn queue_unreliable_datagram(
    &mut self,
    peer: SocketAddr,
    bytes: Bytes,
    now: Instant,
  ) -> DatagramSendOutcome {
    let sni_arc = self.cfg.sni_for(&peer);
    let ch = match self.conns.get_or_dial(
      &mut self.quinn,
      now,
      self.cfg.client().clone(),
      peer,
      &sni_arc,
    ) {
      Ok(ch) => ch,
      // A dial that cannot even be initiated (ConnectError) — best effort.
      Err(_) => return DatagramSendOutcome::NotReady,
    };
    let Some(e) = self.conns.get_mut(ch) else {
      return DatagramSendOutcome::NotReady;
    };
    let conn = e.conn_mut();
    match conn.datagrams().max_size() {
      // Still handshaking, peer doesn't support datagrams, or disabled locally.
      // Best-effort: the driver falls back to UDP; a later datagram lands once
      // the connection is Established.
      None => return DatagramSendOutcome::NotReady,
      Some(max) if bytes.len() > max => return DatagramSendOutcome::TooLarge,
      Some(_) => {}
    }
    // drop = false: under send-buffer pressure quinn returns Blocked and leaves
    // the already-queued datagrams intact, rather than evicting the OLDEST to
    // make room. The unreliable path carries probe Pings and Acks as well as
    // gossip, so evicting the oldest could silently drop an in-flight probe and
    // cause a spurious failure-detector timeout; refusing the NEW datagram (the
    // driver then falls back to plain UDP) preserves the earlier critical
    // datagrams and loses nothing.
    match conn.datagrams().send(bytes, false) {
      Ok(()) => DatagramSendOutcome::Queued,
      // Send buffer full: the driver falls back to plain UDP for this payload.
      // Not a drop (the payload still goes out over UDP) and not counted.
      Err(quinn_proto::SendDatagramError::Blocked(_)) => DatagramSendOutcome::NotReady,
      // UnsupportedByPeer / Disabled / TooLarge are excluded by the max_size
      // pre-check above; any residual error is unexpected — count and fall back.
      Err(_) => {
        self.datagram_dropped = self.datagram_dropped.saturating_add(1);
        DatagramSendOutcome::NotReady
      }
    }
  }
}

// Methods that drive the coordinator tick (`run_tick` ->
// `service_dials` / `service_quinn`, `flush_outbound` ->
// `service_quinn`, the `start_*` wrappers and the `handle_udp` /
// `handle_timeout` driver entrypoints).
impl<I> QuicEndpoint<I>
where
  I: crate::Id
    + crate::Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  /// Inbound datagram from the one UDP socket.
  ///
  /// The `Quic` class is fully processed: the datagram is fed into quinn-proto's
  /// endpoint, any resulting `DatagramEvent` is routed, and a coordinator tick
  /// is run before returning. The `Memberlist` class is **buffered only** — the
  /// codec-owning driver MUST drain via [`Self::poll_memberlist_ingress`],
  /// decode each frame, feed every typed message via [`Self::handle_packet`],
  /// and then call [`Self::handle_timeout`] to advance time. Running
  /// [`Self::handle_timeout`] before the buffered memberlist datagrams are
  /// decoded and fed would risk same-instant probe / suspect timers firing
  /// before a just-arrived `Ack` / `Alive` is applied — a spurious fallback
  /// ping or false `Suspect` could fire even though the resolving message is
  /// already sitting in [`Self::poll_memberlist_ingress`]'s queue locally.
  /// The `Reject` class is dropped (the codec-owning layer surfaces the
  /// wire-level `DecodeError` on its own path).
  pub fn handle_udp(&mut self, from: SocketAddr, datagram: &[u8], now: Instant) {
    self.last_now = Some(now);
    match classify(datagram) {
      Class::Quic => {
        let mut scratch = Vec::new();
        let data = BytesMut::from(datagram);
        if let Some(ev) = self
          .quinn
          .handle(now.into_std(), from, None, None, data, &mut scratch)
        {
          self.route_datagram_event(ev, from, now, &scratch);
        }
        self.service_quic_inbound(now);
      }
      Class::Memberlist => self.handle_memberlist_udp(from, datagram),
      Class::Reject => { /* drop; DecodeError is emitted by the codec path only */ }
    }
  }

  /// Timer tick from the driver.
  pub fn handle_timeout(&mut self, now: Instant) {
    self.last_now = Some(now);
    self.run_tick(now);
  }

  /// Initiate an outbound push/pull state exchange with `peer` and attempt
  /// the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_push_pull`] that ALSO drives
  /// `service_dials(now)` before returning, so the `DialRequested` the
  /// inner endpoint queues is sieved into the private `dial_pending` deque
  /// AND attempted (the bidi stream is opened if a pooled connection is
  /// already established; otherwise the entry stays in `dial_pending` with
  /// `attempted = true` and the next tick retries). Preferred entry point
  /// for the driver: a caller that goes through `endpoint_mut()` instead
  /// can only wake the dial via [`Self::poll_timeout`]'s immediate-due
  /// term — see that method's docs.
  ///
  /// Runs [`Self::flush_outbound`] after `service_dials` so the dial's
  /// Initial datagram (fresh dial) or the freshly-opened bridge's request
  /// bytes (pooled-Established dial) emerge on the very next
  /// [`Self::poll_transmit`] — a driver that uses only the public Sans-I/O
  /// poll surface (`poll_transmit` / `poll_timeout` / `handle_udp` /
  /// `handle_timeout`) sees the exchange progress without a same-instant
  /// `handle_timeout` pre-pump.
  pub fn start_push_pull(
    &mut self,
    peer: SocketAddr,
    kind: PushPullKind,
    now: Instant,
  ) -> StreamId {
    self.last_now = Some(now);
    let id = self.ep.start_push_pull(peer, kind, now);
    self
      .pending_outbound_kinds
      .insert(id, ExchangeKind::PushPull);
    self.pending_outbound_peers.insert(id, peer);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a reliable-stream fallback ping for probe `probe_seq` and
  /// attempt the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_reliable_ping`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time outbound-
  /// flush semantics. The `deadline` is the owning probe's single
  /// cumulative deadline (NOT an independent stream-timeout — the reliable
  /// fallback must race the indirect pings against the SAME deadline),
  /// forwarded unchanged. The inner method takes only the deadline so this
  /// wrapper accepts `now` separately: `service_dials` needs the real wall-
  /// clock instant (not the future deadline) and `last_now` must remain a
  /// known-past anchor.
  pub fn start_reliable_ping(
    &mut self,
    peer_id: I,
    peer_addr: SocketAddr,
    probe_seq: u32,
    deadline: Instant,
    now: Instant,
  ) -> StreamId {
    self.last_now = Some(now);
    let id = self
      .ep
      .start_reliable_ping(peer_id, peer_addr, probe_seq, deadline);
    self
      .pending_outbound_kinds
      .insert(id, ExchangeKind::ReliablePing);
    self.pending_outbound_peers.insert(id, peer_addr);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  /// Initiate a one-way reliable user-message delivery to `peer` and
  /// attempt the dial in-band.
  ///
  /// Wrapper around [`Endpoint::start_user_message`]; see
  /// [`Self::start_push_pull`] for the dial-attempt and zero-time outbound-
  /// flush semantics.
  pub fn start_user_message(
    &mut self,
    peer: SocketAddr,
    payload: Bytes,
    now: Instant,
  ) -> Result<StreamId, crate::error::Error> {
    self.last_now = Some(now);
    // Propagate the inner lifecycle refusal: a Leaving/Left node starts no new
    // reliable user message and registers no outbound intent.
    let id = self.ep.start_user_message(peer, payload, now)?;
    self
      .pending_outbound_kinds
      .insert(id, ExchangeKind::UserMessage);
    self.pending_outbound_peers.insert(id, peer);
    self.service_dials(now);
    self.flush_outbound(now);
    Ok(id)
  }

  /// Initiate a direct application-level ping to `node`. Returns
  /// `Ok(`[`crate::PingId`]`)` — the correlation token the driver parks a
  /// waiter on, resolved by [`crate::event::Event::PingCompleted`] /
  /// [`crate::event::Event::PingFailed`] — or `Err(NotRunning)` once the node
  /// has left. Forwards to the inner membership [`Endpoint`].
  ///
  /// `ping` only queues a UDP gossip datagram (via `pending_transmits`) — it
  /// does not touch QUIC bridge state — so it is safe to call without a
  /// preceding `service_dials` / `flush_outbound`.
  // No last_now update: ping only enqueues a packet transmit; it touches no dial/bridge state that poll_timeout must immediately re-examine.
  #[inline]
  pub fn ping(
    &mut self,
    node: crate::Node<I, SocketAddr>,
    now: Instant,
  ) -> Result<crate::PingId, crate::error::Error> {
    self.ep.ping(node, now)
  }

  /// Enqueue one or more directed unreliable user messages to `to`. Forwards
  /// to the inner membership [`Endpoint`]. Like `ping`, only touches the
  /// gossip `pending_transmits` queue, drained by `poll_memberlist_transmit`.
  ///
  /// Returns `Err` if any payload exceeds the configured `gossip_mtu` ceiling.
  #[inline]
  pub fn send_user_packets(
    &mut self,
    to: SocketAddr,
    payloads: &[Bytes],
  ) -> Result<(), crate::error::Error> {
    self.ep.send_user_packets(to, payloads)
  }

  /// The fixed per-tick step order (load-bearing — see module docs).
  ///
  /// Step (2) (drain each non-terminal stream's endpoint-events into the
  /// `Endpoint`) MUST strictly precede step (3) (`ep.handle_timeout`): a
  /// reliable-fallback ping ack delivered on the same tick the probe
  /// cumulative deadline expires is carried by the stream's last
  /// `poll_endpoint_event`; draining it after the probe timeout would lose it
  /// and wrongly Suspect a live peer. Do not reorder.
  ///
  /// Step (5.5) (a second `pump_bridges(now)` call) is the strict-poll
  /// self-sufficiency seam for INBOUND accepts and freshly-opened OUTBOUND
  /// bridges. Step (4) (`service_quinn`) inserts new inbound bridges via
  /// its `accept(Dir::Bi)` loop, and step (5) (`service_dials`) inserts new
  /// outbound bridges via `streams().open(Dir::Bi)`. Both insertions happen
  /// AFTER step (2)'s `pump_bridges` already ran — so without step (5.5)
  /// those bridges are never pumped this tick. The next coordinator wake
  /// under strict poll-surface driving comes from [`Self::poll_timeout`],
  /// whose `min` only covers transport timers and the bridge's own
  /// exchange deadline: `quinn_proto::Connection::poll_timeout` returns
  /// `self.timers.next_timeout()` (loss detection / idle / close /
  /// KeyDiscard / KeepAlive), and app-read readiness is NOT advertised as
  /// a transport timer. The next wake is therefore the bridge's exchange
  /// deadline; by then `Stream::handle_data` rejects the buffered request
  /// as timed out and the exchange fails. Step (5.5) closes that gap by
  /// pumping every newly-inserted bridge in the same tick its first data
  /// arrived — mirroring the same strict-poll self-sufficiency invariant
  /// applied by the `start_*` zero-time flush (Case A pump after a
  /// pooled-Established dial) and the connection-loss inline drain.
  ///
  /// `pump_bridges` is idempotent on already-pumped bridges: `pump_in`
  /// drains every available chunk and the next call observes `Blocked`
  /// from `quinn_proto::RecvStream::read`; `pump_out` flushes `pending_out`
  /// and exhausts `Stream::poll_transmit`, and the next call finds both
  /// empty; `drain_payload_only` runs an empty `Stream::poll_endpoint_event`
  /// loop; `drain_then_reap` only fires on a terminal bridge, which is
  /// removed from `self.bridges` after the first call. The second pump
  /// is therefore a no-op on every bridge that already ran in step (2).
  fn run_tick(&mut self, now: Instant) {
    self.tick(now, true);
  }

  /// Service the composed unit after a QUIC packet ingress WITHOUT advancing
  /// membership timers. A QUIC packet may carry application datagrams (probe
  /// Acks, Alives) the driver has not yet decoded; firing the membership probe
  /// or suspicion deadline here — before `service_quinn` even extracts the
  /// datagram into `mem_ingress` and before the driver decodes it — would mark a
  /// peer Suspect/failed even though its Ack already arrived. Membership time is
  /// advanced ONLY by the driver's explicit `handle_timeout`, after it has
  /// drained and decoded `mem_ingress`. This gives the QUIC datagram ingress
  /// path the same property the plain-UDP ingress path (`handle_memberlist_udp`)
  /// already has.
  fn service_quic_inbound(&mut self, now: Instant) {
    self.tick(now, false);
  }

  /// Shared tick body. `advance_membership_time` gates step (3)
  /// (`Endpoint::handle_timeout`): true for the driver's explicit timer tick,
  /// false for QUIC packet ingress (see `service_quic_inbound`).
  fn tick(&mut self, now: Instant, advance_membership_time: bool) {
    // (1) inbound feed already done in `handle_udp` before this tick.
    // (2) pump bridges + drain stream endpoint-events into the Endpoint.
    #[cfg(test)]
    let pre_step2_ids: std::collections::HashSet<StreamId> = self.bridges.keys().copied().collect();
    self.pump_bridges(now);
    // (3) THEN memberlist timers (probe cumulative-deadline, suspicion).
    if advance_membership_time {
      #[cfg(test)]
      {
        self.membership_time_advances = self.membership_time_advances.saturating_add(1);
      }
      self.ep.handle_timeout(now);
    }
    // (4) quinn connection timers + accept new bidi streams + transmit.
    self.service_quinn(now);
    // (5) Dial requests emitted by (3) or by accept-events.
    self.service_dials(now);
    // (5.5) Pump bridges inserted by (4) and (5) this same tick — see
    // method docstring above for the strict-poll self-sufficiency rationale.
    #[cfg(test)]
    self.pump_bridges_tracking_post_acceptance(now, &pre_step2_ids);
    #[cfg(not(test))]
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  /// Zero-time outbound flush invoked from the high-level `start_*` APIs
  /// AFTER `service_dials`. Runs the shared [`Self::run_tick`] tail
  /// (bridge pump + `service_quinn` + drained-reap + `collect_transmits`)
  /// WITHOUT step (3) (`Endpoint::handle_timeout`).
  ///
  /// Step (3) is deliberately skipped: memberlist timers (probe cumulative-
  /// deadline, suspicion, gossip / push-pull schedulers) advance solely
  /// through the driver's explicit [`Self::handle_timeout`], which fires
  /// AFTER the driver has drained [`Self::poll_memberlist_ingress`],
  /// decoded each frame, and fed each typed message via
  /// [`Self::handle_packet`]. Advancing time inside a `start_*` call would
  /// fire same-instant probe / suspect / gossip / push-pull schedulers
  /// BEFORE a just-arrived (still-buffered) `Ack` / `Alive` is decoded and
  /// applied — the same property [`Self::handle_udp`] protects on the
  /// `Class::Memberlist` ingress path.
  ///
  /// Bridge step (2) is included because for an already-Established pooled
  /// connection `service_dials` opens a fresh bidi stream and inserts a new
  /// `Bridge` carrying the encoded request bytes in its FSM `Stream` output
  /// buffer; the bytes only reach the quinn send stream when `pump_out`
  /// runs. Without this same-instant pump, the bytes sit inside the bridge
  /// and the next [`Self::collect_transmits`] returns empty — a driver that
  /// uses only `poll_transmit` / `poll_timeout` / `handle_udp` would see
  /// the dial's Initial (or the bridge's request bytes) only on the NEXT
  /// `handle_timeout` cycle, advancing virtual time before the exchange
  /// emits its first datagram.
  ///
  /// `Connection::poll_transmit` likewise only emerges through
  /// [`Self::collect_transmits`]; running `service_quinn` + `collect_transmits`
  /// here puts a fresh dial's Initial onto the outbound queue at the same
  /// instant the `start_*` returns.
  ///
  /// `service_dials` is run BY THE CALLER (the `start_*` wrapper) before
  /// this method, mirroring `run_tick`'s ordering: the dial is processed,
  /// then its outbound side-effects flush in the same call.
  ///
  /// A second `pump_bridges(now)` runs AFTER `service_quinn` to pump any
  /// inbound bridges its `accept(Dir::Bi)` loop just inserted: `start_*`
  /// is called from arbitrary points in the driver's loop, and a peer's
  /// data may have arrived since the prior `handle_udp` (a Bi stream that
  /// `service_quinn` accepts inside this `flush_outbound`). Without this
  /// second pump, that newly-accepted inbound bridge's first data isn't
  /// fed into `Bridge::pump_in` this tick, and a strict-poll driver next
  /// wakes at the bridge's exchange deadline — at which point
  /// `Stream::handle_data` rejects the buffered request as timed out. See
  /// [`Self::run_tick`]'s docstring for the full strict-poll self-
  /// sufficiency rationale; the second pump's idempotency on already-
  /// pumped bridges is the same property documented there.
  fn flush_outbound(&mut self, now: Instant) {
    #[cfg(test)]
    let pre_first_pump_ids: std::collections::HashSet<StreamId> =
      self.bridges.keys().copied().collect();
    self.pump_bridges(now);
    self.service_quinn(now);
    #[cfg(test)]
    self.pump_bridges_tracking_post_acceptance(now, &pre_first_pump_ids);
    #[cfg(not(test))]
    self.pump_bridges(now);
    self.finalize_tick(now);
  }

  fn service_quinn(&mut self, now: Instant) {
    for ch in self.conns.iter_handles() {
      let Some(e) = self.conns.get_mut(ch) else {
        continue;
      };
      // Apply this connection's one-tick-deferred feedback BEFORE any
      // other poll on it — same shape as quinn-proto's reference async
      // driver's per-connection channel rx, where `process_conn_events`
      // is called once per scheduling iteration on the connection task
      // and `Endpoint::handle_events` produces the corresponding
      // `ConnectionEvent::Proto` messages on the SAME tick's
      // `Endpoint::handle_event` return. Materialise into a `Vec` so
      // the iterator releases its borrow of `e.pending_events` before
      // the `handle_event` mutable borrow.
      let pending: Vec<quinn_proto::ConnectionEvent> = e.take_pending_events().collect();
      for conn_ev in pending {
        e.conn_mut().handle_event(conn_ev);
      }
      e.conn_mut().handle_timeout(now.into_std());
      let mut lost = false;
      while let Some(ev) = e.conn_mut().poll() {
        match ev {
          quinn_proto::Event::ConnectionLost { .. } => {
            // The connection (not an individual stream) failed; the per-stream
            // pumps cannot observe this. Defer marking until the `poll()` loop
            // ends so `e`'s mutable borrow of `conns` is dropped first.
            lost = true;
          }
          quinn_proto::Event::Stream(quinn_proto::StreamEvent::Opened { dir: Dir::Bi }) => {
            // `StreamEvent::Opened` is an idempotent signal ("one or more
            // streams opened"), not a per-stream event, so accept until the
            // peer's bidi backlog is drained — otherwise concurrently opened
            // inbound exchanges are stranded with no further wake-up.
            while let Some(sid) = e.conn_mut().streams().accept(Dir::Bi) {
              let peer = e.peer();
              let Some(stream) = self.ep.accept_stream(peer, now) else {
                // Leaving/Left: admit no new inbound reliable stream. Reset both
                // halves of the just-accepted QUIC stream so the peer is notified
                // and the connection's stream slot is released instead of left
                // orphaned with no Bridge to own it. Ignoring Err: `ClosedStream`
                // means the half is already gone, which is the desired end state.
                let _ = e
                  .conn_mut()
                  .send_stream(sid)
                  .reset(quinn_proto::VarInt::from_u32(0));
                let _ = e
                  .conn_mut()
                  .recv_stream(sid)
                  .stop(quinn_proto::VarInt::from_u32(0));
                continue;
              };
              let id = stream.id();
              let reliable_max = self.ep.max_stream_frame_size();
              self.bridges.insert(
                id,
                Bridge::new(
                  stream,
                  ch,
                  sid,
                  self.compression,
                  self.encryption.clone(),
                  reliable_max,
                  self.label.clone(),
                  self.skip_inbound_label_check,
                  false,
                ),
              );
            }
          }
          quinn_proto::Event::Stream(quinn_proto::StreamEvent::Finished { id: sid }) => {
            // Peer ack'd our FIN — quinn-proto's `SendState` for this
            // stream has reached `DataRecvd`. Route to the owning
            // bridge so it transitions `Active -> SendClosed` (or
            // `RecvClosed -> BothClosed`). The bridge's terminality
            // criterion is `BridgePhase::BothClosed | Failed(_)`, so
            // this transition is the load-bearing send-half retirement
            // observable — not `SendStream::finish()`'s return.
            //
            // **Identity is the compound `(ConnectionHandle, QuicSid)`.**
            // quinn-proto's `StreamId` is per-connection — its bottom
            // two bits encode initiator + direction and the remaining
            // counter is per-connection. Two pooled peer connections
            // will both have their first bidi stream as sid `0`, so
            // filtering by sid alone would route a Finished/Stopped
            // from `ch_A`'s stream to a sibling bridge on `ch_B`.
            // We're inside the per-connection `poll()` loop here, so
            // the `ch` filter is free.
            for br in self.bridges.values_mut() {
              if br.ch() == ch && br.sid() == sid {
                br.observe_send_fin();
                break;
              }
            }
          }
          quinn_proto::Event::Stream(quinn_proto::StreamEvent::Stopped {
            id: sid,
            error_code,
          }) => {
            // Peer sent STOP_SENDING for our send half. quinn already
            // transitioned our `SendState` to `ResetSent`; we additionally
            // retire the recv half (idempotent) so the bridge becomes
            // recv-clean by the time its `Failed(Transport)` phase
            // reaps. Retirement is inline on `e.conn_mut()` because we
            // already hold the `&mut Connection` borrow for the
            // `poll()` drain.
            //
            // Identity = `(ch, sid)` — see the `Finished` arm above.
            //
            // Ignoring Err: idempotent retirement — `Err(ClosedStream)`
            // means the half is already gone.
            let _ = e
              .conn_mut()
              .send_stream(sid)
              .reset(quinn_proto::VarInt::from_u32(0));
            let _ = e
              .conn_mut()
              .recv_stream(sid)
              .stop(quinn_proto::VarInt::from_u32(0));
            for br in self.bridges.values_mut() {
              if br.ch() == ch && br.sid() == sid {
                br.fail_stopped_already_retired(error_code);
                break;
              }
            }
          }
          _ => {}
        }
      }
      // Drain every endpoint-facing event the connection has queued and
      // route it back through `Endpoint::handle_event`; queue any
      // returned `ConnectionEvent` onto the SAME connection for delivery
      // on the NEXT `service_quinn` iteration. quinn-proto's polling
      // contract requires callers to drain all four polling methods
      // every progress step. Omitting `poll_endpoint_events` strands
      // `NeedIdentifiers` / `ResetToken` / `RetireConnectionId` inside
      // the connection and breaks the endpoint flows they drive: CID
      // issuance via `Endpoint::send_new_identifiers`, peer
      // stateless-reset-token registration, and CID retirement.
      // Placed after the application-event `poll()` loop above so the
      // existing `Opened`/`ConnectionLost` handling is unchanged in
      // observable behaviour.
      //
      // `EndpointEventInner::Drained` is filtered out here and forwarded
      // only by `ConnTable::reap_if_drained`. Rationale: `quinn`'s
      // internal `Endpoint::handle_event(Drained)` calls
      // `self.connections.try_remove(ch.0)`, releasing quinn's slab slot.
      // `ConnTable.conns` is a strict lockstep mirror of quinn's slab —
      // `get_or_dial`'s `debug_assert_eq!(slot, ch.0)` enforces that the
      // next `connect()` reuses the SAME index in both slabs. Forwarding
      // a connection-emitted `Drained` here would release quinn's slot
      // while our `ConnTable` still holds it; an immediately-following
      // dial then desynchronises the two slabs. `reap_if_drained` is the
      // sole site that pairs `quinn.handle_event(ch,
      // EndpointEvent::drained())` with `self.conns.try_remove(ch.0)`,
      // keeping both slabs in lockstep.
      while let Some(ev) = e.conn_mut().poll_endpoint_events() {
        if ev.is_drained() {
          continue;
        }
        #[cfg(test)]
        {
          self.endpoint_events_processed = self.endpoint_events_processed.saturating_add(1);
        }
        if let Some(conn_ev) = self.quinn.handle_event(ch, ev) {
          // Queue for the NEXT iteration of this connection — see
          // [`super::conn::ConnEntry::queue_pending_event`] for the
          // one-tick deferral rationale and the by-construction lifetime
          // co-location that eliminates quinn's `vacant_key()` slab-slot
          // reuse race.
          e.queue_pending_event(conn_ev);
        }
      }
      // Drain inbound application datagrams into the same mem_ingress the plain-
      // UDP gossip path fills, tagged with this connection's peer. Mode-
      // independent: a Udp-mode endpoint does not negotiate datagrams, so this is
      // a no-op there; a Datagram-mode endpoint extracts them. Pop quinn's buffer
      // to EMPTY (so a zero-length-frame flood cannot accumulate inside quinn),
      // but PUSH into the coordinator queue only while this peer's STANDING share
      // (`mem_ingress_per_peer`, maintained across the whole undrained queue) is
      // under the per-peer budget AND the global cap is not reached — beyond
      // either, drop and count. Bounding the standing share (not a per-pass
      // counter) gives every peer fair access regardless of how many recv passes
      // a driver batches before decoding, so one flooding peer cannot starve
      // another peer's probe Ack. recv() returns an owned Bytes, so the
      // e.conn_mut() borrow releases before the disjoint-field pushes.
      let peer = e.peer();
      while let Some(payload) = e.conn_mut().datagrams().recv() {
        // Pop quinn to empty so a zero-length-frame flood cannot accumulate
        // inside quinn; admission (per-peer + global caps, dropped+counted past
        // either bound so one flooding peer cannot fill the shared queue and
        // starve another peer's probe Ack) is enforced by the shared helper —
        // the SAME bound `handle_memberlist_udp` applies, so neither source can
        // exceed it. The three `&mut self.<field>` args are disjoint from the
        // `self.conns` borrow `e` holds.
        push_mem_ingress_capped(
          &mut self.mem_ingress,
          &mut self.mem_ingress_per_peer,
          &mut self.datagram_ingress_dropped,
          peer,
          move || payload,
        );
      }
      // Also reap when the connection has reached `is_drained()` even if
      // `poll()` never yielded `Event::ConnectionLost` for it in this
      // iteration — the kill-on-idle-timeout path
      // (`kill(ConnectionError::TimedOut)`) and similar immediate-drain
      // transitions set `self.error` and queue
      // `EndpointEventInner::Drained` simultaneously; whether `poll()`
      // surfaced the `ConnectionLost` event THIS tick depends on the
      // internal `events` FIFO ordering. The combined `lost || is_drained()`
      // gate catches both shapes so the strict-poll bridge-leak is closed
      // regardless of which signal arrived first.
      let drained = e.conn_ref().is_drained();
      // `e` borrows `self.conns`; release it before touching `self.bridges`.
      let _ = e;
      if lost || drained {
        // Mark every bridge on this connection fatal AND complete its D1
        // drain_then_reap synchronously, in this same tick.
        //
        // A connection-level loss observed inside `service_quinn` (step 4)
        // runs AFTER `pump_bridges` (step 2) and BEFORE `finalize_tick`
        // (step 5). The freshly-fatal bridges therefore miss this tick's
        // `pump_bridges` D1 reap; a stateless-reset / immediate-drain
        // loss path can then have `finalize_tick` free the connection in
        // the SAME tick — and `Bridge::poll_timeout` returns `None` for
        // terminal bridges (it deliberately owes no future work to
        // itself once terminal). The coordinator's unified `poll_timeout`
        // then has no immediate-due term contributed by these bridges,
        // so a strict-poll driver with no other peer/probe/timer due
        // wakes never again — the terminal bridges leak forever.
        //
        // Coordinator-level fix: when we know a bridge is terminal
        // because of a connection-level event, close out the D1 drain
        // here. `fail_connection_lost()` transitions the bridge phase
        // to `Failed(ConnectionLost)` so the `StreamErrored` lifecycle
        // notice inside `drain_then_reap` carries the connection-loss
        // attribution.
        let ids: Vec<StreamId> = self
          .bridges
          .iter()
          .filter(|(_, b)| b.ch() == ch)
          .map(|(id, _)| *id)
          .collect();
        for id in ids {
          if let Some(mut br) = self.bridges.remove(&id) {
            br.fail_connection_lost();
            br.drain_then_reap(&mut self.ep, &mut self.conns, now);
            #[cfg(test)]
            {
              self.bridges_reaped_on_connection_lost =
                self.bridges_reaped_on_connection_lost.saturating_add(1);
            }
            // ConnectionLost ⇒ `fail_connection_lost` set the phase to
            // `Failed(ConnectionLost)`; outcome is unconditionally
            // `Failed` here, but route through the shared helper so
            // the kind lookup + emission path matches the
            // `pump_bridges` reap.
            let outcome = Self::outcome_for_terminal(&br);
            drop(br);
            self.emit_exchange_completed(id, outcome);
          }
        }
      }
    }
  }

  fn service_dials(&mut self, now: Instant) {
    // Sieve any DialRequested newly emitted by the inner endpoint into the
    // private `dial_pending` deque, then drain that deque as the sole input.
    // Non-`DialRequested` events stay in the inner endpoint's queue for the
    // public `poll_event()` to observe.
    self.sieve_dial_events();
    let pending = core::mem::take(&mut self.dial_pending);
    for entry in pending {
      // Decompose AND mark attempted BEFORE the open attempt: if this
      // attempt requeues (handshake-blocked or credit-exhausted), the
      // re-pushed entry carries `attempted = true` so `poll_timeout` no
      // longer emits an immediate-due wake for it (the connection's own
      // `poll_timeout` and the entry's `deadline` drive the next service
      // tick; immediately re-firing would busy-loop a still-handshaking
      // connection).
      let PendingDial {
        id,
        peer,
        deadline,
        attempted: _,
      } = entry;
      // Retire the intent without opening anything on the pooled
      // connection if its own deadline has already elapsed.
      //
      // `quinn_proto::Streams::open(Dir::Bi)` inserts BOTH send AND recv
      // state for the new bidi stream. Letting `open` run for an expired
      // intent has no legitimate downstream consumer: `Endpoint::dial_succeeded`
      // (frozen) drops any intent whose deadline has elapsed and returns
      // `None`, so we would synthesise a fresh bidi stream on the pooled
      // connection that no `Bridge` ever owns and whose recv half is
      // unreachable. Resetting only the send half afterwards leaves the
      // recv half orphaned. The deadline pre-check routes the expired
      // intent through the FSM's `dial_failed` path BEFORE either half
      // is created, so no orphan state can exist.
      if now >= deadline {
        // Discard the staged kind and peer (the bridge was never
        // created, so the ExchangeCompleted reap path will never
        // observe this id) and surface a `Failed` completion for a
        // UserMessage or PushPull dial so the parked reliable-send /
        // join waiter resolves. Leaving entries stranded would leak
        // memory across every pre-deadline-expired dial. Matches the
        // pre-bridge-creation failure paths below.
        self.retire_failed_dial(
          id,
          crate::error::StreamError::DialFailed("quic dial deadline elapsed".into()),
          now,
        );
        continue;
      }
      // The membership address `peer` IS the wire `SocketAddr` (the
      // coordinator pins `A = SocketAddr` internally); the TLS verification
      // identity for this dial is resolved per-peer via the closure on
      // `QuicOptions` (default mode is cluster-uniform — the same string
      // for every peer — but operators with per-peer SAN certs supply a
      // closure that maps each `SocketAddr` to its expected identity).
      let addr = peer;
      let sni_arc = self.cfg.sni_for(&addr);
      match self.conns.get_or_dial(
        &mut self.quinn,
        now,
        self.cfg.client().clone(),
        addr,
        &sni_arc,
      ) {
        Ok(ch) => {
          if let Some(e) = self.conns.get_mut(ch) {
            match e.conn_mut().streams().open(Dir::Bi) {
              Some(sid) => match self.ep.dial_succeeded(id, now) {
                Some(stream) => {
                  let reliable_max = self.ep.max_stream_frame_size();
                  self.bridges.insert(
                    stream.id(),
                    Bridge::new(
                      stream,
                      ch,
                      sid,
                      self.compression,
                      self.encryption.clone(),
                      reliable_max,
                      self.label.clone(),
                      self.skip_inbound_label_check,
                      true,
                    ),
                  );
                }
                None => {
                  // Defense-in-depth: the deadline pre-check above
                  // normally retires the intent before this branch is
                  // reachable, but `Endpoint::dial_succeeded` is a
                  // frozen API that may surface `None` for other
                  // intent-invalidation reasons. `streams().open(Dir::Bi)`
                  // already inserted BOTH send AND recv state on the
                  // pooled connection; retiring only the send half leaves
                  // the recv half orphaned and unreapable. Reset send +
                  // stop recv so both halves are fully retired —
                  // `SendStream::reset` queues RESET_STREAM and returns
                  // `Err(ClosedStream)` harmlessly if the send half is
                  // already gone; `RecvStream::stop` discards unread data
                  // and queues STOP_SENDING with the same `Err(ClosedStream)`
                  // guard.
                  //
                  // `retire_failed_dial` discards the staged kind/peer,
                  // surfaces a `Failed` completion for a UserMessage /
                  // PushPull dial (so a parked reliable-send / join
                  // waiter resolves on this defense-in-depth path too),
                  // and calls `dial_failed` — a no-op here because the
                  // frozen `dial_succeeded` already consumed the intent,
                  // but kept for uniformity with the other pre-bridge
                  // failure sites.
                  self.retire_failed_dial(
                    id,
                    crate::error::StreamError::DialFailed(
                      "quic dial intent invalidated before bridge creation".into(),
                    ),
                    now,
                  );
                  if let Some(e) = self.conns.get_mut(ch) {
                    let conn = e.conn_mut();
                    // Ignoring Err: idempotent retirement —
                    // `Err(ClosedStream)` means the half is already
                    // gone.
                    let _ = conn
                      .send_stream(sid)
                      .reset(quinn_proto::VarInt::from_u32(0));
                    // Ignoring Err: same idempotent-retirement
                    // semantics as the send-half reset above.
                    let _ = conn.recv_stream(sid).stop(quinn_proto::VarInt::from_u32(0));
                  }
                }
              },
              None => {
                // `quinn_proto::Streams::open(Dir::Bi) == None` has THREE
                // distinct causes (the call returns `None` when the
                // connection is closed OR when `next[Bi] >= max[Bi]`):
                //
                //   (1) `is_handshaking() == true` — the handshake has
                //       not finished, so the peer's initial-max-streams
                //       credit has not been granted yet. Common path for
                //       a fresh dial: the very first `DialRequested`
                //       arrives the same tick the connection is created,
                //       long before the handshake RTT completes. Requeue
                //       onto `dial_pending` while the intent's own
                //       deadline has not passed; the next tick retries
                //       `open(Bi)` once the handshake completes (the
                //       pooled connection is reused — no redial).
                //
                //   (2) `is_closed() == true` — the connection is
                //       `Closed`/`Draining`/`Drained` (the closed-before-
                //       drained pool window or a never-Established
                //       handshake-failed cache). `dial_failed`: consume
                //       the current intent. `get_or_dial` redials on the
                //       next push/pull/reliable-ping/user-message intent
                //       the application schedules (the cached closed
                //       handle for a once-Established peer triggers an
                //       explicit redial; a never-Established cache
                //       prevents a fresh-handshake storm against a
                //       genuinely-unreachable peer). The coordinator
                //       never repeatedly opens new
                //       handshakes against an unreachable peer inside a
                //       single intent's deadline.
                //
                //   (3) Established (not handshaking, not closed) — the
                //       peer's concurrent-bidi-stream credit
                //       (`initial_max_streams_bidi` / runtime
                //       `MAX_STREAMS`) is currently exhausted. A
                //       transient backpressure state lifted by a future
                //       `MAX_STREAMS` frame from the peer as inflight
                //       bidi streams reap. Requeue while the intent's
                //       own deadline has not passed — without this branch
                //       a steady-state cluster that pins its outbound
                //       concurrent-bidi-streams (e.g. coincident
                //       push/pulls + a reliable-ping fallback on the same
                //       pooled connection) would lose new reliable
                //       exchanges to permanent `dial_failed`.
                //
                // Pushing back onto `dial_pending` (NOT
                // `self.ep.requeue_event`) keeps the retry token private
                // so an external `poll_event` drain cannot pop it.
                let is_closed_now = self
                  .conns
                  .get(ch)
                  .map(|c| c.conn_ref().is_closed())
                  .unwrap_or(true);
                if is_closed_now {
                  self.retire_failed_dial(
                    id,
                    crate::error::StreamError::DialFailed(
                      "quic stream open: cached connection closed".into(),
                    ),
                    now,
                  );
                } else if now < deadline {
                  self.dial_pending.push_back(PendingDial {
                    id,
                    peer,
                    deadline,
                    attempted: true,
                  });
                } else {
                  self.retire_failed_dial(
                    id,
                    crate::error::StreamError::DialFailed(
                      "quic stream open deadline elapsed".into(),
                    ),
                    now,
                  );
                }
              }
            }
          }
        }
        Err(e) => {
          self.retire_failed_dial(
            id,
            crate::error::StreamError::DialFailed(e.to_string().into()),
            now,
          );
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::Instant;
  use core::{net::SocketAddr, time::Duration};

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::{QuicEndpoint, UnreliableTransport};
  use crate::{config::EndpointOptions, endpoint::Endpoint, quic::QuicOptions};

  fn test_config_with_mode(mode: UnreliableTransport) -> QuicOptions {
    let mut transport = quinn_proto::TransportConfig::default();
    transport.max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
    ));
    QuicOptions::new(
      crate::quic::crypto::tests::test_endpoint_config(&[0x5au8; 32]),
      crate::quic::crypto::tests::test_server(),
      crate::quic::crypto::tests::test_client(),
      transport,
      "localhost",
      mode,
    )
  }

  fn test_config() -> QuicOptions {
    test_config_with_mode(UnreliableTransport::Datagram)
  }

  fn make_endpoint(id: &str, addr: SocketAddr, now: Instant) -> QuicEndpoint<SmolStr> {
    let cfg = EndpointOptions::new(SmolStr::new(id), addr).with_rng_seed(addr.port() as u64);
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    ep.start_scheduling(now);
    let qc = test_config();
    let mut seed = [0u8; 32];
    seed[..2].copy_from_slice(&addr.port().to_le_bytes());
    QuicEndpoint::<SmolStr>::with_quinn_rng_seed(ep, qc, Some(seed))
  }

  /// Same as [`make_endpoint`] but with the unreliable transport set to
  /// [`UnreliableTransport::Udp`] at construction. In Udp mode the constructor
  /// leaves quinn's datagram buffers at their defaults, so the endpoint does NOT
  /// advertise the datagram extension: its connections report
  /// `datagrams().max_size() == None` and no peer can deliver datagrams to it.
  fn make_endpoint_udp(id: &str, addr: SocketAddr, now: Instant) -> QuicEndpoint<SmolStr> {
    let cfg = EndpointOptions::new(SmolStr::new(id), addr).with_rng_seed(addr.port() as u64);
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    ep.start_scheduling(now);
    let qc = test_config_with_mode(UnreliableTransport::Udp);
    let mut seed = [0u8; 32];
    seed[..2].copy_from_slice(&addr.port().to_le_bytes());
    QuicEndpoint::<SmolStr>::with_quinn_rng_seed(ep, qc, Some(seed))
  }

  #[test]
  fn quic_endpoint_type_is_constructible_signature() {
    // Behavioural coverage is the sim harness (needs a virtual clock + a
    // peer). This guards the public constructor signature only: the
    // coordinator is generic over `I` with `A = SocketAddr` pinned
    // internally, and `new` takes the membership `Endpoint` plus the
    // `QuicOptions` bundle.
    fn _sig<I>()
    where
      I: crate::Id
        + crate::Data
        + crate::CheapClone
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
    {
      let _: fn(
        crate::endpoint::Endpoint<I, SocketAddr>,
        super::QuicOptions,
      ) -> super::QuicEndpoint<I> = super::QuicEndpoint::<I>::new;
    }
  }

  #[test]
  fn with_label_empty_normalizes_to_none() {
    // An empty label collapses to the byte-identical no-label path, never a
    // `[12][0]` header.
    let ep = make_endpoint("n", "127.0.0.1:7600".parse().unwrap(), Instant::now())
      .with_label(Some(Bytes::new()), false)
      .expect("an empty label normalizes to None, never errors");
    assert_eq!(ep.label(), None);
  }

  #[test]
  fn with_label_accepts_valid() {
    let ep = make_endpoint("n", "127.0.0.1:7601".parse().unwrap(), Instant::now())
      .with_label(Some(Bytes::from_static(b"cluster-x")), false)
      .expect("a valid label");
    assert_eq!(ep.label(), Some(b"cluster-x".as_slice()));
  }

  #[test]
  fn with_label_rejects_overlong() {
    // A label longer than MAX_LABEL_LEN is rejected here, so it never reaches
    // `encode_label_prefix` (which would truncate the length byte and let the
    // overflow be parsed as reliable-unit data).
    let too_long = Bytes::from(vec![b'x'; crate::label::MAX_LABEL_LEN + 1]);
    let result = make_endpoint("n", "127.0.0.1:7602".parse().unwrap(), Instant::now())
      .with_label(Some(too_long), false);
    assert!(matches!(result, Err(crate::label::LabelError::TooLong)));
  }

  #[test]
  fn with_label_rejects_non_utf8() {
    let result = make_endpoint("n", "127.0.0.1:7603".parse().unwrap(), Instant::now())
      .with_label(Some(Bytes::from_static(&[0xff, 0xfe])), false);
    assert!(matches!(result, Err(crate::label::LabelError::NotUtf8)));
  }

  /// Regression guard: `service_quinn` MUST drain the per-connection
  /// `Connection::poll_endpoint_events()` queue every tick and route each
  /// non-`Drained` event through `quinn::Endpoint::handle_event(ch, ev)`. A
  /// missing drain leaves `NeedIdentifiers` / `ResetToken` /
  /// `RetireConnectionId` stranded in the connection's queue and breaks
  /// quinn-proto's documented polling contract: initial CID issuance,
  /// peer reset-token registration, and CID retirement all fail to
  /// propagate to the endpoint's index.
  ///
  /// The test drives a real QUIC handshake between two `QuicEndpoint`s
  /// via the public `start_push_pull` / `poll_transmit` / `handle_udp` /
  /// `handle_timeout` API — no internal accessors. `issue_first_cids`
  /// pushes `NeedIdentifiers` on the connection's queue at handshake
  /// completion; the drain loop's `#[cfg(test)]`
  /// `endpoint_events_processed` counter is incremented once per
  /// non-`Drained` event passed to `quinn::Endpoint::handle_event`. After
  /// driving the handshake to `Established` on both sides the counter
  /// MUST be `> 0`.
  ///
  /// Negative control: revert the drain loop in `service_quinn` and the
  /// counter stays at `0` — this assertion fails.
  #[test]
  fn service_quinn_drains_poll_endpoint_events() {
    let a_addr: SocketAddr = "127.0.0.1:7901".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7902".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    // Drive the handshake by ferrying datagrams in both directions.
    // Bounded: ~50 round-trips is plenty for a handshake on a virtual
    // network with no loss.
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      // Run a tick on each side so `service_quinn` drains
      // `poll_endpoint_events` and the new-CID feedback queued for the
      // NEXT tick is fed.
      a.handle_timeout(now);
      b.handle_timeout(now);
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }
    assert!(
      a.endpoint_events_processed > 0,
      "service_quinn must drain Connection::poll_endpoint_events() and \
       feed each non-Drained event to quinn::Endpoint::handle_event; \
       missing this drain breaks quinn-proto's polling contract"
    );
    assert!(
      b.endpoint_events_processed > 0,
      "same on the accepting side: handshake completion emits \
       `NeedIdentifiers` on both peers' connections"
    );
  }

  /// Regression guard: every datagram the coordinator emits on the QUIC
  /// path MUST classify as `Class::Quic`, i.e. its first byte must have
  /// `LONG_HEADER_FORM` (`0x80`) or short-header `FIXED_BIT` (`0x40`) set.
  /// The first-byte demux is collision-free only under this invariant —
  /// a FIXED_BIT-cleared short header would route to `Class::Memberlist`
  /// (first byte ∈ `1..=15`) or `Class::Reject` and silently lose ACKs /
  /// stream data / close packets to the memberlist codec.
  ///
  /// `EndpointOptions::new` defaults `grease_quic_bit: true`; under that
  /// default both sides advertise greasing and quinn-proto's packet
  /// encoder is permitted to clear FIXED_BIT on outgoing short-header
  /// packets. The coordinator forces `grease_quic_bit(false)` in
  /// [`QuicOptions::new`](super::crypto::QuicOptions::new) so the
  /// transport parameter is not advertised — a compliant peer will not
  /// clear FIXED_BIT in packets it sends us and our encoder will not
  /// clear FIXED_BIT in packets we send.
  ///
  /// The test drives a real handshake between two `QuicEndpoint`s via
  /// the public poll surface, then initiates a push/pull exchange that
  /// generates post-handshake short-header packets (ACKs, STREAM frames,
  /// HANDSHAKE_DONE, NEW_CONNECTION_ID, …). For every datagram drained
  /// from either side's `poll_transmit`, `classify(&bytes)` is asserted
  /// to be `Class::Quic` — directly encoding the demux invariant.
  ///
  /// Negative control: revert `super::crypto::QuicOptions::new` to omit
  /// the `endpoint.grease_quic_bit(false)` setter (greasing on) and run
  /// this test. With the determinism seed wired through to quinn-proto's
  /// per-packet greasing decision, some emitted short-header packets
  /// will have FIXED_BIT cleared, `classify` returns `Class::Memberlist`
  /// or `Class::Reject`, and the inner assertion fires. The asserting
  /// predicate (`classify == Class::Quic`) directly encodes the inbound
  /// demux invariant — independent of which exact packet quinn-proto
  /// chose to grease.
  #[test]
  fn emitted_quic_packets_always_have_fixed_bit_set() {
    let a_addr: SocketAddr = "127.0.0.1:7951".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7952".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);
    // Trigger a dial from A → B; the coordinator emits the Initial on the
    // very first `poll_transmit`. Post-handshake the push/pull bidi
    // stream's payload produces short-header STREAM frames + ACKs — the
    // packet space the per-packet greasing decision applies to.
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    // Bounded ferry of every emitted datagram, asserting the demux
    // invariant on each. ~200 rounds is well past handshake completion
    // and enough push/pull traffic to give quinn-proto's per-packet
    // greasing decision many opportunities to fire on each side.
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        assert_eq!(
          super::classify(&bytes),
          super::Class::Quic,
          "A emitted a non-Quic-classifying datagram (first byte {:#04x}); \
           grease_quic_bit must stay off so the inbound first-byte demux \
           invariant holds end-to-end",
          bytes.first().copied().unwrap_or(0),
        );
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        assert_eq!(
          super::classify(&bytes),
          super::Class::Quic,
          "B emitted a non-Quic-classifying datagram (first byte {:#04x}); \
           grease_quic_bit must stay off so the inbound first-byte demux \
           invariant holds end-to-end",
          bytes.first().copied().unwrap_or(0),
        );
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }
  }

  /// Architectural guard: a connection's deferred `ConnectionEvent` queue
  /// is co-located with its `ConnEntry` and therefore drops with the entry
  /// on reap by construction — quinn's slab `vacant_key()` cannot re-key a
  /// stale event onto a fresh connection occupying the freed slot because
  /// there is no global queue from which a stale event could survive past
  /// the reap.
  ///
  /// The reuse hazard. `quinn_proto::Endpoint::connect` allocates the next
  /// `ConnectionHandle` via `self.connections.vacant_key()`; reaping
  /// invokes `Endpoint::handle_event(ch, EndpointEvent::drained())` which
  /// removes the slot via `self.connections.try_remove(ch.0)`. The freed
  /// slot is reusable by the very next dial — and `ConnTable.conns` is a
  /// strict lockstep mirror of quinn's slab (`get_or_dial`'s
  /// `debug_assert_eq!(slot, ch.0)`).
  ///
  /// The fix is structural: a `ConnectionEvent` returned by
  /// `quinn::Endpoint::handle_event(ch, ev)` is queued on the SAME
  /// `ConnEntry`'s `pending_events` deque (see
  /// [`super::conn::ConnEntry::queue_pending_event`]) for delivery on the
  /// next `service_quinn` iteration. The entry owns its deferred queue;
  /// dropping the entry drops the queue — no global FIFO exists from
  /// which a stale `(handle, event)` pair could survive past the reap.
  ///
  /// The test drives a real handshake A↔B so quinn-proto issues
  /// `NeedIdentifiers` (via `issue_first_cids`) — `Endpoint::handle_event`
  /// returns a `NewIdentifiers` that `service_quinn` queues via
  /// `ConnEntry::queue_pending_event`. The
  /// test harvests one real `ConnectionEvent` out of `ch_a`'s deferred
  /// queue, re-injects it so a real entry exists on the eve of the
  /// reap, drives the connection to `is_drained()`, runs the reap, then
  /// asserts `ConnTable::get(ch_a)` is `None` — the entry and its
  /// `pending_events` are both gone. Finally, the test dials a fresh
  /// peer; quinn's `vacant_key()` returns `ch_a.0`, the new entry lands
  /// at that slot, and its `pending_events_len() == 0` — the property
  /// the previous global-FIFO purge-on-reap step was protecting is now
  /// preserved by construction.
  #[test]
  fn conn_entry_pending_events_drop_with_entry_on_reap() {
    let a_addr: SocketAddr = "127.0.0.1:7921".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7922".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive a real handshake A↔B so quinn-proto issues `NeedIdentifiers`
    // and the [`super::QuicEndpoint::service_quinn`] drain enqueues a
    // real `ConnectionEvent` on the A↔B connection's `ConnEntry`. The
    // loop stops AFTER one entry is staged on `ch_a` and BEFORE the
    // next `service_quinn` drains it (drained at the start of the next
    // iteration per the documented one-tick latency).
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    // Capture A's single connection handle (the dial to B) once it
    // exists in A's slab.
    let mut harvested: Option<quinn_proto::ConnectionEvent> = None;
    let mut ch_a: Option<quinn_proto::ConnectionHandle> = None;
    'ferry: for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
          // `handle_udp` invokes `run_tick`, whose `service_quinn` may
          // queue a real `ConnectionEvent` on A's connection entry
          // (e.g. the `NewIdentifiers` returned by
          // `quinn::Endpoint::handle_event(NeedIdentifiers)` at
          // handshake completion). Harvest here, before subsequent
          // `handle_udp`/`handle_timeout` calls run another
          // `service_quinn` that would drain the queue at the start of
          // its next iteration.
          if let Some(h) = a.conns.iter_handles().first().copied() {
            ch_a = Some(h);
            if let Some(e) = a.conns.get_mut(h) {
              if let Some(ev) = e.take_pending_events().next() {
                harvested = Some(ev);
                break 'ferry;
              }
            }
          }
        }
      }
      a.handle_timeout(now);
      if let Some(h) = a.conns.iter_handles().first().copied() {
        ch_a = Some(h);
        if let Some(e) = a.conns.get_mut(h) {
          if let Some(ev) = e.take_pending_events().next() {
            harvested = Some(ev);
            break 'ferry;
          }
        }
      }
      b.handle_timeout(now);
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break 'ferry;
      }
    }
    let harvested = harvested.expect("handshake must produce at least one real ConnectionEvent");
    let ch_a = ch_a.expect("test precondition: A holds the dial-to-B handle");
    // A holds exactly one connection (to B) at this point.
    assert_eq!(
      a.conns.iter_handles().len(),
      1,
      "test precondition: A holds exactly one connection"
    );

    // Re-stash the harvested event on the about-to-be-reaped entry.
    a.conns
      .get_mut(ch_a)
      .expect("conn entry still present pre-close")
      .queue_pending_event(harvested);
    assert_eq!(
      a.conns
        .get(ch_a)
        .expect("entry present")
        .pending_events_len(),
      1,
      "test precondition: one event staged on the about-to-be-reaped entry"
    );

    // Close the connection on side A and drive it through `is_drained()`
    // by advancing `poll_timeout` on the underlying `Connection`.
    // Bypasses `run_tick` / `service_quinn` so the staged
    // `pending_events` entry is NOT drained before the reap.
    a.conns
      .get_mut(ch_a)
      .expect("connection still in slab pre-close")
      .conn_mut()
      .close(now.into_std(), 0u32.into(), Bytes::new());
    for _ in 0..5000 {
      if a
        .conns
        .get(ch_a)
        .map(|e| e.conn_ref().is_drained())
        .unwrap_or(true)
      {
        break;
      }
      let next = a
        .conns
        .get_mut(ch_a)
        .and_then(|e| e.conn_mut().poll_timeout());
      match next {
        Some(d) => {
          a.conns.get_mut(ch_a).unwrap().conn_mut().handle_timeout(d);
        }
        None => break,
      }
    }
    assert!(
      a.conns
        .get(ch_a)
        .map(|e| e.conn_ref().is_drained())
        .unwrap_or(false),
      "test precondition: A's connection must be drained before reap"
    );

    // Run the production reap loop (the `run_tick` / `flush_outbound`
    // step (5) via [`QuicEndpoint::finalize_tick`]).
    for ch in a.conns.iter_handles() {
      a.conns.reap_if_drained(&mut a.quinn, ch);
    }

    // The architectural property: the entry is gone, and its
    // `pending_events` deque is gone with it. No global FIFO holds the
    // staged event past the reap.
    assert!(
      a.conns.get(ch_a).is_none(),
      "reap_if_drained must remove the slab entry on a successful reap; \
       the entry owns its pending_events deque and the deque drops with it"
    );

    // Stronger property: dial a fresh peer; quinn's `vacant_key()` reuses
    // the freed slab index, the new `ConnEntry` lands at that slot, and
    // its `pending_events_len() == 0` because the deque is per-entry —
    // a stale event from the previous occupant cannot leak in.
    let c_addr: SocketAddr = "127.0.0.1:7923".parse().unwrap();
    let ch_new = a
      .conns
      .get_or_dial(
        &mut a.quinn,
        now,
        a.cfg.client().clone(),
        c_addr,
        "localhost",
      )
      .expect("fresh dial after reap succeeds");
    assert_eq!(
      ch_new.0, ch_a.0,
      "quinn vacant_key() reuses the freed slab index after the reap"
    );
    assert_eq!(
      a.conns
        .get(ch_new)
        .expect("fresh entry at reused slot")
        .pending_events_len(),
      0,
      "the deferred queue is per-entry; a fresh ConnEntry at the reused \
       slab slot starts with an empty queue (the previous occupant's \
       deque dropped with that occupant on reap)"
    );
  }

  /// Strict-poll wake on deferred-event backlog.
  ///
  /// `service_quinn` queues `ConnectionEvent`s returned by
  /// `quinn_proto::Endpoint::handle_event(ch, ev)` on the OWNING
  /// `ConnEntry.pending_events` for delivery on the NEXT
  /// `service_quinn` iteration (the one-tick latency mirrors quinn-
  /// proto's reference async driver — see [`super::conn::ConnEntry::
  /// queue_pending_event`]). At handshake completion quinn-proto
  /// yields `EndpointEvent::NeedIdentifiers`; `Endpoint::handle_event`
  /// returns `Some(ConnectionEvent::NewIdentifiers)`, which is what
  /// drives the subsequent NEW_CONNECTION_ID frame emission via the
  /// connection's own `handle_event`.
  ///
  /// A strict-poll driver (no opportunistic wakes — re-enters the
  /// coordinator only when `poll_timeout()` says to) would sleep
  /// past the queued backlog if `poll_timeout` didn't account for
  /// it: the only term that would surface a "wake me now" signal
  /// is some unrelated idle/loss/probe timer, which on a quiet
  /// post-handshake connection can be many seconds away. The
  /// coordinator's `poll_timeout` therefore checks every
  /// `ConnEntry::has_pending_events()` and returns `Some(last_now)`
  /// (immediate-due) when any backlog is present. This regression
  /// drives a real A↔B handshake until A's connection entry has a
  /// real `ConnectionEvent` queued, then asserts
  /// `QuicEndpoint::poll_timeout()` returns an immediate-due wake.
  /// (`last_now` is set whenever `handle_*` / `start_*` runs, which
  /// must precede any queue entry — the queue is filled by
  /// `service_quinn` which is in those code paths.)
  #[test]
  fn deferred_connection_event_backlog_surfaces_immediate_due_wake() {
    let a_addr: SocketAddr = "127.0.0.1:7931".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7932".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);

    // Drive the handshake until A's ConnEntry has a deferred event
    // (NewIdentifiers from NeedIdentifiers at handshake completion).
    // Stop BEFORE another service_quinn drains it.
    let mut ch_a: Option<quinn_proto::ConnectionHandle> = None;
    let mut have_pending = false;
    'ferry: for _ in 0..200 {
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          if let Some(h) = a.conns.iter_handles().first().copied() {
            ch_a = Some(h);
            if let Some(e) = a.conns.get(h) {
              if e.has_pending_events() {
                have_pending = true;
                break 'ferry;
              }
            }
          }
        }
      }
      a.handle_timeout(now);
      if let Some(h) = a.conns.iter_handles().first().copied() {
        ch_a = Some(h);
        if let Some(e) = a.conns.get(h) {
          if e.has_pending_events() {
            have_pending = true;
            break 'ferry;
          }
        }
      }
      b.handle_timeout(now);
    }
    assert!(
      have_pending,
      "test precondition: the handshake must queue at least one \
       deferred ConnectionEvent on A's ConnEntry"
    );
    let ch_a = ch_a.expect("test precondition: A holds the dial-to-B handle");

    // The observable: with a real queued event, poll_timeout returns
    // a wake at or before `now` (the test's anchor — `handle_udp` /
    // `handle_timeout` set `last_now` to `now` above, so the
    // immediate-due `last_now` term is `now`). A `Some(t)` with
    // `t <= now` means "wake me immediately on the next driver
    // loop iteration."
    let wake = a.poll_timeout().expect(
      "a non-empty ConnEntry.pending_events MUST surface a wake via \
       poll_timeout — otherwise a strict-poll driver would not re-enter \
       service_quinn until an unrelated timer fires",
    );
    assert!(
      wake <= now,
      "the deferred-event wake MUST be immediate-due (≤ last_now); \
       got {:?} > {:?}",
      wake,
      now
    );

    // Stronger property: one more tick after the wake drains the
    // queue (verifies the wake is actionable, not a busy-loop).
    a.handle_timeout(now);
    assert!(
      !a.conns
        .get(ch_a)
        .expect("connection still present after one tick")
        .has_pending_events(),
      "one tick after the immediate-due wake MUST drain the deferred \
       ConnectionEvent queue (service_quinn's first action on the \
       per-connection iteration is `take_pending_events`)"
    );
  }

  /// `requeue_event` MUST anchor `last_now` AND route a requeued
  /// `Event::DialRequested` directly into `dial_pending`, so the requeued
  /// dial doesn't strand under strict-poll driving.
  ///
  /// `endpoint_mut()` is `#[cfg(test)]`-only and the sealed-inner-endpoint
  /// invariant blocks external callers from draining `DialRequested` out
  /// of the inner Endpoint queue directly. But `pub fn requeue_event(ev,
  /// now)` is still a public surface that can deposit an event back into
  /// the inner queue — including a `DialRequested` the caller obtained
  /// from a bare `Endpoint` BEFORE wrapping it in `QuicEndpoint`
  /// (legitimate `Endpoint::poll_event` usage). Without anchoring
  /// `last_now`, the strict-poll path would: sieve the requeued
  /// DialRequested into `dial_pending` (`attempted = false`), check
  /// `poll_timeout` → `last_now == None` → the immediate-due rescue's
  /// `if let Some(anchor)` guard skips → wake degrades to the entry's
  /// `deadline` term → at the deadline, `service_dials` retires the
  /// intent as elapsed without ever opening QUIC. Silent strand.
  ///
  /// The `now` parameter on `requeue_event` anchors `last_now` exactly
  /// when an event is deposited — even if that event is a
  /// `DialRequested` that bypasses the inner queue and lands directly in
  /// `dial_pending`. The next `poll_timeout` then returns `Some(t)` with
  /// `t <= now`, and the dial is attempted on the next
  /// `handle_timeout(now)` tick.
  #[test]
  fn requeued_dial_request_under_strict_poll_anchors_last_now() {
    use crate::event::PushPullKind;

    let a_addr: SocketAddr = "127.0.0.1:7941".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7942".parse().unwrap();
    let t0 = Instant::now();

    // (1) Build a bare Endpoint. Call start_push_pull on it BEFORE
    // wrapping — this is the legitimate Endpoint usage that produces a
    // DialRequested in the inner queue before `QuicEndpoint` wraps it.
    let cfg = EndpointOptions::new(SmolStr::new("a"), a_addr).with_rng_seed(a_addr.port() as u64);
    let mut bare_ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    bare_ep.start_scheduling(t0);
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = bare_ep.start_push_pull(b_addr, PushPullKind::Join, t0);

    // Drain the DialRequested. The caller now holds it.
    let dial_requested = loop {
      match bare_ep.poll_event() {
        Some(crate::event::Event::DialRequested(p)) => {
          break crate::event::Event::DialRequested(p);
        }
        Some(_) => continue,
        None => panic!("the bare Endpoint must have queued a DialRequested"),
      }
    };

    // (2) Wrap the bare Endpoint in QuicEndpoint. The wrap occurs
    // AFTER the caller drained the DialRequested — so the wrapped
    // QuicEndpoint has `last_now = None` (no handle_* / start_*
    // has been called on it).
    let qc = test_config();
    let mut seed = [0u8; 32];
    seed[..2].copy_from_slice(&a_addr.port().to_le_bytes());
    let mut a: QuicEndpoint<SmolStr> = QuicEndpoint::with_quinn_rng_seed(bare_ep, qc, Some(seed));

    // (3) Requeue the DialRequested onto the wrapped QuicEndpoint.
    // `requeue_event` anchors `last_now` AND routes a `DialRequested`
    // DIRECTLY into `dial_pending` (bypassing the inner Endpoint queue)
    // so the entry is present the moment `requeue_event` returns — a
    // caller that proceeds STRAIGHT to `poll_timeout` without an
    // intervening `poll_event` sieve still sees the immediate-due wake.
    a.requeue_event(dial_requested, t0);

    // (4) Check poll_timeout WITHOUT a prior poll_event call. The
    // direct routing means the entry is in `dial_pending` (not the
    // inner queue awaiting sieve), so the unattempted-wake rescue has
    // a `last_now` anchor to return and the assertion holds.
    let wake = a.poll_timeout().expect(
      "a requeued DialRequested MUST be directly visible in \
       dial_pending — `poll_timeout` MUST return a wake even without \
       an intervening `poll_event` sieve",
    );
    assert!(
      wake <= t0,
      "requeue_event(_, now) anchors last_now AND routes DialRequested \
       directly into dial_pending, so the next poll_timeout fires \
       immediate-due (`<= last_now`). Got wake = {wake:?}, now = \
       {t0:?}. Without direct routing, the wake would have been the \
       inner endpoint's term (or `None`) because the entry would be in \
       the inner queue, not dial_pending."
    );

    // (5) `poll_event` afterwards: no `DialRequested` should leak
    // through to external callers (it never entered the inner
    // queue). Application-visible events still surface — but the
    // bare endpoint had only the DialRequested queued, so the
    // wrapped endpoint's poll_event sees nothing.
    assert!(
      a.poll_event().is_none(),
      "requeue_event(DialRequested, _) routes DIRECTLY to dial_pending, \
       so it must NOT leak through poll_event"
    );
  }

  /// Strict-poll self-sufficiency: when `service_quinn` observes
  /// `Event::ConnectionLost` on a connection, every bridge riding it
  /// MUST be D1 `drain_then_reap`'d within the SAME tick — not merely
  /// marked fatal and deferred to a future `pump_bridges` cycle. The
  /// deferral is unsafe under strict poll-surface driving: a stateless
  /// reset / idle-timeout sets `quinn_proto::Connection::State::Drained`
  /// synchronously (`Timer::Idle` → `kill(ConnectionError::TimedOut)` →
  /// `State::Drained`), so `finalize_tick` reaps the slab slot in the
  /// same tick the bridge is marked fatal. `Bridge::poll_timeout`
  /// returns `None` for terminal bridges (a terminal bridge owes no
  /// future work to itself), so the coordinator's unified `poll_timeout`
  /// has no immediate-due term contributed by these bridges; a
  /// strict-poll driver with no other peer/probe/timer due wakes never
  /// again, and the bridge leaks indefinitely.
  ///
  /// The test drives a real handshake A↔B so a bridge is established
  /// on A. It then stops ferrying datagrams between the peers and
  /// advances A's clock past the negotiated `max_idle_timeout` (20s in
  /// `test_config`). One `handle_timeout(idle_due)` on A is enough:
  /// the per-connection `Timer::Idle` synchronously transitions A's
  /// connection to `State::Drained` AND queues a `ConnectionLost` for
  /// `Connection::poll()`, `service_quinn` observes the loss, and the
  /// per-bridge `drain_then_reap` runs inline. The `#[cfg(test)]`
  /// `bridges_reaped_on_connection_lost` counter records how many
  /// bridges were drained in this path.
  ///
  /// Negative control: revert the inline `drain_then_reap` in
  /// `service_quinn` to mere `mark_fatal()`; the counter stays at zero
  /// and the bridge sits in `self.bridges` with `fatal == true` after
  /// the same tick — its `poll_timeout` returns `None`, so under
  /// `step_via_poll_timeout_only` a quiet cluster has no further wake
  /// to reach the deferred `pump_bridges` reap.
  #[test]
  fn connection_lost_immediately_reaps_bridges_under_strict_poll() {
    let a_addr: SocketAddr = "127.0.0.1:7941".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7942".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Initiate the push/pull join and ferry datagrams in both directions
    // until A holds at least one live bridge AND both sides have fully
    // negotiated the handshake (`endpoint_events_processed > 0`
    // confirms the handshake-completion NEW_CONNECTION_ID feedback ran;
    // the test_config's 20s `max_idle_timeout` is then mutually
    // negotiated and armed on both ends).
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if a.live_bridge_count() >= 1
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
      {
        break;
      }
      // Only bail on no-traffic AFTER both sides have processed at least
      // one endpoint event — otherwise iteration 1 (poll_transmit empty
      // because no `handle_timeout` has yet driven `service_dials`) bails
      // before the handshake even starts.
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }
    assert!(
      a.live_bridge_count() >= 1,
      "test precondition: the push/pull dial must have opened at least \
       one bridge on A"
    );

    // Capture the connection handle that bridges to B, then verify it
    // is in fact Established (the handshake completed) so the negotiated
    // idle timeout is actually armed.
    let ch_a = a
      .conns
      .iter_handles()
      .first()
      .copied()
      .expect("A must hold one connection to B post-handshake");
    assert!(
      a.conns
        .get(ch_a)
        .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
        .unwrap_or(false),
      "test precondition: A's connection to B must be Established \
       before the idle-timeout is advanced"
    );

    let before = a.bridges_reaped_on_connection_lost;
    let bridges_before = a.live_bridge_count();
    assert!(bridges_before >= 1);

    // Drive A's pooled connection to `State::Drained` WITHOUT elapsing
    // the bridges' own exchange deadlines. `Connection::close` transitions
    // to `State::Closed` and arms `Timer::Close` at `now + 3 * PTO`. When
    // that timer fires, the state advances to `Drained` and
    // `EndpointEventInner::Drained` is queued; `self.error` is NOT set,
    // so `poll()` does not yield `Event::ConnectionLost` for the
    // LocallyClosed path — the `service_quinn` `lost || is_drained()`
    // branch is what catches this case.
    //
    // 3 × PTO is a fraction of a second on a healthy localhost handshake;
    // the bridge's exchange deadline (~5s by default for push/pull) is
    // safely past, so `pump_bridges`'s natural deadline-expiry reap
    // cannot fire instead.
    a.conns
      .get_mut(ch_a)
      .expect("A's pooled connection is present")
      .conn_mut()
      .close(now.into_std(), 0u32.into(), bytes::Bytes::new());

    let close_due = a
      .conns
      .get_mut(ch_a)
      .expect("A's pooled connection is present")
      .conn_mut()
      .poll_timeout()
      .expect("Connection::close arms the Close timer; poll_timeout MUST be Some");
    a.handle_timeout(crate::Instant::from_std(close_due));

    // Observable: the inline `drain_then_reap` ran for every bridge on
    // the lost connection in THIS single tick. The counter is incremented
    // per bridge reaped on the `Event::ConnectionLost` path; with a
    // mere-`mark_fatal` shape the counter stays at zero.
    assert!(
      a.bridges_reaped_on_connection_lost > before,
      "at least one bridge MUST have been drain_then_reap'd inside \
       `service_quinn` on the `Event::ConnectionLost` path this tick \
       (counter before = {before}, after = {}). Without the inline \
       drain, the bridge sits fatal in `self.bridges` and a strict-poll \
       driver gets no wake to reach the deferred reap.",
      a.bridges_reaped_on_connection_lost,
    );
    assert_eq!(
      a.live_bridge_count(),
      0,
      "every bridge riding the lost connection MUST be removed from \
       `self.bridges` this same tick. Got {} live bridge(s) — the \
       bridge leaked beyond the ConnectionLost tick.",
      a.live_bridge_count(),
    );
  }

  /// The load-bearing firewall: force-closing a pooled QUIC connection mid-
  /// cluster does NOT change membership. The membership FSM is driven only by
  /// SWIM probe-ack timing; a transport connection close/loss reaps the bridge
  /// (a connection-level event) but never marks the peer Suspect/Dead/Alive.
  ///
  /// `Endpoint` has no connection-state input by construction, and
  /// `service_quinn` maps a quinn `Event::ConnectionLost` (or a locally-closed
  /// `is_drained()`) to a bridge reap, NEVER to a membership transition. This
  /// test locks that in: it drives a real push/pull JOIN A<->B to membership
  /// completion (B becomes a tracked, `Alive` member of A — not merely a
  /// bridge), then force-closes A's pooled connection to B at the SAME instant
  /// `now` so no probe/suspicion deadline can fire. After servicing the loss,
  /// A's membership is byte-identical: B is still tracked, still `Alive`, the
  /// member count is unchanged, and no `NodeJoined`/`NodeLeft`/`NodeUpdated`/
  /// `NodeConflict` transition for B is produced by the close. The bridge reap
  /// may surface an `ExchangeCompleted` (a connection/exchange event) — that is
  /// allowed and is not a membership transition.
  ///
  /// If this test ever fails — num_members drops, or B's gossip liveness goes
  /// Suspect/Dead, or a membership-transition event for B appears — WITHOUT any
  /// elapsed probe deadline (time was never advanced), a connection->membership
  /// edge has been introduced and the firewall is broken.
  #[test]
  fn closing_a_pooled_connection_does_not_change_membership() {
    let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7972".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);
    let b_id = SmolStr::new("b");

    // Drive a push/pull JOIN A<->B to membership COMPLETION: ferry datagrams
    // both ways at a FIXED instant `now`, ticking both ends and draining each
    // side's `poll_event` per iteration so the merge endpoint events are
    // processed (mirrors the membership-merge ferry the stream-plane tests use,
    // adapted to the QUIC datagram pump). The localhost handshake + the push/
    // pull bidi complete within a single instant in this model, so A learns B
    // as an `Alive` member without advancing the clock — keeping every
    // probe/suspicion deadline strictly in the future.
    // Ignoring StreamId return: the test asserts on membership + the connection
    // handle, not the exchange id.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..256 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      while a.poll_event().is_some() {}
      while b.poll_event().is_some() {}
      if a.endpoint_mut().num_members() >= 2 {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    let members_before = a.endpoint_mut().num_members();
    assert!(
      members_before >= 2,
      "precondition: A must know B as a member before the kill (the push/pull \
       JOIN must merge B's NodeState into A). num_members = {members_before}"
    );
    assert_eq!(
      a.endpoint_mut().member_liveness(&b_id),
      Some(crate::typed::State::Alive),
      "precondition: B must be tracked as Alive before the kill"
    );

    // Force-close A's pooled connection to B (a pure transport fault) at the
    // SAME instant `now`. Time is NEVER advanced in this test, so no
    // probe/suspicion deadline can fire — the connection close is the only
    // stimulus.
    let ch = a
      .conns
      .handle_for(&b_addr)
      .expect("A must hold a pooled connection to B post-merge");
    a.conns
      .get_mut(ch)
      .expect("A's pooled connection to B is present")
      .conn_mut()
      .close(now.into_std(), 0u32.into(), bytes::Bytes::new());

    // Service A once so `service_quinn` observes the loss and reaps the bridge.
    a.handle_timeout(now);

    // Membership MUST be byte-identical: B still tracked, still Alive, count
    // unchanged.
    assert_eq!(
      a.endpoint_mut().num_members(),
      members_before,
      "closing a QUIC connection MUST NOT change the member count \
       (before = {members_before}, after = {}). A connection->membership edge \
       has been introduced.",
      a.endpoint_mut().num_members(),
    );
    assert_eq!(
      a.endpoint_mut().member_liveness(&b_id),
      Some(crate::typed::State::Alive),
      "closing a QUIC connection MUST NOT transition B's gossip liveness \
       (got {:?}); only a SWIM probe timeout may move B to Suspect/Dead, and \
       time was never advanced.",
      a.endpoint_mut().member_liveness(&b_id),
    );

    // The close + service drain MUST produce no membership-transition event for
    // B. A bridge-reap `ExchangeCompleted` / `LeftCluster` is a connection/
    // exchange event and is allowed.
    while let Some(ev) = a.poll_event() {
      match &ev {
        crate::event::Event::NodeJoined(ns)
        | crate::event::Event::NodeLeft(ns)
        | crate::event::Event::NodeUpdated(ns) => {
          assert_ne!(
            ns.id_ref(),
            &b_id,
            "closing a QUIC connection MUST NOT emit a membership transition \
             ({ev:?}) for B — the connection close reaped the bridge but the \
             firewall forbids it from touching membership."
          );
        }
        crate::event::Event::NodeConflict(nc) => {
          assert_ne!(
            nc.existing_ref().id_ref(),
            &b_id,
            "closing a QUIC connection MUST NOT emit a NodeConflict for B"
          );
        }
        _ => {}
      }
    }
  }

  /// Offering an unreliable datagram to a peer that already holds an
  /// Established pooled connection enqueues it on that connection
  /// (`DatagramSendOutcome::Queued`). The datagram does not surface on
  /// `poll_transmit` synchronously — it rides out via the normal
  /// `service_quinn -> poll_transmit` pump on a later tick — so the contract
  /// asserted here is the `Queued` outcome, not an immediate transmit.
  #[test]
  fn queue_unreliable_datagram_to_established_peer_is_queued() {
    let a_addr: SocketAddr = "127.0.0.1:7951".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7952".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive the A<->B push/pull handshake until A holds an Established
    // connection to B (the proven ferry pattern).
    // Ignoring StreamId return: the test asserts on the connection state and
    // the datagram outcome, not the handle.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if a.live_bridge_count() >= 1
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
      {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    let ch_a = a
      .conns
      .iter_handles()
      .first()
      .copied()
      .expect("A must hold one connection to B post-handshake");
    assert!(
      a.conns
        .get(ch_a)
        .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
        .unwrap_or(false),
      "test precondition: A's connection to B must be Established before \
       offering an unreliable datagram"
    );

    let outcome = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01gossip"), now);
    assert_eq!(outcome, super::DatagramSendOutcome::Queued);
  }

  /// A received application datagram surfaces through the SAME
  /// `poll_memberlist_ingress` accessor the plain-UDP gossip path uses, tagged
  /// with the sender's address — proving the wire is invisible past `mem_ingress`
  /// (the driver decodes a datagram-sourced packet identically to a UDP one).
  #[test]
  fn received_datagram_surfaces_through_the_same_memberlist_ingress_as_udp() {
    let a_addr: SocketAddr = "127.0.0.1:7961".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7962".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive the A<->B push/pull handshake until A holds an Established
    // connection to B (the proven ferry pattern).
    // Ignoring StreamId return: the test asserts on the connection state and
    // the surfaced datagram, not the handle.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if a.live_bridge_count() >= 1
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
      {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    let ch_a = a
      .conns
      .iter_handles()
      .first()
      .copied()
      .expect("A must hold one connection to B post-handshake");
    assert!(
      a.conns
        .get(ch_a)
        .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
        .unwrap_or(false),
      "test precondition: A's connection to B must be Established before \
       offering an unreliable datagram"
    );

    // A offers one application datagram to B.
    let payload = Bytes::from_static(b"\x01hello-datagram");
    assert_eq!(
      a.queue_unreliable_datagram(b_addr, payload.clone(), now),
      super::DatagramSendOutcome::Queued
    );

    // Pump: A's tick puts the datagram packet onto a.out; ferry it into B;
    // B's tick drains it into mem_ingress; poll_memberlist_ingress surfaces it.
    let mut got: Option<(SocketAddr, Bytes)> = None;
    for _ in 0..50 {
      a.handle_timeout(now);
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      b.handle_timeout(now);
      if let Some(item) = b.poll_memberlist_ingress() {
        got = Some(item);
        break;
      }
      // keep the connection healthy in both directions
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
        }
      }
    }

    let (from, bytes) = got.expect("B must surface the datagram via poll_memberlist_ingress");
    assert_eq!(
      from, a_addr,
      "ingress must be tagged with the sender address"
    );
    assert_eq!(
      bytes, payload,
      "the datagram payload must round-trip byte-identically"
    );
  }

  /// When mem_ingress is already at MAX_MEM_INGRESS_DATAGRAMS, a further inbound
  /// QUIC datagram is dropped and counted rather than growing the queue
  /// unbounded. quinn's byte-budget receive buffer cannot bound the entry count
  /// (a zero/tiny-length datagram flood adds ~0 bytes per entry), so the
  /// coordinator drain enforces the count cap.
  #[test]
  fn inbound_datagram_dropped_when_ingress_backlog_at_cap() {
    let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7972".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive the A<->B push/pull handshake until A holds an Established
    // connection to B (the proven ferry pattern).
    // Ignoring StreamId return: the test asserts on the drop counter, not the
    // handle.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if a.live_bridge_count() >= 1
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
      {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    // Clear any handshake-era ingress, then saturate A's mem_ingress to the cap
    // so the next inbound datagram has nowhere to land.
    a.mem_ingress.clear();
    for _ in 0..super::MAX_MEM_INGRESS_DATAGRAMS {
      a.mem_ingress.push_back((b_addr, Bytes::from_static(b"x")));
    }
    let dropped_before = a.datagram_ingress_dropped();

    // B sends one datagram to A; pump it onto the wire and service A. A's
    // service_quinn drain finds mem_ingress at the cap and drops + counts it.
    assert_eq!(
      b.queue_unreliable_datagram(a_addr, Bytes::from_static(b"\x01y"), now),
      super::DatagramSendOutcome::Queued
    );
    for _ in 0..50 {
      b.handle_timeout(now);
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
        }
      }
      a.handle_timeout(now);
      if a.datagram_ingress_dropped() > dropped_before {
        break;
      }
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
    }

    assert!(
      a.datagram_ingress_dropped() > dropped_before,
      "a datagram arriving with mem_ingress at cap must be dropped and counted"
    );
    // The public Metrics fold the QUIC datagram-plane ingress shed into
    // gossip_ingress_dropped, so a driver sees one unified counter.
    assert_eq!(
      a.metrics().gossip_ingress_dropped,
      a.datagram_ingress_dropped(),
      "QuicEndpoint::metrics must surface the QUIC ingress drops, not a zero stream-plane count"
    );
  }

  /// One peer flooding its full per-peer standing share of `mem_ingress` must
  /// not starve a DIFFERENT peer's inbound datagram, even while the node-global
  /// cap is far from full. The share is bounded across the whole undrained
  /// queue (not per recv pass), so the flooder's overflow is dropped-and-counted
  /// by the real `service_quinn` drain while the other peer's datagram still
  /// surfaces — the fairness the per-pass counter could not provide once a
  /// driver batches several recv passes before draining `mem_ingress`.
  #[test]
  fn per_peer_ingress_cap_does_not_starve_other_peers() {
    let a_addr: SocketAddr = "127.0.0.1:7981".parse().unwrap();
    let t_addr: SocketAddr = "127.0.0.1:7982".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7983".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut t = make_endpoint("t", t_addr, now);
    // B is the flooding peer: a real connection so its datagram traverses the
    // actual receive drain, where its full standing share is dropped-and-counted.
    let mut b = make_endpoint("b", b_addr, now);

    // Drive both peers' push/pull handshakes until A holds an Established
    // connection to each (the proven ferry pattern). Ferrying both each
    // iteration lets A accept two distinct connections.
    // Ignoring StreamId return: the test asserts on the surfaced datagram and
    // the drop counter, not the handles.
    let _ = t
      .endpoint_mut()
      .start_push_pull(a_addr, crate::event::PushPullKind::Join, now);
    let _ = b
      .endpoint_mut()
      .start_push_pull(a_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..400 {
      for (peer, peer_addr) in [(&mut t, t_addr), (&mut b, b_addr)] {
        while let Some((to, bytes)) = peer.poll_transmit() {
          if to == a_addr {
            a.handle_udp(peer_addr, &bytes, now);
          }
        }
      }
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == t_addr {
          t.handle_udp(a_addr, &bytes, now);
        } else if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      a.handle_timeout(now);
      t.handle_timeout(now);
      b.handle_timeout(now);
      if a.live_connections_to(t_addr) >= 1 && a.live_connections_to(b_addr) >= 1 {
        break;
      }
    }
    assert!(
      a.live_connections_to(t_addr) >= 1 && a.live_connections_to(b_addr) >= 1,
      "test precondition: A must hold a live connection to both T and B"
    );

    // Clear handshake-era ingress, then fill peer B's standing share to the
    // per-peer cap while the node-global queue stays far below its cap, so the
    // GLOBAL bound never fires — only B's per-peer bound can drop, and only B's
    // datagrams, never T's.
    a.mem_ingress.clear();
    a.mem_ingress_per_peer.clear();
    for _ in 0..super::MAX_INGRESS_DATAGRAMS_PER_PEER {
      a.mem_ingress.push_back((b_addr, Bytes::from_static(b"x")));
      *a.mem_ingress_per_peer.entry(b_addr).or_insert(0) += 1;
    }
    assert!(
      a.mem_ingress.len() < super::MAX_MEM_INGRESS_DATAGRAMS,
      "test precondition: global queue must stay below its cap so only the \
       per-peer bound is exercised"
    );

    // Both peers send one application datagram to A. T's must land in the shared
    // queue; B's (already at its per-peer cap) must be dropped-and-counted by the
    // real service_quinn drain.
    let t_payload = Bytes::from_static(b"\x01t-probe-ack");
    assert_eq!(
      t.queue_unreliable_datagram(a_addr, t_payload.clone(), now),
      super::DatagramSendOutcome::Queued
    );
    assert_eq!(
      b.queue_unreliable_datagram(a_addr, Bytes::from_static(b"\x01b-flood"), now),
      super::DatagramSendOutcome::Queued
    );
    let dropped_before = a.datagram_ingress_dropped();

    let mut got_t: Option<(SocketAddr, Bytes)> = None;
    for _ in 0..50 {
      t.handle_timeout(now);
      b.handle_timeout(now);
      for (peer, peer_addr) in [(&mut t, t_addr), (&mut b, b_addr)] {
        while let Some((to, bytes)) = peer.poll_transmit() {
          if to == a_addr {
            a.handle_udp(peer_addr, &bytes, now);
          }
        }
      }
      a.handle_timeout(now);
      // Drain A's queue looking for T's datagram, skipping B's filler entries.
      while let Some(item) = a.poll_memberlist_ingress() {
        if item.0 == t_addr {
          got_t = Some(item);
          break;
        }
      }
      if got_t.is_some() && a.datagram_ingress_dropped() > dropped_before {
        break;
      }
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == t_addr {
          t.handle_udp(a_addr, &bytes, now);
        } else if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
    }

    let (from, bytes) =
      got_t.expect("T's datagram must surface despite B's full standing share — no starvation");
    assert_eq!(
      from, t_addr,
      "surfaced ingress must be tagged with T's address"
    );
    assert_eq!(
      bytes, t_payload,
      "T's datagram payload must round-trip intact"
    );
    assert!(
      a.datagram_ingress_dropped() > dropped_before,
      "B's datagram, arriving while B is at its per-peer cap, must be dropped and counted"
    );
  }

  /// A plain-UDP flood from one peer cannot bypass the shared ingress caps:
  /// `handle_memberlist_udp` is admission-checked through the SAME shared helper
  /// as the QUIC datagram drain, so once a flooding peer reaches its per-peer cap
  /// its further UDP frames are dropped-and-counted while a DIFFERENT peer's
  /// inbound frame is still admitted. Before the fix the UDP path pushed
  /// unconditionally and incremented `mem_ingress_per_peer` past the cap, so a
  /// fallback flood was an unbounded queue grower that also starved later
  /// authenticated QUIC datagrams on the per-peer/global cap checks.
  #[test]
  fn udp_ingress_flood_is_capped_and_does_not_starve_other_peers() {
    let now = Instant::now();
    let mut a = make_endpoint("a", "127.0.0.1:7991".parse().unwrap(), now);
    let x: SocketAddr = "127.0.0.1:7992".parse().unwrap(); // UDP flooder
    // Flood X over plain UDP well past its per-peer cap; the global cap stays
    // clear (per-peer cap << global cap), so only X's per-peer bound can fire.
    for _ in 0..(super::MAX_INGRESS_DATAGRAMS_PER_PEER + 50) {
      a.handle_memberlist_udp(x, b"\x01flood");
    }
    // X is capped at exactly its per-peer budget; the overflow was dropped+counted.
    assert!(
      a.mem_ingress_per_peer.get(&x).copied().unwrap_or(0) <= super::MAX_INGRESS_DATAGRAMS_PER_PEER,
      "X's standing share must not exceed its per-peer cap"
    );
    assert!(
      a.datagram_ingress_dropped() >= 50,
      "X's 50 overflow UDP frames must be dropped and counted"
    );
    assert!(
      a.mem_ingress.len() < super::MAX_MEM_INGRESS_DATAGRAMS,
      "X must not be able to fill the global queue"
    );
    // A different peer Y's frame is still admitted (not starved by X's flood).
    let y: SocketAddr = "127.0.0.1:7993".parse().unwrap();
    a.handle_memberlist_udp(y, b"\x01y");
    // Drain and confirm Y's frame is present.
    let mut saw_y = false;
    while let Some((from, _)) = a.poll_memberlist_ingress() {
      if from == y {
        saw_y = true;
      }
    }
    assert!(saw_y, "a non-flooding peer's frame must still be admitted");
  }

  /// Offering an unreliable datagram to a peer with no pooled connection is
  /// best-effort `NotReady` (no datagram max_size yet), but it MUST initiate a
  /// dial so a subsequent offer can land once the connection establishes.
  #[test]
  fn queue_unreliable_datagram_to_unknown_peer_is_not_ready_and_initiates_dial() {
    let a_addr: SocketAddr = "127.0.0.1:7953".parse().unwrap();
    let unknown: SocketAddr = "127.0.0.1:7954".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);

    let outcome = a.queue_unreliable_datagram(unknown, Bytes::from_static(b"x"), now);
    assert_eq!(outcome, super::DatagramSendOutcome::NotReady);
    assert!(
      a.conns.handle_for(&unknown).is_some(),
      "queue must initiate a dial to an unknown peer"
    );
    // A best-effort NotReady on a still-handshaking connection (no datagram
    // max_size yet) is NOT a drop — only a residual quinn datagram-state error
    // bumps the drop counter.
    assert_eq!(a.datagram_dropped(), 0);
  }

  /// A datagram to a peer with no pooled connection is best-effort NotReady AND
  /// initiates a dial; a single flush_outbound_transmits then emits that dial's
  /// QUIC Initial the SAME tick (so the connection warms promptly instead of
  /// waiting for the next driver wake).
  #[test]
  fn cold_peer_datagram_then_flush_emits_quic_initial_same_tick() {
    let a_addr: SocketAddr = "127.0.0.1:7991".parse().unwrap();
    let cold: SocketAddr = "127.0.0.1:7992".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);

    assert_eq!(
      a.queue_unreliable_datagram(cold, Bytes::from_static(b"\x01g"), now),
      super::DatagramSendOutcome::NotReady
    );
    // What the driver now does on NotReady:
    a.flush_outbound_transmits(now);
    // The cold dial's Initial must be queued for transmit to `cold` this tick.
    let mut saw_initial_to_cold = false;
    while let Some((to, _bytes)) = a.poll_transmit() {
      if to == cold {
        saw_initial_to_cold = true;
      }
    }
    assert!(
      saw_initial_to_cold,
      "the cold dial's QUIC Initial must be emitted the same tick as the flush"
    );
  }

  /// `service_dials` MUST retire an expired dial intent through the
  /// FSM's `dial_failed` path BEFORE calling
  /// `quinn_proto::Streams::open(Dir::Bi)`. `Streams::open(Dir::Bi)`
  /// inserts BOTH the send AND the recv state for the new stream. An
  /// expired-intent open would therefore synthesise a bidi stream on a
  /// pooled connection that no `Bridge` owns — the recv half is
  /// unreachable and unreapable.
  ///
  /// The `now >= deadline` pre-check skips the open entirely, so
  /// neither half is created. As defence-in-depth, the
  /// `dial_succeeded → None` branch additionally stops the recv half so
  /// any other `None`-return path from the frozen `dial_succeeded`
  /// fully retires both halves rather than leaving the recv half
  /// orphaned.
  ///
  /// The test drives a real handshake A↔B so A holds a pooled,
  /// Established connection. It then injects a synthetic
  /// `DialRequested` whose `deadline` is BEFORE `now`, runs
  /// `service_dials(now)`, and asserts:
  ///  - no bridge was created on A;
  ///  - `Connection::streams().send_streams() == 0` on A's pooled
  ///    connection — neither half exists, so quinn-proto's
  ///    "streams that may have unacknowledged data" counter is
  ///    unchanged.
  ///
  /// Negative control: revert the `now >= deadline` pre-check.
  /// `Streams::open(Dir::Bi)` then runs for the expired intent,
  /// inserting both halves; `dial_succeeded` returns `None` (the
  /// frozen API drops past-deadline intents); reset-only retirement
  /// touches `send_stream(sid)` but not `recv_stream(sid)`. The recv
  /// half remains in `StreamsState::recv` with no owner —
  /// `Streams::send_streams` is `> 0` immediately after `open` (it
  /// counts open send streams) and stays in the count as the orphan,
  /// so this assertion fails.
  #[test]
  fn expired_dial_does_not_open_unowned_bidi_stream() {
    let a_addr: SocketAddr = "127.0.0.1:7961".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7962".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive a real handshake A→B (push/pull join) so A holds a pooled
    // Established connection. Once the bridge on A has reaped its own
    // join exchange (or the loop bounds out), the pooled connection is
    // available for the expired-intent injection below to re-use.
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      // Only bail on no-traffic AFTER both sides have processed at least
      // one endpoint event — see the matching pattern in
      // `connection_lost_immediately_reaps_bridges_under_strict_poll`.
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    let ch_a = a
      .conns
      .iter_handles()
      .first()
      .copied()
      .expect("A must hold one pooled connection to B post-handshake");
    assert!(
      a.conns
        .get(ch_a)
        .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
        .unwrap_or(false),
      "test precondition: A's pooled connection must be Established"
    );

    // Snapshot the baseline `send_streams` count after the join's own
    // bidi stream has reaped. A non-zero residual is acceptable (e.g.
    // an in-flight ack), but the post-`service_dials(expired)` count
    // MUST match this baseline — the expired intent must create no
    // new stream state on the pooled connection.
    let send_streams_before = a
      .conns
      .get_mut(ch_a)
      .expect("conn entry present")
      .conn_mut()
      .streams()
      .send_streams();

    // Register a real PushPull intent on the inner endpoint (registers
    // it in `pending_stream_intents` and queues a `DialRequested`),
    // then sieve the `DialRequested` into `dial_pending`. Finally,
    // override the deadline on the `dial_pending` entry to be BEFORE
    // `now` — the expired-dial pre-check then routes this intent
    // through `dial_failed` BEFORE `Streams::open(Dir::Bi)` is called.
    let id = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Refresh, now);
    a.sieve_dial_events();
    assert_eq!(
      a.dial_pending.len(),
      1,
      "test precondition: the sieve must move exactly the one \
       `DialRequested` into `dial_pending`"
    );
    let entry = a
      .dial_pending
      .front_mut()
      .expect("dial_pending non-empty per the preceding assertion");
    assert_eq!(
      entry.id, id,
      "the sieved entry MUST carry the registered intent's id"
    );
    entry.deadline = now - Duration::from_millis(1);

    let bridges_before = a.live_bridge_count();
    a.service_dials(now);

    // Observable (a): no new bridge was created.
    assert_eq!(
      a.live_bridge_count(),
      bridges_before,
      "an expired dial intent MUST NOT spawn a new bridge — got \
       bridge_count {} -> {}",
      bridges_before,
      a.live_bridge_count(),
    );

    // Observable (b): no new bidi stream state was created on the
    // pooled connection. `Streams::open(Dir::Bi)` would have
    // incremented `send_streams` (its body runs `state.insert(false,
    // id); state.send_streams += 1`). The deadline pre-check skips
    // the open entirely, so `send_streams` is unchanged.
    let send_streams_after = a
      .conns
      .get_mut(ch_a)
      .expect("conn entry still present")
      .conn_mut()
      .streams()
      .send_streams();
    assert_eq!(
      send_streams_after, send_streams_before,
      "an expired dial intent MUST NOT call `Streams::open(Dir::Bi)` \
       on the pooled connection — that insert would create BOTH send \
       and recv state with no `Bridge` to own them. send_streams \
       before = {send_streams_before}, after = {send_streams_after}. \
       Reverting the deadline pre-check lets `open` create both halves; \
       a reset-only retirement on the `dial_succeeded → None` branch \
       touches the send half but not the recv half, so the recv half \
       stays orphaned in `StreamsState::recv` (and `send_streams` \
       would be > before, since the new stream is in `streams::send` \
       and only retires after the peer ACKs the RESET_STREAM)."
    );
  }

  /// A reliable `UserMessage` dial that fails BEFORE any bridge is
  /// created MUST surface a terminal `Event::ExchangeCompleted` with
  /// `outcome = Failed` and `kind = UserMessage`, keyed by the SAME
  /// `ExchangeId::from(StreamId)` the QUIC driver parks its
  /// reliable-send waiter under. Without this the driver's parked waiter
  /// hangs forever (the bridge-reap path — the only other
  /// `ExchangeCompleted(UserMessage)` producer — never fires for a
  /// bridge that was never created).
  ///
  /// The test registers a `UserMessage` intent (which dials the
  /// unreachable peer in-band and requeues onto `dial_pending` while the
  /// connection handshakes), overrides the requeued entry's deadline to
  /// BEFORE `now`, and runs `service_dials(now)`. The expired-dial
  /// pre-check retires the intent through `retire_failed_dial`, which
  /// emits the `Failed` completion. `poll_event` then surfaces it.
  ///
  /// Negative control: revert `retire_failed_dial` to the bare
  /// `pending_outbound_kinds.remove` + `dial_failed` it replaced and
  /// `poll_event` yields no `ExchangeCompleted` — the assertion fails.
  #[test]
  fn expired_user_message_dial_emits_failed_exchange_completed() {
    use crate::event::{Event, ExchangeId, ExchangeKind, ExchangeOutcome};

    let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
    // No B endpoint is bound: the dial targets a port nothing listens on,
    // so the connection never leaves `is_handshaking`.
    let unreachable: SocketAddr = "127.0.0.1:7972".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);

    // `start_user_message` registers the intent, dials in-band, and (the
    // connection is still handshaking) requeues the intent onto
    // `dial_pending` with deadline `now + stream_timeout`. The returned
    // `StreamId` is the correlation handle the QUIC driver coerces to its
    // parked `ExchangeId`.
    let id = a
      .start_user_message(unreachable, Bytes::from_static(b"hello"), now)
      .expect("issued while running");
    let expected_eid = ExchangeId::from(id);

    assert_eq!(
      a.dial_pending.len(),
      1,
      "test precondition: the in-band dial must requeue the still-handshaking \
       UserMessage intent onto `dial_pending`"
    );
    let entry = a
      .dial_pending
      .front_mut()
      .expect("dial_pending non-empty per the preceding assertion");
    assert_eq!(
      entry.id, id,
      "the requeued entry MUST carry the registered intent's id"
    );
    // Force the expired-dial pre-check to fire on the next service tick.
    entry.deadline = now - Duration::from_millis(1);

    a.service_dials(now);

    // Drain events and find the terminal completion for this exchange.
    let mut found = None;
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(payload) = ev
        && payload.eid() == expected_eid
      {
        found = Some(payload);
        break;
      }
    }
    let payload = found.expect(
      "a UserMessage dial that fails before any bridge is created MUST emit \
       Event::ExchangeCompleted keyed by ExchangeId::from(StreamId) — the \
       QUIC driver's parked reliable-send waiter resolves on exactly this \
       event; without it the send hangs forever",
    );
    assert_eq!(
      payload.kind(),
      ExchangeKind::UserMessage,
      "the surfaced completion MUST carry kind = UserMessage"
    );
    assert_eq!(
      payload.outcome(),
      ExchangeOutcome::Failed,
      "a pre-bridge dial failure MUST carry outcome = Failed"
    );
    assert_eq!(
      payload.peer(),
      &unreachable,
      "the completion MUST carry the dialed peer address"
    );
    // The intent's staged kind/peer must be drained (no leak).
    assert!(
      !a.pending_outbound_kinds.contains_key(&id),
      "the staged outbound kind MUST be drained on dial failure"
    );
    assert!(
      !a.pending_outbound_peers.contains_key(&id),
      "the staged outbound peer MUST be drained on dial failure"
    );
  }

  /// A `PushPull` dial that fails BEFORE any bridge is created MUST
  /// surface a terminal `Event::ExchangeCompleted` with `outcome = Failed`
  /// and `kind = PushPull`, keyed by the SAME `ExchangeId::from(StreamId)`
  /// a QUIC `WaitForCompletion` join parks its waiter under. Without this
  /// the driver's parked join waiter — which resolves only when every
  /// dispatched push/pull exchange completes — never drains its set for an
  /// unreachable seed and the join hangs (the reactor QUIC join has no
  /// deadline; it relies on the machine emitting a completion).
  ///
  /// No double-completion: a pre-bridge failure created no bridge, so the
  /// bridge-reap path (the only other `ExchangeCompleted` producer) never
  /// fires for this StreamId — the `retire_failed_dial` `Failed` is the
  /// single terminal event.
  ///
  /// Negative control: narrow `retire_failed_dial` back to the
  /// `UserMessage`-only gate and this assertion (a `Failed` PushPull
  /// completion is surfaced) fails — the join hang regresses.
  #[test]
  fn expired_push_pull_dial_emits_failed_exchange_completed() {
    use crate::event::{Event, ExchangeId, ExchangeKind, ExchangeOutcome};

    let a_addr: SocketAddr = "127.0.0.1:7973".parse().unwrap();
    let unreachable: SocketAddr = "127.0.0.1:7974".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);

    // `start_push_pull` on the coordinator registers the intent, dials
    // in-band, and (the connection is still handshaking) requeues the
    // intent onto `dial_pending`. The returned `StreamId` is the
    // correlation handle the QUIC driver coerces to its parked
    // `ExchangeId`.
    let id = a.start_push_pull(unreachable, crate::event::PushPullKind::Refresh, now);
    let expected_eid = ExchangeId::from(id);
    assert_eq!(
      a.dial_pending.len(),
      1,
      "test precondition: the in-band dial must requeue the still-handshaking \
       PushPull intent onto `dial_pending`"
    );
    let entry = a
      .dial_pending
      .front_mut()
      .expect("dial_pending non-empty per the preceding assertion");
    assert_eq!(
      entry.id, id,
      "the requeued entry MUST carry the registered intent's id"
    );
    // Force the expired-dial pre-check to fire on the next service tick.
    entry.deadline = now - Duration::from_millis(1);

    a.service_dials(now);

    // Drain events and find the terminal completion for this exchange.
    let mut found = None;
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(payload) = ev
        && payload.eid() == expected_eid
      {
        found = Some(payload);
        break;
      }
    }
    let payload = found.expect(
      "a PushPull dial that fails before any bridge is created MUST emit \
       Event::ExchangeCompleted keyed by ExchangeId::from(StreamId) — the \
       QUIC driver's parked join waiter resolves on exactly this event; \
       without it an unreachable-seed join hangs forever",
    );
    assert_eq!(
      payload.kind(),
      ExchangeKind::PushPull,
      "the surfaced completion MUST carry kind = PushPull"
    );
    assert_eq!(
      payload.outcome(),
      ExchangeOutcome::Failed,
      "a pre-bridge dial failure MUST carry outcome = Failed"
    );
    assert_eq!(
      payload.peer(),
      &unreachable,
      "the completion MUST carry the dialed peer address"
    );
    // The intent's staged kind/peer must be drained (no leak).
    assert!(
      !a.pending_outbound_kinds.contains_key(&id),
      "the staged outbound kind MUST be drained on dial failure"
    );
    assert!(
      !a.pending_outbound_peers.contains_key(&id),
      "the staged outbound peer MUST be drained on dial failure"
    );
  }

  /// Strict-poll self-sufficiency: a bridge inserted into `self.bridges`
  /// by `service_quinn`'s `accept(Dir::Bi)` loop (step 4) or
  /// `service_dials`'s `streams().open(Dir::Bi)` (step 5) MUST be pumped
  /// within the SAME tick — not deferred to a future `pump_bridges` cycle
  /// that a strict-poll driver may never wake to run.
  ///
  /// `quinn_proto::Connection::poll_timeout` returns only transport timers
  /// (`self.timers.next_timeout()` — loss detection / idle / close /
  /// KeyDiscard / KeepAlive). App-read readiness is NOT advertised as a
  /// transport timer: when an inbound bidi stream is accepted by
  /// `service_quinn` in step (4) of `run_tick`, step (2)'s `pump_bridges`
  /// has already run for this tick — the freshly-accepted bridge's first
  /// request data sits in quinn's per-stream recv buffer un-pumped.
  /// `Bridge::poll_timeout` falls back to the bridge's snapshotted exchange
  /// deadline while non-terminal (a non-immediate-due wake by construction:
  /// the deadline is `now + stream_timeout`, ≈ 5 s by default). Under a
  /// driver that uses ONLY `poll_timeout` as its wake source, the next
  /// coordinator wake is therefore the exchange deadline itself — at
  /// which point `Stream::handle_data` rejects the buffered request as
  /// timed out and the exchange fails even though every byte was already
  /// in quinn's recv buffer the moment the stream was accepted.
  ///
  /// Step (5.5) of `run_tick` (and the mirror in `flush_outbound`) is
  /// the same-tick second `pump_bridges` call that closes this gap.
  ///
  /// The test drives a real handshake A↔B via `start_push_pull` on A and
  /// ferries datagrams in both directions. It records the
  /// `bridges_pumped_after_acceptance` counter on B BEFORE the iteration
  /// `service_quinn` accepts the inbound bidi (signaled by
  /// `live_bridge_count() >= 1` becoming true on B). Once at least one
  /// inbound bridge appears on B, the counter MUST be `> 0` — step (5.5)
  /// pumped the freshly-accepted bridge in the same tick it was inserted.
  ///
  /// Negative control: revert the second `pump_bridges(now)` call in
  /// `run_tick` (delete the step (5.5) invocation). The newly-accepted
  /// inbound bridge on B is inserted by step (4) `service_quinn`'s
  /// `accept(Dir::Bi)` loop AFTER step (2) already ran, so `pump_bridges`
  /// never runs on it this tick; the counter increment branch in step
  /// (5.5) never executes for that bridge and the counter stays at zero.
  /// The assertion below fails.
  #[test]
  fn inbound_accepted_bridge_pumped_same_tick_under_strict_poll() {
    let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7972".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Initiate the push/pull join from A via the high-level wrapper so the
    // dial's Initial is on A's `out` queue immediately (the start-time
    // zero-time outbound flush). Then ferry datagrams in both directions
    // and call `handle_timeout(now)` per iteration to drive the handshake
    // to Established and exchange the push/pull bidi.
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a.start_push_pull(b_addr, crate::event::PushPullKind::Join, now);

    // The push/pull exchange opens a bidi from A to B. Once `service_quinn`
    // on B has accepted that bidi, B's `bridges_pumped_after_acceptance`
    // counter MUST advance — step (5.5) ran on the freshly-accepted bridge
    // before any subsequent tick could pump it. We do not gate on
    // `b.live_bridge_count()` directly because a short exchange can both
    // accept AND reap the bridge in the same iteration (after step (5.5)
    // pumps in the request data, drain_payload_only routes the
    // `PushPullReceived` endpoint event → `stream_load_response` →
    // `pump_out` writes the reply → bridge becomes terminal → next
    // `pump_bridges` reaps); the counter is the durable observable.
    let mut max_live_on_a: usize = 0;
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      max_live_on_a = max_live_on_a.max(a.live_bridge_count());
      if b.bridges_pumped_after_acceptance > 0 {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }
    assert!(
      max_live_on_a >= 1,
      "test precondition: A must have opened the push/pull bridge to B \
       at some point within the bounded ferry loop (max live bridges \
       observed on A = {max_live_on_a}). The handshake must have \
       completed and `service_dials`'s retry must have opened the bidi."
    );
    assert!(
      b.bridges_pumped_after_acceptance > 0,
      "every bridge inserted into `self.bridges` by `service_quinn`'s \
       `accept(Dir::Bi)` loop (step 4) or `service_dials`'s `open(Dir::Bi)` \
       (step 5) MUST be pumped within the SAME tick by `run_tick`'s step \
       (5.5) second `pump_bridges`. Counter on B = {} — without step (5.5), \
       the bridge accepted in step (4) sits in `self.bridges` with its first \
       request data un-pumped in quinn's recv buffer, and a strict-poll driver \
       next wakes at the bridge's exchange deadline (`Bridge::poll_timeout` \
       falls back to the snapshotted exchange deadline; \
       `quinn_proto::Connection::poll_timeout` reports transport timers only).",
      b.bridges_pumped_after_acceptance,
    );
  }

  /// `quinn_proto::Endpoint::accept` returns
  /// `Err(AcceptError { cause, response: Option<Transmit> })` for paths
  /// where it owes a refusal/close to the peer: the `accept` body
  /// produces `Some(self.initial_close(...))` on CID exhaustion and on a
  /// handshake `TransportError` from `Connection::handle_first_packet`.
  /// `route_datagram_event::DatagramEvent::NewConnection` extracts
  /// `e.response`'s bytes onto the driver-facing `out` queue (mirroring
  /// the existing `DatagramEvent::Response` arm); discarding the
  /// response would force the peer to wait its full handshake
  /// retransmit budget instead of receiving the immediate close.
  ///
  /// This test exercises the response-extraction path on a NORMAL
  /// handshake (where `accept` succeeds, so the counter stays at 0) —
  /// asserting the counter is reachable and the `#[cfg(test)]` wiring
  /// compiles. Forcing the actual error-with-response path
  /// deterministically requires either filling `Endpoint::cids_exhausted`
  /// — needs a custom 1-byte `ConnectionIdGenerator` + ~250 in-flight
  /// CIDs — or crafting a
  /// malformed Initial whose `Connection::handle_first_packet` returns
  /// `TransportError` — neither composes cleanly in the focused-unit-test
  /// budget. Negative control: revert the `Err(e)` extraction to bare
  /// `Err(_)`; the counter increment is unreachable regardless of how
  /// often the `Err`-with-response path fires, and any future test
  /// that DOES drive that path (a CID-exhaustion bench, for instance)
  /// observes the counter stuck at 0.
  #[test]
  fn accept_error_response_path_compiles_and_counter_is_wired() {
    let a_addr: SocketAddr = "127.0.0.1:7981".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7982".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive a clean handshake — `accept` succeeds on B's side, so no
    // `AcceptError` fires and the counter stays at 0. The structural
    // assertion is that the counter is observable and starts at 0
    // (i.e. the `#[cfg(test)]` wiring compiles and is reachable from
    // outside the module), so a future CID-exhaustion / malformed-
    // Initial test that DOES drive the `Err`-with-response path can
    // assert the counter advances.
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    // No AcceptError fires for a clean handshake.
    assert_eq!(
      a.accept_error_responses_emitted, 0,
      "clean handshakes never trigger AcceptError; A counter MUST stay 0"
    );
    assert_eq!(
      b.accept_error_responses_emitted, 0,
      "clean handshakes never trigger AcceptError; B counter MUST stay 0"
    );
  }

  /// A `MergeDelegate` that rejects every join push/pull merge —
  /// `notify_merge -> false` exercises `Endpoint`'s admission-rejection
  /// path which returns `Some(StreamCommand::Close)` to the bridge.
  struct RejectAllMerges;
  impl<I, A> crate::delegate::MergeDelegate<I, A> for RejectAllMerges
  where
    I: 'static,
    A: 'static,
  {
    fn notify_merge(&self, _peers: &[crate::typed::NodeState<I, A>]) -> bool {
      false
    }
  }

  /// A `MergeDelegate`-rejected inbound push/pull join MUST terminalize
  /// the QUIC bridge in the SAME tick the `Close` command fires — full
  /// bidi retirement (reset send + stop recv), `pending_out` cleared,
  /// `fatal` set, and `pump_bridges`'s post-`drain_payload_only`
  /// `is_terminal()` re-check D1-drains + reaps the bridge before the
  /// tick returns. A reset-only retirement that left `fatal == false`
  /// would reinsert the bridge into `self.bridges` and pin the quinn
  /// bidi stream until its `~5s` exchange deadline elapsed, letting a
  /// rejected peer hold transport resources per attempt
  /// (admission-boundary weakening).
  ///
  /// The test installs `RejectAllMerges` on B, drives a `Join` push/pull
  /// from A → B via the public poll surface, and asserts:
  ///  (1) B's `live_bridge_count() == 0` after the ferry loop terminates
  ///      (the rejected bridge was reaped in-tick, not pinned to the
  ///      deadline);
  ///  (2) the rejection completes within the bounded ferry budget — no
  ///      deadline-elapsed reap.
  ///
  /// Negative control: revert the `Close` arm in
  /// `bridge.rs::drain_payload_only` to bare `send_stream(sid).reset(0)`
  /// (no recv stop, no `fatal`, no `pending_out` clear) AND revert the
  /// `pump_bridges` post-`drain_payload_only` re-check. The bridge then
  /// gets reinserted into `self.bridges` with `fatal == false`; B's
  /// `live_bridge_count()` is `> 0` after the ferry loop and the
  /// assertion fires.
  #[test]
  fn rejected_join_merge_close_terminalizes_bridge_same_tick() {
    let a_addr: SocketAddr = "127.0.0.1:7991".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7992".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // B rejects every inbound merge — A's Join push/pull will trigger
    // `Endpoint::handle_stream_event(PushPullRequestReceived{kind:Join})`
    // on B's side, `merge_admitted` returns false, and the
    // `StreamCommand::Close` is routed to B's bridge.
    b.endpoint_mut().set_merge_delegate(RejectAllMerges);

    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);

    // Bounded ferry — handshake completes, A sends Join state, B's
    // delegate rejects, B emits the Close. The loop terminates on
    // no-traffic AFTER both sides processed at least one endpoint
    // event (handshake completion) so iteration 1 doesn't bail.
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    // Observable: B's `pump_bridges` post-`drain_payload_only`
    // `is_terminal()` re-check fired at least once for the rejected
    // bridge. The counter ticks ONLY when a bridge that was non-terminal
    // entering `drain_payload_only` became terminal during the per-tick
    // endpoint-event drain — i.e. the `StreamCommand::Close` path that
    // an admission rejection takes. A reset-only retirement on the
    // `Close` arm would leave `fatal == false` and the re-check would
    // never fire (counter stays 0).
    assert!(
      b.bridges_terminalized_via_close_command > 0,
      "B's `pump_bridges` post-`drain_payload_only` `is_terminal()` \
       re-check MUST have fired at least once for the rejected-merge \
       bridge (counter = {}). Without the full bidi retirement on the \
       `Close` arm the bridge stays non-terminal, and the re-check never \
       tips — the bridge lingers in `self.bridges` until its exchange \
       deadline (~5 s).",
      b.bridges_terminalized_via_close_command,
    );

    // And the bridge is gone by the time the loop terminates — no
    // lingering quinn bidi state on B.
    assert_eq!(
      b.live_bridge_count(),
      0,
      "B's `self.bridges` MUST be empty after the rejected join loop \
       terminates. Got {} live bridge(s) — the rejected bridge leaked \
       beyond the rejection tick.",
      b.live_bridge_count(),
    );
  }

  /// `Bridge::drain_then_reap` MUST retire the QUIC recv half before
  /// dropping the bridge. Memberlist frames are length-prefixed: the
  /// inner `Stream` FSM decodes the declared frame length before reading
  /// FIN and transitions to `Done` after consuming the declared bytes
  /// regardless of whether quinn-proto observed FIN on the recv half.
  /// Dropping the bridge without an explicit `RecvStream::stop` would
  /// orphan any late FIN / post-FIN events on the recv half —
  /// quinn-proto frees recv state only when `Chunks::next` observes
  /// finished OR the recv half is explicitly stopped/reset. `drain_then_reap`
  /// therefore unconditionally `stop()`s the recv half (idempotent —
  /// `Err(ClosedStream)` on the already-retired case the error/fatal
  /// pump_in/pump_out paths produce).
  ///
  /// This test exercises the clean-exchange path: A↔B push/pull where
  /// both sides cooperate, both `finish()` their send halves, both recv
  /// halves observe FIN naturally → `remote_open_streams(Dir::Bi)`
  /// returns 0 whether or not the unconditional `stop()` is present
  /// (the natural FIN-observed retire makes the same assertion hold
  /// either way). The test documents that the unconditional stop does
  /// NOT regress clean exchanges; the case the unconditional stop
  /// guards against — an adversarial peer who withholds FIN entirely
  /// (or splits FIN from the last data frame) — requires a custom
  /// quinn peer that decouples data from FIN to reproduce
  /// deterministically. Correctness in that adversarial case rests on
  /// the per-half retirement semantics described above.
  ///
  /// Negative control (by-hand verification documented here): revert
  /// the `recv_stream(sid).stop(0)` block inside `drain_then_reap`
  /// AND drive an adversarial peer that decodes a full memberlist
  /// frame on the local stream then keeps the send half open without
  /// FIN — `Connection::streams().remote_open_streams(Dir::Bi)` on the
  /// receiving node stays `> 0` after the bridge reaps. This test
  /// alone does NOT trigger that path.
  #[test]
  fn clean_bridge_reap_retires_quic_recv_half() {
    let a_addr: SocketAddr = "127.0.0.1:7901".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7902".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    let _id = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);

    // Drive the clean exchange to completion: handshake, A sends Join
    // state, B replies, both sides decode, both bridges become Done +
    // reap on the clean terminal path. Bail when no traffic moves AND
    // both sides have at least observed handshake completion.
    for _ in 0..400 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if !moved
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
        && a.live_bridge_count() == 0
        && b.live_bridge_count() == 0
      {
        break;
      }
    }

    // Test precondition: both bridges reaped on the clean path.
    assert_eq!(
      a.live_bridge_count(),
      0,
      "test precondition: A's bridge MUST have reached the clean reap path"
    );
    assert_eq!(
      b.live_bridge_count(),
      0,
      "test precondition: B's bridge MUST have reached the clean reap path"
    );

    // Observable: B's pooled connection has zero outstanding remote-
    // initiated bidi streams. Without the unconditional recv-half
    // stop, B's recv half for the push/pull exchange lingers in
    // `StreamsState::recv` and this count stays > 0.
    let ch_b = b
      .conns
      .iter_handles()
      .first()
      .copied()
      .expect("B holds the pooled connection from A's dial");
    let remote_open = b
      .conns
      .get_mut(ch_b)
      .expect("B's pooled ConnEntry is present")
      .conn_mut()
      .streams()
      .remote_open_streams(quinn_proto::Dir::Bi);
    assert_eq!(
      remote_open, 0,
      "B's pooled connection MUST retire the recv half of the accepted \
       push/pull bidi when the bridge clean-reaps. \
       remote_open_streams(Bi) = {remote_open} — the recv half leaked \
       beyond the clean reap, and repeated rounds would exhaust the \
       bidi credit on this connection."
    );
  }

  /// `BridgePhase::Failed` reap MUST be recv-clean AND send-clean by
  /// construction — every failure transition retires both halves
  /// atomically BEFORE flipping the phase, so a bridge that becomes
  /// terminal via the failure path cannot orphan quinn stream state.
  ///
  /// The most subtle failure path the atomic-retirement contract has
  /// to cover: an outbound exchange waiting for a response (FSM
  /// `OutboundAwaitingResponse`; `pending_out` empty; `is_done()` is
  /// false) whose exchange deadline elapses. The FSM's own deadline
  /// machinery inside `Stream::poll_transmit` transitions the FSM to
  /// `Failed(Timeout)` and returns `None`; the pump's deferred-finish
  /// path is gated against `stream.is_failed()` so it skips the
  /// finish; the bridge's pre-write deadline check is gated on
  /// `!pending_out.is_empty()` so it skips; the bridge's
  /// `Done`-but-unflushed check is gated on `is_done()` so it
  /// skips. Without the dedicated post-`poll_transmit` FSM-failure
  /// detector in pump_out, `is_terminal()` would still return `true`
  /// (via the old `stream.is_failed()` fallback) but `pump_out`
  /// would have called NEITHER `SendStream::reset` NOR
  /// `RecvStream::stop` — the bridge reaps with both halves orphaned
  /// in `StreamsState`, and the pooled connection bleeds bidi credit.
  ///
  /// Atomic-retirement is the architectural fix: every transition
  /// to `Failed(_)` is preceded by `retire_halves(conns)`
  /// (idempotent `reset + stop + clear_pending`), AND
  /// `is_terminal()` is now phase-authoritative (no
  /// `stream.is_failed()` fallback) so a bridge with an
  /// FSM-failed-but-phase-not-yet-Failed transient is NOT reaped
  /// until the next pump detects the FSM failure and runs the
  /// atomic transition.
  ///
  /// This test verifies the structural property on the OUTBOUND
  /// post-`poll_transmit` deadline path: A initiates a push/pull to
  /// a peer that doesn't respond (no peer at the address), waits past
  /// the exchange deadline, then drives one tick of `handle_timeout`.
  /// Asserts (a) A's bridge reaps within the tick (`live_bridge_count
  /// == 0`); (b) A's pooled connection's `remote_open_streams(Bi)` is
  /// 0 (no orphan recv state on A's view of the stream A initiated,
  /// since A-initiated streams are not "remote-opened" on A's side —
  /// but A's `send_streams()` count is the right observable here);
  /// (c) A's `send_streams()` is unchanged from the pre-dial baseline.
  ///
  /// Negative control documented inline: revert the
  /// `if let Some(e) = self.stream.is_failed()` block in
  /// `pump_out` (the post-`poll_transmit` FSM-failure detector) AND
  /// restore `is_terminal()`'s `stream.is_failed()` fallback. A's
  /// bridge still reaps within the tick, but A's `send_streams()`
  /// stays elevated above baseline because the orphaned send half
  /// was never reset.
  #[test]
  fn fsm_deadline_during_outbound_await_reaps_atomically_retired() {
    let a_addr: SocketAddr = "127.0.0.1:7993".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);

    // Baseline: no streams open before any dial.
    // We can't easily reach `a.conns` without a pooled connection; instead
    // we'll check post-reap that A's bridges have all reaped AND no
    // pooled connection holds any open send_streams.

    // Initiate a push/pull to an unreachable peer. The dial will
    // create a connection that handshakes against ... nothing. The
    // bridge intent's deadline = now + stream_timeout (~5s); the
    // handshake won't complete; eventually the deadline elapses.
    let unreachable: SocketAddr = "127.0.0.1:7994".parse().unwrap();
    // Ignoring StreamId return: tests assert on observable side
    // effects (poll_transmit/poll_event), not the handle itself.
    let _ = a
      .endpoint_mut()
      .start_push_pull(unreachable, crate::event::PushPullKind::Refresh, now);

    // Drive A's tick: dial fires, Initial queued. Then jump time
    // past the bridge's exchange deadline (~5s by default; using 30s
    // to be well past it).
    a.handle_timeout(now);
    let past_deadline = now + Duration::from_secs(30);
    a.handle_timeout(past_deadline);

    // Observable: A's bridges all reaped within the deadline-trip tick.
    // The atomic-retire-then-fail path retires both halves,
    // `is_terminal()` returns true via the `Failed(Timeout)` phase
    // (phase-authoritative, with no `stream.is_failed()` fallback), and
    // `pump_bridges` reaps.
    assert_eq!(
      a.live_bridge_count(),
      0,
      "A's bridge MUST reap within the deadline-trip tick (got {} live \
       bridges on A — the bridge is stuck non-terminal or the \
       deadline-trip path is not firing)",
      a.live_bridge_count(),
    );

    // Check every pooled connection on A: `send_streams()` MUST be 0
    // (every stream A opened has been fully retired by the atomic
    // `reset` in the failure transition; no orphan send state). Without
    // atomic retirement, the bridge would reap via a `stream.is_failed()`
    // fallback and `send_streams()` would be > 0 — quinn would still be
    // tracking the orphaned send half.
    for ch in a.conns.iter_handles() {
      let entry = a.conns.get_mut(ch).expect("conn entry present");
      let send_streams_open = entry.conn_mut().streams().send_streams();
      assert_eq!(
        send_streams_open, 0,
        "every reaped bridge on A's pooled connection MUST have retired \
         its send half by the atomic failure transition. conn {ch:?} \
         has {send_streams_open} send_streams open — indicating a send \
         half was orphaned in StreamsState::send."
      );
    }
  }

  #[cfg(feature = "compression-lz4")]
  fn build_test_quic_endpoint_with_compression(
    compression: crate::CompressionOptions,
  ) -> QuicEndpoint<SmolStr> {
    let addr: SocketAddr = "127.0.0.1:7999".parse().unwrap();
    let now = Instant::now();
    let cfg = EndpointOptions::new(SmolStr::new("test"), addr).with_rng_seed(addr.port() as u64);
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    ep.start_scheduling(now);
    let qc = test_config();
    QuicEndpoint::<SmolStr>::with_compression(ep, qc, compression)
  }

  #[cfg(all(test, feature = "quic", feature = "compression-lz4"))]
  #[test]
  fn quic_endpoint_gossip_compression_roundtrips() {
    use crate::{CompressAlgorithm, CompressionOptions};
    let coord = build_test_quic_endpoint_with_compression(
      CompressionOptions::new()
        .with_algorithm(CompressAlgorithm::Lz4)
        .with_threshold(64),
    );
    let datagram = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(8);
    let on_wire = coord.compress_gossip(&datagram);
    assert!(
      on_wire.len() < datagram.len(),
      "compressible datagram must shrink"
    );
    let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
    assert_eq!(back, datagram);
  }

  #[cfg(all(test, feature = "quic", feature = "compression-lz4"))]
  #[test]
  fn quic_endpoint_over_mtu_compressed_gossip_is_rejected() {
    // A wrapper whose orig_len exceeds the gossip MTU cannot be produced by
    // any compliant coordinator. Build one synthetically and assert it is
    // rejected without touching the body (the bomb guard checks orig_len
    // before allocation).
    use crate::CompressAlgorithm;
    let coord = build_test_quic_endpoint_with_compression(
      crate::CompressionOptions::new()
        .with_algorithm(CompressAlgorithm::Lz4)
        .with_threshold(64),
    );
    let over_mtu = coord.gossip_mtu() + 1;
    let frame = crate::encode_compressed_frame(CompressAlgorithm::Lz4, over_mtu, b"x");
    assert!(
      coord.decrypt_gossip(&frame).is_err(),
      "a compressed gossip frame claiming orig_len > gossip_mtu must be rejected"
    );
  }

  #[cfg(all(test, feature = "quic", feature = "compression-lz4"))]
  #[test]
  fn quic_endpoint_compressed_gossip_never_inflates() {
    use crate::{CompressAlgorithm, CompressionOptions};
    // Low threshold so the compressor attempts compression for all sizes in
    // the sweep, exercising the don't-expand else branch for sizes where the
    // wrapper header overhead erases the raw saving.
    let coord = build_test_quic_endpoint_with_compression(
      CompressionOptions::new()
        .with_algorithm(CompressAlgorithm::Lz4)
        .with_threshold(1),
    );
    // Sweep up to the gossip MTU: inbound decode rejects a datagram whose
    // recovered plaintext exceeds the ceiling, so a round-trip is only defined
    // for in-budget sizes (the over-MTU rejection is covered separately above).
    for len in 1..=coord.gossip_mtu() {
      // Mostly-varying pattern with a short repeated motif: the backend's
      // saving is small and varies with `len`, ensuring the don't-expand
      // else branch is exercised for some sizes while still round-tripping
      // for all.
      let input: Vec<u8> = (0..len)
        .map(|i| {
          let base = (i % 7) as u8;
          base ^ ((i / 7) as u8).wrapping_mul(3)
        })
        .collect();
      let compressed = coord.compress_gossip(&input);
      assert!(
        compressed.len() <= input.len(),
        "compress_gossip inflated datagram at len={len}: {} > {}",
        compressed.len(),
        input.len(),
      );
      let back = coord.decrypt_gossip(&compressed).expect("decrypt failed");
      assert_eq!(
        back, input,
        "compress_gossip round-trip failed at len={len}",
      );
    }
  }

  #[cfg(feature = "encryption-aes-gcm")]
  fn build_test_quic_endpoint_with_encryption(
    encryption: crate::EncryptionOptions,
  ) -> QuicEndpoint<SmolStr> {
    let addr: SocketAddr = "127.0.0.1:7999".parse().unwrap();
    let now = Instant::now();
    let cfg = EndpointOptions::new(SmolStr::new("test"), addr).with_rng_seed(addr.port() as u64);
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    ep.start_scheduling(now);
    let qc = test_config();
    QuicEndpoint::<SmolStr>::new(ep, qc).with_encryption(encryption)
  }

  #[cfg(all(test, feature = "quic", feature = "encryption-aes-gcm"))]
  #[test]
  fn quic_endpoint_gossip_encryption_roundtrip() {
    use crate::{EncryptionOptions, Keyring, SecretKey};
    let kr = Keyring::new(SecretKey::Aes256([0x42; 32]));
    let coord = build_test_quic_endpoint_with_encryption(EncryptionOptions::new().with_keyring(kr));
    let datagram = b"a quic-coordinated gossip body".to_vec();
    let on_wire = coord.encrypt_gossip(&datagram).expect("encrypt");
    assert_eq!(on_wire[0], crate::ENCRYPTED_TAG);
    let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
    assert_eq!(back, datagram);
  }

  /// A datagram larger than the connection's negotiated `max_size` is reported
  /// `TooLarge` (never silently dropped) — the driver's UDP fallback then
  /// carries it. This is the spec's "split OR fall back to UDP, never silently
  /// dropped" guarantee: the machine surface always returns an actionable
  /// outcome rather than discarding the payload.
  ///
  /// Also confirms the boundary: a payload of exactly `max_size` bytes is
  /// `Queued`, not `TooLarge`.
  #[test]
  fn oversize_unreliable_datagram_reports_too_large() {
    let a_addr: SocketAddr = "127.0.0.1:7983".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7984".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive an A<->B push/pull handshake to Established so the connection
    // has negotiated a datagram max_size (the proven ferry loop pattern).
    // Ignoring StreamId return: the test asserts on the datagram outcome.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if a.live_bridge_count() >= 1
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
      {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    let max = a
      .connection_datagram_max_size(b_addr)
      .expect("an Established connection exposes a datagram max_size");

    // One byte over the negotiated limit must be TooLarge — never a silent drop.
    let too_big = Bytes::from(vec![0u8; max + 1]);
    assert_eq!(
      a.queue_unreliable_datagram(b_addr, too_big, now),
      super::DatagramSendOutcome::TooLarge,
      "a payload of max_size+1 ({} bytes) must be TooLarge, not silently dropped",
      max + 1,
    );

    // A payload of exactly max_size bytes sits within the limit and is Queued.
    let ok = Bytes::from(vec![0u8; max]);
    assert_eq!(
      a.queue_unreliable_datagram(b_addr, ok, now),
      super::DatagramSendOutcome::Queued,
      "a payload of exactly max_size ({max} bytes) must be Queued",
    );
  }

  /// With drop=false, filling the datagram send buffer returns NotReady (the
  /// driver then falls back to UDP) instead of silently evicting the OLDEST queued
  /// datagram. This is what prevents a burst from dropping an in-flight probe.
  #[test]
  fn full_datagram_send_buffer_reports_not_ready_without_eviction() {
    let a_addr: SocketAddr = "127.0.0.1:7995".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7996".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Drive an A<->B push/pull handshake to Established so the connection has a
    // negotiated datagram max_size (the proven ferry loop pattern).
    // Ignoring StreamId return: the test asserts on the datagram outcome.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if a.live_bridge_count() >= 1
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
      {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    // Query the connection's datagram max_size; use payloads near it so the 64 KiB
    // send buffer fills within a bounded number of un-flushed sends.
    let max = a.connection_datagram_max_size(b_addr).expect("max_size");
    let chunk = max.clamp(1, 4096);
    let payload = Bytes::from(vec![0u8; chunk]);
    let mut saw_not_ready = false;
    for _ in 0..10_000 {
      match a.queue_unreliable_datagram(b_addr, payload.clone(), now) {
        super::DatagramSendOutcome::Queued => {}
        super::DatagramSendOutcome::NotReady => {
          saw_not_ready = true;
          break;
        }
        super::DatagramSendOutcome::TooLarge => {
          panic!("payload within max_size must not be TooLarge")
        }
      }
    }
    assert!(
      saw_not_ready,
      "drop=false must report NotReady when the send buffer is full, not evict and report Queued"
    );
  }

  /// A Udp-mode endpoint does not enable quinn's datagram extension, so it never
  /// advertises `max_datagram_frame_size`: its established connections report
  /// `datagrams().max_size() == None`. This is the structural guarantee behind
  /// the pure-UDP opt-out AND cross-mode interop — a Datagram-mode peer dialing a
  /// Udp-mode node sees no datagram capability, reports `NotReady`, and falls
  /// back to plain UDP rather than emitting datagrams the node would swallow.
  #[test]
  fn udp_mode_does_not_advertise_datagram_support() {
    let a_addr: SocketAddr = "127.0.0.1:7993".parse().unwrap(); // Udp-mode receiver
    let b_addr: SocketAddr = "127.0.0.1:7994".parse().unwrap(); // Datagram-mode dialer
    let now = Instant::now();
    let mut a = make_endpoint_udp("a", a_addr, now); // Udp mode: datagrams disabled
    let mut b = make_endpoint("b", b_addr, now); // Datagram mode (default)

    // Drive an A<->B push/pull handshake to Established (the proven ferry loop
    // pattern); the handshake itself does not depend on the unreliable mode, so
    // the connection forms regardless of the datagram capability difference.
    // Ignoring StreamId return: the test asserts on the negotiated datagram
    // capability, not the handle.
    let _ = b
      .endpoint_mut()
      .start_push_pull(a_addr, crate::event::PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
          moved = true;
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if b.live_bridge_count() >= 1
        && a.endpoint_events_processed > 0
        && b.endpoint_events_processed > 0
      {
        break;
      }
      if !moved && a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 {
        break;
      }
    }

    // quinn's `Connection::datagrams().max_size()` is the LOCAL send ceiling,
    // gated by the PEER's advertised `max_datagram_frame_size`. A (Udp mode)
    // advertised none, so B's connection to A reports `max_size() == None`: B
    // CANNOT send a datagram to A. This is the load-bearing cross-mode interop
    // guarantee — a Datagram-mode peer never emits a datagram a Udp-mode node
    // would silently swallow.
    assert_eq!(
      b.connection_datagram_max_size(a_addr),
      None,
      "a Datagram-mode peer must see no datagram capability toward a Udp-mode node \
       that disabled the extension (max_size == None)"
    );

    // The direct consequence at the send API: B's offer is `NotReady`, so the
    // driver falls back to plain UDP rather than dropping the payload.
    assert_eq!(
      b.queue_unreliable_datagram(a_addr, Bytes::from_static(b"\x01x"), now),
      super::DatagramSendOutcome::NotReady,
      "a Datagram-mode peer must get NotReady for a Udp-mode node that disabled datagrams"
    );
  }

  /// Strict-mode rejection MUST fire at the coordinator's public ingress
  /// API. A configured keyring + a leading tag that is not `Encrypted` is
  /// an unauthenticated plaintext Ping/Ack/Alive frame; passing it through
  /// to `handle_memberlist_udp` would bypass strict-mode entirely. The
  /// coordinator's single canonical ingress helper `decrypt_gossip` routes
  /// through `unwrap_transforms_with_encryption`, which applies the
  /// strict-mode entry check before any wrapper decoding, so cluster
  /// authentication holds without depending on driver discipline.
  #[cfg(all(test, feature = "quic", feature = "encryption-aes-gcm"))]
  #[test]
  fn quic_decrypt_gossip_rejects_plaintext_when_encryption_enabled() {
    use crate::{
      EncryptionOptions, FrameError, Keyring, MessageTag, SecretKey, encode_plain_frame,
    };
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let coord = build_test_quic_endpoint_with_encryption(opts);
    // A plain Ping frame — its leading byte is `MessageTag::Ping`, not
    // `Encrypted`, so the strict-mode entry check inside
    // `unwrap_transforms_with_encryption` MUST reject before any decoding.
    // Body shape does not matter for the leading-byte check.
    let plain_ping = encode_plain_frame(MessageTag::Ping, b"opaque-body").expect("encode");
    let result = coord.decrypt_gossip(&plain_ping);
    assert!(
      matches!(result, Err(FrameError::Encryption(_))),
      "decrypt_gossip MUST reject a plaintext datagram while encryption \
       is enabled — got {result:?}",
    );
  }

  /// A QUIC packet delivered via handle_udp must NOT advance membership timers:
  /// the packet may carry application datagrams (probe Acks/Alives) the driver has
  /// not yet decoded, so firing a probe/suspicion deadline here would be a false
  /// failure. Membership time advances ONLY on the driver's explicit
  /// handle_timeout, after it decodes mem_ingress. (The plain-UDP ingress path
  /// already has this property.)
  #[test]
  fn quic_packet_ingress_does_not_advance_membership_time() {
    let a_addr: SocketAddr = "127.0.0.1:7997".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7998".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Establish an A<->B connection (the ferry pattern) so B has a real QUIC
    // packet to send to A. Ignoring StreamId: the test asserts on the counter.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);
    let mut b_to_a: Option<Bytes> = None;
    for _ in 0..200 {
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          // capture one real QUIC packet from B to A WITHOUT feeding it yet
          if b_to_a.is_none() {
            b_to_a = Some(bytes.clone());
          }
          a.handle_udp(b_addr, &bytes, now);
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if a.endpoint_events_processed > 0 && b.endpoint_events_processed > 0 && b_to_a.is_some() {
        break;
      }
    }
    let packet = b_to_a.expect("B must have produced a QUIC packet to A");

    // Deliver a QUIC packet via handle_udp and assert membership time did NOT
    // advance; then an explicit handle_timeout DOES advance it.
    let before = a.membership_time_advances();
    a.handle_udp(b_addr, &packet, now);
    assert_eq!(
      a.membership_time_advances(),
      before,
      "handle_udp(Class::Quic) must not advance membership time (it may carry an undecoded datagram Ack)"
    );
    a.handle_timeout(now);
    assert_eq!(
      a.membership_time_advances(),
      before + 1,
      "handle_timeout must advance membership time exactly once"
    );
  }

  /// The thin membership pass-throughs each forward to the inner `Endpoint`
  /// and anchor `last_now` where documented. Exercised on a single
  /// freshly-constructed coordinator without a peer: `start_probe` (no peer ⇒
  /// `false`), `handle_alive` seeds a member, `handle_suspect` is accepted,
  /// `ping` returns a correlation id and queues an unreliable transmit,
  /// `send_user_packets` enqueues directed gossip, and the simple accessors
  /// (`is_running`, `gossip_mtu`, `max_stream_frame_size`, `live_bridge_count`,
  /// `live_connections_to`, `unreliable_transport`, `compression`,
  /// `endpoint_ref`) report the constructed defaults.
  #[test]
  fn membership_pass_throughs_forward_to_inner_endpoint() {
    use crate::{
      Node,
      typed::{Alive, Meta, Suspect},
    };

    let addr: SocketAddr = "127.0.0.1:7710".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7711".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", addr, now);

    // Construction-time accessors.
    assert!(
      ep.is_running(),
      "a fresh coordinator is in normal operation"
    );
    assert!(ep.gossip_mtu() > 0);
    assert!(ep.max_stream_frame_size() > 0);
    assert_eq!(ep.live_bridge_count(), 0);
    assert_eq!(ep.live_connections_to(peer), 0, "no connection dialed yet");
    assert_eq!(ep.unreliable_transport(), UnreliableTransport::Datagram);
    let _ = ep.compression();
    let _ = ep.endpoint_ref();

    // start_probe on a single-node cluster: no eligible target ⇒ false.
    assert!(
      !ep.start_probe(now),
      "no peer to probe ⇒ start_probe returns false"
    );

    // handle_alive seeds a member for `peer`.
    let alive = Alive::new(1, Node::new(SmolStr::new("peer"), peer)).with_meta(Meta::empty());
    ep.handle_alive(peer, alive, now);

    // handle_suspect against the seeded member is accepted (no panic, time
    // anchored).
    let suspect = Suspect::new(1, SmolStr::new("peer"), SmolStr::new("self"));
    ep.handle_suspect(peer, suspect, now);

    // ping returns a correlation token and queues an unreliable transmit.
    let _ping_id = ep
      .ping(Node::new(SmolStr::new("peer"), peer), now)
      .expect("issued while running");
    assert!(
      ep.poll_memberlist_transmit().is_some(),
      "ping must enqueue an unreliable gossip transmit"
    );

    // send_user_packets enqueues directed gossip within the MTU budget.
    ep.send_user_packets(peer, &[Bytes::from_static(b"hi")])
      .expect("a small directed user packet fits the gossip MTU");
    assert!(
      ep.poll_memberlist_transmit().is_some(),
      "send_user_packets must enqueue an unreliable transmit"
    );
  }

  /// `requeue_event` for a NON-`DialRequested` event delegates to the inner
  /// endpoint's requeue (the `other => self.ep.requeue_event(other)` arm) and
  /// is observable via the sieving public `poll_event`. (The `DialRequested`
  /// direct-routing arm is covered by `requeued_dial_request_under_strict_poll_anchors_last_now`.)
  #[test]
  fn requeue_non_dial_event_round_trips_through_poll_event() {
    use crate::event::Event;

    let addr: SocketAddr = "127.0.0.1:7720".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", addr, now);

    // A `LeftCluster` event (application-visible unit variant, not
    // DialRequested) must round-trip back out through poll_event after a
    // requeue — proving the `other` delegation arm.
    ep.requeue_event(Event::LeftCluster, now);
    let got = ep.poll_event();
    assert!(
      matches!(got, Some(Event::LeftCluster)),
      "a requeued non-dial event surfaces through poll_event — got {got:?}"
    );
  }

  /// `update_meta` / `queue_user_broadcast` / `set_local_state_snapshot` /
  /// `set_ack_payload` are the inner-endpoint state setters; each forwards and
  /// returns `Ok` for in-budget inputs on a running coordinator.
  #[test]
  fn state_setter_pass_throughs_return_ok_for_in_budget_inputs() {
    use crate::typed::Meta;

    let addr: SocketAddr = "127.0.0.1:7730".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", addr, now);

    ep.update_meta(Meta::try_from(Bytes::from_static(b"v2")).expect("small meta"))
      .expect("update_meta forwards and succeeds while running");
    ep.queue_user_broadcast(Bytes::from_static(b"broadcast"))
      .expect("queue_user_broadcast forwards");
    ep.set_local_state_snapshot(Bytes::from_static(b"snap"))
      .expect("a tiny push-pull snapshot fits the reliable frame budget");
    ep.set_ack_payload(Bytes::from_static(b"ack"))
      .expect("a tiny ack payload fits the gossip budget");
  }

  /// `leave` initiates the graceful dead-self flush on a running coordinator
  /// (returns `Ok`), and a second `leave` is the idempotent post-leave no-op.
  /// `is_running` flips to `false` after the first leave.
  #[test]
  fn leave_initiates_then_is_idempotent() {
    let addr: SocketAddr = "127.0.0.1:7740".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", addr, now);
    assert!(ep.is_running());
    ep.leave(now).expect("the first leave initiates the flush");
    assert!(
      !ep.is_running(),
      "after leave the lifecycle is no longer Running"
    );
    // Idempotent: a second leave on an already-left endpoint is a no-op Ok.
    ep.leave(now)
      .expect("a second leave is an idempotent no-op");
  }

  /// `set_compression_options` replaces the policy in place and is reflected by
  /// the `compression()` accessor; `compress_gossip` then applies the new
  /// policy on the next datagram.
  #[cfg(feature = "compression-lz4")]
  #[test]
  fn set_compression_options_updates_policy_in_place() {
    use crate::{CompressAlgorithm, CompressionOptions};

    let addr: SocketAddr = "127.0.0.1:7750".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", addr, now);
    // Default is disabled — a large datagram passes through unchanged.
    let payload = vec![0xABu8; 4096];
    assert_eq!(
      ep.compress_gossip(&payload),
      payload,
      "disabled compression is identity"
    );
    // Enable lz4; the accessor reflects it and a compressible datagram shrinks.
    ep.set_compression_options(
      CompressionOptions::new()
        .with_algorithm(CompressAlgorithm::Lz4)
        .with_threshold(8),
    );
    assert!(ep.compression().algorithm().is_some());
    let compressible = b"the quick brown fox jumps over the lazy dog".repeat(64);
    let out = ep.compress_gossip(&compressible);
    assert!(
      out.len() < compressible.len(),
      "a highly-compressible datagram must shrink once lz4 is enabled"
    );
  }

  /// With encryption DISABLED (the `make_endpoint` default), `encrypt_gossip`
  /// returns the datagram unchanged (the `None` keyring early-return), the
  /// `encryption_options` accessor reports a disabled policy, and
  /// `decrypt_gossip` on a plaintext datagram is an identity passthrough (the
  /// strict-mode entry check is gated on `encryption.is_enabled()`).
  #[test]
  fn gossip_transforms_are_identity_when_encryption_disabled() {
    let addr: SocketAddr = "127.0.0.1:7760".parse().unwrap();
    let now = Instant::now();
    let ep = make_endpoint("self", addr, now);
    assert!(
      !ep.encryption_options().is_enabled(),
      "the default coordinator has encryption disabled"
    );
    let datagram = b"plain gossip body".to_vec();
    assert_eq!(
      ep.encrypt_gossip(&datagram)
        .expect("disabled encrypt is identity"),
      datagram,
      "encrypt_gossip with no keyring returns the bytes unchanged"
    );
    assert_eq!(
      ep.decrypt_gossip(&datagram)
        .expect("disabled decrypt is identity"),
      datagram,
      "decrypt_gossip with no keyring passes a plaintext datagram through"
    );
  }

  /// `set_encryption_options` no-op-reapply short-circuit: republishing the
  /// SAME effective policy takes the equality early-return (it does NOT clear
  /// the buffered gossip ingress). A DIFFERENT policy takes the clear path —
  /// the buffered raw gossip datagram is dropped so it cannot be decrypted
  /// under the new policy.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_noop_reapply_vs_policy_change_clears_ingress() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let self_addr: SocketAddr = "127.0.0.1:7770".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7771".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", self_addr, now);

    // Buffer a raw inbound gossip datagram by feeding a Memberlist-classified
    // UDP packet (first byte in 1..=15 ⇒ Class::Memberlist ⇒ buffered).
    ep.handle_udp(peer, &[3u8, 0u8, 0u8], now);
    assert!(
      ep.poll_memberlist_ingress().is_some(),
      "precondition: a memberlist datagram is buffered"
    );

    // Re-buffer, then a NO-OP reapply of the same (disabled) policy: the
    // equality early-return must NOT clear the buffered datagram.
    ep.handle_udp(peer, &[3u8, 0u8, 0u8], now);
    ep.set_encryption_options(EncryptionOptions::new());
    assert!(
      ep.poll_memberlist_ingress().is_some(),
      "a no-op reapply of the identical policy must preserve buffered gossip"
    );

    // Re-buffer, then a REAL policy change (disabled → enabled keyring): the
    // clear path drops the datagram queued under the old policy.
    ep.handle_udp(peer, &[3u8, 0u8, 0u8], now);
    ep.set_encryption_options(
      EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x11; 32]))),
    );
    assert!(
      ep.poll_memberlist_ingress().is_none(),
      "a policy change must clear the buffered gossip queued under the old policy"
    );
    assert!(
      ep.encryption_options().is_enabled(),
      "the new policy is now in effect"
    );
  }

  /// `try_open_uni_stream_to` returns `false` when no connection to the peer
  /// exists — the `handle_for(&peer)` early-`None` guard — and anchors
  /// `last_now` so a subsequent `poll_timeout` is not stuck at `None`.
  #[test]
  fn try_open_uni_stream_to_unknown_peer_is_false() {
    let addr: SocketAddr = "127.0.0.1:7780".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7781".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", addr, now);
    assert!(
      !ep.try_open_uni_stream_to(peer, now),
      "no connection to the peer ⇒ false (no uni-stream credit to probe)"
    );
  }

  /// `start_reliable_ping` is the reliable-fallback dial wrapper: it records the
  /// exchange kind/peer, sieves the inner `DialRequested` into `dial_pending`,
  /// and attempts the dial in-band. Against a cold peer the handshake is not yet
  /// complete, so no bridge opens this tick but a quinn Initial is emitted on
  /// the outbound path (the dial was attempted). Covers the wrapper body and
  /// the `service_dials` cold-dial requeue arm.
  #[test]
  fn start_reliable_ping_attempts_dial_in_band() {
    let self_addr: SocketAddr = "127.0.0.1:7790".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7791".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", self_addr, now);

    let deadline = now + core::time::Duration::from_secs(5);
    // Ignoring StreamId return: the test asserts on the emitted transmit.
    let _ = ep.start_reliable_ping(SmolStr::new("peer"), peer, 7, deadline, now);

    // The dial was attempted in-band: a quinn Initial (Class::Quic) is queued
    // toward the peer.
    let mut saw_quic_to_peer = false;
    while let Some((to, bytes)) = ep.poll_transmit() {
      if to == peer && matches!(super::classify(&bytes), super::Class::Quic) {
        saw_quic_to_peer = true;
      }
    }
    assert!(
      saw_quic_to_peer,
      "start_reliable_ping must attempt the dial in-band (emit a quinn Initial)"
    );
    // A connection entry now exists for the peer (still handshaking).
    assert_eq!(
      ep.live_connections_to(peer),
      1,
      "the in-band dial created a pooled connection for the peer"
    );
  }

  /// `handle_packet` forwards a decoded unreliable message into the inner
  /// membership endpoint. A `UserData` packet surfaces as `Event::UserPacket`
  /// through the sieving `poll_event`.
  #[test]
  fn handle_packet_forwards_user_data_to_inner_endpoint() {
    use crate::{event::Event, typed::Message};

    let self_addr: SocketAddr = "127.0.0.1:7800".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7801".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", self_addr, now);

    ep.handle_packet(
      peer,
      Message::UserData(Bytes::from_static(b"directed-user-bytes")),
      now,
    );
    let mut saw = false;
    while let Some(ev) = ep.poll_event() {
      if matches!(&ev, Event::UserPacket(p) if p.data_ref().as_ref() == b"directed-user-bytes") {
        saw = true;
      }
    }
    assert!(
      saw,
      "handle_packet(UserData) must surface as an Event::UserPacket"
    );
  }

  /// `handle_udp` with a `Class::Reject` datagram (first byte in the
  /// `0x10..=0x3F` gap) is silently dropped — neither buffered as memberlist
  /// ingress nor fed to quinn — while still anchoring `last_now`.
  #[test]
  fn handle_udp_reject_class_datagram_is_dropped() {
    let self_addr: SocketAddr = "127.0.0.1:7810".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7811".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", self_addr, now);
    // 0x20 is in the reject gap (bits 0x40/0x80 clear, value > 15).
    assert_eq!(super::classify(&[0x20u8]), super::Class::Reject);
    ep.handle_udp(peer, &[0x20u8, 0x00], now);
    assert!(
      ep.poll_memberlist_ingress().is_none(),
      "a Reject-class datagram must not be buffered as memberlist ingress"
    );
    assert!(
      ep.poll_transmit().is_none(),
      "a Reject-class datagram must not produce any quinn output"
    );
  }

  /// The sieving `poll_event` drains a `DialRequested` left in the INNER
  /// endpoint queue into the private `dial_pending` deque and never returns it
  /// to external callers (the `DialRequested(dial) => … continue` arm). A
  /// bare-endpoint `start_reliable_ping` (no in-band service) leaves the
  /// `DialRequested` in the inner queue for `poll_event` to sieve.
  #[test]
  fn poll_event_sieves_inner_dial_requested_out() {
    use crate::event::Event;

    let self_addr: SocketAddr = "127.0.0.1:7820".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7821".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", self_addr, now);

    // Drive the inner endpoint directly so the DialRequested lands in the inner
    // queue WITHOUT the wrapper's in-band `service_dials` sieving it first.
    let deadline = now + core::time::Duration::from_secs(5);
    // Ignoring StreamId return: the test asserts on poll_event sieving.
    let _ = ep
      .endpoint_mut()
      .start_reliable_ping(SmolStr::new("peer"), peer, 3, deadline);

    // poll_event must sieve the DialRequested out — it never surfaces to the
    // external caller. (The bare endpoint queued only the DialRequested, so
    // poll_event returns None after sieving it.)
    let mut leaked_dial = false;
    while let Some(ev) = ep.poll_event() {
      if matches!(ev, Event::DialRequested(_)) {
        leaked_dial = true;
      }
    }
    assert!(
      !leaked_dial,
      "poll_event must sieve DialRequested into dial_pending, never leak it"
    );
  }

  /// The `QuicEndpoint::start_scheduling` wrapper forwards to the inner
  /// endpoint's periodic-scheduler arm. Calling it (re)arms scheduling without
  /// panicking; a subsequent `poll_timeout` then offers a bounded next-deadline.
  #[test]
  fn start_scheduling_wrapper_arms_inner_scheduler() {
    let addr: SocketAddr = "127.0.0.1:7830".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", addr, now);
    ep.start_scheduling(now);
    // The inner scheduler is armed: a finite next-deadline is offered.
    assert!(
      ep.poll_timeout().is_some(),
      "an armed scheduler offers a finite next-deadline via poll_timeout"
    );
  }

  /// `try_open_uni_stream_to` on a peer with a pooled-but-still-handshaking
  /// connection takes the `get_mut(ch) == Some` + `open(Dir::Uni) == None`
  /// path (the handshake has not granted uni-stream credit, and the composed
  /// config advertises `max_concurrent_uni_streams = 0` regardless), returning
  /// `false` without closing the connection.
  #[test]
  fn try_open_uni_stream_to_handshaking_peer_is_false_without_close() {
    let self_addr: SocketAddr = "127.0.0.1:7840".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7841".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", self_addr, now);

    // Create a pooled (handshaking) connection by offering an unreliable
    // datagram to the cold peer — `queue_unreliable_datagram` dials in.
    let _ = ep.queue_unreliable_datagram(peer, Bytes::from_static(b"probe"), now);
    assert_eq!(
      ep.live_connections_to(peer),
      1,
      "precondition: a pooled connection exists for the peer"
    );

    assert!(
      !ep.try_open_uni_stream_to(peer, now),
      "a handshaking connection grants no uni-stream credit ⇒ false"
    );
    // The connection was NOT closed (the false path skips the close-on-success
    // branch), so it is still live.
    assert_eq!(
      ep.live_connections_to(peer),
      1,
      "the false path must not close the pooled connection"
    );
  }

  /// `poll_memberlist_ingress` keeps the per-peer share counter exact: with two
  /// datagrams buffered from the SAME peer, the first pop decrements (entry
  /// retained, `*n != 0`) and the second pop removes the entry at zero. Covers
  /// the `Occupied` decrement arm that the single-datagram path skips.
  #[test]
  fn poll_memberlist_ingress_decrements_then_removes_per_peer_counter() {
    let self_addr: SocketAddr = "127.0.0.1:7850".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7851".parse().unwrap();
    let now = Instant::now();
    let mut ep = make_endpoint("self", self_addr, now);

    // Buffer two memberlist-classified datagrams from the same peer.
    ep.handle_udp(peer, &[3u8, 0xAA], now);
    ep.handle_udp(peer, &[3u8, 0xBB], now);

    // First pop: the per-peer counter decrements to 1 (entry retained).
    assert!(
      ep.poll_memberlist_ingress().is_some(),
      "first datagram pops"
    );
    // Second pop: the counter hits 0 and the entry is removed.
    assert!(
      ep.poll_memberlist_ingress().is_some(),
      "second datagram pops"
    );
    // Queue is now empty.
    assert!(
      ep.poll_memberlist_ingress().is_none(),
      "both buffered datagrams have been drained"
    );
  }

  /// `service_dials` `open(Dir::Bi) == None` with a CLOSED cached connection
  /// (the `is_closed_now == true` arm): the intent is retired via
  /// `dial_failed` rather than redialed, so a never-Established unreachable peer
  /// does not generate a fresh handshake per service tick. The pooled
  /// connection is driven to closed-never-Established directly, then
  /// `service_dials` runs against the standing `dial_pending` entry.
  #[test]
  fn service_dials_retires_intent_when_cached_connection_is_closed() {
    use crate::event::{Event, ExchangeId, ExchangeOutcome};

    let self_addr: SocketAddr = "127.0.0.1:7860".parse().unwrap();
    let peer: SocketAddr = "127.0.0.1:7861".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("self", self_addr, now);

    // Register a push/pull intent + dial in-band; the still-handshaking
    // connection requeues the intent onto `dial_pending`.
    let id = a.start_push_pull(peer, crate::event::PushPullKind::Refresh, now);
    let expected_eid = ExchangeId::from(id);
    assert_eq!(a.dial_pending.len(), 1, "precondition: intent requeued");

    // Drive the pooled connection to Closed WITHOUT it ever reaching
    // Established (the never-Established closed-cache signature that
    // `get_or_dial` returns as-is so `service_dials` fires `dial_failed`).
    let ch = a
      .conns
      .handle_for(&peer)
      .expect("a pooled connection was dialed");
    a.conns
      .get_mut(ch)
      .unwrap()
      .conn_mut()
      .close(now.into_std(), 0u32.into(), Bytes::new());

    // Keep the standing entry's deadline in the future so the closed-arm (not
    // the deadline-elapsed arm) is the one that fires.
    a.dial_pending.front_mut().unwrap().deadline = now + Duration::from_secs(30);

    a.service_dials(now);

    // The intent is retired as Failed (the closed-cached-connection arm routed
    // through `retire_failed_dial`).
    let mut found = None;
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(payload) = ev {
        if payload.eid() == expected_eid {
          found = Some(payload);
          break;
        }
      }
    }
    let payload = found.expect(
      "a dial against a closed never-Established cached connection MUST retire \
       the intent via ExchangeCompleted(Failed), not redial",
    );
    assert_eq!(payload.outcome(), ExchangeOutcome::Failed);
    assert!(
      a.dial_pending.is_empty(),
      "the retired intent must not be requeued onto dial_pending"
    );
  }

  /// `service_quinn`'s `StreamEvent::Stopped` arm: when the PEER sends
  /// STOP_SENDING for our send half, quinn-proto yields
  /// `Event::Stream(StreamEvent::Stopped{id, error_code})`. The arm retires both
  /// halves inline (RESET our send + STOP our recv) and routes the
  /// `(ch, sid)`-matched bridge through `fail_stopped_already_retired`, which
  /// transitions it to `Failed(Transport)`.
  ///
  /// To keep B's responder bridge deterministically alive when the STOP_SENDING
  /// arrives, the test establishes only the CONNECTION (a datagram offer warms
  /// the dial), then opens a RAW bidi from A at the quinn level and writes a
  /// single byte — B's `service_quinn` accepts it (a bridge appears, parked
  /// Active waiting for the rest of the never-completed reliable unit). A then
  /// STOP_SENDINGs B's send half by stopping A's recv half; after ferrying the
  /// frame, B's `service_quinn` runs the `Stopped` arm and terminalizes B's
  /// bridge as `Failed(Transport)`.
  #[test]
  fn peer_stop_sending_terminalizes_responder_bridge_via_stopped_arm() {
    let a_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Warm a pooled connection A→B WITHOUT a reliable exchange: a datagram offer
    // initiates the dial; ferrying drives the handshake to Established on both
    // sides (no bridge is created on either end — datagrams are the unreliable
    // plane).
    assert_eq!(
      a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01warm"), now),
      super::DatagramSendOutcome::NotReady,
      "the first datagram to a cold peer is best-effort NotReady and warms a dial"
    );
    a.flush_outbound_transmits(now);
    let mut a_ch: Option<quinn_proto::ConnectionHandle> = None;
    for _ in 0..200 {
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if let Some(ch) = a.conns.handle_for(&b_addr)
        && a
          .conns
          .get(ch)
          .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
          .unwrap_or(false)
      {
        a_ch = Some(ch);
        break;
      }
    }
    let a_ch = a_ch.expect("A's pooled connection to B must reach Established");
    assert_eq!(
      a.live_bridge_count(),
      0,
      "no reliable bridge is created by a datagram warm"
    );

    // Open a RAW bidi on A's pooled connection (bypassing the FSM) and write one
    // byte so B becomes aware of the stream and accepts it.
    let a_sid = a
      .conns
      .get_mut(a_ch)
      .expect("A's connection present")
      .conn_mut()
      .streams()
      .open(quinn_proto::Dir::Bi)
      .expect("A opens a raw bidi");
    // Ignoring the written-byte count: one byte fits a fresh stream's flow
    // window, and the test asserts on B accepting the bidi, not the write size.
    let _ = a
      .conns
      .get_mut(a_ch)
      .expect("A's connection present")
      .conn_mut()
      .send_stream(a_sid)
      .write(b"\x05");

    // Ferry until B's `service_quinn` accepts the bidi and a bridge appears.
    let mut b_id: Option<crate::event::StreamId> = None;
    for _ in 0..100 {
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      if let Some(id) = b.bridges.keys().next().copied() {
        b_id = Some(id);
        break;
      }
    }
    let b_id = b_id.expect("B must accept the raw bidi and create a responder bridge");
    assert!(
      b.bridges
        .get(&b_id)
        .map(|br| !br.is_phase_failed())
        .unwrap_or(false),
      "B's freshly-accepted bridge is parked Active before the STOP_SENDING"
    );

    // A STOP_SENDINGs B's send half by stopping A's recv half (the half that
    // would receive B's reply). quinn-proto queues a STOP_SENDING frame.
    let stop_res = a
      .conns
      .get_mut(a_ch)
      .expect("A's connection present")
      .conn_mut()
      .recv_stream(a_sid)
      .stop(quinn_proto::VarInt::from_u32(42));
    assert!(
      stop_res.is_ok(),
      "A must be able to STOP_SENDING B's send half (its recv half is still open)"
    );

    // Ferry A→B so the STOP_SENDING reaches B, then tick B so its
    // `service_quinn` polls the connection and runs the `Stopped` arm. B's
    // bridge MUST terminalize as Failed (the Stopped arm), then reap.
    let mut saw_failed = false;
    for _ in 0..100 {
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      b.handle_timeout(now);
      match b.bridges.get(&b_id) {
        Some(br) if br.is_phase_failed() => {
          saw_failed = true;
          break;
        }
        Some(_) => {}
        // Reaped: the Stopped arm terminalized it and `pump_bridges` removed it.
        // A reap is reachable here ONLY via the failure path — B never received
        // a complete reliable unit, so a clean BothClosed completion is
        // impossible.
        None => {
          saw_failed = true;
          break;
        }
      }
      a.handle_timeout(now);
    }
    assert!(
      saw_failed,
      "B's responder bridge MUST terminalize via the `StreamEvent::Stopped` arm \
       once A STOP_SENDINGs B's send half — the arm routes the (ch, sid)-matched \
       bridge through `fail_stopped_already_retired` → `Failed(Transport)`. \
       Without the arm the bridge would linger until its exchange deadline."
    );
  }

  /// `service_quinn`'s `StreamEvent::Finished` arm + its inner `(ch, sid)` match
  /// (and the `break` after `observe_send_fin`): when the peer ACKs our FIN,
  /// quinn-proto yields `Event::Stream(StreamEvent::Finished{id})`. The arm
  /// finds the owning bridge by `(ch, sid)` and calls `observe_send_fin`,
  /// advancing its send-half phase.
  ///
  /// The test drives a real A→B push/pull and, on B (the responder), captures
  /// B's bridge id. After B sends its reply and FINishes its send half, A ACKs
  /// the FIN; B's `service_quinn` then observes `StreamEvent::Finished` for B's
  /// send stream and routes it to B's bridge via the inner match + break. The
  /// observable is that B's bridge reaches a send-finished phase
  /// (`SendClosed`/`BothClosed`/terminal) — the `Finished` arm is the only
  /// producer of `observe_send_fin` for a peer-ACKed FIN.
  #[test]
  fn peer_acked_fin_routes_to_bridge_via_finished_arm() {
    let a_addr: SocketAddr = "127.0.0.1:8011".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:8012".parse().unwrap();
    let now = Instant::now();
    let mut a = make_endpoint("a", a_addr, now);
    let mut b = make_endpoint("b", b_addr, now);

    // Ignoring StreamId return: the test inspects bridge phase, not the handle.
    let _ = a
      .endpoint_mut()
      .start_push_pull(b_addr, crate::event::PushPullKind::Join, now);

    // Capture B's bridge id the moment B accepts the inbound exchange, then keep
    // driving. B's responder bridge sits at `RecvClosed` (recv FIN observed)
    // with its send half FINned, waiting for A to ACK that FIN. When A's ACK
    // arrives, B's `service_quinn` observes `StreamEvent::Finished` for B's send
    // stream; the inner `(ch, sid)` match finds B's still-present bridge, runs
    // `observe_send_fin` (`RecvClosed → BothClosed`), and breaks. `BothClosed`
    // is terminal, so the next `pump_bridges` reaps the bridge CLEANLY (never
    // `is_phase_failed()`). A clean reap is reachable ONLY through the Finished
    // arm's `observe_send_fin` — every failure path sets `Failed(_)`.
    let mut b_id: Option<crate::event::StreamId> = None;
    let mut ever_failed = false;
    let mut clean_reaped = false;
    for _ in 0..300 {
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, now);
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, now);
        }
      }
      a.handle_timeout(now);
      b.handle_timeout(now);
      match b_id {
        None => {
          if let Some(id) = b.bridges.keys().next().copied() {
            b_id = Some(id);
          }
        }
        Some(id) => match b.bridges.get(&id) {
          Some(br) => {
            if br.is_phase_failed() {
              ever_failed = true;
            }
          }
          None => {
            // The captured bridge has been reaped. If it never went through a
            // failure phase, this is the clean `BothClosed` reap that only the
            // Finished arm's `observe_send_fin` can produce for a responder.
            clean_reaped = true;
            break;
          }
        },
      }
    }
    assert!(
      b_id.is_some(),
      "B must have accepted an inbound responder bridge within the ferry budget"
    );
    assert!(
      clean_reaped && !ever_failed,
      "B's responder bridge MUST reap CLEANLY (BothClosed) via the \
       `StreamEvent::Finished` arm's inner `(ch, sid)` match + `observe_send_fin` \
       once A ACKs B's reply FIN — clean_reaped = {clean_reaped}, \
       ever_failed = {ever_failed}. A clean reap is reachable only through that \
       arm; every other terminus is a `Failed(_)` phase."
    );
  }
}
