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
pub use transport_mode::{DatagramSendStatus, UnreliableTransport};

use crate::Instant;
use core::net::SocketAddr;
use rand::{Rng, rngs::SmallRng};
use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use quinn_proto::{DatagramEvent, Dir, Endpoint as QuinnEndpoint};

use crate::{
  endpoint::Endpoint,
  event::{
    Event, ExchangeCompleted, ExchangeId, ExchangeKind, ExchangeStatus, PushPullKind, StreamId,
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
pub struct QuicEndpoint<I, R = SmallRng> {
  ep: Endpoint<I, SocketAddr, R>,
  quinn: QuinnEndpoint,
  cfg: QuicOptions,
  /// Cross-transport compression configuration. A disabled `CompressionOptions`
  /// makes the gossip compress/decompress methods identity.
  #[cfg(compression)]
  compression: crate::CompressionOptions,
  /// Cross-transport encryption configuration. Applied to the QUIC gossip
  /// path (plain UDP on the same socket as the QUIC packets); the QUIC
  /// reliable path always skips — quinn-encrypted streams already provide
  /// confidentiality.
  #[cfg(encryption)]
  encryption: crate::EncryptionOptions,
  /// Checksum configuration for the gossip (unreliable) plane — the QUIC
  /// datagram path. A checksum guards the connectionless datagram path, which
  /// carries no transport-level integrity of its own, so it is applied in
  /// [`Self::checksum_gossip`]. The QUIC reliable bidi bridge carries no
  /// checksum: quinn streams are already integrity-protected end to end, so
  /// corruption detection is an unreliable-plane concern (matching the original
  /// Go memberlist and the legacy port). A disabled `ChecksumOptions` makes the
  /// gossip path identity.
  #[cfg(checksum)]
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
  /// Test-only instrumentation counters — one per negative-control regression
  /// test; see [`TestCounters`] for the per-counter contract. Never compiled
  /// into production builds.
  #[cfg(test)]
  counters: TestCounters,
}

/// Test-only instrumentation for [`QuicEndpoint`] — one counter per
/// negative-control regression test. Never compiled into production builds.
#[cfg(test)]
#[derive(Debug, Default)]
struct TestCounters {
  /// Test-only counter incremented once per `EndpointEvent` drained from
  /// every connection's `poll_endpoint_events()` queue inside
  /// [`QuicEndpoint::service_quinn`]. Exists ONLY for the negative-control regression
  /// test that proves the endpoint-event drain loop runs at all (a missing
  /// drain leaves the counter at zero and breaks CID issuance / reset-token
  /// registration in quinn-proto — see [`QuicEndpoint::service_quinn`] for the
  /// per-event contract). Never compiled into production builds.
  endpoint_events_processed: u64,
  /// Test-only counter incremented once per [`Endpoint::handle_timeout`] call,
  /// i.e. once per membership-time advance. Exists ONLY for the regression test
  /// proving a QUIC packet ingress (`service_quic_inbound`) does NOT advance
  /// membership time — only the driver's explicit `handle_timeout` does — so a
  /// probe Ack carried in a datagram cannot be timed out before it is decoded.
  /// Never compiled into production builds.
  membership_time_advances: u64,
  /// Test-only counter incremented once per bridge `drain_then_reap`'d
  /// inside [`QuicEndpoint::service_quinn`] on an `Event::ConnectionLost` — the
  /// strict-poll self-sufficiency path that closes the D1 drain within
  /// the SAME tick the loss is observed (rather than deferring to a
  /// future `pump_bridges` that a strict-poll driver may never wake to
  /// run on a quiet cluster). The negative-control regression test asserts
  /// this counter advances under strict poll-surface driving; reverting
  /// the inline drain to mere `mark_fatal()` leaves it at zero. Never
  /// compiled into production builds.
  bridges_reaped_on_connection_lost: u64,
  /// Test-only counter incremented once per bridge pumped by the
  /// post-acceptance second `pump_bridges` invocation in [`QuicEndpoint::run_tick`]
  /// (step 5.5) / [`QuicEndpoint::flush_outbound`] that was inserted into
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
  bridges_pumped_after_acceptance: u64,
  /// Test-only counter incremented once each time [`QuicEndpoint::route_datagram_event`]
  /// surfaces an `AcceptError::response` from `quinn_proto::Endpoint::accept`
  /// onto the driver-facing `out` queue. quinn-proto attaches an
  /// `Option<Transmit>` to its `AcceptError` whenever `accept` owes a
  /// refusal/close to the peer (CID exhaustion or a handshake
  /// `TransportError` produce an `initial_close` response). The close bytes
  /// are written into the caller-supplied `buf` and the returned
  /// `Transmit.size` equals `buf.len()`; without this counter we have no
  /// observable seam to assert the refusal/close `Transmit` actually reaches
  /// the driver via the `out` queue. Never compiled into production builds.
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
  bridges_terminalized_via_close_command: u64,
}

// Construction, transform configuration, transport plumbing, and accessors —
// methods whose bodies touch only non-generic fields or delegate to an
// `Endpoint` accessor that needs no node identity. No bound required.
impl<I, R> QuicEndpoint<I, R> {
  /// Build the coordinator. The quinn endpoint is created with the bundled
  /// config; `allow_mtud = true`, and `rng_seed = None` so quinn seeds its
  /// connection-ID / path-challenge RNG from the OS (production entropy).
  ///
  /// Signature (quinn-proto 0.11.14): `Endpoint::new(Arc<EndpointOptions>,
  /// Option<Arc<ServerConfig>>, allow_mtud: bool, rng_seed: Option<[u8; 32]>)`.
  pub fn new(ep: Endpoint<I, SocketAddr, R>, cfg: QuicOptions) -> Self {
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
    ep: Endpoint<I, SocketAddr, R>,
    cfg: QuicOptions,
    rng_seed: Option<[u8; 32]>,
  ) -> Self {
    let quinn = QuinnEndpoint::new(cfg.endpoint_arc(), Some(cfg.server_arc()), true, rng_seed);
    Self {
      ep,
      quinn,
      cfg,
      #[cfg(compression)]
      compression: crate::CompressionOptions::new(),
      #[cfg(encryption)]
      encryption: crate::EncryptionOptions::new(),
      #[cfg(checksum)]
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
      counters: TestCounters::default(),
    }
  }

  /// Build the coordinator with an explicit cross-transport compression
  /// configuration. [`Self::new`] is `with_compression` with compression
  /// disabled.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  #[must_use]
  pub fn with_compression(
    ep: Endpoint<I, SocketAddr, R>,
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
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
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
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  pub fn set_compression_options(&mut self, compression: crate::CompressionOptions) {
    self.compression = compression;
  }

  /// Compress one outbound gossip datagram for the wire. When compression is
  /// disabled, or the datagram does not benefit, the original bytes are
  /// returned.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  pub fn compress_gossip(&self, datagram: &[u8]) -> Vec<u8> {
    match self.compression.apply(datagram) {
      Ok(crate::CompressionOutput::Compressed(packed)) => {
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
  pub fn checksum(&self) -> crate::ChecksumOptions {
    self.checksum
  }

  /// Reconfigure the gossip-plane checksum policy in place. Applies to the next
  /// outbound gossip datagram via [`Self::checksum_gossip`]. The reliable bidi
  /// bridge carries no checksum (quinn provides its own integrity), so there is
  /// no per-bridge fan-out.
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
  pub fn checksum_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, crate::ChecksumError> {
    match self.checksum.apply(datagram)? {
      crate::ChecksumOutput::Checksumed(framed) => Ok(framed),
      crate::ChecksumOutput::Plain => Ok(datagram.to_vec()),
    }
  }

  /// The configured cross-transport encryption options. Applies to the
  /// gossip path only; the QUIC reliable path always skips.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
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
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
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
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
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
  pub fn endpoint_ref(&self) -> &Endpoint<I, SocketAddr, R> {
    &self.ep
  }

  /// The machine's load-shedding counters for this QUIC endpoint. Folds the QUIC
  /// datagram-plane ingress shed (`datagram_ingress_dropped`) into
  /// [`gossip_ingress_dropped`](crate::metrics::Metrics::gossip_ingress_dropped)
  /// — the inner `Endpoint`'s own gossip-ingress count is the STREAM plane's
  /// (zero on a QUIC endpoint) — so a driver reads one unified counter regardless
  /// of transport.
  pub fn metrics(&self) -> crate::metrics::Metrics {
    // `Endpoint::metrics` returns a borrow; `Metrics` is `Copy`, so take an
    // owned copy to fold in this endpoint's datagram-plane shed count.
    let mut m = *self.ep.metrics();
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
  pub(crate) fn endpoint_mut(&mut self) -> &mut Endpoint<I, SocketAddr, R> {
    &mut self.ep
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

  /// The reliable-stream frame ceiling
  /// ([`max_stream_frame_size`](crate::config::EndpointOptions::max_stream_frame_size)).
  /// The driver derives its observation-channel payload byte budget from this.
  #[inline]
  pub fn max_stream_frame_size(&self) -> usize {
    self.ep.max_stream_frame_size()
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
  /// Build the coordinator with an explicit cross-transport encryption
  /// configuration. [`Self::new`] is `with_encryption` with encryption
  /// disabled. The configuration applies to the QUIC gossip (plain UDP)
  /// path only; the QUIC reliable path always skips encryption because
  /// quinn-encrypted streams already provide confidentiality.
  ///
  /// Routes through [`Self::set_encryption_options`] so the bridge-fan-out
  /// runs in both the builder and the in-place setter, matching
  /// [`crate::streams::StreamEndpoint::with_encryption`].
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
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
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
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
                  self.counters.accept_error_responses_emitted = self
                    .counters
                    .accept_error_responses_emitted
                    .saturating_add(1);
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
  fn emit_exchange_completed(&mut self, id: StreamId, outcome: ExchangeStatus) {
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
          ExchangeStatus::Failed,
          kind,
        )));
    }
    self.ep.dial_failed(id, err, now);
  }

  /// Determine the [`ExchangeStatus`] of a bridge at the moment it is
  /// being reaped. Mirrors [`super::streams::StreamEndpoint::reap_bridge`]'s
  /// rule: any failure phase (`BridgeFailure::Timeout`, `Transport`,
  /// `Decode`, `ConnectionLost`, `AdmissionClosed`, `DialRetired`,
  /// `EncryptionPolicyChanged`) maps to `Failed`; the cooperative
  /// `BothClosed` clean terminus maps to `Succeeded`. The bridge MUST
  /// be terminal before this is called — terminal-after-D1 is the only
  /// site that knows the final outcome.
  #[inline]
  fn outcome_for_terminal(br: &Bridge<I, SocketAddr>) -> ExchangeStatus {
    if br.is_phase_failed() {
      ExchangeStatus::Failed
    } else {
      ExchangeStatus::Succeeded
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
    self.counters.membership_time_advances
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
  ) -> DatagramSendStatus {
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
      Err(_) => return DatagramSendStatus::NotReady,
    };
    let Some(e) = self.conns.get_mut(ch) else {
      return DatagramSendStatus::NotReady;
    };
    let conn = e.conn_mut();
    match conn.datagrams().max_size() {
      // Still handshaking, peer doesn't support datagrams, or disabled locally.
      // Best-effort: the driver falls back to UDP; a later datagram lands once
      // the connection is Established.
      None => return DatagramSendStatus::NotReady,
      Some(max) if bytes.len() > max => return DatagramSendStatus::TooLarge,
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
      Ok(()) => DatagramSendStatus::Queued,
      // Send buffer full: the driver falls back to plain UDP for this payload.
      // Not a drop (the payload still goes out over UDP) and not counted.
      Err(quinn_proto::SendDatagramError::Blocked(_)) => DatagramSendStatus::NotReady,
      // UnsupportedByPeer / Disabled / TooLarge are excluded by the max_size
      // pre-check above; any residual error is unexpected — count and fall back.
      Err(_) => {
        self.datagram_dropped = self.datagram_dropped.saturating_add(1);
        DatagramSendStatus::NotReady
      }
    }
  }
}

// Membership-machine forwarders and wire encoders that need full node identity
// but never draw the gossip RNG.
impl<I, R> QuicEndpoint<I, R>
where
  I: crate::Id,
{
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

  /// Begin a graceful leave; delegates to the membership endpoint.
  pub fn leave(&mut self, now: Instant) -> Result<(), crate::error::Error> {
    self.last_now = Some(now);
    self.ep.leave(now)
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
                      #[cfg(compression)]
                      self.compression,
                      #[cfg(encryption)]
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

// The coordinator tick, scheduler arming, datagram/inbound handlers, and the
// bridge pump that fan out into probe/gossip work — drawing the gossip RNG.
impl<I, R> QuicEndpoint<I, R>
where
  R: Rng,
  I: crate::Id,
{
  /// Arm the periodic probe / gossip / push-pull schedulers. Forwards to
  /// [`Endpoint::start_scheduling`].
  #[inline]
  pub fn start_scheduling(&mut self, now: Instant) {
    self.ep.start_scheduling(now);
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
              self.counters.bridges_terminalized_via_close_command = self
                .counters
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
  /// [`TestCounters::bridges_pumped_after_acceptance`] once for each bridge whose
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
          self.counters.bridges_pumped_after_acceptance = self
            .counters
            .bridges_pumped_after_acceptance
            .saturating_add(1);
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
            self.counters.bridges_terminalized_via_close_command = self
              .counters
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
        self.counters.membership_time_advances =
          self.counters.membership_time_advances.saturating_add(1);
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
                  #[cfg(compression)]
                  self.compression,
                  #[cfg(encryption)]
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
            // criterion is `LinkState::BothClosed | Failed(_)`, so
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
          self.counters.endpoint_events_processed =
            self.counters.endpoint_events_processed.saturating_add(1);
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
              self.counters.bridges_reaped_on_connection_lost = self
                .counters
                .bridges_reaped_on_connection_lost
                .saturating_add(1);
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
}

#[cfg(test)]
mod tests;
