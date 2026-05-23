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
mod crypto;
mod demux;

pub use crypto::QuicConfig;

use std::{collections::HashMap, net::SocketAddr, time::Instant};

use bytes::{Bytes, BytesMut};
use quinn_proto::{DatagramEvent, Dir, Endpoint as QuinnEndpoint};

use crate::{
  addr_bridge::AddrBridge,
  endpoint::Endpoint,
  event::{Event, PushPullKind, StreamId, Transmit},
};
use bridge::Bridge;
use conn::ConnTable;
use demux::{classify, Class};

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
struct PendingDial<A> {
  id: StreamId,
  peer: A,
  deadline: Instant,
  attempted: bool,
}

/// Coordinator: `memberlist::Endpoint` (unreliable + membership) composed with
/// `quinn_proto::Endpoint` (reliable). Pure Sans-I/O — inject `now`.
///
/// `B` translates the membership address `A` to the QUIC `SocketAddr`
/// (see [`AddrBridge`]); it is a marker type parameter only — no value of
/// `B` is stored.
pub struct QuicEndpoint<I, A, B>
where
  I: nodecraft::Id
    + memberlist_wire::Data
    + nodecraft::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::Data
    + nodecraft::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  ep: Endpoint<I, A>,
  quinn: QuinnEndpoint,
  cfg: QuicConfig,
  /// Cross-transport compression configuration. A disabled `CompressionOptions`
  /// makes the gossip compress/decompress methods identity.
  compression: memberlist_wire::CompressionOptions,
  /// Cross-transport encryption configuration. Applied to the QUIC gossip
  /// path (plain UDP on the same socket as the QUIC packets); the QUIC
  /// reliable path always skips — quinn-encrypted streams already provide
  /// confidentiality.
  encryption: memberlist_wire::EncryptionOptions,
  conns: ConnTable,
  bridges: HashMap<StreamId, Bridge<I, A>>,
  /// Outbound UDP datagrams produced this tick (quinn datagrams + stateless
  /// `Response`s; the memberlist unreliable path is NOT prebuffered — see
  /// [`poll_memberlist_transmit`](Self::poll_memberlist_transmit)).
  out: std::collections::VecDeque<(SocketAddr, Bytes)>,
  /// Raw inbound memberlist datagrams the first-byte demux classified as
  /// `Class::Memberlist`. `memberlist-machine` has no umbrella `codec`
  /// dependency, so the coordinator cannot decode them in-crate and MUST NOT
  /// silently drop them (that would lose every UDP ping/ack/alive/suspect on
  /// the composed unit's public ingress). They are buffered here and surfaced
  /// as an explicit action via [`poll_memberlist_ingress`](Self::poll_memberlist_ingress)
  /// — the same idiom the coordinator uses for QUIC `Transmit`/`DatagramEvent`
  /// — for the codec-owning layer to unwrap and feed back through
  /// [`handle_packet`](Self::handle_packet).
  mem_ingress: std::collections::VecDeque<(SocketAddr, Bytes)>,
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
  dial_pending: std::collections::VecDeque<PendingDial<A>>,
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
  /// Test-only counter incremented once per `EndpointEvent` drained from
  /// every connection's `poll_endpoint_events()` queue inside
  /// [`Self::service_quinn`]. Exists ONLY for the negative-control regression
  /// test that proves the endpoint-event drain loop runs at all (a missing
  /// drain leaves the counter at zero and breaks CID issuance / reset-token
  /// registration in quinn-proto — see [`Self::service_quinn`] for the
  /// per-event contract). Never compiled into production builds.
  #[cfg(test)]
  endpoint_events_processed: u64,
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
  _addr: core::marker::PhantomData<fn(B)>,
}

impl<I, A, B> QuicEndpoint<I, A, B>
where
  I: nodecraft::Id
    + memberlist_wire::Data
    + nodecraft::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::Data
    + nodecraft::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  B: AddrBridge<A>,
{
  /// Build the coordinator. The quinn endpoint is created with the bundled
  /// config; `allow_mtud = true`, and `rng_seed = None` so quinn seeds its
  /// connection-ID / path-challenge RNG from the OS (production entropy).
  ///
  /// Signature (quinn-proto 0.11.14): `Endpoint::new(Arc<EndpointConfig>,
  /// Option<Arc<ServerConfig>>, allow_mtud: bool, rng_seed: Option<[u8; 32]>)`.
  pub fn new(ep: Endpoint<I, A>, cfg: QuicConfig) -> Self {
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
  pub fn with_quinn_rng_seed(
    ep: Endpoint<I, A>,
    cfg: QuicConfig,
    rng_seed: Option<[u8; 32]>,
  ) -> Self {
    let quinn = QuinnEndpoint::new(
      cfg.endpoint().clone(),
      Some(cfg.server().clone()),
      true,
      rng_seed,
    );
    Self {
      ep,
      quinn,
      cfg,
      compression: memberlist_wire::CompressionOptions::new(),
      encryption: memberlist_wire::EncryptionOptions::new(),
      conns: ConnTable::new(),
      bridges: HashMap::new(),
      out: std::collections::VecDeque::new(),
      mem_ingress: std::collections::VecDeque::new(),
      dial_pending: std::collections::VecDeque::new(),
      last_now: None,
      #[cfg(test)]
      endpoint_events_processed: 0,
      #[cfg(test)]
      bridges_reaped_on_connection_lost: 0,
      #[cfg(test)]
      bridges_pumped_after_acceptance: 0,
      #[cfg(test)]
      accept_error_responses_emitted: 0,
      #[cfg(test)]
      bridges_terminalized_via_close_command: 0,
      _addr: core::marker::PhantomData,
    }
  }

  /// Build the coordinator with an explicit cross-transport compression
  /// configuration. [`Self::new`] is `with_compression` with compression
  /// disabled.
  pub fn with_compression(
    ep: Endpoint<I, A>,
    cfg: QuicConfig,
    compression: memberlist_wire::CompressionOptions,
  ) -> Self {
    let mut this = Self::new(ep, cfg);
    this.compression = compression;
    this
  }

  /// The configured cross-transport compression options.
  pub fn compression(&self) -> memberlist_wire::CompressionOptions {
    self.compression
  }

  /// Compress one outbound gossip datagram for the wire. When compression is
  /// disabled, or the datagram does not benefit, the original bytes are
  /// returned.
  pub fn compress_gossip(&self, datagram: &[u8]) -> Vec<u8> {
    match self.compression.apply(datagram) {
      Ok(memberlist_wire::CompressionOutcome::Compressed(packed)) => {
        let wrapped = memberlist_wire::encode_compressed_frame(
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

  /// Build the coordinator with an explicit cross-transport encryption
  /// configuration. [`Self::new`] is `with_encryption` with encryption
  /// disabled. The configuration applies to the QUIC gossip (plain UDP)
  /// path only; the QUIC reliable path always skips encryption because
  /// quinn-encrypted streams already provide confidentiality.
  ///
  /// Routes through [`Self::set_encryption_options`] so the bridge-fan-out
  /// runs in both the builder and the in-place setter, matching
  /// [`crate::streams::StreamEndpoint::with_encryption`].
  pub fn with_encryption(mut self, encryption: memberlist_wire::EncryptionOptions) -> Self {
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
  pub fn set_encryption_options(&mut self, encryption: memberlist_wire::EncryptionOptions) {
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
    self.encryption = encryption;
  }

  /// The configured cross-transport encryption options. Applies to the
  /// gossip path only; the QUIC reliable path always skips.
  pub fn encryption_options(&self) -> &memberlist_wire::EncryptionOptions {
    &self.encryption
  }

  /// Encrypt one outbound gossip datagram for the wire. The codec-owning
  /// driver calls this on the already-compressed gossip bytes (from
  /// [`Self::compress_gossip`]) before handing them to the UDP socket. When
  /// encryption is disabled the bytes are returned unchanged.
  ///
  /// The on-wire byte order is `[Encrypted[Compressed[frame]]]` when both
  /// transforms are enabled and compression won, or `[Encrypted[frame]]`
  /// when compression is disabled or did not shrink.
  ///
  /// Returns `Err` when encryption is configured but the backend rejects the
  /// request — typically [`memberlist_wire::EncryptionError::UnsupportedAlgorithm`]
  /// for a primary key whose backend was not built into this binary. The
  /// driver MUST drop the gossip in that case; emitting unencrypted bytes
  /// on an encrypted-cluster path would bypass authentication silently.
  pub fn encrypt_gossip(
    &self,
    datagram: &[u8],
  ) -> Result<Vec<u8>, memberlist_wire::EncryptionError> {
    let keyring = match self.encryption.keyring() {
      Some(kr) => kr,
      None => return Ok(datagram.to_vec()),
    };
    let key = keyring.primary();
    memberlist_wire::encode_encrypted_frame(key.algorithm(), key, datagram)
  }

  /// Unwrap one inbound gossip datagram. The codec-owning driver calls this
  /// on the raw bytes from [`Self::poll_memberlist_ingress`] BEFORE decoding
  /// frames — it strips the Encrypted-then-Compressed wrapper stack in one
  /// pass (each layer identity when its wrapper is absent). A datagram with
  /// no Encrypted wrapper is returned unchanged when no keyring is
  /// configured; when a keyring IS configured the strict-mode entry check
  /// rejects any non-Encrypted leading tag. A corrupt or unknown-algorithm
  /// wrapper, or a frame the keyring cannot decrypt, is an `Err` — the
  /// driver drops the datagram (gossip is lossy and self-healing).
  ///
  /// This is the SINGLE canonical ingress unwrap on the coordinator. The
  /// outbound side uses [`Self::compress_gossip`] → [`Self::encrypt_gossip`]
  /// (compress, then encrypt) so the on-wire order is
  /// `[Encrypted[Compressed[frame]]]`; this helper reverses both layers, so
  /// authentication never depends on integration discipline.
  pub fn decrypt_gossip(&self, datagram: &[u8]) -> Result<Vec<u8>, memberlist_wire::FrameError> {
    // Ceiling is the gossip MTU — the maximum size any compliant gossip
    // datagram decompresses to. A wrapper claiming more is not a compliant
    // datagram and is rejected. The encryption-aware unwrap consumes an
    // Encrypted wrapper through the keyring, then strips a Compressed
    // wrapper if present; a non-Encrypted-led datagram is returned unchanged
    // when no keyring is configured (the strict-mode entry check is gated
    // on `encryption.is_enabled()`).
    memberlist_wire::unwrap_transforms_with_encryption(
      datagram,
      self.ep.gossip_mtu(),
      &self.encryption,
    )
    .map(|cow| cow.into_owned())
  }

  /// Borrow the inner membership endpoint (members / queue_user_broadcast / …).
  pub fn endpoint(&self) -> &Endpoint<I, A> {
    &self.ep
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
  pub(crate) fn endpoint_mut(&mut self) -> &mut Endpoint<I, A> {
    &mut self.ep
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
  pub fn handle_alive(&mut self, from: A, alive: memberlist_wire::typed::Alive<I, A>, at: Instant) {
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
    from: A,
    suspect: memberlist_wire::typed::Suspect<I>,
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
  pub fn requeue_event(&mut self, ev: Event<I, A>, now: Instant) {
    self.last_now = Some(now);
    match ev {
      Event::DialRequested { id, peer, deadline } => {
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
  pub fn poll_event(&mut self) -> Option<Event<I, A>> {
    loop {
      match self.ep.poll_event()? {
        Event::DialRequested { id, peer, deadline } => {
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
        if let Some(t) = e.conn_mut().poll_timeout() {
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

  /// Next outbound UDP datagram (quinn or encoded memberlist), if any.
  pub fn poll_transmit(&mut self) -> Option<(SocketAddr, Bytes)> {
    self.out.pop_front()
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
  pub fn poll_memberlist_transmit(&mut self) -> Option<Transmit<I, A>> {
    self.ep.poll_transmit()
  }

  /// Next raw inbound memberlist datagram (the first-byte demux classified it
  /// `Class::Memberlist`), if any.
  ///
  /// `memberlist-machine` has no umbrella `codec` dependency, so the
  /// coordinator cannot perform the structured unwrap
  /// (label → decrypt → decompress → split-compound) in-crate; it surfaces
  /// the raw `(from, bytes)` as an explicit action instead of silently
  /// dropping it (a silent drop would lose every UDP ping/ack/alive/suspect
  /// on the composed unit's public ingress). The codec-owning layer drains
  /// this, decodes each `Message`, and feeds it back through
  /// [`handle_packet`](Self::handle_packet).
  pub fn poll_memberlist_ingress(&mut self) -> Option<(SocketAddr, Bytes)> {
    self.mem_ingress.pop_front()
  }

  /// Feed one decoded unreliable memberlist [`Message`](memberlist_wire::typed::Message)
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
    from: A,
    msg: memberlist_wire::typed::Message<I, A>,
    now: Instant,
  ) {
    self.ep.handle_packet(from, msg, now);
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
          .map(|e| !e.conn().is_drained())
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
  /// Sourced from [`crate::config::EndpointConfig::gossip_mtu`] (default
  /// [`crate::config::DEFAULT_GOSSIP_MTU`]). The on-wire datagram may
  /// exceed this by [`memberlist_wire::ENCRYPTED_WRAPPER_OVERHEAD`] when
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
  /// installed by [`QuicConfig::new`] advertises
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
      e.conn_mut()
        .close(now, quinn_proto::VarInt::from_u32(0), bytes::Bytes::new());
    }
    opened
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
        if let Some(ev) = self.quinn.handle(now, from, None, None, data, &mut scratch) {
          self.route_datagram_event(ev, from, now, &scratch);
        }
        self.run_tick(now);
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
  pub fn start_push_pull(&mut self, peer: A, kind: PushPullKind, now: Instant) -> StreamId {
    self.last_now = Some(now);
    let id = self.ep.start_push_pull(peer, kind, now);
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
    peer_addr: A,
    probe_seq: u32,
    deadline: Instant,
    now: Instant,
  ) -> StreamId {
    self.last_now = Some(now);
    let id = self
      .ep
      .start_reliable_ping(peer_id, peer_addr, probe_seq, deadline);
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
  pub fn start_user_message(&mut self, peer: A, payload: Bytes, now: Instant) -> StreamId {
    self.last_now = Some(now);
    let id = self.ep.start_user_message(peer, payload, now);
    self.service_dials(now);
    self.flush_outbound(now);
    id
  }

  // ---- internals --------------------------------------------------------

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
        match self
          .quinn
          .accept(incoming, now, &mut buf, Some(self.cfg.server().clone()))
        {
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
    // The umbrella `codec` is not a dependency of memberlist-machine, so the
    // byte-level decode (decode_incoming -> parse_messages -> handle_packet)
    // cannot run in-crate. Surfacing the raw datagram as an explicit ingress
    // action — never a silent no-op — is required for the composed unit's
    // ingress to remain correct: a no-op here would lose every UDP
    // ping/ack/alive/suspect on the composed unit's public ingress. The
    // codec-owning layer drains it via `poll_memberlist_ingress`, decodes
    // each `Message`, and feeds it back through `handle_packet`.
    self
      .mem_ingress
      .push_back((from, Bytes::copy_from_slice(datagram)));
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
    // (1) inbound feed already done in `handle_udp` before `run_tick`.
    // (2) pump bridges + drain stream endpoint-events into the Endpoint.
    #[cfg(test)]
    let pre_step2_ids: std::collections::HashSet<StreamId> = self.bridges.keys().copied().collect();
    self.pump_bridges(now);
    // (3) THEN memberlist timers (probe cumulative-deadline, suspicion).
    self.ep.handle_timeout(now);
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
          // (5) reap AFTER drain: dropping the bridge frees its slot.
          drop(br);
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
            drop(br);
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
          drop(br);
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
            drop(br);
          } else {
            self.bridges.insert(*id, br);
          }
        }
      }
    }
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
      e.conn_mut().handle_timeout(now);
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
              let stream = self.ep.accept_stream(B::from_socket(peer), now);
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
      let drained = e.conn().is_drained();
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
            drop(br);
          }
        }
      }
    }
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
    let mut others: Vec<Event<I, A>> = Vec::new();
    while let Some(ev) = self.ep.poll_event() {
      match ev {
        Event::DialRequested { id, peer, deadline } => {
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

  fn service_dials(&mut self, now: Instant) {
    // Sieve any DialRequested newly emitted by the inner endpoint into the
    // private `dial_pending` deque, then drain that deque as the sole input.
    // Non-`DialRequested` events stay in the inner endpoint's queue for the
    // public `poll_event()` to observe.
    self.sieve_dial_events();
    let pending = std::mem::take(&mut self.dial_pending);
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
        self.ep.dial_failed(
          id,
          crate::error::StreamError::DialFailed("quic dial deadline elapsed".into()),
          now,
        );
        continue;
      }
      let addr = B::to_socket(&peer);
      let sni = match B::server_name(&peer) {
        Some(s) => s,
        None => {
          // Soft-fail-via-dial_failed — the user's `AddrBridge` returned
          // `None`, but QUIC requires a verification identity. Mirrors
          // the TLS coordinator's parse-failure path: the bridge author's
          // contract for QUIC use is "return `Some(_)`"; `None` is treated
          // as a per-peer misconfiguration that fails just that one dial —
          // other peers/exchanges continue unaffected.
          self.ep.dial_failed(
            id,
            crate::error::StreamError::DialFailed(
              "quic bridge returned None for server_name".into(),
            ),
            now,
          );
          continue;
        }
      };
      let sni_str: &str = sni.as_ref();
      match self.conns.get_or_dial(
        &mut self.quinn,
        now,
        self.cfg.client().clone(),
        addr,
        sni_str,
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
                  if let Some(e) = self.conns.get_mut(ch) {
                    let conn = e.conn_mut();
                    let _ = conn
                      .send_stream(sid)
                      .reset(quinn_proto::VarInt::from_u32(0));
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
                //       handle for a previously-Established peer
                //       triggers an explicit redial; a never-Established
                //       cache prevents a fresh-handshake storm against a
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
                  .map(|c| c.conn().is_closed())
                  .unwrap_or(true);
                if is_closed_now {
                  self.ep.dial_failed(
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
                  self.ep.dial_failed(
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
        Err(e) => self.ep.dial_failed(
          id,
          crate::error::StreamError::DialFailed(e.to_string()),
          now,
        ),
      }
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
      while let Some(tr) = e.conn_mut().poll_transmit(now, 1, &mut buf) {
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
}

#[cfg(test)]
mod tests {
  use std::{
    net::SocketAddr,
    time::{Duration, Instant},
  };

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::{AddrBridge, QuicEndpoint};
  use crate::{config::EndpointConfig, endpoint::Endpoint, quic::QuicConfig};

  struct IdBridge;
  impl AddrBridge<SocketAddr> for IdBridge {
    type ServerName = str;

    fn to_socket(addr: &SocketAddr) -> SocketAddr {
      *addr
    }
    fn from_socket(socket: SocketAddr) -> SocketAddr {
      socket
    }
    fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
      // Sim/test cert SAN is "localhost"; the identity bridge for
      // `A = SocketAddr` returns it for every peer.
      Some("localhost")
    }
  }

  fn test_config() -> QuicConfig {
    let mut transport = quinn_proto::TransportConfig::default();
    transport.max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
    ));
    QuicConfig::new(
      crate::quic::crypto::tests::test_endpoint_config(&[0x5au8; 32]),
      crate::quic::crypto::tests::test_server(),
      crate::quic::crypto::tests::test_client(),
      transport,
    )
  }

  fn make_endpoint(
    id: &str,
    addr: SocketAddr,
    now: Instant,
  ) -> QuicEndpoint<SmolStr, SocketAddr, IdBridge> {
    let cfg = EndpointConfig::new(SmolStr::new(id), addr).with_rng_seed(Some(addr.port() as u64));
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    ep.start_scheduling(now);
    let qc = test_config();
    let mut seed = [0u8; 32];
    seed[..2].copy_from_slice(&addr.port().to_le_bytes());
    QuicEndpoint::<SmolStr, SocketAddr, IdBridge>::with_quinn_rng_seed(ep, qc, Some(seed))
  }

  #[test]
  fn quic_endpoint_type_is_constructible_signature() {
    // Behavioural coverage is the sim harness (needs a virtual clock + a
    // peer). This guards the public constructor signature only: the
    // coordinator is generic over `I`, `A`, and the `AddrBridge` marker `B`,
    // and `new` takes the membership `Endpoint` plus the `QuicConfig` bundle.
    fn _sig<I, A, B>()
    where
      I: nodecraft::Id
        + memberlist_wire::Data
        + nodecraft::CheapClone
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
      A: memberlist_wire::Data
        + nodecraft::CheapClone
        + Eq
        + core::hash::Hash
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
      B: super::AddrBridge<A>,
    {
      let _: fn(
        crate::endpoint::Endpoint<I, A>,
        super::QuicConfig,
      ) -> super::QuicEndpoint<I, A, B> = super::QuicEndpoint::<I, A, B>::new;
    }
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
  /// `EndpointConfig::new` defaults `grease_quic_bit: true`; under that
  /// default both sides advertise greasing and quinn-proto's packet
  /// encoder is permitted to clear FIXED_BIT on outgoing short-header
  /// packets. The coordinator forces `grease_quic_bit(false)` in
  /// [`QuicConfig::new`](super::crypto::QuicConfig::new) so the
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
  /// Negative control: revert `super::crypto::QuicConfig::new` to omit
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
      .close(now, 0u32.into(), Bytes::new());
    for _ in 0..5000 {
      if a
        .conns
        .get(ch_a)
        .map(|e| e.conn().is_drained())
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
        .map(|e| e.conn().is_drained())
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
    let cfg =
      EndpointConfig::new(SmolStr::new("a"), a_addr).with_rng_seed(Some(a_addr.port() as u64));
    let mut bare_ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    bare_ep.start_scheduling(t0);
    let _ = bare_ep.start_push_pull(b_addr, PushPullKind::Join, t0);

    // Drain the DialRequested. The caller now holds it.
    let dial_requested = loop {
      match bare_ep.poll_event() {
        Some(crate::event::Event::DialRequested { id, peer, deadline }) => {
          break crate::event::Event::DialRequested { id, peer, deadline };
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
    let mut a: QuicEndpoint<SmolStr, SocketAddr, IdBridge> =
      QuicEndpoint::with_quinn_rng_seed(bare_ep, qc, Some(seed));

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
        .map(|e| !e.conn().is_handshaking() && !e.conn().is_closed())
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
      .close(now, 0u32.into(), bytes::Bytes::new());

    let close_due = a
      .conns
      .get_mut(ch_a)
      .expect("A's pooled connection is present")
      .conn_mut()
      .poll_timeout()
      .expect("Connection::close arms the Close timer; poll_timeout MUST be Some");
    a.handle_timeout(close_due);

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
        .map(|e| !e.conn().is_handshaking() && !e.conn().is_closed())
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
    fn notify_merge(&self, _peers: &[memberlist_wire::typed::NodeState<I, A>]) -> bool {
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
    compression: memberlist_wire::CompressionOptions,
  ) -> QuicEndpoint<SmolStr, SocketAddr, IdBridge> {
    let addr: SocketAddr = "127.0.0.1:7999".parse().unwrap();
    let now = Instant::now();
    let cfg =
      EndpointConfig::new(SmolStr::new("test"), addr).with_rng_seed(Some(addr.port() as u64));
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    ep.start_scheduling(now);
    let qc = test_config();
    QuicEndpoint::<SmolStr, SocketAddr, IdBridge>::with_compression(ep, qc, compression)
  }

  #[cfg(all(test, feature = "quic", feature = "compression-lz4"))]
  #[test]
  fn quic_endpoint_gossip_compression_roundtrips() {
    use memberlist_wire::{CompressAlgorithm, CompressionOptions};
    let coord = build_test_quic_endpoint_with_compression(
      CompressionOptions::new()
        .with_algorithm(Some(CompressAlgorithm::Lz4))
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
    use memberlist_wire::CompressAlgorithm;
    let coord = build_test_quic_endpoint_with_compression(
      memberlist_wire::CompressionOptions::new()
        .with_algorithm(Some(CompressAlgorithm::Lz4))
        .with_threshold(64),
    );
    let over_mtu = coord.gossip_mtu() + 1;
    let frame = memberlist_wire::encode_compressed_frame(CompressAlgorithm::Lz4, over_mtu, b"x");
    assert!(
      coord.decrypt_gossip(&frame).is_err(),
      "a compressed gossip frame claiming orig_len > gossip_mtu must be rejected"
    );
  }

  #[cfg(all(test, feature = "quic", feature = "compression-lz4"))]
  #[test]
  fn quic_endpoint_compressed_gossip_never_inflates() {
    use memberlist_wire::{CompressAlgorithm, CompressionOptions};
    // Low threshold so the compressor attempts compression for all sizes in
    // the sweep, exercising the don't-expand else branch for sizes where the
    // wrapper header overhead erases the raw saving.
    let coord = build_test_quic_endpoint_with_compression(
      CompressionOptions::new()
        .with_algorithm(Some(CompressAlgorithm::Lz4))
        .with_threshold(1),
    );
    for len in 1..=1500 {
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
    encryption: memberlist_wire::EncryptionOptions,
  ) -> QuicEndpoint<SmolStr, SocketAddr, IdBridge> {
    let addr: SocketAddr = "127.0.0.1:7999".parse().unwrap();
    let now = Instant::now();
    let cfg =
      EndpointConfig::new(SmolStr::new("test"), addr).with_rng_seed(Some(addr.port() as u64));
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
    ep.start_scheduling(now);
    let qc = test_config();
    QuicEndpoint::<SmolStr, SocketAddr, IdBridge>::new(ep, qc).with_encryption(encryption)
  }

  #[cfg(all(test, feature = "quic", feature = "encryption-aes-gcm"))]
  #[test]
  fn quic_endpoint_gossip_encryption_roundtrip() {
    use memberlist_wire::{EncryptionOptions, Keyring, SecretKey};
    let kr = Keyring::new(SecretKey::Aes256([0x42; 32]));
    let coord = build_test_quic_endpoint_with_encryption(EncryptionOptions::new().with_keyring(kr));
    let datagram = b"a quic-coordinated gossip body".to_vec();
    let on_wire = coord.encrypt_gossip(&datagram).expect("encrypt");
    assert_eq!(on_wire[0], memberlist_wire::ENCRYPTED_TAG);
    let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
    assert_eq!(back, datagram);
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
    use memberlist_wire::{
      encode_plain_frame, EncryptionOptions, FrameError, Keyring, MessageTag, SecretKey,
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
}
