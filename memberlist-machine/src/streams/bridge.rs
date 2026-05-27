//! `memberlist::Stream` <-> record-layer byte-pump for one reliable exchange.
//!
//! Phase: `Handshaking` (no `Stream` yet) → `Established(BridgePhase)` (the
//! byte pump runs and the half-close lifecycle tracks via the shared
//! [`BridgePhase`]). The `Stream` is minted by the coordinator at
//! `dial_succeeded` / `accept_stream` once
//! [`StreamTransport::is_handshaking`] clears the bridge-mint gate. For the
//! acceptor that gate clears once the inbound label is read and validated (the
//! bridge mustn't dispatch a merge until the cluster is confirmed right); for
//! the dialer it is open from construction (classic memberlist sends
//! `[label][request]` together — see
//! `memberlist-core/src/state.rs::push_pull_node` /
//! `memberlist-core/src/network.rs::reliable_encoder`), so a dialer bridge
//! mints its `Stream` in the same tick the request is queued and runs the
//! inbound response-label validation in-line on the established intake.
//!
//! `StreamBridge<I, A, R>` is generic over `R: StreamTransport` — the same
//! bridge drives plain TCP (`R = tcp::RawRecords`) and TLS-over-TCP
//! (`R = tls::TlsRecords`). D1 (drain-before-reap) and the atomic-failure /
//! queue-clear discipline apply uniformly: every failure transition clears the
//! FSM's queued endpoint events before the terminal phase, so no queued merge /
//! ack survives an aborted exchange.
//!
//! # Transport anchors and the FIN-only half-close
//!
//! A TLS record layer anchors its half-close on in-band `close_notify`: its
//! `send_close_notify()` queues an alert that retires the send half, and the
//! peer's close is the `close_notify` latch (`peer_has_closed()`). Plain TCP has
//! NO in-band close record — its `send_close_notify()` emits no bytes and its
//! `peer_has_closed()` is always `false` (frozen `memberlist-net` reliable close
//! is the out-of-band TCP FIN). The two anchors therefore become:
//!
//! * **RECV close** is driven by the transport `eof` (a `read == 0`) the
//!   coordinator passes into the byte pump, OR'd with the record layer's
//!   in-band close latch: `fin = eof || records.peer_has_closed()`. For a
//!   transport with no in-band close signal the second term is just always
//!   `false`. An `eof` feeds `Stream::handle_data(&[], now)` and, on a clean
//!   accept, retires the recv half (`RecvClosed`); a premature `eof` mid-frame
//!   is the truncation path (`PeerClosed` → decode failure).
//! * **SEND close** calls `R::send_close_notify()` and transitions the send
//!   half to `SendClosed`. For a record layer with an in-band close that call
//!   queues a `close_notify` alert; for plain TCP it produces no bytes and the
//!   actual close anchor is the out-of-band shutdown-write the driver issues.
//!   The bridge records that its send half is retired in the [`Self::fin_sent`]
//!   latch and surfaces it through [`Self::fin_owed`]; the coordinator maps that
//!   to a driver shutdown-write / close action.
//!
//! A transport with no record-layer back-pressure (raw passthrough) never
//! returns `Intake::Pending` — it has no crypto buffer to fill — so the
//! retained-ciphertext-tail handling collapses: [`Self::pending_inbound`] is
//! always empty and [`Self::replay_pending`] is the unconditional
//! drain-when-a-`Stream`-exists. The coalesced shape it still covers is a peer
//! that sends `[label][first request]` in one read before the coordinator mints
//! the `Stream`: the record layer strips the label and buffers the first
//! request as inbound plaintext, which the post-promotion drain feeds into the
//! `Stream`.
//!
//! `StreamBridge` is consumed by the unified stream-transport coordinator.

use std::time::Instant;

use crate::{
  bridge_phase::{BridgeFailure, BridgePhase},
  endpoint::Endpoint,
  event::{EndpointEvent, StreamClosed, StreamCommand, StreamErrored},
  stream::Stream,
  streams::{
    phase::StreamPhase,
    transport::{Intake, StreamTransport},
  },
};

/// Couples one reliable-exchange [`Stream`] to one `R: StreamTransport`,
/// pumping plaintext through the record layer both ways. Built `Handshaking`
/// (pre-`Stream`); the coordinator promotes it to `Established` once the
/// record-layer handshake / label step settles and the `Stream` is minted.
/// Accessor-only.
pub(crate) struct StreamBridge<I, A, R> {
  /// `None` until the handshake / label step settles and the coordinator mints
  /// the `Stream`. The `Handshaking` phase is exactly the `stream.is_none()`
  /// window.
  stream: Option<Stream<I, A>>,
  records: R,
  /// `true` once `Stream::poll_transmit` yielded this exchange's output bytes
  /// (so the deferred send-half close is owed). Captures the
  /// inbound-response-not-yet-loaded subtlety: an inbound stream whose response
  /// has not been `stream_load_response`'d also yields `None` from
  /// `poll_transmit` (its phase is `InboundSendingResponse`, not `Done`), so
  /// closing the send half on the first empty poll would half-close before the
  /// reply is written. The owed close is taken only once `poll_transmit` yields
  /// nothing more.
  sent_any: bool,
  /// `true` once this bridge has retired its send half. For a record layer with
  /// an in-band close the latch records that the `close_notify` alert was
  /// queued; for plain TCP it records that our send-half close is owed to the
  /// driver as an out-of-band TCP FIN (a shutdown-write), since
  /// `R::send_close_notify()` emits no in-band bytes there. Distinct from the
  /// `BridgePhase::SendClosed` transition (which records that the send half is
  /// retired): the two move in lockstep, but the latch is still needed because
  /// both `retire_halves` (failure path) and `pump_out` (clean path) may reach
  /// the close call and it must run at most once. Surfaced via [`Self::fin_owed`].
  fin_sent: bool,
  /// The retained record-layer tail. A TLS record layer retains the unconsumed
  /// ciphertext tail when its handshake completes mid-feed (TLS 1.3 coalesces
  /// the final flight with the first app records, and a >16 KiB first frame
  /// trips rustls's received-plaintext backpressure before a `Stream` exists).
  /// A raw-passthrough record layer has no crypto buffer to fill and never
  /// returns `Intake::Pending`, so its `handle_transport_data` always consumes
  /// all input — the coalesced `[label][first request]` is stripped of its
  /// label and surfaced as inbound plaintext in one call, leaving NO
  /// bridge-level tail to retain, and this field stays empty. It is kept so
  /// [`Self::replay_pending`] has one shape across every record layer. The
  /// buffered first request still reaches the `Stream` via `replay_pending`'s
  /// unconditional drain.
  ///
  /// Companion: [`Self::pending_eof`] carries the pre-promote out-of-band FIN
  /// signal across the promote boundary so the post-promotion drain honors it.
  /// Together they form the pre-promote carry-set.
  pending_inbound: Vec<u8>,
  /// Out-of-band transport FIN latched while the bridge is still in
  /// `Handshaking` (pre-promote), so the post-promotion [`Self::replay_pending`]
  /// drain observes it. A TLS record layer carries the peer's close in-band via
  /// its `close_notify` latch (`peer_has_closed()`); plain TCP has no in-band
  /// close signal — the FIN is the driver's `read == 0`, a transport-level event
  /// the record layer cannot observe — so the latch lives on the bridge
  /// instead, keeping the record layer strictly transport-agnostic.
  ///
  /// Set whenever a zero-length [`Self::handle_transport_data`] feed is
  /// delivered while `phase == Handshaking`; read by [`Self::replay_pending`]
  /// to seed `eof` into the post-promotion `pump_in_established` so the
  /// recv-half retirement (or the premature-EOF truncation path) fires on the
  /// SAME tick as the promote rather than waiting for a later transport read
  /// the peer (already half-closed) will never produce. Sticky like
  /// [`Self::fin_sent`] — once latched it is never cleared, so repeated
  /// replays observe the same EOF (idempotent).
  pending_eof: bool,
  phase: StreamPhase,
  /// The exchange deadline, snapshotted from the inner `Stream` at promotion.
  /// During `Handshaking` the deadline is the dial / accept deadline, supplied
  /// by the coordinator, and is the only timer the bridge contributes (no
  /// `Stream` exists yet to fold in via `min`).
  deadline: Instant,
  /// Cross-transport compression configuration. The bridge is the single
  /// compress/decompress point on the reliable path; a disabled
  /// `CompressionOptions` makes the path identity (a plain `[unit_len][bytes]`
  /// frame with the framed bytes verbatim).
  compression: memberlist_wire::CompressionOptions,
  /// Cross-transport encryption configuration. The bridge is the single
  /// encrypt/decrypt point on the reliable path when `R::is_secure() ==
  /// false`. For a secure transport (TLS), the constructor receives a
  /// disabled `EncryptionOptions` regardless of the endpoint configuration —
  /// the per-impl const `is_secure()` branch is optimized away.
  encryption: memberlist_wire::EncryptionOptions,
  /// Inbound reliable-unit accumulation buffer. A byte stream does not
  /// preserve `write_plaintext`/`read_plaintext` boundaries, so each
  /// `read_plaintext` chunk is appended here and every complete
  /// `[unit_len][payload]` unit is drained off the front; a trailing partial
  /// unit stays buffered until the rest arrives.
  recv_accum: Vec<u8>,
  /// Hard ceiling on a reliable unit's on-wire size and its decompressed
  /// payload — the decompression-bomb guard bound, and the cap on the inbound
  /// accumulation buffer (a forged `unit_len` over this is rejected before any
  /// wait). Derived from `EndpointConfig::max_stream_frame_size` so it tracks
  /// the Stream FSM's own configured frame limit rather than a separate
  /// constant.
  reliable_max: usize,
}

impl<I, A, R> StreamBridge<I, A, R>
where
  R: StreamTransport,
  I: memberlist_wire::Id
    + memberlist_wire::Data
    + memberlist_wire::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::Data
    + memberlist_wire::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  /// Build a `Handshaking` bridge wrapping a fresh record layer. The dial /
  /// accept deadline bounds the handshake / label exchange; the `Stream` (and
  /// its own exchange deadline) is installed by [`Self::promote`] once the
  /// handshake / label step settles.
  ///
  /// `reliable_max` is the reliable-unit / decompressed-payload ceiling — the
  /// coordinator passes `EndpointConfig::max_stream_frame_size` so the bound
  /// always matches the Stream FSM's configured frame limit.
  pub(crate) fn new(
    records: R,
    deadline: Instant,
    compression: memberlist_wire::CompressionOptions,
    encryption: memberlist_wire::EncryptionOptions,
    reliable_max: usize,
  ) -> Self {
    // A transport that already provides confidentiality (TLS) forces a
    // disabled `EncryptionOptions` here: double-encrypting on the reliable
    // path costs CPU and bandwidth without adding security, and the peer's
    // bridge will mirror the same skip. The branch is optimized away —
    // `R::is_secure()` is a per-impl const fn.
    let effective_encryption = if R::is_secure() {
      memberlist_wire::EncryptionOptions::new()
    } else {
      encryption
    };
    Self {
      stream: None,
      records,
      sent_any: false,
      fin_sent: false,
      pending_inbound: Vec::new(),
      pending_eof: false,
      phase: StreamPhase::Handshaking,
      deadline,
      compression,
      encryption: effective_encryption,
      recv_accum: Vec::new(),
      reliable_max,
    }
  }

  /// Replace the bridge's effective encryption options. Called by
  /// [`super::StreamEndpoint::set_encryption_options`] when the operator updates
  /// the encryption policy at runtime — the new options propagate to every
  /// live bridge so an attacker cannot keep an exchange opened under the
  /// prior (disabled / different-keyring) policy sending plaintext on the
  /// reliable path.
  ///
  /// Mirrors [`Self::new`]'s `is_secure()`-gated branch: for a secure
  /// transport (`R::is_secure() == true`, e.g. TLS) the stored encryption is
  /// force-disabled regardless of the caller's intent — the reliable path
  /// is already protected by the transport — and the bridge is left running
  /// because its on-wire bytes are TLS records (no plaintext-leak path).
  ///
  /// For an INSECURE transport (`R::is_secure() == false`, e.g. plain TCP)
  /// the bridge MAY hold bytes the FSM already encoded under the prior
  /// policy — queued in the record layer's outbound buffer, or already
  /// drained into the coordinator's [`super::StreamEndpoint`] transmit queue.
  /// Those bytes are plaintext under the old policy; emitting them on the
  /// wire after the operator publishes a new policy would leak plaintext
  /// post-enablement. The bridge therefore stores the new encryption (so the
  /// failure transition's telemetry reflects the policy change) AND fails to
  /// [`BridgePhase::Failed(BridgeFailure::EncryptionPolicyChanged)`]; `fail`
  /// clears the record layer's outbound buffer, and the coordinator's
  /// post-iteration step in [`super::StreamEndpoint::set_encryption_options`]
  /// purges the already-drained chunks from the transmit queue. The SWIM
  /// FSM retries the affected exchange under a fresh bridge that uses the
  /// new policy from construction.
  ///
  /// [`BridgePhase::Failed(BridgeFailure::EncryptionPolicyChanged)`]: BridgePhase::Failed
  pub(crate) fn set_encryption(&mut self, encryption: memberlist_wire::EncryptionOptions) {
    if R::is_secure() {
      self.encryption = memberlist_wire::EncryptionOptions::new();
      return;
    }
    self.encryption = encryption;
    self.fail(BridgeFailure::EncryptionPolicyChanged);
  }

  /// Replace the per-bridge compression options. Takes effect on the
  /// next outbound encode through this bridge's
  /// [`encode_reliable_unit_with_encryption`] call site; in-flight
  /// chunks already in [`Self::poll_transport_transmit`]'s queue are
  /// not re-encoded.
  ///
  /// Unlike [`Self::set_encryption`], no bridge-failure cascade fires
  /// — compression is a non-security codec layer and the wire frame
  /// self-describes its algorithm via the compression-tag prefix, so
  /// a peer always decompresses the bytes it received under whatever
  /// policy was active at the producer's encode time.
  pub(crate) fn set_compression(&mut self, compression: memberlist_wire::CompressionOptions) {
    self.compression = compression;
  }

  /// `true` while the bridge is still gating the `Stream` mint on its record
  /// layer. The acceptor stays handshaking until the inbound label / handshake
  /// is validated; the dialer with no record-layer handshake never does (its
  /// [`StreamTransport::is_handshaking`] is `false` from construction). Once
  /// `!records.is_handshaking()` and the coordinator calls [`Self::promote`]
  /// the bridge moves to `Established`; the phase is still `Handshaking` in
  /// that one-tick window, so both conjuncts gate the pre-`Stream` state.
  pub(crate) fn is_handshaking(&self) -> bool {
    matches!(self.phase, StreamPhase::Handshaking) && self.records.is_handshaking()
  }

  /// Feed bytes the driver read from the transport connection. A zero-length
  /// slice is the transport `read == 0` (peer half-closed the socket) anchor.
  /// Returns `Err(())` if the record layer rejected the bytes (handshake /
  /// label mismatch) — the bridge becomes terminal and the coordinator reaps it
  /// with NO `Stream` minted (during `Handshaking`) or via the `StreamErrored`
  /// D1 path (during `Established`).
  ///
  /// During `Handshaking` a record-layer rejection just makes the bridge
  /// terminal: there is no `Stream` and no FSM queue to clear, and no endpoint
  /// side effect can have happened (this is the handshake / label-mismatch
  /// reject path).
  ///
  /// During `Established` a record-layer rejection routes through the
  /// atomic-retire-then-fail helper (`BridgeFailure::Transport`); plaintext is
  /// drained and fed to `Stream::handle_data` (a decode `Err` →
  /// `BridgeFailure::Decode`); and the close anchor — the transport `read == 0`
  /// EOF, or an in-band close — feeds `Stream::handle_data(&[], now)`. A
  /// non-terminal-phase `Err` (`StreamError::PeerClosed`) is the truncation
  /// path → `BridgeFailure::Decode`; a clean `Ok(())` advances
  /// `observe_recv_fin()` → `RecvClosed`.
  ///
  /// A zero-length feed delivered while `phase == Handshaking` (the pre-promote
  /// window) latches [`Self::pending_eof`] so the post-promotion
  /// [`Self::replay_pending`] honors the out-of-band FIN. Without this latch a
  /// driver delivering `[label||first request]` + the same-tick transport
  /// `read == 0` as two coordinator-level calls — bytes first, then the
  /// empty-slice EOF anchor — would have its EOF dropped (the empty-slice arm
  /// of [`Self::intake_handshaking`] is a no-op while the bridge is still
  /// pre-`Stream`), and the post-promote `pump_in_established` would never
  /// observe the recv-half FIN (`R::peer_has_closed()` is permanently `false`
  /// for a transport whose close is out of band — see [`Self::pending_eof`]).
  pub(crate) fn handle_transport_data(&mut self, data: &[u8], now: Instant) -> Result<(), ()> {
    // Terminal-ingress stop. A bridge that has reached a terminal
    // [`BridgePhase`] — `BothClosed` (clean reap pending) or `Failed(_)`
    // (any failure transition, e.g. an [`BridgeFailure::EncryptionPolicyChanged`]
    // on a runtime [`super::StreamEndpoint::set_encryption_options`] update) —
    // refuses further inbound bytes. Without this guard, network reads
    // delivered between the failure transition and the wake-latch reap can
    // still feed [`Self::pump_in_established`], decode + commit stream events
    // from an exchange already declared dead, and have those events applied
    // to the FSM by [`Self::drain_then_reap`]. The symmetric inbound
    // complement to the outbound queue-purge that the failure transition
    // already runs via `clear_outbound`.
    if self.is_terminal() {
      return Ok(());
    }
    // Pre-`Stream` (handshake / label) window: shuttle bytes until the
    // handshake settles, retaining any tail for post-promotion replay (always
    // empty for a raw-passthrough record layer — see `pending_inbound`). Once
    // promoted, the established intake feeds the record layer and drains the
    // surfaced plaintext into the `Stream`.
    if self.stream.is_none() {
      // Latch a same-tick out-of-band FIN delivered while still pre-`Stream` so
      // the post-promote `replay_pending` honors it (see `pending_eof`). The
      // bridge is still pre-`Stream` here, so the empty-slice intake itself is a
      // no-op and the latch is the only carrier of the EOF signal across the
      // promote boundary. Robust to ordering: a coalesced `[label||first request]`
      // delivered as `(bytes, eof=true)` reaches this branch with
      // `phase == Handshaking` regardless of whether `bytes` or `&[]` arrives
      // first (the empty-slice second call latches; the first call surfaces the
      // request as inbound plaintext, leaving the bridge still pre-`Stream` since
      // the mint runs in `service_handshake_completions`).
      if data.is_empty() {
        self.pending_eof = true;
      }
      return self.intake_handshaking(data, now);
    }

    // Established: a real transport read (a zero-length slice is the transport
    // `read == 0` EOF anchor). A non-empty retained pre-promotion tail is
    // replayed FIRST, ahead of the newly-supplied data, so the request
    // reassembles in wire order. (`pending_inbound` is always empty for a
    // raw-passthrough record layer, so the second arm is vestigial there.)
    let eof = data.is_empty();
    if self.pending_inbound.is_empty() {
      self.pump_in_established(data, eof, now)
    } else {
      let mut combined = std::mem::take(&mut self.pending_inbound);
      combined.extend_from_slice(data);
      self.pump_in_established(&combined, eof, now)
    }
  }

  /// Pre-`Stream` intake. Feeds bytes into the record layer until either all
  /// supplied input is consumed or the handshake / label step settles mid-feed.
  ///
  /// A peer may coalesce its `[label]` prefix with its first application
  /// request into ONE transport read: `R::handle_transport_data` then settles
  /// the label (`is_handshaking()` flips false) AND surfaces the trailing
  /// request bytes as inbound plaintext in the SAME call. A raw-passthrough
  /// record layer has no backpressure, so it consumes all input in one pass and
  /// the bridge retains NO tail (`pending_inbound` stays empty); the buffered
  /// request is drained into the just-minted `Stream` by [`Self::replay_pending`]
  /// post-promotion.
  ///
  /// An [`Intake::Failed`] is a handshake / label rejection. During
  /// `Handshaking` it just terminalizes the bridge (no `Stream`, no queue, no
  /// endpoint side effect — the handshake / label-mismatch reject path).
  fn intake_handshaking(&mut self, data: &[u8], now: Instant) -> Result<(), ()> {
    let mut offset = 0usize;
    loop {
      let intake = self.records.handle_transport_data(&data[offset..], now);
      let consumed = match intake {
        // A raw-passthrough record layer consumes all supplied input in one
        // call; a backpressured one consumes a prefix and back-pressures.
        Intake::Done => {
          offset = data.len();
          true
        }
        // A bounded-progress step — `n` bytes consumed before the record layer
        // back-pressured.
        Intake::Pending(n) => {
          offset += n;
          n > 0
        }
        // Handshake / label rejection: terminalize with NO `Stream` minted.
        Intake::Failed => {
          self.fail(BridgeFailure::Transport(
            "stream record rejected".to_string(),
          ));
          return Err(());
        }
      };

      // The handshake / label step just settled inside this feed: any trailing
      // request is already surfaced as inbound plaintext inside the record
      // layer (drained by `replay_pending` post-promotion), unless the record
      // layer back-pressured and left a ciphertext tail this bridge must
      // retain for post-promotion replay.
      if !self.records.is_handshaking() {
        if offset < data.len() {
          self.pending_inbound.extend_from_slice(&data[offset..]);
        }
        return Ok(());
      }

      // Still settling the handshake / label (a partial header was buffered):
      // keep shuttling. The `consumed` guard bounds the loop; an `Intake::Done`
      // that left us still handshaking means the partial header was buffered in
      // full.
      if matches!(intake, Intake::Done) || !consumed {
        return Ok(());
      }
    }
  }

  /// Established intake: feed `input` through the record layer in bounded steps,
  /// draining surfaced plaintext into the `Stream` between steps, then map the
  /// close anchor. `eof` is the transport `read == 0` marker (a zero-length
  /// real transport read); a retained-tail replay passes `eof = false` (the
  /// tail is buffered data, not an end-of-stream).
  ///
  /// A raw-passthrough record layer has no decrypted-plaintext buffer to fill
  /// and never returns `Intake::Pending`, so a single read is consumed in one
  /// pass — the bounded-interleave loop collapses to one iteration there. The
  /// loop shape (each iteration makes progress or breaks) holds for a
  /// backpressured record layer too.
  fn pump_in_established(&mut self, input: &[u8], eof: bool, now: Instant) -> Result<(), ()> {
    // Defense-in-depth terminal guard. The outermost
    // [`Self::handle_transport_data`] entry already short-circuits a terminal
    // bridge, and [`Self::replay_pending`] only runs at promote time. Guard
    // here too so any future caller cannot feed bytes through this path
    // post-failure (e.g. a same-tick decode failure during a multi-step
    // intake loop must not re-enter and commit further stream events).
    if self.is_terminal() {
      return Ok(());
    }
    let mut offset = 0usize;
    let mut decode_failed = false;
    loop {
      // An `Intake::Failed` is a handshake / label rejection — never produced
      // once the handshake / label step has settled (this is the `Established`
      // path), but routed through the atomic-retire-then-fail path defensively
      // so no half-applied merge / ack survives an aborted exchange.
      let intake = self.records.handle_transport_data(&input[offset..], now);
      let consumed = match intake {
        Intake::Done => {
          offset = input.len();
          true
        }
        // The carried count is bytes consumed before backpressure.
        Intake::Pending(n) => {
          offset += n;
          n > 0
        }
        Intake::Failed => {
          self.fail_with_retire(BridgeFailure::Transport(
            "stream record rejected".to_string(),
          ));
          return Err(());
        }
      };

      // Append the record layer's surfaced plaintext to the reliable-unit
      // accumulator, then drain every COMPLETE `[unit_len][payload]` unit into
      // the FSM. A trailing partial unit stays buffered for the next read.
      let mut surfaced = Vec::new();
      let drained = self.records.read_plaintext(&mut surfaced);
      self.recv_accum.extend_from_slice(&surfaced);
      loop {
        match memberlist_wire::take_reliable_unit_with_encryption(
          &self.recv_accum,
          &self.encryption,
          self.reliable_max,
        ) {
          Ok(Some((plaintext, consumed))) => {
            self.recv_accum.drain(..consumed);
            if self
              .stream
              .as_mut()
              .expect("stream is Some in the established intake")
              .handle_data(&plaintext, now)
              .is_err()
            {
              decode_failed = true;
              break;
            }
          }
          // Need more bytes — keep the partial and wait for the next read.
          Ok(None) => break,
          // A corrupt unit (bad inner wrapper, or an over-ceiling `unit_len`):
          // terminalize the exchange.
          Err(_) => {
            self.fail_with_retire(BridgeFailure::Decode);
            return Err(());
          }
        }
      }

      // Terminate once all input is consumed; a decode failure also stops the
      // feed (the exchange is being torn down). Otherwise, if this step made no
      // progress at all, break to honor the bounded-work contract.
      if matches!(intake, Intake::Done) || decode_failed {
        break;
      }
      if !consumed && drained == 0 {
        break;
      }
    }

    // Close anchor. A transport `read == 0` is the EOF marker; `records.peer_has_closed()`
    // additionally carries a record layer's in-band close (always `false` for a
    // transport whose close is out of band, so `eof` is the sole driver there):
    // feed `Stream::handle_data(&[], now)`, the FSM's per-phase
    // premature-vs-clean EOF decision. A premature EOF
    // (`StreamError::PeerClosed`) is the truncation path → decode failure; a
    // clean EOF (`Ok`) retires the recv half below via `observe_recv_fin`.
    let fin_seen = eof || self.records.peer_has_closed();
    if fin_seen && !decode_failed && !self.recv_accum.is_empty() {
      // A trailing partial reliable unit at EOF is a truncated transmission —
      // the peer closed mid-unit. Treat it as a decode failure (the same
      // outcome the FSM's premature-EOF path produces for a half frame).
      self.fail_with_retire(BridgeFailure::Decode);
      return Err(());
    }
    if fin_seen
      && !decode_failed
      && self
        .stream
        .as_mut()
        .expect("stream is Some in the established intake")
        .handle_data(&[], now)
        .is_err()
    {
      decode_failed = true;
    }

    if decode_failed {
      // Atomic failure: retire our half + discard buffered inbound plaintext
      // BEFORE flipping the phase, so a same-tick reap cannot leak a
      // half-applied exchange. The FSM's failure variant is preserved in
      // `Stream::is_failed()`; `drain_then_reap`'s `StreamErrored` notice uses
      // the `BridgeFailure::Decode` high-level reason.
      self.fail_with_retire(BridgeFailure::Decode);
      return Err(());
    }

    // Only after the FSM accepted the EOF (clean phase, or already terminal)
    // do we retire the recv half. A premature-EOF rejection routed through the
    // decode-fail path above before reaching here, so `RecvClosed` is the
    // FSM-blessed recv-half-retired transition.
    if fin_seen {
      self.observe_recv_fin();
    }

    Ok(())
  }

  /// Drain any plaintext the record layer buffered pre-promotion through the
  /// established intake, SAME tick as the promotion. The coordinator drives this
  /// from its post-mint bridge pump (after [`Self::promote`]) so a first request
  /// coalesced with the peer's `[label]` prefix reaches the just-minted `Stream`
  /// without waiting for the next transport read.
  ///
  /// A TLS record layer distinguishes a retained large-frame ciphertext tail
  /// from a small frame rustls already buffered; a raw-passthrough record layer
  /// has no backpressure and so no retained tail (`pending_inbound` stays
  /// empty — see its docs). The drain still runs whenever a `Stream` exists,
  /// NOT only when a tail was retained: a peer that coalesced
  /// `[label][first request]` had that request stripped of its label and
  /// buffered as inbound plaintext inside the record layer while the bridge was
  /// still `Handshaking`; feeding an EMPTY slice through
  /// [`Self::pump_in_established`] drains that buffered plaintext into the
  /// `Stream`. Gating on a non-empty tail would strand it until a later
  /// transport read that a request-awaiting-reply peer will never send.
  ///
  /// The replay seeds `eof` from the [`Self::pending_eof`] latch: a same-tick
  /// out-of-band FIN that arrived while the bridge was still `Handshaking` is
  /// honored here, so a coalesced `[label||first request]||FIN` delivery
  /// terminalizes on the same tick as the promote rather than stalling to the
  /// exchange deadline. A peer that has NOT yet half-closed leaves the latch
  /// `false`, so a push/pull request awaiting its response is not prematurely
  /// signaled EOF. The latch is sticky (never cleared), mirroring
  /// [`Self::fin_sent`] on the send side: `replay_pending` is idempotent, so a
  /// subsequent same-tick call observes the same EOF and the FSM's already-EOF
  /// state is a no-op. Returns `Err(())` if the drain terminalized the bridge
  /// (decode / record failure), mirroring [`Self::handle_transport_data`].
  pub(crate) fn replay_pending(&mut self, now: Instant) -> Result<(), ()> {
    if self.stream.is_none() {
      return Ok(());
    }
    // A terminal bridge — e.g. one failed at promote time by a policy change
    // delivered between the handshake-settling tick and the post-mint replay
    // — must not surface its retained pre-promote plaintext into the
    // just-minted `Stream`. The retained tail (if any) is dropped along
    // with the bridge by the next `pump_bridges` reap.
    if self.is_terminal() {
      return Ok(());
    }
    // Feed the retained tail (if any) FIRST, then drain. `std::mem::take`
    // clears `pending_inbound` (always empty for a raw-passthrough record
    // layer); an empty tail still drains the buffered surfaced plaintext and
    // honors the close anchor.
    let combined = std::mem::take(&mut self.pending_inbound);
    self.pump_in_established(&combined, self.pending_eof, now)
  }

  /// Drain bytes the record layer wants to write into `out`. Returns the byte
  /// count appended. Used by the coordinator's `collect_transmits`.
  pub(crate) fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    self.records.poll_transport_transmit(out)
  }

  /// Promote a `Handshaking` bridge to `Established` with the freshly-minted
  /// `Stream`. Snapshots the `Stream`'s exchange deadline as the bridge deadline
  /// (a freshly dialed/accepted `Stream` is pre-`Done` with a `Some` deadline).
  pub(crate) fn promote(&mut self, stream: Stream<I, A>) {
    self.deadline = stream
      .poll_timeout()
      .expect("a freshly dialed/accepted Stream is pre-`Done` with a Some exchange deadline");
    self.stream = Some(stream);
    self.phase = StreamPhase::Established(BridgePhase::Active);
  }

  /// Drive the SEND-half transition: `Active → SendClosed`, or
  /// `RecvClosed → BothClosed`. No-op for already-`Failed`/-terminal states and
  /// during `Handshaking` (no `Stream` yet). Terminal states are sticky.
  ///
  /// Anchor: our send-half close, recorded by `R::send_close_notify()` — a
  /// `close_notify` alert for a record layer with an in-band close, or the
  /// no-op whose real anchor is the out-of-band TCP FIN owed to the driver via
  /// [`Self::fin_owed`].
  fn observe_send_fin(&mut self) {
    let StreamPhase::Established(bp) = &mut self.phase else {
      return;
    };
    *bp = match bp {
      BridgePhase::Active => BridgePhase::SendClosed,
      BridgePhase::RecvClosed => BridgePhase::BothClosed,
      BridgePhase::SendClosed | BridgePhase::BothClosed | BridgePhase::Failed(_) => return,
    };
  }

  /// Drive the RECV-half transition: `Active → RecvClosed`, or
  /// `SendClosed → BothClosed`. No-op for already-`Failed`/-terminal states and
  /// during `Handshaking`.
  ///
  /// Anchor: a clean `Stream::handle_data(&[], now)` after the peer's transport
  /// `read == 0` or in-band close.
  fn observe_recv_fin(&mut self) {
    let StreamPhase::Established(bp) = &mut self.phase else {
      return;
    };
    *bp = match bp {
      BridgePhase::Active => BridgePhase::RecvClosed,
      BridgePhase::SendClosed => BridgePhase::BothClosed,
      BridgePhase::RecvClosed | BridgePhase::BothClosed | BridgePhase::Failed(_) => return,
    };
  }

  /// Idempotent retirement of both halves: record the send-half close (latched
  /// against a second call) and discard any surfaced-but-unconsumed inbound
  /// plaintext. Used by every failure transition, so a `BridgePhase::Failed`
  /// reap has the send half closed AND no buffered inbound plaintext by
  /// construction. For plain TCP `R::send_close_notify()` is a no-op (the TCP
  /// FIN is the out-of-band shutdown-write the driver issues once it sees
  /// [`Self::fin_owed`]) and the latch is what records that our send half is
  /// retired; for a record layer with an in-band close it queues the
  /// `close_notify` alert. A failed FSM's `enter_failed` clears its own
  /// `output_buf`, so the only bridge-reachable buffer to clear is the inbound
  /// plaintext the record layer surfaced but we have not yet fed to the
  /// `Stream`.
  fn retire_halves(&mut self) {
    if !self.fin_sent {
      self.records.send_close_notify();
      self.fin_sent = true;
    }
    let mut discard = Vec::new();
    self.records.read_plaintext(&mut discard);
  }

  /// Atomic FAILURE transition: retire both halves AND set
  /// `BridgePhase::Failed(reason)` in one step. The `is_terminal()` predicate is
  /// phase-authoritative, so a bridge that becomes `Failed` must already have its
  /// send half closed — otherwise a same-tick reap leaves the driver without the
  /// FIN signal.
  ///
  /// Sticky — first failure wins; subsequent calls preserve the original cause
  /// for `drain_then_reap`'s `StreamErrored` notice. Idempotent retirement still
  /// runs (cheap, latched) so a recovery race that observes a prior `Failed`
  /// phase cannot leave the send half un-retired.
  fn fail_with_retire(&mut self, reason: BridgeFailure) {
    self.retire_halves();
    self.fail(reason);
  }

  /// FAILURE transition without retiring halves.
  ///
  /// Pre-FIN side-effect hygiene: if a `Stream` exists and the recv half has
  /// NOT yet been retired by peer-FIN observation (phase ∉ `RecvClosed` /
  /// `BothClosed`), discard the FSM's queued endpoint / lifecycle events. The
  /// `drain_payload_only` deferred-commit gate holds these events while the
  /// bridge is non-terminal pending peer FIN as protocol-level proof the
  /// exchange completed; `drain_then_reap` (the terminal D1 path) drains the
  /// queue unconditionally. Without this pre-FIN clear, a peer that sends one
  /// complete frame and then drops the connection BEFORE the clean EOF would
  /// have its dispatched frame's side effects committed by `drain_then_reap`
  /// even though the exchange was never authorized. Mirrors
  /// `Stream::enter_failed`'s queue clearing on the FSM-failure path — same
  /// atomicity, applied at the transport-failure boundary. A `Handshaking`
  /// failure has no `Stream` and no queue, so the clear is skipped (the
  /// handshake / label-mismatch reject path). `RecvClosed` / `BothClosed`
  /// failures preserve the queue: the clean EOF already authorized the
  /// dispatched events; the failure is orthogonal.
  fn fail(&mut self, reason: BridgeFailure) {
    if matches!(self.phase, StreamPhase::Established(BridgePhase::Failed(_))) {
      return;
    }
    if !matches!(
      self.phase,
      StreamPhase::Established(BridgePhase::RecvClosed | BridgePhase::BothClosed)
    ) {
      if let Some(stream) = self.stream.as_mut() {
        stream.discard_pending_events();
      }
    }
    // Drop any bytes queued by the record layer — the dialer's eager label
    // prefix (queued at construction by the dialer), any application bytes the
    // FSM had written into the record layer before this tick's failure, and an
    // acceptor's lazy label prefix if it had already validated its inbound
    // label (lazy queue fired in `R::handle_transport_data`'s accepted branch)
    // and then failed mid-reply. A pre-validation acceptor failure
    // (`intake_handshaking` label-mismatch / handshake-deadline slow-loris)
    // sees an empty outbound buffer because the lazy queue had not yet fired,
    // so the clear is a no-op on those paths — the acceptor's lazy queue is the
    // primary guard against pre-validation label disclosure; this clear is the
    // symmetric mid-exchange / dialer guard. A `Failed` bridge has no business
    // retaining outbound bytes: the coordinator's reap path drains the record
    // layer's outbound into `out_transmit` via `collect_bridge_transmits` AFTER
    // `purge_transmit_for` has dropped any already-drained chunks tagged with
    // the exchange. Placing the clear here covers EVERY failure transition
    // uniformly — `fail_with_retire` reaches `fail` via its own atomic
    // retire-then-fail step. Clean closes go through `pump_out` /
    // `observe_send_fin` and never call `fail`, so a same-tick clean half-close
    // cannot be observed clearing outbound here. For a record layer that owns
    // its own write queue (rustls) `R::clear_outbound` is a documented no-op;
    // the call stays uniform either way.
    self.records.clear_outbound();
    self.phase = StreamPhase::Established(BridgePhase::Failed(reason));
  }

  /// `true` once this bridge has retired its send half (clean `pump_out`
  /// half-close or a failure-path `retire_halves`) and therefore OWES the driver
  /// an out-of-band transport FIN (a shutdown-write on the connection's write
  /// side).
  ///
  /// The coordinator reads this to emit one shutdown-write per exchange after
  /// the final flush — the signal that lets the peer read a clean EOF (its
  /// `read == 0`) once it has drained our buffered bytes. The coordinator maps
  /// it to a `Shutdown` action; for a record layer with an in-band close the
  /// `close_notify` record is the wire signal and the shutdown-write is the
  /// transport-level companion, while for plain TCP the send-half close is
  /// out-of-band (the TCP FIN) and no bytes are emitted by the record layer for
  /// it.
  pub(crate) fn fin_owed(&self) -> bool {
    self.fin_sent
  }

  /// `true` iff the bridge is in [`BridgePhase::Failed`] — observable shorthand
  /// for the `pump_out` leading-fatal guard and for `drain_then_reap`'s
  /// lifecycle-notice selection. Distinct from `Stream::is_failed()` (FSM-level
  /// failure, which cascades into the bridge via the failure transitions).
  fn is_phase_failed(&self) -> bool {
    matches!(self.phase, StreamPhase::Established(BridgePhase::Failed(_)))
  }

  /// `true` iff this bridge has reached a failed terminal phase
  /// ([`BridgePhase::Failed`]). The coordinator uses this to distinguish a
  /// failed reap (stale pre-failure outbound chunks must be purged from
  /// `out_transmit` so they do not leak after the bridge is torn down) from a
  /// clean [`BridgePhase::BothClosed`] reap (response chunks queued by earlier
  /// pumps are legitimate wire bytes that must reach the peer).
  ///
  /// A `Handshaking` bridge failure (handshake / label rejection / deadline
  /// timeout) ALSO transitions to `Established(Failed(_))` — there is no
  /// distinct `Handshaking(Failed)` variant — so this predicate covers every
  /// failure transition (label-mismatch, handshake-deadline, dial-deadline,
  /// decode-fail, transport reset). Both [`Self::is_terminal`] (which also
  /// returns `true` for `BothClosed`) and `Stream::is_failed()` (which inspects
  /// the inner FSM's terminal error, independent of the bridge phase) are
  /// deliberately separate observations.
  pub(crate) fn is_failed(&self) -> bool {
    self.is_phase_failed()
  }

  /// The bridge's flush/lifetime deadline, folded into the unified `min` across
  /// active bridge / Endpoint timers in the coordinator's `poll_timeout`.
  ///
  /// During `Handshaking` (no `Stream`) this is the dial / accept deadline — the
  /// only timer bounding the handshake / label exchange. Once `Established`,
  /// returns the bridge's OWN snapshotted `deadline` `min` the inner stream's
  /// `poll_timeout()` (which goes `None` the moment `Stream::poll_transmit`
  /// flips an inbound-response / one-way-user-message phase to `Done`).
  /// Delegating solely to the inner stream would drop a `Done`-but-awaiting-peer-FIN
  /// bridge out of the unified timer, so a peer that never closes its send half
  /// would pin the bridge forever; the bridge deadline is therefore the
  /// lifetime authority, while a tighter inner deadline (e.g. the
  /// inbound-response `now + 5s`) is folded in via `min`. A terminal bridge is
  /// reaped this same tick and contributes no deadline.
  pub(crate) fn poll_timeout(&self) -> Option<Instant> {
    if self.is_terminal() {
      return None;
    }
    Some(match self.stream.as_ref().and_then(|s| s.poll_timeout()) {
      Some(inner) => inner.min(self.deadline),
      None => self.deadline,
    })
  }

  /// Mark the bridge failed because its dial intent was retired by the
  /// inner endpoint before a `Stream` could be minted (deadline elapsed
  /// in `Endpoint::dial_succeeded`, or an external `dial_failed` cleared
  /// the intent). Like every failure transition this runs
  /// `records.clear_outbound()` via `fail`; the coordinator's
  /// `dial_succeeded(None)` reap path additionally purges the transmit
  /// queue and the pending `Connect` before dropping the bridge.
  pub(crate) fn fail_dial_retired(&mut self) {
    self.fail(BridgeFailure::DialRetired);
  }

  /// Pump outbound memberlist bytes through the record layer.
  ///
  /// Handshake-deadline guard FIRST, before the no-`Stream` early return: a
  /// `Handshaking` bridge has no `Stream` and no FSM timer, so nothing else
  /// terminalizes it when its dial / accept budget elapses without the
  /// handshake / label step settling (the empty-slice EOF anchor is a no-op,
  /// and the `is_done()`-gated flush-deadline path below needs a `Stream`). If
  /// `now >= self.deadline` while still `Handshaking`, fail to `Failed(Timeout)`
  /// with NO `Stream` minted so the coordinator's pre-`Stream` reap collects it
  /// — bounding a connect-refused / peer-silent dial AND a slowloris accept-side
  /// label stall. The `fail` transition has no queue to clear during
  /// `Handshaking`, matching the handshake / label-mismatch reject path.
  ///
  /// Terminal-bridge guard NEXT: if a prior `handle_transport_data` made the
  /// bridge terminal this tick, or the inner `Stream` entered `Failed` on a
  /// previous tick (D1 reap still pending), every write path below must be
  /// unreachable — writing newly-yielded bytes would deliver a stale
  /// request/response (memberlist frames are length-delimited, so the peer
  /// applies a partial wire as a full message). The guard retires the halves,
  /// flips the phase to `Failed(Transport)`, and returns `Err(())`.
  ///
  /// The bridge-level flush deadline covers the `Done`-but-awaiting-peer-close
  /// gap: once `Stream::poll_transmit` flips a phase to `Done`, the FSM's
  /// `poll_timeout` is `None` and it can no longer self-enforce; if the bridge
  /// is not yet completion-terminal and `now >= self.deadline`, abandon the
  /// exchange (`Failed(Timeout)`) so the coordinator reaps it rather than leaking
  /// a bridge whose peer never closed its send half.
  ///
  /// The send half is closed exactly once (`fin_sent` guards a second call), on
  /// the first tick EITHER the memberlist `Stream` emitted its full
  /// request/response (`sent_any` — so a mid-exchange outbound request
  /// half-closes its send side *before* the stream is `Done`, so the peer can
  /// reply) OR the inner `Stream` is `Done` (so an inbound one-way
  /// `Message::UserData`, which reaches `Done` with NO outbound bytes and never
  /// arms `sent_any`, still half-closes and becomes reapable instead of leaking
  /// the bridge). It is never taken on an empty `poll_transmit` whose response
  /// buffer is merely not yet `stream_load_response`'d (that phase is
  /// `InboundSendingResponse`, not `Done`, with `sent_any` false), and it is
  /// gated against `Stream::is_failed()` / a failed phase so a timed-out or
  /// transport-errored stream cannot half-close cleanly on top of a failure.
  pub(crate) fn pump_out(&mut self, now: Instant) -> Result<(), ()> {
    // Handshake-deadline guard. A `Handshaking` bridge (no `Stream`, no FSM
    // timer) is otherwise never terminalized when its deadline elapses without
    // the handshake / label step settling; fail it to `Failed(Timeout)` so the
    // coordinator's pre-`Stream` reap collects it. Runs before the no-`Stream`
    // early return, and only while `Handshaking` so the `Established` flush
    // deadline (the `is_done()`-gated path below) is unchanged.
    if matches!(self.phase, StreamPhase::Handshaking) && now >= self.deadline {
      self.fail(BridgeFailure::Timeout);
      return Err(());
    }

    if self.stream.is_none() {
      return Ok(());
    }

    // Terminal-bridge guard. A failed phase, or an FSM-failed inner stream,
    // means no further bytes may be written. Retire + fail atomically (the
    // failure transition is phase-authoritative) and return `Err`.
    let fsm_failed = self
      .stream
      .as_ref()
      .expect("stream is Some")
      .is_failed()
      .map(|e| e.to_string());
    if self.is_phase_failed() || fsm_failed.is_some() {
      let reason = match fsm_failed {
        Some(e) => BridgeFailure::Transport(e),
        None => BridgeFailure::Transport("bridge fatal".to_string()),
      };
      self.fail_with_retire(reason);
      return Err(());
    }

    // Bridge-level flush deadline — the `Done`-but-awaiting-peer-close gap.
    // The inner stream can no longer self-enforce its deadline once `Done`
    // (`poll_timeout` is `None`); enforce the bridge deadline so a peer that
    // never closes its send half cannot pin the bridge.
    if now >= self.deadline
      && self.stream.as_ref().expect("stream is Some").is_done()
      && !self.is_terminal()
    {
      self.fail_with_retire(BridgeFailure::Timeout);
      return Err(());
    }

    // Gather every `poll_transmit` yield from this drain into one buffer,
    // then write it as ONE self-delimiting reliable unit. A byte stream does
    // not preserve write/read boundaries, so framing each drain as one
    // `[unit_len][payload]` unit lets the peer re-delimit it regardless of
    // how the transport chunks the bytes. The `poll_transmit` borrow on
    // `stream` ends before the `write_plaintext` borrow on `records` begins.
    let mut gathered = Vec::new();
    let mut chunk = Vec::new();
    loop {
      chunk.clear();
      let yielded = self
        .stream
        .as_mut()
        .expect("stream is Some")
        .poll_transmit(now, &mut chunk)
        .is_some();
      if !yielded {
        break;
      }
      // `poll_transmit` yielded this exchange's output: the send half now
      // owes its close.
      self.sent_any = true;
      gathered.extend_from_slice(&chunk);
    }
    if !gathered.is_empty() {
      // An encryption backend error here (e.g. a primary key whose backend
      // feature was not built into this binary) is fatal — emitting plaintext
      // on the wire would silently bypass authentication on an
      // encrypted-cluster reliable exchange. Atomically retire-then-fail.
      let unit = match memberlist_wire::encode_reliable_unit_with_encryption(
        &self.compression,
        &self.encryption,
        &gathered,
      ) {
        Ok(u) => u,
        Err(e) => {
          self.fail_with_retire(BridgeFailure::Transport(format!("encrypt: {e}")));
          return Err(());
        }
      };
      self.records.write_plaintext(&unit);
    }

    // FSM-failure detection AFTER `poll_transmit`: the FSM checks its own
    // deadline inside `poll_transmit` and transitions to `Failed(Timeout)` if
    // elapsed (yielding nothing). Mirror it into `BridgePhase::Failed` + retire
    // atomically so the next tick reaps a fully-retired bridge.
    let fsm_failed = self
      .stream
      .as_ref()
      .expect("stream is Some")
      .is_failed()
      .map(|e| e.to_string());
    if let Some(e) = fsm_failed {
      self.fail_with_retire(BridgeFailure::Transport(format!("stream failed: {e}")));
      return Err(());
    }

    // Close the send half exactly once. Reaching here guarantees `poll_transmit`
    // is exhausted, so an inbound response pre-`stream_load_response` (phase
    // `InboundSendingResponse`, `is_done()` false, `sent_any` false) does not
    // half-close before the reply is written. A failed / fatal stream MUST NOT
    // half-close cleanly — the gate forbids it.
    let done = self.stream.as_ref().expect("stream is Some").is_done();
    let failed = self
      .stream
      .as_ref()
      .expect("stream is Some")
      .is_failed()
      .is_some();
    if !self.fin_sent && !self.is_phase_failed() && !failed && (self.sent_any || done) {
      self.records.send_close_notify();
      self.fin_sent = true;
      self.observe_send_fin();
    }

    Ok(())
  }

  /// `true` once the bridge has reached a terminal [`BridgePhase`] — either
  /// `BothClosed` (clean: both halves retired) or `Failed` (any failure path).
  /// The coordinator then stops pumping and runs [`Self::drain_then_reap`].
  ///
  /// Phase is the SINGLE source of truth. The failure transitions retire the
  /// send half and discard buffered inbound plaintext atomically BEFORE flipping
  /// the phase, so a `Failed`-phase bridge is retired by construction. No
  /// `Stream::is_failed()` fallback: every FSM-internal failure is observed by
  /// `pump_out` / `handle_transport_data` and cascaded into `Failed` via the
  /// atomic-retire-then-fail helpers; falling back to `Stream::is_failed()` for
  /// terminality would let a reap fire before the retirement step.
  pub(crate) fn is_terminal(&self) -> bool {
    matches!(
      self.phase,
      StreamPhase::Established(BridgePhase::BothClosed | BridgePhase::Failed(_))
    )
  }

  /// D1 drain-before-reap. In strict order:
  ///
  /// 1. drain every queued [`Stream::poll_endpoint_event`] into the
  ///    [`Endpoint`] (the push/pull reply / `ReliablePingAcked` lives here);
  /// 2. route each returned [`StreamCommand`] back to this stream
  ///    (`SendPushPullResponse` -> encode + load + flush our reply;
  ///    `Close` -> fail);
  /// 3. **only then** deliver the `StreamClosed` / `StreamErrored` lifecycle
  ///    notice.
  ///
  /// The caller removes the bridge **only after** this returns. The order is
  /// load-bearing: delivering the lifecycle notice before the payload events
  /// makes the `Endpoint` tear the exchange down before the join reply /
  /// reliable-ping ack is applied. A `Handshaking` bridge has no `Stream` and no
  /// events, so this is a no-op (the coordinator reaps a failed-label bridge
  /// directly).
  pub(crate) fn drain_then_reap(&mut self, ep: &mut Endpoint<I, A>, now: Instant) {
    if self.stream.is_none() {
      return;
    }
    while let Some(ev) = self
      .stream
      .as_mut()
      .expect("stream is Some")
      .poll_endpoint_event()
    {
      if let Some(cmd) = ep.handle_stream_event(ev, now) {
        match cmd {
          StreamCommand::SendPushPullResponse(resp) => {
            let (local_states, user_data) = resp.into_parts();
            // `handle_stream_event` returns the response state UNENCODED with
            // the inbound stream's `output_buf` still empty: encode the snapshot
            // and load it before any of it can be transmitted, or the peer is
            // left with a half-applied merge (split-brain). The bridge-level
            // `self.deadline` is advanced to the SAME `now + 5s` value
            // `stream_load_response` writes into the inner stream so the
            // `Done`-but-awaiting-peer-close abandon does not fire on the stale
            // accept deadline before the fresh response window elapses.
            let response_deadline = now + core::time::Duration::from_secs(5);
            let encoded =
              Endpoint::<I, A>::encode_push_pull_response(&local_states, user_data, false);
            Endpoint::<I, A>::stream_load_response(
              self.stream.as_mut().expect("stream is Some"),
              encoded,
              response_deadline,
            );
            self.deadline = response_deadline;
            // Ignoring Err: `pump_out` failing here terminalizes the bridge,
            // which the lifecycle-notice selection below reflects as
            // `StreamErrored` — there is no separate action to take.
            let _ = self.pump_out(now);
          }
          StreamCommand::Close => {
            // Admission rejection (`MergeDelegate::notify_merge -> false`).
            // Atomic failure → the lifecycle notice surfaces as `StreamErrored`
            // carrying the rejection.
            self.fail_with_retire(BridgeFailure::AdmissionClosed);
          }
        }
      }
    }

    // Recv-half retirement is structurally guaranteed by [`BridgePhase`]:
    //   * `BothClosed` — `observe_recv_fin` already fired on the clean EOF.
    //   * `Failed(_)` — the failure transition retired the send half and
    //     discarded buffered inbound plaintext; the read side is retired by
    //     ceasing reads.
    // An adversarial peer that never closes its send half does not produce
    // `BothClosed` — the bridge stays non-terminal until `self.deadline`
    // elapses, at which point `pump_out`'s flush-deadline path transitions to
    // `Failed(Timeout)`.
    let id = self.stream.as_ref().expect("stream is Some").id();
    let fsm_failed = self.stream.as_ref().expect("stream is Some").is_failed();
    let notice = match (&self.phase, fsm_failed) {
      (StreamPhase::Established(BridgePhase::Failed(reason)), _) => {
        let err = match reason {
          BridgeFailure::Timeout => "exchange deadline elapsed".to_string(),
          BridgeFailure::Transport(s) => format!("transport: {s}"),
          BridgeFailure::Decode => "decode failed".to_string(),
          BridgeFailure::ConnectionLost => "connection lost".to_string(),
          BridgeFailure::AdmissionClosed => "merge rejected by delegate".to_string(),
          BridgeFailure::DialRetired => "dial intent retired before stream".to_string(),
          BridgeFailure::EncryptionPolicyChanged => {
            "encryption policy changed mid-exchange".to_string()
          }
        };
        EndpointEvent::StreamErrored(StreamErrored::new(id, err))
      }
      (_, Some(e)) => EndpointEvent::StreamErrored(StreamErrored::new(id, e.to_string())),
      _ => EndpointEvent::StreamClosed(StreamClosed::new(id)),
    };
    // Ignoring Err: the post-payload lifecycle notice
    // (`StreamClosed`/`StreamErrored`) returns no actionable `StreamCommand` —
    // the bridge is being reaped this tick and no outbound work can run after
    // the notice.
    let _ = ep.handle_stream_event(notice, now);
  }

  /// Drain a **non-terminal** stream's queued [`Stream::poll_endpoint_event`]
  /// into the [`Endpoint`] every tick, routing each returned [`StreamCommand`]
  /// with the **same** handling as [`Self::drain_then_reap`] — but **without**
  /// the post-loop lifecycle notice.
  ///
  /// Gated on `BridgePhase::RecvClosed`: a decoded frame's side effects
  /// (`PushPullRequestReceived`, `ReliablePingAcked`, `UserDataReceived`, …)
  /// queue on dispatch BEFORE the stream proves there are no trailing bytes.
  /// Holding the commit until the recv half observes the clean EOF means an
  /// adversarial `[valid_frame][trailing junk]` split across ticks fails the
  /// stream (via `enter_failed`, which clears the queue) before the chunk-1 side
  /// effects are ever committed. The encode + load for an inbound push/pull
  /// response still runs here, though: without it the reply for an in-flight
  /// inbound exchange is never produced and the peer is left with a half-applied
  /// merge. Mirrors [`Self::drain_then_reap`] minus the lifecycle notice.
  pub(crate) fn drain_payload_only(&mut self, ep: &mut Endpoint<I, A>, now: Instant) {
    if !matches!(
      self.phase,
      StreamPhase::Established(BridgePhase::RecvClosed)
    ) {
      return;
    }
    while let Some(ev) = self
      .stream
      .as_mut()
      .expect("stream is Some past the handshake window")
      .poll_endpoint_event()
    {
      if let Some(cmd) = ep.handle_stream_event(ev, now) {
        match cmd {
          StreamCommand::SendPushPullResponse(resp) => {
            let (local_states, user_data) = resp.into_parts();
            let response_deadline = now + core::time::Duration::from_secs(5);
            let encoded =
              Endpoint::<I, A>::encode_push_pull_response(&local_states, user_data, false);
            Endpoint::<I, A>::stream_load_response(
              self.stream.as_mut().expect("stream is Some"),
              encoded,
              response_deadline,
            );
            self.deadline = response_deadline;
            // Ignoring Err: a `pump_out` failure terminalizes the bridge,
            // which the next-tick reap reflects.
            let _ = self.pump_out(now);
          }
          StreamCommand::Close => {
            // Terminalize this tick so the coordinator's post-`drain_payload_only`
            // `is_terminal()` re-check reaps within the SAME tick rather than
            // pinning the bridge until its exchange deadline.
            self.fail_with_retire(BridgeFailure::AdmissionClosed);
          }
        }
      }
    }
  }

  /// Test-only: expose the bridge phase.
  #[cfg(test)]
  #[inline(always)]
  pub(crate) fn phase_ref(&self) -> &StreamPhase {
    &self.phase
  }

  /// Test-only: expose whether no `Stream` has been minted yet (pre-promote
  /// window).
  #[cfg(test)]
  pub(crate) fn stream_is_none(&self) -> bool {
    self.stream.is_none()
  }

  /// Test-only: expose whether a `Stream` has been minted (post-promote).
  #[cfg(all(test, feature = "tls"))]
  pub(crate) fn stream_is_some(&self) -> bool {
    self.stream.is_some()
  }

  /// Test-only: expose the snapshotted exchange deadline for assertions.
  #[cfg(all(test, feature = "tls"))]
  pub(crate) fn deadline(&self) -> Instant {
    self.deadline
  }

  /// Test-only: expose whether the pre-promote inbound buffer is empty
  /// (always `true` for a raw-passthrough record layer; may be non-empty for a
  /// TLS bridge that retained a ciphertext tail).
  #[cfg(test)]
  pub(crate) fn pending_inbound_is_empty(&self) -> bool {
    self.pending_inbound.is_empty()
  }

  /// Test-only: expose the pre-promote out-of-band FIN latch (asserted by the
  /// plain-TCP bridge tests, whose FIN is the only close anchor).
  #[cfg(all(test, feature = "tcp"))]
  pub(crate) fn pending_eof(&self) -> bool {
    self.pending_eof
  }

  /// Test-only: return the inner stream's FSM failure reason, if any. Mirrors
  /// `stream.as_ref().and_then(|s| s.is_failed())` for use in tests that live
  /// outside the `streams` module (where `stream` is private).
  #[cfg(test)]
  pub(crate) fn stream_is_failed(&self) -> Option<&crate::error::StreamError> {
    self.stream.as_ref().and_then(|s| s.is_failed())
  }

  /// Test-only: expose the reliable-unit / decompressed-payload ceiling so a
  /// test can assert it tracks the configured `max_stream_frame_size` rather
  /// than a hard-coded constant.
  #[cfg(all(test, feature = "tcp"))]
  pub(crate) fn reliable_max(&self) -> usize {
    self.reliable_max
  }

  /// Test-only: expose the effective [`EncryptionOptions`] the bridge stored
  /// after the `R::is_secure()` selection in [`Self::new`] (or its later
  /// [`Self::set_encryption`] update). Lets a TLS-flavor test assert that a
  /// bridge handed an ENABLED keyring still ends up with a disabled
  /// `EncryptionOptions` (TLS already provides confidentiality, so the
  /// reliable path skips its inner Encrypted wrapper), and a TCP-flavor test
  /// assert that a runtime
  /// [`super::StreamEndpoint::set_encryption_options`] update reached every
  /// live bridge. Gated on the transport features (`tls` / `tcp`) and on
  /// `encryption-aes-gcm` (the asserting tests build a `Keyring`/`SecretKey`
  /// from `memberlist-wire`).
  ///
  /// [`EncryptionOptions`]: memberlist_wire::EncryptionOptions
  #[cfg(all(
    test,
    any(feature = "tls", feature = "tcp"),
    feature = "encryption-aes-gcm"
  ))]
  pub(crate) fn encryption_for_test(&self) -> &memberlist_wire::EncryptionOptions {
    &self.encryption
  }
}
