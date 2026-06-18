//! `memberlist::Stream` <-> quinn bidi-stream byte-pump.
//!
//! D1 (drain-before-reap): a bridge is forgotten only after its
//! [`Stream::poll_endpoint_event`] is fully drained into the [`Endpoint`] and
//! the `StreamClosed`/`StreamErrored` notice is delivered — never before, or a
//! join reply / reliable-ping ack is lost (split-brain / false
//! failure-detect). A push/pull reply or a `ReliablePingAcked` is produced by
//! the last `poll_endpoint_event` of the stream; if the slot-gone notice
//! reaches the `Endpoint` first, the `Endpoint` tears the exchange down before
//! that reply is applied. The ordering in [`Bridge::drain_then_reap`] is
//! therefore load-bearing: payload events -> `StreamCommand` routing -> THEN
//! the lifecycle notice.
//!
//! `WriteError::Blocked` (quinn flow-control / congestion back-pressure)
//! retains the unwritten remainder in `pending_out` and returns without
//! erroring the stream, so no memberlist bytes are dropped when the peer's
//! receive window is momentarily full; the next pump retries from the
//! remainder.

use crate::Instant;

use quinn_proto::{ConnectionHandle, StreamId as QuicSid, VarInt};

use super::conn::ConnTable;
use crate::{
  bridge_phase::{BridgeFailure, BridgePhase},
  endpoint::Endpoint,
  event::{EndpointEvent, StreamClosed, StreamCommand, StreamErrored},
  label::{LabelVerdict, classify_header, encode_label_prefix},
  stream::Stream,
};

/// Couples one memberlist reliable-exchange [`Stream`] to one quinn bidi
/// stream on a pooled connection, pumping bytes both ways and enforcing the
/// D1 drain-before-reap invariant (see module docs).
pub(crate) struct Bridge<I, A> {
  stream: Stream<I, A>,
  ch: ConnectionHandle,
  sid: QuicSid,
  /// Bytes accepted by memberlist but not yet accepted by the quinn send
  /// stream because it returned `WriteError::Blocked`. Drained head-first on
  /// the next [`Bridge::pump_out`]; never discarded while non-empty.
  pending_out: Vec<u8>,
  /// "Owe a finish": `true` once `Stream::poll_transmit` has yielded this
  /// exchange's output bytes (armed on the yield, NOT only after a full
  /// successful write — so a first write that partial-accepts then `Blocked`s
  /// still owes the deferred `finish()`). This — not `poll_transmit() == None`
  /// — is the signal that the send half MAY be finished: an inbound stream
  /// whose response has not yet been `stream_load_response`'d also yields
  /// `None` from `poll_transmit` (its `output_buf` is empty and the phase is
  /// left at `InboundSendingResponse`), so finishing on the first empty poll
  /// closes the send half before the reply is loaded and the later-loaded
  /// bytes are dropped (`WriteError::ClosedStream`) — the inbound push/pull
  /// reply is then never sent (split-brain). The owed `finish()` is performed
  /// only once `pending_out` has fully drained and `poll_transmit` yields
  /// nothing more.
  sent_any: bool,
  /// `SendStream::finish()` is a one-shot per quinn send stream — this
  /// latch guards against a second call once the request/response has
  /// been fully written. Distinct from the `BridgePhase::SendClosed`
  /// transition: `finish_called` records that WE invoked `finish()`;
  /// `SendClosed` records that quinn has notified us the FIN was
  /// ack'd by the peer (`Event::Stream(StreamEvent::Finished)`), which is
  /// `~1` RTT later. The deferred-finish path uses `finish_called` to
  /// avoid re-finishing; terminality uses the phase.
  finish_called: bool,
  /// Composed transport lifecycle — see [`BridgePhase`]. Replaces the
  /// scattered `send_finished` / `fatal` / `fin_observed` / `read_any`
  /// flag combinations that the asymmetric (send-side only)
  /// `is_terminal` formula had to gate. Every transition is anchored to
  /// a quinn observable; `is_terminal` is now a single
  /// `matches!(self.phase, BothClosed | Failed(_))` check by
  /// construction.
  phase: BridgePhase,
  /// The bridge's own flush/lifetime deadline, snapshotted from the inner
  /// [`Stream`]'s exchange deadline at construction.
  ///
  /// `Stream::poll_transmit` moves an inbound-response / one-way-user-message
  /// phase to `Done` *as soon as it hands bytes to the bridge*, and a `Done`
  /// stream's `poll_timeout()` is `None` while `poll_transmit` /
  /// `handle_timeout` / `handle_data` all no-op on the terminal guard — so
  /// once the inner stream is `Done` it can no longer self-enforce its
  /// exchange deadline. A bridge that delegated its timeout to the inner
  /// stream would then contribute NO deadline to the coordinator's unified
  /// `min` across active timers: a peer withholding flow-control credit would
  /// keep a backpressured `pending_out` bridge alive forever, and if credit
  /// arrived after the exchange deadline the retained tail would still be
  /// written — a post-deadline stale send. The bridge therefore
  /// keeps its OWN deadline (the exchange deadline, captured while the stream
  /// is still pre-`Done` and its `poll_timeout()` is `Some`) and enforces it
  /// for the `Done`-but-unflushed window only (see [`Bridge::poll_timeout`] /
  /// [`Bridge::pump_out`]). While the inner stream is NOT `Done` the FSM's
  /// own deadline machinery (`poll_transmit` / `handle_data` →
  /// `enter_failed(Timeout)`) authoritatively governs the timeout, so the
  /// bridge's enforcement defers to the FSM in that case.
  deadline: Instant,
  /// Cross-transport compression configuration. The bridge is the single
  /// compress/decompress point on the QUIC reliable path; a disabled
  /// `CompressionOptions` makes the path identity (a plain `[unit_len][bytes]`
  /// frame with the framed bytes verbatim).
  #[cfg(compression)]
  compression: crate::CompressionOptions,
  /// Cross-transport encryption configuration. The QUIC reliable path
  /// always skips the inner Encrypted wrapper — quinn-encrypted streams
  /// already provide confidentiality, and double-encrypting costs CPU
  /// and bandwidth without adding security. [`Self::new`] force-disables
  /// this field regardless of the caller's intent; the field is retained
  /// so the reliable encode/decode can fold in the encryption stage
  /// uniformly (a disabled `EncryptionOptions` makes that stage identity).
  #[cfg(encryption)]
  encryption: crate::EncryptionOptions,
  /// Inbound reliable-unit accumulation buffer. A QUIC stream chunks bytes
  /// arbitrarily, so each `chunk.bytes` is appended here and every complete
  /// `[unit_len][payload]` unit is drained off the front; a trailing partial
  /// unit stays buffered until a later chunk completes it.
  recv_accum: Vec<u8>,
  /// Hard ceiling on a reliable unit's on-wire size and its decompressed
  /// payload — the decompression-bomb guard bound, and the cap on the inbound
  /// accumulation buffer (a forged `unit_len` over this is rejected before any
  /// wait). Derived from `EndpointOptions::max_stream_frame_size` so it tracks
  /// the Stream FSM's own configured frame limit rather than a separate
  /// constant.
  reliable_max: usize,
  /// Cluster label for this bridge, or `None` when no label is configured.
  ///
  /// When `Some`, a `[LABELED_TAG=12][len:u8][label_bytes]` frame is prepended
  /// to the bidi stream as the FIRST outbound bytes, and the peer's inbound
  /// label header is validated off the head of `recv_accum` before any
  /// reliable unit reaches `self.stream`. Faithful to
  /// `memberlist-core/src/network.rs` bidirectional labeling: both sides
  /// write their label AND validate the peer's.
  ///
  /// `None` is byte-identical to today: no label frame is written, and the
  /// inbound path skips `classify_header` entirely.
  label: Option<bytes::Bytes>,
  /// When `true`, suppress the "label expected but missing from inbound peer"
  /// check — the `classify_header` `skip_inbound_label_check` argument.
  /// Passed through verbatim; does not suppress `DoubleLabel` (a labeled
  /// frame arriving at an unlabeled bridge is always rejected).
  skip_inbound_label_check: bool,
  /// `true` for the dialer role: write the outbound label frame eagerly
  /// as the first bytes, without waiting for the peer's inbound label to
  /// validate. `false` for the acceptor: hold the outbound label until
  /// `inbound_label_validated` latches (lazy disclosure).
  eager_outbound_label: bool,
  /// Latch: `true` once the peer's inbound label header has been validated
  /// (or confirmed absent for an unlabeled stream). Initialized to
  /// `label.is_none()` so the unlabeled path is byte-identical to today —
  /// no `classify_header` call, no early-return.
  inbound_label_validated: bool,
  /// Latch: `true` once the outbound label frame has been inserted into
  /// `pending_out` (or skipped because `label` is `None`). Initialized to
  /// `label.is_none()`.
  outbound_label_written: bool,
}

impl<I, A> Bridge<I, A>
where
  A: crate::Data + crate::CheapClone + PartialEq + 'static,
{
  /// Build a `Bridge` over a freshly opened/accepted quinn bidi stream.
  ///
  /// `reliable_max` is the reliable-unit / decompressed-payload ceiling — the
  /// coordinator passes `EndpointOptions::max_stream_frame_size` so the bound
  /// always matches the Stream FSM's configured frame limit.
  ///
  /// `_encryption` is accepted to keep the per-transport bridge constructors
  /// signature-aligned, but the QUIC reliable path always skips encryption:
  /// quinn-encrypted streams already provide confidentiality, and
  /// double-encrypting on top costs CPU and bandwidth without adding
  /// security. The field is force-disabled in the body regardless of the
  /// caller's intent (see [`Self::encryption`] field docs).
  ///
  /// `label` is the cluster label to frame, or `None` for an unlabeled stream
  /// (byte-identical to today). `skip_inbound_label_check` suppresses the
  /// missing-label rejection for an unlabeled peer (forwarded to
  /// [`classify_header`]). `eager_outbound_label` selects the dialer (eager,
  /// writes before inbound validation) vs. acceptor (lazy, writes after
  /// inbound validation) role — faithfully reproduced from
  /// `memberlist-core/src/network.rs` bidirectional labeling.
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    stream: Stream<I, A>,
    ch: ConnectionHandle,
    sid: QuicSid,
    #[cfg(compression)] compression: crate::CompressionOptions,
    #[cfg(encryption)] _encryption: crate::EncryptionOptions,
    reliable_max: usize,
    label: Option<bytes::Bytes>,
    skip_inbound_label_check: bool,
    eager_outbound_label: bool,
  ) -> Self {
    // Snapshot the exchange deadline now, while the stream is still pre-`Done`.
    // Both stream constructors (`Endpoint::dial_succeeded` —
    // `deadline: Some(intent.deadline)`; `Endpoint::accept_stream` —
    // `deadline: Some(now + stream_timeout)`) build the stream with a `Some`
    // deadline and a non-terminal phase (`OutboundSendingRequest` /
    // `InboundAwaitingFirstMessage`), so `poll_timeout()` is `Some` at this
    // instant — see the `deadline` field docs for why the bridge needs its
    // own copy. The `expect` documents that construction-time invariant; it
    // is an internal property of those two constructors, not a runtime or
    // peer-controlled condition.
    let deadline = stream
      .poll_timeout()
      .expect("a freshly dialed/accepted Stream is pre-`Done` with a Some exchange deadline");
    // QUIC streams are quinn-encrypted by construction. The reliable path
    // never carries an inner `Encrypted` wrapper, so the bridge stores a
    // disabled `EncryptionOptions` regardless of the caller's input. The
    // encryption stage of the reliable encode/decode collapses to identity
    // when handed a disabled `EncryptionOptions`, keeping the on-wire bytes
    // byte-identical to the pre-encryption-port shape.
    #[cfg(encryption)]
    let encryption = crate::EncryptionOptions::new();
    // Defense-in-depth: an empty label normalizes to no-label so an empty
    // configured label never emits a `[12][0]` header, and an over-long label
    // (a caller-side bug — `QuicEndpoint::with_label` validates) is caught in
    // debug before it could truncate the length byte in `encode_label_prefix`.
    let label = label.filter(|bytes| !bytes.is_empty());
    debug_assert!(
      label
        .as_ref()
        .is_none_or(|bytes| bytes.len() <= crate::label::MAX_LABEL_LEN),
      "QUIC bridge label exceeds MAX_LABEL_LEN; validate via QuicEndpoint::with_label"
    );
    let inbound_label_validated = label.is_none();
    let outbound_label_written = label.is_none();
    Self {
      stream,
      ch,
      sid,
      pending_out: Vec::new(),
      sent_any: false,
      finish_called: false,
      phase: BridgePhase::Active,
      deadline,
      #[cfg(compression)]
      compression,
      #[cfg(encryption)]
      encryption,
      recv_accum: Vec::new(),
      reliable_max,
      label,
      skip_inbound_label_check,
      eager_outbound_label,
      inbound_label_validated,
      outbound_label_written,
    }
  }

  /// Encode one outbound reliable unit: compress (when built in), then encrypt
  /// (when built in), then length-delimit. The inverse of
  /// [`Self::take_reliable_unit`]. With no transform backend this is just
  /// `[unit_len][framed]`.
  ///
  /// The QUIC reliable path force-disables encryption in the constructor
  /// (quinn already encrypts the stream), so the encryption stage is identity
  /// at runtime even when an encryption backend is built in; the stage is
  /// retained for shape-parity with the plain-stream bridge.
  // `unused_mut` / `unused_assignments`: the `payload` binding is reassigned
  // only under the compression/encryption cfgs, so with one or neither built in
  // the initial binding (or a stage's write) is never observed.
  #[allow(unused_mut, unused_assignments)]
  fn encode_reliable_unit(&self, framed: &[u8]) -> Result<Vec<u8>, crate::framing::FrameError> {
    use std::borrow::Cow;
    let mut payload: Cow<'_, [u8]> = Cow::Borrowed(framed);
    #[cfg(compression)]
    {
      payload = Cow::Owned(crate::compression::compress_reliable_payload(
        &self.compression,
        framed,
      ));
    }
    #[cfg(encryption)]
    {
      payload = Cow::Owned(
        crate::encryption::encrypt_reliable_payload(&self.encryption, &payload)
          .map_err(crate::framing::FrameError::Encryption)?,
      );
    }
    let mut out = Vec::with_capacity(5 + payload.len());
    crate::framing::encode_varint_u32(payload.len() as u32, &mut out);
    out.extend_from_slice(&payload);
    Ok(out)
  }

  /// Take one complete reliable unit `[unit_len][payload]` off the front of
  /// `buf`, stripping the encryption (when built in) then checksum/compression
  /// wrappers. `Ok(None)` when more bytes are needed. The inverse of
  /// [`Self::encode_reliable_unit`].
  fn take_reliable_unit(
    &self,
    buf: &[u8],
    max: usize,
  ) -> Result<Option<(Vec<u8>, usize)>, crate::framing::FrameError> {
    use crate::framing::{FrameError, decode_varint_u32};
    let (unit_len, vbytes) = match decode_varint_u32(buf) {
      Ok(v) => v,
      Err(FrameError::Incomplete(..)) | Err(FrameError::Empty) => return Ok(None),
      Err(e) => return Err(e),
    };
    let unit_len = unit_len as usize;
    // With encryption the wrapper inflates the payload by a fixed
    // ENCRYPTED_WRAPPER_OVERHEAD, so allow that slack on the on-wire unit bound;
    // the post-decrypt plaintext is still bounded tightly by `max`.
    #[cfg(encryption)]
    let effective_unit_max = if self.encryption.is_enabled() {
      max.saturating_add(crate::encryption::ENCRYPTED_WRAPPER_OVERHEAD)
    } else {
      max
    };
    #[cfg(not(encryption))]
    let effective_unit_max = max;
    if unit_len > effective_unit_max {
      return Err(FrameError::FrameTooLarge(unit_len));
    }
    let total = vbytes + unit_len;
    if buf.len() < total {
      return Ok(None);
    }
    let payload = &buf[vbytes..total];
    let plaintext = {
      #[cfg(encryption)]
      {
        crate::framing::unwrap_transforms_with_encryption(payload, max, &self.encryption)?
      }
      #[cfg(not(encryption))]
      {
        crate::framing::unwrap_transforms(payload, max)?
      }
    }
    .into_owned();
    Ok(Some((plaintext, total)))
  }

  /// Replace the bridge's effective encryption options. Called by
  /// [`super::QuicEndpoint::set_encryption_options`] when the operator updates
  /// the encryption policy at runtime — the setter is invoked uniformly on
  /// every live bridge so the propagation is symmetric with the plain-TCP
  /// `StreamBridge` setter.
  ///
  /// QUIC reliable streams are quinn-encrypted by construction, so the
  /// stored encryption is force-disabled regardless of the caller's intent,
  /// matching [`Self::new`]'s body. The propagation is therefore a no-op on
  /// the QUIC reliable path (gossip strictness propagates immediately via
  /// the coordinator's `self.encryption` since `decrypt_gossip` reads that
  /// field directly).
  #[cfg(encryption)]
  pub(crate) fn set_encryption(&mut self, _encryption: crate::EncryptionOptions) {
    self.encryption = crate::EncryptionOptions::new();
  }

  /// Drive the SEND-half transition: `Active → SendClosed`, or
  /// `RecvClosed → BothClosed`. No-op for already-`Failed`/-terminal
  /// states (terminal states are sticky).
  ///
  /// Anchor: quinn-proto `Connection::poll() -> Event::Stream(
  /// StreamEvent::Finished{id})` for this bridge's `sid`. The peer
  /// has acknowledged our FIN; quinn's `SendState` has reached
  /// `DataRecvd` and the send half is retired. Routed by
  /// [`super::QuicEndpoint::service_quinn`] via its `sid → StreamId`
  /// index.
  pub(crate) fn observe_send_fin(&mut self) {
    self.phase = match &self.phase {
      BridgePhase::Active => BridgePhase::SendClosed,
      BridgePhase::RecvClosed => BridgePhase::BothClosed,
      BridgePhase::SendClosed | BridgePhase::BothClosed | BridgePhase::Failed(_) => return,
    };
  }

  /// Drive the RECV-half transition: `Active → RecvClosed`, or
  /// `SendClosed → BothClosed`. No-op for already-`Failed`/-terminal
  /// states.
  ///
  /// Anchor: `Chunks::next() -> Ok(None)` inside [`Self::pump_in`].
  /// quinn-proto's `RecvState` has reached `DataRecvd` and the recv
  /// half is retired by natural FIN consumption.
  fn observe_recv_fin(&mut self) {
    self.phase = match &self.phase {
      BridgePhase::Active => BridgePhase::RecvClosed,
      BridgePhase::SendClosed => BridgePhase::BothClosed,
      BridgePhase::RecvClosed | BridgePhase::BothClosed | BridgePhase::Failed(_) => return,
    };
  }

  /// Idempotent retirement of BOTH QUIC halves on this bridge's `sid`:
  /// `SendStream::reset(0)` + `RecvStream::stop(0)` + clear
  /// `pending_out`. Used by every failure transition that has `conns`
  /// in scope, so a `BridgePhase::Failed` reap is recv-clean AND
  /// send-clean by construction — no orphaned QUIC stream state.
  ///
  /// Idempotent: `reset` and `stop` return `Err(ClosedStream)` only
  /// when their half is already terminated (peer reset, prior
  /// `finish`/`stop`); both states satisfy "half is retired" which is
  /// what this helper achieves. The discards are documented.
  fn retire_halves(&mut self, conns: &mut ConnTable) {
    if let Some(entry) = conns.get_mut(self.ch) {
      let conn = entry.conn_mut();
      // Ignoring Err: `SendStream::reset` returns `Err(ClosedStream)`
      // only when the send half is already terminated — idempotent
      // retirement is what this helper needs.
      let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
      // Ignoring Err: `RecvStream::stop` returns `Err(ClosedStream)`
      // only when the recv half is already terminated — same
      // idempotent-retirement semantics as the send-half reset above.
      let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
    }
    self.pending_out.clear();
  }

  /// Atomic FAILURE transition: retire both QUIC halves AND set
  /// `BridgePhase::Failed(reason)` in one step. Use this everywhere
  /// `conns` is in scope. The `fail()` helper without retirement is
  /// reserved for cases where the underlying connection is already
  /// gone (e.g. `fail_connection_lost`) so half retirement is moot.
  ///
  /// Sticky — first failure wins; subsequent calls preserve the
  /// original cause for `drain_then_reap`'s `EndpointEvent::StreamErrored`
  /// notice. Idempotent halves retirement still runs (cheap if already
  /// retired) so a recovery race that observes a prior Failed phase
  /// cannot leave halves orphaned.
  fn fail_with_retire(&mut self, conns: &mut ConnTable, reason: BridgeFailure) {
    self.retire_halves(conns);
    self.fail(reason);
  }

  /// FAILURE transition without retiring halves. Reserved for the
  /// `ConnectionLost` path where the underlying quinn `Connection` is
  /// gone — `conns.get_mut(self.ch)` would still succeed briefly (the
  /// slab entry is reaped via `reap_if_drained` shortly after) but
  /// `SendStream::reset`/`RecvStream::stop` on a drained connection's
  /// streams is a no-op anyway. Sticky — see [`Self::fail_with_retire`].
  ///
  /// Pre-FIN side-effect hygiene: if the recv half has NOT yet been
  /// retired by peer FIN observation (phase ∉ `RecvClosed` /
  /// `BothClosed`), discard the FSM's queued endpoint / lifecycle
  /// events. The `drain_payload_only` deferred-commit gate holds these
  /// events while the bridge is non-terminal pending peer FIN as
  /// protocol-level proof the exchange completed; `drain_then_reap`
  /// (the terminal D1 path) drains the queue unconditionally to enforce
  /// D1 before lifecycle. Without this pre-FIN clear, a peer that sends
  /// one complete frame and then resets (or whose connection drops)
  /// BEFORE FIN would have its dispatched frame's side effects committed
  /// by `drain_then_reap` even though the exchange was never authorized.
  /// Mirrors `Stream::enter_failed`'s queue clearing on the FSM-failure
  /// path — same atomicity, applied at the transport-failure boundary.
  /// `RecvClosed` / `BothClosed` failures (e.g. connection drops AFTER
  /// peer FIN but before our send half completed) preserve the queue:
  /// FIN already authorized the dispatched events; the failure is
  /// orthogonal.
  fn fail(&mut self, reason: BridgeFailure) {
    if matches!(self.phase, BridgePhase::Failed(_)) {
      return;
    }
    if !matches!(
      self.phase,
      BridgePhase::RecvClosed | BridgePhase::BothClosed
    ) {
      self.stream.discard_pending_events();
    }
    self.phase = BridgePhase::Failed(reason);
  }

  /// Stream ID of this bridge's underlying quinn bidi — used by the
  /// coordinator's `sid → StreamId` index to route per-stream events
  /// (`StreamEvent::Finished`, `StreamEvent::Stopped`) to the right
  /// bridge inside `service_quinn`.
  pub(crate) fn sid(&self) -> QuicSid {
    self.sid
  }

  /// `true` iff the bridge is in [`BridgePhase::Failed`] — observable
  /// shorthand for the pump_in/pump_out leading-fatal guards, for
  /// `drain_then_reap`'s lifecycle-notice selection, and for the
  /// coordinator's `Event::ExchangeCompleted` outcome decision at
  /// bridge-reap time. Distinct from `Stream::is_failed()` which
  /// reports FSM-level failure; this reports BRIDGE-level failure
  /// (FSM failure cascades into the bridge via the failure
  /// transitions).
  pub(crate) fn is_phase_failed(&self) -> bool {
    matches!(self.phase, BridgePhase::Failed(_))
  }

  /// The pooled quinn connection this bridge rides on.
  pub(crate) fn ch(&self) -> ConnectionHandle {
    self.ch
  }

  /// The bridge's flush/lifetime deadline, folded into the unified `min`
  /// across active bridge / Endpoint / quinn timers in
  /// `QuicEndpoint::poll_timeout`.
  ///
  /// Returns the bridge's OWN snapshotted `deadline` while the bridge is not
  /// yet completion-terminal — NOT the inner stream's `poll_timeout()`, which
  /// goes `None` the moment `Stream::poll_transmit` flips an inbound-response
  /// / one-way-user-message phase to `Done` (before the bytes reach quinn).
  /// Delegating to the inner stream would silently drop this bridge out of
  /// the unified timer for a backpressured-but-`Done` stream, so a peer
  /// withholding credit would never get the bridge ticked again — unbounded
  /// lifetime + a post-deadline stale write if credit ever arrived. The
  /// bridge deadline is therefore the lifetime authority; while the inner
  /// stream still has a (possibly earlier, e.g. the inbound-response
  /// `now + 5s`) deadline it is folded in via `min` so the tighter bound
  /// still wins. A completion-terminal bridge is reaped by `run_tick`'s D1
  /// path this same tick, so it contributes no deadline.
  pub(crate) fn poll_timeout(&self) -> Option<Instant> {
    if self.is_terminal() {
      return None;
    }
    Some(match self.stream.poll_timeout() {
      Some(inner) => inner.min(self.deadline),
      None => self.deadline,
    })
  }

  /// Force the bridge terminal with [`BridgeFailure::ConnectionLost`].
  /// Used when the underlying quinn connection is lost: the per-stream
  /// read/write pumps never observe the failure (the connection is
  /// gone, not the stream), so the coordinator transitions every
  /// bridge on that connection through this entry and the same tick's
  /// inline D1 drain + `StreamErrored` reap runs as for any other
  /// transport error.
  pub(crate) fn fail_connection_lost(&mut self) {
    self.fail(BridgeFailure::ConnectionLost);
  }

  /// Transition the bridge's phase to `Failed(Transport)` for a
  /// peer's STOP_SENDING. quinn-proto emits `Event::Stream(
  /// StreamEvent::Stopped{id, error_code})` when the peer asks us to
  /// stop sending on a stream; our send half goes to `ResetSent`
  /// natively, but the recv half is independent and needs explicit
  /// retirement.
  ///
  /// **Caller contract:** the caller MUST retire both halves
  /// (`SendStream::reset` + `RecvStream::stop`) on the same
  /// connection BEFORE invoking this method. The borrow constraint in
  /// [`super::QuicEndpoint::service_quinn`] (where this is routed
  /// from `Connection::poll()`) holds the connection's `&mut` via
  /// `e.conn_mut()` already, so retiring the halves inline at the
  /// call site is the natural shape; passing `conns` through would
  /// require dropping the `e` borrow mid-loop.
  pub(crate) fn fail_stopped_already_retired(&mut self, error_code: quinn_proto::VarInt) {
    self.pending_out.clear();
    self.fail(BridgeFailure::Transport(format!(
      "peer STOP_SENDING: {error_code}"
    )));
  }

  /// Pump outbound memberlist bytes into the quinn send stream.
  ///
  /// Terminal-bridge guard (FIRST). `run_tick` invokes `pump_in` then
  /// `pump_out` unconditionally. If `pump_in` made the bridge terminal this
  /// same tick (a [`ReadError::Reset`], a decode failure inside
  /// [`Stream::handle_data`], an illegal ordered read — every path that
  /// sets `self.fatal`) OR a prior tick already terminalized it (the bridge
  /// is still waiting on its D1 reap), the bottom `pending_out` flush /
  /// `poll_transmit` loop must NOT run: the send half may still be `Ready`
  /// and newly credited, and writing the retained tail or any newly-yielded
  /// bytes here would deliver a stale request/response to the peer (memberlist
  /// frames are length-delimited, so the peer's decoder applies a partial
  /// wire as a full message without needing a FIN). The first thing
  /// `pump_out` does is therefore: if `self.fatal || self.stream.is_failed().is_some()`,
  /// abandon the QUIC halves (`SendStream::reset` + `RecvStream::stop`;
  /// both return harmlessly if their half is already gone), clear
  /// `pending_out`, force `self.fatal=true` (so
  /// `is_terminal()` fires on the fatal arm even when only `is_failed()` was
  /// set), and return `Err(())`. After this guard the pre-write deadline
  /// check (next) is unreachable for a terminal bridge, and so are the
  /// `is_done()`-gated flush-deadline abandon, the `pending_out` flush,
  /// and the `poll_transmit` loop.
  ///
  /// Pre-write deadline enforcement covers a non-terminal stream
  /// with a non-empty `pending_out` (the back-pressured retained tail):
  /// the inner [`Stream::poll_timeout`] (the FSM's exchange deadline
  /// while the stream is non-terminal) is consulted, and if it is
  /// `Some(t)` with `t <= now`, [`Stream::handle_timeout`] is driven (so
  /// the FSM transitions to `Failed(Timeout)` and `enter_failed` clears
  /// `input_buf`/`output_buf`), the QUIC halves are abandoned (RESET +
  /// STOP), `pending_out` is cleared, the bridge marks itself `fatal`,
  /// and `pump_out` returns WITHOUT writing the retained tail and
  /// WITHOUT FINing. This closes the gap where a back-pressured
  /// `OutboundAwaitingResponse` / `OutboundSendingRequest` /
  /// `InboundAwaitingFirstMessage` / `InboundSendingResponse` stream
  /// cannot self-fail (the `pending_out` flush below `WriteError::Blocked`s
  /// and `pump_out` returns before `poll_transmit` is called, so
  /// `Stream`'s own deadline-at-`poll_transmit` never fires); without this
  /// check the bridge would either survive until the connection-level idle
  /// timeout or write a stale post-deadline tail if credit eventually
  /// arrived. The gate is `!pending_out.is_empty()`: when `pending_out` is
  /// empty there is no retained tail to risk a post-deadline write, and
  /// the existing `poll_transmit` deadline check below self-fails the
  /// stream at the same instant.
  ///
  /// The bridge-level flush deadline covers the `Done`-but-unflushed
  /// gap: when the inner stream is `Done` (terminal guard makes
  /// `poll_transmit`/`handle_timeout`/`handle_data` no-op so `Stream::poll_timeout`
  /// is `None` and the pre-write check above cannot fire) AND the bridge
  /// is not yet completion-terminal (`pending_out` non-empty OR send half not
  /// finished) AND `now >= self.deadline`, abandon the exchange — RESET the
  /// send half + STOP the recv half + mark `fatal` (→ `is_terminal()` → D1
  /// reap) — and return WITHOUT writing.
  ///
  /// On `WriteError::Blocked` the unwritten remainder is retained in
  /// `pending_out` (or carried over from a prior tick) and the pump stops —
  /// it is retried head-first next tick. No bytes are dropped and the stream
  /// is not failed for back-pressure. Any other write error (peer `Stopped`,
  /// `ClosedStream`) marks the bridge `fatal` and returns `Err(())` so the
  /// coordinator runs the D1 reap promptly rather than waiting out the
  /// exchange deadline.
  ///
  /// The send half is `finish()`ed exactly once (`send_finished` guards a
  /// second call), on the first tick EITHER the memberlist [`Stream`] has
  /// emitted its full request/response (`sent_any` — so a mid-exchange
  /// outbound request FINs its send half *before* the stream is `Done`, so
  /// the peer can reply) OR the inner [`Stream`] is `Done` with `pending_out`
  /// empty (so an inbound one-way `Message::UserData`, which drives the stream
  /// to `Done` with NO outbound bytes and therefore never arms `sent_any`,
  /// still finishes its send half and becomes reapable instead of leaking the
  /// bridge + bidi stream forever). It is never finished on an empty
  /// `poll_transmit` whose response buffer is merely not yet
  /// `stream_load_response`'d (that phase is `InboundSendingResponse`, not
  /// `Done`, with `sent_any` false — neither branch fires, so the reply is
  /// not truncated). It is also gated against `Stream::is_failed()` / `fatal`
  /// so a timed-out or transport-errored stream cannot emit FIN on top of a
  /// RESET_STREAM (a FIN would make the expired request look complete to the
  /// peer).
  ///
  /// `sent_any` ("owe a finish") is armed as soon as `poll_transmit` yields
  /// bytes for this exchange — NOT only after a full successful write — so a
  /// first write that partial-accepts then `Blocked`s still owes the deferred
  /// `finish()`. When that back-pressured remainder finally drains on a later
  /// tick and `poll_transmit` yields nothing more, the owed `finish()` runs:
  /// a backpressured terminal stream therefore stays alive across ticks until
  /// every retained byte is written and the send half is `finish()`ed (FIN),
  /// and only then is it reapable (see [`Bridge::is_terminal`]).
  pub(crate) fn pump_out(&mut self, conns: &mut ConnTable, now: Instant) -> Result<(), ()> {
    // Terminal-bridge guard.
    //
    // If this bridge is already terminal — either because `pump_in` set
    // `self.fatal` this same tick (peer reset, illegal ordered read,
    // `handle_data` decode failure) or because the inner Stream entered
    // `Failed(...)` on a previous tick and the D1 reap is still pending —
    // every write path below must be unreachable. quinn's send half can
    // still be `Ready` (a peer reset only flips our recv half) and a late
    // `MAX_STREAM_DATA` can newly credit a `pending_out` tail, so the
    // bottom flush would otherwise hand stale bytes to `SendStream::write`.
    // Memberlist frames are length-delimited (the peer decodes/applies the
    // partial buffer as a full message before ever observing a FIN), so the
    // no-FIN gate further down is not sufficient — the write itself must
    // not happen.
    //
    // Action: drop any retained tail (`SendStream::reset` drives
    // `Ready`/`DataSent` → `ResetSent`; `RecvStream::stop` discards unread
    // data + queues STOP_SENDING; both `Err(ClosedStream)` harmlessly
    // when their half is already gone), clear `pending_out`, set
    // `self.fatal = true` (so `is_terminal()` short-circuits via the fatal
    // arm even when only `Stream::is_failed()` was the trigger), and return
    // `Err(())` (the same `Err` convention the other fatal arms use, picked
    // up by `run_tick`'s terminal check → `drain_then_reap` → reap).
    if self.is_phase_failed() || self.stream.is_failed().is_some() {
      let reason = match self.stream.is_failed() {
        Some(e) => BridgeFailure::Transport(e.to_string()),
        None => BridgeFailure::Transport("bridge fatal".to_string()),
      };
      self.fail_with_retire(conns, reason);
      return Err(());
    }

    // Pre-write deadline enforcement — non-terminal stream with retained
    // tail.
    //
    // `Stream::poll_transmit` and `Stream::handle_data` BOTH self-enforce
    // `stream.deadline` (`enter_failed(StreamError::Timeout)` on `now >=
    // deadline`), but only when they are actually called. An OUTBOUND
    // request (push/pull, reliable-ping) yields its bytes pre-deadline,
    // transitions to `OutboundAwaitingResponse` (NOT `Done`), and may leave
    // a blocked tail in `pending_out`. On a later tick at/after the deadline
    // the `pending_out` flush below returns `Blocked` (no credit) and
    // `pump_out` returns early — `poll_transmit` is never reached, so the
    // Stream FSM never gets to self-fail, and if credit ever arrives the
    // retained tail is written *after* the exchange deadline (a stale
    // post-deadline send).
    // Even when credit never returns, the bridge survives until quinn's
    // connection-level `max_idle_timeout` — the coordinator's unified `min`
    // across active timers no longer reaps the stream at its own deadline.
    //
    // The gate is `!pending_out.is_empty()`: that is exactly the state in
    // which the FSM canNOT self-enforce its deadline (the early-return on
    // `Blocked` skips `poll_transmit`). For an empty `pending_out`, the
    // existing `poll_transmit` deadline check at the bottom of this function
    // already self-fails the stream at the same instant (`enter_failed` →
    // `is_failed()` → `is_terminal()` → D1 reap). Restricting the check to
    // the retained-tail case avoids piling RESET_STREAM/STOP_SENDING onto
    // the symmetric FSM-timeout for every coincident-deadline stream — when
    // many bridges' deadlines align (e.g. a steady-state cluster whose
    // periodic push/pulls all race `stream_timeout`), an indiscriminate
    // RESET cascade through quinn-proto's stream-state machinery starves
    // concurrent, otherwise-healthy reliable exchanges on the same pooled
    // connection of forward progress.
    //
    // When the gate fires: drive `Stream::handle_timeout(now)` (the FSM
    // transitions to `Failed(StreamError::Timeout)` and `enter_failed`
    // clears `input_buf`/`output_buf`), then abandon the QUIC halves —
    // `SendStream::reset` drives `Ready`/`DataSent` → `ResetSent`;
    // `RecvStream::stop` discards unread data and queues STOP_SENDING;
    // both return `Err(ClosedStream)` harmlessly if their half is already
    // gone. Clear `pending_out` so no post-deadline bytes can be written,
    // mark `fatal`, return WITHOUT FINing (the deferred finish below is
    // gated against `Stream::is_failed()` so a timed-out stream cannot emit
    // FIN on top of a RESET_STREAM). After this `is_terminal()` is true via
    // both the `fatal` arm and `Stream::is_failed()`, so `run_tick`'s D1
    // drain-then-reap completes the teardown this same tick.
    if !self.is_terminal() && !self.pending_out.is_empty() {
      if let Some(t) = self.stream.poll_timeout() {
        if t <= now {
          self.stream.handle_timeout(now);
          self.fail_with_retire(conns, BridgeFailure::Timeout);
          return Err(());
        }
      }
    }

    // Bridge-level flush deadline — the `Done`-but-unflushed gap.
    //
    // Once `Stream::poll_transmit` flips an inbound-response / one-way-user-
    // message phase to `Done` (before the bytes reach quinn), `poll_timeout`
    // returns `None` and `poll_transmit`/`handle_timeout`/`handle_data`
    // no-op on the terminal guard — so a `Done` stream with a non-empty
    // `pending_out` (peer withholding flow-control credit) or an
    // un-`finish()`ed send half has NO FSM deadline authority left and the
    // pre-write check above never fires for it. `is_done() && !is_terminal()`
    // is exactly that state. Abandon it: RESET our send half + STOP the recv
    // half so quinn emits RESET_STREAM/STOP_SENDING (the peer sees a cleanly
    // failed exchange, not a truncated reply masquerading as complete), mark
    // `fatal` so `is_terminal()` fires and `run_tick`'s D1 drain-then-reap
    // reaps it, and return WITHOUT writing — which is what prevents the
    // post-deadline tail write.
    if now >= self.deadline && self.stream.is_done() && !self.is_terminal() {
      self.fail_with_retire(conns, BridgeFailure::Timeout);
      return Err(());
    }

    let Some(entry) = conns.get_mut(self.ch) else {
      return Ok(());
    };
    let conn = entry.conn_mut();

    // Outbound label frame — emitted exactly once, BEFORE any reliable unit.
    //
    // The dialer (eager_outbound_label=true) writes its label immediately; the
    // acceptor (eager_outbound_label=false) holds until inbound_label_validated
    // latches, so it never discloses its own label before confirming the peer's.
    // `pending_out` is guaranteed empty when this fires: outbound_label_written
    // is initialized to label.is_none(), so the first write-eligible invocation
    // is the very first pump_out call, at which point no bytes have been sent.
    if !self.outbound_label_written && (self.eager_outbound_label || self.inbound_label_validated) {
      if let Some(lbl) = &self.label {
        encode_label_prefix(lbl, &mut self.pending_out);
      }
      self.outbound_label_written = true;
    }

    // Flush any back-pressured remainder first so stream order is preserved.
    if !self.pending_out.is_empty() {
      match conn.send_stream(self.sid).write(&self.pending_out) {
        Ok(n) => {
          self.pending_out.drain(..n);
        }
        Err(quinn_proto::WriteError::Blocked) => return Ok(()),
        Err(e) => {
          // Atomic failure: retire both halves on `conn` (already
          // borrowed in scope) BEFORE flipping the phase. The
          // `is_terminal()` predicate is phase-authoritative, so a
          // bridge that becomes Failed must already have both halves
          // retired — otherwise a same-tick reap orphans QUIC state.
          // Ignoring Err: idempotent retirement.
          let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
          let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
          self.pending_out.clear();
          self.fail(BridgeFailure::Transport(format!("send write: {e:?}")));
          return Err(());
        }
      }
      if !self.pending_out.is_empty() {
        return Ok(());
      }
    }

    // Gather every `poll_transmit` yield into one buffer, then write it as
    // ONE self-delimiting reliable unit. A QUIC stream chunks bytes
    // arbitrarily (and flow-control credit may split the write), so framing
    // each drain as one `[unit_len][payload]` unit lets the peer re-delimit
    // it regardless of how the stream chunks the bytes.
    let mut gathered = Vec::new();
    let mut chunk = Vec::new();
    while self.stream.poll_transmit(now, &mut chunk).is_some() {
      // `poll_transmit` yielded this exchange's output: the send half now
      // owes a `finish()` (armed here, before any write, so a partial-accept-
      // then-Blocked still owes it — unchanged from today).
      self.sent_any = true;
      gathered.extend_from_slice(&chunk);
      chunk.clear();
    }
    if !gathered.is_empty() {
      // The QUIC bridge force-disables encryption in its constructor (quinn
      // already encrypts the stream), so the encryption stage never fails
      // here. The fallible `Result` is still matched — the encode shares its
      // shape with the StreamBridge path — but on this path the error branch
      // is unreachable in practice.
      let unit = match self.encode_reliable_unit(&gathered) {
        Ok(u) => u,
        Err(e) => {
          // Ignoring Err: idempotent retirement.
          let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
          let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
          self.pending_out.clear();
          self.fail(BridgeFailure::Transport(format!("encrypt: {e}")));
          return Err(());
        }
      };
      let mut off = 0;
      while off < unit.len() {
        match conn.send_stream(self.sid).write(&unit[off..]) {
          Ok(w) => off += w,
          Err(quinn_proto::WriteError::Blocked) => {
            // Retain the COMPRESSED-unit remainder; replayed head-first next
            // tick by the existing `pending_out` flush at the top of pump_out.
            self.pending_out.extend_from_slice(&unit[off..]);
            return Ok(());
          }
          Err(e) => {
            // Atomic failure — same retire-then-fail as the existing arm.
            // Ignoring Err: idempotent retirement.
            let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
            let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
            self.pending_out.clear();
            self.fail(BridgeFailure::Transport(format!("send write: {e:?}")));
            return Err(());
          }
        }
      }
    }

    // FSM-failure detection AFTER `poll_transmit`: the FSM checks its
    // own deadline inside `poll_transmit` and transitions to
    // `Failed(Timeout)` if elapsed (returning `None` without yielding
    // bytes). The bridge mirrors this into `BridgePhase::Failed` AND
    // retires both halves atomically, so the next tick's
    // `is_terminal()` (phase-authoritative) reaps a fully-retired
    // bridge. Without this an outbound exchange waiting for response
    // (empty `pending_out`, `!is_done()`) that trips its deadline via
    // `poll_transmit` would reap before any half retirement —
    // orphaning QUIC stream state and bleeding bidi credit on the
    // pooled connection.
    if let Some(e) = self.stream.is_failed() {
      // Ignoring Err: idempotent retirement.
      let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
      let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
      self.pending_out.clear();
      self.fail(BridgeFailure::Transport(format!("stream failed: {e}")));
      return Err(());
    }

    // Finish the send half exactly once, on the first tick EITHER:
    //
    //  (a) the memberlist Stream has emitted its full request/response
    //      (`sent_any`) — the mid-exchange case: an OUTBOUND request whose
    //      phase is now `OutboundAwaitingResponse` (NOT `Done`) MUST FIN its
    //      send half *before* the stream is `Done` so the peer sees EOF and
    //      replies; or
    //
    //  (b) the inner Stream is `Done` with nothing left buffered, regardless
    //      of whether anything was ever sent. This covers an inbound one-way
    //      `Message::UserData`: the Stream goes straight to `Done` with NO
    //      outbound bytes, so `sent_any` is never armed and (a) never fires —
    //      without (b) `send_finished` would stay false forever, `is_terminal`
    //      could never hold, and the bridge + its QUIC bidi stream would leak
    //      (drained every tick by `drain_payload_only`, never reaped). The
    //      inbound push/pull / reliable-ping response also reaches (b) (the
    //      response was sent → `sent_any` AND `is_done()` are both true here,
    //      either branch fires it once under the `!send_finished` guard).
    //
    // Reaching this point guarantees `pending_out` is empty (a non-empty
    // `pending_out` returned early above) and `poll_transmit` is exhausted, so
    // the FIN is never emitted before the response buffer is loaded (an
    // inbound response pre-`stream_load_response` leaves the phase at
    // `InboundSendingResponse`, so `is_done()` is false and `sent_any` is
    // false — neither branch fires). `SendStream::finish` → `Send::finish`
    // only requires `state == Ready` (an empty `Ready` send half finishes
    // fine: it sets `fin_pending`); the FIN is ordered AFTER any buffered
    // bytes, so finishing here cannot truncate.
    //
    // A failed or `fatal` stream MUST NOT emit FIN: the pre-write deadline
    // check above already RESET our send half and cleared `pending_out`; a
    // late FIN on top of a RESET_STREAM would make a timed-out request look
    // complete to the peer (the intended outcome is a timed-out exchange,
    // not a delivered-late-then-FINned one). The same gate also
    // guards a stream that `pump_in` failed via `Stream::handle_data`: the
    // bridge is `fatal` but `sent_any` from a prior tick would otherwise
    // still fire the deferred finish in this tick.
    if !self.finish_called
      && !self.is_phase_failed()
      && self.stream.is_failed().is_none()
      && (self.sent_any || (self.stream.is_done() && self.pending_out.is_empty()))
    {
      // Ignoring Err: `SendStream::finish` returns `Err(FinishError)`
      // only if the stream is already finished or stopped — both
      // states satisfy "send half has reached its terminal phase"
      // which is what the deferred-finish path is trying to achieve.
      // The `finish_called` latch below prevents a second call.
      let _ = conn.send_stream(self.sid).finish();
      self.finish_called = true;
    }
    Ok(())
  }

  /// `true` once the bridge has reached a terminal [`BridgePhase`] —
  /// either [`BridgePhase::BothClosed`] (clean: both halves transport-
  /// retired) or [`BridgePhase::Failed`] (any failure path). The driver
  /// then stops pumping and runs [`Bridge::drain_then_reap`].
  ///
  /// Symmetric terminality: phase is the SINGLE source of truth.
  /// Reaches `true` iff the bridge has entered [`BridgePhase::BothClosed`]
  /// (natural FIN paths on both halves) or [`BridgePhase::Failed`] (any
  /// failure transition). The failure transitions retire BOTH QUIC
  /// halves (`SendStream::reset` + `RecvStream::stop`) atomically BEFORE
  /// flipping the phase, so a `Failed`-phase bridge has both halves
  /// retired by construction.
  ///
  /// **No `stream.is_failed()` fallback.** Every FSM-internal failure
  /// path that sets `stream.is_failed()` is observed by `pump_in` /
  /// `pump_out` and cascaded into [`BridgePhase::Failed`] via the
  /// atomic-retire-then-fail helpers. A bridge whose FSM is_failed
  /// but whose phase is still `Active`/`SendClosed`/`RecvClosed` is
  /// a transient inconsistency — the next pump observes it and
  /// transitions atomically. Falling back to `stream.is_failed()` for
  /// terminality would let a reap fire before the retirement step,
  /// orphaning QUIC stream state.
  pub(crate) fn is_terminal(&self) -> bool {
    matches!(self.phase, BridgePhase::BothClosed | BridgePhase::Failed(_))
  }

  /// Test-only: expose the effective [`EncryptionOptions`] the bridge
  /// stored after the force-disable in [`Self::new`]. Lets a QUIC test
  /// assert that a bridge handed an ENABLED keyring still ends up with a
  /// disabled `EncryptionOptions` — quinn already provides
  /// confidentiality, so the reliable path skips its inner Encrypted
  /// wrapper. Gated on `aes-gcm` (the asserting test builds a
  /// `Keyring`/`SecretKey` from `memberlist-wire`).
  ///
  /// [`EncryptionOptions`]: crate::EncryptionOptions
  #[cfg(all(test, feature = "aes-gcm"))]
  pub(crate) fn encryption_for_test(&self) -> &crate::EncryptionOptions {
    &self.encryption
  }

  /// Test-only: feed `bytes` into `recv_accum` and run the inbound label
  /// classifier once. Returns `Ok(true)` when the latch is now set (either it
  /// was already set or it just flipped), `Ok(false)` when the header is still
  /// `Incomplete`, and `Err(reason)` when the header is `Rejected`.
  ///
  /// This drives the same classify path that `pump_in`'s chunk loop runs,
  /// without requiring a real quinn connection. Lets label-unit tests drive
  /// the validation state machine directly.
  #[cfg(test)]
  pub(crate) fn push_recv_and_classify(
    &mut self,
    bytes: &[u8],
  ) -> Result<bool, crate::label::LabelError> {
    self.recv_accum.extend_from_slice(bytes);
    if self.inbound_label_validated {
      return Ok(true);
    }
    let expected = self.label.as_deref();
    match classify_header(&self.recv_accum, expected, self.skip_inbound_label_check) {
      LabelVerdict::Accepted(consumed) => {
        self.recv_accum.drain(..consumed);
        self.inbound_label_validated = true;
        if !self.eager_outbound_label && !self.outbound_label_written {
          if let Some(lbl) = &self.label {
            encode_label_prefix(lbl, &mut self.pending_out);
          }
          self.outbound_label_written = true;
        }
        Ok(true)
      }
      LabelVerdict::Incomplete => Ok(false),
      LabelVerdict::Rejected(e) => Err(e),
    }
  }

  /// Test-only: `true` once the inbound label latch is set.
  #[cfg(test)]
  pub(crate) fn inbound_label_validated(&self) -> bool {
    self.inbound_label_validated
  }

  /// Test-only: `true` once the outbound label frame has been staged.
  #[cfg(test)]
  pub(crate) fn outbound_label_written(&self) -> bool {
    self.outbound_label_written
  }

  /// Test-only: the bytes currently staged in `pending_out`.
  #[cfg(test)]
  pub(crate) fn pending_out_bytes(&self) -> &[u8] {
    &self.pending_out
  }
}

impl<I, A> Bridge<I, A>
where
  A: crate::Data + crate::CheapClone + PartialEq + 'static,
  I: crate::Id,
{
  /// Pump inbound quinn bytes into the memberlist stream.
  ///
  /// Returns `Err(())` if the quinn read or the memberlist decode failed
  /// fatally (illegal ordered read, peer reset, decode error); the bridge is
  /// marked `fatal` so the caller begins the D1 reap promptly rather than
  /// waiting out the exchange deadline. `Ok(())` covers the normal and
  /// would-block paths.
  ///
  /// The [`quinn_proto::Chunks`] handle is finalized on **every** path
  /// (normal end, would-block, peer reset, decode failure) so quinn issues
  /// flow-control credit and does not leave the peer blocked. (Its `Drop`
  /// also finalizes, but the explicit call keeps the credit-release point
  /// deterministic.)
  pub(crate) fn pump_in(&mut self, conns: &mut ConnTable, now: Instant) -> Result<(), ()> {
    let Some(entry) = conns.get_mut(self.ch) else {
      return Ok(());
    };
    let conn = entry.conn_mut();

    // Scope the `RecvStream`/`Chunks` borrow so we can re-borrow `conn`
    // after the read for atomic half retirement on any failure path.
    //
    // **Incremental feed.** Each chunk is handed to `Stream::handle_data`
    // immediately, so the FSM's `max_stream_frame_size` cap applies BEFORE
    // the bridge accumulates bytes. A previous staging-vector design
    // copied every available chunk into one local `Vec` before calling
    // `handle_data`, which let an adversarial peer force allocation of
    // up to the QUIC `stream_receive_window` per bridge — bypassing the
    // application-layer frame cap. Feeding per-chunk preserves the
    // configured memory bound as the effective limit.
    let mut fin_seen = false;
    let mut reset = false;
    let mut illegal_ordered = false;
    let mut decode_failed = false;
    {
      let mut rs = conn.recv_stream(self.sid);
      match rs.read(true) {
        Ok(mut chunks) => {
          loop {
            match chunks.next(usize::MAX) {
              Ok(Some(chunk)) => {
                self.recv_accum.extend_from_slice(&chunk.bytes);

                // Inbound label validation — runs before any reliable unit
                // reaches the FSM. While the latch is unset, accumulate chunks
                // and classify; a split label header is held in recv_accum
                // until a later chunk completes it.
                //
                // CRITICAL INVARIANT: when label is None, inbound_label_validated
                // starts true, so this block is unreachable — byte-identical to
                // today's behavior, no classify_header call is ever made.
                if !self.inbound_label_validated {
                  let expected = self.label.as_deref();
                  match classify_header(&self.recv_accum, expected, self.skip_inbound_label_check) {
                    LabelVerdict::Accepted(consumed) => {
                      self.recv_accum.drain(..consumed);
                      self.inbound_label_validated = true;
                      // Acceptor lazy release: now that the peer's label is
                      // validated, the acceptor may disclose its own. Prepend
                      // the label frame to pending_out (guaranteed empty here —
                      // acceptors hold outbound_label_written=false until now,
                      // so no bytes have been sent yet).
                      if !self.eager_outbound_label && !self.outbound_label_written {
                        if let Some(lbl) = &self.label {
                          encode_label_prefix(lbl, &mut self.pending_out);
                        }
                        self.outbound_label_written = true;
                      }
                    }
                    LabelVerdict::Incomplete => {
                      // Not enough bytes yet; hold recv_accum and wait for the
                      // next chunk.
                      break;
                    }
                    LabelVerdict::Rejected(_) => {
                      decode_failed = true;
                      break;
                    }
                  }
                }

                // Drain every COMPLETE `[unit_len][payload]` unit; a trailing
                // partial unit stays buffered for a later chunk. `stream` is a
                // plain `Stream` here (no Option), so feed it directly.
                let mut unit_err = false;
                loop {
                  match self.take_reliable_unit(&self.recv_accum, self.reliable_max) {
                    Ok(Some((plaintext, consumed))) => {
                      self.recv_accum.drain(..consumed);
                      if self.stream.handle_data(&plaintext, now).is_err() {
                        decode_failed = true;
                        break;
                      }
                    }
                    // Need more bytes — keep the partial, read more next chunk.
                    Ok(None) => break,
                    // A corrupt unit (bad inner wrapper, or over-ceiling
                    // `unit_len`): terminalize via the post-borrow decode-fail
                    // block (the same path a `handle_data` decode failure takes).
                    Err(_) => {
                      unit_err = true;
                      break;
                    }
                  }
                }
                if decode_failed || unit_err {
                  decode_failed = true;
                  break;
                }
              }
              Ok(None) => {
                // End-of-stream: peer sent FIN. quinn's `RecvState` has
                // reached `DataRecvd`. Defer the phase transition until
                // after the borrow ends.
                fin_seen = true;
                break;
              }
              Err(quinn_proto::ReadError::Blocked) => break,
              Err(quinn_proto::ReadError::Reset(_)) => {
                reset = true;
                break;
              }
            }
          }
          // Ignoring Err: `Chunks::finalize` returns a wakeup signal we
          // don't use here (we drive readability per tick via `pump_in`,
          // not via wakeups). Release-side is unconditional so the peer
          // is not left flow-control blocked.
          let _ = chunks.finalize();
        }
        Err(quinn_proto::ReadableError::ClosedStream) => return Ok(()),
        Err(quinn_proto::ReadableError::IllegalOrderedRead) => {
          illegal_ordered = true;
        }
      }
    }
    // `rs` / `chunks` dropped; `conn` is freely re-borrowable for the
    // atomic-retire-then-fail failure paths below.

    if illegal_ordered {
      // Ignoring Err: idempotent retirement.
      let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
      let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
      self.pending_out.clear();
      self.fail(BridgeFailure::Transport("illegal ordered read".to_string()));
      return Err(());
    }

    // A trailing partial reliable unit at EOF is a truncated transmission —
    // the peer FINned mid-unit. Treat it as a decode failure (the FIN's own
    // `handle_data(&[])` premature-EOF path produces the same outcome for a
    // half frame, and the existing decode_failed block retires the halves).
    if fin_seen && !decode_failed && !self.recv_accum.is_empty() {
      decode_failed = true;
    }

    // FIN-with-EOF-signal: `Stream::handle_data(&[], now)` is the FSM's
    // PeerClosed entry (`stream.rs::handle_data_inner`: an empty buffer
    // returns `Err(StreamError::PeerClosed)` for non-terminal phases;
    // an already-terminal FSM ignores the call and returns `Ok(())`).
    // This routes a premature FIN — peer FINs while the FSM still
    // expects inbound bytes (outbound push/pull awaiting response,
    // reliable-ping awaiting ACK, or any partially-buffered inbound
    // frame) — through the FSM's failure path. Without it, an
    // adversarial peer could close mid-exchange and the bridge would
    // record a clean transport lifecycle (`BothClosed` via
    // `observe_recv_fin`) even though the exchange was incomplete.
    if fin_seen && self.stream.handle_data(&[], now).is_err() {
      decode_failed = true;
    }

    if decode_failed {
      // Atomic failure: retire both halves BEFORE flipping the phase.
      // The FSM's failure variant (Decode / PeerClosed / FrameTooLarge /
      // etc.) is preserved in `stream.is_failed()` for any caller that
      // queries it; `drain_then_reap`'s `StreamErrored` notice uses the
      // BridgeFailure::Decode mapping for the high-level reason.
      // Ignoring Err: idempotent retirement.
      let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
      let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
      self.pending_out.clear();
      self.fail(BridgeFailure::Decode);
      return Err(());
    }

    if reset {
      // Atomic failure on peer-reset recv.
      // Ignoring Err: idempotent retirement.
      let _ = conn.send_stream(self.sid).reset(VarInt::from_u32(0));
      let _ = conn.recv_stream(self.sid).stop(VarInt::from_u32(0));
      self.pending_out.clear();
      self.fail(BridgeFailure::Transport("peer reset recv".to_string()));
      return Err(());
    }

    // Only after the FSM has accepted the EOF (clean phase or already
    // terminal) do we transition the bridge's recv-half phase. A
    // PeerClosed rejection above sends us through the decode-fail path
    // BEFORE reaching here, so `RecvClosed` is the FSM-blessed
    // recv-half-retired transition.
    if fin_seen {
      self.observe_recv_fin();
    }

    Ok(())
  }

  /// D1 drain-before-reap. In strict order:
  ///
  /// 1. drain every queued [`Stream::poll_endpoint_event`] into the
  ///    [`Endpoint`] (the push/pull reply / `ReliablePingAcked` lives here);
  /// 2. route each returned [`StreamCommand`] back to this stream
  ///    (`SendPushPullResponse` -> encode + load + flush our reply;
  ///    `Close` -> RESET);
  /// 3. **only then** deliver the `StreamClosed` / `StreamErrored` lifecycle
  ///    notice.
  ///
  /// The caller removes the bridge **only after** this returns. The order is
  /// load-bearing: delivering the lifecycle notice before the payload events
  /// makes the `Endpoint` tear the exchange down before the join reply /
  /// reliable-ping ack is applied (see module docs).
  ///
  /// **Coordinator contract (D1):** the coordinator MUST route every
  /// non-terminal inbound stream's [`Stream::poll_endpoint_event`] through
  /// this function every tick — the inbound push/pull reply is encoded and
  /// loaded only inside the `SendPushPullResponse`/`Close` handling here, so
  /// skipping a tick silently drops the reply and leaves the peer with a
  /// half-applied merge.
  pub(crate) fn drain_then_reap<G>(
    &mut self,
    ep: &mut Endpoint<I, A, G>,
    conns: &mut ConnTable,
    now: Instant,
  ) where
    G: rand::Rng,
  {
    while let Some(ev) = self.stream.poll_endpoint_event() {
      if let Some(cmd) = ep.handle_stream_event(ev, now) {
        match cmd {
          StreamCommand::SendPushPullResponse(resp) => {
            let (local_states, user_data) = resp.into_parts();
            // `handle_stream_event` returns the response state UNENCODED and
            // the inbound stream's `output_buf` is still empty: the driver
            // must encode the snapshot and load it into the stream before
            // any of it can be transmitted. Without the encode + load the
            // inbound push/pull reply is never produced and the peer is left
            // with a half-applied merge (split-brain). `join = false` and the
            // 5s write deadline mirror the inbound-response path of the
            // approved memberlist-simulation driver.
            //
            // Response-deadline refresh in lockstep with the inner stream's
            // write deadline. `stream_load_response` sets the inner stream's
            // `deadline` to `now + 5s`; the bridge-level `self.deadline`
            // (the flush deadline that enforces the `Done`-but-unflushed
            // abandon in `pump_out`) MUST advance to the SAME value.
            // Without this refresh, a request that arrives near the
            // original accept deadline (`accept_time + stream_timeout`)
            // and whose response is flow-control-blocked would trip the
            // `Done`-but-unflushed abandon at the stale accept deadline
            // and reset/stop the bidi stream before the fresh response
            // write deadline elapses — the reply is truncated and the
            // peer is left with a half-applied merge. The shared local
            // (declared once, reused in both call args) keeps the two
            // deadlines byte-identical. The send-side timeout is reset
            // on the response leg for exactly this reason: the response
            // window must measure from response start, not from accept.
            let response_deadline = now + core::time::Duration::from_secs(5);
            let encoded =
              Endpoint::<I, A>::encode_push_pull_response(&local_states, user_data, false);
            Endpoint::<I, A>::stream_load_response(&mut self.stream, encoded, response_deadline);
            self.deadline = response_deadline;
            // Ignoring Err: `pump_out` failing here terminalizes the bridge,
            // which the next-tick `pump_bridges` reap reflects as
            // `StreamErrored` — no separate action to take.
            let _ = self.pump_out(conns, now);
          }
          StreamCommand::Close => {
            // Admission rejection (`MergeDelegate::notify_merge -> false`
            // on inbound push/pull join). Atomic failure: retire both
            // halves then transition phase to `Failed(AdmissionClosed)`
            // so the lifecycle notice surfaces as `StreamErrored`
            // carrying the rejection.
            self.fail_with_retire(conns, BridgeFailure::AdmissionClosed);
          }
        }
      }
    }

    // Recv-half retirement is structurally guaranteed by [`BridgePhase`]:
    //   * `BothClosed` — `observe_recv_fin` already fired on
    //     `Chunks::next == Ok(None)` (quinn-proto retires recv state on
    //     this read).
    //   * `Failed(_)` — the failure transition's caller explicitly
    //     `recv_stream(sid).stop(0)`'d before flipping the phase (see
    //     the leading-fatal guard, deadline-trip path, and the
    //     admission-rejection `StreamCommand::Close` arm above).
    // No additional `stop()` is needed here. An adversarial peer who
    // withholds FIN does not produce `BothClosed` — the bridge stays
    // non-terminal until `Bridge::deadline` elapses, at which point
    // `pump_out`'s deadline-trip path retires the recv half and
    // transitions to `Failed(Timeout)`.

    let id = self.stream.id();
    let fsm_failed = self.stream.is_failed();
    let notice = match (&self.phase, fsm_failed) {
      (BridgePhase::Failed(reason), _) => {
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
    // Ignoring Err: `handle_stream_event` returning a `StreamCommand` on
    // the post-payload lifecycle notice (`StreamClosed`/`StreamErrored`)
    // would be a no-op — the bridge is being reaped this tick and no
    // outbound work can run after the notice. The frozen `Endpoint`
    // contract today returns `None` for the lifecycle notice; the
    // discard keeps future-faithful behavior identical to current.
    let _ = ep.handle_stream_event(notice, now);
  }

  /// Drain a **non-terminal** stream's queued [`Stream::poll_endpoint_event`]
  /// into the [`Endpoint`] every tick, routing each returned [`StreamCommand`]
  /// with the **same** handling as [`Bridge::drain_then_reap`]
  /// (`SendPushPullResponse`: encode + `stream_load_response` + flush;
  /// `Close`: RESET) — but **without** the post-loop
  /// `StreamClosed`/`StreamErrored` lifecycle notice.
  ///
  /// The slot-gone notice tells the `Endpoint` the exchange is over and tears
  /// it down; delivering it for a stream that is still live would discard a
  /// not-yet-completed exchange. The encode + load **must** still run here,
  /// though: `handle_stream_event` returns the inbound push/pull response state
  /// UNENCODED with the stream's `output_buf` still empty, so without this
  /// per-tick encode + `stream_load_response` the reply for an in-flight
  /// inbound exchange is never produced and the peer is left with a
  /// half-applied merge (split-brain) — the exact failure the D1 module
  /// contract warns about. Mirrors [`Bridge::drain_then_reap`] minus step (3).
  pub(crate) fn drain_payload_only<G>(
    &mut self,
    ep: &mut Endpoint<I, A, G>,
    conns: &mut ConnTable,
    now: Instant,
  ) where
    G: rand::Rng,
  {
    // Defer endpoint-event commit until the recv half observes peer
    // FIN. A decoded frame's side effects (`PushPullRequestReceived`,
    // `PushPullReplyReceived`, `ReliablePingAcked`, `UserDataReceived`)
    // queue in `Stream::endpoint_events` on dispatch — BEFORE the
    // stream proves there are no trailing bytes. For split-delivery
    // `[valid_frame][later_junk]` arriving in separate quinn
    // `Chunks::next` iterations, chunk 1 dispatches into Done /
    // InboundSendingResponse with the event queued; chunk 2 arrives
    // in a later tick and fails the stream via the terminal-Done /
    // PhaseKind::Ignore guards. Without this gate, the chunk-1 side
    // effects would drain here on the intervening tick — committing
    // a merge / emitting `UserDataReceived` BEFORE the adversarial
    // trailing bytes could invalidate the stream. With the gate, the
    // drain waits until `BridgePhase::RecvClosed` (peer-FIN ack at
    // the QUIC layer, the protocol-level proof that no further bytes
    // can arrive). On adversarial split-delivery the chunk-2 failure
    // routes through `Stream::enter_failed` which clears
    // `endpoint_events` + `stream_events` atomically before the
    // failed bridge ever reaches `drain_then_reap`.
    //
    // The terminal D1 path (`drain_then_reap`) is already safe:
    // `BridgePhase::BothClosed` requires `observe_recv_fin` (peer
    // FIN); `BridgePhase::Failed` requires `enter_failed` (queue
    // already cleared). Cooperative peers that FIN in the same QUIC
    // delivery as the request/response bytes pay no extra latency —
    // `observe_recv_fin` fires the same tick. Only peers that split
    // FIN from the payload across ticks pay one extra tick.
    if !matches!(self.phase, BridgePhase::RecvClosed) {
      return;
    }
    while let Some(ev) = self.stream.poll_endpoint_event() {
      if let Some(cmd) = ep.handle_stream_event(ev, now) {
        match cmd {
          StreamCommand::SendPushPullResponse(resp) => {
            let (local_states, user_data) = resp.into_parts();
            // Mirror `drain_then_reap`'s response-deadline refresh: the
            // bridge-level `self.deadline` is advanced to the SAME `now + 5s`
            // value `stream_load_response` writes into the inner stream's
            // own deadline so the `Done`-but-unflushed abandon does not
            // fire on the stale accept deadline before the fresh response
            // window elapses (see the `drain_then_reap` arm's full
            // rationale).
            let response_deadline = now + core::time::Duration::from_secs(5);
            let encoded =
              Endpoint::<I, A>::encode_push_pull_response(&local_states, user_data, false);
            Endpoint::<I, A>::stream_load_response(&mut self.stream, encoded, response_deadline);
            self.deadline = response_deadline;
            // Ignoring Err: a `pump_out` failure terminalizes the bridge,
            // which the next-tick `pump_bridges` reap reflects.
            let _ = self.pump_out(conns, now);
          }
          StreamCommand::Close => {
            // A `Close` from `Endpoint::handle_stream_event` on a non-
            // terminal bridge is typically a MergeDelegate / AliveDelegate
            // rejection on an inbound push/pull exchange. Terminalize the
            // bridge in this same tick so `pump_bridges`'s post-
            // `drain_payload_only` `is_terminal()` re-check (mod.rs) fires
            // and the bridge D1-drains + reaps within the SAME tick —
            // without this the bridge would sit in `self.bridges` holding
            // a quinn bidi stream until its exchange deadline elapses
            // (~5 s by default), letting a rejected peer pin
            // `Bridge`+stream resources per attempt.
            self.fail_with_retire(conns, BridgeFailure::AdmissionClosed);
          }
        }
      }
    }
  }
}

#[cfg(test)]
mod tests;
