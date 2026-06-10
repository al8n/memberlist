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
  event::{EndpointEvent, StreamClosed, StreamCommand, StreamErrored, StreamId},
  label::{LabelOutcome, classify_header, encode_label_prefix},
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
  compression: crate::CompressionOptions,
  /// Cross-transport encryption configuration. The QUIC reliable path
  /// always skips the inner Encrypted wrapper — quinn-encrypted streams
  /// already provide confidentiality, and double-encrypting costs CPU
  /// and bandwidth without adding security. [`Self::new`] force-disables
  /// this field regardless of the caller's intent; the field is retained
  /// so [`Self::pump_out`] / [`Self::pump_in`] can call the
  /// encryption-aware codec helpers uniformly (a disabled
  /// `EncryptionOptions` makes those helpers byte-identical to the
  /// non-encryption variants).
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
  I: crate::Id
    + crate::Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: crate::Data
    + crate::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
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
    compression: crate::CompressionOptions,
    _encryption: crate::EncryptionOptions,
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
    // codec helpers below (`encode_reliable_unit_with_encryption` /
    // `take_reliable_unit_with_encryption`) collapse to the non-encryption
    // variants when handed a disabled `EncryptionOptions`, keeping the
    // on-wire bytes byte-identical to the pre-encryption-port shape.
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
      compression,
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

  /// The memberlist [`StreamId`] that correlates this bridge to its
  /// [`Stream`] — the observation seam for the async driver shell to map a
  /// reliable exchange back to its `StreamId`. The Sans-I/O coordinator keys
  /// `bridges` by `stream.id()` directly at `accept`/`dial` time and does
  /// not call this accessor in-crate, hence the narrowly-scoped allow below.
  #[allow(dead_code)]
  pub(crate) fn id(&self) -> StreamId {
    self.stream.id()
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
      // already encrypts the stream), so the encryption layer never fails
      // here. The fallible `Result` is still matched — the codec helper's
      // signature is shared with the StreamBridge path — but on this path the
      // error branch is unreachable in practice.
      let unit = match crate::encode_reliable_unit_with_encryption(
        &self.compression,
        &self.encryption,
        &gathered,
      ) {
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
                    LabelOutcome::Accepted(consumed) => {
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
                    LabelOutcome::Incomplete => {
                      // Not enough bytes yet; hold recv_accum and wait for the
                      // next chunk.
                      break;
                    }
                    LabelOutcome::Rejected(_) => {
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
                  match crate::take_reliable_unit_with_encryption(
                    &self.recv_accum,
                    &self.encryption,
                    self.reliable_max,
                  ) {
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
  pub(crate) fn drain_then_reap(
    &mut self,
    ep: &mut Endpoint<I, A>,
    conns: &mut ConnTable,
    now: Instant,
  ) {
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
  pub(crate) fn drain_payload_only(
    &mut self,
    ep: &mut Endpoint<I, A>,
    conns: &mut ConnTable,
    now: Instant,
  ) {
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

  /// Test-only: expose the effective [`EncryptionOptions`] the bridge
  /// stored after the force-disable in [`Self::new`]. Lets a QUIC test
  /// assert that a bridge handed an ENABLED keyring still ends up with a
  /// disabled `EncryptionOptions` — quinn already provides
  /// confidentiality, so the reliable path skips its inner Encrypted
  /// wrapper. Gated on `encryption-aes-gcm` (the asserting test builds a
  /// `Keyring`/`SecretKey` from `memberlist-wire`).
  ///
  /// [`EncryptionOptions`]: crate::EncryptionOptions
  #[cfg(all(test, feature = "encryption-aes-gcm"))]
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
      LabelOutcome::Accepted(consumed) => {
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
      LabelOutcome::Incomplete => Ok(false),
      LabelOutcome::Rejected(e) => Err(e),
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

#[cfg(test)]
mod tests {
  use super::*;
  use core::net::{IpAddr, Ipv4Addr, SocketAddr};

  use crate::typed::Message;
  use bytes::Bytes;
  use quinn_proto::{Dir, Side};
  use smol_str::SmolStr;

  use crate::{config::EndpointOptions, event::Event};

  /// `drain_payload_only` must not commit a dispatched frame's side
  /// effects until the recv half has observed peer FIN. Without this
  /// gate, a split-delivery `[valid_frame][later_junk]` (chunk 1
  /// dispatches and is drained on tick 1; chunk 2 fails the stream on
  /// tick 2) commits the merge / emits `UserDataReceived` BEFORE chunk
  /// 2 can invalidate the stream. The verification observable here: a
  /// UserData inbound frame queues `EndpointEvent::UserDataReceived`,
  /// which `drain_payload_only` would normally convert into
  /// `Event::UserPacket` on the Endpoint. While the bridge is `Active`,
  /// no `Event::UserPacket` may surface. Once `observe_recv_fin` flips
  /// the bridge to `RecvClosed`, the drain runs and the event surfaces.
  #[test]
  fn drain_payload_only_defers_until_recv_fin_observed() {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();

    // Bridge wraps a fresh accept_stream — the construction-time
    // invariant for `Bridge::new` is a pre-Done stream with a Some
    // exchange deadline. Dispatching the frame BELOW transitions the
    // FSM out of `InboundAwaitingFirstMessage`, which is why the
    // bridge is constructed first.
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    let mut bridge = Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      None,
      false,
      false,
    );
    assert!(matches!(bridge.phase, BridgePhase::Active));

    // Dispatch the UserData frame through the bridge's inner FSM. The
    // FSM transitions to `Done` and queues `EndpointEvent::UserDataReceived`
    // — but the bridge phase stays `Active` because the recv half has
    // NOT yet observed peer FIN.
    let msg = Message::<SmolStr, SocketAddr>::UserData(Bytes::from_static(b"hello"));
    let bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&msg).expect("encode");
    bridge
      .stream
      .handle_data(&bytes, t0)
      .expect("dispatch user data frame");

    // Pre-drain: the Endpoint must have no observable side effects from
    // this stream.
    let mut conns = ConnTable::new();
    assert!(
      !ep
        .poll_event()
        .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
      "no UserPacket may be queued before any drain"
    );

    // Drain attempt #1 — bridge is Active. The deferred-commit gate
    // MUST block.
    bridge.drain_payload_only(&mut ep, &mut conns, t0);
    assert!(
      ep.poll_event().is_none(),
      "drain_payload_only on an Active bridge MUST be a no-op — \
       the dispatched frame's `UserDataReceived` event MUST stay \
       queued until peer FIN proves no trailing bytes can invalidate \
       this stream"
    );

    // Flip the bridge to RecvClosed (peer FIN observed).
    bridge.observe_recv_fin();
    assert!(matches!(bridge.phase, BridgePhase::RecvClosed));

    // Drain attempt #2 — gate releases. The queued event flows through
    // `Endpoint::handle_stream_event` which emits `Event::UserPacket`.
    bridge.drain_payload_only(&mut ep, &mut conns, t0);
    let ev = ep.poll_event();
    assert!(
      matches!(&ev, Some(Event::UserPacket(p)) if p.data_ref().as_ref() == b"hello"),
      "drain_payload_only on a RecvClosed bridge MUST commit the \
       queued UserData event — got {ev:?}"
    );
  }

  /// A transport-level failure BEFORE peer FIN must not commit the
  /// queued endpoint events. `drain_payload_only` gates on
  /// `BridgePhase::RecvClosed`, but `drain_then_reap` is the terminal
  /// D1 path and drains the FSM queue unconditionally to enforce D1
  /// before lifecycle delivery. When `Bridge::fail` is driven by a
  /// transport signal (peer RESET, ConnectionLost) while the bridge is
  /// still `Active` / `SendClosed` (recv FIN never observed), the queue
  /// must be cleared at the failure transition — symmetric with
  /// `Stream::enter_failed`'s queue-clearing on the FSM-failure path.
  /// Without this, a peer that sends one complete `UserData` /
  /// `PushPull` frame and then resets the connection before FIN could
  /// commit `Event::UserPacket` / a merge through the terminal drain
  /// path.
  #[test]
  fn pre_fin_transport_failure_discards_queued_payload_events() {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();

    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    let mut bridge = Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      None,
      false,
      false,
    );
    assert!(matches!(bridge.phase, BridgePhase::Active));

    // Dispatch a UserData frame — FSM → Done, queues
    // `EndpointEvent::UserDataReceived` AND `StreamEvent::Closed`.
    // The bridge is STILL Active (recv FIN not observed).
    let msg = Message::<SmolStr, SocketAddr>::UserData(Bytes::from_static(b"hello"));
    let bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&msg).expect("encode");
    bridge
      .stream
      .handle_data(&bytes, t0)
      .expect("dispatch user data frame");

    // Simulate a transport-level failure BEFORE peer FIN. ConnectionLost
    // (or peer RESET) routes through `Bridge::fail`, which MUST clear
    // the FSM queue here: phase was Active (no FIN observed).
    bridge.fail_connection_lost();
    assert!(matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::ConnectionLost)
    ));

    // Drive the terminal D1 reap path. Without the pre-FIN queue clear
    // in `Bridge::fail`, `drain_then_reap` would drain
    // `UserDataReceived` into `Endpoint::handle_stream_event`, which
    // would emit `Event::UserPacket`. With the clear, the queue is
    // already empty — only the `StreamErrored` lifecycle notice is
    // delivered (visible via the FSM's `StreamEvent::Failed` after
    // `handle_stream_event(StreamErrored{...})`).
    let mut conns = ConnTable::new();
    bridge.drain_then_reap(&mut ep, &mut conns, t0);

    // Public observable: NO `Event::UserPacket` may surface. The
    // dispatched frame's side effects were never authorized by peer
    // FIN and the transport-level failure does not authorize them
    // either.
    let mut saw_user_packet = false;
    while let Some(ev) = ep.poll_event() {
      if matches!(ev, Event::UserPacket(..)) {
        saw_user_packet = true;
      }
    }
    assert!(
      !saw_user_packet,
      "a transport-level failure BEFORE recv FIN must NOT commit the \
       dispatched frame's side effects — `Event::UserPacket` would \
       mean the deferred-commit gate was bypassed by the terminal \
       drain path"
    );
  }

  #[test]
  fn bridge_module_compiles() {
    fn _assert<I, A>()
    where
      I: crate::Id
        + crate::Data
        + crate::CheapClone
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
      A: crate::Data
        + crate::CheapClone
        + Eq
        + core::hash::Hash
        + core::fmt::Debug
        + core::fmt::Display
        + Send
        + Sync
        + 'static,
    {
      let _: fn(
        Stream<I, A>,
        ConnectionHandle,
        QuicSid,
        crate::CompressionOptions,
        crate::EncryptionOptions,
        usize,
        Option<bytes::Bytes>,
        bool,
        bool,
      ) -> Bridge<I, A> = Bridge::<I, A>::new;
    }
  }

  #[cfg(feature = "compression-lz4")]
  #[test]
  fn quic_reliable_unit_accumulation_roundtrips() {
    use crate::{CompressAlgorithm, CompressionOptions, encode_reliable_unit, take_reliable_unit};
    let opts = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(8);
    let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
    let unit = encode_reliable_unit(&opts, &framed);
    let mut accum = unit.clone();
    let (back, consumed) = take_reliable_unit(&accum, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("a complete unit is present");
    accum.drain(..consumed);
    assert_eq!(back, framed);
    assert!(accum.is_empty());
  }

  #[cfg(feature = "compression-lz4")]
  #[test]
  fn quic_reliable_unit_split_across_two_chunks_buffers_then_completes() {
    // A reliable unit may arrive split across two `chunks.next` yields. The
    // accumulator must hold the first partial chunk (no frame yet) and
    // complete on the second.
    use crate::{CompressAlgorithm, CompressionOptions, encode_reliable_unit, take_reliable_unit};
    let opts = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(8);
    let framed = b"the quick brown fox jumps over the lazy dog".repeat(32);
    let unit = encode_reliable_unit(&opts, &framed);
    assert!(unit.len() > 4, "unit is large enough to split");
    let split = unit.len() / 2;

    let mut accum: Vec<u8> = Vec::new();
    accum.extend_from_slice(&unit[..split]);
    assert!(
      take_reliable_unit(&accum, 16 * 1024 * 1024)
        .expect("a partial unit is not an error")
        .is_none(),
      "the first partial chunk must buffer, not decode"
    );
    accum.extend_from_slice(&unit[split..]);
    let (back, consumed) = take_reliable_unit(&accum, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("the unit is complete after the second chunk");
    accum.drain(..consumed);
    assert_eq!(back, framed, "the split unit decompresses to the original");
    assert!(accum.is_empty());
  }

  #[test]
  fn quic_reliable_unit_disabled_is_byte_identical() {
    use crate::{CompressionOptions, encode_reliable_unit, take_reliable_unit};
    let opts = CompressionOptions::new();
    let framed = b"plain reliable frame bytes that are not compressed".to_vec();
    let unit = encode_reliable_unit(&opts, &framed);
    let (back, consumed) = take_reliable_unit(&unit, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("a complete unit");
    assert_eq!(back, framed);
    assert_eq!(consumed, unit.len());
  }

  #[cfg(feature = "encryption-aes-gcm")]
  fn build_test_quic_bridge_with_encryption(
    encryption: crate::EncryptionOptions,
  ) -> Bridge<SmolStr, SocketAddr> {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      encryption,
      ep.max_stream_frame_size(),
      None,
      false,
      false,
    )
  }

  /// A QUIC `Bridge` built with an ENABLED `EncryptionOptions` ends up with
  /// a DISABLED effective `EncryptionOptions`: the 9-arg `Bridge::new` zeroes
  /// the encryption field unconditionally. The on-wire reliable bytes
  /// therefore carry no `Encrypted` wrapper — quinn already provides
  /// confidentiality, and double-encrypting on the reliable path costs CPU
  /// and bandwidth without adding security.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn quic_bridge_reliable_skips_encryption_unconditionally() {
    use crate::{EncryptionOptions, Keyring, SecretKey};
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0; 32])));
    let bridge = build_test_quic_bridge_with_encryption(opts);
    assert!(
      !bridge.encryption_for_test().is_enabled(),
      "QUIC bridge zeroes encryption — quinn already encrypts the stream"
    );
  }

  // ── Label mechanism helpers ────────────────────────────────────────────────

  fn make_labeled_dialer(label: bytes::Bytes) -> Bridge<SmolStr, SocketAddr> {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      Some(label),
      false,
      true,
    )
  }

  fn make_labeled_acceptor(label: bytes::Bytes) -> Bridge<SmolStr, SocketAddr> {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      Some(label),
      false,
      false,
    )
  }

  fn make_labeled_acceptor_skip(label: bytes::Bytes) -> Bridge<SmolStr, SocketAddr> {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      Some(label),
      true,
      false,
    )
  }

  fn build_label_header(label: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    crate::label::encode_label_prefix(label, &mut buf);
    buf
  }

  // ── Label mechanism tests ──────────────────────────────────────────────────

  /// Two bridges with the same label — the dialer validates the acceptor's
  /// inbound label in one shot, and the acceptor validates the dialer's.
  /// Both `inbound_label_validated` latches flip to `true` after receiving
  /// the matching header from the other side.
  #[test]
  fn labeled_exchange_same_label_both_validate() {
    let label = bytes::Bytes::from_static(b"cluster-x");

    // Dialer: inbound_label_validated starts false (label is Some).
    let mut dialer = make_labeled_dialer(label.clone());
    assert!(
      !dialer.inbound_label_validated(),
      "dialer starts with inbound latch unset"
    );
    // Dialer is eager: outbound label is staged on first pump opportunity
    // (pending_out still empty at construction; the label gets pushed
    // when pump_out runs — but our test helper drives it directly).
    // Drive inbound classification with the acceptor's label header.
    let header = build_label_header(b"cluster-x");
    assert!(
      dialer
        .push_recv_and_classify(&header)
        .expect("should not fail"),
      "dialer: matching inbound label is accepted"
    );
    assert!(
      dialer.inbound_label_validated(),
      "dialer inbound latch set after matching header"
    );

    // Acceptor: inbound_label_validated starts false (label is Some).
    let mut acceptor = make_labeled_acceptor(label.clone());
    assert!(
      !acceptor.inbound_label_validated(),
      "acceptor starts with inbound latch unset"
    );
    assert!(
      !acceptor.outbound_label_written(),
      "acceptor outbound latch unset before inbound validation"
    );
    // Drive inbound classification with the dialer's label header.
    assert!(
      acceptor
        .push_recv_and_classify(&header)
        .expect("should not fail"),
      "acceptor: matching inbound label is accepted"
    );
    assert!(
      acceptor.inbound_label_validated(),
      "acceptor inbound latch set after matching header"
    );
    // Acceptor lazy release: outbound label must have been staged.
    assert!(
      acceptor.outbound_label_written(),
      "acceptor outbound latch set after inbound validates"
    );
    // The staged bytes must be the expected label frame.
    let expected_frame = build_label_header(b"cluster-x");
    assert_eq!(
      acceptor.pending_out_bytes(),
      expected_frame.as_slice(),
      "acceptor pending_out must contain the label frame after lazy release"
    );
  }

  /// A mismatched inbound label is rejected before any reliable unit reaches
  /// the FSM — `classify_header` returns `Rejected(Mismatch)`.
  #[test]
  fn labeled_bridge_mismatched_inbound_label_rejected() {
    let label = bytes::Bytes::from_static(b"cluster-x");
    let mut acceptor = make_labeled_acceptor(label);

    // Send a label frame claiming a DIFFERENT cluster.
    let wrong_header = build_label_header(b"cluster-y");
    let result = acceptor.push_recv_and_classify(&wrong_header);
    assert!(
      matches!(result, Err(crate::label::LabelError::Mismatch)),
      "a mismatched inbound label must be Rejected(Mismatch) — got {result:?}"
    );
    // The FSM must not have received any data (recv_accum still has the
    // rejected header bytes, latch is still unset).
    assert!(
      !acceptor.inbound_label_validated(),
      "inbound latch must remain unset after a rejected label"
    );
  }

  /// `skip_inbound_label_check=true` on an acceptor allows an unlabeled peer
  /// (no `[12]` header present). The inbound latch flips to `true` consuming 0
  /// bytes from recv_accum.
  #[test]
  fn labeled_bridge_skip_accepts_unlabeled_inbound() {
    let label = bytes::Bytes::from_static(b"cluster-x");
    let mut acceptor = make_labeled_acceptor_skip(label);

    // Inbound peer sends a plain reliable unit byte-sequence (no label header).
    // The first byte is NOT 12, so classify_header returns Accepted(0) with
    // skip=true.
    let payload = b"some plain payload data";
    let validated = acceptor
      .push_recv_and_classify(payload)
      .expect("skip=true must not fail for an unlabeled inbound");
    assert!(validated, "skip must accept an unlabeled inbound");
    assert!(
      acceptor.inbound_label_validated(),
      "inbound latch must be set after skip-accepted unlabeled inbound"
    );
    // recv_accum retains the payload bytes (Accepted(0) consumes 0 bytes).
    assert_eq!(
      acceptor.pending_out_bytes().len(),
      build_label_header(b"cluster-x").len(),
      "acceptor must have staged its own label frame after lazy release"
    );
  }

  /// An unlabeled bridge (label=None) passes a 12-byte first reliable unit
  /// through unchanged — no false reject, no classify_header call.
  ///
  /// Faithful to `memberlist-core/src/network.rs`: an unlabeled coordinator
  /// treats a 12-byte first unit as a normal reliable frame, not as a label
  /// header to parse. inbound_label_validated starts true so the classifier
  /// is never invoked.
  #[test]
  fn unlabeled_bridge_twelve_byte_first_unit_passes_through() {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    let bridge = Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      None,
      false,
      false,
    );
    // The latch starts true for an unlabeled bridge — the classifier is
    // never invoked, so a first unit whose varint-encoded length happens
    // to be 12 (== LABELED_TAG) is never falsely classified as a label frame.
    assert!(
      bridge.inbound_label_validated(),
      "unlabeled bridge: inbound latch must start true — byte-identical to today"
    );
    // Outbound latch is also true: no label frame is ever written.
    assert!(
      bridge.outbound_label_written(),
      "unlabeled bridge: outbound latch must start true — no label frame emitted"
    );
  }

  /// A label header split across two `push_recv_and_classify` calls validates
  /// only once the full header is present. The first partial push returns
  /// `Ok(false)` (Incomplete); the second push with the remaining bytes
  /// returns `Ok(true)` (Accepted).
  #[test]
  fn labeled_bridge_split_label_header_validates_on_completion() {
    let label = bytes::Bytes::from_static(b"cluster-x");
    let header = build_label_header(b"cluster-x");
    assert!(header.len() > 3, "label header large enough to split");

    let split = header.len() / 2;
    let mut acceptor = make_labeled_acceptor(label);

    // First half — Incomplete.
    let result = acceptor
      .push_recv_and_classify(&header[..split])
      .expect("partial header must not fail");
    assert!(
      !result,
      "partial label header must return Incomplete (Ok(false))"
    );
    assert!(
      !acceptor.inbound_label_validated(),
      "inbound latch must remain unset after partial header"
    );

    // Second half — Accepted.
    let result = acceptor
      .push_recv_and_classify(&header[split..])
      .expect("completing the header must not fail");
    assert!(
      result,
      "completing the label header must return Accepted (Ok(true))"
    );
    assert!(
      acceptor.inbound_label_validated(),
      "inbound latch must be set after the split header completes"
    );
  }

  /// The acceptor MUST NOT write its outbound label before the inbound label
  /// validates. Before `push_recv_and_classify` is called, `pending_out` must
  /// be empty and `outbound_label_written` must be false.
  #[test]
  fn acceptor_holds_outbound_label_until_inbound_validates() {
    let label = bytes::Bytes::from_static(b"cluster-x");
    let acceptor = make_labeled_acceptor(label);

    // At construction: outbound label must NOT be staged.
    assert!(
      !acceptor.outbound_label_written(),
      "acceptor: outbound latch must be false at construction"
    );
    assert!(
      acceptor.pending_out_bytes().is_empty(),
      "acceptor: pending_out must be empty at construction — no label written yet"
    );

    // Note: the dialer (eager=true) gets its label staged the first time
    // pump_out runs (when it calls encode_label_prefix into pending_out).
    // That path is tested via make_labeled_dialer + pump_out; here we only
    // assert the construction-time invariant for the acceptor.
    let dialer = make_labeled_dialer(bytes::Bytes::from_static(b"cluster-x"));
    // Dialer: outbound_label_written starts FALSE too — the label is written
    // into pending_out the first time pump_out fires (eager, not at construction).
    // At construction-time pending_out is empty.
    assert!(
      !dialer.outbound_label_written(),
      "dialer: outbound latch also false at construction — written on first pump_out"
    );
    assert!(
      dialer.pending_out_bytes().is_empty(),
      "dialer: pending_out must be empty at construction"
    );
  }

  // ── Phase / accessor / drain-then-reap coverage ────────────────────────────

  /// Build a plain unlabeled, encryption-disabled bridge over a freshly
  /// accepted stream. `ch = ConnectionHandle(0)` and an empty `ConnTable` mean
  /// the `pump_*`/`retire_halves` paths that look up `conns.get_mut(self.ch)`
  /// short-circuit — these tests drive phase + drain logic in isolation.
  fn make_plain_bridge() -> Bridge<SmolStr, SocketAddr> {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      None,
      false,
      false,
    )
  }

  /// `set_encryption` force-disables regardless of input (quinn already
  /// encrypts the reliable stream); `id`/`ch`/`sid` accessors report the
  /// construction values; `is_phase_failed` is false for a fresh `Active`
  /// bridge.
  #[test]
  fn bridge_accessors_and_set_encryption_force_disable() {
    let mut bridge = make_plain_bridge();
    assert!(
      !bridge.is_phase_failed(),
      "fresh bridge is Active, not Failed"
    );
    assert_eq!(bridge.ch(), ConnectionHandle(0));
    assert_eq!(bridge.sid(), QuicSid::new(Side::Client, Dir::Bi, 0));
    // `id()` returns the inner stream's StreamId — exercise the accessor.
    let _ = bridge.id();
    // Even handed a (would-be) enabled policy, the stored options stay
    // disabled on the QUIC reliable path.
    bridge.set_encryption(crate::EncryptionOptions::new());
    // The field is reachable directly from the child test module.
    assert!(
      !bridge.encryption.is_enabled(),
      "QUIC bridge encryption stays disabled after set_encryption"
    );
  }

  /// `poll_timeout` returns the bridge's own deadline while non-terminal and
  /// `None` once terminal (`BothClosed`/`Failed`) — the terminal short-circuit
  /// that keeps a reaped bridge from contributing a stale wake to the
  /// coordinator's unified `min`.
  #[test]
  fn bridge_poll_timeout_some_while_active_none_when_terminal() {
    let mut bridge = make_plain_bridge();
    assert!(
      bridge.poll_timeout().is_some(),
      "an Active bridge contributes its exchange deadline"
    );
    // Drive to BothClosed via the two FIN observers.
    bridge.observe_send_fin();
    assert!(matches!(bridge.phase, BridgePhase::SendClosed));
    bridge.observe_recv_fin();
    assert!(matches!(bridge.phase, BridgePhase::BothClosed));
    assert!(bridge.is_terminal(), "BothClosed is terminal");
    assert!(
      bridge.poll_timeout().is_none(),
      "a terminal bridge contributes no deadline"
    );
  }

  /// `observe_send_fin` / `observe_recv_fin` are sticky no-ops once terminal:
  /// a `Failed` bridge stays `Failed` after either observer fires.
  #[test]
  fn fin_observers_are_noops_on_terminal_phase() {
    let mut bridge = make_plain_bridge();
    bridge.fail_connection_lost();
    assert!(matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::ConnectionLost)
    ));
    bridge.observe_send_fin();
    bridge.observe_recv_fin();
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::ConnectionLost)
      ),
      "FIN observers must not overwrite a sticky Failed phase"
    );
  }

  /// `fail_stopped_already_retired` records the peer STOP_SENDING error code as
  /// a `Transport` failure and clears `pending_out`. Sticky: a follow-up
  /// `fail_connection_lost` does not overwrite the first cause.
  #[test]
  fn fail_stopped_already_retired_sets_transport_failure_and_is_sticky() {
    let mut bridge = make_plain_bridge();
    bridge.pending_out.extend_from_slice(b"stale outbound tail");
    bridge.fail_stopped_already_retired(quinn_proto::VarInt::from_u32(7));
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::Transport(_))
      ),
      "STOP_SENDING maps to a Transport failure"
    );
    assert!(
      bridge.pending_out.is_empty(),
      "the staged outbound tail is cleared on failure"
    );
    // First failure wins.
    bridge.fail_connection_lost();
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::Transport(_))
      ),
      "the first failure cause is sticky — ConnectionLost must not overwrite it"
    );
  }

  /// `drain_then_reap` selects the lifecycle notice from the bridge's terminal
  /// phase. For every `BridgeFailure` variant the bridge emits a
  /// `StreamErrored` notice (covering each reason-string arm); for a clean
  /// `BothClosed` it emits `StreamClosed`. Driven with an empty `ConnTable`
  /// (the directly-set `Failed` phase needs no half retirement at drain time)
  /// so the notice-selection match is exercised in isolation. Observable: no
  /// panic, and a clean reap surfaces no `UserPacket` (no payload was queued).
  #[test]
  fn drain_then_reap_notice_selection_covers_every_failure_reason() {
    let mut conns = ConnTable::new();
    let t0 = Instant::now();
    let reasons = [
      BridgeFailure::Timeout,
      BridgeFailure::Transport("boom".to_string()),
      BridgeFailure::Decode,
      BridgeFailure::ConnectionLost,
      BridgeFailure::AdmissionClosed,
      BridgeFailure::DialRetired,
      BridgeFailure::EncryptionPolicyChanged,
    ];
    for reason in reasons {
      let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
        SmolStr::new("self"),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
      ));
      let mut bridge = make_plain_bridge();
      bridge.phase = BridgePhase::Failed(reason);
      assert!(bridge.is_terminal());
      // Drains the (empty) FSM event queue then emits the StreamErrored notice
      // for this reason — the arm under test. No panic == arm executed.
      bridge.drain_then_reap(&mut ep, &mut conns, t0);
      assert!(
        !ep
          .poll_event()
          .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
        "a failed reap with no queued payload must not emit a UserPacket"
      );
    }
  }

  /// The clean-`BothClosed` arm of `drain_then_reap`'s notice selection emits
  /// `StreamClosed` (not `StreamErrored`). Driven in isolation on a bridge with
  /// no queued payload events.
  #[test]
  fn drain_then_reap_clean_bothclosed_emits_stream_closed() {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let mut conns = ConnTable::new();
    let t0 = Instant::now();
    let mut bridge = make_plain_bridge();
    bridge.observe_send_fin();
    bridge.observe_recv_fin();
    assert!(matches!(bridge.phase, BridgePhase::BothClosed));
    bridge.drain_then_reap(&mut ep, &mut conns, t0);
    // No payload was dispatched, so the only effect is the clean StreamClosed
    // lifecycle notice routed into the Endpoint — no UserPacket may surface.
    assert!(
      !ep
        .poll_event()
        .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
      "a clean reap with no queued payload must not emit a UserPacket"
    );
  }

  /// A `MergeDelegate` that rejects every inbound merge — drives
  /// `Endpoint::handle_stream_event(PushPullRequestReceived{Join})` to return
  /// `StreamCommand::Close`.
  struct RejectAllMerges;
  impl crate::delegate::MergeDelegate<SmolStr, SocketAddr> for RejectAllMerges {
    fn notify_merge(&self, _peers: &[crate::typed::NodeState<SmolStr, SocketAddr>]) -> bool {
      false
    }
  }

  /// Build an inbound bridge whose stream has already decoded a PushPull join
  /// request (FSM in `InboundSendingResponse`, `PushPullRequestReceived{Join}`
  /// queued), over an `Endpoint` that rejects every merge. Returns
  /// `(ep, conns, bridge)` so the caller drives the drain paths.
  fn inbound_join_bridge_with_rejecting_endpoint() -> (
    Endpoint<SmolStr, SocketAddr>,
    ConnTable,
    Bridge<SmolStr, SocketAddr>,
    Instant,
  ) {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    ep.set_merge_delegate(RejectAllMerges);
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
    let t0 = Instant::now();
    let stream = ep.accept_stream(peer, t0).expect("node is running");
    let ch = ConnectionHandle(0);
    let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
    let mut bridge = Bridge::new(
      stream,
      ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      None,
      false,
      false,
    );
    // Dispatch a join PushPull request so the FSM queues
    // `PushPullRequestReceived{Join}` — the event the rejecting delegate turns
    // into `StreamCommand::Close`.
    let dave = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7004);
    let dave_state =
      crate::typed::PushNodeState::new(1, SmolStr::new("dave"), dave, crate::typed::State::Alive);
    let pp =
      crate::typed::PushPull::new(true, core::iter::once(dave_state)).with_user_data(Bytes::new());
    let bytes =
      crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::PushPull(pp)).expect("encode");
    bridge
      .stream
      .handle_data(&bytes, t0)
      .expect("dispatch the join push/pull request");
    (ep, ConnTable::new(), bridge, t0)
  }

  /// `drain_payload_only`'s `StreamCommand::Close` arm: a rejected inbound join
  /// terminalizes the bridge (`Failed(AdmissionClosed)`) in the same drain. The
  /// deferred-commit gate is released first via `observe_recv_fin`.
  #[test]
  fn drain_payload_only_close_arm_terminalizes_on_rejected_merge() {
    let (mut ep, mut conns, mut bridge, t0) = inbound_join_bridge_with_rejecting_endpoint();
    // Release the RecvClosed gate so `drain_payload_only` actually drains.
    bridge.observe_recv_fin();
    assert!(matches!(bridge.phase, BridgePhase::RecvClosed));
    bridge.drain_payload_only(&mut ep, &mut conns, t0);
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::AdmissionClosed)
      ),
      "the rejected-merge Close command must terminalize the bridge as \
       AdmissionClosed — got {:?}",
      bridge.phase
    );
  }

  /// `drain_then_reap`'s `StreamCommand::Close` arm (the terminal D1 path
  /// drains unconditionally): the same rejected-join scenario terminalizes the
  /// bridge as `Failed(AdmissionClosed)` and delivers the `StreamErrored`
  /// notice (no `UserPacket` surfaces — the join state was rejected).
  #[test]
  fn drain_then_reap_close_arm_terminalizes_on_rejected_merge() {
    let (mut ep, mut conns, mut bridge, t0) = inbound_join_bridge_with_rejecting_endpoint();
    bridge.drain_then_reap(&mut ep, &mut conns, t0);
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::AdmissionClosed)
      ),
      "drain_then_reap's Close arm must terminalize as AdmissionClosed — got {:?}",
      bridge.phase
    );
    assert!(
      !ep
        .poll_event()
        .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
      "a rejected join must not surface application user data"
    );
  }

  /// `push_recv_and_classify` short-circuits to `Ok(true)` when the inbound
  /// label latch is already set — the `if self.inbound_label_validated` early
  /// return. An unlabeled bridge starts with the latch set, so any feed returns
  /// `Ok(true)` without invoking `classify_header`.
  #[test]
  fn push_recv_and_classify_returns_true_when_latch_already_set() {
    let mut bridge = make_plain_bridge();
    assert!(
      bridge.inbound_label_validated(),
      "an unlabeled bridge starts with the inbound latch set"
    );
    assert_eq!(
      bridge.push_recv_and_classify(b"arbitrary reliable bytes"),
      Ok(true),
      "a feed on an already-validated bridge returns Ok(true) immediately"
    );
  }

  /// `pump_in` / `pump_out` short-circuit to `Ok(())` when the bridge's
  /// connection handle is absent from the `ConnTable` (`conns.get_mut(self.ch)
  /// == None`) — the missing-connection guard. No phase change, no panic.
  #[test]
  fn pump_in_out_are_ok_noops_when_connection_absent() {
    let mut bridge = make_plain_bridge();
    let mut conns = ConnTable::new();
    let t0 = Instant::now();
    assert!(
      bridge.pump_in(&mut conns, t0).is_ok(),
      "pump_in is a no-op Ok when the connection is gone"
    );
    assert!(
      bridge.pump_out(&mut conns, t0).is_ok(),
      "pump_out is a no-op Ok when the connection is gone"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Active),
      "neither pump changes phase when the connection is absent"
    );
  }

  // ---------------------------------------------------------------------------
  // Real quinn-proto connection pair, for the bridge fault paths that only fire
  // when `pump_in` / `pump_out` operate over a LIVE quinn stream (the
  // `conns.get_mut(self.ch)` guard short-circuits over an empty `ConnTable`, so
  // the `make_plain_bridge` tests above cannot reach them). The harness drives a
  // real client↔server handshake by ferrying datagrams, parks the SERVER
  // connection in a `ConnTable` (where a `Bridge` finds it via its `ch`), and
  // keeps the CLIENT connection raw so a test can open/write/reset/finish a
  // single bidi stream and observe the server bridge's reaction.
  // ---------------------------------------------------------------------------

  use quinn_proto::{ConnectionHandle as QpCh, Endpoint as QuinnEndpoint, Transmit as QpTransmit};

  use crate::quic::crypto::tests::{test_client, test_endpoint_config, test_server};

  const CLIENT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6500);
  const SERVER_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6501);

  /// One real quinn client connection + one real quinn server connection, each
  /// driven directly (no `QuicEndpoint` coordinator). The server connection is
  /// parked in `server_conns` under `server_ch` so a `Bridge` can pump it.
  struct RawQuicPair {
    client_ep: QuinnEndpoint,
    server_ep: QuinnEndpoint,
    client_conn: quinn_proto::Connection,
    client_ch: QpCh,
    server_conns: ConnTable,
    server_ch: QpCh,
    now: std::time::Instant,
  }

  impl RawQuicPair {
    /// Mint a client+server endpoint pair and drive the handshake to
    /// Established on both sides by ferrying datagrams.
    fn handshaked() -> Self {
      let cfg = super::super::crypto::QuicOptions::new(
        test_endpoint_config(&[9u8; 32]),
        test_server(),
        test_client(),
        quinn_proto::TransportConfig::default(),
        "localhost",
        super::super::UnreliableTransport::Datagram,
      );
      let mut client_ep = QuinnEndpoint::new(cfg.endpoint_arc(), None, true, Some([0x42; 32]));
      let mut server_ep = QuinnEndpoint::new(
        cfg.endpoint_arc(),
        Some(cfg.server_arc()),
        true,
        Some([0x24; 32]),
      );
      let now = Instant::now().into_std();
      let (client_ch, client_conn) = client_ep
        .connect(now, cfg.client().clone(), SERVER_ADDR, "localhost")
        .expect("client dial");

      let mut server_conns = ConnTable::new();
      let mut client_conn = client_conn;
      let mut server_ch: Option<QpCh> = None;

      // Ferry datagrams both ways until both connections are Established.
      for _ in 0..200 {
        // Client → server.
        let mut buf = Vec::new();
        while let Some(t) = client_conn.poll_transmit(now, 1, &mut buf) {
          Self::deliver(
            &mut server_ep,
            &mut server_conns,
            &mut server_ch,
            CLIENT_ADDR,
            &t,
            &buf,
            now,
          );
          buf.clear();
        }
        // Server → client.
        if let Some(sch) = server_ch {
          let mut sbuf = Vec::new();
          if let Some(e) = server_conns.get_mut(sch) {
            while let Some(t) = e.conn_mut().poll_transmit(now, 1, &mut sbuf) {
              let data = bytes::BytesMut::from(&sbuf[..t.size]);
              let mut scratch = Vec::new();
              if let Some(ev) = client_ep.handle(now, SERVER_ADDR, None, None, data, &mut scratch) {
                Self::apply_client_event(&mut client_conn, client_ch, ev);
              }
              sbuf.clear();
            }
          }
        }
        // Drain endpoint events on both sides so CID issuance completes.
        Self::pump_endpoint_events(&mut client_ep, &mut client_conn, client_ch);
        if let Some(sch) = server_ch
          && let Some(e) = server_conns.get_mut(sch)
        {
          while let Some(ev) = e.conn_mut().poll_endpoint_events() {
            if ev.is_drained() {
              continue;
            }
            if let Some(cev) = server_ep.handle_event(sch, ev) {
              e.conn_mut().handle_event(cev);
            }
          }
        }
        client_conn.handle_timeout(now);
        if let Some(sch) = server_ch
          && let Some(e) = server_conns.get_mut(sch)
        {
          e.conn_mut().handle_timeout(now);
        }
        let client_up = !client_conn.is_handshaking() && !client_conn.is_closed();
        let server_up = server_ch
          .and_then(|sch| server_conns.get(sch))
          .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
          .unwrap_or(false);
        if client_up && server_up {
          break;
        }
      }

      let server_ch = server_ch.expect("server must have accepted the connection");
      assert!(
        !client_conn.is_handshaking(),
        "client connection must reach Established"
      );
      assert!(
        server_conns
          .get(server_ch)
          .map(|e| !e.conn_ref().is_handshaking())
          .unwrap_or(false),
        "server connection must reach Established"
      );
      Self {
        client_ep,
        server_ep,
        client_conn,
        client_ch,
        server_conns,
        server_ch,
        now,
      }
    }

    /// Route a client-originated datagram into the server endpoint, accepting a
    /// brand-new connection on first contact.
    fn deliver(
      server_ep: &mut QuinnEndpoint,
      server_conns: &mut ConnTable,
      server_ch: &mut Option<QpCh>,
      from: SocketAddr,
      t: &QpTransmit,
      buf: &[u8],
      now: std::time::Instant,
    ) {
      let data = bytes::BytesMut::from(&buf[..t.size]);
      let mut scratch = Vec::new();
      let Some(ev) = server_ep.handle(now, from, None, None, data, &mut scratch) else {
        return;
      };
      match ev {
        quinn_proto::DatagramEvent::ConnectionEvent(ch, cev) => {
          if let Some(e) = server_conns.get_mut(ch) {
            e.conn_mut().handle_event(cev);
          }
        }
        quinn_proto::DatagramEvent::NewConnection(incoming) => {
          let mut abuf = Vec::new();
          if let Ok((ch, conn)) = server_ep.accept(incoming, now, &mut abuf, None) {
            server_conns.insert_accepted(ch, conn, from);
            *server_ch = Some(ch);
          }
        }
        quinn_proto::DatagramEvent::Response(_) => {}
      }
    }

    fn apply_client_event(
      client_conn: &mut quinn_proto::Connection,
      client_ch: QpCh,
      ev: quinn_proto::DatagramEvent,
    ) {
      if let quinn_proto::DatagramEvent::ConnectionEvent(ch, cev) = ev {
        debug_assert_eq!(ch, client_ch);
        client_conn.handle_event(cev);
      }
    }

    fn pump_endpoint_events(
      client_ep: &mut QuinnEndpoint,
      client_conn: &mut quinn_proto::Connection,
      client_ch: QpCh,
    ) {
      while let Some(ev) = client_conn.poll_endpoint_events() {
        if ev.is_drained() {
          continue;
        }
        if let Some(cev) = client_ep.handle_event(client_ch, ev) {
          client_conn.handle_event(cev);
        }
      }
    }

    /// Open a bidi stream on the CLIENT and return its `StreamId`.
    fn client_open_bi(&mut self) -> QuicSid {
      self
        .client_conn
        .streams()
        .open(Dir::Bi)
        .expect("client opens a bidi stream")
    }

    /// Write `bytes` on the client's send half of `sid`.
    fn client_write(&mut self, sid: QuicSid, bytes: &[u8]) {
      let mut off = 0;
      while off < bytes.len() {
        match self.client_conn.send_stream(sid).write(&bytes[off..]) {
          Ok(n) => off += n,
          Err(quinn_proto::WriteError::Blocked) => {
            self.ferry();
            // After a ferry the peer may have credited us; retry.
          }
          Err(e) => panic!("client write failed: {e:?}"),
        }
      }
    }

    /// Reset the client's send half of `sid` (peer's recv half sees
    /// RESET_STREAM and `pump_in` reads `ReadError::Reset`).
    fn client_reset(&mut self, sid: QuicSid) {
      // Ignoring Err: `reset` returns `Err(ClosedStream)` only if the half is
      // already gone; the test asserts on the peer-side effect, not this call.
      let _ = self.client_conn.send_stream(sid).reset(VarInt::from_u32(0));
    }

    /// Finish the client's send half of `sid` (peer's recv half sees FIN).
    fn client_finish(&mut self, sid: QuicSid) {
      // Ignoring Err: `finish` returns `Err` only if already finished/reset; the
      // test asserts on the peer observing FIN, not this call's result.
      let _ = self.client_conn.send_stream(sid).finish();
    }

    /// Ferry every queued datagram both ways once and advance both connections
    /// one timer tick. Drives client-written stream bytes to the server and the
    /// server's responses (ACKs, RESET_STREAM, …) back to the client.
    fn ferry(&mut self) {
      let now = self.now;
      let mut buf = Vec::new();
      while let Some(t) = self.client_conn.poll_transmit(now, 1, &mut buf) {
        Self::deliver(
          &mut self.server_ep,
          &mut self.server_conns,
          &mut Some(self.server_ch),
          CLIENT_ADDR,
          &t,
          &buf,
          now,
        );
        buf.clear();
      }
      let sch = self.server_ch;
      let mut sbuf = Vec::new();
      if let Some(e) = self.server_conns.get_mut(sch) {
        while let Some(t) = e.conn_mut().poll_transmit(now, 1, &mut sbuf) {
          let data = bytes::BytesMut::from(&sbuf[..t.size]);
          let mut scratch = Vec::new();
          if let Some(ev) = self
            .client_ep
            .handle(now, SERVER_ADDR, None, None, data, &mut scratch)
          {
            Self::apply_client_event(&mut self.client_conn, self.client_ch, ev);
          }
          sbuf.clear();
        }
      }
      Self::pump_endpoint_events(&mut self.client_ep, &mut self.client_conn, self.client_ch);
      if let Some(e) = self.server_conns.get_mut(sch) {
        while let Some(ev) = e.conn_mut().poll_endpoint_events() {
          if ev.is_drained() {
            continue;
          }
          if let Some(cev) = self.server_ep.handle_event(sch, ev) {
            e.conn_mut().handle_event(cev);
          }
        }
      }
      self.client_conn.handle_timeout(now);
      if let Some(e) = self.server_conns.get_mut(sch) {
        e.conn_mut().handle_timeout(now);
      }
    }

    /// Accept the server's next inbound bidi stream id (ferrying until it
    /// appears).
    fn server_accept_bi(&mut self) -> QuicSid {
      for _ in 0..200 {
        let sch = self.server_ch;
        if let Some(e) = self.server_conns.get_mut(sch)
          && let Some(sid) = e.conn_mut().streams().accept(Dir::Bi)
        {
          return sid;
        }
        self.ferry();
      }
      panic!("server never accepted an inbound bidi stream");
    }

    /// Build a `Bridge` over the server's accepted bidi `sid` (inbound role,
    /// no label). The bridge's inner FSM is a fresh `accept_stream`.
    fn server_bridge(
      &mut self,
      ep: &mut Endpoint<SmolStr, SocketAddr>,
      sid: QuicSid,
    ) -> Bridge<SmolStr, SocketAddr> {
      let stream = ep
        .accept_stream(CLIENT_ADDR, Instant::from_std(self.now))
        .expect("node is running");
      Bridge::new(
        stream,
        self.server_ch,
        sid,
        crate::CompressionOptions::new(),
        crate::EncryptionOptions::new(),
        ep.max_stream_frame_size(),
        None,
        false,
        false,
      )
    }
  }

  fn make_server_endpoint() -> Endpoint<SmolStr, SocketAddr> {
    Endpoint::new(EndpointOptions::new(SmolStr::new("server"), SERVER_ADDR))
  }

  /// `pump_in` over a live quinn stream: an over-ceiling `unit_len` (a forged
  /// varint above `reliable_max`) makes `take_reliable_unit_with_encryption`
  /// return `Err` inside the unit loop, driving the `unit_err` → `decode_failed`
  /// path that retires both halves and transitions the bridge to
  /// `Failed(Decode)`. Reaches the `take_reliable_unit` `Err(_)` arm and the
  /// post-borrow `decode_failed` retire block.
  #[test]
  fn pump_in_over_ceiling_unit_len_fails_decode() {
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    // Forge a `[unit_len: varint]` far above any plausible `reliable_max`, with
    // no payload — `take_reliable_unit` rejects the unit before reading bytes.
    let mut forged = Vec::new();
    crate::framing::encode_varint_u32(u32::MAX, &mut forged);
    pair.client_write(sid, &forged);
    pair.client_finish(sid);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_sid);

    // Pump the forged unit through the live server stream.
    let mut result = Ok(());
    for _ in 0..50 {
      result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if result.is_err() {
        break;
      }
      pair.ferry();
    }
    assert_eq!(
      result,
      Err(()),
      "an over-ceiling unit_len must fail pump_in via the decode-fail retire path"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
      "a forged over-ceiling unit_len terminalizes the bridge as Failed(Decode) — got {:?}",
      bridge.phase
    );
  }

  /// `pump_in` over a live quinn stream: a well-FRAMED reliable unit whose
  /// payload is not a decodable memberlist `Message` makes `Stream::handle_data`
  /// return `Err` inside the unit loop (the `decode_failed = true; break` arm),
  /// then the post-borrow `decode_failed` block retires both halves and sets
  /// `Failed(Decode)`.
  #[test]
  fn pump_in_undecodable_unit_payload_fails_decode() {
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    // A structurally valid `[unit_len][payload]` (so `take_reliable_unit`
    // succeeds) whose 64-byte payload is not a valid `Message` (so the FSM's
    // `handle_data` rejects it).
    let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[0xffu8; 64]);
    pair.client_write(sid, &unit);
    pair.client_finish(sid);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_sid);

    let mut result = Ok(());
    for _ in 0..50 {
      result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if result.is_err() {
        break;
      }
      pair.ferry();
    }
    assert_eq!(
      result,
      Err(()),
      "an undecodable unit payload must fail pump_in via handle_data → decode_failed"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
      "an undecodable unit payload terminalizes the bridge as Failed(Decode) — got {:?}",
      bridge.phase
    );
  }

  /// `pump_in` over a live quinn stream: the peer FINs in the middle of a
  /// reliable unit (writes a partial `[unit_len][..]` then finishes). At EOF the
  /// trailing partial unit in `recv_accum` is a truncated transmission — the
  /// `fin_seen && !recv_accum.is_empty()` arm flips `decode_failed`, and the
  /// retire block terminalizes the bridge `Failed(Decode)`.
  #[test]
  fn pump_in_truncated_unit_at_fin_fails_decode() {
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    // A full unit's prefix: declare a 64-byte payload but deliver only 8 bytes,
    // then FIN. The recv side decodes the varint length, waits for 64 bytes,
    // and at FIN finds a partial unit → truncated.
    let full = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[7u8; 64]);
    // A 64-byte payload yields a unit well over 10 bytes; send only the first 10
    // (a partial `[unit_len][..]`) so the recv side waits for the rest, then
    // FINs.
    assert!(full.len() > 10);
    let partial = &full[..10];
    pair.client_write(sid, partial);
    pair.client_finish(sid);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_sid);

    let mut result = Ok(());
    for _ in 0..50 {
      result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if result.is_err() {
        break;
      }
      pair.ferry();
    }
    assert_eq!(
      result,
      Err(()),
      "a FIN mid-unit must be treated as a truncated transmission (decode fail)"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
      "a truncated unit at FIN terminalizes the bridge as Failed(Decode) — got {:?}",
      bridge.phase
    );
  }

  /// `pump_in` over a live quinn stream: the peer FINs while the bridge's FSM
  /// still expects inbound bytes (the request stream is opened and FINned with
  /// NO data). The `fin_seen && handle_data(&[]).is_err()` premature-FIN arm
  /// (the FSM's `PeerClosed` entry) flips `decode_failed` and the bridge
  /// terminalizes `Failed(Decode)`.
  #[test]
  fn pump_in_premature_fin_with_no_data_fails_decode() {
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    // Open + immediately FIN with no bytes. The server FSM (an inbound stream
    // awaiting its first message) sees an empty-buffer EOF → PeerClosed.
    pair.client_finish(sid);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_sid);

    let mut result = Ok(());
    for _ in 0..50 {
      result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if result.is_err() {
        break;
      }
      pair.ferry();
    }
    assert_eq!(
      result,
      Err(()),
      "a premature FIN with no data must route through the FSM PeerClosed failure"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
      "a premature empty FIN terminalizes the bridge as Failed(Decode) — got {:?}",
      bridge.phase
    );
  }

  /// `pump_in` over a live quinn stream: the peer RESETs its send half, so the
  /// bridge's ordered read returns `ReadError::Reset`, driving the `reset`
  /// retire block (RESET + STOP + `Failed(Transport)`).
  #[test]
  fn pump_in_peer_reset_recv_fails_transport() {
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    // Write a partial unit then RESET — the recv side sees the bytes followed by
    // a RESET_STREAM rather than a clean FIN.
    let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[2u8; 24]);
    // Send only the first few bytes (a partial unit) before resetting, so the
    // recv side sees bytes followed by a RESET_STREAM rather than a clean FIN.
    assert!(unit.len() > 6);
    pair.client_write(sid, &unit[..6]);
    pair.ferry();
    pair.client_reset(sid);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_sid);

    let mut result = Ok(());
    for _ in 0..50 {
      result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if result.is_err() {
        break;
      }
      pair.ferry();
    }
    assert_eq!(
      result,
      Err(()),
      "a peer RESET_STREAM on the recv half must fail pump_in via the reset path"
    );
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::Transport(_))
      ),
      "a peer reset terminalizes the bridge as Failed(Transport) — got {:?}",
      bridge.phase
    );
  }

  /// `pump_in` over a live quinn stream: a prior UNORDERED read on the recv
  /// half makes the bridge's ordered `RecvStream::read(true)` return
  /// `ReadableError::IllegalOrderedRead`, driving the `illegal_ordered` retire
  /// block (RESET + STOP + `Failed(Transport)`).
  #[test]
  fn pump_in_illegal_ordered_read_fails_transport() {
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[1u8; 16]);
    pair.client_write(sid, &unit);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_sid);

    // Ferry so the client bytes are buffered on the server recv stream, then do
    // a poisoning UNORDERED read on the SAME recv half. The bridge's subsequent
    // ordered `read(true)` then trips `IllegalOrderedRead`.
    for _ in 0..50 {
      pair.ferry();
      let mut buffered = false;
      if let Some(e) = pair.server_conns.get_mut(pair.server_ch) {
        let mut rs = e.conn_mut().recv_stream(server_sid);
        if let Ok(mut chunks) = rs.read(false) {
          if matches!(chunks.next(usize::MAX), Ok(Some(_))) {
            buffered = true;
          }
          // Ignoring Err: the unordered-read finalize wakeup is unused here.
          let _ = chunks.finalize();
        }
      }
      if buffered {
        break;
      }
    }

    let result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    assert_eq!(
      result,
      Err(()),
      "an ordered read after an unordered read must fail via IllegalOrderedRead"
    );
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::Transport(_))
      ),
      "an illegal ordered read terminalizes the bridge as Failed(Transport) — got {:?}",
      bridge.phase
    );
  }

  /// Build a LABELED inbound bridge (acceptor role, lazy outbound disclosure)
  /// over the server's accepted bidi `sid`.
  fn server_labeled_bridge(
    pair: &mut RawQuicPair,
    ep: &mut Endpoint<SmolStr, SocketAddr>,
    sid: QuicSid,
    label: bytes::Bytes,
  ) -> Bridge<SmolStr, SocketAddr> {
    let stream = ep
      .accept_stream(CLIENT_ADDR, Instant::from_std(pair.now))
      .expect("node is running");
    Bridge::new(
      stream,
      pair.server_ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      Some(label),
      false,
      false,
    )
  }

  /// Build a LABELED outbound DIALER bridge (eager outbound disclosure) over the
  /// server's open bidi `sid`.
  fn server_labeled_dialer_bridge(
    pair: &mut RawQuicPair,
    ep: &mut Endpoint<SmolStr, SocketAddr>,
    sid: QuicSid,
    label: bytes::Bytes,
  ) -> Bridge<SmolStr, SocketAddr> {
    let stream = ep
      .accept_stream(CLIENT_ADDR, Instant::from_std(pair.now))
      .expect("node is running");
    Bridge::new(
      stream,
      pair.server_ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      Some(label),
      false,
      true,
    )
  }

  /// `pump_out` over a live quinn stream for a LABELED dialer: the first
  /// `pump_out` writes the eager outbound label frame into `pending_out` (the
  /// `!outbound_label_written && eager_outbound_label` arm) and the
  /// `pending_out` flush writes it onto the quinn send stream. After this the
  /// `outbound_label_written` latch is set and `pending_out` has drained.
  #[test]
  fn pump_out_labeled_dialer_writes_eager_label_over_live_stream() {
    let label = bytes::Bytes::from_static(b"cluster-out");
    let mut pair = RawQuicPair::handshaked();
    // The server opens a bidi (live send half) for the dialer bridge to write on.
    let server_open_sid = {
      let e = pair
        .server_conns
        .get_mut(pair.server_ch)
        .expect("server connection present");
      e.conn_mut()
        .streams()
        .open(Dir::Bi)
        .expect("server opens a bidi")
    };
    let mut ep = make_server_endpoint();
    let mut bridge =
      server_labeled_dialer_bridge(&mut pair, &mut ep, server_open_sid, label.clone());
    assert!(
      !bridge.outbound_label_written(),
      "a labeled dialer starts with the outbound-label latch unset"
    );

    let result = bridge.pump_out(&mut pair.server_conns, Instant::from_std(pair.now));
    assert_eq!(
      result,
      Ok(()),
      "the eager-label write + flush must succeed on a healthy send half"
    );
    assert!(
      bridge.outbound_label_written(),
      "the first pump_out must write the eager outbound label frame and set the latch"
    );
    assert!(
      bridge.pending_out_bytes().is_empty(),
      "the label frame must flush onto the send stream (no back-pressure on a fresh stream)"
    );

    // Confirm the label bytes actually reached the peer: ferry, accept the bidi
    // on the client, and read the label header off the front.
    let mut client_recv = Vec::new();
    for _ in 0..50 {
      pair.ferry();
      if let Some(csid) = pair.client_conn.streams().accept(Dir::Bi) {
        if let Ok(mut chunks) = pair.client_conn.recv_stream(csid).read(true) {
          while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
            client_recv.extend_from_slice(&chunk.bytes);
          }
          // Ignoring Err: the read finalize wakeup is unused here.
          let _ = chunks.finalize();
        }
        break;
      }
    }
    let expected_header = build_label_header(&label);
    assert!(
      client_recv.starts_with(&expected_header),
      "the eager label frame must be the FIRST bytes on the wire — got {client_recv:?}"
    );
  }

  /// `pump_in` over a live quinn stream for a LABELED bridge: the peer writes
  /// the matching label header followed by a reliable unit. The inline label
  /// classifier (the `if !self.inbound_label_validated` block inside the chunk
  /// loop) drains the header, latches `inbound_label_validated`, and the
  /// acceptor's lazy outbound-label disclosure prepends its own label to
  /// `pending_out`. The trailing reliable unit then dispatches into the FSM.
  #[test]
  fn pump_in_labeled_validates_inbound_label_over_live_stream() {
    let label = bytes::Bytes::from_static(b"cluster-quic");
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    // The dialer writes its label header first, then one reliable unit carrying
    // a valid Ping message (so the FSM accepts it cleanly after the header).
    let mut wire = Vec::new();
    crate::label::encode_label_prefix(&label, &mut wire);
    let ping = crate::typed::Message::<SmolStr, SocketAddr>::Ping(crate::typed::Ping::new(
      1,
      crate::typed::Node::new(SmolStr::new("client"), CLIENT_ADDR),
      crate::typed::Node::new(SmolStr::new("server"), SERVER_ADDR),
    ));
    let framed = crate::wire::encode_message::<SmolStr, SocketAddr>(&ping).expect("encode ping");
    let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &framed);
    wire.extend_from_slice(&unit);
    pair.client_write(sid, &wire);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = server_labeled_bridge(&mut pair, &mut ep, server_sid, label);
    assert!(
      !bridge.inbound_label_validated(),
      "a labeled bridge starts with the inbound latch unset"
    );

    // Pump until the label header validates (the inline classifier consumes it
    // and latches).
    for _ in 0..50 {
      // Ignoring Err: this loop drives the pump and inspects the bridge's
      // latch/phase below; a terminal Err is itself the asserted outcome.
      let _ = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if bridge.inbound_label_validated() {
        break;
      }
      pair.ferry();
    }
    assert!(
      bridge.inbound_label_validated(),
      "the inline label classifier must validate the matching inbound header \
       and latch over a live stream"
    );
    assert!(
      bridge.outbound_label_written(),
      "an acceptor's lazy outbound-label disclosure must arm once the peer's \
       label validates"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Active),
      "a matching label + valid reliable unit keeps the bridge non-failed — got {:?}",
      bridge.phase
    );
  }

  /// `pump_in` over a live quinn stream for a LABELED bridge whose peer sends a
  /// MISMATCHED label header: the inline classifier returns
  /// `LabelOutcome::Rejected`, flipping `decode_failed` and routing the bridge
  /// through the decode-fail retire block (`Failed(Decode)`).
  #[test]
  fn pump_in_labeled_mismatched_inbound_label_rejected_over_live_stream() {
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    // The peer writes a DIFFERENT label than the bridge expects.
    let mut wire = Vec::new();
    crate::label::encode_label_prefix(b"wrong-cluster", &mut wire);
    pair.client_write(sid, &wire);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = server_labeled_bridge(
      &mut pair,
      &mut ep,
      server_sid,
      bytes::Bytes::from_static(b"expected-cluster"),
    );

    let mut result = Ok(());
    for _ in 0..50 {
      result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if result.is_err() {
        break;
      }
      pair.ferry();
    }
    assert_eq!(
      result,
      Err(()),
      "a mismatched inbound label must fail pump_in via LabelOutcome::Rejected"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
      "a mismatched inbound label terminalizes the bridge as Failed(Decode) — got {:?}",
      bridge.phase
    );
  }

  /// `pump_in` over a live quinn stream for a LABELED bridge whose peer delivers
  /// the label header SPLIT across two writes: the first `pump_in` sees only a
  /// partial header → `LabelOutcome::Incomplete` (the `break` that holds
  /// `recv_accum` for the next chunk), and a later `pump_in` completes it and
  /// latches `inbound_label_validated`.
  #[test]
  fn pump_in_labeled_split_label_header_incompletes_then_validates() {
    let label = bytes::Bytes::from_static(b"split-cluster");
    let mut pair = RawQuicPair::handshaked();
    let sid = pair.client_open_bi();
    let header = build_label_header(&label);
    assert!(header.len() >= 2, "a label header is at least [tag][len]");
    // Write ONLY the first byte (the tag); the classifier needs more → Incomplete.
    pair.client_write(sid, &header[..1]);

    let server_sid = pair.server_accept_bi();
    let mut ep = make_server_endpoint();
    let mut bridge = server_labeled_bridge(&mut pair, &mut ep, server_sid, label);

    // Drive until the partial header has arrived and been pumped (Incomplete
    // break holds it). The latch must still be UNSET.
    let mut pumped_partial = false;
    for _ in 0..50 {
      pair.ferry();
      // Ignoring Err: this loop drives the pump and inspects the bridge's
      // latch/phase below; a terminal Err is itself the asserted outcome.
      let _ = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      // The test module is a child of this module, so it reads the private
      // `recv_accum` directly: the Incomplete break holds the partial header
      // here rather than draining it.
      if !bridge.recv_accum.is_empty() {
        pumped_partial = true;
        break;
      }
    }
    assert!(
      pumped_partial,
      "the partial label header (1 byte) must reach recv_accum and be held by \
       the Incomplete break"
    );
    assert!(
      !bridge.inbound_label_validated(),
      "a partial label header must NOT latch — it Incompletes and waits for more"
    );

    // Now write the REST of the header; a later pump_in completes it and latches.
    pair.client_write(sid, &header[1..]);
    for _ in 0..50 {
      pair.ferry();
      // Ignoring Err: this loop drives the pump and inspects the bridge's
      // latch/phase below; a terminal Err is itself the asserted outcome.
      let _ = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
      if bridge.inbound_label_validated() {
        break;
      }
    }
    assert!(
      bridge.inbound_label_validated(),
      "the completing chunk must latch the inbound label after the Incomplete hold"
    );
  }

  /// `pump_out` deadline enforcement over a live quinn stream: a non-terminal
  /// bridge with a back-pressured `pending_out` tail whose inner-stream deadline
  /// has elapsed routes through the pre-write deadline check
  /// (`stream.poll_timeout() <= now`), which drives `handle_timeout`, retires
  /// both halves, and transitions the bridge to `Failed(Timeout)`.
  #[test]
  fn pump_out_deadline_with_pending_out_tail_fails_timeout() {
    let mut pair = RawQuicPair::handshaked();
    // The server opens a bidi (so it is the outbound dialer with a live send
    // half) and accepts a fresh outbound FSM stream whose deadline we can read.
    let server_open_sid = {
      let e = pair
        .server_conns
        .get_mut(pair.server_ch)
        .expect("server connection present");
      e.conn_mut()
        .streams()
        .open(Dir::Bi)
        .expect("server opens a bidi")
    };
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_open_sid);

    // Stage a back-pressured tail and pin the inner stream's deadline in the
    // past so the pre-write deadline check fires on the next pump_out.
    bridge.pending_out.extend_from_slice(b"back-pressured tail");
    let past = Instant::from_std(pair.now);
    // The inner FSM's deadline must be `Some` and `<= now`. A fresh inbound
    // accept_stream carries an exchange deadline at `now + stream_timeout`; we
    // pump at a `now` far in the future so `poll_timeout() <= now`.
    let future = past + core::time::Duration::from_secs(3600);
    assert!(
      bridge.stream.poll_timeout().is_some(),
      "a pre-Done stream must expose its exchange deadline"
    );

    let result = bridge.pump_out(&mut pair.server_conns, future);
    assert_eq!(
      result,
      Err(()),
      "a back-pressured tail past the exchange deadline must fail pump_out"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Timeout)),
      "the pre-write deadline check must terminalize the bridge as \
       Failed(Timeout) — got {:?}",
      bridge.phase
    );
    assert!(
      bridge.pending_out.is_empty(),
      "the back-pressured tail must be cleared so no post-deadline bytes are written"
    );
  }

  /// The back-pressure safety of `pump_out`: when the stream is flow-control
  /// blocked, the staged `pending_out` flush RETAINS the remainder intact and
  /// returns `Ok` (the bridge waits for credit) rather than losing it, growing
  /// it, or failing the exchange — and `pump_out` returns at the flush WITHOUT
  /// reaching the gather loop, so a new unit is never started while a remainder
  /// is outstanding (the one-unit-in-flight invariant). The companion of the
  /// deadline test: same staged tail, but pumped before the deadline against a
  /// saturated send window.
  #[test]
  fn pump_out_blocked_pending_out_flush_retains_tail_and_waits() {
    let mut pair = RawQuicPair::handshaked();
    let server_open_sid = {
      let e = pair
        .server_conns
        .get_mut(pair.server_ch)
        .expect("server connection present");
      e.conn_mut()
        .streams()
        .open(Dir::Bi)
        .expect("server opens a bidi")
    };
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_open_sid);

    // Saturate the stream's flow-control window so the next write blocks.
    let big = vec![0u8; 64 * 1024];
    loop {
      let e = pair
        .server_conns
        .get_mut(pair.server_ch)
        .expect("server connection present");
      match e.conn_mut().send_stream(server_open_sid).write(&big) {
        Ok(_) => continue,
        Err(quinn_proto::WriteError::Blocked) => break,
        Err(other) => panic!("unexpected write error while saturating the window: {other:?}"),
      }
    }

    // Stage a back-pressured tail and pump BEFORE the exchange deadline.
    bridge.pending_out.extend_from_slice(b"back-pressured tail");
    let result = bridge.pump_out(&mut pair.server_conns, Instant::from_std(pair.now));

    assert_eq!(
      result,
      Ok(()),
      "a flow-control-blocked flush waits for credit; it does not fail"
    );
    assert_eq!(
      bridge.pending_out_bytes(),
      b"back-pressured tail",
      "the blocked flush retains the staged tail intact — no loss, no growth, no duplication"
    );
    assert!(
      !matches!(bridge.phase, BridgePhase::Failed(_)),
      "a flow-control block is normal back-pressure, not a failure: phase = {:?}",
      bridge.phase
    );
  }

  /// `pump_out`'s post-`poll_transmit` FSM-failure retire (the
  /// `if let Some(e) = self.stream.is_failed()` block AFTER the gather loop):
  /// an outbound stream with an EMPTY `pending_out` (so the pre-write deadline
  /// check is skipped) whose exchange deadline has elapsed self-fails INSIDE
  /// `poll_transmit`; the post-loop check then retires both halves and sets
  /// `Failed(Transport)`.
  #[test]
  fn pump_out_post_transmit_fsm_timeout_retires() {
    let mut pair = RawQuicPair::handshaked();
    let server_open_sid = {
      let e = pair
        .server_conns
        .get_mut(pair.server_ch)
        .expect("server connection present");
      e.conn_mut()
        .streams()
        .open(Dir::Bi)
        .expect("server opens a bidi")
    };
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_open_sid);
    // `pending_out` is empty (a fresh bridge), so the pre-write deadline check
    // (`!pending_out.is_empty()`) is skipped and the FSM must self-fail inside
    // `poll_transmit` at the future `now`.
    assert!(bridge.pending_out.is_empty());
    let future = Instant::from_std(pair.now) + core::time::Duration::from_secs(3600);

    let result = bridge.pump_out(&mut pair.server_conns, future);
    assert_eq!(
      result,
      Err(()),
      "an empty-pending_out outbound stream past its deadline must fail via the \
       post-poll_transmit FSM-failure retire"
    );
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::Transport(_))
      ),
      "the post-poll_transmit `stream.is_failed()` retire must terminalize the \
       bridge as Failed(Transport) — got {:?}",
      bridge.phase
    );
  }

  /// `pump_out`'s leading terminal-bridge guard, `Some(e)` arm: when the inner
  /// FSM has already failed (here via an elapsed exchange deadline) but the
  /// BRIDGE phase is still non-`Failed`, the first thing `pump_out` does is map
  /// `self.stream.is_failed()` Some → `BridgeFailure::Transport(e.to_string())`,
  /// retire both halves, and return `Err(())`.
  #[test]
  fn pump_out_leading_guard_fsm_failed_retires() {
    let mut bridge = make_plain_bridge();
    let mut conns = ConnTable::new();
    // Fail the inner FSM via its own exchange deadline WITHOUT routing through
    // the bridge's `fail*` helpers, so `stream.is_failed()` is Some while
    // `bridge.phase` is still `Active`.
    let inner_deadline = bridge
      .stream
      .poll_timeout()
      .expect("a fresh stream exposes its exchange deadline");
    bridge.stream.handle_timeout(inner_deadline);
    assert!(bridge.stream.is_failed().is_some());
    assert!(
      matches!(bridge.phase, BridgePhase::Active),
      "the bridge phase has not yet cascaded the FSM failure"
    );

    // The leading guard's `Some(e)` arm fires (the empty ConnTable makes the
    // retire a no-op, but the phase transition still runs).
    let result = bridge.pump_out(&mut conns, inner_deadline);
    assert_eq!(
      result,
      Err(()),
      "pump_out's leading guard must fail an FSM-failed bridge before any write"
    );
    assert!(
      matches!(
        bridge.phase,
        BridgePhase::Failed(BridgeFailure::Transport(_))
      ),
      "the leading guard's `Some(e)` arm maps the FSM error to \
       Failed(Transport) — got {:?}",
      bridge.phase
    );
  }

  /// Build an inbound bridge over a default-ACCEPTING endpoint that has already
  /// decoded a PushPull join request (FSM in `InboundSendingResponse`,
  /// `PushPullRequestReceived{Join}` queued). The bridge rides a live server
  /// stream so its `pump_out` can actually write the response.
  fn inbound_join_bridge_accepting(
    pair: &mut RawQuicPair,
    server_sid: QuicSid,
  ) -> (Endpoint<SmolStr, SocketAddr>, Bridge<SmolStr, SocketAddr>) {
    let mut ep = make_server_endpoint();
    let mut bridge = pair.server_bridge(&mut ep, server_sid);
    // Dispatch a join PushPull request directly into the FSM (no rejecting
    // delegate is set, so `handle_stream_event` will return
    // `SendPushPullResponse`).
    let dave = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7004);
    let dave_state =
      crate::typed::PushNodeState::new(1, SmolStr::new("dave"), dave, crate::typed::State::Alive);
    let pp =
      crate::typed::PushPull::new(true, core::iter::once(dave_state)).with_user_data(Bytes::new());
    let bytes =
      crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::PushPull(pp)).expect("encode");
    bridge
      .stream
      .handle_data(&bytes, Instant::from_std(pair.now))
      .expect("dispatch the join push/pull request");
    (ep, bridge)
  }

  /// `drain_then_reap`'s `StreamCommand::SendPushPullResponse` arm: a TERMINAL
  /// inbound bridge that still holds an undrained `PushPullRequestReceived`
  /// (queued by the FSM, never drained by `drain_payload_only`) drains it
  /// through the accepting endpoint, which returns `SendPushPullResponse`. The
  /// arm encodes + `stream_load_response`s the reply, refreshes the bridge
  /// deadline, and `pump_out`s the response onto the live quinn stream.
  #[test]
  fn drain_then_reap_sends_push_pull_response_over_live_stream() {
    let mut pair = RawQuicPair::handshaked();
    let client_sid = pair.client_open_bi();
    // Nudge the client stream into existence on the server so it accepts a bidi.
    pair.client_write(client_sid, b"\x00");
    let server_sid = pair.server_accept_bi();
    let (mut ep, mut bridge) = inbound_join_bridge_accepting(&mut pair, server_sid);

    // Drive the bridge terminal (BothClosed) WITHOUT draining payload events, so
    // the queued `PushPullRequestReceived` is still pending when the terminal D1
    // path runs.
    bridge.observe_send_fin();
    bridge.observe_recv_fin();
    assert!(matches!(bridge.phase, BridgePhase::BothClosed));
    assert!(bridge.is_terminal());

    let deadline_before = bridge.deadline;
    bridge.drain_then_reap(&mut ep, &mut pair.server_conns, Instant::from_std(pair.now));

    // The SendPushPullResponse arm refreshes the bridge deadline to exactly
    // `now + 5s` (the response write window, measured from response start, not
    // from accept). The accept deadline was `now + stream_timeout` (10s by
    // default), so the refresh moves it to a DIFFERENT, earlier value — the
    // observable that the arm ran (a no-op drain would leave the accept
    // deadline untouched).
    let expected = Instant::from_std(pair.now) + core::time::Duration::from_secs(5);
    assert_eq!(
      bridge.deadline, expected,
      "the SendPushPullResponse arm must refresh the bridge deadline to \
       `now + 5s`; deadline_before = {deadline_before:?}"
    );
    // The encoded response was loaded + flushed by the arm's `pump_out`: the
    // inner FSM advanced past `InboundSendingResponse` to `Done` and the send
    // half was finished.
    assert!(
      bridge.stream.is_done(),
      "the inner stream must be Done after the response is encoded, loaded, and \
       pumped out"
    );
    assert!(
      bridge.finish_called,
      "pump_out must have finished the send half after writing the response"
    );
  }

  /// `drain_then_reap`'s notice selection `(_, Some(e))` arm: a bridge whose
  /// inner FSM has failed (here via an elapsed exchange deadline →
  /// `Failed(Timeout)` at the FSM level) while the BRIDGE phase is still
  /// non-`Failed` emits a `StreamErrored` notice carrying the FSM error string.
  /// This is the transient-inconsistency window the phase-authoritative
  /// terminality contract tolerates for one drain.
  #[test]
  fn drain_then_reap_fsm_failed_notice_arm() {
    let mut ep = make_server_endpoint();
    let mut conns = ConnTable::new();
    let mut bridge = make_plain_bridge();
    let t0 = Instant::now();
    // Fail the inner FSM via its own exchange deadline WITHOUT routing through
    // the bridge's `fail*` helpers, so `stream.is_failed()` is Some while
    // `bridge.phase` is still `Active`.
    let inner_deadline = bridge
      .stream
      .poll_timeout()
      .expect("a fresh stream exposes its exchange deadline");
    bridge.stream.handle_timeout(inner_deadline);
    assert!(
      bridge.stream.is_failed().is_some(),
      "the inner FSM must be failed after its exchange deadline elapses"
    );
    assert!(
      matches!(bridge.phase, BridgePhase::Active),
      "the bridge phase is still Active — the FSM failure has not yet cascaded"
    );
    assert!(
      !bridge.is_terminal(),
      "a non-Failed phase is not terminal even with a failed inner FSM"
    );

    // `drain_then_reap` selects the lifecycle notice from `(phase, fsm_failed)`;
    // the `(_, Some(e))` arm emits `StreamErrored(e.to_string())`. No panic ==
    // the arm executed; no UserPacket may surface (no payload was queued).
    bridge.drain_then_reap(&mut ep, &mut conns, t0);
    assert!(
      !ep
        .poll_event()
        .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
      "the FSM-failed notice arm must not surface application user data"
    );
  }
}
