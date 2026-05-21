//! Per-reliable-stream state machine.
//!
//! One [`Stream`] per logical message exchange (push/pull, reliable ping,
//! reliable user message). The driver opens or accepts a single connection,
//! then drives the resulting Stream:
//!
//! ```text
//!   driver calls: handle_data(&[u8], now)  →  decode, advance FSM
//!   driver calls: poll_transmit(&mut Vec<u8>)  →  drain encoded bytes
//!   driver calls: poll_endpoint_event()  →  drain events for Endpoint
//!   driver calls: poll_event()  →  drain app-visible events
//!   driver calls: handle_timeout(now)  →  advance on deadline
//!   driver calls: poll_timeout()  →  next deadline to schedule
//! ```
//!
//! **Wire framing:** The machine sees plain `[TAG u8][VARINT_LEN][BODY]`
//! bytes. Label / encryption / checksum / compression are driver concerns.
//!
//! **Decode strategy:** `DecodeError::BufferUnderflow` signals insufficient
//! bytes, not a distinct `Incomplete` variant. Before calling
//! `MessageRef::decode`, `handle_data` peeks the varint length prefix to
//! confirm the full frame is buffered. See `try_decode_frame`.

use std::{collections::VecDeque, time::Instant};

use bytes::{Bytes, BytesMut};
use memberlist_wire::{Data, typed::PushNodeState};

use crate::{
  error::StreamError,
  event::{EndpointEvent, PushPullKind, StreamEvent},
};

/// Unique handle for a stream-oriented exchange.
/// Re-exported from `event.rs`; used here to key the stream in `Endpoint`.
pub use crate::event::StreamId;

// ─────────────────────────────────────── public snapshot type ────────────────

/// Decoded snapshot of a peer's push/pull message. Owned; emitted in
/// `EndpointEvent::PushPullRequestReceived` or `EndpointEvent::PushPullReplyReceived`
/// so the application/Endpoint can merge it.
#[derive(Debug)]
pub struct PushPullSnapshot<I, A> {
  /// Whether the peer considers this exchange a join (vs. periodic refresh).
  pub join: bool,
  /// The peer's membership view (wire-level, includes incarnation/state/versions).
  pub states: Vec<PushNodeState<I, A>>,
  /// Application-level payload the peer attached to its push/pull message.
  pub user_data: Bytes,
}

// ─────────────────────────────────────── internal enums ──────────────────────

/// What kind of outbound exchange a Stream is performing.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum OutboundKind {
  /// Full membership sync (push/pull).
  PushPull {
    /// Whether this is a join-triggered exchange (vs. periodic anti-entropy).
    kind: PushPullKind,
  },
  /// TCP/QUIC fallback ping for a timed-out UDP probe.
  ReliablePing {
    /// Sequence number of the original probe (links back to Probe).
    probe_seq: u32,
  },
  /// One-way delivery of application-supplied bytes via reliable transport.
  UserMessage,
}

/// What kind of inbound exchange a Stream is processing.
///
/// Note: `InboundKind::PushPull` is a unit variant — the decoded snapshot is
/// moved into `EndpointEvent::PushPullRequestReceived` immediately. The Endpoint
/// provides the response payload via `StreamCommand::SendPushPullResponse`
/// rather than having it stashed here.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum InboundKind {
  /// Peer sent a PushPull; we emitted the event; awaiting our response from
  /// the Endpoint to write back via `SendPushPullResponse`.
  PushPull,
  /// Peer sent a Ping; we have the seq number; awaiting our Ack write.
  ReliablePing {
    /// Sequence number from the peer's Ping.
    ping_seq: u32,
  },
  /// Peer sent user data; we emitted the event; stream is done.
  UserMessage,
}

/// Phase of the per-stream FSM.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum StreamPhase {
  // ── Outbound (we dialed) ──────────────────────────────────────────────────
  /// We have bytes in `output_buf`; driver is draining them via `poll_transmit`.
  OutboundSendingRequest {
    /// The kind of outbound exchange being performed.
    kind: OutboundKind,
  },
  /// Request fully sent (the last byte was drained via `poll_transmit`).
  /// Waiting for the peer's response bytes.
  OutboundAwaitingResponse {
    /// The kind of outbound exchange being performed.
    kind: OutboundKind,
  },
  // ── Inbound (peer dialed us) ──────────────────────────────────────────────
  /// Connected; waiting for the peer to send the first message.
  InboundAwaitingFirstMessage,
  /// First message decoded. For PushPull: our response is in `output_buf`.
  /// For ReliablePing: our Ack is in `output_buf`.
  InboundSendingResponse {
    /// The kind of inbound exchange being processed.
    kind: InboundKind,
  },
  // ── Terminal ─────────────────────────────────────────────────────────────
  /// Exchange completed successfully.
  Done,
  /// Exchange failed. The error is delivered via `poll_endpoint_event` and
  /// `poll_event` once; after that the stream stays in Failed and is GCd.
  Failed(StreamError),
}

// ─────────────────────────────────────── Stream ──────────────────────────────

/// Per-reliable-stream Sans-I/O state machine.
///
/// Created by `Endpoint::dial_succeeded` (outbound) or
/// `Endpoint::accept_stream` (inbound). The driver feeds bytes via
/// `handle_data`, drains encoded response bytes via `poll_transmit`, and
/// drains events via `poll_endpoint_event` / `poll_event`. Time advances via
/// `handle_timeout`; the next relevant deadline is returned by `poll_timeout`.
#[derive(Debug)]
pub struct Stream<I, A> {
  pub(crate) id: StreamId,
  pub(crate) peer: A,
  /// The local node's id. Used to reject an inbound reliable Ping whose
  /// `target` is not us (mirrors `Endpoint::handle_ping`); a misrouted /
  /// spoofed ping must not be Acked, or a dead target could be reported
  /// healthy and suspicion suppressed.
  pub(crate) local_id: I,
  /// Hard cap on a single inbound frame (`cfg.max_stream_frame_size`). A
  /// declared frame total above this is rejected the instant the length
  /// varint decodes — before the body is buffered — bounding memory
  /// against a peer that declares a huge length and dribbles bytes.
  pub(crate) max_frame_size: usize,
  pub(crate) phase: StreamPhase,
  /// Accumulator for incoming raw plain-frame bytes.
  pub(crate) input_buf: BytesMut,
  /// Encoded bytes queued for transmission; driver drains via `poll_transmit`.
  pub(crate) output_buf: VecDeque<u8>,
  /// Deadline for the current phase (write-drain deadline OR read deadline).
  pub(crate) deadline: Option<Instant>,
  /// Events destined for Endpoint (PushPullRequestReceived/PushPullReplyReceived, ReliablePingAcked, …).
  pub(crate) endpoint_events: VecDeque<EndpointEvent<I, A>>,
  /// App-visible lifecycle events (Closed, Failed).
  pub(crate) stream_events: VecDeque<StreamEvent>,
}

impl<I, A> Stream<I, A>
where
  I: nodecraft::Id
    + Data
    + nodecraft::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: Data
    + nodecraft::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  /// The unique identifier for this stream.
  pub fn id(&self) -> StreamId {
    self.id
  }

  /// The peer address this stream is connected to.
  pub fn peer(&self) -> &A {
    &self.peer
  }

  /// Returns `true` if the exchange has completed successfully.
  pub fn is_done(&self) -> bool {
    matches!(self.phase, StreamPhase::Done)
  }

  /// Returns `Some(&err)` if the stream has failed.
  pub fn is_failed(&self) -> Option<&StreamError> {
    match &self.phase {
      StreamPhase::Failed(e) => Some(e),
      _ => None,
    }
  }

  /// Drain encoded bytes into `buf`. Returns the number of bytes written,
  /// or `None` if there is nothing to send right now.
  ///
  /// Draining the request/response is what advances the FSM: once the last
  /// byte is handed to the I/O layer here, an `OutboundSendingRequest`
  /// becomes `OutboundAwaitingResponse` and an `InboundSendingResponse`
  /// becomes `Done`. This makes "drain `poll_transmit` until `None`" the
  /// complete public driver contract — there is no separate
  /// write-completion call the driver must remember (and could not make
  /// anyway, since it lives in another crate). The existing
  /// `deadline` (the overall exchange deadline set at dial/accept or by
  /// `stream_load_response`) carries over as the read deadline — a single
  /// per-exchange deadline covers the whole request/response.
  pub fn poll_transmit(&mut self, now: Instant, buf: &mut Vec<u8>) -> Option<usize> {
    // A terminal stream emits nothing — ever. Without this guard a stream
    // that `handle_timeout`/`handle_data` already moved to `Failed` (its
    // deadline elapsed) would still have its queued push/pull reply or
    // reliable user-message bytes drained here and written to the wire,
    // delivering a timed-out exchange AFTER the deadline. `enter_failed`
    // clears `output_buf`, so this is belt-and-suspenders, but the guard
    // is the authoritative invariant. `Done` likewise must not re-emit.
    if matches!(self.phase, StreamPhase::Done | StreamPhase::Failed(_)) {
      return None;
    }
    // Write-side deadline authority, symmetric to the read side
    // (`handle_data`) and the dial side (`dial_succeeded`). The terminal
    // guard + buffer-clear closed the post-`handle_timeout` hole, but
    // `poll_transmit` had no `now`: a stream whose dial succeeded just
    // *before* the deadline could still have its queued bytes drained by a
    // `poll_transmit` the driver issues at/after the deadline but before it
    // gets to `handle_timeout` — and for a one-way `UserMessage` that same
    // call also marked the stream `Done`, making the post-deadline send
    // unrevocable. With `now` the deadline owns the outcome regardless of
    // poll ordering: fail the stream and emit nothing, exactly as
    // `handle_timeout` would.
    if let Some(deadline) = self.deadline {
      if now >= deadline {
        self.enter_failed(StreamError::Timeout);
        return None;
      }
    }
    if self.output_buf.is_empty() {
      return None;
    }
    let n = self.output_buf.len();
    buf.reserve(n);
    let (a, b) = self.output_buf.as_slices();
    buf.extend_from_slice(a);
    buf.extend_from_slice(b);
    self.output_buf.clear();
    // The output buffer is fully drained (poll_transmit always empties it):
    // advance the write phase.
    match std::mem::replace(&mut self.phase, StreamPhase::Done) {
      // A reliable user message is one-way (`send_user_msg`: write then
      // done — no reply is read). It must terminate on send, NOT wait
      // for a response that never comes.
      StreamPhase::OutboundSendingRequest {
        kind: OutboundKind::UserMessage,
      } => {
        // Left as Done by the mem::replace.
        self.stream_events.push_back(StreamEvent::Closed);
      }
      StreamPhase::OutboundSendingRequest { kind } => {
        self.phase = StreamPhase::OutboundAwaitingResponse { kind };
      }
      StreamPhase::InboundSendingResponse { .. } => {
        // Left as Done by the mem::replace; the exchange is complete.
        self.stream_events.push_back(StreamEvent::Closed);
      }
      other => {
        // Not a write phase we advance (shouldn't normally have queued
        // bytes here) — restore it untouched.
        self.phase = other;
      }
    }
    Some(n)
  }

  /// Drain one endpoint-directed event, if any.
  pub fn poll_endpoint_event(&mut self) -> Option<EndpointEvent<I, A>> {
    self.endpoint_events.pop_front()
  }

  /// Drain one app-visible stream lifecycle event, if any.
  pub fn poll_event(&mut self) -> Option<StreamEvent> {
    self.stream_events.pop_front()
  }

  /// Discard the FSM's queued endpoint events and lifecycle events
  /// without changing the FSM phase. Reserved for transport-bridge
  /// layers that need to suppress a dispatched frame's side effects
  /// when the transport demonstrates the exchange was NOT authorized
  /// (peer reset / connection lost before the cooperative close
  /// signal — recv FIN for QUIC, `read() == 0` for TCP,
  /// `close_notify` for TLS). Mirrors `enter_failed`'s queue-clearing
  /// semantics on the FSM-failure path, but leaves the FSM phase
  /// intact (the dispatch itself was decode-clean; the transport
  /// authorization was what failed).
  #[cfg_attr(not(feature = "quic"), allow(dead_code))]
  pub(crate) fn discard_pending_events(&mut self) {
    self.endpoint_events.clear();
    self.stream_events.clear();
  }

  /// The next deadline this stream wants the driver to fire `handle_timeout` at.
  /// Returns `None` if the stream is terminal.
  pub fn poll_timeout(&self) -> Option<Instant> {
    match &self.phase {
      StreamPhase::Done | StreamPhase::Failed(_) => None,
      _ => self.deadline,
    }
  }

  /// Feed raw plain-frame bytes from the driver. Accumulates into `input_buf`
  /// and attempts to decode one complete frame. May queue endpoint events.
  ///
  /// Returns `Err` if the stream should be torn down.
  pub fn handle_data(&mut self, data: &[u8], now: Instant) -> Result<(), StreamError> {
    // Terminal-Failed: ignore data unconditionally (a stream that
    // already failed cannot re-fail or re-emit a second `Failed`
    // event).
    if matches!(self.phase, StreamPhase::Failed(_)) {
      return Ok(());
    }
    // Terminal-Done: an empty-buffer call is the EOF marker post-
    // completion (clean transport close — peer FIN'd / TCP read==0 /
    // TLS close_notify). NON-empty data after `Done` is adversarial:
    // peer sent extra bytes after the exchange-completing frame, and
    // the QUIC bridge's per-chunk feed (quinn-proto's `Chunks::next`
    // yields chunks one at a time) can deliver
    // `[valid frame][trailing junk]` across two `handle_data` calls.
    // Failing here surfaces the protocol violation through
    // `StreamError::Decode`; `enter_failed` is invoked below so the
    // FSM ends in `Failed(Decode)` rather than silently keeping the
    // `Done` lifecycle.
    if matches!(self.phase, StreamPhase::Done) {
      if data.is_empty() {
        return Ok(());
      }
      let err = StreamError::Decode(format!(
        "{} byte(s) of data after exchange completed (terminal Done phase)",
        data.len()
      ));
      self.enter_failed(err.clone());
      return Err(err);
    }
    // Every fatal inbound error — deadline elapsed, frame-cap exceeded,
    // an undecodable / oversize / length-desynced frame, or an unexpected
    // message for the phase — must TERMINALIZE the FSM, not merely be
    // returned. Callers observe stream liveness via `is_failed()` /
    // `poll_event()` / `poll_timeout()` and reap on terminal state; a
    // stream left non-terminal after a fatal frame would linger "active"
    // (and keep pulling a deadline) until some unrelated timeout. Funnel
    // every error path through `enter_failed` exactly once, then surface
    // the same error to the driver. The deadline case keeps the identical
    // observable outcome as `handle_timeout`: a frame whose bytes arrive
    // at/after the deadline is NOT delivered even if the driver processes
    // socket-readability before the timer callback.
    match self.handle_data_inner(data, now) {
      Ok(()) => Ok(()),
      Err(err) => {
        self.enter_failed(err.clone());
        Err(err)
      }
    }
  }

  /// The fallible body of [`handle_data`]. Returns `Err` for any condition
  /// that must tear the stream down; it does NOT itself terminalize the
  /// FSM — the single caller funnels every `Err` through `enter_failed`
  /// exactly once so no error path can forget to.
  fn handle_data_inner(&mut self, data: &[u8], now: Instant) -> Result<(), StreamError> {
    if let Some(deadline) = self.deadline {
      if now >= deadline {
        return Err(StreamError::Timeout);
      }
    }
    // Driver contract: a zero-length slice is the peer-closed / EOF marker
    // (driver_net `Some(Ok(0))` — clean TCP EOF or TLS `close_notify`;
    // driver_quic stream-finished / connection-gone). The phase + kind
    // determines whether EOF is premature:
    //
    //   * `OutboundAwaitingResponse`: peer hung up before sending the
    //     response — premature → `PeerClosed`.
    //   * `InboundAwaitingFirstMessage`: peer hung up before sending the
    //     first message (with a partial frame possibly buffered) —
    //     premature → `PeerClosed`. A non-empty `input_buf` confirms
    //     truncation, but the empty-buffer case also fails (we awaited
    //     a frame and got nothing).
    //   * `OutboundSendingRequest`: gated on the exchange kind. A
    //     response-bearing kind (`PushPull`, `ReliablePing`) requires
    //     the peer to send a response/ack after our request — even if
    //     EOF arrives before our `poll_transmit` advances the FSM to
    //     `OutboundAwaitingResponse`, the response would never arrive,
    //     so this is premature → `PeerClosed`. One-way `UserMessage`
    //     never expects inbound bytes; an EOF here is benign → `Ok`.
    //   * `InboundSendingResponse`: the peer already sent the full
    //     request (we transitioned out of `InboundAwaitingFirstMessage`).
    //     Their FIN is the natural close of their send half AFTER all
    //     input has been consumed → `Ok`. BUT if `input_buf` still
    //     holds bytes (partial-trailing-frame after the first), that
    //     is a truncated extra frame → `PeerClosed`.
    //
    // Terminal phases (`Done`, `Failed`) are short-circuited at the
    // `handle_data` entry guard (post-failure EOF is a silent no-op).
    if data.is_empty() {
      let buf_truncated = !self.input_buf.is_empty();
      return match &self.phase {
        StreamPhase::OutboundAwaitingResponse { .. }
        | StreamPhase::InboundAwaitingFirstMessage => Err(StreamError::PeerClosed),
        StreamPhase::OutboundSendingRequest { kind } => match kind {
          OutboundKind::UserMessage => Ok(()),
          OutboundKind::PushPull { .. } | OutboundKind::ReliablePing { .. } => {
            Err(StreamError::PeerClosed)
          }
        },
        StreamPhase::InboundSendingResponse { .. } => {
          if buf_truncated {
            Err(StreamError::PeerClosed)
          } else {
            Ok(())
          }
        }
        // Terminal phases unreachable here — caught by the entry guard.
        StreamPhase::Done | StreamPhase::Failed(_) => Ok(()),
      };
    }
    // Hard memory bound BEFORE the append. `probe_frame` already rejects a
    // declared frame whose total exceeds `max_frame_size`, but it runs
    // *after* `extend_from_slice`, so a single delivery of
    // `[oversize header + huge body]` (or `[valid frame + huge trailing]`)
    // would still balloon `input_buf` past the cap for one call. A
    // memberlist reliable exchange is a single frame per direction/phase
    // (push/pull, reliable ping, user message — no pipelining), and a
    // legitimate frame is itself <= max_frame_size and is consumed as soon
    // as it completes, so `input_buf` never legitimately needs to hold
    // more than the cap. Reject before allocating.
    if self.input_buf.len().saturating_add(data.len()) > self.max_frame_size {
      return Err(StreamError::Decode(format!(
        "inbound stream buffer would exceed max {} bytes",
        self.max_frame_size
      )));
    }
    self.input_buf.extend_from_slice(data);
    self.try_decode_frame(now)
  }

  /// Fire the deadline. If `now >= deadline`, transition the phase toward failure.
  pub fn handle_timeout(&mut self, now: Instant) {
    let deadline = match self.deadline {
      Some(d) => d,
      None => return,
    };
    if now < deadline {
      return;
    }
    match &self.phase {
      StreamPhase::Done | StreamPhase::Failed(_) => {}
      _ => {
        self.enter_failed(StreamError::Timeout);
      }
    }
  }

  // ─── internal helpers ──────────────────────────────────────────────────────

  fn enter_failed(&mut self, err: StreamError) {
    // A failed stream holds no pending bytes. Clearing `output_buf` makes
    // a timed-out/failed exchange un-drainable by a later `poll_transmit`
    // (paired with that method's terminal guard); clearing `input_buf`
    // drops any half-buffered inbound frame nothing will ever consume
    // (frees memory). Clearing `endpoint_events` is load-bearing: a
    // late post-dispatch validation failure (e.g. trailing-bytes after
    // a legitimate frame) MUST NOT let the FSM-event side effects of
    // the rejected frame survive — `dispatch_message` enqueues
    // `PushPullRequestReceived` / `PushPullReplyReceived` /
    // `ReliablePingAcked` / `UserDataReceived` BEFORE the post-dispatch
    // guard fires, and the driver's bridge would drain them and call
    // back into `Endpoint::handle_stream_event` (merging state /
    // encoding a response) for a stream that has just been declared
    // invalid. Discarding the queue at failure-entry makes the entire
    // exchange's side effects atomic with the success/failure decision.
    // Idempotent — `handle_data` early-returns on a terminal phase so
    // this runs at most once per stream.
    self.input_buf.clear();
    self.output_buf.clear();
    self.endpoint_events.clear();
    // Clear queued lifecycle events too. `dispatch_message` queues
    // `StreamEvent::Closed` when a frame completes the exchange, but
    // a subsequent post-dispatch validation (trailing-bytes guard,
    // for example) can still reject the same delivery — without
    // clearing the queue here, a `poll_event` drain would observe
    // `Closed` then `Failed`, contradicting the dispatch/validation
    // atomicity. After this function returns the only lifecycle
    // event the driver sees for this stream is the `Failed` we push
    // below.
    self.stream_events.clear();
    self
      .stream_events
      .push_back(StreamEvent::Failed(err.to_string()));
    self.phase = StreamPhase::Failed(err);
  }

  /// Probe whether a complete frame is in `input_buf`.
  ///
  /// Plain-frame layout: `[TAG: 1 byte][VARINT body_len][BODY: body_len bytes]`.
  /// We read the varint manually so we can compute `total` (and reject an
  /// oversize frame) BEFORE the body is buffered, rather than relying on
  /// the later `MessageRef::decode` underflow.
  ///
  /// - `Ok(Some((tag, total)))` — a full frame is present.
  /// - `Ok(None)` — header/body not yet fully buffered (keep reading).
  /// - `Err(_)` — the declared frame total exceeds `max_frame_size`, or the
  ///   length varint overflows `u32`. Both bound memory: the oversize
  ///   check fires the instant the varint decodes (before the body is
  ///   buffered), so a peer cannot make us buffer a huge declared body;
  ///   the overflow is caught within the first 5 varint bytes.
  #[allow(dead_code)]
  fn probe_frame(&self) -> Result<Option<(u8, usize)>, StreamError> {
    let buf = &self.input_buf;
    if buf.is_empty() {
      return Ok(None);
    }
    let tag = buf[0];
    // Decode the length varint into a u64 so a crafted *terminal* byte
    // cannot wrap (`80 80 80 80 10` is 2^32: the 5th byte shifted left
    // by 28 would wrap the u32 to 0 and slip past the size guard). 5
    // LEB128 bytes already cover a 35-bit length, far beyond any sane
    // `max_frame_size`; a 6th continuation byte means the declared length
    // is absurd → reject.
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut varint_bytes = 0usize;
    for i in 1..buf.len().min(6) {
      let b = buf[i];
      varint_bytes += 1;
      value |= ((b & 0x7f) as u64) << shift;
      if b & 0x80 == 0 {
        // Terminal byte: `value` is exact (no wrap). The wire framing
        // length is a u32 by protocol (`framing::decode_varint_u32`), so
        // a declared body length beyond u32::MAX is malformed REGARDLESS
        // of `max_stream_frame_size` — reject it before the cap compare.
        // Otherwise a cap configured above 4 GiB would let a terminal
        // overflowing varint pass here and then mis-decode downstream
        // (the u32 wire decoder wraps it), reopening the boundary/DoS path.
        if value > u32::MAX as u64 {
          return Err(StreamError::Decode(format!(
            "inbound stream frame length {value} exceeds the u32 wire limit"
          )));
        }
        // Compare the full frame total in u64 against the cap BEFORE
        // buffering the body.
        let total_u64 = 1u64 + varint_bytes as u64 + value;
        if total_u64 > self.max_frame_size as u64 {
          return Err(StreamError::Decode(format!(
            "inbound stream frame declares {total_u64} bytes, exceeds max {}",
            self.max_frame_size
          )));
        }
        let total = total_u64 as usize; // <= max_frame_size, fits usize
        if buf.len() >= total {
          return Ok(Some((tag, total)));
        } else {
          return Ok(None); // body not yet fully buffered (bounded by max)
        }
      }
      shift += 7;
      if shift >= 35 {
        return Err(StreamError::Decode(
          "inbound stream frame length varint too long / out of range".into(),
        ));
      }
    }
    Ok(None) // varint bytes not yet fully buffered
  }

  #[allow(dead_code)]
  fn try_decode_frame(&mut self, now: Instant) -> Result<(), StreamError> {
    let (_, total) = match self.probe_frame()? {
      Some(v) => v,
      None => return Ok(()), // not enough bytes yet
    };

    // Copy the frame bytes so we can release the borrow on input_buf before
    // calling dispatch_message (which needs &mut self).
    let frame_bytes: Vec<u8> = self.input_buf[..total].to_vec();

    // Advance input buffer past the consumed frame before decoding.
    let _ = self.input_buf.split_to(total);

    // Decode the full frame from our owned copy via the wire framing+bridge
    // path (`framing::decode_message` → `message_from_any`), yielding an
    // owned `typed::Message<I, A>`. `typed::Message` carries no codec, so
    // the zero-copy `MessageRef`/`DataRef` decode is no longer available.
    let (consumed, msg) =
      crate::wire::decode_message::<I, A>(&frame_bytes).map_err(StreamError::Decode)?;
    // A well-formed frame must consume exactly the bytes `probe_frame`
    // measured. A mismatch means the pre-scan and the real decoder
    // disagree on framing (a malformed/adversarial frame, or a framing
    // bug) — fail the stream at RUNTIME rather than only debug-asserting,
    // since silently `split_to(total)` after a short decode desyncs every
    // subsequent frame in release builds.
    if consumed != total {
      return Err(StreamError::Decode(format!(
        "frame length mismatch: pre-scan {total}, decoder consumed {consumed}"
      )));
    }

    self.dispatch_message(msg, now)?;

    // After dispatch, reject any trailing bytes in `input_buf` for
    // phases that FORBID further inbound bytes. Memberlist's reliable
    // exchanges are single-frame-per-direction, so any bytes beyond the
    // legitimate frame consumed above are adversarial.
    //
    // Per-phase rule:
    //   * `Done` — exchange completed. Trailing bytes are post-exchange
    //     junk; the next `handle_data(&[])` FIN call would short-circuit
    //     at the terminal entry guard and clean-reap the bridge if we
    //     don't fail here.
    //   * `InboundSendingResponse` — we already consumed the peer's
    //     full request; the peer's protocol obligation is to FIN, not
    //     send a second frame. Without this check, the bridge would
    //     drain the legitimate first frame's endpoint events into the
    //     `Endpoint` (merging state, encoding a response) BEFORE the
    //     `PhaseKind::Ignore` arm catches the second frame on the next
    //     `handle_data` call — the protocol violation is reported too
    //     late.
    //   * `OutboundSendingRequest` — would not normally be reached
    //     post-dispatch (the `OutboundAwaiting...` phase is what
    //     consumes a peer response). Including it is defensive: if a
    //     future dispatch arm transitions back to or stays in
    //     `OutboundSendingRequest`, trailing input bytes are still
    //     unexpected.
    //   * `OutboundAwaitingResponse` / `InboundAwaitingFirstMessage` —
    //     these phases legitimately accept partial-frame buffering
    //     between calls; allow trailing bytes (they may complete a
    //     future frame). The next `try_decode_frame` will attempt to
    //     decode them.
    //   * `Failed(_)` — unreachable here (`handle_data` entry guard
    //     short-circuits terminal phases).
    let forbids_further_input = match &self.phase {
      StreamPhase::Done
      | StreamPhase::InboundSendingResponse { .. }
      | StreamPhase::OutboundSendingRequest { .. } => true,
      StreamPhase::OutboundAwaitingResponse { .. }
      | StreamPhase::InboundAwaitingFirstMessage
      | StreamPhase::Failed(_) => false,
    };
    if forbids_further_input && !self.input_buf.is_empty() {
      return Err(StreamError::Decode(format!(
        "{} trailing byte(s) after frame in {} phase",
        self.input_buf.len(),
        match &self.phase {
          StreamPhase::Done => "Done",
          StreamPhase::InboundSendingResponse { .. } => "InboundSendingResponse",
          StreamPhase::OutboundSendingRequest { .. } => "OutboundSendingRequest",
          // Unreachable — `forbids_further_input` gated above.
          _ => "non-input",
        }
      )));
    }

    Ok(())
  }

  #[allow(dead_code)]
  fn dispatch_message(
    &mut self,
    msg: memberlist_wire::typed::Message<I, A>,
    now: Instant,
  ) -> Result<(), StreamError> {
    use memberlist_wire::typed::Message;

    // The decode path now yields an owned `typed::Message`, so the message
    // is matched *by value*. To avoid the original `match &self.phase { … }`
    // immutable borrow overlapping the `self.*` mutations in each arm, the
    // phase-relevant data (all `Copy`) is extracted into owned locals first;
    // the phase borrow is then dropped before the owned `msg` is consumed.
    enum PhaseKind {
      OutboundPushPull(PushPullKind),
      OutboundReliablePing(u32),
      OutboundUserMessage,
      InboundFirst,
      /// Terminal / sending phases: ignore inbound data.
      Ignore,
    }

    let phase_kind = match &self.phase {
      StreamPhase::OutboundAwaitingResponse { kind } => match kind {
        OutboundKind::PushPull { kind } => PhaseKind::OutboundPushPull(*kind),
        OutboundKind::ReliablePing { probe_seq } => PhaseKind::OutboundReliablePing(*probe_seq),
        OutboundKind::UserMessage => PhaseKind::OutboundUserMessage,
      },
      StreamPhase::InboundAwaitingFirstMessage => PhaseKind::InboundFirst,
      StreamPhase::OutboundSendingRequest { .. }
      | StreamPhase::InboundSendingResponse { .. }
      | StreamPhase::Done
      | StreamPhase::Failed(_) => PhaseKind::Ignore,
    };

    match phase_kind {
      // ── Outbound: awaiting peer's reply ────────────────────────────────
      PhaseKind::OutboundPushPull(pp_kind) => match msg {
        Message::PushPull(pp) => {
          let snapshot = push_pull_to_snapshot(pp, pp_kind)?;
          let peer = self.peer.cheap_clone();
          // Outbound stream: we initiated, peer replied. Emit PushPullReplyReceived
          // so handle_stream_event knows no response is needed.
          self
            .endpoint_events
            .push_back(EndpointEvent::PushPullReplyReceived {
              peer,
              states: snapshot.states,
              user_data: snapshot.user_data,
              kind: pp_kind,
            });
          self.phase = StreamPhase::Done;
          self.stream_events.push_back(StreamEvent::Closed);
        }
        other => {
          return Err(StreamError::UnexpectedMessage(format!(
            "in outbound PushPull({:?}) got unexpected {:?}",
            pp_kind, other
          )));
        }
      },
      PhaseKind::OutboundReliablePing(probe_seq) => match msg {
        Message::Ack(ack) => {
          // Reject a mismatched sequence number — accepting any Ack would
          // let a stale/relayed/spoofed Ack falsely complete this probe and
          // report a dead target healthy. The reliable ping's correctness
          // depends on matching the ack sequence to the probe sequence.
          if ack.sequence_number() != probe_seq {
            return Err(StreamError::UnexpectedMessage(format!(
              "in outbound ReliablePing(seq={}) got Ack for seq {}",
              probe_seq,
              ack.sequence_number()
            )));
          }
          self
            .endpoint_events
            .push_back(EndpointEvent::ReliablePingAcked {
              seq: probe_seq,
              at: now,
            });
          self.phase = StreamPhase::Done;
          self.stream_events.push_back(StreamEvent::Closed);
        }
        other => {
          return Err(StreamError::UnexpectedMessage(format!(
            "in outbound ReliablePing got unexpected {:?}",
            other
          )));
        }
      },
      PhaseKind::OutboundUserMessage => {
        // Outbound user messages expect no reply; any bytes are noise.
        // Do nothing; driver will close the stream.
      }

      // ── Inbound: awaiting first message from peer ───────────────────────
      PhaseKind::InboundFirst => {
        match msg {
          Message::PushPull(pp) => {
            let join = pp.join();
            let kind = if join {
              PushPullKind::Join
            } else {
              PushPullKind::Refresh
            };
            let snapshot = push_pull_to_snapshot(pp, kind)?;
            let peer = self.peer.cheap_clone();
            // Inbound stream: peer initiated, we must respond. Emit
            // PushPullRequestReceived so handle_stream_event returns
            // StreamCommand::SendPushPullResponse.
            self
              .endpoint_events
              .push_back(EndpointEvent::PushPullRequestReceived {
                peer,
                states: snapshot.states,
                user_data: snapshot.user_data,
                kind,
              });
            self.phase = StreamPhase::InboundSendingResponse {
              kind: InboundKind::PushPull,
            };
          }
          Message::Ping(ping) => {
            // Only Ack a Ping addressed to us. A misrouted or spoofed
            // ping for another node must not be answered (mirrors
            // `Endpoint::handle_ping`); answering it could report an
            // unreachable target healthy and suppress suspicion.
            if ping.target().id() != &self.local_id {
              return Err(StreamError::UnexpectedMessage(format!(
                "inbound reliable Ping target {} is not local {}",
                ping.target().id(),
                self.local_id
              )));
            }
            let ping_seq = ping.sequence_number();
            // Encode the Ack reply immediately into output_buf.
            // The driver drains it via poll_transmit; no EndpointEvent needed
            // (the ACK is fully self-contained — the responder doesn't need to
            // consult the Endpoint).
            let ack = memberlist_wire::typed::Ack::new(ping_seq);
            let ack_msg = memberlist_wire::typed::Message::<I, A>::Ack(ack);
            let encoded = crate::wire::encode_message::<I, A>(&ack_msg)
              .expect("Ack encode cannot fail for well-formed data");
            self.output_buf.extend(encoded);
            self.phase = StreamPhase::InboundSendingResponse {
              kind: InboundKind::ReliablePing { ping_seq },
            };
          }
          Message::UserData(data) => {
            // `typed::Message::UserData` already carries `Bytes`; move it
            // through without re-copying (the pre-9.4d proto path had to
            // `Bytes::copy_from_slice` a borrowed `&[u8]`).
            let peer = self.peer.cheap_clone();
            self
              .endpoint_events
              .push_back(EndpointEvent::UserDataReceived { peer, data });
            self.phase = StreamPhase::Done;
            self.stream_events.push_back(StreamEvent::Closed);
          }
          other => {
            return Err(StreamError::UnexpectedMessage(format!(
              "inbound first message: unexpected {:?}",
              other
            )));
          }
        }
      }

      // ── Sending phases: NO new messages expected ─────────────
      //
      // `OutboundSendingRequest` — we are still writing our request;
      // the peer should not be sending anything until they read it.
      // `InboundSendingResponse` — we already consumed the peer's full
      // request and are now sending the response; the peer should not
      // be sending more bytes in their single-frame-per-direction
      // protocol obligation.
      //
      // Terminal phases `Done` / `Failed` are short-circuited at the
      // `handle_data` entry guard (line ~310) and never reach this
      // arm. Reaching `Ignore` therefore means the peer sent a frame
      // when none was expected — adversarial behaviour; fail rather
      // than silently consume.
      PhaseKind::Ignore => {
        return Err(StreamError::UnexpectedMessage(format!(
          "unexpected frame in sending phase: {msg:?}"
        )));
      }
    }
    Ok(())
  }
}

// ─────────────────────────────── helper fns ──────────────────────────────────

/// Convert an owned `typed::PushPull` into an owned `PushPullSnapshot`.
///
/// The wire framing+bridge path already decoded every `PushNodeState` into
/// owned values, so this is infallible — no `from_ref`, no per-element
/// decode, no decode errors. `into_components()` moves the `join` flag and
/// the `user_data` `Bytes` out; the per-node states are then cloned out of
/// the shared `Arc<[PushNodeState]>` into the snapshot's `Vec` (the snapshot
/// owns a `Vec`, so the `Arc` slice cannot be moved wholesale).
#[allow(dead_code)]
fn push_pull_to_snapshot<I, A>(
  pp: memberlist_wire::typed::PushPull<I, A>,
  _kind: PushPullKind,
) -> Result<PushPullSnapshot<I, A>, StreamError>
where
  PushNodeState<I, A>: Clone,
{
  let (join, user_data, states) = pp.into_components();
  let states: Vec<PushNodeState<I, A>> = states.iter().cloned().collect();
  Ok(PushPullSnapshot {
    join,
    states,
    user_data,
  })
}
