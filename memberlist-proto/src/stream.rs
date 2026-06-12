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

use crate::Instant;
use std::collections::VecDeque;
#[cfg(not(feature = "std"))]
use std::{string::ToString, vec::Vec};

use crate::{Data, typed::PushNodeState};
use bytes::{Bytes, BytesMut};

use crate::{
  error::StreamError,
  event::{
    EndpointEvent, PushPullKind, PushPullReplyReceived, PushPullRequestReceived, ReliablePingAcked,
    StreamEvent, UserDataReceived,
  },
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
  join: bool,
  states: Vec<PushNodeState<I, A>>,
  user_data: Bytes,
}

impl<I, A> PushPullSnapshot<I, A> {
  /// Construct a new push/pull snapshot.
  #[inline(always)]
  pub const fn new(join: bool, states: Vec<PushNodeState<I, A>>, user_data: Bytes) -> Self {
    Self {
      join,
      states,
      user_data,
    }
  }

  /// Whether the peer considers this exchange a join (vs. periodic refresh).
  #[inline(always)]
  pub const fn is_join(&self) -> bool {
    self.join
  }

  /// Borrow the membership-view slice.
  #[inline(always)]
  pub fn states_slice(&self) -> &[PushNodeState<I, A>] {
    self.states.as_slice()
  }

  /// Borrow the application user-data as bytes.
  #[inline(always)]
  pub fn user_data(&self) -> &[u8] {
    self.user_data.as_ref()
  }

  /// Cheap-clone the user-data as a `Bytes` handle.
  #[inline(always)]
  pub fn user_data_bytes(&self) -> Bytes {
    self.user_data.clone()
  }

  /// Consume and return the (is_join, states, user_data) triple.
  #[inline(always)]
  pub fn into_parts(self) -> (bool, Vec<PushNodeState<I, A>>, Bytes) {
    (self.join, self.states, self.user_data)
  }
}

// ─────────────────────────────────────── internal enums ──────────────────────

/// What kind of outbound exchange a Stream is performing.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum OutboundKind {
  /// Full membership sync (push/pull). Carries whether this is a
  /// join-triggered exchange (vs. periodic anti-entropy).
  PushPull(PushPullKind),
  /// TCP/QUIC fallback ping for a timed-out UDP probe. Carries the
  /// sequence number of the original probe (links back to Probe).
  ReliablePing(u32),
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
  /// Carries the sequence number from the peer's Ping.
  ReliablePing(u32),
  /// Peer sent user data; we emitted the event; stream is done.
  UserMessage,
}

/// Phase of the per-stream FSM.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum StreamPhase {
  // ── Outbound (we dialed) ──────────────────────────────────────────────────
  /// We have bytes in `output_buf`; driver is draining them via `poll_transmit`.
  /// Carries the kind of outbound exchange being performed.
  OutboundSendingRequest(OutboundKind),
  /// Request fully sent (the last byte was drained via `poll_transmit`).
  /// Waiting for the peer's response bytes. Carries the kind of outbound
  /// exchange being performed.
  OutboundAwaitingResponse(OutboundKind),
  // ── Inbound (peer dialed us) ──────────────────────────────────────────────
  /// Connected; waiting for the peer to send the first message.
  InboundAwaitingFirstMessage,
  /// First message decoded. For PushPull: our response is in `output_buf`.
  /// For ReliablePing: our Ack is in `output_buf`. Carries the kind of
  /// inbound exchange being processed.
  InboundSendingResponse(InboundKind),
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
  I: crate::Id
    + Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: Data
    + crate::CheapClone
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
  #[inline(always)]
  pub fn peer_ref(&self) -> &A {
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
    match core::mem::replace(&mut self.phase, StreamPhase::Done) {
      // A reliable user message is one-way (`send_user_msg`: write then
      // done — no reply is read). It must terminate on send, NOT wait
      // for a response that never comes.
      StreamPhase::OutboundSendingRequest(OutboundKind::UserMessage) => {
        // Left as Done by the mem::replace.
        self.stream_events.push_back(StreamEvent::Closed);
      }
      StreamPhase::OutboundSendingRequest(kind) => {
        self.phase = StreamPhase::OutboundAwaitingResponse(kind);
      }
      StreamPhase::InboundSendingResponse(_) => {
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
      let err = StreamError::Decode(
        format!(
          "{} byte(s) of data after exchange completed (terminal Done phase)",
          data.len()
        )
        .into(),
      );
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
        StreamPhase::OutboundAwaitingResponse(_) | StreamPhase::InboundAwaitingFirstMessage => {
          Err(StreamError::PeerClosed)
        }
        StreamPhase::OutboundSendingRequest(kind) => match kind {
          OutboundKind::UserMessage => Ok(()),
          OutboundKind::PushPull(_) | OutboundKind::ReliablePing(_) => Err(StreamError::PeerClosed),
        },
        StreamPhase::InboundSendingResponse(_) => {
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
      return Err(StreamError::Decode(
        format!(
          "inbound stream buffer would exceed max {} bytes",
          self.max_frame_size
        )
        .into(),
      ));
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
          return Err(StreamError::Decode(
            format!("inbound stream frame length {value} exceeds the u32 wire limit").into(),
          ));
        }
        // Compare the full frame total in u64 against the cap BEFORE
        // buffering the body.
        let total_u64 = 1u64 + varint_bytes as u64 + value;
        if total_u64 > self.max_frame_size as u64 {
          return Err(StreamError::Decode(
            format!(
              "inbound stream frame declares {total_u64} bytes, exceeds max {}",
              self.max_frame_size
            )
            .into(),
          ));
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

    // Advance input buffer past the consumed frame before decoding. The
    // returned `BytesMut` is the consumed prefix; we already copied it into
    // `frame_bytes` above.
    let _ = self.input_buf.split_to(total);

    // Decode the full frame from our owned copy via the wire framing+bridge
    // path (`framing::decode_message` → `message_from_any`), yielding an
    // owned `typed::Message<I, A>`. `typed::Message` carries no codec, so
    // the zero-copy `MessageRef`/`DataRef` decode is no longer available.
    let (consumed, msg) = crate::wire::decode_message::<I, A>(&frame_bytes)?;
    // A well-formed frame must consume exactly the bytes `probe_frame`
    // measured. A mismatch means the pre-scan and the real decoder
    // disagree on framing (a malformed/adversarial frame, or a framing
    // bug) — fail the stream at RUNTIME rather than only debug-asserting,
    // since silently `split_to(total)` after a short decode desyncs every
    // subsequent frame in release builds.
    if consumed != total {
      return Err(StreamError::Decode(
        format!("frame length mismatch: pre-scan {total}, decoder consumed {consumed}").into(),
      ));
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
      | StreamPhase::InboundSendingResponse(_)
      | StreamPhase::OutboundSendingRequest(_) => true,
      StreamPhase::OutboundAwaitingResponse(_)
      | StreamPhase::InboundAwaitingFirstMessage
      | StreamPhase::Failed(_) => false,
    };
    if forbids_further_input && !self.input_buf.is_empty() {
      return Err(StreamError::Decode(
        format!(
          "{} trailing byte(s) after frame in {} phase",
          self.input_buf.len(),
          match &self.phase {
            StreamPhase::Done => "Done",
            StreamPhase::InboundSendingResponse(_) => "InboundSendingResponse",
            StreamPhase::OutboundSendingRequest(_) => "OutboundSendingRequest",
            // Unreachable — `forbids_further_input` gated above.
            _ => "non-input",
          }
        )
        .into(),
      ));
    }

    Ok(())
  }

  #[allow(dead_code)]
  fn dispatch_message(
    &mut self,
    msg: crate::typed::Message<I, A>,
    now: Instant,
  ) -> Result<(), StreamError> {
    use crate::typed::Message;

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
      StreamPhase::OutboundAwaitingResponse(kind) => match kind {
        OutboundKind::PushPull(k) => PhaseKind::OutboundPushPull(*k),
        OutboundKind::ReliablePing(probe_seq) => PhaseKind::OutboundReliablePing(*probe_seq),
        OutboundKind::UserMessage => PhaseKind::OutboundUserMessage,
      },
      StreamPhase::InboundAwaitingFirstMessage => PhaseKind::InboundFirst,
      StreamPhase::OutboundSendingRequest(_)
      | StreamPhase::InboundSendingResponse(_)
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
          let (_, states, user_data) = snapshot.into_parts();
          self
            .endpoint_events
            .push_back(EndpointEvent::PushPullReplyReceived(
              PushPullReplyReceived::new(peer, states, user_data, pp_kind),
            ));
          self.phase = StreamPhase::Done;
          self.stream_events.push_back(StreamEvent::Closed);
        }
        other => {
          return Err(StreamError::UnexpectedMessage(
            format!(
              "in outbound PushPull({:?}) got unexpected {:?}",
              pp_kind, other
            )
            .into(),
          ));
        }
      },
      PhaseKind::OutboundReliablePing(probe_seq) => match msg {
        Message::Ack(ack) => {
          // Reject a mismatched sequence number — accepting any Ack would
          // let a stale/relayed/spoofed Ack falsely complete this probe and
          // report a dead target healthy. The reliable ping's correctness
          // depends on matching the ack sequence to the probe sequence.
          if ack.sequence_number() != probe_seq {
            return Err(StreamError::UnexpectedMessage(
              format!(
                "in outbound ReliablePing(seq={}) got Ack for seq {}",
                probe_seq,
                ack.sequence_number()
              )
              .into(),
            ));
          }
          self
            .endpoint_events
            .push_back(EndpointEvent::ReliablePingAcked(ReliablePingAcked::new(
              probe_seq, now,
            )));
          self.phase = StreamPhase::Done;
          self.stream_events.push_back(StreamEvent::Closed);
        }
        other => {
          return Err(StreamError::UnexpectedMessage(
            format!("in outbound ReliablePing got unexpected {:?}", other).into(),
          ));
        }
      },
      PhaseKind::OutboundUserMessage => {
        // Outbound user messages expect no reply; any bytes are noise.
        // Deliberately lenient (NOT UnexpectedMessage): a one-way send already
        // succeeded once its bytes were gathered, so stray inbound bytes from
        // the peer must not retroactively fail it — the driver just closes the
        // stream. (Locked by `outbound_user_message_ignores_inbound_noise`.)
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
            let (_, states, user_data) = snapshot.into_parts();
            self
              .endpoint_events
              .push_back(EndpointEvent::PushPullRequestReceived(
                PushPullRequestReceived::new(peer, states, user_data, kind),
              ));
            self.phase = StreamPhase::InboundSendingResponse(InboundKind::PushPull);
          }
          Message::Ping(ping) => {
            // Only Ack a Ping addressed to us. A misrouted or spoofed
            // ping for another node must not be answered (mirrors
            // `Endpoint::handle_ping`); answering it could report an
            // unreachable target healthy and suppress suspicion.
            if ping.target_ref().id_ref() != &self.local_id {
              return Err(StreamError::UnexpectedMessage(
                format!(
                  "inbound reliable Ping target {} is not local {}",
                  ping.target_ref().id_ref(),
                  self.local_id
                )
                .into(),
              ));
            }
            let ping_seq = ping.sequence_number();
            // Encode the Ack reply immediately into output_buf.
            // The driver drains it via poll_transmit; no EndpointEvent needed
            // (the ACK is fully self-contained — the responder doesn't need to
            // consult the Endpoint).
            let ack = crate::typed::Ack::new(ping_seq);
            let ack_msg = crate::typed::Message::<I, A>::Ack(ack);
            let encoded = crate::wire::encode_message::<I, A>(&ack_msg)
              .expect("Ack encode cannot fail for well-formed data");
            self.output_buf.extend(encoded);
            self.phase = StreamPhase::InboundSendingResponse(InboundKind::ReliablePing(ping_seq));
          }
          Message::UserData(data) => {
            // `typed::Message::UserData` already carries `Bytes`; move it
            // through without re-copying (the pre-9.4d proto path had to
            // `Bytes::copy_from_slice` a borrowed `&[u8]`).
            let peer = self.peer.cheap_clone();
            self
              .endpoint_events
              .push_back(EndpointEvent::UserDataReceived(UserDataReceived::new(
                peer, data,
              )));
            self.phase = StreamPhase::Done;
            self.stream_events.push_back(StreamEvent::Closed);
          }
          other => {
            return Err(StreamError::UnexpectedMessage(
              format!("inbound first message: unexpected {:?}", other).into(),
            ));
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
        return Err(StreamError::UnexpectedMessage(
          format!("unexpected frame in sending phase: {msg:?}").into(),
        ));
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
  pp: crate::typed::PushPull<I, A>,
  _kind: PushPullKind,
) -> Result<PushPullSnapshot<I, A>, StreamError>
where
  PushNodeState<I, A>: Clone,
{
  let (join, user_data, states) = pp.into_components();
  let states: Vec<PushNodeState<I, A>> = states.iter().cloned().collect();
  Ok(PushPullSnapshot::new(join, states, user_data))
}

#[cfg(test)]
mod snapshot_tests {
  use std::net::SocketAddr;

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::PushPullSnapshot;
  use crate::typed::{PushNodeState, State};

  fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
  }

  #[test]
  fn push_pull_snapshot_join_round_trips() {
    let states = std::vec![
      PushNodeState::new(1, SmolStr::new("a"), addr(1), State::Alive),
      PushNodeState::new(2, SmolStr::new("b"), addr(2), State::Dead),
    ];
    let snap = PushPullSnapshot::new(true, states, Bytes::from_static(b"ud"));
    assert!(snap.is_join());
    assert_eq!(snap.states_slice().len(), 2);
    assert_eq!(snap.user_data(), b"ud");
    assert_eq!(snap.user_data_bytes().as_ref(), b"ud");

    let (join, decoded_states, ud) = snap.into_parts();
    assert!(join);
    assert_eq!(decoded_states.len(), 2);
    assert_eq!(ud.as_ref(), b"ud");
  }

  #[test]
  fn push_pull_snapshot_refresh_can_be_empty() {
    let snap = PushPullSnapshot::<SmolStr, SocketAddr>::new(false, std::vec![], Bytes::new());
    assert!(!snap.is_join());
    assert!(snap.states_slice().is_empty());
    assert!(snap.user_data().is_empty());
  }
}

#[cfg(test)]
mod fsm_tests {
  use crate::Instant;
  use core::{net::SocketAddr, time::Duration};
  use std::collections::VecDeque;

  use bytes::{Bytes, BytesMut};
  use smol_str::SmolStr;

  use super::{InboundKind, OutboundKind, Stream, StreamPhase};
  use crate::{
    error::StreamError,
    event::{EndpointEvent, PushPullKind, StreamEvent, StreamId},
    node::Node,
    typed::{Ack, Message, Ping, PushNodeState, PushPull, State},
  };

  const FRAME_MAX: usize = 64 * 1024;

  fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
  }

  /// Build a `Stream` directly in the named phase. Fields are `pub(crate)`,
  /// so the FSM can be exercised in isolation from the bridge/endpoint —
  /// mirroring `Endpoint::dial_succeeded` / `accept_stream`'s literals.
  fn stream_in(phase: StreamPhase, deadline: Option<Instant>) -> Stream<SmolStr, SocketAddr> {
    Stream {
      id: StreamId::from_raw(1),
      peer: addr(7002),
      local_id: SmolStr::new("local"),
      max_frame_size: FRAME_MAX,
      phase,
      input_buf: BytesMut::new(),
      output_buf: VecDeque::new(),
      deadline,
      endpoint_events: VecDeque::new(),
      stream_events: VecDeque::new(),
    }
  }

  /// Encode a typed message into a single plain frame `[tag][varint][body]`.
  fn frame(msg: &Message<SmolStr, SocketAddr>) -> Vec<u8> {
    crate::wire::encode_message::<SmolStr, SocketAddr>(msg).expect("encode test frame")
  }

  fn ping_frame(target_id: &str, target_port: u16, seq: u32) -> Vec<u8> {
    let ping = Ping::new(
      seq,
      Node::new(SmolStr::new("src"), addr(7000)),
      Node::new(SmolStr::new(target_id), addr(target_port)),
    );
    frame(&Message::Ping(ping))
  }

  fn ack_frame(seq: u32) -> Vec<u8> {
    frame(&Message::Ack(Ack::new(seq)))
  }

  fn push_pull_frame(join: bool) -> Vec<u8> {
    let pp = PushPull::new(
      join,
      std::iter::once(PushNodeState::new(
        7,
        SmolStr::new("p"),
        addr(7777),
        State::Alive,
      )),
    );
    frame(&Message::PushPull(pp))
  }

  // ─────────────────────────── trivial accessors ────────────────────────────

  /// `peer_ref` / `poll_event` / `poll_timeout(None on terminal)` accessors.
  #[test]
  fn accessors_peer_event_timeout() {
    let mut s = stream_in(StreamPhase::InboundAwaitingFirstMessage, None);
    assert_eq!(s.peer_ref(), &addr(7002));
    // No queued lifecycle event yet.
    assert!(s.poll_event().is_none());
    // Non-terminal with no deadline → poll_timeout None.
    assert!(s.poll_timeout().is_none());

    // Push a lifecycle event manually and drain it via the public accessor.
    s.stream_events.push_back(StreamEvent::Closed);
    assert!(matches!(s.poll_event(), Some(StreamEvent::Closed)));
    assert!(s.poll_event().is_none());
  }

  /// `poll_timeout` returns the deadline for a live phase and `None` once
  /// terminal (Done / Failed).
  #[test]
  fn poll_timeout_live_then_none_on_terminal() {
    let t0 = Instant::now();
    let dl = t0 + Duration::from_secs(5);
    let mut s = stream_in(StreamPhase::InboundAwaitingFirstMessage, Some(dl));
    assert_eq!(s.poll_timeout(), Some(dl));

    s.phase = StreamPhase::Done;
    assert!(s.poll_timeout().is_none());
    s.phase = StreamPhase::Failed(StreamError::Timeout);
    assert!(s.poll_timeout().is_none());
  }

  // ─────────────────────────── poll_transmit paths ──────────────────────────

  /// `poll_transmit` on a terminal stream emits nothing (the terminal guard),
  /// for both `Done` and `Failed`.
  #[test]
  fn poll_transmit_terminal_emits_nothing() {
    let t0 = Instant::now();
    let mut done = stream_in(StreamPhase::Done, None);
    done.output_buf.extend([1u8, 2, 3]);
    let mut out = Vec::new();
    assert_eq!(done.poll_transmit(t0, &mut out), None);
    assert!(out.is_empty());

    let mut failed = stream_in(StreamPhase::Failed(StreamError::Timeout), None);
    failed.output_buf.extend([4u8, 5]);
    assert_eq!(failed.poll_transmit(t0, &mut out), None);
    assert!(out.is_empty());
  }

  /// `poll_transmit` past the deadline fails the stream (Timeout) and emits
  /// nothing — the write-side deadline authority, independent of poll order.
  #[test]
  fn poll_transmit_past_deadline_fails_and_emits_nothing() {
    let t0 = Instant::now();
    let dl = t0 + Duration::from_secs(1);
    let mut s = stream_in(
      StreamPhase::OutboundSendingRequest(OutboundKind::UserMessage),
      Some(dl),
    );
    s.output_buf.extend([9u8, 9, 9]);
    let mut out = Vec::new();
    assert_eq!(
      s.poll_transmit(dl + Duration::from_millis(1), &mut out),
      None
    );
    assert!(out.is_empty());
    assert!(matches!(s.is_failed(), Some(StreamError::Timeout)));
    // A failed stream carries one queued `Failed` lifecycle event.
    assert!(matches!(s.poll_event(), Some(StreamEvent::Failed(_))));
  }

  /// Draining a one-way `UserMessage` request transitions straight to `Done`
  /// and queues `Closed` (the one-way terminate-on-send arm).
  #[test]
  fn poll_transmit_user_message_drains_to_done_closed() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundSendingRequest(OutboundKind::UserMessage),
      Some(t0 + Duration::from_secs(5)),
    );
    s.output_buf.extend([1u8, 2, 3, 4]);
    let mut out = Vec::new();
    let n = s.poll_transmit(t0, &mut out).expect("bytes drained");
    assert_eq!(n, 4);
    assert_eq!(out, [1, 2, 3, 4]);
    assert!(s.is_done());
    assert!(matches!(s.poll_event(), Some(StreamEvent::Closed)));
  }

  /// Draining an outbound response-bearing request (`PushPull`) moves to
  /// `OutboundAwaitingResponse` and queues NO lifecycle event.
  #[test]
  fn poll_transmit_push_pull_request_awaits_response() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundSendingRequest(OutboundKind::PushPull(PushPullKind::Join)),
      Some(t0 + Duration::from_secs(5)),
    );
    s.output_buf.extend([5u8, 6, 7]);
    let mut out = Vec::new();
    assert_eq!(s.poll_transmit(t0, &mut out), Some(3));
    assert!(matches!(
      s.phase,
      StreamPhase::OutboundAwaitingResponse(OutboundKind::PushPull(PushPullKind::Join))
    ));
    assert!(s.poll_event().is_none());
  }

  /// Draining an inbound response (`InboundSendingResponse`) terminates the
  /// exchange `Done` and queues `Closed`.
  #[test]
  fn poll_transmit_inbound_response_drains_to_done_closed() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundSendingResponse(InboundKind::ReliablePing(3)),
      Some(t0 + Duration::from_secs(5)),
    );
    s.output_buf.extend([8u8]);
    let mut out = Vec::new();
    assert_eq!(s.poll_transmit(t0, &mut out), Some(1));
    assert!(s.is_done());
    assert!(matches!(s.poll_event(), Some(StreamEvent::Closed)));
  }

  /// Draining bytes from a non-write phase (`OutboundAwaitingResponse`) leaves
  /// the phase untouched (the `other => restore` arm).
  #[test]
  fn poll_transmit_non_write_phase_restores_phase() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::UserMessage),
      Some(t0 + Duration::from_secs(5)),
    );
    s.output_buf.extend([1u8, 1]);
    let mut out = Vec::new();
    assert_eq!(s.poll_transmit(t0, &mut out), Some(2));
    assert!(matches!(
      s.phase,
      StreamPhase::OutboundAwaitingResponse(OutboundKind::UserMessage)
    ));
  }

  /// Empty `output_buf` with a live deadline → `None`, phase unchanged.
  #[test]
  fn poll_transmit_empty_buffer_returns_none() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundSendingRequest(OutboundKind::UserMessage),
      Some(t0 + Duration::from_secs(5)),
    );
    let mut out = Vec::new();
    assert_eq!(s.poll_transmit(t0, &mut out), None);
    assert!(matches!(
      s.phase,
      StreamPhase::OutboundSendingRequest(OutboundKind::UserMessage)
    ));
  }

  // ─────────────────────────── handle_timeout paths ─────────────────────────

  /// `handle_timeout` with no deadline is a no-op.
  #[test]
  fn handle_timeout_no_deadline_is_noop() {
    let t0 = Instant::now();
    let mut s = stream_in(StreamPhase::InboundAwaitingFirstMessage, None);
    s.handle_timeout(t0 + Duration::from_secs(100));
    assert!(!s.is_done());
    assert!(s.is_failed().is_none());
  }

  /// `handle_timeout` before the deadline does not fail the stream.
  #[test]
  fn handle_timeout_before_deadline_does_not_fail() {
    let t0 = Instant::now();
    let dl = t0 + Duration::from_secs(5);
    let mut s = stream_in(StreamPhase::InboundAwaitingFirstMessage, Some(dl));
    s.handle_timeout(t0);
    assert!(s.is_failed().is_none());
    assert_eq!(s.poll_timeout(), Some(dl));
  }

  /// `handle_timeout` at/after the deadline fails a live stream (Timeout).
  #[test]
  fn handle_timeout_at_deadline_fails_live_stream() {
    let t0 = Instant::now();
    let dl = t0 + Duration::from_secs(5);
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::UserMessage),
      Some(dl),
    );
    s.handle_timeout(dl);
    assert!(matches!(s.is_failed(), Some(StreamError::Timeout)));
  }

  /// `handle_timeout` past the deadline on an already-terminal stream is inert
  /// (the `Done | Failed => {}` arm) and does not re-queue a lifecycle event.
  #[test]
  fn handle_timeout_on_terminal_is_inert() {
    let t0 = Instant::now();
    let dl = t0 + Duration::from_secs(1);
    let mut s = stream_in(StreamPhase::Done, Some(dl));
    s.handle_timeout(dl + Duration::from_secs(1));
    assert!(s.is_done());
    assert!(s.poll_event().is_none());
  }

  // ─────────────────────────── handle_data guards ───────────────────────────

  /// Terminal-Failed ignores all data unconditionally (returns `Ok`, no
  /// re-fail).
  #[test]
  fn handle_data_on_failed_is_silent_ok() {
    let t0 = Instant::now();
    let mut s = stream_in(StreamPhase::Failed(StreamError::PeerClosed), None);
    assert!(s.handle_data(&[1, 2, 3], t0).is_ok());
    // Still the original failure; no new lifecycle event queued.
    assert!(matches!(s.is_failed(), Some(StreamError::PeerClosed)));
    assert!(s.poll_event().is_none());
  }

  /// Terminal-Done + empty data is the EOF marker → `Ok`, stays Done.
  #[test]
  fn handle_data_done_empty_is_eof_ok() {
    let t0 = Instant::now();
    let mut s = stream_in(StreamPhase::Done, None);
    assert!(s.handle_data(&[], t0).is_ok());
    assert!(s.is_done());
  }

  /// Terminal-Done + NON-empty data is adversarial trailing junk → `Decode`
  /// error AND the FSM terminalizes to `Failed(Decode)`.
  #[test]
  fn handle_data_done_nonempty_fails_decode() {
    let t0 = Instant::now();
    let mut s = stream_in(StreamPhase::Done, None);
    let err = s
      .handle_data(&[0xAB], t0)
      .expect_err("post-Done junk rejected");
    assert!(matches!(err, StreamError::Decode(_)));
    assert!(matches!(s.is_failed(), Some(StreamError::Decode(_))));
  }

  /// A data feed at/after the deadline fails Timeout (read-side deadline
  /// authority), even with otherwise-valid bytes buffered.
  #[test]
  fn handle_data_past_deadline_fails_timeout() {
    let t0 = Instant::now();
    let dl = t0 + Duration::from_secs(1);
    let mut s = stream_in(StreamPhase::InboundAwaitingFirstMessage, Some(dl));
    let f = ping_frame("local", 7002, 1);
    let err = s
      .handle_data(&f, dl + Duration::from_millis(1))
      .expect_err("past-deadline feed fails");
    assert!(matches!(err, StreamError::Timeout));
    assert!(matches!(s.is_failed(), Some(StreamError::Timeout)));
  }

  // ─────────────────────────── empty-data EOF arms ──────────────────────────

  /// EOF while `OutboundAwaitingResponse` is premature → `PeerClosed`.
  #[test]
  fn eof_while_awaiting_response_is_peer_closed() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::PushPull(PushPullKind::Join)),
      Some(t0 + Duration::from_secs(5)),
    );
    let err = s.handle_data(&[], t0).expect_err("premature EOF");
    assert!(matches!(err, StreamError::PeerClosed));
  }

  /// EOF while `InboundAwaitingFirstMessage` is premature → `PeerClosed`.
  #[test]
  fn eof_while_awaiting_first_message_is_peer_closed() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let err = s.handle_data(&[], t0).expect_err("premature EOF");
    assert!(matches!(err, StreamError::PeerClosed));
  }

  /// EOF while sending a one-way `UserMessage` request is benign → `Ok`.
  #[test]
  fn eof_while_sending_user_message_is_ok() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundSendingRequest(OutboundKind::UserMessage),
      Some(t0 + Duration::from_secs(5)),
    );
    assert!(s.handle_data(&[], t0).is_ok());
  }

  /// EOF while sending a response-bearing `ReliablePing` request is premature
  /// → `PeerClosed`.
  #[test]
  fn eof_while_sending_reliable_ping_request_is_peer_closed() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundSendingRequest(OutboundKind::ReliablePing(11)),
      Some(t0 + Duration::from_secs(5)),
    );
    let err = s.handle_data(&[], t0).expect_err("premature EOF");
    assert!(matches!(err, StreamError::PeerClosed));
  }

  /// EOF while `InboundSendingResponse` with an empty input buffer is the
  /// natural close → `Ok`.
  #[test]
  fn eof_while_inbound_sending_response_clean_is_ok() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundSendingResponse(InboundKind::PushPull),
      Some(t0 + Duration::from_secs(5)),
    );
    assert!(s.handle_data(&[], t0).is_ok());
  }

  /// EOF while `InboundSendingResponse` but with a partial trailing frame
  /// buffered → `PeerClosed`.
  #[test]
  fn eof_while_inbound_sending_response_with_partial_is_peer_closed() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundSendingResponse(InboundKind::PushPull),
      Some(t0 + Duration::from_secs(5)),
    );
    s.input_buf.extend_from_slice(&[0x08, 0x05]); // partial frame header
    let err = s.handle_data(&[], t0).expect_err("trailing partial frame");
    assert!(matches!(err, StreamError::PeerClosed));
  }

  // ─────────────────────────── memory bound ─────────────────────────────────

  /// A single delivery that would push `input_buf` past `max_frame_size` is
  /// rejected BEFORE the append → `Decode`.
  #[test]
  fn oversize_single_delivery_rejected_before_append() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let huge = std::vec![0u8; FRAME_MAX + 1];
    let err = s
      .handle_data(&huge, t0)
      .expect_err("oversize feed rejected");
    assert!(matches!(err, StreamError::Decode(_)));
    assert!(matches!(s.is_failed(), Some(StreamError::Decode(_))));
  }

  // ─────────────────────────── probe_frame edge cases ───────────────────────

  /// A frame whose declared length varint exceeds the `u32` wire limit is
  /// rejected the instant the terminal varint byte decodes — `80 80 80 80 10`
  /// is 2^32.
  #[test]
  fn frame_length_over_u32_wire_limit_rejected() {
    let t0 = Instant::now();
    // A frame cap above 4 GiB so the u32-wire check (not the cap) is the
    // gate exercised here.
    let mut s = Stream::<SmolStr, SocketAddr> {
      id: StreamId::from_raw(1),
      peer: addr(7002),
      local_id: SmolStr::new("local"),
      max_frame_size: usize::MAX,
      phase: StreamPhase::InboundAwaitingFirstMessage,
      input_buf: BytesMut::new(),
      output_buf: VecDeque::new(),
      deadline: Some(t0 + Duration::from_secs(5)),
      endpoint_events: VecDeque::new(),
      stream_events: VecDeque::new(),
    };
    // [tag=8][varint 0x80 0x80 0x80 0x80 0x10 == 2^32]
    let bytes = [8u8, 0x80, 0x80, 0x80, 0x80, 0x10];
    let err = s
      .handle_data(&bytes, t0)
      .expect_err("over-u32 length rejected");
    match err {
      StreamError::Decode(m) => assert!(
        m.contains("u32 wire limit"),
        "expected the u32-wire-limit message, got: {m}"
      ),
      other => panic!("expected Decode, got {other:?}"),
    }
  }

  /// A frame whose declared total exceeds `max_frame_size` (but is a valid
  /// u32) is rejected the instant the length varint decodes, before the body
  /// is buffered.
  #[test]
  fn frame_total_over_cap_rejected_before_body() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    // Declare a body length just over the cap with a 2-byte varint, then send
    // ONLY the 4-byte header (no body). The cap check fires regardless.
    // body_len = FRAME_MAX (so total = 1 + 2 + FRAME_MAX > FRAME_MAX).
    let mut bytes = std::vec![8u8];
    crate::framing::encode_varint_u32(FRAME_MAX as u32, &mut bytes);
    let err = s
      .handle_data(&bytes, t0)
      .expect_err("over-cap declared length rejected");
    match err {
      StreamError::Decode(m) => assert!(
        m.contains("exceeds max"),
        "expected the cap message, got: {m}"
      ),
      other => panic!("expected Decode, got {other:?}"),
    }
  }

  /// A length varint with a 6th continuation byte (out of LEB128 range) is
  /// rejected.
  #[test]
  fn frame_length_varint_too_long_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    // tag + six 0x80 continuation bytes: shift reaches 35 before a terminal
    // byte → out-of-range reject.
    let bytes = [8u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80];
    let err = s
      .handle_data(&bytes, t0)
      .expect_err("over-long varint rejected");
    assert!(matches!(err, StreamError::Decode(_)));
  }

  /// A partial header (only the tag byte buffered) is not yet decodable;
  /// `handle_data` returns `Ok` and the stream stays live awaiting more bytes.
  #[test]
  fn partial_header_buffers_and_waits() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    assert!(s.handle_data(&[8u8], t0).is_ok());
    assert!(s.is_failed().is_none());
    assert!(!s.is_done());
  }

  /// A frame body that is only partially buffered (header present, body
  /// incomplete) waits: `handle_data` returns `Ok`, no dispatch yet.
  #[test]
  fn partial_body_waits_for_remainder() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let full = ping_frame("local", 7002, 1);
    // Feed all but the last body byte.
    assert!(s.handle_data(&full[..full.len() - 1], t0).is_ok());
    assert!(s.is_failed().is_none());
    assert!(!s.is_done());
    assert!(s.poll_endpoint_event().is_none());
  }

  // ─────────────────────────── dispatch: inbound first ──────────────────────

  /// Inbound PushPull (Join) → emits `PushPullRequestReceived`, phase advances
  /// to `InboundSendingResponse(PushPull)`.
  #[test]
  fn inbound_push_pull_join_emits_request_received() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let f = push_pull_frame(true);
    s.handle_data(&f, t0).expect("valid push/pull decodes");
    match s.poll_endpoint_event().expect("request-received queued") {
      EndpointEvent::PushPullRequestReceived(r) => {
        assert_eq!(r.kind(), PushPullKind::Join);
        assert_eq!(r.states_slice().len(), 1);
      }
      other => panic!("expected PushPullRequestReceived, got {other:?}"),
    }
    assert!(matches!(
      s.phase,
      StreamPhase::InboundSendingResponse(InboundKind::PushPull)
    ));
  }

  /// Inbound PushPull with join=false maps to `PushPullKind::Refresh`.
  #[test]
  fn inbound_push_pull_refresh_kind() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let f = push_pull_frame(false);
    s.handle_data(&f, t0).expect("decodes");
    match s.poll_endpoint_event().expect("queued") {
      EndpointEvent::PushPullRequestReceived(r) => assert_eq!(r.kind(), PushPullKind::Refresh),
      other => panic!("expected PushPullRequestReceived, got {other:?}"),
    }
  }

  /// Inbound Ping addressed to us → an Ack is encoded into `output_buf` and
  /// the phase advances to `InboundSendingResponse(ReliablePing)`; no endpoint
  /// event is queued (the Ack is self-contained).
  #[test]
  fn inbound_ping_for_us_encodes_ack() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let f = ping_frame("local", 7002, 77);
    s.handle_data(&f, t0).expect("ping decodes");
    assert!(s.poll_endpoint_event().is_none(), "Ack is self-contained");
    assert!(matches!(
      s.phase,
      StreamPhase::InboundSendingResponse(InboundKind::ReliablePing(77))
    ));
    // The encoded Ack is queued for transmit.
    let mut out = Vec::new();
    let n = s.poll_transmit(t0, &mut out).expect("ack bytes queued");
    assert!(n > 0);
    assert_eq!(
      out[0],
      Message::<SmolStr, SocketAddr>::Ack(Ack::new(77)).tag()
    );
    // Draining the response moves to Done.
    assert!(s.is_done());
  }

  /// Inbound Ping addressed to ANOTHER node is rejected (not Acked) →
  /// `UnexpectedMessage`, FSM fails.
  #[test]
  fn inbound_ping_for_other_target_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let f = ping_frame("not-local", 9999, 5);
    let err = s.handle_data(&f, t0).expect_err("misrouted ping rejected");
    assert!(matches!(err, StreamError::UnexpectedMessage(_)));
    assert!(matches!(
      s.is_failed(),
      Some(StreamError::UnexpectedMessage(_))
    ));
  }

  /// Inbound UserData → emits `UserDataReceived`, phase Done, `Closed` queued.
  #[test]
  fn inbound_user_data_emits_received_and_done() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let f = frame(&Message::UserData(Bytes::from_static(b"payload-bytes")));
    s.handle_data(&f, t0).expect("user data decodes");
    match s.poll_endpoint_event().expect("user-data event queued") {
      EndpointEvent::UserDataReceived(u) => {
        assert_eq!(u.data_ref().as_ref(), b"payload-bytes");
        assert_eq!(u.peer_ref(), &addr(7002));
      }
      other => panic!("expected UserDataReceived, got {other:?}"),
    }
    assert!(s.is_done());
    assert!(matches!(s.poll_event(), Some(StreamEvent::Closed)));
  }

  /// Inbound first message of an UNEXPECTED variant (e.g. a bare Ack) →
  /// `UnexpectedMessage`.
  #[test]
  fn inbound_first_unexpected_variant_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let f = ack_frame(1);
    let err = s
      .handle_data(&f, t0)
      .expect_err("bare Ack is not a valid first message");
    assert!(matches!(err, StreamError::UnexpectedMessage(_)));
  }

  // ─────────────────────────── dispatch: outbound replies ───────────────────

  /// Outbound PushPull awaiting a reply, peer replies with a PushPull →
  /// `PushPullReplyReceived`, Done, `Closed`.
  #[test]
  fn outbound_push_pull_reply_received() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::PushPull(PushPullKind::Refresh)),
      Some(t0 + Duration::from_secs(5)),
    );
    let f = push_pull_frame(false);
    s.handle_data(&f, t0).expect("reply decodes");
    match s.poll_endpoint_event().expect("reply queued") {
      EndpointEvent::PushPullReplyReceived(r) => {
        assert_eq!(r.kind(), PushPullKind::Refresh);
        assert_eq!(r.states_slice().len(), 1);
      }
      other => panic!("expected PushPullReplyReceived, got {other:?}"),
    }
    assert!(s.is_done());
    assert!(matches!(s.poll_event(), Some(StreamEvent::Closed)));
  }

  /// Outbound PushPull awaiting reply but peer sends a non-PushPull (Ack) →
  /// `UnexpectedMessage`.
  #[test]
  fn outbound_push_pull_unexpected_reply_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::PushPull(PushPullKind::Join)),
      Some(t0 + Duration::from_secs(5)),
    );
    let f = ack_frame(1);
    let err = s
      .handle_data(&f, t0)
      .expect_err("non-PushPull reply rejected");
    assert!(matches!(err, StreamError::UnexpectedMessage(_)));
  }

  /// Outbound ReliablePing awaiting an Ack with the matching sequence number →
  /// `ReliablePingAcked`, Done.
  #[test]
  fn outbound_reliable_ping_ack_matching_seq() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::ReliablePing(42)),
      Some(t0 + Duration::from_secs(5)),
    );
    let f = ack_frame(42);
    s.handle_data(&f, t0).expect("matching ack decodes");
    match s.poll_endpoint_event().expect("ack-acked queued") {
      EndpointEvent::ReliablePingAcked(a) => assert_eq!(a.seq(), 42),
      other => panic!("expected ReliablePingAcked, got {other:?}"),
    }
    assert!(s.is_done());
  }

  /// Outbound ReliablePing awaiting an Ack but the Ack sequence MISMATCHES →
  /// `UnexpectedMessage` (a stale/spoofed ack must not complete the probe).
  #[test]
  fn outbound_reliable_ping_ack_mismatched_seq_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::ReliablePing(42)),
      Some(t0 + Duration::from_secs(5)),
    );
    let f = ack_frame(7);
    let err = s
      .handle_data(&f, t0)
      .expect_err("mismatched ack seq rejected");
    assert!(matches!(err, StreamError::UnexpectedMessage(_)));
  }

  /// Outbound ReliablePing awaiting an Ack but peer sends a non-Ack (PushPull)
  /// → `UnexpectedMessage`.
  #[test]
  fn outbound_reliable_ping_unexpected_variant_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::ReliablePing(42)),
      Some(t0 + Duration::from_secs(5)),
    );
    let f = push_pull_frame(true);
    let err = s.handle_data(&f, t0).expect_err("non-Ack reply rejected");
    assert!(matches!(err, StreamError::UnexpectedMessage(_)));
  }

  /// Outbound UserMessage expects no reply; any inbound frame is benign noise
  /// (the FSM consumes it without erroring and without queuing an event).
  #[test]
  fn outbound_user_message_ignores_inbound_noise() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::OutboundAwaitingResponse(OutboundKind::UserMessage),
      Some(t0 + Duration::from_secs(5)),
    );
    let f = ack_frame(1);
    s.handle_data(&f, t0)
      .expect("noise is consumed, not an error");
    assert!(s.poll_endpoint_event().is_none());
  }

  // ─────────────────────────── dispatch: sending-phase guard ────────────────

  /// A frame arriving while the FSM is in a sending phase
  /// (`InboundSendingResponse`) — the peer should be reading our response, not
  /// sending — is the `PhaseKind::Ignore` reject arm → `UnexpectedMessage`.
  #[test]
  fn frame_in_sending_phase_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundSendingResponse(InboundKind::PushPull),
      Some(t0 + Duration::from_secs(5)),
    );
    let f = push_pull_frame(true);
    let err = s
      .handle_data(&f, t0)
      .expect_err("unexpected frame in sending phase");
    assert!(matches!(err, StreamError::UnexpectedMessage(_)));
  }

  // ─────────────────────────── trailing-bytes guard ─────────────────────────

  /// A valid first frame followed by trailing bytes (a second frame's worth)
  /// in the SAME delivery, landing the FSM in a forbids-further-input phase
  /// (`Done` for an inbound UserData), is rejected → `Decode` ("trailing
  /// byte(s)").
  #[test]
  fn trailing_bytes_after_terminal_frame_rejected() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let mut bytes = frame(&Message::UserData(Bytes::from_static(b"hi")));
    // Append junk that survives past the consumed frame: a second tag byte +
    // partial header keeps `input_buf` non-empty after the first dispatch.
    bytes.extend_from_slice(&[8u8, 0x01]);
    let err = s
      .handle_data(&bytes, t0)
      .expect_err("trailing bytes rejected");
    match err {
      StreamError::Decode(m) => assert!(
        m.contains("trailing byte"),
        "expected trailing-byte message, got: {m}"
      ),
      other => panic!("expected Decode, got {other:?}"),
    }
    assert!(matches!(s.is_failed(), Some(StreamError::Decode(_))));
  }

  /// `discard_pending_events` clears queued endpoint + lifecycle events
  /// without changing the phase (the transport-deauthorization suppression
  /// seam).
  #[test]
  #[cfg(feature = "quic")]
  fn discard_pending_events_clears_queues_keeps_phase() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    let f = frame(&Message::UserData(Bytes::from_static(b"x")));
    s.handle_data(&f, t0).expect("decodes");
    assert!(s.is_done());
    s.discard_pending_events();
    assert!(s.poll_endpoint_event().is_none());
    assert!(s.poll_event().is_none());
    assert!(s.is_done(), "phase preserved");
  }

  /// A well-FRAMED frame (`probe_frame` accepts the `[tag][len][body]` shape and
  /// confirms the full body is buffered) whose body is undecodable for its tag
  /// fails the `wire::decode_message` step → `StreamError::Decode`. This is the
  /// `try_decode_frame` decode-error arm: the pre-scan and the real decoder
  /// agree on framing, but the bytes inside the frame are garbage for a
  /// PushPull, so `PushPull::decode_from_slice` rejects them.
  #[test]
  fn well_framed_but_undecodable_body_fails_decode() {
    let t0 = Instant::now();
    let mut s = stream_in(
      StreamPhase::InboundAwaitingFirstMessage,
      Some(t0 + Duration::from_secs(5)),
    );
    // [tag=8 PushPull][varint len = 4][4 garbage body bytes]. probe_frame sees a
    // complete frame (tag is not validated there, the declared body is fully
    // buffered, total <= max_frame_size); the decoder then fails on the body.
    let mut bytes = std::vec![8u8];
    crate::framing::encode_varint_u32(4, &mut bytes);
    bytes.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);
    let err = s
      .handle_data(&bytes, t0)
      .expect_err("a well-framed but undecodable body fails decode");
    assert!(
      matches!(err, StreamError::Frame(_)),
      "an undecodable body surfaces as Frame (the inner-frame codec decode fails), got {err:?}"
    );
    assert!(matches!(s.is_failed(), Some(StreamError::Frame(_))));
  }
}
