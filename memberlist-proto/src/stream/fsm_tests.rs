use crate::Instant;
use core::{net::SocketAddr, time::Duration};
use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use smol_str::SmolStr;

use super::{OutboundKind, Stream, StreamPhase};
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

/// Concatenate the `Bytes` chunk chain `poll_transmit` drains into, for
/// byte-level assertions.
fn concat(chunks: &VecDeque<Bytes>) -> Vec<u8> {
  chunks.iter().flat_map(|c| c.iter().copied()).collect()
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
  done
    .output_buf
    .push_back(Bytes::copy_from_slice(&[1, 2, 3]));
  let mut out: VecDeque<Bytes> = VecDeque::new();
  assert_eq!(done.poll_transmit(t0, &mut out), None);
  assert!(out.is_empty());

  let mut failed = stream_in(StreamPhase::Failed(StreamError::Timeout), None);
  failed.output_buf.push_back(Bytes::copy_from_slice(&[4, 5]));
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
  s.output_buf.push_back(Bytes::copy_from_slice(&[9, 9, 9]));
  let mut out: VecDeque<Bytes> = VecDeque::new();
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
  s.output_buf
    .push_back(Bytes::copy_from_slice(&[1, 2, 3, 4]));
  let mut out: VecDeque<Bytes> = VecDeque::new();
  let n = s.poll_transmit(t0, &mut out).expect("bytes drained");
  assert_eq!(n, 4);
  assert_eq!(concat(&out), [1, 2, 3, 4]);
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
  s.output_buf.push_back(Bytes::copy_from_slice(&[5, 6, 7]));
  let mut out: VecDeque<Bytes> = VecDeque::new();
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
    StreamPhase::InboundSendingResponse,
    Some(t0 + Duration::from_secs(5)),
  );
  s.output_buf.push_back(Bytes::copy_from_slice(&[8]));
  let mut out: VecDeque<Bytes> = VecDeque::new();
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
  s.output_buf.push_back(Bytes::copy_from_slice(&[1, 1]));
  let mut out: VecDeque<Bytes> = VecDeque::new();
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
  let mut out: VecDeque<Bytes> = VecDeque::new();
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
    StreamPhase::InboundSendingResponse,
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
    StreamPhase::InboundSendingResponse,
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
/// to `InboundSendingResponse`.
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
  assert!(matches!(s.phase, StreamPhase::InboundSendingResponse));
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
/// the phase advances to `InboundSendingResponse`; no endpoint
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
  assert!(matches!(s.phase, StreamPhase::InboundSendingResponse));
  // The encoded Ack is queued for transmit.
  let mut out: VecDeque<Bytes> = VecDeque::new();
  let n = s.poll_transmit(t0, &mut out).expect("ack bytes queued");
  assert!(n > 0);
  assert_eq!(
    concat(&out)[0],
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
    StreamPhase::InboundSendingResponse,
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
