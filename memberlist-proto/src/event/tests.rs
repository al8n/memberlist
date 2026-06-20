use std::{net::SocketAddr, sync::Arc};

use bytes::Bytes;
use smallvec_wrapper::TinyVec;
use smol_str::SmolStr;

use super::{
  CompoundTransmit, ExchangeCompleted, ExchangeId, ExchangeKind, ExchangeStatus, NodeConflict,
  PacketTransmit, PingId, PushPullKind, Reliability, Transmit, UserPacket,
};
use crate::typed::{Ack, Message, NodeState, PushNodeState, State};
use core::time::Duration;

fn addr(port: u16) -> SocketAddr {
  SocketAddr::from(([127, 0, 0, 1], port))
}

fn ack_msg(seq: u32) -> Message<SmolStr, SocketAddr> {
  Message::ack(Ack::new(seq))
}

#[test]
fn hint_enums_expose_their_predicates() {
  assert!(Reliability::Reliable.is_reliable());
  assert!(!Reliability::Unreliable.is_reliable());
  assert!(PushPullKind::Join.is_join());
  assert!(!PushPullKind::Refresh.is_join());
  assert!(ExchangeKind::PushPull.is_push_pull());
  assert!(!ExchangeKind::ReliablePing.is_push_pull());
  assert!(!ExchangeKind::UserMessage.is_push_pull());
  assert!(ExchangeStatus::Succeeded.is_succeeded());
  assert!(!ExchangeStatus::Failed.is_succeeded());
}

#[test]
fn id_newtypes_round_trip_their_raw_value() {
  assert_eq!(ExchangeId::new(99).get(), 99);
  assert_eq!(PingId::new(7).get(), 7);
}

#[test]
fn exchange_completed_carries_its_fields() {
  let ec = ExchangeCompleted::new(
    ExchangeId::new(3),
    addr(7000),
    ExchangeStatus::Failed,
    ExchangeKind::PushPull,
  );
  assert_eq!(ec.eid(), ExchangeId::new(3));
  assert_eq!(ec.peer(), &addr(7000));
  assert_eq!(ec.outcome(), ExchangeStatus::Failed);
  assert_eq!(ec.kind(), ExchangeKind::PushPull);
}

#[test]
fn packet_and_compound_transmit_round_trip() {
  let pt = PacketTransmit::new(addr(1), ack_msg(5));
  assert_eq!(pt.to_ref(), &addr(1));
  assert!(matches!(pt.message_ref(), Message::Ack(_)));
  let (to, m) = pt.into_parts();
  assert_eq!(to, addr(1));
  assert!(matches!(m, Message::Ack(_)));

  let ct = CompoundTransmit::new(addr(2), TinyVec::from_iter([ack_msg(1), ack_msg(2)]));
  assert_eq!(ct.to_ref(), &addr(2));
  assert_eq!(ct.messages_slice().len(), 2);
  let (to, msgs) = ct.into_parts();
  assert_eq!(to, addr(2));
  assert_eq!(msgs.len(), 2);
}

#[test]
fn transmit_enum_wraps_packet_and_compound() {
  let packet = Transmit::Packet(PacketTransmit::new(addr(1), ack_msg(1)));
  let compound = Transmit::Compound(CompoundTransmit::new(
    addr(2),
    TinyVec::from_iter([ack_msg(1), ack_msg(2)]),
  ));
  assert!(matches!(packet, Transmit::Packet(_)));
  assert!(matches!(compound, Transmit::Compound(_)));
}

#[test]
fn node_conflict_holds_both_peers() {
  let existing = Arc::new(NodeState::new(SmolStr::new("n"), addr(10), State::Alive));
  let other = Arc::new(NodeState::new(SmolStr::new("n"), addr(20), State::Alive));
  let nc = NodeConflict::new(existing.clone(), other.clone());
  assert!(Arc::ptr_eq(nc.existing_ref(), &existing));
  assert!(Arc::ptr_eq(nc.other_ref(), &other));
}

#[test]
fn user_packet_exposes_payload_and_parts() {
  let up = UserPacket::new(
    addr(30),
    Bytes::from_static(b"hello"),
    Reliability::Unreliable,
  );
  assert_eq!(up.from_ref(), &addr(30));
  assert_eq!(up.data_ref().as_ref(), b"hello");
  assert!(!up.reliability().is_reliable());
  let (from, data, rel) = up.into_parts();
  assert_eq!(from, addr(30));
  assert_eq!(data.as_ref(), b"hello");
  assert!(!rel.is_reliable());
}

#[test]
fn exchange_id_from_stream_id_preserves_raw_value() {
  use super::{ExchangeId, StreamId};
  let sid = StreamId::from_raw(0xFEED_BEEF);
  assert_eq!(sid.as_u64(), 0xFEED_BEEF);
  // `From<StreamId> for ExchangeId` must preserve the raw correlation token.
  let eid: ExchangeId = sid.into();
  assert_eq!(eid.get(), 0xFEED_BEEF);
  assert_eq!(eid, ExchangeId::new(0xFEED_BEEF));
}

#[test]
fn compound_transmit_requires_two_messages() {
  let result =
    std::panic::catch_unwind(|| CompoundTransmit::new(addr(1), TinyVec::from_iter([ack_msg(1)])));
  assert!(
    result.is_err(),
    "CompoundTransmit with a single message must panic"
  );
}

#[test]
fn remote_state_received_exposes_fields_and_parts() {
  use super::RemoteStateReceived;
  let rs = RemoteStateReceived::new(addr(40), Bytes::from_static(b"state"), true);
  assert_eq!(rs.peer_ref(), &addr(40));
  assert_eq!(rs.user_data_ref().as_ref(), b"state");
  assert!(rs.join());
  let (peer, data, join) = rs.into_parts();
  assert_eq!(peer, addr(40));
  assert_eq!(data.as_ref(), b"state");
  assert!(join);

  // The non-join (refresh) construction path.
  let refresh = RemoteStateReceived::new(addr(41), Bytes::new(), false);
  assert!(!refresh.join());
}

fn node_arc(port: u16) -> Arc<NodeState<SmolStr, SocketAddr>> {
  Arc::new(NodeState::new(
    SmolStr::new("peer"),
    addr(port),
    State::Alive,
  ))
}

#[test]
fn ping_completed_exposes_all_fields() {
  use super::{PingCompleted, PingId};
  let node = node_arc(7000);
  let pc = PingCompleted::new(
    PingId::new(11),
    node.clone(),
    Duration::from_millis(42),
    Bytes::from_static(b"ack-payload"),
  );
  assert_eq!(pc.ping_id(), PingId::new(11));
  assert!(Arc::ptr_eq(pc.node_ref(), &node));
  assert_eq!(pc.rtt(), Duration::from_millis(42));
  assert_eq!(pc.payload_ref().as_ref(), b"ack-payload");
}

#[test]
fn ping_failed_exposes_id_and_node() {
  use super::{PingFailed, PingId};
  let node = node_arc(7001);
  let pf = PingFailed::new(PingId::new(12), node.clone());
  assert_eq!(pf.ping_id(), PingId::new(12));
  assert!(Arc::ptr_eq(pf.node_ref(), &node));
}

#[test]
fn decode_error_exposes_source_and_message() {
  use super::DecodeError;
  let de = DecodeError::new(addr(50), String::from("bad frame"));
  assert_eq!(de.from_ref(), &addr(50));
  assert_eq!(de.err(), "bad frame");
}

#[test]
fn dial_requested_exposes_fields_and_parts() {
  use super::{DialRequested, StreamId};
  let now = crate::Instant::now();
  let deadline = now + Duration::from_secs(5);
  let dr = DialRequested::new(StreamId::from_raw(77), addr(60), deadline);
  assert_eq!(dr.id(), StreamId::from_raw(77));
  assert_eq!(dr.peer_ref(), &addr(60));
  assert_eq!(dr.deadline(), deadline);
  let (id, peer, dl) = dr.into_parts();
  assert_eq!(id, StreamId::from_raw(77));
  assert_eq!(peer, addr(60));
  assert_eq!(dl, deadline);
}

fn push_state(port: u16) -> PushNodeState<SmolStr, SocketAddr> {
  PushNodeState::new(3, SmolStr::new("p"), addr(port), State::Alive)
}

#[test]
fn push_pull_request_received_exposes_fields_and_parts() {
  use super::{PushPullKind, PushPullRequestReceived};
  let pr = PushPullRequestReceived::new(
    addr(70),
    std::vec![push_state(70), push_state(71)],
    Bytes::from_static(b"ud"),
    PushPullKind::Join,
  );
  assert_eq!(pr.peer_ref(), &addr(70));
  assert_eq!(pr.states_slice().len(), 2);
  assert_eq!(pr.user_data_ref().as_ref(), b"ud");
  assert!(pr.kind().is_join());
  let (peer, states, ud, kind) = pr.into_parts();
  assert_eq!(peer, addr(70));
  assert_eq!(states.len(), 2);
  assert_eq!(ud.as_ref(), b"ud");
  assert!(kind.is_join());
}

#[test]
fn push_pull_reply_received_exposes_fields_and_parts() {
  use super::{PushPullKind, PushPullReplyReceived};
  let pr = PushPullReplyReceived::new(
    addr(80),
    std::vec![push_state(80)],
    Bytes::new(),
    PushPullKind::Refresh,
  );
  assert_eq!(pr.peer_ref(), &addr(80));
  assert_eq!(pr.states_slice().len(), 1);
  assert!(pr.user_data_ref().is_empty());
  assert!(!pr.kind().is_join());
  let (peer, states, ud, kind) = pr.into_parts();
  assert_eq!(peer, addr(80));
  assert_eq!(states.len(), 1);
  assert!(ud.is_empty());
  assert!(!kind.is_join());
}

#[test]
fn reliable_ping_acked_and_failed_expose_seq() {
  use super::{ReliablePingAcked, ReliablePingFailed};
  let now = crate::Instant::now();
  let acked = ReliablePingAcked::new(101, now);
  assert_eq!(acked.seq(), 101);
  assert_eq!(acked.at(), now);

  let failed = ReliablePingFailed::new(102);
  assert_eq!(failed.seq(), 102);
}

#[test]
fn stream_closed_and_errored_expose_id_and_reason() {
  use super::{StreamClosed, StreamErrored, StreamId};
  let closed = StreamClosed::new(StreamId::from_raw(5));
  assert_eq!(closed.id(), StreamId::from_raw(5));

  let errored = StreamErrored::new(StreamId::from_raw(6), String::from("reset"));
  assert_eq!(errored.id(), StreamId::from_raw(6));
  assert_eq!(errored.err(), "reset");
}

#[test]
fn user_data_received_exposes_fields_and_parts() {
  use super::UserDataReceived;
  let ud = UserDataReceived::new(addr(90), Bytes::from_static(b"payload"));
  assert_eq!(ud.peer_ref(), &addr(90));
  assert_eq!(ud.data_ref().as_ref(), b"payload");
  let (peer, data) = ud.into_parts();
  assert_eq!(peer, addr(90));
  assert_eq!(data.as_ref(), b"payload");
}

#[test]
fn send_push_pull_response_exposes_fields_and_parts() {
  use super::SendPushPullResponse;
  let resp = SendPushPullResponse::new(
    std::vec![push_state(95), push_state(96)],
    Bytes::from_static(b"local-ud"),
  );
  assert_eq!(resp.local_states_slice().len(), 2);
  assert_eq!(resp.user_data_ref().as_ref(), b"local-ud");
  let (states, ud) = resp.into_parts();
  assert_eq!(states.len(), 2);
  assert_eq!(ud.as_ref(), b"local-ud");
}

#[test]
fn stream_event_variants_are_distinct() {
  use super::StreamEvent;
  let closed = StreamEvent::Closed;
  let failed = StreamEvent::Failed(String::from("boom"));
  assert!(matches!(closed, StreamEvent::Closed));
  match failed {
    StreamEvent::Failed(reason) => assert_eq!(reason, "boom"),
    StreamEvent::Closed => panic!("expected Failed"),
  }
}

#[test]
fn stream_command_close_and_response_variants() {
  use super::{SendPushPullResponse, StreamCommand};
  let close: StreamCommand<SmolStr, SocketAddr> = StreamCommand::Close;
  assert!(matches!(close, StreamCommand::Close));
  let respond: StreamCommand<SmolStr, SocketAddr> = StreamCommand::SendPushPullResponse(
    SendPushPullResponse::new(std::vec![push_state(97)], Bytes::new()),
  );
  assert!(matches!(respond, StreamCommand::SendPushPullResponse(_)));
}
