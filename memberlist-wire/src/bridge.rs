//! Conversion between generic typed shapes (`crate::typed`) and the
//! buffa-generated concrete codec (`crate::messages`).
//!
//! The typed side is generic over `I: Data, A: Data`; the buffa side
//! stores `id`/`addr` as opaque `Bytes`. These functions are the single
//! boundary where `I`/`A` are (de)serialised via the `Data` trait. The
//! machine wire path calls `message_to_any` / `message_from_any`.

use buffa::MessageField as BuffaMessageField;
use bytes::Bytes;

use crate::{
  data::{Data, DataRef, DecodeError, EncodeError},
  framing::AnyMessage,
  messages::memberlist::v1 as pb,
  typed,
};

// ─── BridgeError ─────────────────────────────────────────────────────────────

/// Errors that can occur when converting between typed shapes and buffa types.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
  /// An encode error occurred while serialising an `I` or `A` field.
  #[error("encode error: {0}")]
  Encode(#[from] EncodeError),
  /// A decode error occurred while deserialising an `I` or `A` field.
  #[error("decode error: {0}")]
  Decode(#[from] DecodeError),
  /// A required `MessageField` was unset when it must be present.
  #[error("missing required field: {0}")]
  MissingField(&'static str),
}

// ─── I / A ↔ Bytes helpers ───────────────────────────────────────────────────

/// Encode a `Data` value to raw bytes (no length prefix).
fn data_to_bytes<T: Data>(val: &T) -> Result<Bytes, BridgeError> {
  Ok(Bytes::from(val.encode_to_vec()?))
}

/// Decode a `Data` value from raw bytes (no length prefix; the whole slice
/// must be consumed). A fixed-width decoder (e.g. `SocketAddr`) returns
/// after its expected prefix, so a `debug_assert!` would let trailing
/// garbage through in release builds — this is a runtime check so a
/// malformed wire field is rejected at the wire→machine boundary in every
/// build.
fn data_from_bytes<T: Data>(buf: &Bytes) -> Result<T, BridgeError> {
  let (bytes_read, val) = <T::Ref<'_> as DataRef<'_, T>>::decode(buf.as_ref())?;
  if bytes_read != buf.len() {
    return Err(BridgeError::Decode(DecodeError::custom(format!(
      "trailing data in encoded field: decoder consumed {bytes_read} of {} bytes",
      buf.len()
    ))));
  }
  Ok(T::from_ref(val)?)
}

/// Unwrap a proto3-`optional` required `bytes` field, rejecting an omitted
/// one with `MissingField` — faithful to the frozen `memberlist-proto`
/// decoder which presence-tracks and rejects missing required fields.
fn require_bytes<'a>(f: &'a Option<Bytes>, field: &'static str) -> Result<&'a Bytes, BridgeError> {
  f.as_ref().ok_or(BridgeError::MissingField(field))
}

// ─── State ───────────────────────────────────────────────────────────────────

/// Convert [`typed::State`] → `buffa::EnumValue<pb::State>`.
///
/// `typed::State` and `pb::State` share discriminants (Alive=0, Suspect=1,
/// Dead=2, Left=3); `State::Unknown(n)` is a forward-compat byte and is
/// preserved as an `EnumValue::Unknown(n)` raw discriminant rather than
/// being rewritten to ALIVE (lossless round-trip).
fn state_to_buffa(s: typed::State) -> buffa::EnumValue<pb::State> {
  buffa::EnumValue::from(i32::from(u8::from(s)))
}

/// Convert `buffa::EnumValue<pb::State>` → [`typed::State`].
///
/// A memberlist state is a single wire byte; an `EnumValue` raw value
/// outside `0..=255` is malformed and rejected, while an in-range unknown
/// discriminant is preserved as `State::Unknown(n)`.
fn state_from_buffa(ev: &buffa::EnumValue<pb::State>) -> Result<typed::State, BridgeError> {
  let raw = ev.to_i32();
  let byte = u8::try_from(raw).map_err(|_| {
    BridgeError::Decode(DecodeError::custom(format!(
      "State discriminant {raw} out of single-byte range"
    )))
  })?;
  Ok(typed::State::from(byte))
}

/// Range-check a protobuf `uint32` version field down to the single wire
/// byte the typed layer uses. memberlist protocol/delegate versions are
/// single bytes; a value above 255 is malformed and must be rejected
/// rather than silently truncated (`257 as u8 == 1`).
fn checked_version_byte(v: u32, field: &'static str) -> Result<u8, BridgeError> {
  u8::try_from(v).map_err(|_| {
    BridgeError::Decode(DecodeError::custom(format!(
      "{field} {v} exceeds the single-byte protocol version range"
    )))
  })
}

/// Decode an `optional` protocol/delegate version field. An OMITTED field
/// decodes as the type default (V1) — faithful to frozen
/// `memberlist-proto` which uses `unwrap_or_default()` (alive.rs:384-385,
/// push_pull/state.rs:305-306) with version enums defaulting to V1. An
/// explicitly-present value (including 0) goes through the range-checked
/// byte path so it round-trips (e.g. 0 → Unknown(0)), keeping
/// omitted-vs-explicit-zero distinct.
fn versioned<T: From<u8> + Default>(v: Option<u32>, field: &'static str) -> Result<T, BridgeError> {
  match v {
    None => Ok(T::default()),
    Some(v) => Ok(T::from(checked_version_byte(v, field)?)),
  }
}

// ─── Node ────────────────────────────────────────────────────────────────────

/// Convert `typed::Node<I,A>` → `pb::Node`.
pub fn node_to_buffa<I: Data, A: Data>(n: &typed::Node<I, A>) -> Result<pb::Node, BridgeError> {
  Ok(pb::Node {
    id: Some(data_to_bytes(n.id_ref())?),
    addr: Some(data_to_bytes(n.addr_ref())?),
    ..Default::default()
  })
}

/// Convert `pb::Node` → `typed::Node<I,A>`.
pub fn node_from_buffa<I: Data, A: Data>(b: &pb::Node) -> Result<typed::Node<I, A>, BridgeError> {
  let id: I = data_from_bytes(require_bytes(&b.id, "Node.id")?)?;
  let addr: A = data_from_bytes(require_bytes(&b.addr, "Node.addr")?)?;
  Ok(typed::Node::new(id, addr))
}

// ─── NodeState ───────────────────────────────────────────────────────────────

/// Convert `typed::NodeState<I,A>` → `pb::PushNodeState`
/// (`NodeState` maps to the proto `PushNodeState` shape).
pub fn node_state_to_buffa<I: Data, A: Data>(
  ns: &typed::NodeState<I, A>,
) -> Result<pb::PushNodeState, BridgeError> {
  Ok(pb::PushNodeState {
    id: Some(data_to_bytes(ns.id_ref())?),
    addr: Some(data_to_bytes(ns.address_ref())?),
    meta: Bytes::copy_from_slice(ns.meta_ref().as_bytes()),
    incarnation: Some(0),
    state: Some(state_to_buffa(ns.state())),
    protocol_version: Some(u8::from(ns.protocol_version()) as u32),
    delegate_version: Some(u8::from(ns.delegate_version()) as u32),
    ..Default::default()
  })
}

/// Convert `pb::PushNodeState` → `typed::NodeState<I,A>`.
pub fn node_state_from_buffa<I: Data, A: Data>(
  b: &pb::PushNodeState,
) -> Result<typed::NodeState<I, A>, BridgeError> {
  let id: I = data_from_bytes(require_bytes(&b.id, "PushNodeState.id")?)?;
  let addr: A = data_from_bytes(require_bytes(&b.addr, "PushNodeState.addr")?)?;
  // NodeState carries no incarnation, but the wire field is required
  // (frozen rejects a PushNodeState missing it); enforce presence.
  let _ = b
    .incarnation
    .ok_or(BridgeError::MissingField("PushNodeState.incarnation"))?;
  let state = state_from_buffa(
    b.state
      .as_ref()
      .ok_or(BridgeError::MissingField("PushNodeState.state"))?,
  )?;
  let meta =
    typed::Meta::try_from(b.meta.clone()).map_err(|e| DecodeError::custom(e.to_string()))?;
  let protocol_version: typed::ProtocolVersion = versioned(b.protocol_version, "protocol_version")?;
  let delegate_version: typed::DelegateVersion = versioned(b.delegate_version, "delegate_version")?;
  let ns = typed::NodeState::new(id, addr, state)
    .with_meta(meta)
    .with_protocol_version(protocol_version)
    .with_delegate_version(delegate_version);
  Ok(ns)
}

// ─── PushNodeState ───────────────────────────────────────────────────────────

/// Convert `typed::PushNodeState<I,A>` → `pb::PushNodeState`.
pub fn push_node_state_to_buffa<I: Data, A: Data>(
  pns: &typed::PushNodeState<I, A>,
) -> Result<pb::PushNodeState, BridgeError> {
  Ok(pb::PushNodeState {
    id: Some(data_to_bytes(pns.id_ref())?),
    addr: Some(data_to_bytes(pns.address_ref())?),
    meta: Bytes::copy_from_slice(pns.meta_ref().as_bytes()),
    incarnation: Some(pns.incarnation()),
    state: Some(state_to_buffa(pns.state())),
    protocol_version: Some(u8::from(pns.protocol_version()) as u32),
    delegate_version: Some(u8::from(pns.delegate_version()) as u32),
    ..Default::default()
  })
}

/// Convert `pb::PushNodeState` → `typed::PushNodeState<I,A>`.
pub fn push_node_state_from_buffa<I: Data, A: Data>(
  b: &pb::PushNodeState,
) -> Result<typed::PushNodeState<I, A>, BridgeError> {
  let id: I = data_from_bytes(require_bytes(&b.id, "PushNodeState.id")?)?;
  let addr: A = data_from_bytes(require_bytes(&b.addr, "PushNodeState.addr")?)?;
  let incarnation = b
    .incarnation
    .ok_or(BridgeError::MissingField("PushNodeState.incarnation"))?;
  let state = state_from_buffa(
    b.state
      .as_ref()
      .ok_or(BridgeError::MissingField("PushNodeState.state"))?,
  )?;
  let meta =
    typed::Meta::try_from(b.meta.clone()).map_err(|e| DecodeError::custom(e.to_string()))?;
  let protocol_version: typed::ProtocolVersion = versioned(b.protocol_version, "protocol_version")?;
  let delegate_version: typed::DelegateVersion = versioned(b.delegate_version, "delegate_version")?;
  let pns = typed::PushNodeState::new(incarnation, id, addr, state)
    .with_meta(meta)
    .with_protocol_version(protocol_version)
    .with_delegate_version(delegate_version);
  Ok(pns)
}

// ─── Alive ───────────────────────────────────────────────────────────────────

/// Convert `typed::Alive<I,A>` → `pb::Alive`.
pub fn alive_to_buffa<I: Data, A: Data>(t: &typed::Alive<I, A>) -> Result<pb::Alive, BridgeError> {
  let pb_node = node_to_buffa(t.node_ref())?;
  Ok(pb::Alive {
    incarnation: Some(t.incarnation()),
    meta: Bytes::copy_from_slice(t.meta_ref().as_bytes()),
    node: BuffaMessageField::some(pb_node),
    protocol_version: Some(u8::from(t.protocol_version()) as u32),
    delegate_version: Some(u8::from(t.delegate_version()) as u32),
    ..Default::default()
  })
}

/// Convert `pb::Alive` → `typed::Alive<I,A>`.
pub fn alive_from_buffa<I: Data, A: Data>(
  b: &pb::Alive,
) -> Result<typed::Alive<I, A>, BridgeError> {
  let pb_node = b
    .node
    .as_option()
    .ok_or(BridgeError::MissingField("Alive.node"))?;
  let node = node_from_buffa(pb_node)?;
  let meta =
    typed::Meta::try_from(b.meta.clone()).map_err(|e| DecodeError::custom(e.to_string()))?;
  let protocol_version: typed::ProtocolVersion = versioned(b.protocol_version, "protocol_version")?;
  let delegate_version: typed::DelegateVersion = versioned(b.delegate_version, "delegate_version")?;
  let incarnation = b
    .incarnation
    .ok_or(BridgeError::MissingField("Alive.incarnation"))?;
  let alive = typed::Alive::new(incarnation, node)
    .with_meta(meta)
    .with_protocol_version(protocol_version)
    .with_delegate_version(delegate_version);
  Ok(alive)
}

// ─── Suspect ─────────────────────────────────────────────────────────────────

/// Convert `typed::Suspect<I>` → `pb::Suspect`.
pub fn suspect_to_buffa<I: Data>(t: &typed::Suspect<I>) -> Result<pb::Suspect, BridgeError> {
  Ok(pb::Suspect {
    incarnation: Some(t.incarnation()),
    node: Some(data_to_bytes(t.node_ref())?),
    from: Some(data_to_bytes(t.from_ref())?),
    ..Default::default()
  })
}

/// Convert `pb::Suspect` → `typed::Suspect<I>`.
pub fn suspect_from_buffa<I: Data>(b: &pb::Suspect) -> Result<typed::Suspect<I>, BridgeError> {
  let node: I = data_from_bytes(require_bytes(&b.node, "Suspect.node")?)?;
  let from: I = data_from_bytes(require_bytes(&b.from, "Suspect.from")?)?;
  let incarnation = b
    .incarnation
    .ok_or(BridgeError::MissingField("Suspect.incarnation"))?;
  Ok(typed::Suspect::new(incarnation, node, from))
}

// ─── Dead ────────────────────────────────────────────────────────────────────

/// Convert `typed::Dead<I>` → `pb::Dead`.
pub fn dead_to_buffa<I: Data>(t: &typed::Dead<I>) -> Result<pb::Dead, BridgeError> {
  Ok(pb::Dead {
    incarnation: Some(t.incarnation()),
    node: Some(data_to_bytes(t.node_ref())?),
    from: Some(data_to_bytes(t.from_ref())?),
    ..Default::default()
  })
}

/// Convert `pb::Dead` → `typed::Dead<I>`.
pub fn dead_from_buffa<I: Data>(b: &pb::Dead) -> Result<typed::Dead<I>, BridgeError> {
  let node: I = data_from_bytes(require_bytes(&b.node, "Dead.node")?)?;
  let from: I = data_from_bytes(require_bytes(&b.from, "Dead.from")?)?;
  let incarnation = b
    .incarnation
    .ok_or(BridgeError::MissingField("Dead.incarnation"))?;
  Ok(typed::Dead::new(incarnation, node, from))
}

// ─── Ping ────────────────────────────────────────────────────────────────────

/// Convert `typed::Ping<I,A>` → `pb::Ping`.
pub fn ping_to_buffa<I: Data, A: Data>(t: &typed::Ping<I, A>) -> Result<pb::Ping, BridgeError> {
  Ok(pb::Ping {
    sequence_number: Some(t.sequence_number()),
    source: BuffaMessageField::some(node_to_buffa(t.source_ref())?),
    target: BuffaMessageField::some(node_to_buffa(t.target_ref())?),
    ..Default::default()
  })
}

/// Convert `pb::Ping` → `typed::Ping<I,A>`.
pub fn ping_from_buffa<I: Data, A: Data>(b: &pb::Ping) -> Result<typed::Ping<I, A>, BridgeError> {
  let source_pb = b
    .source
    .as_option()
    .ok_or(BridgeError::MissingField("Ping.source"))?;
  let target_pb = b
    .target
    .as_option()
    .ok_or(BridgeError::MissingField("Ping.target"))?;
  let sequence_number = b
    .sequence_number
    .ok_or(BridgeError::MissingField("Ping.sequence_number"))?;
  Ok(typed::Ping::new(
    sequence_number,
    node_from_buffa(source_pb)?,
    node_from_buffa(target_pb)?,
  ))
}

// ─── IndirectPing ─────────────────────────────────────────────────────────────

/// Convert `typed::IndirectPing<I,A>` → `pb::IndirectPing`.
pub fn indirect_ping_to_buffa<I: Data, A: Data>(
  t: &typed::IndirectPing<I, A>,
) -> Result<pb::IndirectPing, BridgeError> {
  Ok(pb::IndirectPing {
    sequence_number: Some(t.sequence_number()),
    source: BuffaMessageField::some(node_to_buffa(t.source_ref())?),
    target: BuffaMessageField::some(node_to_buffa(t.target_ref())?),
    ..Default::default()
  })
}

/// Convert `pb::IndirectPing` → `typed::IndirectPing<I,A>`.
pub fn indirect_ping_from_buffa<I: Data, A: Data>(
  b: &pb::IndirectPing,
) -> Result<typed::IndirectPing<I, A>, BridgeError> {
  let source_pb = b
    .source
    .as_option()
    .ok_or(BridgeError::MissingField("IndirectPing.source"))?;
  let target_pb = b
    .target
    .as_option()
    .ok_or(BridgeError::MissingField("IndirectPing.target"))?;
  let sequence_number = b
    .sequence_number
    .ok_or(BridgeError::MissingField("IndirectPing.sequence_number"))?;
  Ok(typed::IndirectPing::new(
    sequence_number,
    node_from_buffa(source_pb)?,
    node_from_buffa(target_pb)?,
  ))
}

// ─── Ack ─────────────────────────────────────────────────────────────────────

/// Convert `typed::Ack` → `pb::Ack`.
pub fn ack_to_buffa(t: &typed::Ack) -> pb::Ack {
  pb::Ack {
    sequence_number: t.sequence_number(),
    payload: t.payload_bytes(),
    ..Default::default()
  }
}

/// Convert `pb::Ack` → `typed::Ack`.
pub fn ack_from_buffa(b: &pb::Ack) -> typed::Ack {
  typed::Ack::new(b.sequence_number).with_payload(b.payload.clone())
}

// ─── Nack ────────────────────────────────────────────────────────────────────

/// Convert `typed::Nack` → `pb::Nack`.
pub fn nack_to_buffa(t: &typed::Nack) -> pb::Nack {
  pb::Nack {
    sequence_number: t.sequence_number(),
    ..Default::default()
  }
}

/// Convert `pb::Nack` → `typed::Nack`.
pub fn nack_from_buffa(b: &pb::Nack) -> typed::Nack {
  typed::Nack::new(b.sequence_number)
}

// ─── PushPull ────────────────────────────────────────────────────────────────

/// Convert `typed::PushPull<I,A>` → `pb::PushPull`.
pub fn push_pull_to_buffa<I: Data, A: Data>(
  t: &typed::PushPull<I, A>,
) -> Result<pb::PushPull, BridgeError> {
  let states = t
    .states_slice()
    .iter()
    .map(push_node_state_to_buffa)
    .collect::<Result<Vec<_>, _>>()?;
  Ok(pb::PushPull {
    join: t.join(),
    states,
    user_data: t.user_data_bytes(),
    ..Default::default()
  })
}

/// Convert `pb::PushPull` → `typed::PushPull<I,A>`.
pub fn push_pull_from_buffa<I: Data, A: Data>(
  b: &pb::PushPull,
) -> Result<typed::PushPull<I, A>, BridgeError> {
  let states_iter = b
    .states
    .iter()
    .map(push_node_state_from_buffa::<I, A>)
    .collect::<Result<Vec<_>, _>>()?;
  let pp =
    typed::PushPull::new(b.join, states_iter.into_iter()).with_user_data(b.user_data.clone());
  Ok(pp)
}

// ─── UserData ────────────────────────────────────────────────────────────────

/// Convert a `typed::Message::UserData` bytes payload → `pb::UserData`.
pub fn user_data_to_buffa(data: &Bytes) -> pb::UserData {
  pb::UserData {
    data: data.clone(),
    ..Default::default()
  }
}

/// Convert `pb::UserData` → raw `Bytes` payload (typed side stores `Bytes`).
pub fn user_data_from_buffa(b: &pb::UserData) -> Bytes {
  b.data.clone()
}

// ─── ErrorResponse ───────────────────────────────────────────────────────────

/// Convert `typed::ErrorResponse` → `pb::ErrorResponse`.
pub fn error_response_to_buffa(t: &typed::ErrorResponse) -> pb::ErrorResponse {
  pb::ErrorResponse {
    error: t.message().to_string(),
    ..Default::default()
  }
}

/// Convert `pb::ErrorResponse` → `typed::ErrorResponse`.
pub fn error_response_from_buffa(b: &pb::ErrorResponse) -> typed::ErrorResponse {
  typed::ErrorResponse::new(b.error.as_str())
}

// ─── Top-level Message ↔ AnyMessage ─────────────────────────────────────────

/// Convert a generic `typed::Message<I,A>` → `framing::AnyMessage`.
///
/// This is the entry point the machine wire path calls when serialising
/// outbound messages.
pub fn message_to_any<I: Data, A: Data>(
  m: &typed::Message<I, A>,
) -> Result<AnyMessage, BridgeError> {
  match m {
    typed::Message::Ping(v) => Ok(AnyMessage::Ping(ping_to_buffa(v)?)),
    typed::Message::IndirectPing(v) => Ok(AnyMessage::IndirectPing(indirect_ping_to_buffa(v)?)),
    typed::Message::Ack(v) => Ok(AnyMessage::Ack(ack_to_buffa(v))),
    typed::Message::Suspect(v) => Ok(AnyMessage::Suspect(suspect_to_buffa(v)?)),
    typed::Message::Alive(v) => Ok(AnyMessage::Alive(alive_to_buffa(v)?)),
    typed::Message::Dead(v) => Ok(AnyMessage::Dead(dead_to_buffa(v)?)),
    typed::Message::PushPull(v) => Ok(AnyMessage::PushPull(push_pull_to_buffa(v)?)),
    typed::Message::UserData(v) => Ok(AnyMessage::UserData(user_data_to_buffa(v))),
    typed::Message::Nack(v) => Ok(AnyMessage::Nack(nack_to_buffa(v))),
    typed::Message::ErrorResponse(v) => Ok(AnyMessage::ErrorResponse(error_response_to_buffa(v))),
  }
}

/// Convert a `framing::AnyMessage` → `typed::Message<I,A>`.
///
/// This is the entry point the machine wire path calls when deserialising
/// inbound messages.
pub fn message_from_any<I: Data, A: Data>(
  a: &AnyMessage,
) -> Result<typed::Message<I, A>, BridgeError> {
  match a {
    AnyMessage::Ping(v) => Ok(typed::Message::Ping(ping_from_buffa(v)?)),
    AnyMessage::IndirectPing(v) => Ok(typed::Message::IndirectPing(indirect_ping_from_buffa(v)?)),
    AnyMessage::Ack(v) => Ok(typed::Message::Ack(ack_from_buffa(v))),
    AnyMessage::Suspect(v) => Ok(typed::Message::Suspect(suspect_from_buffa(v)?)),
    AnyMessage::Alive(v) => Ok(typed::Message::Alive(alive_from_buffa(v)?)),
    AnyMessage::Dead(v) => Ok(typed::Message::Dead(dead_from_buffa(v)?)),
    AnyMessage::PushPull(v) => Ok(typed::Message::PushPull(push_pull_from_buffa(v)?)),
    AnyMessage::UserData(v) => Ok(typed::Message::UserData(user_data_from_buffa(v))),
    AnyMessage::Nack(v) => Ok(typed::Message::Nack(nack_from_buffa(v))),
    AnyMessage::ErrorResponse(v) => Ok(typed::Message::ErrorResponse(error_response_from_buffa(v))),
  }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::*;
  use crate::typed::{self, DelegateVersion, Node, ProtocolVersion, State};

  type I = SmolStr;
  type A = SocketAddr;

  fn sample_node() -> Node<I, A> {
    Node::new(
      SmolStr::new("node-a"),
      "127.0.0.1:7946".parse::<SocketAddr>().unwrap(),
    )
  }

  // ── Alive ──────────────────────────────────────────────────────────────────

  #[test]
  fn alive_roundtrip() {
    let alive = typed::Alive::new(42, sample_node())
      .with_protocol_version(ProtocolVersion::V1)
      .with_delegate_version(DelegateVersion::V1);

    let pb = alive_to_buffa::<I, A>(&alive).unwrap();
    assert_eq!(pb.incarnation, Some(42));
    assert!(pb.node.is_set());

    let back = alive_from_buffa::<I, A>(&pb).unwrap();
    assert_eq!(back.incarnation(), alive.incarnation());
    assert_eq!(back.node_ref().id_ref(), alive.node_ref().id_ref());
    assert_eq!(back.node_ref().addr_ref(), alive.node_ref().addr_ref());
    assert_eq!(back.protocol_version(), alive.protocol_version());
    assert_eq!(back.delegate_version(), alive.delegate_version());
  }

  // ── Suspect ────────────────────────────────────────────────────────────────

  #[test]
  fn suspect_roundtrip() {
    let suspect = typed::Suspect::new(7, SmolStr::new("node-b"), SmolStr::new("node-a"));

    let pb = suspect_to_buffa::<I>(&suspect).unwrap();
    assert_eq!(pb.incarnation, Some(7));

    let back = suspect_from_buffa::<I>(&pb).unwrap();
    assert_eq!(back.incarnation(), suspect.incarnation());
    assert_eq!(back.node_ref(), suspect.node_ref());
    assert_eq!(back.from_ref(), suspect.from_ref());
  }

  // ── Dead ───────────────────────────────────────────────────────────────────

  #[test]
  fn dead_roundtrip() {
    let dead = typed::Dead::new(3, SmolStr::new("node-c"), SmolStr::new("node-a"));

    let pb = dead_to_buffa::<I>(&dead).unwrap();
    let back = dead_from_buffa::<I>(&pb).unwrap();
    assert_eq!(back.incarnation(), dead.incarnation());
    assert_eq!(back.node_ref(), dead.node_ref());
    assert_eq!(back.from_ref(), dead.from_ref());
  }

  // ── Ping ───────────────────────────────────────────────────────────────────

  #[test]
  fn ping_roundtrip() {
    let src = sample_node();
    let tgt = Node::new(
      SmolStr::new("node-b"),
      "127.0.0.2:7946".parse::<SocketAddr>().unwrap(),
    );
    let ping = typed::Ping::new(99, src, tgt);

    let pb = ping_to_buffa::<I, A>(&ping).unwrap();
    assert_eq!(pb.sequence_number, Some(99));

    let back = ping_from_buffa::<I, A>(&pb).unwrap();
    assert_eq!(back.sequence_number(), ping.sequence_number());
    assert_eq!(back.source_ref().id_ref(), ping.source_ref().id_ref());
    assert_eq!(back.target_ref().id_ref(), ping.target_ref().id_ref());
  }

  // ── IndirectPing ───────────────────────────────────────────────────────────

  #[test]
  fn indirect_ping_roundtrip() {
    let src = sample_node();
    let tgt = Node::new(
      SmolStr::new("node-b"),
      "127.0.0.2:7946".parse::<SocketAddr>().unwrap(),
    );
    let iping = typed::IndirectPing::new(55, src, tgt);

    let pb = indirect_ping_to_buffa::<I, A>(&iping).unwrap();
    let back = indirect_ping_from_buffa::<I, A>(&pb).unwrap();
    assert_eq!(back.sequence_number(), iping.sequence_number());
    assert_eq!(back.source_ref().id_ref(), iping.source_ref().id_ref());
    assert_eq!(back.target_ref().id_ref(), iping.target_ref().id_ref());
  }

  // ── Ack / Nack ─────────────────────────────────────────────────────────────

  #[test]
  fn ack_roundtrip() {
    let ack = typed::Ack::new(11).with_payload(Bytes::from_static(b"hello"));

    let pb = ack_to_buffa(&ack);
    assert_eq!(pb.sequence_number, 11);

    let back = ack_from_buffa(&pb);
    assert_eq!(back.sequence_number(), ack.sequence_number());
    assert_eq!(back.payload(), ack.payload());
  }

  #[test]
  fn nack_roundtrip() {
    let nack = typed::Nack::new(22);
    let pb = nack_to_buffa(&nack);
    let back = nack_from_buffa(&pb);
    assert_eq!(back.sequence_number(), nack.sequence_number());
  }

  // ── PushPull ───────────────────────────────────────────────────────────────

  #[test]
  fn push_pull_roundtrip() {
    let pns1 = typed::PushNodeState::new(
      1,
      SmolStr::new("node-a"),
      "127.0.0.1:7946".parse::<SocketAddr>().unwrap(),
      State::Alive,
    );
    let pns2 = typed::PushNodeState::new(
      2,
      SmolStr::new("node-b"),
      "127.0.0.2:7946".parse::<SocketAddr>().unwrap(),
      State::Suspect,
    );
    let pp = typed::PushPull::new(true, [pns1, pns2].into_iter())
      .with_user_data(Bytes::from_static(b"userdata"));

    let pb = push_pull_to_buffa::<I, A>(&pp).unwrap();
    assert!(pb.join);
    assert_eq!(pb.states.len(), 2);
    assert_eq!(&pb.user_data[..], b"userdata");

    let back = push_pull_from_buffa::<I, A>(&pb).unwrap();
    assert_eq!(back.join(), pp.join());
    assert_eq!(back.states_slice().len(), pp.states_slice().len());
    assert_eq!(back.user_data(), pp.user_data());
    assert_eq!(
      back.states_slice()[0].id_ref(),
      pp.states_slice()[0].id_ref()
    );
    assert_eq!(back.states_slice()[1].state(), pp.states_slice()[1].state());
  }

  // ── UserData ───────────────────────────────────────────────────────────────

  #[test]
  fn user_data_roundtrip() {
    let data = Bytes::from_static(b"app-gossip");
    let pb = user_data_to_buffa(&data);
    let back = user_data_from_buffa(&pb);
    assert_eq!(back, data);
  }

  // ── ErrorResponse ──────────────────────────────────────────────────────────

  #[test]
  fn error_response_roundtrip() {
    let er = typed::ErrorResponse::new("something went wrong");
    let pb = error_response_to_buffa(&er);
    let back = error_response_from_buffa(&pb);
    assert_eq!(back.message(), er.message());
  }

  // ── message_to_any / message_from_any: Alive ───────────────────────────────

  #[test]
  fn message_roundtrip_alive() {
    let alive = typed::Alive::new(100, sample_node());
    let msg: typed::Message<I, A> = typed::Message::Alive(alive.clone());

    let any = message_to_any(&msg).unwrap();
    assert!(matches!(any, AnyMessage::Alive(_)));

    let back: typed::Message<I, A> = message_from_any(&any).unwrap();
    match back {
      typed::Message::Alive(a) => {
        assert_eq!(a.incarnation(), alive.incarnation());
        assert_eq!(a.node_ref().id_ref(), alive.node_ref().id_ref());
      }
      _ => panic!("expected Alive variant"),
    }
  }

  // ── message_to_any / message_from_any: PushPull ────────────────────────────

  #[test]
  fn message_roundtrip_push_pull() {
    let pns = typed::PushNodeState::new(
      5,
      SmolStr::new("node-x"),
      "10.0.0.1:7946".parse::<SocketAddr>().unwrap(),
      State::Dead,
    );
    let pp =
      typed::PushPull::new(false, [pns].into_iter()).with_user_data(Bytes::from_static(b"ud"));
    let msg: typed::Message<I, A> = typed::Message::PushPull(pp.clone());

    let any = message_to_any(&msg).unwrap();
    assert!(matches!(any, AnyMessage::PushPull(_)));

    let back: typed::Message<I, A> = message_from_any(&any).unwrap();
    match back {
      typed::Message::PushPull(p) => {
        assert_eq!(p.join(), pp.join());
        assert_eq!(p.states_slice().len(), pp.states_slice().len());
        assert_eq!(p.user_data(), pp.user_data());
      }
      _ => panic!("expected PushPull variant"),
    }
  }

  // ── alive_from_buffa: missing node → MissingField error ──────────────────

  #[test]
  fn alive_missing_node_returns_error() {
    use crate::messages::memberlist::v1 as pb;
    let pb_alive = pb::Alive {
      incarnation: Some(1),
      node: buffa::MessageField::none(),
      ..Default::default()
    };
    let err = alive_from_buffa::<I, A>(&pb_alive).unwrap_err();
    assert!(
      matches!(err, BridgeError::MissingField("Alive.node")),
      "got: {err:?}"
    );
  }

  // ── State mapping ─────────────────────────────────────────────────────────

  #[test]
  fn state_roundtrip_all_variants_including_unknown() {
    // Unknown(n) is a forward-compat byte and MUST round-trip, not
    // collapse to Alive.
    let variants = [
      State::Alive,
      State::Suspect,
      State::Dead,
      State::Left,
      State::Unknown(4),
      State::Unknown(99),
      State::Unknown(255),
    ];
    for &s in &variants {
      let pb_state = state_to_buffa(s);
      let back = state_from_buffa(&pb_state).unwrap();
      assert_eq!(back, s, "roundtrip failed for {s:?}");
    }
  }

  #[test]
  fn state_from_buffa_rejects_out_of_byte_range_discriminant() {
    // A wire State is a single byte; a raw EnumValue outside 0..=255 is
    // malformed and must be rejected, not truncated into a valid state.
    for raw in [256i32, 300, -1, i32::MAX] {
      let ev = buffa::EnumValue::<crate::messages::memberlist::v1::State>::from(raw);
      assert!(
        matches!(state_from_buffa(&ev), Err(BridgeError::Decode(_))),
        "raw discriminant {raw} must be rejected"
      );
    }
  }

  #[test]
  fn version_fields_round_trip_unknown_and_reject_overflow() {
    // Protocol/delegate versions are single wire bytes. Unknown(n) must
    // round-trip; an inbound uint32 > 255 must be REJECTED, not truncated
    // (`257 as u8 == 1` would forge V1).
    let ns = typed::NodeState::new(
      sample_node().id_ref().clone(),
      *sample_node().addr_ref(),
      State::Alive,
    )
    .with_protocol_version(ProtocolVersion::Unknown(7))
    .with_delegate_version(DelegateVersion::Unknown(200));
    let pb = node_state_to_buffa::<I, A>(&ns).unwrap();
    let back = node_state_from_buffa::<I, A>(&pb).unwrap();
    assert_eq!(back.protocol_version(), ProtocolVersion::Unknown(7));
    assert_eq!(back.delegate_version(), DelegateVersion::Unknown(200));

    let mut bad = pb.clone();
    bad.protocol_version = Some(257);
    assert!(
      matches!(
        node_state_from_buffa::<I, A>(&bad),
        Err(BridgeError::Decode(_))
      ),
      "protocol_version 257 must be rejected, not truncated to 1"
    );
  }

  #[test]
  fn p2_omitted_version_defaults_v1_distinct_from_explicit_zero() {
    use crate::messages::memberlist::v1 as pb;

    let good_node = || pb::Node {
      id: Some(Bytes::from_static(b"n")),
      addr: Some(data_to_bytes(&"127.0.0.1:1".parse::<SocketAddr>().unwrap()).unwrap()),
      ..Default::default()
    };

    // Omitted version fields ⇒ default V1 (frozen `unwrap_or_default`),
    // NOT Unknown(0).
    let omitted = pb::Alive {
      incarnation: Some(1),
      node: buffa::MessageField::some(good_node()),
      protocol_version: None,
      delegate_version: None,
      ..Default::default()
    };
    let a = alive_from_buffa::<I, A>(&omitted).unwrap();
    assert_eq!(a.protocol_version(), ProtocolVersion::V1);
    assert_eq!(a.delegate_version(), DelegateVersion::V1);

    // Explicit 0 ⇒ Unknown(0) — must remain distinct from omitted.
    let explicit_zero = pb::Alive {
      incarnation: Some(1),
      node: buffa::MessageField::some(good_node()),
      protocol_version: Some(0),
      delegate_version: Some(0),
      ..Default::default()
    };
    let z = alive_from_buffa::<I, A>(&explicit_zero).unwrap();
    assert_eq!(z.protocol_version(), ProtocolVersion::Unknown(0));
    assert_eq!(z.delegate_version(), DelegateVersion::Unknown(0));

    // Explicit 1 ⇒ V1; explicit >255 ⇒ rejected.
    let one = pb::Alive {
      incarnation: Some(1),
      node: buffa::MessageField::some(good_node()),
      protocol_version: Some(1),
      delegate_version: Some(1),
      ..Default::default()
    };
    let o = alive_from_buffa::<I, A>(&one).unwrap();
    assert_eq!(o.protocol_version(), ProtocolVersion::V1);

    let over = pb::Alive {
      incarnation: Some(1),
      node: buffa::MessageField::some(good_node()),
      protocol_version: Some(300),
      ..Default::default()
    };
    assert!(matches!(
      alive_from_buffa::<I, A>(&over),
      Err(BridgeError::Decode(_))
    ));
  }

  #[test]
  fn data_from_bytes_rejects_trailing_garbage() {
    // A fixed-width field (SocketAddr) with trailing bytes must be
    // rejected at the bridge in release builds, not silently
    // canonicalized.
    let addr: A = "127.0.0.1:7946".parse().unwrap();
    let mut raw = data_to_bytes(&addr).unwrap().to_vec();
    raw.push(0xff); // trailing garbage
    let buf = Bytes::from(raw);
    assert!(
      matches!(data_from_bytes::<A>(&buf), Err(BridgeError::Decode(_))),
      "trailing bytes after a valid SocketAddr must be rejected"
    );
    // The clean encoding still decodes.
    let clean = data_to_bytes(&addr).unwrap();
    assert_eq!(data_from_bytes::<A>(&clean).unwrap(), addr);
  }

  #[test]
  fn scoped_ipv6_address_is_rejected_not_silently_canonicalized() {
    // The compact wire layout cannot carry flowinfo/scope_id. A scoped
    // link-local address (scope_id matters for dialing) must be REJECTED
    // on encode rather than silently flattened to scope 0 (which would
    // gossip an undialable peer). Tested through the real node address
    // path.
    use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

    let scoped = SocketAddr::V6(SocketAddrV6::new(
      Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
      7946,
      0x1234, // flowinfo
      7,      // scope_id (interface zone)
    ));
    let node: Node<I, A> = Node::new(SmolStr::new("ll"), scoped);
    assert!(
      node_to_buffa::<I, A>(&node).is_err(),
      "a scoped IPv6 node address must be rejected, not silently zeroed"
    );

    // An unscoped IPv6 address still round-trips cleanly.
    let unscoped = SocketAddr::V6(SocketAddrV6::new(
      Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
      7946,
      0,
      0,
    ));
    let n2: Node<I, A> = Node::new(SmolStr::new("g"), unscoped);
    let pb = node_to_buffa::<I, A>(&n2).expect("unscoped IPv6 must encode");
    let back = node_from_buffa::<I, A>(&pb).expect("unscoped IPv6 must decode");
    assert_eq!(*back.addr_ref(), unscoped);
  }

  // ── Omitted legacy-required fields must be rejected ──────────────────────

  #[test]
  fn p1_missing_required_fields_are_rejected() {
    use crate::messages::memberlist::v1 as pb;

    // A valid Node to satisfy message-typed presence where needed.
    let good_node = || pb::Node {
      id: Some(Bytes::from_static(b"n")),
      addr: Some(data_to_bytes(&"127.0.0.1:1".parse::<SocketAddr>().unwrap()).unwrap()),
      ..Default::default()
    };

    // Alive: incarnation omitted (proto3 would default it to 0).
    let a = pb::Alive {
      incarnation: None,
      node: buffa::MessageField::some(good_node()),
      ..Default::default()
    };
    assert!(
      matches!(
        alive_from_buffa::<I, A>(&a),
        Err(BridgeError::MissingField("Alive.incarnation"))
      ),
      "omitted Alive.incarnation must be rejected"
    );

    // Suspect: node bytes omitted.
    let s = pb::Suspect {
      incarnation: Some(1),
      node: None,
      from: Some(Bytes::from_static(b"x")),
      ..Default::default()
    };
    assert!(matches!(
      suspect_from_buffa::<I>(&s),
      Err(BridgeError::MissingField("Suspect.node"))
    ));

    // Ping: sequence_number omitted.
    let p = pb::Ping {
      sequence_number: None,
      source: buffa::MessageField::some(good_node()),
      target: buffa::MessageField::some(good_node()),
      ..Default::default()
    };
    assert!(matches!(
      ping_from_buffa::<I, A>(&p),
      Err(BridgeError::MissingField("Ping.sequence_number"))
    ));

    // Node: id omitted.
    let n = pb::Node {
      id: None,
      addr: Some(Bytes::from_static(b"a")),
      ..Default::default()
    };
    assert!(matches!(
      node_from_buffa::<I, A>(&n),
      Err(BridgeError::MissingField("Node.id"))
    ));

    // PushNodeState: state omitted.
    let pns = pb::PushNodeState {
      id: Some(Bytes::from_static(b"n")),
      addr: Some(data_to_bytes(&"127.0.0.1:1".parse::<SocketAddr>().unwrap()).unwrap()),
      incarnation: Some(1),
      state: None,
      ..Default::default()
    };
    assert!(matches!(
      push_node_state_from_buffa::<I, A>(&pns),
      Err(BridgeError::MissingField("PushNodeState.state"))
    ));

    // Sanity: a fully-populated Alive still round-trips (present values
    // are wire-identical under proto3 `optional`).
    let ok = typed::Alive::new(5, sample_node())
      .with_protocol_version(ProtocolVersion::V1)
      .with_delegate_version(DelegateVersion::V1);
    let pb_ok = alive_to_buffa::<I, A>(&ok).unwrap();
    let back = alive_from_buffa::<I, A>(&pb_ok).unwrap();
    assert_eq!(back.incarnation(), 5);
  }
}
