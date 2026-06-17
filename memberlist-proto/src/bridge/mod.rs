//! Conversion between generic typed shapes (`crate::typed`) and the
//! buffa-generated concrete codec (`crate::messages`).
//!
//! The typed side is generic over `I: Data, A: Data`; the buffa side
//! stores `id`/`addr` as opaque `Bytes`. These functions are the single
//! boundary where `I`/`A` are (de)serialised via the `Data` trait. The
//! machine wire path calls `message_to_any` / `message_from_any`.
use std::{borrow::Cow, string::ToString, vec::Vec};

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
#[derive(Debug, Clone, thiserror::Error)]
pub enum BridgeError {
  /// An encode error occurred while serialising an `I` or `A` field.
  #[error(transparent)]
  Encode(#[from] EncodeError),
  /// A decode error occurred while deserialising an `I` or `A` field.
  #[error(transparent)]
  Decode(#[from] DecodeError),
  /// A required `MessageField` was unset when it must be present. The payload
  /// names the wire field (e.g. `"Alive.node"`).
  #[error("missing required field: {0}")]
  MissingField(Cow<'static, str>),
}

// ─── I / A ↔ Bytes helpers ───────────────────────────────────────────────────

/// Encode a `Data` value to raw bytes (no length prefix).
///
/// Built from the always-available `encoded_len`/`encode` primitives: the
/// `encode_to_vec` convenience method is gated behind the data module's
/// `std`/`alloc` features, so a generic `T: Data` cannot depend on it across
/// every feature combination.
fn data_to_bytes<T: Data>(val: &T) -> Result<Bytes, BridgeError> {
  let mut buf = vec![0u8; val.encoded_len()];
  val.encode(&mut buf)?;
  Ok(Bytes::from(buf))
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
  f.as_ref().ok_or(BridgeError::MissingField(field.into()))
}

/// Copy a decoded wire `bytes` field into an owned buffer, detaching it from the
/// inbound frame.
///
/// The codec decodes zero-copy, so a wire `bytes` field aliases the whole inbound
/// datagram allocation. A field that crosses into long-lived or machine-retained
/// state — membership `meta`, or an ack payload routed through the ack registry,
/// a `PingCompleted` event, or a forwarded ack — must be detached: otherwise a
/// tiny retained field pins the entire datagram, and protobuf unknown-field
/// padding lets a remote inflate that datagram into a network-reachable memory
/// amplification. Copying a small field is cheap and defeats the attack by
/// construction (a padded frame carries a small real field). Application-delivered
/// fields use [`app_field`] instead, which keeps large payloads zero-copy.
fn detach_field(field: &Bytes) -> Bytes {
  Bytes::copy_from_slice(field)
}

/// Build the typed [`Meta`](typed::Meta) from a wire `bytes` field, detaching it
/// from the inbound frame (see [`detach_field`]).
fn meta_from_wire(meta: &Bytes) -> Result<typed::Meta, DecodeError> {
  typed::Meta::try_from(detach_field(meta)).map_err(|e| DecodeError::custom(e.to_string()))
}

/// Size threshold for [`app_field`]: an application-delivered byte field below
/// this is copied, at or above it is kept zero-copy. Small enough that copying
/// is cheap.
const ZEROCOPY_MIN_LEN: usize = 1024;

/// Handle an application-delivered byte field (`UserData`, PushPull `user_data`):
/// keep a large one zero-copy — it aliases the inbound frame and is handed to
/// the app via a bounded event — but detach a small one.
///
/// The app is expected to copy a delivered payload it retains, but it does so
/// only AFTER receiving the event; while the event sits in the (capacity-)
/// bounded queue, a small logical payload aliasing a remote-padded frame pins the
/// whole frame allocation. Copying small payloads is cheap and bounds that
/// retention to `ZEROCOPY_MIN_LEN`, while large payloads — where the copy is
/// worth avoiding and the frame is large because of real data — stay zero-copy.
fn app_field(field: &Bytes) -> Bytes {
  if field.len() < ZEROCOPY_MIN_LEN {
    detach_field(field)
  } else {
    field.clone()
  }
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
  // (frozen rejects a PushNodeState missing it); enforce presence. The
  // value itself is discarded — only the `ok_or(...).? ` early-return
  // matters for missing-field validation.
  let _ = b.incarnation.ok_or(BridgeError::MissingField(
    "PushNodeState.incarnation".into(),
  ))?;
  let state = state_from_buffa(
    b.state
      .as_ref()
      .ok_or(BridgeError::MissingField("PushNodeState.state".into()))?,
  )?;
  let meta = meta_from_wire(&b.meta)?;
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
  let incarnation = b.incarnation.ok_or(BridgeError::MissingField(
    "PushNodeState.incarnation".into(),
  ))?;
  let state = state_from_buffa(
    b.state
      .as_ref()
      .ok_or(BridgeError::MissingField("PushNodeState.state".into()))?,
  )?;
  let meta = meta_from_wire(&b.meta)?;
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
    .ok_or(BridgeError::MissingField("Alive.node".into()))?;
  let node = node_from_buffa(pb_node)?;
  let meta = meta_from_wire(&b.meta)?;
  let protocol_version: typed::ProtocolVersion = versioned(b.protocol_version, "protocol_version")?;
  let delegate_version: typed::DelegateVersion = versioned(b.delegate_version, "delegate_version")?;
  let incarnation = b
    .incarnation
    .ok_or(BridgeError::MissingField("Alive.incarnation".into()))?;
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
    .ok_or(BridgeError::MissingField("Suspect.incarnation".into()))?;
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
    .ok_or(BridgeError::MissingField("Dead.incarnation".into()))?;
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
    .ok_or(BridgeError::MissingField("Ping.source".into()))?;
  let target_pb = b
    .target
    .as_option()
    .ok_or(BridgeError::MissingField("Ping.target".into()))?;
  let sequence_number = b
    .sequence_number
    .ok_or(BridgeError::MissingField("Ping.sequence_number".into()))?;
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
    .ok_or(BridgeError::MissingField("IndirectPing.source".into()))?;
  let target_pb = b
    .target
    .as_option()
    .ok_or(BridgeError::MissingField("IndirectPing.target".into()))?;
  let sequence_number = b.sequence_number.ok_or(BridgeError::MissingField(
    "IndirectPing.sequence_number".into(),
  ))?;
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
  // Detach the payload: handle_ack routes it through the ack registry into a
  // PingCompleted event or a forwarded ack, so the machine retains it past the
  // decode. See [`detach_field`].
  typed::Ack::new(b.sequence_number).with_payload(detach_field(&b.payload))
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
    typed::PushPull::new(b.join, states_iter.into_iter()).with_user_data(app_field(&b.user_data));
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
  app_field(&b.data)
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
mod tests;
