//! Plain-frame encoder/decoder for `[TAG_BYTE][VARINT_LEN][BUFFA_BYTES]`.
//!
//! The state machine (`memberlist-machine::Stream::handle_data`) consumes
//! exactly this shape. Outer wrappers (label / encryption / checksum /
//! compression / compound) are driver-level concerns and are NOT covered
//! here.
//!
//! # Wire-format note
//!
//! The outer plain-frame wrapping (single tag byte + varint length) is
//! byte-compatible with the legacy `memberlist-proto` framing. The inner
//! body bytes differ because buffa emits standard protobuf wire format
//! while `memberlist-proto` uses a custom high-3-bits-wire-type scheme.
//! Mixed-version clusters are not supported — this is a clean cut-over.

use std::borrow::Cow;

use buffa::Message as _;

use crate::messages::memberlist::v1::{
  Ack, Alive, Dead, ErrorResponse, IndirectPing, Nack, Ping, PushPull, Suspect, UserData,
};

/// One-byte tag identifying which message variant follows. Matches the
/// legacy `memberlist-proto::proto::*_MESSAGE_TAG` constants.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageTag {
  /// Compound message (outer wrapper; not decoded by this layer).
  Compound = 1,
  /// Ping probe message.
  Ping = 2,
  /// Indirect ping probe message.
  IndirectPing = 3,
  /// Ack response to a ping.
  Ack = 4,
  /// Suspect — node suspected dead.
  Suspect = 5,
  /// Alive — node liveness advertisement.
  Alive = 6,
  /// Dead — node declared dead.
  Dead = 7,
  /// PushPull — full cluster state sync.
  PushPull = 8,
  /// UserData — application-level gossip payload.
  UserData = 9,
  /// Nack — indirect-ping relay timed out.
  Nack = 10,
  /// ErrorResponse — protocol error from remote.
  ErrorResponse = 11,
  /// Compressed wrapper (outer transform; nests outside a message or compound
  /// frame). Decoded by the tag-driven unwrap loop, not by `decode_message`.
  Compressed = 14,
  // Tags 12 (Labeled), 13 (Checksumed), 15 (Encrypted) are reserved transform
  // wrappers; 14 (Compressed) is live above. Transform wrappers are
  // codec-level — stripped by the tag-driven unwrap loop in this module — not
  // driver-level.
}

impl MessageTag {
  /// Returns the tag for a given message variant.
  pub fn for_message(m: &AnyMessage) -> Self {
    match m {
      AnyMessage::Alive(_) => MessageTag::Alive,
      AnyMessage::Suspect(_) => MessageTag::Suspect,
      AnyMessage::Dead(_) => MessageTag::Dead,
      AnyMessage::Ping(_) => MessageTag::Ping,
      AnyMessage::IndirectPing(_) => MessageTag::IndirectPing,
      AnyMessage::Ack(_) => MessageTag::Ack,
      AnyMessage::Nack(_) => MessageTag::Nack,
      AnyMessage::PushPull(_) => MessageTag::PushPull,
      AnyMessage::UserData(_) => MessageTag::UserData,
      AnyMessage::ErrorResponse(_) => MessageTag::ErrorResponse,
    }
  }
}

impl TryFrom<u8> for MessageTag {
  type Error = FrameError;

  fn try_from(b: u8) -> Result<Self, FrameError> {
    match b {
      1 => Ok(Self::Compound),
      2 => Ok(Self::Ping),
      3 => Ok(Self::IndirectPing),
      4 => Ok(Self::Ack),
      5 => Ok(Self::Suspect),
      6 => Ok(Self::Alive),
      7 => Ok(Self::Dead),
      8 => Ok(Self::PushPull),
      9 => Ok(Self::UserData),
      10 => Ok(Self::Nack),
      11 => Ok(Self::ErrorResponse),
      14 => Ok(Self::Compressed),
      _ => Err(FrameError::UnknownTag(b)),
    }
  }
}

/// Compound-frame tag byte length (the leading `MessageTag::Compound`).
pub const COMPOUND_TAG_LEN: usize = 1;

/// Conservative upper bound on the compound `count` prefix: a `u32` LEB128
/// varint is at most 5 bytes. The machine's gossip MTU budget reserves
/// this so an assembled compound datagram can never exceed the sub-MTU
/// ceiling regardless of how many parts are selected.
pub const COMPOUND_MAX_COUNT_PREFIX_LEN: usize = 5;

/// Conservative upper bound on a single part's `inner_len` prefix: a `u32`
/// LEB128 varint is at most 5 bytes. Used as the per-message `overhead`
/// argument to the broadcast queue's MTU-budgeted drain.
pub const COMPOUND_MAX_PART_PREFIX_LEN: usize = 5;

/// Top-level wrapper enum for the 10 inner wire messages.
///
/// buffa generates each message as a separate struct; this enum is the
/// hand-rolled discriminated union needed by the plain-frame layer.
#[derive(Debug, Clone, PartialEq)]
pub enum AnyMessage {
  /// Alive — node liveness advertisement.
  Alive(Alive),
  /// Suspect — node suspected dead.
  Suspect(Suspect),
  /// Dead — node declared dead.
  Dead(Dead),
  /// Ping probe message.
  Ping(Ping),
  /// Indirect ping probe message.
  IndirectPing(IndirectPing),
  /// Ack response to a ping.
  Ack(Ack),
  /// Nack — indirect-ping relay timed out.
  Nack(Nack),
  /// PushPull — full cluster state sync.
  PushPull(PushPull),
  /// UserData — application-level gossip payload.
  UserData(UserData),
  /// ErrorResponse — protocol error from remote.
  ErrorResponse(ErrorResponse),
}

/// Errors returned by [`decode_plain_frame`] and [`decode_message`].
#[derive(Debug, thiserror::Error)]
pub enum FrameError {
  /// The input buffer is empty; no frame to decode.
  #[error("frame buffer is empty")]
  Empty,
  /// The buffer holds a partial frame: `available` bytes present, `required` needed.
  #[error("incomplete frame: {0} bytes available, {1} required")]
  Incomplete(usize, usize),
  /// The tag byte does not correspond to a known message type.
  #[error("unknown message tag: {0}")]
  UnknownTag(u8),
  /// The varint length field overflows a `u32` (more than 5 continuation bytes).
  #[error("varint overflow")]
  VarintOverflow,
  /// The buffa decoder rejected the body bytes.
  #[error("buffa decode error")]
  Decode,
  /// The body is larger than the `u32` length prefix can represent.
  /// Encoding it would silently truncate the length and desynchronize
  /// the receiver, so it is rejected instead.
  #[error("frame body too large: {0} bytes exceeds u32::MAX")]
  FrameTooLarge(usize),
  /// A compressed wrapper frame failed to decode (corrupt bytes, an unknown
  /// algorithm, or a declared length over the bomb-guard ceiling).
  #[error("compressed frame decode failed: {0}")]
  Compression(String),
}

/// Convenience: encode any [`AnyMessage`] variant into a plain frame.
///
/// Picks the right [`MessageTag`] automatically and delegates body encoding
/// to the buffa-generated `encode_to_vec` implementation.
pub fn encode_message(msg: &AnyMessage) -> Result<Vec<u8>, FrameError> {
  let tag = MessageTag::for_message(msg);
  let body = match msg {
    AnyMessage::Alive(m) => m.encode_to_vec(),
    AnyMessage::Suspect(m) => m.encode_to_vec(),
    AnyMessage::Dead(m) => m.encode_to_vec(),
    AnyMessage::Ping(m) => m.encode_to_vec(),
    AnyMessage::IndirectPing(m) => m.encode_to_vec(),
    AnyMessage::Ack(m) => m.encode_to_vec(),
    AnyMessage::Nack(m) => m.encode_to_vec(),
    AnyMessage::PushPull(m) => m.encode_to_vec(),
    AnyMessage::UserData(m) => m.encode_to_vec(),
    AnyMessage::ErrorResponse(m) => m.encode_to_vec(),
  };
  encode_plain_frame(tag, &body)
}

/// Convenience: decode a plain frame into the appropriate [`AnyMessage`] variant.
///
/// Returns `(consumed_bytes, AnyMessage)` on success. The `consumed_bytes`
/// value is the total number of bytes read from `buf` (tag + varint + body),
/// allowing the caller to advance their read cursor.
pub fn decode_message(buf: &[u8]) -> Result<(usize, AnyMessage), FrameError> {
  let (tag, body, consumed) = decode_plain_frame(buf)?;
  let msg = match tag {
    MessageTag::Alive => {
      AnyMessage::Alive(Alive::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Suspect => {
      AnyMessage::Suspect(Suspect::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Dead => {
      AnyMessage::Dead(Dead::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Ping => {
      AnyMessage::Ping(Ping::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::IndirectPing => AnyMessage::IndirectPing(
      IndirectPing::decode_from_slice(body).map_err(|_| FrameError::Decode)?,
    ),
    MessageTag::Ack => {
      AnyMessage::Ack(Ack::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Nack => {
      AnyMessage::Nack(Nack::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::PushPull => {
      AnyMessage::PushPull(PushPull::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::UserData => {
      AnyMessage::UserData(UserData::decode_from_slice(body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::ErrorResponse => AnyMessage::ErrorResponse(
      ErrorResponse::decode_from_slice(body).map_err(|_| FrameError::Decode)?,
    ),
    MessageTag::Compound => {
      // Compound is an outer wrapper; not decoded at the plain-frame layer.
      return Err(FrameError::UnknownTag(MessageTag::Compound as u8));
    }
    MessageTag::Compressed => {
      // Compressed is an outer transform wrapper; not decoded at the
      // plain-frame layer.
      return Err(FrameError::UnknownTag(MessageTag::Compressed as u8));
    }
  };
  Ok((consumed, msg))
}

/// Encode two or more messages into one compound frame:
/// `[Compound][varint count][ (varint inner_len, inner plain-frame) … ]`.
///
/// Each part is a complete plain frame (`encode_message`), so the inbound
/// split feeds each straight back through `decode_message` with no
/// per-message decoding change. New-wire-idiom only — intentionally NOT
/// byte-compatible with the frozen `memberlist-proto` batch format
/// `count >= 2` (a single message must stay a plain frame so its bytes
/// are byte-identical); fewer ⇒ `FrameError::Decode`.
/// Oversize count / part ⇒ `FrameError::FrameTooLarge`.
pub fn encode_compound(msgs: &[AnyMessage]) -> Result<Vec<u8>, FrameError> {
  if msgs.len() < 2 {
    return Err(FrameError::Decode);
  }
  let count = u32::try_from(msgs.len()).map_err(|_| FrameError::FrameTooLarge(msgs.len()))?;
  // Lower-bound preallocation (Compound tag + count varint + per-part
  // inner_len varint), mirroring `encode_plain_frame`'s capacity hint.
  let mut out = Vec::with_capacity(1 + 5 + msgs.len() * (1 + 5));
  out.push(MessageTag::Compound as u8);
  encode_varint_u32(count, &mut out);
  for m in msgs {
    let part = encode_message(m)?;
    let inner_len = u32::try_from(part.len()).map_err(|_| FrameError::FrameTooLarge(part.len()))?;
    encode_varint_u32(inner_len, &mut out);
    out.extend_from_slice(&part);
  }
  Ok(out)
}

/// Split a compound frame into its inner plain-frame slices, in order.
///
/// Each returned slice borrows from `buf` and is exactly what
/// `decode_message` consumes. Rejects: a non-`Compound` tag
/// (`FrameError::UnknownTag`), `count < 2` (`FrameError::Decode` — a single
/// message must be a plain frame), a truncated count / part
/// (`FrameError::Incomplete`), and any trailing bytes after the last part
/// (`FrameError::Decode` — all-or-nothing per datagram). The caller drops
/// the whole datagram on any error (no partial-prefix delivery).
pub fn decode_compound(buf: &[u8]) -> Result<Vec<&[u8]>, FrameError> {
  if buf.is_empty() {
    return Err(FrameError::Empty);
  }
  if MessageTag::try_from(buf[0])? != MessageTag::Compound {
    return Err(FrameError::UnknownTag(buf[0]));
  }
  let (count, count_bytes) = match decode_varint_u32(&buf[1..]) {
    Ok(v) => v,
    Err(FrameError::Incomplete(..)) => {
      return Err(FrameError::Incomplete(buf.len(), buf.len() + 1));
    }
    Err(e) => return Err(e),
  };
  if count < 2 {
    return Err(FrameError::Decode);
  }
  let mut cursor = 1 + count_bytes;
  // `count` is wire-supplied. Each part is at minimum a 1-byte zero-length
  // `inner_len` varint, so a count exceeding the bytes remaining after the
  // header is structurally impossible. Reject it BEFORE `Vec::with_capacity`
  // so a 6-byte datagram declaring `count = u32::MAX` cannot trigger a
  // multi-GB allocation / `handle_alloc_error` process abort — a
  // network-reachable DoS from a single malformed gossip packet.
  let remaining = buf.len().saturating_sub(cursor);
  if count as usize > remaining {
    return Err(FrameError::Decode);
  }
  let mut parts: Vec<&[u8]> = Vec::with_capacity(count as usize);
  for _ in 0..count {
    let (inner_len, len_bytes) = match decode_varint_u32(&buf[cursor..]) {
      Ok(v) => v,
      Err(FrameError::Incomplete(..)) => {
        return Err(FrameError::Incomplete(buf.len(), buf.len() + 1));
      }
      Err(e) => return Err(e),
    };
    // `len_bytes` is a decoder constant (<= 5), not wire-supplied, and
    // `cursor < buf.len()` is held by the loop, so this add cannot wrap.
    let body_start = cursor + len_bytes;
    // checked_add: defensive at this attacker-reachable parse site — a
    // wrapping add on a 32-bit `usize` could otherwise yield a
    // `body_start > body_end` slice index and panic.
    let body_end = body_start
      .checked_add(inner_len as usize)
      .ok_or(FrameError::Decode)?;
    if buf.len() < body_end {
      return Err(FrameError::Incomplete(buf.len(), body_end));
    }
    parts.push(&buf[body_start..body_end]);
    cursor = body_end;
  }
  if cursor != buf.len() {
    return Err(FrameError::Decode); // trailing bytes — malformed datagram
  }
  Ok(parts)
}

/// Strip every leading transform wrapper off `buf`, returning the innermost
/// plain-message-or-compound bytes.
///
/// This is the codec-level transform-stack unwrap loop: peek the leading tag —
/// if it is a transform wrapper ([`MessageTag::Compressed`]), strip that
/// transform and repeat; once the tag is a plain-message or compound tag,
/// return the buffer for the existing [`decode_message`] / [`decode_compound`]
/// path. The loop handles a stack of arbitrary depth, so it is
/// forward-compatible with an outer encryption wrapper.
///
/// `max_orig_len` bounds every wrapper's decompressed length (the
/// decompression-bomb guard); it is the UDP max-packet-size on the gossip path
/// and the reliable max-frame-size on the reliable path.
///
/// A buffer with no transform wrapper is returned unchanged (borrowed) — the
/// steady state when compression is disabled cluster-wide.
pub fn unwrap_transforms(buf: &[u8], max_orig_len: usize) -> Result<Cow<'_, [u8]>, FrameError> {
  let mut current: Cow<'_, [u8]> = Cow::Borrowed(buf);
  loop {
    let lead = match current.first() {
      Some(b) => *b,
      None => return Err(FrameError::Empty),
    };
    if lead != MessageTag::Compressed as u8 {
      // Plain-message or compound tag — the transform stack is fully stripped.
      return Ok(current);
    }
    let decoded = crate::compression::decode_compressed_frame(&current, max_orig_len)
      .map_err(|e| FrameError::Compression(e.to_string()))?;
    current = Cow::Owned(decoded);
  }
}

/// Build a `[TAG][VARINT_LEN][BODY]` plain frame around the supplied body bytes.
///
/// The outer tag byte and varint length are byte-identical to the legacy
/// `memberlist-proto` framing. The body bytes are caller-supplied and may
/// differ (buffa standard protobuf vs. legacy custom encoding).
pub fn encode_plain_frame(tag: MessageTag, body: &[u8]) -> Result<Vec<u8>, FrameError> {
  // The length prefix is a u32 varint; a body that does not fit would
  // have its length silently truncated mod 2^32 while the full body is
  // still appended, desynchronizing the receiver. Reject instead of
  // emitting an unrepresentable frame.
  let len = u32::try_from(body.len()).map_err(|_| FrameError::FrameTooLarge(body.len()))?;
  let mut out = Vec::with_capacity(1 + 5 + body.len());
  out.push(tag as u8);
  encode_varint_u32(len, &mut out);
  out.extend_from_slice(body);
  Ok(out)
}

/// Decode one plain frame off the front of `buf`.
///
/// Returns `(tag, body_slice, total_bytes_consumed)` on success. The
/// `body_slice` borrows from `buf`; the caller owns the lifetime.
/// `total_bytes_consumed` includes the tag byte, varint, and body.
pub fn decode_plain_frame(buf: &[u8]) -> Result<(MessageTag, &[u8], usize), FrameError> {
  if buf.is_empty() {
    return Err(FrameError::Empty);
  }
  let tag = MessageTag::try_from(buf[0])?;
  let (body_len, varint_bytes) = match decode_varint_u32(&buf[1..]) {
    Ok(v) => v,
    // A truncated length prefix is reported by the varint decoder relative
    // to `buf[1..]`; re-express it against the whole frame so streaming
    // callers see consistent absolute "need more bytes" numbers.
    Err(FrameError::Incomplete(..)) => {
      return Err(FrameError::Incomplete(buf.len(), buf.len() + 1));
    }
    Err(e) => return Err(e),
  };
  let header_len = 1 + varint_bytes;
  let body_end = header_len + body_len as usize;
  if buf.len() < body_end {
    return Err(FrameError::Incomplete(buf.len(), body_end));
  }
  Ok((tag, &buf[header_len..body_end], body_end))
}

// ── Private varint helpers ───────────────────────────────────────────────────

pub(crate) fn encode_varint_u32(mut value: u32, out: &mut Vec<u8>) {
  while value >= 0x80 {
    out.push(((value & 0x7f) as u8) | 0x80);
    value >>= 7;
  }
  out.push(value as u8);
}

pub(crate) fn decode_varint_u32(buf: &[u8]) -> Result<(u32, usize), FrameError> {
  let mut value: u32 = 0;
  let mut shift: u32 = 0;
  for (i, &byte) in buf.iter().enumerate().take(5) {
    // A u32 LEB128 is at most 5 bytes; the 5th byte (i == 4) carries only
    // the top 4 bits (32 - 4*7 = 4), so it must be <= 0x0f with the
    // continuation bit clear. Without this guard `0x10..=0xff` shifts left
    // by 28 and silently wraps the u32 (e.g. `80 80 80 80 10` => 0 with the
    // continuation bit clear), so a malformed 2^32 length prefix would be
    // accepted as a valid short frame instead of rejected.
    if i == 4 && byte > 0x0f {
      return Err(FrameError::VarintOverflow);
    }
    value |= u32::from(byte & 0x7f) << shift;
    if byte & 0x80 == 0 {
      return Ok((value, i + 1));
    }
    shift += 7;
  }
  // The loop can only fall through when it ran out of input before a
  // terminating byte (i == 4 with the continuation bit set is rejected
  // above as `> 0x0f`). That is a truncated length prefix — an INCOMPLETE
  // frame that needs more bytes — not a corrupt/overflowing varint. The
  // caller (`decode_plain_frame`) normalizes this to absolute terms.
  Err(FrameError::Incomplete(buf.len(), buf.len() + 1))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;
  use crate::messages::memberlist::v1::Node;

  #[test]
  fn varint_roundtrip() {
    for value in [0u32, 1, 127, 128, 16383, 16384, u32::MAX] {
      let mut buf = Vec::new();
      encode_varint_u32(value, &mut buf);
      let (decoded, consumed) = decode_varint_u32(&buf).unwrap();
      assert_eq!(value, decoded, "value {value}");
      assert_eq!(consumed, buf.len(), "value {value} length");
    }
  }

  #[test]
  fn varint_max_five_byte_boundary_is_accepted() {
    // u32::MAX encodes as `FF FF FF FF 0F`; the 5th byte is exactly the
    // 0x0f ceiling and must still decode (regression guard for the new
    // 5th-byte overflow check).
    let buf = [0xff, 0xff, 0xff, 0xff, 0x0f];
    let (v, n) = decode_varint_u32(&buf).expect("max u32 must decode");
    assert_eq!(v, u32::MAX);
    assert_eq!(n, 5);
  }

  #[test]
  fn varint_overflowing_fifth_byte_is_rejected_not_aliased() {
    // `80 80 80 80 10` encodes 2^32. Without the fifth-byte guard the
    // shift wraps the u32 to 0 with the continuation bit clear, so the
    // value decoded as length 0 and `decode_plain_frame` accepted a bogus
    // empty-body frame.
    for fifth in [0x10u8, 0x7f, 0x80, 0xff] {
      let buf = [0x80, 0x80, 0x80, 0x80, fifth];
      assert!(
        matches!(decode_varint_u32(&buf), Err(FrameError::VarintOverflow)),
        "fifth byte {fifth:#x} must overflow, not alias"
      );
    }
    // And it must not be silently accepted at the frame layer either:
    // `[tag=UserData][80 80 80 80 10][...]` must NOT decode as a 0-len frame.
    let mut frame = vec![MessageTag::UserData as u8, 0x80, 0x80, 0x80, 0x80, 0x10];
    frame.extend_from_slice(b"trailing");
    assert!(
      matches!(decode_plain_frame(&frame), Err(FrameError::VarintOverflow)),
      "a 2^32 length prefix must be rejected, not accepted as an empty frame"
    );
  }

  #[test]
  fn truncated_varint_is_incomplete_not_overflow() {
    // A length prefix cut mid-continuation needs more bytes; it must
    // report Incomplete (so streaming callers wait) rather than the hard
    // VarintOverflow corruption error.
    for prefix in [vec![0x80], vec![0x80, 0x80], vec![0x80, 0x80, 0x80, 0x80]] {
      assert!(
        matches!(
          decode_varint_u32(&prefix),
          Err(FrameError::Incomplete(_, _))
        ),
        "truncated varint {prefix:?} must be Incomplete"
      );
    }
    // Through the frame decoder: tag present, length prefix truncated.
    let frame = [MessageTag::Ping as u8, 0x80, 0x80];
    match decode_plain_frame(&frame) {
      Err(FrameError::Incomplete(have, need)) => {
        assert_eq!(have, frame.len());
        assert!(need > have);
      }
      other => panic!("expected Incomplete, got {other:?}"),
    }
  }

  #[test]
  fn plain_frame_roundtrip_for_alive() {
    let alive = Alive {
      incarnation: Some(7),
      meta: bytes::Bytes::new(),
      node: Some(Node {
        id: Some(bytes::Bytes::from_static(b"node-a")),
        addr: Some(bytes::Bytes::from_static(b"127.0.0.1:7000")),
        ..Default::default()
      })
      .into(),
      protocol_version: Some(1),
      delegate_version: Some(1),
      ..Default::default()
    };
    let msg = AnyMessage::Alive(alive.clone());
    let encoded = encode_message(&msg).expect("encode");
    assert_eq!(encoded[0], MessageTag::Alive as u8);

    let (consumed, decoded) = decode_message(&encoded).expect("decode");
    assert_eq!(consumed, encoded.len());
    match decoded {
      AnyMessage::Alive(d) => {
        assert_eq!(d.incarnation, alive.incarnation);
        assert_eq!(d.protocol_version, alive.protocol_version);
      }
      _ => panic!("wrong variant"),
    }
  }

  #[test]
  fn outer_wrapping_matches_legacy_byte_layout() {
    // Outer wrapping is `[TAG][VARINT_LEN][BODY]`. Verify byte positions.
    // This is the byte-level contract that must be preserved across versions.
    let body = b"hello";
    let frame = encode_plain_frame(MessageTag::UserData, body).expect("encode");
    // First byte: tag
    assert_eq!(frame[0], MessageTag::UserData as u8);
    // Second byte: varint for length 5 — fits in a single byte (< 0x80).
    assert_eq!(frame[1], 0x05);
    // Remaining bytes: body unchanged.
    assert_eq!(&frame[2..], body);
  }

  #[test]
  fn incomplete_frame_returns_error() {
    let mut frame = encode_plain_frame(MessageTag::Nack, b"x").expect("encode");
    frame.pop(); // truncate body by one byte
    let err = decode_plain_frame(&frame).unwrap_err();
    assert!(matches!(err, FrameError::Incomplete(_, _)));
  }

  fn sample_ping() -> AnyMessage {
    AnyMessage::Ping(Ping {
      sequence_number: Some(7),
      source: Some(Node {
        id: Some(bytes::Bytes::from_static(b"a")),
        addr: Some(bytes::Bytes::from_static(b"\x7f\x00\x00\x01")),
        ..Default::default()
      })
      .into(),
      target: Some(Node {
        id: Some(bytes::Bytes::from_static(b"b")),
        addr: Some(bytes::Bytes::from_static(b"\x7f\x00\x00\x02")),
        ..Default::default()
      })
      .into(),
      ..Default::default()
    })
  }

  fn sample_ack() -> AnyMessage {
    AnyMessage::Ack(Ack {
      sequence_number: 9,
      ..Default::default()
    })
  }

  #[test]
  fn compound_roundtrip_splits_into_ordered_parts() {
    let msgs = [sample_ping(), sample_ack(), sample_ping()];
    let buf = encode_compound(&msgs).expect("encode");
    assert_eq!(buf[0], MessageTag::Compound as u8);
    let parts = decode_compound(&buf).expect("decode");
    assert_eq!(parts.len(), 3);
    let decoded: Vec<AnyMessage> = parts
      .iter()
      .map(|p| decode_message(p).expect("part decodes").1)
      .collect();
    assert_eq!(decoded, vec![sample_ping(), sample_ack(), sample_ping()]);
  }

  #[test]
  fn compound_encode_rejects_fewer_than_two() {
    assert!(matches!(encode_compound(&[]), Err(FrameError::Decode)));
    assert!(matches!(
      encode_compound(&[sample_ping()]),
      Err(FrameError::Decode)
    ));
  }

  #[test]
  fn compound_decode_rejects_count_below_two() {
    let part = encode_message(&sample_ping()).unwrap();
    let mut buf = vec![MessageTag::Compound as u8];
    encode_varint_u32(1, &mut buf);
    encode_varint_u32(part.len() as u32, &mut buf);
    buf.extend_from_slice(&part);
    assert!(matches!(decode_compound(&buf), Err(FrameError::Decode)));
  }

  #[test]
  fn compound_decode_rejects_truncated_part() {
    let msgs = [sample_ping(), sample_ack()];
    let mut buf = encode_compound(&msgs).unwrap();
    buf.truncate(buf.len() - 1);
    assert!(matches!(
      decode_compound(&buf),
      Err(FrameError::Incomplete(..))
    ));
  }

  #[test]
  fn compound_decode_rejects_truncated_count_varint() {
    // [Compound][0x80]: count varint cut after a continuation byte — the
    // adversary's first probe. Must surface as Incomplete, not panic.
    let buf = [MessageTag::Compound as u8, 0x80];
    assert!(matches!(
      decode_compound(&buf),
      Err(FrameError::Incomplete(..))
    ));
  }

  #[test]
  fn compound_decode_rejects_truncated_inner_len_varint() {
    // Valid [Compound][count=2][len0][part0] but the entire second part
    // (its inner_len varint included) is absent: the loop's varint read
    // hits an empty slice and must report Incomplete. `count(2)` is <=
    // the bytes remaining after the header so the DoS guard does not fire
    // first — this exercises the per-part Incomplete path specifically.
    let p0 = encode_message(&sample_ping()).unwrap();
    let mut buf = vec![MessageTag::Compound as u8];
    encode_varint_u32(2, &mut buf);
    encode_varint_u32(p0.len() as u32, &mut buf);
    buf.extend_from_slice(&p0);
    assert!(matches!(
      decode_compound(&buf),
      Err(FrameError::Incomplete(..))
    ));
  }

  #[test]
  fn compound_decode_rejects_oversized_count_without_huge_alloc() {
    // A 6-byte datagram whose count varint decodes to u32::MAX must be
    // rejected as Decode BEFORE any `Vec::with_capacity(count)` — a
    // single malformed gossip packet must not abort the process via a
    // multi-GB allocation. The test passing (rather than OOM-aborting the
    // harness) is the assertion.
    let buf = [MessageTag::Compound as u8, 0xff, 0xff, 0xff, 0xff, 0x0f];
    assert!(matches!(decode_compound(&buf), Err(FrameError::Decode)));
  }

  #[test]
  fn compound_decode_rejects_trailing_bytes() {
    let msgs = [sample_ping(), sample_ack()];
    let mut buf = encode_compound(&msgs).unwrap();
    buf.push(0xAB);
    assert!(matches!(decode_compound(&buf), Err(FrameError::Decode)));
  }

  #[test]
  fn decode_message_still_rejects_compound_tag() {
    let buf = encode_compound(&[sample_ping(), sample_ack()]).unwrap();
    assert!(matches!(
      decode_message(&buf),
      Err(FrameError::UnknownTag(1))
    ));
  }

  #[test]
  fn compound_overhead_constants_are_upper_bounds() {
    assert_eq!(COMPOUND_TAG_LEN, 1);
    assert_eq!(COMPOUND_MAX_COUNT_PREFIX_LEN, 5);
    assert_eq!(COMPOUND_MAX_PART_PREFIX_LEN, 5);
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn unwrap_loop_strips_compression_off_a_plain_frame() {
    use crate::compression::{compress, encode_compressed_frame, CompressAlgorithm};
    let inner = encode_message(&sample_ping()).expect("encode ping");
    let packed = compress(CompressAlgorithm::Lz4, &inner).expect("compress");
    let wrapped = encode_compressed_frame(CompressAlgorithm::Lz4, inner.len(), &packed);
    let unwrapped = unwrap_transforms(&wrapped, 1 << 20).expect("unwrap");
    assert_eq!(unwrapped, inner);
    let (_consumed, msg) = decode_message(&unwrapped).expect("decode inner");
    assert_eq!(msg, sample_ping());
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn unwrap_loop_strips_compression_off_a_compound_frame() {
    use crate::compression::{compress, encode_compressed_frame, CompressAlgorithm};
    let inner = encode_compound(&[sample_ping(), sample_ack()]).expect("encode compound");
    let packed = compress(CompressAlgorithm::Lz4, &inner).expect("compress");
    let wrapped = encode_compressed_frame(CompressAlgorithm::Lz4, inner.len(), &packed);
    let unwrapped = unwrap_transforms(&wrapped, 1 << 20).expect("unwrap");
    assert_eq!(unwrapped, inner);
    let parts = decode_compound(&unwrapped).expect("decode compound");
    assert_eq!(parts.len(), 2);
  }

  #[test]
  fn unwrap_loop_passes_a_non_wrapper_frame_through() {
    let inner = encode_message(&sample_ping()).expect("encode ping");
    let out = unwrap_transforms(&inner, 1 << 20).expect("unwrap");
    assert_eq!(out, inner);
  }

  #[test]
  fn unwrap_loop_rejects_an_unknown_algorithm_wrapper() {
    let mut frame = vec![MessageTag::Compressed as u8, 222u8];
    encode_varint_u32(4, &mut frame);
    frame.extend_from_slice(b"data");
    assert!(matches!(
      unwrap_transforms(&frame, 1 << 20),
      Err(FrameError::Compression(_))
    ));
  }

  #[test]
  fn multibyte_length_body_round_trips_through_fallible_encode() {
    // Encode is fallible — it rejects bodies whose length overflows the
    // u32 prefix instead of silently truncating via `as u32`. A
    // large-but-representable body must still encode Ok and round-trip
    // with a correct multi-byte varint length. The >u32::MAX rejection is
    // structurally guaranteed by `u32::try_from` and is not unit-testable
    // without a >4 GiB allocation.
    let body = vec![0xABu8; 200_000];
    let frame = encode_plain_frame(MessageTag::UserData, &body).expect("encode");
    assert_eq!(frame[0], MessageTag::UserData as u8);
    let (tag, decoded_body, consumed) = decode_plain_frame(&frame).expect("decode");
    assert_eq!(tag, MessageTag::UserData);
    assert_eq!(decoded_body, &body[..]);
    assert_eq!(consumed, frame.len());
    // The error variant is wired for the overflow path.
    let e = FrameError::FrameTooLarge(u32::MAX as usize + 1);
    assert!(e.to_string().contains("too large"));
  }
}
