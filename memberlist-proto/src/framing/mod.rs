//! Plain-frame encoder/decoder for `[TAG_BYTE][VARINT_LEN][BUFFA_BYTES]`.
//!
//! The state machine (`memberlist-proto::Stream::handle_data`) consumes
//! exactly this shape. Outer transform wrappers nest outside this plain frame
//! (inner→outer: message → `Compressed` → `Checksumed` → `Encrypted` →
//! `Labeled`) and are stripped by the tag-driven unwrap loop
//! ([`unwrap_transforms_with_encryption`]) / the codec's label layer, not by
//! `decode_message`.
//!
//! # Wire-format note
//!
//! The outer plain-frame wrapping (single tag byte + varint length) is
//! byte-compatible with the legacy `memberlist-proto` framing. The inner
//! body bytes differ because buffa emits standard protobuf wire format
//! while `memberlist-proto` uses a custom high-3-bits-wire-type scheme.
//! Mixed-version clusters are not supported — this is a clean cut-over.

#[cfg(not(feature = "std"))]
use std::vec::Vec;

use std::borrow::Cow;

use buffa::Message as _;
use bytes::Bytes;
use varing::encoded_u32_varint_len;

use crate::{
  checksum::ChecksumError,
  compression::CompressionError,
  encryption::EncryptionError,
  messages::memberlist::v1::{
    Ack, Alive, Dead, ErrorResponse, IndirectPing, Nack, Ping, PushPull, Suspect, UserData,
  },
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
  /// Encrypted wrapper (outer transform; nests outside a Compressed wrapper
  /// or, when compression is disabled, outside the plain-message / compound
  /// frame). Decoded by the tag-driven unwrap loop, not by `decode_message`.
  Encrypted = 13,
  /// Compressed wrapper (outer transform; nests just inside a Checksumed
  /// wrapper, or directly outside a message / compound frame when no checksum
  /// is configured). Decoded by the tag-driven unwrap loop, not by
  /// `decode_message`.
  Compressed = 14,
  /// Checksumed wrapper (outer transform; nests just outside a Compressed
  /// wrapper / message / compound frame and inside an Encrypted wrapper). The
  /// wrapper is `[15][algorithm tag][digest bytes][payload]`; the digest is
  /// verified over `payload` by the tag-driven unwrap loop before the payload
  /// is unwrapped further. Not decoded by `decode_message`.
  Checksumed = 15,
  // Tag 12 (Labeled) is a transform wrapper handled by the codec's label
  // layer; 13 (Encrypted), 14 (Compressed), and 15 (Checksumed) are live
  // above. Transform wrappers are codec-level — stripped by the tag-driven
  // unwrap loop in this module — not driver-level.
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
      13 => Ok(Self::Encrypted),
      14 => Ok(Self::Compressed),
      15 => Ok(Self::Checksumed),
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

/// The `(available, required)` byte-count pair carried by
/// [`FrameError::Incomplete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("incomplete frame: {available} bytes available, {required} required")]
pub struct IncompleteFrame {
  available: usize,
  required: usize,
}

impl IncompleteFrame {
  /// Construct an incomplete-frame payload.
  #[inline(always)]
  pub const fn new(available: usize, required: usize) -> Self {
    Self {
      available,
      required,
    }
  }

  /// Bytes available in the buffer.
  #[inline(always)]
  pub const fn available(&self) -> usize {
    self.available
  }

  /// Bytes required to complete the frame.
  #[inline(always)]
  pub const fn required(&self) -> usize {
    self.required
  }
}

/// Errors returned by [`decode_plain_frame`] and [`decode_message`].
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum FrameError {
  /// The input buffer is empty; no frame to decode.
  #[error("frame buffer is empty")]
  Empty,
  /// The buffer holds a partial frame.
  #[error(transparent)]
  Incomplete(IncompleteFrame),
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
  Compression(#[from] CompressionError),
  /// An encrypted wrapper frame failed to decode (corrupt bytes, an unknown
  /// algorithm, a declared length over the bomb-guard ceiling, or every
  /// variant-filtered key in the keyring failed AEAD auth).
  #[error("encrypted frame decode failed: {0}")]
  Encryption(#[from] EncryptionError),
  /// A checksumed wrapper frame failed to verify (a recomputed digest did not
  /// match the carried digest, or the algorithm tag is unknown / its backend
  /// feature is not built in).
  #[error("checksumed frame decode failed: {0}")]
  Checksum(#[from] ChecksumError),
  /// After every transform wrapper was stripped, the recovered gossip frame
  /// exceeds the plaintext ceiling. The decrypt step allows a checksum
  /// wrapper's worth of slack; this guards against a peer smuggling an
  /// over-ceiling plain or checksum-only frame through that slack.
  #[error("decoded gossip plaintext too large: {0} bytes exceeds the ceiling")]
  OversizePlaintext(usize),
  /// The transform wrappers are not in canonical order, or a wrapper is
  /// repeated. The canonical decode order is `Encrypted` then `Checksumed`
  /// then `Compressed`, each at most once. Rejecting a non-canonical stack
  /// bounds the per-datagram unwrap work to O(1): without it, a peer could
  /// nest many (publicly forgeable) `Checksumed` wrappers in one packet to
  /// force thousands of digest passes and buffer copies.
  #[error("non-canonical or repeated transform wrapper")]
  NonCanonicalTransform,
}

/// Body length of a message's buffa encoding (no plain-frame tag/varint).
fn body_encoded_len(msg: &AnyMessage) -> usize {
  (match msg {
    AnyMessage::Alive(m) => m.encoded_len(),
    AnyMessage::Suspect(m) => m.encoded_len(),
    AnyMessage::Dead(m) => m.encoded_len(),
    AnyMessage::Ping(m) => m.encoded_len(),
    AnyMessage::IndirectPing(m) => m.encoded_len(),
    AnyMessage::Ack(m) => m.encoded_len(),
    AnyMessage::Nack(m) => m.encoded_len(),
    AnyMessage::PushPull(m) => m.encoded_len(),
    AnyMessage::UserData(m) => m.encoded_len(),
    AnyMessage::ErrorResponse(m) => m.encoded_len(),
  }) as usize
}

/// Append a message's buffa body encoding to `out` (the plain-frame body, no
/// tag/varint). Encodes in place — no intermediate buffer.
fn encode_body_into(msg: &AnyMessage, out: &mut Vec<u8>) {
  match msg {
    AnyMessage::Alive(m) => m.encode(out),
    AnyMessage::Suspect(m) => m.encode(out),
    AnyMessage::Dead(m) => m.encode(out),
    AnyMessage::Ping(m) => m.encode(out),
    AnyMessage::IndirectPing(m) => m.encode(out),
    AnyMessage::Ack(m) => m.encode(out),
    AnyMessage::Nack(m) => m.encode(out),
    AnyMessage::PushPull(m) => m.encode(out),
    AnyMessage::UserData(m) => m.encode(out),
    AnyMessage::ErrorResponse(m) => m.encode(out),
  }
}

/// Convenience: encode any [`AnyMessage`] variant into a plain frame.
///
/// Picks the right [`MessageTag`] automatically and encodes the body in place —
/// one allocation, no intermediate buffer.
pub fn encode_message(msg: &AnyMessage) -> Result<Vec<u8>, FrameError> {
  let tag = MessageTag::for_message(msg);
  let body_len = body_encoded_len(msg);
  // One allocation, body encoded in place: tag + varint + buffa body (a u32
  // varint is at most 5 bytes). Replaces the previous encode_to_vec() body
  // buffer + encode_plain_frame copy.
  let mut out = Vec::with_capacity(1 + 5 + body_len);
  out.push(tag as u8);
  encode_varint_u32(body_len as u32, &mut out);
  let body_start = out.len();
  encode_body_into(msg, &mut out);
  // `encoded_len()` is a `u32`, so a body whose true size exceeds that domain
  // wraps: the precomputed `body_len` (and the length prefix written above) then
  // disagrees with the bytes actually appended, yielding a frame the receiver
  // desyncs on. Validate the real write and reject rather than emit it. (The old
  // path validated `body.len()` in `encode_plain_frame` after encoding.)
  let written = out.len() - body_start;
  if written != body_len {
    return Err(FrameError::FrameTooLarge(written));
  }
  Ok(out)
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
    MessageTag::Encrypted => {
      // Encrypted is an outer transform wrapper; not decoded at the
      // plain-frame layer.
      return Err(FrameError::UnknownTag(MessageTag::Encrypted as u8));
    }
    MessageTag::Compressed => {
      // Compressed is an outer transform wrapper; not decoded at the
      // plain-frame layer.
      return Err(FrameError::UnknownTag(MessageTag::Compressed as u8));
    }
    MessageTag::Checksumed => {
      // Checksumed is an outer transform wrapper; not decoded at the
      // plain-frame layer.
      return Err(FrameError::UnknownTag(MessageTag::Checksumed as u8));
    }
  };
  Ok((consumed, msg))
}

/// Zero-copy [`decode_message`]: the decoded message's `bytes` fields (meta,
/// user data, ids, addresses) alias `frame` instead of owning fresh copies.
///
/// buffa reads each `bytes` field via [`bytes::Buf::copy_to_bytes`], which on a
/// `Bytes`-backed buffer is an O(1) refcount share rather than an allocate +
/// memcpy. `frame` must be one whole plain frame (`[tag][varint][body]`); the
/// returned `usize` is the bytes consumed, identical to [`decode_message`].
pub fn decode_message_zerocopy(frame: &Bytes) -> Result<(usize, AnyMessage), FrameError> {
  let (tag, body, consumed) = decode_plain_frame(frame.as_ref())?;
  // `body` is a sub-slice of `frame`, so `slice_ref` shares the allocation O(1)
  // rather than copying; the buffa decoder then carves each `bytes` field out
  // of this shared buffer with `Buf::copy_to_bytes` (also O(1)).
  let mut body = frame.slice_ref(body);
  let msg = match tag {
    MessageTag::Alive => {
      AnyMessage::Alive(Alive::decode(&mut body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Suspect => {
      AnyMessage::Suspect(Suspect::decode(&mut body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Dead => AnyMessage::Dead(Dead::decode(&mut body).map_err(|_| FrameError::Decode)?),
    MessageTag::Ping => AnyMessage::Ping(Ping::decode(&mut body).map_err(|_| FrameError::Decode)?),
    MessageTag::IndirectPing => {
      AnyMessage::IndirectPing(IndirectPing::decode(&mut body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Ack => AnyMessage::Ack(Ack::decode(&mut body).map_err(|_| FrameError::Decode)?),
    MessageTag::Nack => AnyMessage::Nack(Nack::decode(&mut body).map_err(|_| FrameError::Decode)?),
    MessageTag::PushPull => {
      AnyMessage::PushPull(PushPull::decode(&mut body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::UserData => {
      AnyMessage::UserData(UserData::decode(&mut body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::ErrorResponse => {
      AnyMessage::ErrorResponse(ErrorResponse::decode(&mut body).map_err(|_| FrameError::Decode)?)
    }
    MessageTag::Compound => return Err(FrameError::UnknownTag(MessageTag::Compound as u8)),
    MessageTag::Encrypted => return Err(FrameError::UnknownTag(MessageTag::Encrypted as u8)),
    MessageTag::Compressed => return Err(FrameError::UnknownTag(MessageTag::Compressed as u8)),
    MessageTag::Checksumed => return Err(FrameError::UnknownTag(MessageTag::Checksumed as u8)),
  };
  Ok((consumed, msg))
}

/// Encode two or more messages into one compound frame:
/// `[Compound][varint count][ (varint inner_len, inner plain-frame) … ]`.
///
/// Each part is a complete plain frame, so the inbound split feeds each
/// straight back through `decode_message` with no per-message decoding change.
/// New-wire-idiom only — intentionally NOT
/// byte-compatible with the frozen `memberlist-proto` batch format
/// `count >= 2` (a single message must stay a plain frame so its bytes
/// are byte-identical); fewer ⇒ `FrameError::Decode`.
/// Oversize count / part ⇒ `FrameError::FrameTooLarge`.
pub fn encode_compound(msgs: &[AnyMessage]) -> Result<Vec<u8>, FrameError> {
  if msgs.len() < 2 {
    return Err(FrameError::Decode);
  }
  let count = u32::try_from(msgs.len()).map_err(|_| FrameError::FrameTooLarge(msgs.len()))?;
  // Size each part once: a part is a plain frame `[tag][varint body_len][body]`,
  // and its `inner_len` is that frame's total length. Caching (body_len,
  // frame_len) lets the output be pre-sized exactly and every part be encoded
  // straight into `out` — no per-part Vec or copy.
  let mut parts: Vec<(usize, usize)> = Vec::with_capacity(msgs.len());
  let mut total = 1 + encoded_u32_varint_len(count).get();
  for m in msgs {
    let body_len = body_encoded_len(m);
    let body_len_u32 = u32::try_from(body_len).map_err(|_| FrameError::FrameTooLarge(body_len))?;
    let frame_len = 1 + encoded_u32_varint_len(body_len_u32).get() + body_len;
    let frame_len_u32 =
      u32::try_from(frame_len).map_err(|_| FrameError::FrameTooLarge(frame_len))?;
    // Checked: a long list of large parts could otherwise overflow the `usize`
    // capacity accumulator.
    total = total
      .checked_add(encoded_u32_varint_len(frame_len_u32).get())
      .and_then(|t| t.checked_add(frame_len))
      .ok_or(FrameError::FrameTooLarge(frame_len))?;
    parts.push((body_len, frame_len));
  }
  let mut out = Vec::with_capacity(total);
  out.push(MessageTag::Compound as u8);
  encode_varint_u32(count, &mut out);
  for (m, &(body_len, frame_len)) in msgs.iter().zip(&parts) {
    // inner_len (the part-frame length already validated to fit a u32), then
    // the part's plain frame encoded in place.
    encode_varint_u32(frame_len as u32, &mut out);
    out.push(MessageTag::for_message(m) as u8);
    encode_varint_u32(body_len as u32, &mut out);
    let body_start = out.len();
    encode_body_into(m, &mut out);
    // Same wrap guard as `encode_message`: if buffa wrote a body whose real
    // length disagrees with the precomputed `body_len`/`frame_len` (a wrapped
    // `encoded_len`), the inner_len prefix is wrong — reject the whole compound.
    let written = out.len() - body_start;
    if written != body_len {
      return Err(FrameError::FrameTooLarge(written));
    }
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
    Err(FrameError::Incomplete(_)) => {
      return Err(FrameError::Incomplete(IncompleteFrame::new(
        buf.len(),
        buf.len() + 1,
      )));
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
      Err(FrameError::Incomplete(_)) => {
        return Err(FrameError::Incomplete(IncompleteFrame::new(
          buf.len(),
          buf.len() + 1,
        )));
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
      return Err(FrameError::Incomplete(IncompleteFrame::new(
        buf.len(),
        body_end,
      )));
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
/// plain-message-or-compound bytes. Encryption-aware — accepts an
/// `&EncryptionOptions` so an [`MessageTag::Encrypted`] wrapper can be
/// decrypted via the configured keyring.
///
/// The loop strips `Encrypted` first (when present), then `Checksumed` (when
/// present), then `Compressed` (when present), then returns the inner buffer
/// for the existing [`decode_message`] / [`decode_compound`] path. The loop
/// handles a stack of arbitrary depth; the wire framing reserves the order as
/// `[Encrypted][[Checksumed][[Compressed][frame]]]` (inner→outer: message →
/// compress → checksum → encrypt), so a well-formed peer never produces an
/// inverted stack.
///
/// A `Checksumed` wrapper recomputes the digest over its payload and rejects a
/// mismatch (or an unknown / not-built-in algorithm) with
/// [`FrameError::Checksum`] before the payload is unwrapped further, so a
/// corrupted frame never reaches the message decoder.
///
/// `max_orig_len` bounds every wrapper's payload (the decompression-bomb
/// guard for `Compressed` and the ciphertext-bomb guard for `Encrypted`); it
/// is the UDP max-packet-size on the gossip path and the reliable
/// max-frame-size on the reliable path.
pub fn unwrap_transforms_with_encryption<'a>(
  buf: &'a [u8],
  max_orig_len: usize,
  encryption: &crate::encryption::EncryptionOptions,
) -> Result<std::borrow::Cow<'a, [u8]>, FrameError> {
  use std::borrow::Cow;
  // Strict mode: when encryption is configured, the OUTERMOST inbound frame
  // MUST be wrapped in `Encrypted`. A plain-message / compound /
  // compression-only datagram arriving on an encrypted path is
  // unauthenticated — a network attacker injecting SWIM membership traffic,
  // or a misconfigured peer running without encryption — and is rejected
  // before any decoding. Inner (post-strip) bytes can be anything; this
  // check fires once at the entry boundary, not inside the unwrap loop.
  if encryption.is_enabled() {
    let lead = buf.first().copied().ok_or(FrameError::Empty)?;
    if lead != MessageTag::Encrypted as u8 {
      return Err(FrameError::Encryption(EncryptionError::EncryptionRequired));
    }
  }
  // The transform wrappers MUST appear in the canonical decode order
  // `Encrypted` (stage 0) → `Checksumed` (stage 1) → `Compressed` (stage 2),
  // each at most once. `stage` is the index of the next wrapper still allowed;
  // a wrapper whose stage is behind `stage` (repeated or out of order) is
  // rejected BEFORE it is verified or copied. This bounds the loop to at most
  // three strips: without it, a peer could nest many publicly-forgeable
  // `Checksumed` wrappers in one datagram to force thousands of digest passes
  // and buffer copies per packet.
  let mut current: Cow<'a, [u8]> = Cow::Borrowed(buf);
  let mut stage: u8 = 0;
  loop {
    let lead = match current.first() {
      Some(b) => *b,
      None => return Err(FrameError::Empty),
    };
    if lead == MessageTag::Encrypted as u8 {
      if stage > 0 {
        return Err(FrameError::NonCanonicalTransform);
      }
      // The decrypted plaintext may carry a `Checksumed` wrapper nested inside
      // the encryption (`message → compress → checksum → encrypt`), so a frame
      // valid at `max_orig_len` before checksumming decrypts to up to
      // `max_orig_len + CHECKSUMED_WRAPPER_OVERHEAD`. Allow that slack here; the
      // post-decompression payload is still bounded tightly by `max_orig_len`
      // at the `Compressed` arm below.
      let decoded = crate::encryption::decode_encrypted_frame(
        encryption,
        &current,
        max_orig_len.saturating_add(crate::checksum::CHECKSUMED_WRAPPER_OVERHEAD),
      )?;
      current = Cow::Owned(decoded);
      stage = 1;
      continue;
    }
    if lead == MessageTag::Checksumed as u8 {
      if stage > 1 {
        return Err(FrameError::NonCanonicalTransform);
      }
      // Strip the wrapper and verify the digest over the remaining payload;
      // a mismatch / unknown / not-built-in algorithm rejects the frame
      // before it is unwrapped further. The verified payload is a borrow of
      // `current`, so re-own it before `current` is dropped.
      let verified = crate::checksum::decode_checksummed_frame(&current)?.to_vec();
      current = Cow::Owned(verified);
      stage = 2;
      continue;
    }
    if lead == MessageTag::Compressed as u8 {
      if stage > 2 {
        return Err(FrameError::NonCanonicalTransform);
      }
      let decoded = crate::compression::decode_compressed_frame(&current, max_orig_len)?;
      current = Cow::Owned(decoded);
      stage = 3;
      continue;
    }
    // Plain-message or compound tag — transform stack fully stripped. The
    // `Encrypted` arm permitted `max_orig_len + CHECKSUMED_WRAPPER_OVERHEAD` of
    // slack so a checksummed plaintext could decrypt; that slack is legitimate
    // only as a checksum wrapper, which the `Checksumed` arm then strips. The
    // fully-unwrapped frame the decoder finally sees must still honor the tight
    // `max_orig_len` ceiling — otherwise a peer could smuggle an over-ceiling
    // plain or checksum-only frame through the decrypt slack. (`decode_compressed_frame`
    // already bounds a decompressed payload, so this never rejects a legitimate
    // `Compressed` intermediate.)
    if current.len() > max_orig_len {
      return Err(FrameError::OversizePlaintext(current.len()));
    }
    return Ok(current);
  }
}

/// Encryption-unaware version of [`unwrap_transforms_with_encryption`]. A
/// caller that did not opt into encryption never silently consumes an
/// encrypted frame; an `Encrypted` wrapper surfaces as
/// [`FrameError::Encryption`].
pub fn unwrap_transforms(buf: &[u8], max_orig_len: usize) -> Result<Cow<'_, [u8]>, FrameError> {
  unwrap_transforms_with_encryption(
    buf,
    max_orig_len,
    &crate::encryption::EncryptionOptions::new(),
  )
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
    Err(FrameError::Incomplete(_)) => {
      return Err(FrameError::Incomplete(IncompleteFrame::new(
        buf.len(),
        buf.len() + 1,
      )));
    }
    Err(e) => return Err(e),
  };
  let header_len = 1 + varint_bytes;
  let body_end = header_len + body_len as usize;
  if buf.len() < body_end {
    return Err(FrameError::Incomplete(IncompleteFrame::new(
      buf.len(),
      body_end,
    )));
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
  Err(FrameError::Incomplete(IncompleteFrame::new(
    buf.len(),
    buf.len() + 1,
  )))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
