//! Encode/decode bridging `crate::typed::Message<I, A>` to the
//! on-wire byte form used by the UDP/TCP/QUIC drivers.
//!
//! The inner frame `[TAG][VARINT len][BODY]` is produced/consumed by
//! `crate::framing` (body bytes are buffa-encoded protobuf). This module
//! adds the optional outer **label** frame only. The compression / checksum /
//! encryption / compound transform layers live in their own modules
//! (`crate::compression`, `crate::checksum`, `crate::encryption`,
//! `crate::framing`) and are composed by the per-transport coordinators, not
//! here.
use std::vec::Vec;

use crate::{
  Data, framing,
  label::{
    LABEL_OVERHEAD, LabelError, LabelVerdict, classify_header, effective_label,
    encode_label_prefix, validate_label,
  },
  message_from_any, message_to_any,
  typed::Message,
};
// Re-exported from the codec module so the historical `…::codec::MAX_LABEL_LEN`
// path resolves for downstream crates; the constant itself lives in `crate::label`.
pub use crate::label::MAX_LABEL_LEN;
use bytes::Bytes;
use derive_more::{IsVariant, TryUnwrap, Unwrap};

/// Errors from the umbrella codec layer.
#[derive(Debug, thiserror::Error, IsVariant, Unwrap, TryUnwrap)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
#[non_exhaustive]
pub enum CodecError {
  /// Typed ↔ buffa bridge failure.
  #[error(transparent)]
  Bridge(#[from] crate::BridgeError),
  /// Inner plain-frame encode/decode failure.
  #[error(transparent)]
  Frame(#[from] framing::FrameError),
  /// The inner frame is incomplete. A streaming (TCP) caller should buffer
  /// more bytes and retry; a datagram (UDP) caller should treat this as a
  /// malformed packet.
  #[error(transparent)]
  Incomplete(#[from] framing::IncompleteFrame),
  /// A cluster-label frame failed to encode, decode, or match — length,
  /// UTF-8, a mismatching label, or an unexpected label header on an
  /// unlabeled node. See [`LabelError`] for the specific case.
  #[error(transparent)]
  Label(#[from] LabelError),
  /// Input ended before a complete label or inner frame could be read. See
  /// [`TruncatedInput`] for the specific point.
  #[error(transparent)]
  Truncated(#[from] TruncatedInput),
  /// A single-message datagram contained extra bytes after the framed
  /// message. The UDP/QUIC drivers carry exactly one message per
  /// datagram, so trailing bytes are malformed/smuggled data and are
  /// rejected (parity with the stream path's `consumed == total` check).
  #[error(transparent)]
  TrailingData(#[from] TrailingData),
}

/// The point at which inbound bytes ended before a complete label or inner
/// frame could be read. Payload of [`CodecError::Truncated`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error, IsVariant)]
#[non_exhaustive]
pub enum TruncatedInput {
  /// The input was empty — no bytes at all.
  #[error("truncated input: empty input")]
  EmptyInput,
  /// The frame was empty after a non-empty label prefix was stripped.
  #[error("truncated input: empty inner frame after label")]
  EmptyInnerFrame,
  /// A label header was present but ended before it was complete.
  #[error("truncated input: incomplete label header")]
  LabelHeader,
}

/// Trailing bytes after the single framed message in a datagram: of `total`
/// bytes, `consumed` were the message and the remainder is unexpected.
/// Payload of [`CodecError::TrailingData`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("trailing data after message: consumed {consumed} of {total} bytes")]
pub struct TrailingData {
  consumed: usize,
  total: usize,
}

impl TrailingData {
  /// Build from the consumed and total datagram byte counts.
  #[inline]
  pub(crate) const fn new(consumed: usize, total: usize) -> Self {
    Self { consumed, total }
  }

  /// Bytes consumed by the framed message.
  #[inline]
  pub const fn consumed(&self) -> usize {
    self.consumed
  }

  /// Total bytes in the datagram.
  #[inline]
  pub const fn total(&self) -> usize {
    self.total
  }
}

/// Outbound encoding options. Zero value = plain frame, no label.
#[derive(Clone, Default)]
pub struct EncodeOptions {
  label: Option<Bytes>,
  max_payload_size: usize,
}

impl EncodeOptions {
  /// Construct encoding options with the given optional label and the
  /// default `max_payload_size = 0` (= 65507).
  pub fn new(label: Option<Bytes>) -> Self {
    Self {
      label,
      max_payload_size: 0,
    }
  }

  /// Replace the label (empty/None = no label frame).
  pub fn with_label(mut self, label: Option<Bytes>) -> Self {
    self.label = label;
    self
  }

  /// Replace the `max_payload_size` advisory cap (0 = default 65507).
  pub fn with_max_payload_size(mut self, max_payload_size: usize) -> Self {
    self.max_payload_size = max_payload_size;
    self
  }

  /// The optional label to prepend (empty/None = no label frame).
  #[inline(always)]
  pub fn label_ref(&self) -> Option<&Bytes> {
    self.label.as_ref()
  }

  /// Advisory max total encoded size; 0 = default (65507, the practical
  /// UDP max). Oversized datagrams are not fragmented or rejected here;
  /// the driver layer is responsible for transport-level limits.
  #[inline(always)]
  pub const fn max_payload_size(&self) -> usize {
    self.max_payload_size
  }
}

/// Inbound decoding options.
#[derive(Clone, Default)]
pub struct DecodeOptions {
  label: Option<Bytes>,
}

impl DecodeOptions {
  /// Construct decoding options with the given expected label. If `Some`,
  /// a mismatching or missing label is an error; if `None`, an inbound
  /// frame carrying a label header is rejected with
  /// [`CodecError::DoubleLabel`] (an unlabeled node must not silently accept
  /// labeled traffic), rather than stripped.
  pub fn new(label: Option<Bytes>) -> Self {
    Self { label }
  }

  /// The expected label.
  #[inline(always)]
  pub fn label_ref(&self) -> Option<&Bytes> {
    self.label.as_ref()
  }
}

/// Apply the optional outer label wrapper to an already-encoded inner
/// frame (plain or compound). Identical label/UTF-8/length rules as the
/// single-message path, applied uniformly to the whole datagram.
///
/// An empty label is "no label" (frozen memberlist-proto only writes a
/// header when `label_size > 0`); never emit a `[12][0]` empty-label
/// header — the frozen decoder would reject it as a double label.
/// Frozen `memberlist-proto::Label` is a validated UTF-8 type; never
/// emit a non-UTF-8 label a faithful decoder would reject.
fn wrap_label(inner: Vec<u8>, opts: &EncodeOptions) -> Result<Bytes, CodecError> {
  let out = if let Some(label) = effective_label(opts.label_ref().map(|b| b.as_ref())) {
    let label_len = label.len();
    validate_label(label)?;
    let mut buf = Vec::with_capacity(LABEL_OVERHEAD + label_len + inner.len());
    encode_label_prefix(label, &mut buf);
    buf.extend_from_slice(&inner);
    buf
  } else {
    inner
  };
  Ok(Bytes::from(out))
}

/// Encode one typed message into a (optionally label-wrapped) frame.
///
/// Returns a [`Bytes`] buffer containing `[LABELED_TAG][len][label]` (when a
/// label is provided) followed by the inner plain frame
/// `[MSG_TAG][VARINT body_len][BODY]`.
pub fn encode_outgoing<I, A>(msg: &Message<I, A>, opts: &EncodeOptions) -> Result<Bytes, CodecError>
where
  I: Data,
  A: Data,
{
  let any = message_to_any(msg)?;
  let inner = framing::encode_message(&any)?;
  wrap_label(inner, opts)
}

/// Encode a SWIM piggyback batch (`>= 2` messages to one peer) into one
/// label-wrapped compound datagram. Fewer than two messages ⇒
/// [`CodecError::Frame`] (a single message must use [`encode_outgoing`] so
/// its bytes stay a byte-identical plain frame).
pub fn encode_outgoing_compound<I, A>(
  msgs: &[Message<I, A>],
  opts: &EncodeOptions,
) -> Result<Bytes, CodecError>
where
  I: Data,
  A: Data,
{
  let mut anys = Vec::with_capacity(msgs.len());
  for m in msgs {
    anys.push(message_to_any(m)?);
  }
  let inner = framing::encode_compound(&anys)?;
  wrap_label(inner, opts)
}

/// Strip the optional outer label frame, returning the inner frame bytes.
///
/// An empty expected label is "no label" (faithful to frozen
/// `memberlist-proto`), so:
/// - If the first byte is `LABELED_TAG` (12), the label header is parsed.
///   With a non-empty expected label it must match exactly or
///   [`CodecError::LabelMismatch`] is returned; with no/empty expected
///   label a present label header is rejected as [`CodecError::DoubleLabel`]
///   (frozen `ProtoDecoderError::double_label()`) rather than silently
///   unwrapped, so an unlabeled node never accepts traffic labeled for
///   another cluster. A `[12][0]` empty-label header is therefore always
///   rejected (the frozen encoder never emits it).
/// - Otherwise the frame is unlabeled: accepted when no/empty label is
///   expected, or [`CodecError::LabelMismatch`] when a non-empty label is
///   expected (missing-label strictness).
pub fn decode_incoming(raw: Bytes, opts: &DecodeOptions) -> Result<Bytes, CodecError> {
  let expected = effective_label(opts.label_ref().map(|b| b.as_ref()));
  // The datagram path never suppresses the inbound label check; datagrams
  // are single-shot and carry no per-stream policy.
  match classify_header(&raw, expected, false) {
    LabelVerdict::Accepted(consumed) => {
      let inner = raw.slice(consumed..);
      if inner.is_empty() {
        if consumed > 0 {
          return Err(CodecError::Truncated(TruncatedInput::EmptyInnerFrame));
        }
        return Err(CodecError::Truncated(TruncatedInput::EmptyInput));
      }
      Ok(inner)
    }
    LabelVerdict::Incomplete => Err(CodecError::Truncated(TruncatedInput::LabelHeader)),
    LabelVerdict::Rejected(e) => Err(CodecError::Label(e)),
  }
}

/// Parse inner plain-frame bytes into a single typed message.
///
/// The `plain` buffer must start with a message tag byte in the range 2–11
/// and contain **exactly one** framed message — the UDP/QUIC drivers carry
/// one message per datagram. Trailing bytes after the frame are rejected as
/// [`CodecError::TrailingData`] (parity with the stream path's
/// `consumed == total` check); compound multi-frame packets are a separate,
/// currently-unimplemented path. Passing label-prefixed bytes (starting
/// with tag 12) results in a [`CodecError::Frame`] error.
pub fn parse_message<I, A>(plain: Bytes) -> Result<Message<I, A>, CodecError>
where
  I: Data,
  A: Data,
{
  let (consumed, any) = framing::decode_message_zerocopy(&plain).map_err(|e| match e {
    framing::FrameError::Incomplete(f) => CodecError::Incomplete(f),
    other => CodecError::Frame(other),
  })?;
  if consumed != plain.len() {
    return Err(CodecError::TrailingData(TrailingData::new(
      consumed,
      plain.len(),
    )));
  }
  message_from_any::<I, A>(&any).map_err(CodecError::Bridge)
}

/// Parse a (label-stripped) inbound datagram into its messages, in order.
///
/// A plain frame ⇒ a one-element vec (identical result to
/// [`parse_message`]). A `Compound` frame ⇒ the ordered N messages it
/// carries. Enforces `consumed == total` over the whole datagram (the
/// per-part check is `decode_compound`'s no-trailing-bytes rule; the plain
/// path reuses `parse_message`'s [`CodecError::TrailingData`]). A malformed
/// compound (bad count / truncated / trailing) ⇒ an error and the caller
/// drops the whole datagram (no partial-prefix delivery). The single-message
/// [`parse_message`] is retained unchanged for callers needing exactly one.
///
/// The per-part `consumed == part.len()` check in the loop is
/// defense-in-depth: `decode_compound`'s `body_start..body_end` slicing
/// already guarantees `consumed == part.len()` for well-formed parts, so it
/// can only fire if the framing layer's windowing invariant is violated —
/// which is why no separate whole-datagram `consumed == total` pass is
/// needed for the compound branch.
pub fn parse_messages<I, A>(plain: Bytes) -> Result<Vec<Message<I, A>>, CodecError>
where
  I: Data,
  A: Data,
{
  if plain.first() == Some(&(framing::MessageTag::Compound as u8)) {
    let parts = framing::decode_compound(&plain).map_err(|e| match e {
      framing::FrameError::Incomplete(f) => CodecError::Incomplete(f),
      other => CodecError::Frame(other),
    })?;
    let mut out = Vec::with_capacity(parts.len());
    // Parts are flattened in wire order, and callers dispatch them in that
    // order. This is load-bearing for layered-protocol departure classification:
    // a farewell compound packs its user payloads (e.g. a departure intent)
    // ahead of the `Dead` part precisely so this in-order consumption processes
    // them before the membership death carried by the same datagram.
    for part in parts {
      // `part` is a sub-slice of `plain`; `slice_ref` shares the datagram
      // allocation O(1), so the per-part frame decodes its byte fields zero-copy
      // (they alias `plain`) instead of copying out of the buffer.
      let part = plain.slice_ref(part);
      let (consumed, any) = framing::decode_message_zerocopy(&part).map_err(|e| match e {
        framing::FrameError::Incomplete(f) => CodecError::Incomplete(f),
        other => CodecError::Frame(other),
      })?;
      if consumed != part.len() {
        return Err(CodecError::TrailingData(TrailingData::new(
          consumed,
          part.len(),
        )));
      }
      out.push(message_from_any::<I, A>(&any).map_err(CodecError::Bridge)?);
    }
    Ok(out)
  } else {
    Ok(vec![parse_message::<I, A>(plain)?])
  }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
