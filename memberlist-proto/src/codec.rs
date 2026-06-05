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

#[cfg(not(feature = "std"))]
use std::{
  string::{String, ToString},
  vec::Vec,
};

use crate::{
  Data, framing,
  label::{
    LABEL_OVERHEAD, LabelError, LabelOutcome, classify_header, effective_label,
    encode_label_prefix, validate_label,
  },
  message_from_any, message_to_any,
  typed::Message,
};
// Re-exported from the codec module so the historical `…::codec::MAX_LABEL_LEN`
// path resolves for downstream crates; the constant itself lives in `crate::label`.
pub use crate::label::MAX_LABEL_LEN;
use bytes::Bytes;

/// Errors from the umbrella codec layer.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
  /// typed <-> buffa bridge failure.
  #[error("bridge error: {0}")]
  Bridge(String),
  /// Inner plain-frame decode failure.
  #[error("frame error: {0}")]
  Frame(String),
  /// The inner frame is incomplete: `have` bytes present, `need` required.
  /// A streaming (TCP) caller should buffer more bytes and retry; a
  /// datagram (UDP) caller should treat this as a malformed packet.
  #[error("incomplete frame: have {0} bytes, need {1}")]
  Incomplete(usize, usize),
  /// A label longer than `MAX_LABEL_LEN` was supplied for encoding, or an
  /// inbound label header declared a length above it.
  #[error("label too long: {0} bytes (max {max})", max = MAX_LABEL_LEN)]
  LabelTooLong(usize),
  /// A label was not valid UTF-8. Faithful to frozen
  /// `memberlist-proto::Label`, which is a validated UTF-8 type — a
  /// non-UTF-8 label is neither emitted nor accepted.
  #[error("invalid label: {0}")]
  InvalidLabel(&'static str),
  /// The decoded label did not match the expected label.
  #[error("unexpected label")]
  LabelMismatch,
  /// A labeled frame arrived but no inbound label is configured. Faithful
  /// to the frozen `memberlist-proto` decoder
  /// (`ProtoDecoderError::double_label()`): when inbound label checking is
  /// disabled, a present label header is rejected rather than silently
  /// stripped, so an unlabeled node cannot accept traffic explicitly
  /// labeled for another cluster.
  #[error(
    "unexpected double label header: a labeled frame was received but no label is configured"
  )]
  DoubleLabel,
  /// Input ended before a complete label/frame could be read.
  #[error("truncated input: {0}")]
  Truncated(&'static str),
  /// A single-message datagram contained extra bytes after the framed
  /// message. The UDP/QUIC drivers carry exactly one message per
  /// datagram, so trailing bytes are malformed/smuggled data and are
  /// rejected (parity with the stream path's `consumed == total` check).
  #[error("trailing data after message: consumed {0} of {1} bytes")]
  TrailingData(usize, usize),
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
    validate_label(label).map_err(|e| match e {
      LabelError::TooLong => CodecError::LabelTooLong(label_len),
      LabelError::NotUtf8 => CodecError::InvalidLabel("label is not valid UTF-8"),
      _ => CodecError::InvalidLabel("invalid label"),
    })?;
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
  let any = message_to_any(msg).map_err(|e| CodecError::Bridge(e.to_string()))?;
  let inner = framing::encode_message(&any).map_err(|e| CodecError::Frame(e.to_string()))?;
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
    anys.push(message_to_any(m).map_err(|e| CodecError::Bridge(e.to_string()))?);
  }
  let inner = framing::encode_compound(&anys).map_err(|e| CodecError::Frame(e.to_string()))?;
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
    LabelOutcome::Accepted(consumed) => {
      let inner = raw.slice(consumed..);
      if inner.is_empty() {
        if consumed > 0 {
          return Err(CodecError::Truncated("empty inner frame after label"));
        }
        return Err(CodecError::Truncated("empty input"));
      }
      Ok(inner)
    }
    LabelOutcome::Incomplete => Err(CodecError::Truncated("label header")),
    LabelOutcome::Rejected(e) => match e {
      LabelError::TooLong => {
        // Re-derive the declared length for the error payload.
        let label_len = if raw.len() >= LABEL_OVERHEAD {
          raw[1] as usize
        } else {
          raw.len()
        };
        Err(CodecError::LabelTooLong(label_len))
      }
      LabelError::NotUtf8 => Err(CodecError::InvalidLabel("inbound label is not valid UTF-8")),
      LabelError::Mismatch => Err(CodecError::LabelMismatch),
      LabelError::DoubleLabel => Err(CodecError::DoubleLabel),
    },
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
    framing::FrameError::Incomplete(f) => CodecError::Incomplete(f.available(), f.required()),
    other => CodecError::Frame(other.to_string()),
  })?;
  if consumed != plain.len() {
    return Err(CodecError::TrailingData(consumed, plain.len()));
  }
  message_from_any::<I, A>(&any).map_err(|e| CodecError::Bridge(e.to_string()))
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
      framing::FrameError::Incomplete(f) => CodecError::Incomplete(f.available(), f.required()),
      other => CodecError::Frame(other.to_string()),
    })?;
    let mut out = Vec::with_capacity(parts.len());
    for part in parts {
      // `part` is a sub-slice of `plain`; `slice_ref` shares the datagram
      // allocation O(1), so the per-part frame decodes its byte fields zero-copy
      // (they alias `plain`) instead of copying out of the buffer.
      let part = plain.slice_ref(part);
      let (consumed, any) = framing::decode_message_zerocopy(&part).map_err(|e| match e {
        framing::FrameError::Incomplete(f) => CodecError::Incomplete(f.available(), f.required()),
        other => CodecError::Frame(other.to_string()),
      })?;
      if consumed != part.len() {
        return Err(CodecError::TrailingData(consumed, part.len()));
      }
      out.push(message_from_any::<I, A>(&any).map_err(|e| CodecError::Bridge(e.to_string()))?);
    }
    Ok(out)
  } else {
    Ok(vec![parse_message::<I, A>(plain)?])
  }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use core::net::SocketAddr;

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::*;
  use crate::{
    label::LABELED_TAG,
    typed::{Ack, Message},
  };

  type I = SmolStr;
  type A = SocketAddr;

  fn ack_msg() -> Message<I, A> {
    Message::Ack(Ack::new(7))
  }

  fn ping_msg() -> Message<I, A> {
    use crate::typed::{Node, Ping};
    Message::Ping(Ping::new(
      1,
      Node::new("a".into(), "127.0.0.1:1".parse().unwrap()),
      Node::new("b".into(), "127.0.0.1:2".parse().unwrap()),
    ))
  }

  #[test]
  fn parse_messages_plain_frame_yields_one() {
    let encoded = encode_outgoing(&ack_msg(), &EncodeOptions::default()).unwrap();
    let inner = decode_incoming(encoded, &DecodeOptions::default()).unwrap();
    let msgs: Vec<Message<I, A>> = parse_messages(inner).unwrap();
    assert_eq!(msgs.len(), 1);
    assert!(matches!(msgs[0], Message::Ack(_)));
  }

  #[test]
  fn compound_roundtrip_through_codec_ordered() {
    let batch = vec![ping_msg(), ack_msg(), ping_msg()];
    let encoded = encode_outgoing_compound(&batch, &EncodeOptions::default()).unwrap();
    let inner = decode_incoming(encoded, &DecodeOptions::default()).unwrap();
    let msgs: Vec<Message<I, A>> = parse_messages(inner).unwrap();
    assert_eq!(msgs.len(), 3);
    assert!(matches!(msgs[0], Message::Ping(_)));
    assert!(matches!(msgs[1], Message::Ack(_)));
    assert!(matches!(msgs[2], Message::Ping(_)));
  }

  #[test]
  fn compound_roundtrip_with_label() {
    let label = Bytes::from_static(b"cluster-x");
    let opts = EncodeOptions::new(Some(label.clone()));
    let batch = vec![ping_msg(), ack_msg()];
    let encoded = encode_outgoing_compound(&batch, &opts).unwrap();
    assert_eq!(
      encoded[0], 12,
      "compound datagram is label-wrapped as a whole"
    );
    let dec_opts = DecodeOptions::new(Some(label));
    let inner = decode_incoming(encoded, &dec_opts).unwrap();
    let msgs: Vec<Message<I, A>> = parse_messages(inner).unwrap();
    assert_eq!(msgs.len(), 2);
  }

  #[test]
  fn encode_outgoing_compound_rejects_fewer_than_two() {
    let one = vec![ack_msg()];
    // <2 ⇒ framing::encode_compound returns FrameError::Decode ⇒ CodecError::Frame
    assert!(matches!(
      encode_outgoing_compound(&one, &EncodeOptions::default()),
      Err(CodecError::Frame(_))
    ));
  }

  #[test]
  fn parse_messages_rejects_trailing_bytes_in_compound() {
    let batch = vec![ping_msg(), ack_msg()];
    let mut raw = encode_outgoing_compound(&batch, &EncodeOptions::default())
      .unwrap()
      .to_vec();
    raw.push(0xFF);
    let inner = decode_incoming(Bytes::from(raw), &DecodeOptions::default()).unwrap();
    // trailing byte ⇒ decode_compound FrameError::Decode ⇒ CodecError::Frame
    assert!(matches!(
      parse_messages::<I, A>(inner),
      Err(CodecError::Frame(_))
    ));
  }

  #[test]
  fn roundtrip_plain_ack() {
    let msg = ack_msg();
    let encoded = encode_outgoing(&msg, &EncodeOptions::default()).unwrap();
    let inner = decode_incoming(encoded, &DecodeOptions::default()).unwrap();
    let decoded: Message<I, A> = parse_message(inner).unwrap();
    match decoded {
      Message::Ack(ack) => assert_eq!(ack.sequence_number(), 7),
      other => panic!("expected Ack, got {other:?}"),
    }
  }

  #[test]
  fn roundtrip_with_label() {
    let label = Bytes::from_static(b"cluster-x");
    let opts = EncodeOptions::new(Some(label.clone()));
    let msg = ack_msg();
    let encoded = encode_outgoing(&msg, &opts).unwrap();

    // Outer bytes: [12][9][c l u s t e r - x][...]
    assert_eq!(encoded[0], 12, "first byte must be LABELED_TAG");
    assert_eq!(encoded[1], 9, "label length byte");
    assert_eq!(&encoded[2..11], b"cluster-x");

    let dec_opts = DecodeOptions::new(Some(label));
    let inner = decode_incoming(encoded, &dec_opts).unwrap();
    let decoded: Message<I, A> = parse_message(inner).unwrap();
    match decoded {
      Message::Ack(ack) => assert_eq!(ack.sequence_number(), 7),
      other => panic!("expected Ack, got {other:?}"),
    }
  }

  #[test]
  fn label_mismatch_errors() {
    let msg = ack_msg();
    let opts = EncodeOptions::new(Some(Bytes::from_static(b"a")));
    let encoded = encode_outgoing(&msg, &opts).unwrap();
    let dec_opts = DecodeOptions::new(Some(Bytes::from_static(b"b")));
    let result = decode_incoming(encoded, &dec_opts);
    assert!(matches!(result, Err(CodecError::LabelMismatch)));
  }

  #[test]
  fn missing_expected_label_errors() {
    let msg = ack_msg();
    let encoded = encode_outgoing(&msg, &EncodeOptions::default()).unwrap();
    let dec_opts = DecodeOptions::new(Some(Bytes::from_static(b"x")));
    let result = decode_incoming(encoded, &dec_opts);
    assert!(matches!(result, Err(CodecError::LabelMismatch)));
  }

  #[test]
  fn labeled_frame_with_no_expected_label_is_rejected() {
    // A frame explicitly labeled for cluster "other-cluster"
    // arriving at a node with NO inbound label configured must be
    // REJECTED (frozen memberlist-proto `double_label()`), not silently
    // unwrapped — otherwise the node accepts another cluster's traffic.
    let msg = ack_msg();
    let opts = EncodeOptions::new(Some(Bytes::from_static(b"other-cluster")));
    let encoded = encode_outgoing(&msg, &opts).unwrap();
    assert_eq!(encoded[0], 12, "precondition: frame is labeled");

    let result = decode_incoming(encoded, &DecodeOptions::new(None));
    assert!(
      matches!(result, Err(CodecError::DoubleLabel)),
      "labeled frame + no expected label must be DoubleLabel, got {result:?}"
    );
  }

  #[test]
  fn label_too_long_errors() {
    let long_label = Bytes::from(vec![b'x'; 256]);
    let opts = EncodeOptions::new(Some(long_label));
    let msg = ack_msg();
    let result = encode_outgoing(&msg, &opts);
    assert!(matches!(result, Err(CodecError::LabelTooLong(256))));
  }

  #[test]
  fn label_length_cap_is_253_not_255() {
    // frozen memberlist-proto caps labels at Label::MAX_SIZE
    // (u8::MAX - 2 = 253). 253 must encode; 254 must be rejected.
    let msg = ack_msg();
    let ok = encode_outgoing(
      &msg,
      &EncodeOptions::new(Some(Bytes::from(vec![b'x'; 253]))),
    );
    assert!(ok.is_ok(), "253-byte label must be accepted");
    let too_long = encode_outgoing(
      &msg,
      &EncodeOptions::new(Some(Bytes::from(vec![b'x'; 254]))),
    );
    assert!(
      matches!(too_long, Err(CodecError::LabelTooLong(254))),
      "254-byte label must be rejected, got {too_long:?}"
    );
  }

  #[test]
  fn empty_label_is_treated_as_no_label() {
    // Some(Bytes::new()) must behave exactly like None — no
    // `[12][0]` header on encode, and an empty expected label must accept
    // an ordinary unlabeled frame on decode (frozen memberlist-proto
    // semantics: header only when label_size > 0).
    let msg = ack_msg();
    let empty = EncodeOptions::new(Some(Bytes::new()));
    let enc_empty = encode_outgoing(&msg, &empty).unwrap();
    let enc_none = encode_outgoing(&msg, &EncodeOptions::default()).unwrap();
    assert_eq!(
      enc_empty, enc_none,
      "empty label must produce the same bytes as no label"
    );
    assert_ne!(enc_empty[0], LABELED_TAG, "no [12] header for empty label");

    // An empty expected label accepts an unlabeled frame.
    let inner = decode_incoming(enc_empty.clone(), &DecodeOptions::new(Some(Bytes::new())))
      .expect("empty expected label must accept unlabeled input");
    let decoded: Message<I, A> = parse_message(inner).unwrap();
    assert!(matches!(decoded, Message::Ack(_)));
  }

  #[test]
  fn empty_label_header_is_rejected() {
    // The frozen encoder never emits `[12][0]`; an incoming
    // empty-label header must be rejected, not accepted as "no label".
    let mut framed = vec![LABELED_TAG, 0];
    framed.extend_from_slice(&encode_outgoing(&ack_msg(), &EncodeOptions::default()).unwrap());
    let result = decode_incoming(Bytes::from(framed), &DecodeOptions::default());
    assert!(
      matches!(result, Err(CodecError::DoubleLabel)),
      "[12][0] must be rejected, got {result:?}"
    );
  }

  #[test]
  fn non_utf8_label_is_rejected_on_encode() {
    // frozen memberlist-proto::Label is validated UTF-8; never
    // emit `[12][len][0xff..]` a faithful decoder would reject.
    let opts = EncodeOptions::new(Some(Bytes::from_static(&[0xff, 0xfe, 0x00])));
    let result = encode_outgoing(&ack_msg(), &opts);
    assert!(
      matches!(result, Err(CodecError::InvalidLabel(_))),
      "non-UTF-8 label must be rejected on encode, got {result:?}"
    );
  }

  #[test]
  fn oversized_inbound_label_is_rejected_before_match() {
    // An inbound label header declaring > 253 bytes must be
    // rejected even if a (misconfigured) expected label would match it —
    // a faithful peer never sends one.
    let mut framed = vec![LABELED_TAG, 254u8];
    framed.extend_from_slice(&vec![b'x'; 254]);
    framed.extend_from_slice(&encode_outgoing(&ack_msg(), &EncodeOptions::default()).unwrap());
    // Would byte-match the inbound label.
    let dec = DecodeOptions::new(Some(Bytes::from(vec![b'x'; 254])));
    let result = decode_incoming(Bytes::from(framed), &dec);
    assert!(
      matches!(result, Err(CodecError::LabelTooLong(254))),
      "inbound 254-byte label must be rejected, got {result:?}"
    );
  }

  #[test]
  fn non_utf8_inbound_label_is_rejected() {
    // A non-UTF-8 inbound label is rejected before any match,
    // closing the "accepted if DecodeOptions.label matches" footgun.
    let mut framed = vec![LABELED_TAG, 2u8, 0xff, 0xfe];
    framed.extend_from_slice(&encode_outgoing(&ack_msg(), &EncodeOptions::default()).unwrap());
    // Byte-matches the inbound label.
    let dec = DecodeOptions::new(Some(Bytes::from_static(&[0xff, 0xfe])));
    let result = decode_incoming(Bytes::from(framed), &dec);
    assert!(
      matches!(result, Err(CodecError::InvalidLabel(_))),
      "non-UTF-8 inbound label must be rejected, got {result:?}"
    );
  }

  #[test]
  fn parse_message_rejects_trailing_datagram_bytes() {
    // A single UDP/QUIC datagram carries exactly one message.
    // A valid frame followed by extra bytes must be rejected, not
    // silently accepted (parity with the stream `consumed == total`).
    let encoded = encode_outgoing(&ack_msg(), &EncodeOptions::default()).unwrap();
    let mut smuggled = encoded.to_vec();
    smuggled.extend_from_slice(b"junk");
    let inner = decode_incoming(Bytes::from(smuggled), &DecodeOptions::default()).unwrap();
    match parse_message::<I, A>(inner) {
      Err(CodecError::TrailingData(c, t)) => {
        assert!(c < t, "consumed {c} must be < total {t}");
      }
      other => panic!("expected TrailingData, got {other:?}"),
    }
    // The clean single-message datagram still parses.
    let clean = decode_incoming(encoded, &DecodeOptions::default()).unwrap();
    assert!(matches!(parse_message::<I, A>(clean), Ok(Message::Ack(_))));
  }

  #[test]
  fn truncated_label_header_errors() {
    // Only the LABELED_TAG byte, no length byte.
    let result = decode_incoming(Bytes::from_static(&[12]), &DecodeOptions::default());
    assert!(matches!(result, Err(CodecError::Truncated(_))));
  }

  #[test]
  fn truncated_label_body_errors() {
    // Claims 5 label bytes, only 1 present.
    let result = decode_incoming(
      Bytes::from_static(&[12, 5, b'a']),
      &DecodeOptions::default(),
    );
    assert!(matches!(result, Err(CodecError::Truncated(_))));
  }

  #[test]
  fn empty_inner_after_label_errors() {
    // Bytes end exactly at the end of a 1-byte label: no inner frame.
    // An expected label is supplied so the DoubleLabel guard is passed
    // and the empty-inner-frame path is exercised (the point of this test).
    let result = decode_incoming(
      Bytes::from_static(&[12, 1, b'x']),
      &DecodeOptions::new(Some(Bytes::from_static(b"x"))),
    );
    assert!(matches!(result, Err(CodecError::Truncated(_))));
  }

  #[test]
  fn incomplete_inner_frame_is_distinguishable() {
    // A valid encoded Ack (no label), truncated so the varint length
    // promises more body than is present. This MUST surface as the
    // distinct `Incomplete` variant, not an opaque `Frame`.
    let msg = ack_msg();
    let encoded = encode_outgoing(&msg, &EncodeOptions::default()).unwrap();

    // Frame is [TAG][VARINT len][BODY]. Drop the final body byte so the
    // varint promises more body than is present: this is the `Incomplete`
    // (buffer-more-bytes) case, distinct from a corrupt `Frame`.
    assert!(encoded.len() > 1, "encoded Ack must have a non-empty body");
    let truncated = encoded.slice(0..encoded.len() - 1);
    let inner = decode_incoming(truncated.clone(), &DecodeOptions::default()).unwrap();
    assert_eq!(inner, truncated, "no-label path returns bytes unchanged");

    match parse_message::<SmolStr, SocketAddr>(inner) {
      Err(CodecError::Incomplete(_, _)) => {}
      other => panic!("expected CodecError::Incomplete, got {other:?}"),
    }
  }

  #[test]
  fn encode_options_builders_and_accessors() {
    // Default: no label, default (0 = 65507) max payload size.
    let def = EncodeOptions::default();
    assert!(def.label_ref().is_none());
    assert_eq!(def.max_payload_size(), 0);

    // `new` then the two `with_*` builders thread through to the accessors.
    let label = Bytes::from_static(b"cluster-x");
    let opts = EncodeOptions::new(None)
      .with_label(Some(label.clone()))
      .with_max_payload_size(1500);
    assert_eq!(opts.label_ref(), Some(&label));
    assert_eq!(opts.max_payload_size(), 1500);

    // with_label(None) clears it again.
    let cleared = opts.clone().with_label(None);
    assert!(cleared.label_ref().is_none());
    // The original is unaffected (builders consume-and-return a fresh value).
    assert_eq!(opts.max_payload_size(), 1500);
  }

  #[test]
  fn decode_options_accessor() {
    assert!(DecodeOptions::default().label_ref().is_none());
    let label = Bytes::from_static(b"y");
    let opts = DecodeOptions::new(Some(label.clone()));
    assert_eq!(opts.label_ref(), Some(&label));
  }

  #[test]
  fn decode_incoming_rejects_empty_input() {
    // No label expected, empty buffer: classify_header accepts 0 leading
    // bytes, then the inner-empty guard reports the "empty input" truncation
    // (consumed == 0 branch, distinct from the post-label empty-frame branch).
    let result = decode_incoming(Bytes::new(), &DecodeOptions::default());
    assert!(
      matches!(result, Err(CodecError::Truncated(_))),
      "got {result:?}"
    );
  }

  #[test]
  fn codec_error_display_strings_are_nonempty() {
    let cases = [
      CodecError::Bridge("b".to_string()),
      CodecError::Frame("f".to_string()),
      CodecError::Incomplete(3, 10),
      CodecError::LabelTooLong(300),
      CodecError::InvalidLabel("x"),
      CodecError::LabelMismatch,
      CodecError::DoubleLabel,
      CodecError::Truncated("t"),
      CodecError::TrailingData(4, 9),
    ];
    for e in &cases {
      assert!(!e.to_string().is_empty(), "empty display for {e:?}");
    }
    // The Incomplete display surfaces both byte counts.
    let s = CodecError::Incomplete(3, 10).to_string();
    assert!(s.contains('3') && s.contains("10"), "got {s}");
  }

  #[test]
  fn parse_messages_plain_frame_rejects_trailing_bytes() {
    // The plain-frame branch of parse_messages routes through parse_message,
    // so a trailing byte after a single framed message is TrailingData.
    let encoded = encode_outgoing(&ack_msg(), &EncodeOptions::default()).unwrap();
    let mut smuggled = encoded.to_vec();
    smuggled.push(0xAB);
    let inner = decode_incoming(Bytes::from(smuggled), &DecodeOptions::default()).unwrap();
    assert!(matches!(
      parse_messages::<I, A>(inner),
      Err(CodecError::TrailingData(_, _))
    ));
  }

  #[test]
  fn parse_message_maps_corrupt_frame_to_frame_error() {
    // A valid Ping tag + length but a garbage body decodes to a non-Incomplete
    // framing error, which parse_message maps to the opaque `Frame` variant
    // (the `other =>` arm of the decode map, distinct from `Incomplete`).
    let frame = Bytes::from(
      framing::encode_plain_frame(framing::MessageTag::Ping, &[0xFF, 0xFF, 0xFF, 0xFF])
        .expect("encode corrupt frame"),
    );
    assert!(matches!(
      parse_message::<I, A>(frame),
      Err(CodecError::Frame(_))
    ));
  }

  #[test]
  fn parse_messages_maps_truncated_compound_to_incomplete() {
    // A compound datagram truncated mid-part surfaces `decode_compound`'s
    // `Incomplete` framing error, which the compound branch of parse_messages
    // maps to `CodecError::Incomplete` (the leading map arm, not `other`).
    let batch = vec![ping_msg(), ack_msg()];
    let encoded = encode_outgoing_compound(&batch, &EncodeOptions::default()).unwrap();
    // Drop the last byte so the final part's body is short of its declared len.
    let truncated = encoded.slice(0..encoded.len() - 1);
    assert!(matches!(
      parse_messages::<I, A>(truncated),
      Err(CodecError::Incomplete(_, _))
    ));
  }

  /// Build a compound datagram `[Compound][count=2][len0][part0][len1][part1]`
  /// from two raw part-byte payloads (each is a whole inner frame).
  fn compound_of(part0: &[u8], part1: &[u8]) -> Bytes {
    use framing::{MessageTag, encode_varint_u32};
    let mut buf = std::vec![MessageTag::Compound as u8];
    encode_varint_u32(2, &mut buf);
    encode_varint_u32(part0.len() as u32, &mut buf);
    buf.extend_from_slice(part0);
    encode_varint_u32(part1.len() as u32, &mut buf);
    buf.extend_from_slice(part1);
    Bytes::from(buf)
  }

  #[test]
  fn parse_messages_maps_corrupt_compound_part_to_frame_error() {
    // A structurally valid compound (honest count + part lengths) whose SECOND
    // part is a frame with an unknown message tag: `decode_compound` yields the
    // part slices, then the per-part `decode_message_zerocopy` rejects the bad
    // tag — a non-Incomplete framing error the loop maps to `Frame` (the
    // `other =>` arm inside the per-part decode).
    let good = framing::encode_message(&framing::AnyMessage::Ack(
      crate::messages::memberlist::v1::Ack {
        sequence_number: 5,
        ..Default::default()
      },
    ))
    .expect("encode good part");
    // Tag 0 is not a valid MessageTag.
    let bad_tag_part = [0u8, 0u8];
    let raw = compound_of(&good, &bad_tag_part);
    assert!(matches!(
      parse_messages::<I, A>(raw),
      Err(CodecError::Frame(_))
    ));
  }

  #[test]
  fn parse_messages_maps_incomplete_compound_part_to_incomplete() {
    // A compound whose second part's inner frame declares more body than the
    // part slice carries surfaces the per-part `Incomplete` arm (distinct from
    // a whole-datagram truncation): the part length is honest, but the frame
    // inside it is short.
    let good = framing::encode_message(&framing::AnyMessage::Ack(
      crate::messages::memberlist::v1::Ack {
        sequence_number: 5,
        ..Default::default()
      },
    ))
    .expect("encode good part");
    // [Ping tag=2][body_len=10][only 2 body bytes] — a 4-byte part whose frame
    // promises 10 body bytes.
    let short_frame_part = [framing::MessageTag::Ping as u8, 10u8, 0xAA, 0xBB];
    let raw = compound_of(&good, &short_frame_part);
    assert!(matches!(
      parse_messages::<I, A>(raw),
      Err(CodecError::Incomplete(_, _))
    ));
  }

  #[test]
  fn parse_messages_maps_compound_part_with_trailing_bytes_to_trailing_data() {
    // A compound part whose declared inner_len exceeds the frame it contains
    // (valid frame + trailing bytes inside the same part) trips the per-part
    // `consumed != part.len()` guard ⇒ TrailingData.
    let good = framing::encode_message(&framing::AnyMessage::Ack(
      crate::messages::memberlist::v1::Ack {
        sequence_number: 5,
        ..Default::default()
      },
    ))
    .expect("encode good part");
    // A complete Ack frame followed by an extra byte, all inside one part.
    let mut part_with_trailing = good.clone();
    part_with_trailing.push(0xFF);
    let raw = compound_of(&good, &part_with_trailing);
    assert!(matches!(
      parse_messages::<I, A>(raw),
      Err(CodecError::TrailingData(_, _))
    ));
  }
}
