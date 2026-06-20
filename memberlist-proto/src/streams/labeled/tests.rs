use super::*;
use crate::{
  Instant,
  label::{LABEL_OVERHEAD, LABELED_TAG},
};

type Tcp = Labeled<Passthrough>;

fn label(s: &str) -> Option<Vec<u8>> {
  Some(s.as_bytes().to_vec())
}

fn now() -> Instant {
  Instant::now()
}

// ── outbound prefix + passthrough ──────────────────────────────────────────

#[test]
fn dialer_emits_label_prefix_then_passes_plaintext_through() {
  let mut r = Tcp::dialer(label("cluster-x"), false);
  // A dialer is never handshaking (bridge mint gate); its inbound label is
  // validated in-line on the established intake instead.
  assert!(!r.is_handshaking());
  r.write_plaintext(b"hello-frame");
  let mut out = Vec::new();
  r.poll_transport_transmit(&mut out);
  assert_eq!(out[0], 12);
  assert_eq!(out[1] as usize, "cluster-x".len());
  assert_eq!(&out[2..2 + 9], b"cluster-x");
  assert_eq!(&out[2 + 9..], b"hello-frame");
}

#[test]
fn acceptor_does_not_emit_outbound_label_until_inbound_validated() {
  // The acceptor's `[12][len][label]` is queued LAZILY at inbound-label
  // validation, not at construction: a handshaking acceptor reveals nothing
  // on the wire until its peer has proven cluster identity, faithful to
  // `memberlist-core/src/network.rs::handle_conn`'s
  // `read_message`-before-`send_message` ordering. A wrong-cluster /
  // slow-loris / scanner peer that completes only the TCP handshake never
  // learns the local cluster label.
  let mut r = Tcp::acceptor(label("cluster-x"), false);
  assert!(r.is_handshaking(), "acceptor starts handshaking");
  let mut pre = Vec::new();
  let pre_n = r.poll_transport_transmit(&mut pre);
  assert_eq!(
    pre_n, 0,
    "a handshaking acceptor must not queue its outbound label before \
       inbound validation"
  );
  assert!(pre.is_empty(), "no bytes leaked pre-validation");

  // Feed a matching inbound label so validation completes; the lazy queue
  // fires inside the gate's accepted branch and the acceptor's reply prefix
  // becomes available on the next drain.
  let mut input = vec![12u8, 9];
  input.extend_from_slice(b"cluster-x");
  assert!(matches!(
    r.handle_transport_data(&input, now()),
    Intake::Done
  ));
  assert!(!r.is_handshaking(), "inbound validated, handshake done");

  let mut post = Vec::new();
  let post_n = r.poll_transport_transmit(&mut post);
  assert_eq!(
    post_n,
    LABEL_OVERHEAD + "cluster-x".len(),
    "lazy queue fired: acceptor's outbound label is now available"
  );
  assert_eq!(post[0], LABELED_TAG);
  assert_eq!(post[1] as usize, "cluster-x".len());
  assert_eq!(&post[2..2 + 9], b"cluster-x");
}

#[test]
fn acceptor_validates_matching_label_then_surfaces_tail_plaintext() {
  let mut r = Tcp::acceptor(label("cluster-x"), false);
  assert!(r.is_handshaking());
  let mut input = vec![12u8, 9];
  input.extend_from_slice(b"cluster-x");
  input.extend_from_slice(b"req-bytes");
  assert!(matches!(
    r.handle_transport_data(&input, now()),
    Intake::Done
  ));
  assert!(!r.is_handshaking());
  let mut got = Vec::new();
  r.read_plaintext(&mut got);
  assert_eq!(&got, b"req-bytes");
}

#[test]
fn acceptor_rejects_mismatched_label() {
  let mut r = Tcp::acceptor(label("cluster-x"), false);
  let mut input = vec![12u8, 5];
  input.extend_from_slice(b"other");
  assert!(matches!(
    r.handle_transport_data(&input, now()),
    Intake::Failed
  ));
}

#[test]
fn dialer_validates_matching_inbound_label_then_surfaces_response_plaintext() {
  // The dialer is not handshaking for the bridge-mint gate, but the inbound
  // label is still validated in-line by `handle_transport_data` before any
  // response bytes can reach the `Stream`. Feed the dialer a
  // `[12][len][label][response_bytes]` flight and the post-label tail must
  // surface as plaintext with no label leakage.
  let mut r = Tcp::dialer(label("cluster-x"), false);
  let mut input = vec![12u8, 9];
  input.extend_from_slice(b"cluster-x");
  input.extend_from_slice(b"response_bytes");
  assert!(matches!(
    r.handle_transport_data(&input, now()),
    Intake::Done
  ));
  let mut got = Vec::new();
  r.read_plaintext(&mut got);
  assert_eq!(
    &got, b"response_bytes",
    "the dialer surfaces only the post-label tail (no leakage of the [12][len][label] header)"
  );
}

#[test]
fn dialer_rejects_mismatched_inbound_label_prefix() {
  // A wrong-cluster responder with `skip_inbound_label_check = true` could
  // accept the dialer's labeled request, then respond with a DIFFERENT
  // label. Without bidirectional validation the dialer would happily merge
  // the response — the cross-cluster footgun. The dialer must reject.
  let mut r = Tcp::dialer(label("cluster-x"), false);
  let mut input = vec![12u8, 5];
  input.extend_from_slice(b"other");
  assert!(matches!(
    r.handle_transport_data(&input, now()),
    Intake::Failed
  ));
}

#[test]
fn dialer_rejects_missing_but_expected_inbound_label() {
  // The default policy (skip_inbound_label_check = false) requires the
  // responder to label its reply when the dialer is configured with a
  // label — an unlabeled response is rejected.
  let mut r = Tcp::dialer(label("cluster-x"), false);
  assert!(matches!(
    r.handle_transport_data(b"response_bytes", now()),
    Intake::Failed
  ));
}

#[test]
fn dialer_skip_inbound_label_check_accepts_unlabeled_response() {
  // With the inbound check suppressed, the dialer accepts an unlabeled
  // response — symmetric with the acceptor's same suppression — exactly as
  // `memberlist-core::options::skip_inbound_label_check` flows through to
  // `reliable_encoder`/`read_message` on both sides.
  let mut r = Tcp::dialer(label("cluster-x"), true);
  assert!(matches!(
    r.handle_transport_data(b"response_bytes", now()),
    Intake::Done
  ));
  let mut got = Vec::new();
  r.read_plaintext(&mut got);
  assert_eq!(&got, b"response_bytes");
}

// ── partial-header buffering ───────────────────────────────────────────────

#[test]
fn acceptor_buffers_partial_label_then_completes() {
  let mut r = Tcp::acceptor(label("cluster-x"), false);
  // `[12][9]["clus"]` — header declares 9 label bytes, only 4 present.
  let partial = [12u8, 9, b'c', b'l', b'u', b's'];
  assert!(matches!(
    r.handle_transport_data(&partial, now()),
    Intake::Done
  ));
  assert!(
    r.is_handshaking(),
    "still waiting for the rest of the label"
  );
  // The remaining label bytes complete the header.
  assert!(matches!(
    r.handle_transport_data(b"ter-x", now()),
    Intake::Done
  ));
  assert!(!r.is_handshaking(), "label complete, handshake done");
}

#[test]
fn acceptor_buffers_partial_label_then_completes_with_trailing_plaintext() {
  let mut r = Tcp::acceptor(label("cluster-x"), false);
  let partial = [12u8, 9, b'c', b'l', b'u', b's'];
  assert!(matches!(
    r.handle_transport_data(&partial, now()),
    Intake::Done
  ));
  assert!(r.is_handshaking());
  // Rest of the label plus the first plaintext bytes in one read.
  let mut rest = Vec::new();
  rest.extend_from_slice(b"ter-x");
  rest.extend_from_slice(b"req-bytes");
  assert!(matches!(
    r.handle_transport_data(&rest, now()),
    Intake::Done
  ));
  assert!(!r.is_handshaking());
  let mut got = Vec::new();
  r.read_plaintext(&mut got);
  assert_eq!(&got, b"req-bytes", "tail after the label is plaintext");
}

// ── out-of-band close / no-op surface ──────────────────────────────────────

#[test]
fn peer_close_is_out_of_band_only() {
  // A dialer with no label: passthrough, no in-band close signal.
  let mut r = Tcp::dialer(None, false);
  assert!(!r.is_handshaking());
  assert!(!r.peer_has_closed(), "TCP FIN is out of band, not in-band");
  r.send_close_notify(); // harmless no-op
  assert!(
    !r.peer_has_closed(),
    "send_close_notify changes nothing here"
  );
  // Passthrough still works after the no-op close.
  r.write_plaintext(b"frame");
  let mut out = Vec::new();
  r.poll_transport_transmit(&mut out);
  assert_eq!(&out, b"frame");
}

#[test]
fn skip_inbound_label_check_accepts_unlabeled_when_label_expected() {
  // A label is configured, but the inbound check is suppressed: an unlabeled
  // inbound (first byte != 12) is accepted as plaintext rather than rejected
  // for a missing-but-expected label.
  let mut r = Tcp::acceptor(label("cluster-x"), true);
  assert!(r.is_handshaking());
  assert!(matches!(
    r.handle_transport_data(b"req-bytes", now()),
    Intake::Done
  ));
  assert!(!r.is_handshaking());
  let mut got = Vec::new();
  r.read_plaintext(&mut got);
  assert_eq!(&got, b"req-bytes");
}

// ── over-long / non-UTF-8 rejects (still apply when a label IS configured) ──

#[test]
fn acceptor_rejects_over_long_declared_label_length() {
  // A declared label length above MAX_LABEL_LEN (253) is rejected as soon as
  // the two-byte `[tag][len]` prefix is buffered — the guard fires before
  // the `2 + len` completeness check, so no trailing bytes are required.
  // Values 254 and 255 are the only u8 lengths above the cap; use 254 here.
  //
  // The acceptor is configured with a normal expected label. This proves the
  // over-long guard fires before any match attempt: even if trailing bytes
  // were provided that matched the configured label, the count can never
  // become valid, so rejection is immediate.
  let mut r = Tcp::acceptor(label("cluster-x"), false);
  // `[12][254]` — two bytes are sufficient for the guard to fire.
  let input = [LABELED_TAG, 254u8];
  assert!(matches!(
    r.handle_transport_data(&input, now()),
    Intake::Failed
  ));
}

#[test]
fn acceptor_rejects_non_utf8_label_before_match() {
  // A non-UTF-8 inbound label is rejected before any comparison with the
  // configured label — this closes the "accepted if bytes happen to match"
  // footgun. The acceptor is deliberately constructed with the same raw
  // non-UTF-8 bytes as the inbound label, proving "reject before match":
  // even though `inbound[2..4] == expected[..]` byte-for-byte, the UTF-8
  // validity check fires first and returns `Intake::Failed`.
  let mut r = Tcp::acceptor(Some(vec![0xff, 0xfe]), false);
  // `[12][2][0xff][0xfe]` — fully-buffered header with two-byte label that
  // byte-matches the configured bytes but is not valid UTF-8.
  let input = [LABELED_TAG, 2u8, 0xff, 0xfe];
  assert!(matches!(
    r.handle_transport_data(&input, now()),
    Intake::Failed
  ));
}

// ── unlabeled node never runs the [12] classifier on the stream ────────────

#[test]
fn unlabeled_acceptor_passes_through_a_12_byte_first_unit() {
  // The latent-bug regression. On the reliable plane the bytes that follow
  // an unlabeled node's (absent) label prefix are the bridge's
  // `[unit_len][reliable_unit]` framing. A reliable unit whose own length is
  // 12 leads with the byte `0x0C` (== LABELED_TAG). An unlabeled node must
  // NOT classify that first byte as a label tag: no Rejected, no
  // Incomplete-stall, and the bytes flow through unchanged.
  let mut r = Tcp::acceptor(None, false);
  // An unlabeled acceptor is never handshaking — the gate is pure
  // passthrough from byte 0, so the bridge mints the Stream immediately.
  assert!(
    !r.is_handshaking(),
    "an unlabeled node does not gate the mint on a label step"
  );
  // `[12][...12 payload bytes...]` — a 12-byte reliable unit. The leading
  // `0x0C` is the unit length, NOT a label tag.
  let mut first_unit = vec![12u8];
  first_unit.extend_from_slice(b"ABCDEFGHIJKL"); // exactly 12 payload bytes
  assert!(
    matches!(r.handle_transport_data(&first_unit, now()), Intake::Done),
    "the 0x0C-led first unit is passed through, not classified as a label"
  );
  // No outbound label prefix is ever queued by an unlabeled node.
  let mut out = Vec::new();
  assert_eq!(
    r.poll_transport_transmit(&mut out),
    0,
    "an unlabeled node emits no outbound label prefix"
  );
  // Every byte flows through unchanged — the unit length byte and its
  // payload are surfaced verbatim as plaintext.
  let mut got = Vec::new();
  r.read_plaintext(&mut got);
  assert_eq!(
    &got, &first_unit,
    "the 0x0C-led unit surfaced byte-for-byte (length prefix + payload)"
  );
}

#[test]
fn unlabeled_acceptor_passes_through_a_labeled_looking_inbound() {
  // An unlabeled node passes a `[12]...`-led inbound straight through as
  // plaintext rather than rejecting it as a double label. The double-label
  // reject is a GOSSIP-plane concern (`memberlist/src/codec.rs`); on the
  // reliable stream an unlabeled node cannot distinguish a label tag from a
  // 12-byte reliable unit's length byte, so it must not classify byte 0 at
  // all. `skip_inbound_label_check` is irrelevant with no local label.
  for skip in [false, true] {
    let mut r = Tcp::acceptor(None, skip);
    assert!(!r.is_handshaking());
    let mut input = vec![12u8, 9];
    input.extend_from_slice(b"cluster-x");
    input.extend_from_slice(b"req-bytes");
    assert!(
      matches!(r.handle_transport_data(&input, now()), Intake::Done),
      "unlabeled passthrough (skip={skip}) never rejects a [12]-led inbound on the stream",
    );
    let mut got = Vec::new();
    r.read_plaintext(&mut got);
    assert_eq!(
      &got, &input,
      "the whole [12]-led inbound surfaced verbatim (skip={skip})",
    );
  }
}

#[test]
fn unlabeled_dialer_emits_no_prefix_and_passes_through() {
  // The dialer mirror of the unlabeled passthrough: no eager label prefix is
  // queued, and an inbound response (even a `[12]`-led one) flows through.
  let mut r = Tcp::dialer(None, false);
  r.write_plaintext(b"request");
  let mut out = Vec::new();
  r.poll_transport_transmit(&mut out);
  assert_eq!(
    &out, b"request",
    "an unlabeled dialer writes its request with no label prefix",
  );
  let mut resp = vec![12u8];
  resp.extend_from_slice(b"ABCDEFGHIJKL");
  assert!(matches!(
    r.handle_transport_data(&resp, now()),
    Intake::Done
  ));
  let mut got = Vec::new();
  r.read_plaintext(&mut got);
  assert_eq!(&got, &resp, "the 0x0C-led response surfaced verbatim");
}

// ── clear_outbound drops the queued prefix ─────────────────────────────────

#[test]
fn clear_outbound_drops_the_dialer_eager_prefix() {
  // The dialer queues its `[12][len][label]` prefix eagerly at construction.
  // `clear_outbound` (the bridge's failure-retirement step) must drop it so a
  // failed exchange never leaks the local label on the reap path.
  let mut r = Tcp::dialer(label("cluster-x"), false);
  r.clear_outbound();
  let mut out = Vec::new();
  assert_eq!(
    r.poll_transport_transmit(&mut out),
    0,
    "the eager label prefix was cleared before it reached the wire"
  );
  assert!(out.is_empty());
}

// ── LabelOptions construction + validation ─────────────────────────────────

#[test]
fn label_options_carries_label_and_check_policy() {
  let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
  assert_eq!(cfg.label(), Some(b"cluster-x".as_slice()));
  assert!(!cfg.is_skip_inbound_label_check());
  let cfg = LabelOptions::<()>::new_in(None, ()).skip_inbound_label_check();
  assert_eq!(cfg.label(), None);
  assert!(cfg.is_skip_inbound_label_check());
}

#[test]
fn label_options_rejects_overlong_label() {
  let too_long = vec![b'x'; 254];
  assert!(LabelOptions::<()>::try_new_in(Some(too_long), ()).is_err());
}

#[test]
fn label_options_empty_normalizes_to_none() {
  let cfg = LabelOptions::<()>::try_new_in(Some(Vec::new()), ()).unwrap();
  assert_eq!(cfg.label(), None);
}

#[test]
fn label_options_rejects_non_utf8_label() {
  assert!(LabelOptions::<()>::try_new_in(Some(vec![0xff, 0xfe]), ()).is_err());
}

/// `LabelOptionsError` is the single error type for label-validation failures
/// on `LabelOptions::try_new_in`.
#[test]
fn label_options_error_variants_are_exhaustive() {
  let too_long = vec![b'x'; 254];
  let e = LabelOptions::<()>::try_new_in(Some(too_long), ()).unwrap_err();
  assert!(matches!(e, LabelOptionsError::LabelTooLong));
  let e = LabelOptions::<()>::try_new_in(Some(vec![0xff, 0xfe]), ()).unwrap_err();
  assert!(matches!(e, LabelOptionsError::InvalidLabel));
}
