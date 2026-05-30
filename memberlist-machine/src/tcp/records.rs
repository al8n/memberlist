//! Raw-passthrough records codec for the plain-TCP coordinator.
//!
//! Replaces the rustls TLS record layer used by the TLS coordinator with a
//! label-prefix-then-raw-passthrough design: a one-time
//! `[LABELED_TAG=12][len][label]` frame is written once at stream start by
//! BOTH sides, then subsequent bytes are passed through verbatim.
//!
//! [`RawRecords`] mirrors the bridge-facing surface of
//! [`crate::TlsRecords`] byte-for-byte — same constructors,
//! `handle_transport_data` / `poll_transport_transmit` /
//! `read_plaintext` / `write_plaintext` / `is_handshaking` /
//! `send_close_notify` / `peer_has_closed` — so the [`crate::tcp`] bridge and
//! a future generic extraction over "a Sans-I/O record unit" stay identical
//! across the two transports. The deltas are the absence of crypto: there is
//! no encryption, so application bytes are queued/surfaced verbatim and the
//! only "handshake" is the inbound label read.
//!
//! The label is purely a cluster routing / boundary marker, byte-compatible
//! with the frozen `memberlist-proto` label frame and the umbrella
//! `memberlist::codec` outer-label rules: `[LABELED_TAG=12][len:u8][label]`,
//! with an empty label meaning "no label" (never emitted as `[12][0]`).
//!
//! # Bidirectional label on the reliable path
//!
//! Memberlist's reliable-stream encoder applies `.with_label(...)` on BOTH
//! send paths: the dialer's request from `state.rs::push_pull_node` and the
//! acceptor's response from `network.rs::send_message` (the push/pull reply,
//! the reliable-ping ack, and the error-response path) both flow through
//! `reliable_encoder` (`memberlist-core/src/network.rs`), which always
//! `.with_label`s the local label. The matching `read_message`
//! (`memberlist-core/src/network.rs`) sets `decoder.with_label(...)` unless
//! `skip_inbound_label_check` is set — so both peers validate the inbound
//! label on every reliable-stream read (`memberlist/src/codec.rs` is the
//! truth table). Cluster identity is therefore a soft-tag enforced in BOTH
//! directions on the reliable path: a wrong-cluster responder that has
//! `skip_inbound_label_check = true` cannot trick a dialer into merging its
//! state by responding with a different (or absent) label.
//!
//! Each role queues its own outbound `[12][len][label]` prefix once and runs
//! the shared inbound-label validation on `handle_transport_data` until its
//! inbound label is settled. The queue timing is asymmetric: the dialer
//! queues its outbound label EAGERLY at construction (classic memberlist
//! sends `[label][request]` together — the initiator reveals its cluster
//! identity in the same wire write as its first request), while the acceptor
//! queues its outbound label LAZILY at the moment its INBOUND label is
//! validated. A handshaking acceptor therefore has an EMPTY outbound buffer
//! until the dialer has proven cluster identity, so a wrong-cluster /
//! slow-loris / scanner peer that completes only the TCP handshake never
//! learns the local cluster label — faithful to
//! `memberlist-core/src/network.rs::handle_conn`, where the acceptor's reply
//! (`send_message` → `reliable_encoder().with_label(...)`) runs only after
//! `read_message` has validated the inbound label. The acceptor's
//! `is_handshaking()` returns `true` until the inbound label is validated,
//! gating the bridge's `Stream` mint; the dialer's `is_handshaking()` is
//! always `false` (no round-trip wait before the dialer can transmit its
//! request bytes), and the dialer's inbound label validation runs in-line on
//! the established bridge's response read.

use crate::Instant;
#[cfg(not(feature = "std"))]
use std::vec::Vec;

/// Outer label tag byte. Byte-compatible with the frozen `memberlist-proto`
/// label frame and `memberlist::codec` (`LABELED_TAG = 12`).
const LABELED_TAG: u8 = 12;

/// Label-header overhead: the tag byte plus the 1-byte length.
const LABEL_OVERHEAD: usize = 2;

/// Maximum encodable/acceptable label length. Faithful to the frozen
/// `memberlist-proto` `Label::MAX_SIZE` (`u8::MAX - 2 = 253`): the label and
/// its 2-byte `[tag][len]` header share one frame, so 253 — not 255 — is the
/// real cap (`memberlist::codec::MAX_LABEL_LEN`).
const MAX_LABEL_LEN: usize = u8::MAX as usize - 2;

/// Side of the exchange this record unit serves. The role gates the
/// bridge-mint semantics of [`RawRecords::is_handshaking`]: an acceptor
/// stays handshaking until the inbound label is validated (so the bridge
/// withholds the `Stream` mint until the cluster is confirmed right); a
/// dialer is never handshaking (classic memberlist sends `[label][request]`
/// together, so the bridge mints its `Stream` immediately and validates the
/// inbound response label in-line on the established intake).
enum Role {
  Dialer,
  Acceptor,
}

/// One side of a plain-TCP exchange, behind the same byte-only Sans-I/O
/// interface as [`crate::TlsRecords`]. Accessor-only; no `pub` fields. Built
/// via the crate-internal `dialer` / `acceptor` constructors.
///
/// There is no transport-level encryption: the outbound buffer carries the
/// one-time label prefix followed by raw application bytes, and the inbound
/// side strips the one-time label then surfaces the rest verbatim as
/// plaintext. The dialer queues its outbound label EAGERLY at construction
/// (the initiator sends `[label][request]` together, faithful to
/// `memberlist-core/src/state.rs::push_pull_node`'s
/// `reliable_encoder().with_label(...)`); the acceptor queues its outbound
/// label LAZILY at the moment its inbound label is validated, so a
/// handshaking acceptor never reveals the local cluster label to a peer that
/// has only completed the TCP handshake (faithful to
/// `memberlist-core/src/network.rs::handle_conn`, where `read_message`
/// validates the inbound label before `send_message` runs its
/// `reliable_encoder().with_label(...)` on the reply).
pub struct RawRecords {
  /// The configured cluster label, normalized so an empty label is `None`
  /// ("no label"), exactly as `memberlist::codec::effective_label` treats it.
  label: Option<Vec<u8>>,
  /// When `true`, an absent-but-expected inbound label is accepted as
  /// unlabeled instead of rejected. Faithful to memberlist-core
  /// `Options::skip_inbound_label_check` (options.rs): the suppression covers
  /// only the missing-label mismatch — a present `[12]` header with no local
  /// label is still rejected (`DoubleLabel`).
  skip_inbound_label_check: bool,
  /// Which side of the exchange this record unit serves. Gates the
  /// bridge-mint semantics of [`Self::is_handshaking`].
  role: Role,
  /// Bytes to hand to the transport: the one-time label prefix (eager for
  /// the dialer, lazy on inbound-label validation for the acceptor — see the
  /// struct docs), then raw application plaintext appended by
  /// [`Self::write_plaintext`]. Drained by [`Self::poll_transport_transmit`].
  outbound: Vec<u8>,
  /// `true` once the local label prefix has been pushed into
  /// [`Self::outbound`], so the lazy queue helper does not double-queue.
  /// Started `false` for both roles; the dialer flips it inside its
  /// constructor (eager queue), the acceptor flips it the moment its
  /// inbound label is validated (lazy queue — see
  /// [`Self::queue_outbound_label_if_needed`]).
  outbound_label_queued: bool,
  /// Decrypted-equivalent application bytes ready for the bridge, drained by
  /// [`Self::read_plaintext`]. Raw passthrough, so this is just the inbound
  /// tail after the one-time inbound label has been consumed.
  inbound_plaintext: Vec<u8>,
  /// Accumulation for the one-time inbound label header: a coalesced
  /// transport read can split the `[12][len][label]` header, so partial
  /// header bytes are buffered here until the full header is present. Unused
  /// once `inbound_label_validated` is set (plaintext goes straight to
  /// `inbound_plaintext`).
  ///
  /// Bounded at `LABEL_OVERHEAD + MAX_LABEL_LEN` (≤ 255 bytes): the
  /// `len > MAX_LABEL_LEN` guard rejects a declared over-long header as
  /// soon as the two-byte `[tag][len]` prefix arrives, so the buffer never
  /// holds more than that — no unbounded-growth / slow-loris-buffer risk in
  /// `RawRecords` itself.
  inbound_raw: Vec<u8>,
  /// `true` once the inbound label has been read and validated. Both roles
  /// start with `false` and run the shared inbound-label validation on
  /// `handle_transport_data` until this flips; afterwards intake is pure
  /// passthrough.
  inbound_label_validated: bool,
}

/// Outcome of one [`RawRecords::handle_transport_data`] intake step.
///
/// Mirrors the [`crate::tls::records::Intake`] shape so the bridge handles
/// both transports identically. The raw passthrough has no crypto
/// backpressure, so it only ever returns [`Intake::Done`] (all input
/// consumed/buffered) or [`Intake::Failed`] (the one-time inbound label was
/// rejected). [`Intake::Pending`] exists solely for surface parity with the
/// TLS record unit, where rustls's bounded received-plaintext buffer forces
/// drain-and-re-feed; it is never produced here.
pub(crate) enum Intake {
  /// All supplied bytes were consumed: buffered as the partial inbound label,
  /// or surfaced as plaintext.
  Done,
  /// Surface parity with the TLS record unit only — never produced by the raw
  /// passthrough (the carried count would be bytes consumed before
  /// backpressure). See the enum docs.
  #[allow(dead_code)]
  Pending(usize),
  /// The one-time inbound label was rejected (mismatch, an unexpected
  /// `[12]` double-label header, an over-long length, or a non-UTF-8 label).
  /// The bridge tears the exchange down — there is no recovery.
  Failed,
}

/// Outcome of validating the one-time inbound label header against the
/// configured expectation. Internal to [`RawRecords::handle_transport_data`].
enum LabelOutcome {
  /// Header parsed and accepted; the carried count is the number of leading
  /// bytes (`[12][len][label]`, or `0` for an accepted unlabeled inbound) to
  /// drop before the plaintext tail.
  Accepted(usize),
  /// Not enough bytes yet to decide; keep buffering and stay handshaking.
  Incomplete,
  /// The header is present but invalid (mismatch / double-label / over-long /
  /// non-UTF-8). Maps to [`Intake::Failed`].
  Rejected,
}

impl RawRecords {
  /// Dialer role: queues the one-time outbound label prefix (when a non-empty
  /// label is configured) EAGERLY at construction so the request that
  /// follows it is labeled on the same wire write, exactly like
  /// `memberlist-core/src/state.rs::push_pull_node`'s
  /// `send_local_state` → `reliable_encoder().with_label(...)`. The dialer
  /// is never handshaking — classic memberlist sends `[label][request]`
  /// together, so the bridge mints its `Stream` and the dialer's outbound
  /// request flows immediately — but the dialer's INBOUND response is still
  /// label-validated in-line by [`Self::handle_transport_data`]: a
  /// wrong-cluster responder with `skip_inbound_label_check = true` would
  /// otherwise be able to merge state into a dialer that never checked back.
  ///
  /// An empty/`None` label emits no prefix (never a `[12][0]` header),
  /// exactly as the frozen `memberlist-proto` encoder and `memberlist::codec`
  /// only write a header when the label is non-empty.
  pub(crate) fn dialer(label: Option<Vec<u8>>, skip_inbound_label_check: bool) -> Self {
    let label = effective_label(label);
    let mut this = Self {
      label,
      skip_inbound_label_check,
      role: Role::Dialer,
      outbound: Vec::new(),
      outbound_label_queued: false,
      inbound_plaintext: Vec::new(),
      inbound_raw: Vec::new(),
      inbound_label_validated: false,
    };
    // Eager queue: the dialer reveals its cluster identity in the same wire
    // write as its first request, matching classic memberlist's
    // `state.rs::push_pull_node`.
    this.queue_outbound_label_if_needed();
    this
  }

  /// Acceptor role: starts with an EMPTY outbound buffer; the one-time
  /// outbound label prefix is queued LAZILY by
  /// [`Self::queue_outbound_label_if_needed`] at the moment the inbound
  /// label is validated, so a handshaking acceptor never reveals the local
  /// cluster label to a peer that has only completed the TCP handshake
  /// (a wrong-cluster scanner, a slow-loris that connects then stalls, an
  /// attacker probing for cluster identity). Faithful to
  /// `memberlist-core/src/network.rs::handle_conn`, where `read_message`
  /// validates the inbound label before `send_message` runs its
  /// `reliable_encoder().with_label(...)` on the reply.
  ///
  /// Stays handshaking until the one-time inbound label is read and
  /// validated; `skip_inbound_label_check` suppresses only the
  /// missing-but-expected-label mismatch (an unlabeled inbound is then
  /// accepted), exactly as memberlist-core's option does. A present `[12]`
  /// header with no configured label is still rejected as a double label.
  pub(crate) fn acceptor(label: Option<Vec<u8>>, skip_inbound_label_check: bool) -> Self {
    let label = effective_label(label);
    Self {
      label,
      skip_inbound_label_check,
      role: Role::Acceptor,
      outbound: Vec::new(),
      outbound_label_queued: false,
      inbound_plaintext: Vec::new(),
      inbound_raw: Vec::new(),
      inbound_label_validated: false,
    }
  }

  /// Queue the local label prefix `[12][len][label]` into the outbound
  /// buffer if a label is configured and we have not already queued one.
  /// Idempotent: a second call after the first is a no-op (the flag pins it).
  ///
  /// Used by:
  /// * [`Self::dialer`] — eager queue inside the constructor, so the
  ///   dialer's first request flows on the wire with its label prefix.
  /// * [`Self::handle_transport_data`]'s `Accepted` branch — lazy queue at
  ///   the moment the inbound label has been validated, so the acceptor
  ///   responds with its labeled reply but does not leak the label to a
  ///   peer that fails (or never reaches) the inbound-label step.
  ///
  /// An empty/`None` label emits no prefix; the flag is still flipped so
  /// the helper stays idempotent across both code paths.
  fn queue_outbound_label_if_needed(&mut self) {
    if self.outbound_label_queued {
      return;
    }
    if let Some(l) = self.label.as_deref() {
      encode_label_prefix(l, &mut self.outbound);
    }
    self.outbound_label_queued = true;
  }

  /// Feed bytes the driver read from the transport.
  ///
  /// Once the one-time inbound label has been read and validated
  /// (`inbound_label_validated`), this is pure passthrough: all `input` is
  /// appended to the inbound plaintext buffer and [`Intake::Done`] is
  /// returned (no crypto, so never [`Intake::Pending`]).
  ///
  /// While the inbound label is still being read, `input` is accumulated and
  /// the `[12][len][label]` header is validated as soon as it is complete: a
  /// complete, matching (or skip-accepted unlabeled) header clears the
  /// pre-validation state and surfaces any trailing bytes as plaintext
  /// ([`Intake::Done`]); an incomplete header keeps buffering ([`Intake::Done`]
  /// — wait for more); an invalid header yields [`Intake::Failed`]. The
  /// validation runs identically for both roles (cited as bidirectional in
  /// the module docs); the bridge's `is_handshaking` gate only governs the
  /// `Stream` mint timing.
  ///
  /// `now` is accepted for surface parity with the TLS record unit and a
  /// uniform bridge call site; the raw passthrough is timer-free and does not
  /// consult the clock.
  pub(crate) fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    if self.inbound_label_validated {
      self.inbound_plaintext.extend_from_slice(input);
      return Intake::Done;
    }

    // Inbound label not yet settled (either role): accumulate and try to
    // parse a complete header out of everything seen so far.
    self.inbound_raw.extend_from_slice(input);
    match self.classify_inbound_label() {
      LabelOutcome::Accepted(consumed) => {
        // Drop the header (or nothing, for an accepted unlabeled inbound) and
        // surface the rest verbatim. `inbound_raw` is not needed past this.
        let mut tail = self.inbound_raw.split_off(consumed);
        self.inbound_plaintext.append(&mut tail);
        self.inbound_raw = Vec::new();
        self.inbound_label_validated = true;
        // The inbound label is now validated. Fire the lazy outbound-label
        // queue: the acceptor's prefix lands in `outbound` here (its first
        // pre-validation chance), so its reply is labeled when the bridge's
        // pump runs next. The dialer's `outbound_label_queued` is already
        // `true` (set by the eager queue in `Self::dialer`), so this is a
        // no-op on the dialer side — keeping the call site uniform.
        self.queue_outbound_label_if_needed();
        Intake::Done
      }
      // Header not yet fully present: keep the accumulated bytes and wait.
      LabelOutcome::Incomplete => Intake::Done,
      LabelOutcome::Rejected => Intake::Failed,
    }
  }

  /// Validate the accumulated `inbound_raw` against the configured label,
  /// faithful to the `memberlist::codec` inbound truth table:
  ///
  /// * first byte `12`, label `Some(x)`, header label `== x` → accept,
  ///   consuming the header.
  /// * first byte `12`, label `Some(x)`, header label `!= x` → reject
  ///   (mismatch).
  /// * first byte `12`, label `None` → reject (double label); not suppressed
  ///   by `skip_inbound_label_check`.
  /// * first byte `!= 12`, label `None` → accept as unlabeled (consume 0).
  /// * first byte `!= 12`, label `Some(_)` → reject (missing-but-expected),
  ///   unless `skip_inbound_label_check`, which accepts as unlabeled.
  ///
  /// A `12` header that is shorter than `2 + len` bytes is incomplete (keep
  /// buffering); a declared length `> MAX_LABEL_LEN`, or a non-UTF-8 label, is
  /// rejected.
  fn classify_inbound_label(&self) -> LabelOutcome {
    let raw = self.inbound_raw.as_slice();
    match raw.first() {
      None => LabelOutcome::Incomplete,
      Some(&LABELED_TAG) => {
        if raw.len() < LABEL_OVERHEAD {
          return LabelOutcome::Incomplete;
        }
        let len = raw[1] as usize;
        // A faithful peer never declares a length above the cap; reject
        // without waiting for more bytes (the count can never become valid).
        if len > MAX_LABEL_LEN {
          return LabelOutcome::Rejected;
        }
        if raw.len() < LABEL_OVERHEAD + len {
          return LabelOutcome::Incomplete;
        }
        let header_label = &raw[LABEL_OVERHEAD..LABEL_OVERHEAD + len];
        // Frozen `memberlist-proto::Label` is validated UTF-8; reject a
        // malformed label before matching, closing the "matched anyway"
        // footgun (memberlist::codec rejects non-UTF-8 inbound labels).
        if core::str::from_utf8(header_label).is_err() {
          return LabelOutcome::Rejected;
        }
        match self.label.as_deref() {
          Some(expected) if expected == header_label => {
            LabelOutcome::Accepted(LABEL_OVERHEAD + len)
          }
          Some(_) => LabelOutcome::Rejected,
          // Labeled frame but no local label: double label. Not suppressed by
          // `skip_inbound_label_check` (memberlist-core suppresses only the
          // missing-label case), so an unlabeled node never accepts another
          // cluster's labeled traffic.
          None => LabelOutcome::Rejected,
        }
      }
      // First byte is not the label tag: the inbound is unlabeled.
      Some(_) => match self.label.as_deref() {
        // No label expected: accept verbatim, consuming nothing.
        None => LabelOutcome::Accepted(0),
        // A label was expected but none arrived: reject, unless the inbound
        // check is suppressed, in which case accept as unlabeled.
        Some(_) => {
          if self.skip_inbound_label_check {
            LabelOutcome::Accepted(0)
          } else {
            LabelOutcome::Rejected
          }
        }
      },
    }
  }

  /// Drain queued outbound bytes (the one-time label prefix, then raw
  /// application bytes) into `out`. Returns the number of bytes appended.
  pub(crate) fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.outbound.len();
    out.append(&mut self.outbound);
    n
  }

  /// `true` while this side still gates its bridge's `Stream` mint on the
  /// inbound label. Only the acceptor uses this gate: it stays handshaking
  /// until the inbound label is validated so the bridge withholds the
  /// `Stream` mint (and the merge dispatch) until the cluster is confirmed
  /// right. The dialer is never handshaking — classic memberlist sends
  /// `[label][request]` together, so the dialer's request must flow
  /// immediately — and the dialer's INBOUND label is still validated in-line
  /// by [`Self::handle_transport_data`] (the established intake path) before
  /// the response can reach the `Stream`.
  pub(crate) fn is_handshaking(&self) -> bool {
    matches!(self.role, Role::Acceptor) && !self.inbound_label_validated
  }

  /// Drain surfaced application plaintext into `out`. Returns the byte count.
  pub(crate) fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.inbound_plaintext.len();
    out.append(&mut self.inbound_plaintext);
    n
  }

  /// Queue application plaintext for the transport. With no encryption the
  /// bytes are appended verbatim to the outbound buffer (after the one-time
  /// label prefix that was queued at construction) and drained via
  /// [`Self::poll_transport_transmit`].
  pub(crate) fn write_plaintext(&mut self, plaintext: &[u8]) {
    self.outbound.extend_from_slice(plaintext);
  }

  /// No-op: a plain-TCP exchange has no in-band close record. Our send-half
  /// close is the driver-level TCP FIN, issued out of band by the bridge when
  /// it drops the connection — not a byte written through this record unit.
  /// Present only for surface parity with the TLS record unit's
  /// `close_notify`.
  pub(crate) fn send_close_notify(&mut self) {}

  /// Drop any bytes queued in the outbound buffer. Called by the bridge's
  /// failure-retirement transition so a `Failed` bridge cannot later have
  /// its outbound drained into the coordinator's transmit queue by the reap
  /// path. After the asymmetric-queue redesign a handshaking acceptor's
  /// `outbound` is always empty (the lazy queue does not fire until inbound
  /// validation), so this clear is a no-op on the pre-validation
  /// acceptor-failure paths; the function still matters for:
  ///
  /// * The dialer's eager label prefix (queued at construction by
  ///   [`Self::dialer`]) on a mid-exchange failure: the prefix is no longer
  ///   destined for the wire.
  /// * Any application bytes the FSM has written via
  ///   [`Self::write_plaintext`] but not yet drained — a failure
  ///   invalidates them.
  /// * An acceptor that has already validated its inbound label (the lazy
  ///   queue fired) and then failed mid-reply — its labeled response
  ///   chunk is dropped.
  ///
  /// The coordinator's transmit-queue purge runs in parallel for bytes that
  /// had ALREADY been drained out of `outbound` into the queue before the
  /// failure transition.
  pub(crate) fn clear_outbound(&mut self) {
    self.outbound.clear();
  }

  /// Always `false`: peer closure on plain TCP is the out-of-band transport
  /// FIN surfaced by the driver's `read == 0` / EOF anchor, not an in-band
  /// record this unit can observe. Present only for surface parity with the
  /// TLS record unit.
  pub(crate) fn peer_has_closed(&self) -> bool {
    false
  }
}

/// Normalize an optional label: an empty label and an absent label are the
/// same thing — "no label" — exactly as `memberlist::codec::effective_label`
/// and the frozen `memberlist-proto` encoder/decoder treat it.
fn effective_label(label: Option<Vec<u8>>) -> Option<Vec<u8>> {
  match label {
    Some(l) if !l.is_empty() => Some(l),
    _ => None,
  }
}

/// Write a `[LABELED_TAG][len][label]` prefix for a non-empty label into
/// `out`. Callers pass only non-empty labels (an empty label is "no label"
/// and emits nothing).
///
/// The label length is bounded by `MAX_LABEL_LEN`; the configuration layer
/// validates the configured label against that cap, so an over-long label
/// cannot reach a constructor. The `as u8` cast is therefore in range,
/// matching `memberlist::codec::wrap_label`.
fn encode_label_prefix(label: &[u8], out: &mut Vec<u8>) {
  debug_assert!(
    label.len() <= MAX_LABEL_LEN,
    "label length is validated against MAX_LABEL_LEN before construction"
  );
  out.reserve(LABEL_OVERHEAD + label.len());
  out.push(LABELED_TAG);
  out.push(label.len() as u8);
  out.extend_from_slice(label);
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Instant;

  fn label(s: &str) -> Option<Vec<u8>> {
    Some(s.as_bytes().to_vec())
  }

  fn now() -> Instant {
    Instant::now()
  }

  #[test]
  fn dialer_emits_label_prefix_then_passes_plaintext_through() {
    let mut r = RawRecords::dialer(label("cluster-x"), false);
    // A dialer is never handshaking (bridge mint gate); the inbound label is
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
    // validation, not at construction: a handshaking acceptor reveals
    // nothing on the wire until its peer has proven cluster identity,
    // faithful to `memberlist-core/src/network.rs::handle_conn`'s
    // `read_message`-before-`send_message` ordering. A wrong-cluster /
    // slow-loris / scanner peer that completes only the TCP handshake never
    // learns the local cluster label.
    let mut r = RawRecords::acceptor(label("cluster-x"), false);
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
    // fires inside `handle_transport_data`'s `Accepted` branch and the
    // acceptor's reply prefix becomes available on the next drain.
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
    let mut r = RawRecords::acceptor(label("cluster-x"), false);
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
    let mut r = RawRecords::acceptor(label("cluster-x"), false);
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
    let mut r = RawRecords::dialer(label("cluster-x"), false);
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
    let mut r = RawRecords::dialer(label("cluster-x"), false);
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
    let mut r = RawRecords::dialer(label("cluster-x"), false);
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
    let mut r = RawRecords::dialer(label("cluster-x"), true);
    assert!(matches!(
      r.handle_transport_data(b"response_bytes", now()),
      Intake::Done
    ));
    let mut got = Vec::new();
    r.read_plaintext(&mut got);
    assert_eq!(&got, b"response_bytes");
  }

  #[test]
  fn acceptor_buffers_partial_label_then_completes() {
    let mut r = RawRecords::acceptor(label("cluster-x"), false);
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
    let mut r = RawRecords::acceptor(label("cluster-x"), false);
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

  #[test]
  fn peer_close_is_out_of_band_only() {
    // A dialer with no label: passthrough, no in-band close signal.
    let mut r = RawRecords::dialer(None, false);
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
    // A label is configured, but the inbound check is suppressed: an
    // unlabeled inbound (first byte != 12) is accepted as plaintext rather
    // than rejected for a missing-but-expected label.
    let mut r = RawRecords::acceptor(label("cluster-x"), true);
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

  #[test]
  fn acceptor_with_no_label_rejects_labeled_inbound_as_double_label() {
    // No local label configured, but the peer sent a `[12]...` prefix:
    // reject (DoubleLabel) so an unlabeled node never accepts another
    // cluster's labeled traffic. `skip_inbound_label_check` does NOT
    // suppress this.
    let mut r = RawRecords::acceptor(None, true);
    let mut input = vec![12u8, 9];
    input.extend_from_slice(b"cluster-x");
    input.extend_from_slice(b"req-bytes");
    assert!(matches!(
      r.handle_transport_data(&input, now()),
      Intake::Failed
    ));
  }

  #[test]
  fn acceptor_rejects_over_long_declared_label_length() {
    // A declared label length above MAX_LABEL_LEN (253) is rejected as soon
    // as the two-byte `[tag][len]` prefix is buffered — the guard fires
    // before the `2 + len` completeness check, so no trailing bytes are
    // required. Values 254 and 255 are the only u8 lengths above the cap;
    // use 254 here.
    //
    // The acceptor is configured with a normal expected label. This proves
    // the over-long guard fires before any match attempt: even if trailing
    // bytes were provided that matched the configured label, the count can
    // never become valid, so rejection is immediate.
    let mut r = RawRecords::acceptor(label("cluster-x"), false);
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
    // even though `inbound[2..4] == expected[..]` byte-for-byte, the
    // UTF-8 validity check fires first and returns `Intake::Failed`.
    //
    // `RawRecords::acceptor` accepts raw bytes, so the test can supply a
    // non-UTF-8 expected label even though the higher-level config layer
    // would reject such a label at configuration time.
    let mut r = RawRecords::acceptor(Some(vec![0xff, 0xfe]), false);
    // `[12][2][0xff][0xfe]` — fully-buffered header with two-byte label
    // that byte-matches the configured bytes but is not valid UTF-8.
    let input = [LABELED_TAG, 2u8, 0xff, 0xfe];
    assert!(matches!(
      r.handle_transport_data(&input, now()),
      Intake::Failed
    ));
  }
}
