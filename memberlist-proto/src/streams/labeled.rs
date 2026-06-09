//! Composable cluster-label record-layer decorator for the reliable plane.
//!
//! [`Labeled<R>`] wraps any inner [`StreamTransport`] `R` with a one-time
//! `[LABELED_TAG=12][len][label]` cluster-label exchange driven by a
//! [`LabelGate`]. The local label rides as the FIRST application plaintext the
//! inner layer sends — handed to [`StreamTransport::write_plaintext`] ahead of
//! any real application bytes — and the peer's label is stripped off the front
//! of the inner's surfaced plaintext and validated against the configured
//! cluster label before any membership data is surfaced.
//!
//! Routing the label THROUGH the inner write path is what lets one decorator
//! serve every transport: a pure byte pipe ([`Passthrough`]) emits the label as
//! the leading RAW bytes (so [`Labeled<Passthrough>`] is exactly the plain-TCP
//! reliable record layer — a label prefix, then raw passthrough), while a
//! confidential inner ([`crate::tls::TlsRecords`]) ENCRYPTS the label inside
//! its session so the peer recovers it from its own decrypted-plaintext stream.
//! An in-TLS label is therefore never cleartext spliced ahead of the TLS record
//! stream (which the peer's rustls would reject); for the passthrough the two
//! framings are byte-identical, so plain TCP is unchanged.
//!
//! The label is a cluster routing / boundary marker byte-compatible with the
//! frozen `memberlist-proto` label frame and the `memberlist::codec`
//! outer-label rules. See [`super::label`] for the bidirectional-validation
//! and eager-dialer / lazy-acceptor timing rationale, and for the
//! unlabeled-node passthrough that fixes the 12-byte-first-unit
//! disambiguation.

use crate::Instant;
#[cfg(not(feature = "std"))]
use std::vec::Vec;

use bytes::Bytes;

use crate::{
  label::{LabelError, validate_label},
  streams::{
    label::{LabelGate, LabelIntake},
    transport::{Intake, StreamTransport},
  },
};

/// The trivial inner record layer: a pure byte pipe with no handshake and no
/// confidentiality. [`handle_transport_data`](StreamTransport::handle_transport_data)
/// buffers input verbatim as inbound plaintext;
/// [`write_plaintext`](StreamTransport::write_plaintext) appends to an
/// outbound buffer drained by
/// [`poll_transport_transmit`](StreamTransport::poll_transport_transmit).
/// [`is_handshaking`](StreamTransport::is_handshaking) and
/// [`is_secure`](StreamTransport::is_secure) are always `false`; the close
/// signal is the out-of-band transport FIN, so
/// [`peer_has_closed`](StreamTransport::peer_has_closed) is always `false` and
/// [`send_close_notify`](StreamTransport::send_close_notify) is a no-op.
///
/// Wrapped by [`Labeled`] to form the plain-TCP reliable record layer.
/// Accessor-only.
pub struct Passthrough {
  /// Bytes to hand to the transport, appended by [`Self::write_plaintext`] and
  /// drained by [`Self::poll_transport_transmit`].
  outbound: Vec<u8>,
  /// Application bytes ready for the consumer, buffered verbatim by
  /// [`Self::handle_transport_data`] and drained by [`Self::read_plaintext`].
  inbound: Vec<u8>,
}

impl Passthrough {
  /// Build an empty byte pipe. Both the dialer and acceptor sides of a
  /// passthrough are identical (there is no handshake), so there is a single
  /// constructor; [`Labeled`] supplies the role asymmetry through its
  /// [`LabelGate`].
  fn new() -> Self {
    Self {
      outbound: Vec::new(),
      inbound: Vec::new(),
    }
  }
}

impl StreamTransport for Passthrough {
  type Options = ();
  type DialContext = ();
  type ConstructError = core::convert::Infallible;

  fn dial_context<A>(_addr: &A, _server_name: Option<&str>) -> Result<(), &'static str> {
    Ok(())
  }

  fn dialer(_opts: &Self::Options, _ctx: Self::DialContext) -> Result<Self, Self::ConstructError> {
    Ok(Self::new())
  }

  fn acceptor(_opts: &Self::Options) -> Result<Self, Self::ConstructError> {
    Ok(Self::new())
  }

  fn handle_transport_data(&mut self, input: &[u8], _now: Instant) -> Intake {
    // No crypto, no backpressure: every byte is buffered verbatim and surfaced
    // as plaintext, so this only ever returns `Done`.
    self.inbound.extend_from_slice(input);
    Intake::Done
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.outbound.len();
    out.append(&mut self.outbound);
    n
  }

  fn is_handshaking(&self) -> bool {
    false
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.inbound.len();
    out.append(&mut self.inbound);
    n
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> bool {
    // Plain passthrough: the outbound `Vec` is bounded by the bridge's
    // one-unit-per-pump cadence, so a unit is always accepted.
    self.outbound.extend_from_slice(plaintext);
    true
  }

  fn send_close_notify(&mut self) {}

  fn peer_has_closed(&self) -> bool {
    false
  }

  fn clear_outbound(&mut self) {
    self.outbound.clear();
  }

  fn is_secure() -> bool {
    false
  }
}

/// Options bundle for [`Labeled<R>`]: the cluster label, the inbound-check
/// policy, and the inner record layer's own options.
///
/// All fields are private; use the accessors. The label is validated when the
/// caller constructs the options (via [`LabelOptions::try_new_in`] or
/// [`LabelOptions::new_in`]).
#[derive(Debug, Clone)]
pub struct LabelOptions<O> {
  /// The configured cluster label, or `None` for an unlabeled node. An empty
  /// label is normalized to `None` by the constructors.
  label: Option<Bytes>,
  /// When `true`, an absent-but-expected inbound label is accepted as
  /// unlabeled instead of rejected (faithful to memberlist-core
  /// `Options::skip_inbound_label_check`).
  skip_inbound_label_check: bool,
  /// The inner record layer's own options bundle, forwarded verbatim to
  /// `R::dialer` / `R::acceptor`.
  inner: O,
}

impl<O> LabelOptions<O> {
  /// Build label options from a (normalized) label, the inbound-check policy,
  /// and the inner options. An empty label is normalized to `None`.
  ///
  /// The caller is responsible for validating the label bytes (length /
  /// UTF-8) before calling this; use [`LabelOptions::try_new_in`] or
  /// [`LabelOptions::new_in`] for the validated public constructors.
  pub(crate) fn from_parts(label: Option<Bytes>, skip_inbound_label_check: bool, inner: O) -> Self {
    let label = label.filter(|l| !l.is_empty());
    Self {
      label,
      skip_inbound_label_check,
      inner,
    }
  }

  /// The cluster label, or `None` if none was configured.
  #[inline]
  pub fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// Whether the inbound label check is skipped.
  #[inline]
  pub const fn is_skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }

  /// Borrow the inner record layer's options bundle.
  #[inline]
  pub fn inner(&self) -> &O {
    &self.inner
  }

  /// Build a label-decorated options bundle over the inner record layer's
  /// options, validating the cluster label.
  ///
  /// - `Some(bytes)` where `bytes.is_empty()` normalizes to `None` (no label).
  /// - A present label must be ≤ [`crate::label::MAX_LABEL_LEN`] (253) bytes
  ///   and valid UTF-8 (`memberlist-proto::Label` stores labels as `SmolStr`).
  ///
  /// The inbound-check policy defaults to `false` (an absent-but-expected
  /// label is rejected); flip it with
  /// [`skip_inbound_label_check`](Self::skip_inbound_label_check). Returns
  /// [`LabelOptionsError`] if the label violates either constraint.
  ///
  /// The `_in` suffix names the inner-options-bearing form (à la
  /// `Vec::new_in`): it is the transport-agnostic constructor the TLS
  /// coordinator (`R = Labeled<TlsRecords>`, inner `= TlsOptions`) uses to
  /// ride the label on `LabelOptions` without touching `TlsOptions`. The
  /// plain-TCP form (`R = Labeled<Passthrough>`, inner `= ()`) uses this
  /// constructor directly.
  pub fn try_new_in(label: Option<Vec<u8>>, inner: O) -> Result<Self, LabelOptionsError> {
    let label = match label {
      None => None,
      Some(bytes) if bytes.is_empty() => None,
      Some(bytes) => {
        validate_label(&bytes).map_err(LabelOptionsError::from_label_error)?;
        Some(Bytes::from(bytes))
      }
    };
    Ok(Self::from_parts(label, false, inner))
  }

  /// Build a label-decorated options bundle over the inner options, panicking
  /// if the label is invalid.
  ///
  /// Suitable for static / compile-time-known labels; use
  /// [`try_new_in`](Self::try_new_in) for labels supplied at runtime.
  ///
  /// # Panics
  ///
  /// Panics if the label exceeds [`crate::label::MAX_LABEL_LEN`] (253) bytes
  /// or is not valid UTF-8 (see [`try_new_in`](Self::try_new_in) for the
  /// exact constraints).
  pub fn new_in(label: Option<Vec<u8>>, inner: O) -> Self {
    Self::try_new_in(label, inner).expect("invalid cluster label")
  }

  /// Builder: suppress the inbound label check (consuming).
  ///
  /// When set, an absent-but-expected inbound label is accepted as unlabeled
  /// instead of rejected (faithful to memberlist-core
  /// `Options::skip_inbound_label_check`). Defaults to off. A present `[12]`
  /// header with no configured label is still rejected as a double label.
  #[must_use]
  #[inline]
  pub fn skip_inbound_label_check(mut self) -> Self {
    self.skip_inbound_label_check = true;
    self
  }
}

/// Error returned by [`LabelOptions::try_new_in`] / [`LabelOptions::new_in`]
/// when the cluster label violates the wire constraints.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LabelOptionsError {
  /// The label exceeds the 253-byte wire maximum
  /// ([`crate::label::MAX_LABEL_LEN`]): the `[tag][len]` header occupies 2
  /// bytes of the single-`u8` length field.
  #[error("label too long (max 253 bytes)")]
  LabelTooLong,

  /// The label bytes are not valid UTF-8, as required by
  /// `memberlist-proto::Label` (labels round-trip through `SmolStr`).
  #[error("label is not valid UTF-8")]
  InvalidLabel,
}

impl LabelOptionsError {
  /// Map a label-frame validation error onto the options error. The
  /// `Mismatch` / `DoubleLabel` variants are inbound-classification outcomes
  /// that [`validate_label`] never returns, so they collapse to
  /// [`Self::InvalidLabel`] defensively.
  fn from_label_error(e: LabelError) -> Self {
    match e {
      LabelError::TooLong => Self::LabelTooLong,
      LabelError::NotUtf8 | LabelError::Mismatch | LabelError::DoubleLabel => Self::InvalidLabel,
    }
  }
}

/// A cluster-label decorator around an inner [`StreamTransport`] `R`.
///
/// Carries the one-time `[LABELED_TAG=12][len][label]` cluster-label exchange
/// through a `LabelGate` while delegating every byte to the inner record
/// layer. The label rides as the FIRST application plaintext the inner layer
/// sends — handed to [`StreamTransport::write_plaintext`] ahead of any real
/// application bytes — so a confidential inner (TLS) encrypts the label inside
/// its session and a pure byte pipe (passthrough) emits it as the leading raw
/// bytes. The peer's label is stripped off the front of the inner's surfaced
/// plaintext and validated before any membership data is surfaced.
///
/// Routing the label THROUGH the inner write path (rather than prepending it
/// to the inner's ciphertext) is load-bearing for TLS: an in-TLS label is
/// decrypted plaintext the peer's `read_plaintext` yields, never cleartext
/// spliced ahead of the TLS record stream (which the peer's rustls would
/// reject). For passthrough the two are byte-identical, so plain TCP is
/// unchanged. Accessor-only; built via the [`StreamTransport`] `dialer` /
/// `acceptor` constructors (eager / lazy gate respectively).
pub struct Labeled<R> {
  gate: LabelGate,
  inner: R,
  /// `true` once the local label prefix has been handed to the inner layer via
  /// [`StreamTransport::write_plaintext`], so it is sent exactly once and
  /// always AHEAD of the first real application write. Flipped by
  /// [`Self::flush_outbound_label_into_inner`]: eagerly at `dialer`
  /// construction, lazily the moment the acceptor's inbound label validates.
  prefix_emitted: bool,
  /// The post-label inbound tail the gate stripped while validating: the
  /// application plaintext that followed the `[12][len][label]` header in the
  /// same read. Buffered here so [`Self::read_plaintext`] surfaces it AHEAD of
  /// any later inner plaintext, preserving wire order. Empty once drained, and
  /// never refilled after the inbound label validates (subsequent inbound
  /// plaintext flows straight through the inner layer).
  post_label_tail: Vec<u8>,
}

impl<R> Labeled<R>
where
  R: StreamTransport,
{
  /// Hand the gate's queued outbound label prefix to the inner record layer as
  /// its first application plaintext, exactly once. Idempotent and a no-op
  /// while the gate has no prefix queued (an unlabeled node, or a lazy acceptor
  /// whose inbound label has not validated yet).
  ///
  /// Called eagerly at `dialer` construction so the dialer's
  /// `[12][len][label]` precedes its first request inside the session, and
  /// lazily from [`Self::consume_inbound_label`] the moment the acceptor's
  /// inbound label validates (before the bridge writes the response). Because
  /// the prefix is fed THROUGH [`StreamTransport::write_plaintext`], a
  /// confidential inner (TLS) encrypts it and a passthrough inner emits it raw
  /// — uniformly the first application bytes on the wire either way.
  fn flush_outbound_label_into_inner(&mut self) {
    if self.prefix_emitted {
      return;
    }
    let mut prefix = Vec::new();
    let n = self.gate.take_outbound_prefix(&mut prefix);
    if n == 0 {
      // No prefix yet (unlabeled, or the acceptor's lazy prefix is not queued
      // until its inbound label validates). Do NOT pin `prefix_emitted` — a
      // later validation must still get one chance to flush.
      return;
    }
    // Ignoring the accept bool: the cluster label is a short fixed prefix
    // (`[12][len][label]`, at most a few hundred bytes) written first into an
    // empty buffer whose floor is `MIN_TLS_SEND_BUFFER` (64 KiB), so it always
    // fits the send bound.
    let _ = self.inner.write_plaintext(&prefix);
    self.prefix_emitted = true;
  }

  /// Drain the inner layer's surfaced plaintext and feed it through the gate
  /// while the inbound label is still being validated, buffering the
  /// post-label tail for [`Self::read_plaintext`]. Returns `true` if the gate
  /// rejected the inbound label (the caller maps this to [`Intake::Failed`]).
  ///
  /// Runs for BOTH roles until the inbound label settles: the acceptor
  /// validates pre-mint (gating its `Stream`), and the dialer validates
  /// in-line on its established intake (the response-label check). On a
  /// successful validation the lazy acceptor's outbound prefix has just been
  /// queued inside the gate, so this flushes it into the inner write path (the
  /// acceptor's reply label) before returning. Once the label is validated the
  /// inner plaintext flows straight through and this is never invoked again.
  fn consume_inbound_label(&mut self) -> bool {
    // Pull whatever plaintext the inner layer surfaced this feed and hand it
    // to the gate. A passthrough inner surfaces every byte at once; a
    // backpressured inner may surface a prefix per feed, which is fine — the
    // gate accumulates partial headers across feeds.
    let mut surfaced = Vec::new();
    self.inner.read_plaintext(&mut surfaced);
    if surfaced.is_empty() {
      return false;
    }
    match self.gate.consume_inbound(surfaced) {
      LabelIntake::Validated(tail) => {
        self.post_label_tail.extend_from_slice(&tail);
        // The lazy acceptor's outbound prefix (if any) was just queued inside
        // the gate's accepted branch; flush it into the inner write path so the
        // reply label precedes the bridge's response. A no-op for the dialer
        // (already flushed eagerly) and for an unlabeled node (no prefix).
        self.flush_outbound_label_into_inner();
        false
      }
      LabelIntake::Buffering => false,
      LabelIntake::Rejected => true,
    }
  }
}

/// Test-only direct constructors for the plain-TCP reliable record layer
/// ([`Labeled<Passthrough>`]). They build the label gate from raw label bytes
/// and the inbound-check flag without an intervening [`LabelOptions`], so the
/// bridge / records guard tests can mint a dialer or acceptor in one call.
/// The production path always constructs through [`StreamTransport::dialer`] /
/// [`StreamTransport::acceptor`] over [`LabelOptions`].
#[cfg(test)]
impl Labeled<Passthrough> {
  /// Build a dialer-side plain-TCP record layer (eager label gate) directly
  /// from label bytes + the inbound-check flag.
  pub(crate) fn dialer(label: Option<Vec<u8>>, skip_inbound_label_check: bool) -> Self {
    let mut this = Self {
      gate: LabelGate::dialer(label, skip_inbound_label_check),
      inner: Passthrough::new(),
      prefix_emitted: false,
      post_label_tail: Vec::new(),
    };
    // Eager flush: the dialer's `[12][len][label]` is the first application
    // plaintext, ahead of its first request.
    this.flush_outbound_label_into_inner();
    this
  }

  /// Build an acceptor-side plain-TCP record layer (lazy label gate) directly
  /// from label bytes + the inbound-check flag.
  pub(crate) fn acceptor(label: Option<Vec<u8>>, skip_inbound_label_check: bool) -> Self {
    Self {
      gate: LabelGate::acceptor(label, skip_inbound_label_check),
      inner: Passthrough::new(),
      prefix_emitted: false,
      post_label_tail: Vec::new(),
    }
  }
}

impl<R> StreamTransport for Labeled<R>
where
  R: StreamTransport,
{
  type Options = LabelOptions<R::Options>;
  type DialContext = R::DialContext;
  type ConstructError = R::ConstructError;

  fn dial_context<A>(
    addr: &A,
    server_name: Option<&str>,
  ) -> Result<Self::DialContext, &'static str> {
    R::dial_context(addr, server_name)
  }

  fn dialer(opts: &Self::Options, ctx: Self::DialContext) -> Result<Self, Self::ConstructError> {
    let mut this = Self {
      // Eager gate: the dialer queues its label prefix at construction so the
      // first request rides the same wire write.
      gate: LabelGate::dialer(
        opts.label().map(<[u8]>::to_vec),
        opts.is_skip_inbound_label_check(),
      ),
      inner: R::dialer(opts.inner(), ctx)?,
      prefix_emitted: false,
      post_label_tail: Vec::new(),
    };
    // Eager flush: hand the dialer's `[12][len][label]` to the inner write path
    // NOW so it is the first application plaintext (encrypted by TLS / raw on a
    // passthrough), ahead of the first request the bridge will write. A no-op
    // for an unlabeled dialer.
    this.flush_outbound_label_into_inner();
    Ok(this)
  }

  fn acceptor(opts: &Self::Options) -> Result<Self, Self::ConstructError> {
    Ok(Self {
      // Lazy gate: the acceptor queues its label prefix only once the inbound
      // label validates, so a still-validating acceptor reveals nothing. The
      // prefix is flushed into the inner write path at that validation point by
      // `consume_inbound_label`.
      gate: LabelGate::acceptor(
        opts.label().map(<[u8]>::to_vec),
        opts.is_skip_inbound_label_check(),
      ),
      inner: R::acceptor(opts.inner())?,
      prefix_emitted: false,
      post_label_tail: Vec::new(),
    })
  }

  fn handle_transport_data(&mut self, input: &[u8], now: Instant) -> Intake {
    // Delegate the raw bytes to the inner layer (a passthrough buffers them as
    // plaintext; a TLS layer decrypts them). An inner reject is terminal.
    let intake = self.inner.handle_transport_data(input, now);
    if matches!(intake, Intake::Failed) {
      return Intake::Failed;
    }
    // While the inbound label is unsettled (either role), strip + validate it
    // off the front of the inner's surfaced plaintext. A gate reject is a
    // terminal label-mismatch / double-label / over-long / non-UTF-8 reject.
    if !self.gate.inbound_validated() && self.consume_inbound_label() {
      return Intake::Failed;
    }
    intake
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    // The label is NOT prepended here: it was already handed to the inner write
    // path as the first application plaintext (eagerly at `dialer` construction,
    // lazily at the acceptor's inbound-label validation), so the inner layer
    // emits it inline — encrypted by TLS, raw on a passthrough — ahead of the
    // first real application bytes. This method only drains the inner layer's
    // transport bytes.
    self.inner.poll_transport_transmit(out)
  }

  fn is_handshaking(&self) -> bool {
    self.gate.is_validating() || self.inner.is_handshaking()
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    // Surface the gate's buffered post-label tail FIRST (the application bytes
    // that followed the header in the validating read), then the inner
    // layer's plaintext, preserving wire order.
    let mut n = self.post_label_tail.len();
    out.append(&mut self.post_label_tail);
    n += self.inner.read_plaintext(out);
    n
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) -> bool {
    self.inner.write_plaintext(plaintext)
  }

  fn set_send_capacity(&mut self, max_frame: usize) {
    // The label prefix is a few bytes prepended once via the inner layer's own
    // `write_plaintext`, so the inner bound (sized to one reliable unit) already
    // covers it; thread it straight through.
    self.inner.set_send_capacity(max_frame);
  }

  fn send_close_notify(&mut self) {
    self.inner.send_close_notify();
  }

  fn peer_has_closed(&self) -> bool {
    self.inner.peer_has_closed()
  }

  fn clear_outbound(&mut self) {
    // Drop the inner layer's outbound queue (for a passthrough that holds the
    // label-as-first-plaintext plus any raw application bytes; for TLS this is
    // a documented no-op since rustls exposes no API to discard pending
    // ciphertext). Also clear any gate prefix still queued-but-not-yet-flushed
    // (e.g. a lazy acceptor prefix queued at validation but not yet handed to
    // the inner layer), so a `Failed` exchange cannot later flush the local
    // label. The coordinator additionally collects nothing on a failed reap, so
    // a TLS label already encrypted into rustls's write buffer never reaches
    // the wire regardless.
    self.gate.clear();
    self.inner.clear_outbound();
  }

  fn is_secure() -> bool {
    R::is_secure()
  }
}

#[cfg(test)]
mod tests {
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
}
