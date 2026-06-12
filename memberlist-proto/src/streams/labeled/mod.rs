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
      LabelError::TooLong(_) => Self::LabelTooLong,
      _ => Self::InvalidLabel,
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
mod tests;
