//! Streaming cluster-label state machine for the reliable plane.
//!
//! [`LabelGate`] holds the per-exchange streaming state around the pure
//! [`crate::label::classify_header`] decision: which side of the exchange it
//! serves, the one-time outbound label prefix it owes the wire, and the
//! bounded inbound buffer that reassembles a `[LABELED_TAG=12][len][label]`
//! header split across transport reads. It is the composable piece a
//! [`super::labeled::Labeled`] decorator wraps around any inner record layer
//! so the cluster label rides the reliable plane uniformly regardless of the
//! transport underneath.
//!
//! # Bidirectional label on the reliable path
//!
//! Memberlist's reliable-stream encoder applies the local label on BOTH send
//! paths: the dialer's request from `state.rs::push_pull_node` and the
//! acceptor's response from `network.rs::send_message` both flow through
//! `reliable_encoder` (`memberlist-core/src/network.rs`), which always labels
//! the local cluster identity. The matching `read_message` validates the
//! inbound label unless `skip_inbound_label_check` is set, so both peers
//! validate on every reliable-stream read (`memberlist/src/codec.rs` is the
//! truth table). Cluster identity is therefore a soft tag enforced in BOTH
//! directions: a wrong-cluster responder that has `skip_inbound_label_check =
//! true` cannot trick a dialer into merging its state by responding with a
//! different (or absent) label.
//!
//! The outbound prefix timing is asymmetric. The dialer queues its prefix
//! EAGERLY at construction (classic memberlist sends `[label][request]`
//! together — the initiator reveals its cluster identity in the same wire
//! write as its first request), while the acceptor queues its prefix LAZILY
//! at the moment its INBOUND label validates. A still-validating acceptor
//! therefore has an EMPTY outbound prefix until the dialer has proven cluster
//! identity, so a wrong-cluster / slow-loris / scanner peer that completes
//! only the TCP handshake never learns the local cluster label — faithful to
//! `memberlist-core/src/network.rs::handle_conn`, where the acceptor's reply
//! (`send_message` → `reliable_encoder().with_label(...)`) runs only after
//! `read_message` has validated the inbound label.
//!
//! # Unlabeled passthrough
//!
//! When the local label is `None` the gate is a pure passthrough from the
//! very first byte: [`LabelGate::consume_inbound`] accepts everything without
//! inspecting byte 0, and the outbound prefix stays empty. An unlabeled node
//! must NOT run the `[12]` classifier on a reliable stream — a first reliable
//! unit whose own length happens to be 12 begins with the byte `0x0C`, and
//! treating that as a label tag would false-reject or stall a perfectly valid
//! exchange. The double-label rejection (`[12]...` inbound at an unlabeled
//! node) is a gossip-plane concern (`memberlist/src/codec.rs`) and is never
//! enforced on the reliable stream.

use crate::label::{LabelVerdict, classify_header, encode_label_prefix};
#[cfg(not(feature = "std"))]
use std::vec::Vec;

/// Side of the exchange the gate serves. The role drives the outbound-prefix
/// timing (eager dialer / lazy acceptor) and the [`LabelGate::is_validating`]
/// mint gate: an acceptor stays validating until its inbound label settles
/// (so the bridge withholds its `Stream` mint until the cluster is confirmed
/// right); a dialer never gates the mint (classic memberlist sends
/// `[label][request]` together, so its request flows immediately and the
/// inbound response label is validated in-line on the established intake).
enum Role {
  Dialer,
  Acceptor,
}

/// Result of feeding one chunk of inbound plaintext through the gate while it
/// is still validating its inbound label. Newtype variants only.
pub(crate) enum LabelIntake {
  /// The inbound label is not yet settled — either the header is still
  /// incomplete (buffer more), or the chunk was consumed into the bounded
  /// header buffer. No post-label tail is available yet.
  Buffering,
  /// The inbound label validated. The carried `Vec<u8>` is the post-label
  /// tail (the application plaintext that followed the header in the same
  /// chunk, possibly empty) the caller must surface downstream.
  Validated(Vec<u8>),
  /// The inbound label was rejected (mismatch, an unexpected double-label
  /// header, an over-long declared length, or a non-UTF-8 label). The caller
  /// terminalizes the exchange; the specific [`LabelError`] is internal to the
  /// classifier — every reject collapses to one terminal transport failure on
  /// the bridge, exactly as the prior monolithic record layer surfaced a
  /// single `Failed` outcome.
  Rejected,
}

/// Streaming cluster-label state for one side of one reliable exchange.
///
/// Accessor-only; built via [`Self::dialer`] / [`Self::acceptor`]. Holds the
/// one-time outbound label prefix (eager for the dialer, lazy on inbound
/// validation for the acceptor) and the bounded inbound header buffer; the
/// per-call accept/reject decision delegates to [`classify_header`].
pub(crate) struct LabelGate {
  /// The configured cluster label, normalized so an empty label is `None`
  /// ("no label"), exactly as `crate::label::effective_label` treats it.
  label: Option<Vec<u8>>,
  /// When `true`, an absent-but-expected inbound label is accepted as
  /// unlabeled instead of rejected. Faithful to memberlist-core
  /// `Options::skip_inbound_label_check`: the suppression covers only the
  /// missing-label mismatch — a present `[12]` header with no local label is
  /// still rejected (`DoubleLabel`).
  skip_inbound_label_check: bool,
  /// Which side of the exchange the gate serves; drives the outbound-prefix
  /// timing and the [`Self::is_validating`] mint gate.
  role: Role,
  /// The one-time outbound label prefix `[12][len][label]` owed to the wire
  /// (eager for the dialer, lazy on inbound validation for the acceptor).
  /// Application bytes are NOT stored here — the decorator emits this prefix
  /// AHEAD of the inner record layer's first write. Drained by
  /// [`Self::take_outbound_prefix`].
  outbound_prefix: Vec<u8>,
  /// `true` once the local label prefix has been pushed into
  /// [`Self::outbound_prefix`], so the lazy queue helper does not
  /// double-queue. Started `false` for both roles; the dialer flips it inside
  /// its constructor (eager queue), the acceptor flips it the moment its
  /// inbound label validates (lazy queue).
  outbound_prefix_queued: bool,
  /// Accumulation for the one-time inbound label header: a coalesced
  /// transport read can split the `[12][len][label]` header, so partial header
  /// bytes are buffered here until the full header is present. Unused once
  /// `inbound_label_validated` is set.
  ///
  /// Bounded at `LABEL_OVERHEAD + MAX_LABEL_LEN` (≤ 255 bytes): the
  /// `len > MAX_LABEL_LEN` guard inside [`classify_header`] rejects a declared
  /// over-long header as soon as the two-byte `[tag][len]` prefix arrives, so
  /// the buffer never holds more than that — no unbounded-growth /
  /// slow-loris-buffer risk in the gate itself.
  inbound_raw: Vec<u8>,
  /// `true` once the inbound label has been read and validated. Both roles
  /// start `false` and run the shared inbound-label validation until this
  /// flips; afterwards intake is pure passthrough.
  inbound_label_validated: bool,
}

impl LabelGate {
  /// Dialer-side gate: queues the one-time outbound label prefix (when a
  /// non-empty label is configured) EAGERLY at construction so the request
  /// that follows it is labeled on the same wire write, exactly like
  /// `memberlist-core/src/state.rs::push_pull_node`'s
  /// `reliable_encoder().with_label(...)`. The dialer never gates the bridge
  /// mint, but its INBOUND response is still label-validated by
  /// [`Self::consume_inbound`]: a wrong-cluster responder with
  /// `skip_inbound_label_check = true` would otherwise merge state into a
  /// dialer that never checked back.
  ///
  /// An empty/`None` label emits no prefix (never a `[12][0]` header).
  pub(crate) fn dialer(label: Option<Vec<u8>>, skip_inbound_label_check: bool) -> Self {
    // Normalize: an empty label is "no label", consistent with `effective_label`.
    let label = label.filter(|l| !l.is_empty());
    let mut this = Self {
      label,
      skip_inbound_label_check,
      role: Role::Dialer,
      outbound_prefix: Vec::new(),
      outbound_prefix_queued: false,
      inbound_raw: Vec::new(),
      inbound_label_validated: false,
    };
    // Eager queue: the dialer reveals its cluster identity in the same wire
    // write as its first request.
    this.queue_outbound_prefix_if_needed();
    this
  }

  /// Acceptor-side gate: starts with an EMPTY outbound prefix; the one-time
  /// prefix is queued LAZILY at the moment the inbound label validates, so a
  /// still-validating acceptor never reveals the local cluster label to a peer
  /// that has only completed the TCP handshake. Faithful to
  /// `memberlist-core/src/network.rs::handle_conn`, where `read_message`
  /// validates the inbound label before `send_message` runs its
  /// `reliable_encoder().with_label(...)` on the reply.
  ///
  /// Stays validating until the one-time inbound label is read and validated;
  /// `skip_inbound_label_check` suppresses only the missing-but-expected-label
  /// mismatch (an unlabeled inbound is then accepted). A present `[12]` header
  /// with no configured label is still rejected as a double label.
  pub(crate) fn acceptor(label: Option<Vec<u8>>, skip_inbound_label_check: bool) -> Self {
    // Normalize: an empty label is "no label", consistent with `effective_label`.
    let label = label.filter(|l| !l.is_empty());
    Self {
      label,
      skip_inbound_label_check,
      role: Role::Acceptor,
      outbound_prefix: Vec::new(),
      outbound_prefix_queued: false,
      inbound_raw: Vec::new(),
      inbound_label_validated: false,
    }
  }

  /// Queue the local label prefix `[12][len][label]` into the outbound buffer
  /// if a label is configured and none has been queued yet. Idempotent: a
  /// second call after the first is a no-op (the flag pins it).
  ///
  /// An empty/`None` label emits no prefix; the flag is still flipped so the
  /// helper stays idempotent across the eager (dialer) and lazy (acceptor)
  /// code paths.
  fn queue_outbound_prefix_if_needed(&mut self) {
    if self.outbound_prefix_queued {
      return;
    }
    if let Some(l) = self.label.as_deref() {
      encode_label_prefix(l, &mut self.outbound_prefix);
    }
    self.outbound_prefix_queued = true;
  }

  /// Drain the one-time outbound label prefix into `out`, returning the number
  /// of bytes appended. After the prefix is drained the buffer is empty and a
  /// second call appends nothing — the decorator emits the prefix exactly once
  /// AHEAD of the first delegated inner write.
  pub(crate) fn take_outbound_prefix(&mut self, out: &mut Vec<u8>) -> usize {
    let n = self.outbound_prefix.len();
    out.append(&mut self.outbound_prefix);
    n
  }

  /// `true` while this side still gates its bridge's `Stream` mint on the
  /// inbound label. Only a LABELED acceptor uses this gate: it stays
  /// validating until the inbound label settles so the bridge withholds the
  /// `Stream` mint (and the merge dispatch) until the cluster is confirmed
  /// right. The dialer is never validating — classic memberlist sends
  /// `[label][request]` together, so the dialer's request must flow
  /// immediately — and the dialer's INBOUND label is still validated in-line
  /// by [`Self::consume_inbound`] before any response can reach the `Stream`.
  ///
  /// An UNLABELED node never gates the mint: with no local label there is no
  /// cluster identity to confirm, so the acceptor mints immediately (pure
  /// passthrough from byte 0). Gating an unlabeled acceptor would also be the
  /// only place that could mistake a 12-byte reliable unit's `0x0C` length
  /// byte for a label tag, so the gate is held open for it by construction.
  pub(crate) fn is_validating(&self) -> bool {
    self.label.is_some() && matches!(self.role, Role::Acceptor) && !self.inbound_label_validated
  }

  /// Feed one chunk of inbound plaintext while the inbound label is still
  /// being read.
  ///
  /// **Unlabeled passthrough (no local label):** returns
  /// [`LabelIntake::Validated`] with `chunk` handed straight back as the tail
  /// WITHOUT inspecting byte 0, and the inbound label is marked validated. An
  /// unlabeled node never runs the `[12]` classifier on a reliable stream (a
  /// 12-byte first reliable unit begins with `0x0C` and would be mis-parsed as
  /// a label tag).
  ///
  /// **Labeled:** accumulates `chunk` into the bounded inbound buffer and
  /// validates the `[12][len][label]` header as soon as it is complete: a
  /// complete, matching (or skip-accepted unlabeled) header marks the inbound
  /// validated, fires the lazy outbound-prefix queue, and returns the
  /// post-header tail; an incomplete header keeps buffering
  /// ([`LabelIntake::Buffering`]); an invalid header returns
  /// [`LabelIntake::Rejected`].
  ///
  /// The caller must only invoke this while [`Self::is_validating`] is `true`
  /// for the acceptor, or while the dialer's inbound label has not yet
  /// settled; once the label is validated, intake is pure passthrough and the
  /// caller surfaces inner plaintext directly.
  pub(crate) fn consume_inbound(&mut self, chunk: Vec<u8>) -> LabelIntake {
    // Unlabeled passthrough: no classifier, no buffering, no outbound prefix.
    // Byte 0 is never inspected — see the module-level note on the 12-byte
    // first-unit disambiguation. An unlabeled node passes EVERYTHING through
    // verbatim, INCLUDING a `[12]...`-led inbound: a reliable unit whose own
    // length is 12 leads with the byte `0x0C` (== `LABELED_TAG`), so an
    // unlabeled node cannot distinguish a label tag from a unit-length byte
    // and must not classify. The double-label reject is a gossip-plane
    // concern (`memberlist/src/codec.rs`) and is deliberately NOT enforced on
    // the reliable stream — `skip_inbound_label_check` is irrelevant here
    // because it only modulates the missing-but-expected case, which cannot
    // arise without a configured label.
    if self.label.is_none() {
      self.inbound_label_validated = true;
      self.queue_outbound_prefix_if_needed();
      return LabelIntake::Validated(chunk);
    }

    self.inbound_raw.extend_from_slice(&chunk);
    match classify_header(
      self.inbound_raw.as_slice(),
      self.label.as_deref(),
      self.skip_inbound_label_check,
    ) {
      LabelVerdict::Accepted(consumed) => {
        // Drop the header (or nothing, for an accepted unlabeled inbound) and
        // hand back the rest verbatim. `inbound_raw` is not needed past this.
        let tail = self.inbound_raw.split_off(consumed);
        self.inbound_raw = Vec::new();
        self.inbound_label_validated = true;
        // The inbound label is now validated: fire the lazy outbound-prefix
        // queue so the acceptor's prefix is available for the decorator's next
        // write. The dialer's prefix is already queued (eager), so this is a
        // no-op there — keeping the call site uniform.
        self.queue_outbound_prefix_if_needed();
        LabelIntake::Validated(tail)
      }
      LabelVerdict::Incomplete => LabelIntake::Buffering,
      LabelVerdict::Rejected(_) => LabelIntake::Rejected,
    }
  }

  /// `true` once the inbound label has been read and validated; afterwards the
  /// decorator surfaces inner plaintext directly without consulting the gate.
  pub(crate) fn inbound_validated(&self) -> bool {
    self.inbound_label_validated
  }

  /// Drop the queued outbound label prefix. Called by the decorator's
  /// `clear_outbound` (the bridge's failure-retirement transition) so a
  /// `Failed` exchange cannot later leak the local label prefix on the reap
  /// path. After the lazy-acceptor redesign a still-validating acceptor's
  /// prefix is always empty (the lazy queue does not fire until inbound
  /// validation), so this is a no-op on the pre-validation acceptor-failure
  /// paths; it still matters for the dialer's eager prefix on a mid-exchange
  /// failure, and for an acceptor that validated its inbound label (the lazy
  /// queue fired) and then failed mid-reply.
  pub(crate) fn clear(&mut self) {
    self.outbound_prefix.clear();
  }
}
