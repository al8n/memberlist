//! `memberlist::Stream` <-> `TlsRecords` byte-pump for one reliable exchange.
//!
//! Phase: `Handshaking` (shuttle ciphertext until `!is_handshaking()`, before
//! any `Stream` exists) → `Established(BridgePhase)` (the byte pump runs and
//! the half-close lifecycle tracks via the shared [`BridgePhase`]). The
//! `Stream` is minted by the coordinator at `dial_succeeded` / `accept_stream`
//! AFTER the handshake completes, so its exchange deadline excludes the TLS
//! handshake (the handshake is bounded by the dial deadline instead).
//!
//! D1 (drain-before-reap) and the atomic-failure / queue-clear discipline are
//! reused verbatim from the composed-coordinator design: every failure
//! transition clears the FSM's queued endpoint events before the terminal
//! phase, so no queued merge / ack survives an aborted exchange.
//!
//! Transport anchors (vs. the QUIC bridge's `StreamEvent::Finished` /
//! `Chunks::next() -> Ok(None)`): our `TlsRecords::send_close_notify()` retires
//! the send half (`SendClosed`); the peer's `close_notify`
//! (`TlsRecords::peer_has_closed()`) OR a TCP `read == 0` (a zero-length
//! `handle_transport_data`) feeds `Stream::handle_data(&[], now)` and, on a
//! clean accept, retires the recv half (`RecvClosed`). Both halves retired is
//! `BothClosed` → reap. There is no transport back-pressure inside the record
//! layer (rustls buffers plaintext into an unbounded send queue), so the QUIC
//! bridge's `pending_out` retain loop and its `WriteError::Blocked` handling
//! have no analog here.

use std::time::Instant;

use super::records::{Intake, TlsRecords};
use crate::{
  bridge_phase::{BridgeFailure, BridgePhase},
  endpoint::Endpoint,
  event::{EndpointEvent, StreamCommand, StreamId},
  stream::Stream,
};

/// Lifecycle of one composed `(memberlist Stream, TlsRecords)` exchange.
/// `Handshaking` precedes the `Stream`; `Established` carries the shared
/// half-close [`BridgePhase`]. Newtype variant over `BridgePhase` (the
/// no-multi-field-tuple-variant rule).
enum TlsPhase {
  /// The TLS handshake is in flight; no `Stream` exists yet. The bridge only
  /// shuttles ciphertext through `TlsRecords`.
  Handshaking,
  /// Handshake done, `Stream` minted; the byte pump runs and the half-close
  /// lifecycle is tracked by the inner [`BridgePhase`].
  Established(BridgePhase),
}

/// Couples one reliable-exchange [`Stream`] to one [`TlsRecords`], pumping
/// plaintext through the TLS record layer both ways. Built `Handshaking`
/// (pre-`Stream`); the coordinator promotes it to `Established` once the
/// handshake completes and the `Stream` is minted. Accessor-only.
pub(crate) struct TlsBridge<I, A> {
  /// `None` until the handshake completes and the coordinator mints the
  /// `Stream`. The `Handshaking` phase is exactly the `stream.is_none()` window.
  stream: Option<Stream<I, A>>,
  records: TlsRecords,
  /// `true` once `Stream::poll_transmit` yielded this exchange's output bytes
  /// (so the deferred `send_close_notify` is owed). See the QUIC bridge's
  /// `sent_any` doc for the inbound-response-not-yet-loaded subtlety: an
  /// inbound stream whose response has not been `stream_load_response`'d also
  /// yields `None` from `poll_transmit` (its phase is `InboundSendingResponse`,
  /// not `Done`), so sending `close_notify` on the first empty poll would
  /// half-close before the reply is encrypted. The owed `close_notify` is sent
  /// only once `poll_transmit` yields nothing more.
  sent_any: bool,
  /// `send_close_notify()` is one-shot per rustls connection — this latch
  /// guards a second call. Distinct from the `BridgePhase::SendClosed`
  /// transition: this records that WE queued the alert; `SendClosed` records
  /// that the send half is retired. For TLS the alert is buffered locally and
  /// retires our send half immediately, so the two move in lockstep, but the
  /// latch is still needed because `retire_halves` (failure path) and
  /// `pump_out` (clean path) both may reach the `send_close_notify` call.
  close_notify_sent: bool,
  /// Ciphertext the peer coalesced AFTER its final handshake flight (TLS 1.3
  /// allows the final flight + first app records + `close_notify` in one
  /// transport read). Retained by [`Self::intake_handshaking`] when the
  /// handshake completes mid-feed — there is no `Stream` yet to drain the
  /// trailing app records into — and replayed through the established interleave
  /// by [`Self::replay_pending`] the same tick the coordinator promotes the
  /// bridge. Empty in the steady state (a normal handshake carries no trailing
  /// app data, and the established intake consumes it inline).
  pending_ciphertext: Vec<u8>,
  phase: TlsPhase,
  /// The exchange deadline, snapshotted from the inner `Stream` at promotion
  /// (mirrors the QUIC bridge's `deadline` field + its `Done`-but-unflushed
  /// rationale). During `Handshaking` the deadline is the dial deadline,
  /// supplied by the coordinator, and is the only timer the bridge contributes
  /// (no `Stream` exists yet to fold in via `min`).
  deadline: Instant,
}

impl<I, A> TlsBridge<I, A>
where
  I: nodecraft::Id
    + memberlist_wire::Data
    + nodecraft::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: memberlist_wire::Data
    + nodecraft::CheapClone
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  /// Build a `Handshaking` bridge wrapping a fresh `TlsRecords`. The dial /
  /// accept deadline bounds the handshake; the `Stream` (and its own exchange
  /// deadline) is installed by [`Self::promote`] once the handshake completes.
  pub(crate) fn new(records: TlsRecords, deadline: Instant) -> Self {
    Self {
      stream: None,
      records,
      sent_any: false,
      close_notify_sent: false,
      pending_ciphertext: Vec::new(),
      phase: TlsPhase::Handshaking,
      deadline,
    }
  }

  /// `true` while the TLS handshake is still in flight (no `Stream` yet). Once
  /// the record layer reports the handshake done (`!records.is_handshaking()`)
  /// the coordinator mints the `Stream` and calls [`Self::promote`]; the phase
  /// is still `Handshaking` in that one-tick window, so both conjuncts gate the
  /// pre-`Stream` state.
  pub(crate) fn is_handshaking(&self) -> bool {
    matches!(self.phase, TlsPhase::Handshaking) && self.records.is_handshaking()
  }

  /// Feed ciphertext the driver read from the TCP connection. A zero-length
  /// slice is the TCP `read == 0` (peer half-closed the socket) anchor. Returns
  /// `Err(())` if the record layer rejected the bytes (mTLS / decrypt / record
  /// failure) — the bridge becomes terminal and the coordinator reaps it with
  /// NO `Stream` minted (during `Handshaking`) or via the `StreamErrored` D1
  /// path (during `Established`).
  ///
  /// During `Handshaking` a `TlsRecords` error just makes the bridge terminal:
  /// there is no `Stream` and no FSM queue to clear, and no endpoint side
  /// effect can have happened (this is the mTLS-reject path).
  ///
  /// During `Established` the flow mirrors the QUIC bridge's `pump_in`: a
  /// `TlsRecords` error routes through the atomic-retire-then-fail helper
  /// (`BridgeFailure::Transport`); decrypted plaintext is drained and fed to
  /// `Stream::handle_data` (a decode `Err` → `BridgeFailure::Decode`); and the
  /// close anchor — `peer_has_closed()` OR `read == 0` — feeds
  /// `Stream::handle_data(&[], now)`. A non-terminal-phase `Err`
  /// (`StreamError::PeerClosed`) is the truncation path → `BridgeFailure::Decode`;
  /// a clean `Ok(())` advances `observe_recv_fin()` → `RecvClosed`.
  pub(crate) fn handle_transport_data(
    &mut self,
    ciphertext: &[u8],
    now: Instant,
  ) -> Result<(), ()> {
    // Pre-`Stream` (handshake) window: shuttle ciphertext until the handshake
    // completes, retaining any app-data tail for post-promotion replay. Once
    // promoted, the established intake interleaves record feed with plaintext
    // drain (rustls's 16 KiB received-plaintext backpressure).
    if self.stream.is_none() {
      return self.intake_handshaking(ciphertext);
    }

    // Established: a real transport read (a zero-length slice is the TCP
    // `read == 0` EOF anchor). A non-empty retained pre-promotion tail is
    // replayed FIRST, ahead of the newly-supplied ciphertext, so the request
    // reassembles in wire order.
    let eof = ciphertext.is_empty();
    if self.pending_ciphertext.is_empty() {
      self.pump_in_established(ciphertext, eof, now)
    } else {
      let mut combined = std::mem::take(&mut self.pending_ciphertext);
      combined.extend_from_slice(ciphertext);
      self.pump_in_established(&combined, eof, now)
    }
  }

  /// Pre-`Stream` ciphertext intake. Feeds the handshake records into the record
  /// layer until either all supplied ciphertext is consumed or the handshake
  /// completes mid-feed.
  ///
  /// A TLS 1.3 peer may coalesce its final handshake flight with its first
  /// application records (and even `close_notify`) into ONE transport read:
  /// `process_new_packets` then completes the handshake (`is_handshaking()`
  /// flips false) AND buffers the trailing app records in the SAME call. Those
  /// app records cannot be drained yet — no `Stream` exists — so once the
  /// handshake clears we STOP consuming and RETAIN the unconsumed ciphertext
  /// tail in [`Self::pending_ciphertext`] (copied; the input slice is borrowed).
  /// The coordinator mints + promotes the `Stream` this same tick, then
  /// [`Self::replay_pending`] feeds the retained tail through the established
  /// interleave so a FIRST request larger than the 16 KiB received-plaintext
  /// limit (which trips `read_tls` backpressure before a `Stream` exists)
  /// reassembles intact rather than truncating to whatever fit before the
  /// handshake completed.
  ///
  /// An intake `Err` is a `process_new_packets` failure — a REAL mTLS / decrypt
  /// / malformed-record failure (never the buffer-full backpressure, which the
  /// record layer maps to `Pending`). During `Handshaking` it just terminalizes
  /// the bridge (no `Stream`, no queue, no endpoint side effect — the mTLS-reject
  /// path).
  fn intake_handshaking(&mut self, ciphertext: &[u8]) -> Result<(), ()> {
    let mut offset = 0usize;
    loop {
      let intake = match self.records.handle_transport_data(&ciphertext[offset..]) {
        Ok(intake) => intake,
        Err(e) => {
          self.fail(BridgeFailure::Transport(format!("tls handshake: {e}")));
          return Err(());
        }
      };
      let consumed = match intake {
        Intake::Done => {
          offset = ciphertext.len();
          true
        }
        Intake::Pending(n) => {
          offset += n;
          n > 0
        }
      };

      // The handshake just completed inside this feed: any remaining ciphertext
      // is application data the promoted `Stream` must receive. Retain the tail
      // (copied — `ciphertext` is borrowed) for `replay_pending` and stop; the
      // handshake records already consumed advanced the handshake, and any app
      // data already decrypted into the received-plaintext buffer stays in
      // rustls until the established drain pulls it post-promotion.
      if !self.records.is_handshaking() {
        if offset < ciphertext.len() {
          self
            .pending_ciphertext
            .extend_from_slice(&ciphertext[offset..]);
        }
        return Ok(());
      }

      // Still handshaking: keep shuttling. A handshake-only feed never fills the
      // received-plaintext buffer, so `Pending` here means the supplied flight
      // was consumed in full (`Intake::Done`) or made no progress; the
      // `consumed` guard bounds the loop either way.
      if matches!(intake, Intake::Done) || !consumed {
        return Ok(());
      }
    }
  }

  /// Established intake: feed `input` ciphertext through the record layer in
  /// bounded steps, draining decrypted plaintext into the `Stream` between steps,
  /// then map the close anchor. `eof` is the TCP `read == 0` marker (a
  /// zero-length real transport read); a retained-tail replay passes `eof =
  /// false` (the tail is buffered ciphertext, not an end-of-stream), and a
  /// `close_notify` carried in the tail is still observed via
  /// `records.peer_has_closed()`.
  ///
  /// rustls bounds its decrypted-plaintext buffer at 16 KiB (`set_buffer_limit`
  /// only raises the SEND side), so a single coalesced read of a valid frame
  /// larger than that cannot be fed in one pass: `read_tls` reports backpressure
  /// once the buffer fills. Each iteration makes progress (consumes >= 1
  /// ciphertext byte OR drains >= 1 plaintext byte) or breaks, so the loop is
  /// bounded by `input.len()` plus the decrypted plaintext.
  fn pump_in_established(&mut self, input: &[u8], eof: bool, now: Instant) -> Result<(), ()> {
    let mut offset = 0usize;
    let mut decode_failed = false;
    loop {
      // An intake `Err` is a `process_new_packets` failure (mTLS / decrypt /
      // malformed record), never backpressure: route it through the
      // atomic-retire-then-fail path so no half-applied merge / ack survives the
      // aborted exchange.
      let intake = match self.records.handle_transport_data(&input[offset..]) {
        Ok(intake) => intake,
        Err(e) => {
          self.fail_with_retire(BridgeFailure::Transport(format!("tls record: {e}")));
          return Err(());
        }
      };
      let consumed = match intake {
        Intake::Done => {
          offset = input.len();
          true
        }
        Intake::Pending(n) => {
          offset += n;
          n > 0
        }
      };

      // Drain decrypted application plaintext and feed it to the FSM. The borrow
      // on `records` ends before the borrow on `stream` begins (separate
      // statements over disjoint fields). Feeding plaintext in chunks is
      // equivalent to one feed: `Stream::handle_data` accumulates into its own
      // `input_buf` and decodes a complete frame as soon as it is buffered.
      let mut data = Vec::new();
      let drained = self.records.read_plaintext(&mut data);
      if !data.is_empty()
        && self
          .stream
          .as_mut()
          .expect("stream is Some in the established intake")
          .handle_data(&data, now)
          .is_err()
      {
        decode_failed = true;
      }

      // Terminate once all ciphertext is consumed; a decode failure also stops
      // the feed (the exchange is being torn down). Otherwise, if this step
      // made no progress at all, break to honor the bounded-work contract.
      if matches!(intake, Intake::Done) || decode_failed {
        break;
      }
      if !consumed && drained == 0 {
        break;
      }
    }

    // Close anchor. `peer_has_closed()` (peer sent `close_notify`) OR a TCP
    // `read == 0` is the EOF marker: feed `Stream::handle_data(&[], now)`, the
    // FSM's per-phase premature-vs-clean EOF decision. A premature EOF
    // (`StreamError::PeerClosed`) is the truncation path → decode failure; a
    // clean EOF (`Ok`) retires the recv half below via `observe_recv_fin`.
    let fin_seen = eof || self.records.peer_has_closed();
    if fin_seen
      && !decode_failed
      && self
        .stream
        .as_mut()
        .expect("stream is Some in the established intake")
        .handle_data(&[], now)
        .is_err()
    {
      decode_failed = true;
    }

    if decode_failed {
      // Atomic failure: retire our half + discard buffered inbound plaintext
      // BEFORE flipping the phase, so a same-tick reap cannot leak a
      // half-applied exchange. The FSM's failure variant is preserved in
      // `Stream::is_failed()`; `drain_then_reap`'s `StreamErrored` notice uses
      // the `BridgeFailure::Decode` high-level reason.
      self.fail_with_retire(BridgeFailure::Decode);
      return Err(());
    }

    // Only after the FSM accepted the EOF (clean phase, or already terminal)
    // do we retire the recv half. A premature-EOF rejection routed through the
    // decode-fail path above before reaching here, so `RecvClosed` is the
    // FSM-blessed recv-half-retired transition.
    if fin_seen {
      self.observe_recv_fin();
    }

    Ok(())
  }

  /// Drain any plaintext rustls buffered pre-promotion (and replay any retained
  /// ciphertext tail) through the established intake, SAME tick as the
  /// promotion. The coordinator drives this from its post-mint bridge pump
  /// (after [`Self::promote`]) so a first request coalesced with the peer's
  /// final handshake flight reaches the just-minted `Stream` without waiting for
  /// the next transport read.
  ///
  /// Two coalesced shapes both land here, and one [`Self::pump_in_established`]
  /// call covers BOTH:
  ///
  /// * LARGE first frame (> rustls's 16 KiB received-plaintext limit): the
  ///   handshake intake hit backpressure (`Intake::Pending`) and retained the
  ///   unconsumed ciphertext tail in [`Self::pending_ciphertext`]. The tail is
  ///   fed FIRST, then the interleaved drain reassembles the whole frame.
  /// * SMALL first frame (fits in one `read_tls` pass): rustls consumed the
  ///   whole coalesced read in ONE handshake-intake pass — NO backpressure, so
  ///   NO tail is retained — but the decrypted frame is sitting in rustls's
  ///   received-plaintext buffer and `peer_has_closed()` may already be latched.
  ///   Feeding an EMPTY slice through `pump_in_established` still drains that
  ///   buffered plaintext into the `Stream` and fires the close anchor.
  ///
  /// So the drain runs whenever a `Stream` exists, NOT only when a tail was
  /// retained: gating on a non-empty tail would strand the small-frame case's
  /// already-buffered plaintext + close anchor until a later transport read that
  /// a request-awaiting-reply peer will never send.
  ///
  /// The replay passes `eof = false`: a retained tail is buffered ciphertext the
  /// peer already sent, not an end-of-stream, and an empty feed is not a TCP
  /// `read == 0`. A `close_notify` carried in the coalesced read still fires the
  /// close anchor via `records.peer_has_closed()`, while a read WITHOUT
  /// `close_notify` (e.g. a push/pull request awaiting its response) does NOT
  /// prematurely signal EOF. Returns `Err(())` if the drain terminalized the
  /// bridge (decode / record failure), mirroring [`Self::handle_transport_data`].
  pub(crate) fn replay_pending(&mut self, now: Instant) -> Result<(), ()> {
    if self.stream.is_none() {
      return Ok(());
    }
    // Feed the retained tail (if any) FIRST, then drain. `std::mem::take`
    // clears `pending_ciphertext`; an empty tail still drains the buffered
    // received-plaintext and honors the close anchor.
    let combined = std::mem::take(&mut self.pending_ciphertext);
    self.pump_in_established(&combined, false, now)
  }

  /// Drain ciphertext the record layer wants to write into `out`. Returns the
  /// byte count appended. Used by the coordinator's `collect_transmits`.
  pub(crate) fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    self.records.poll_transport_transmit(out)
  }

  /// Promote a `Handshaking` bridge to `Established` with the freshly-minted
  /// `Stream`. Snapshots the `Stream`'s exchange deadline as the bridge
  /// deadline (the QUIC bridge's `expect`-on-`poll_timeout` invariant: a
  /// freshly dialed/accepted `Stream` is pre-`Done` with a `Some` deadline).
  pub(crate) fn promote(&mut self, stream: Stream<I, A>) {
    self.deadline = stream
      .poll_timeout()
      .expect("a freshly dialed/accepted Stream is pre-`Done` with a Some exchange deadline");
    self.stream = Some(stream);
    self.phase = TlsPhase::Established(BridgePhase::Active);
  }

  /// Drive the SEND-half transition: `Active → SendClosed`, or
  /// `RecvClosed → BothClosed`. No-op for already-`Failed`/-terminal states and
  /// during `Handshaking` (no `Stream` yet). Terminal states are sticky.
  ///
  /// Anchor: our `TlsRecords::send_close_notify()` queued the `close_notify`
  /// alert that retires the send half (vs. the QUIC bridge's
  /// `StreamEvent::Finished`).
  fn observe_send_fin(&mut self) {
    let TlsPhase::Established(bp) = &mut self.phase else {
      return;
    };
    *bp = match bp {
      BridgePhase::Active => BridgePhase::SendClosed,
      BridgePhase::RecvClosed => BridgePhase::BothClosed,
      BridgePhase::SendClosed | BridgePhase::BothClosed | BridgePhase::Failed(_) => return,
    };
  }

  /// Drive the RECV-half transition: `Active → RecvClosed`, or
  /// `SendClosed → BothClosed`. No-op for already-`Failed`/-terminal states and
  /// during `Handshaking`.
  ///
  /// Anchor: a clean `Stream::handle_data(&[], now)` after the peer's
  /// `close_notify` / TCP `read == 0` (vs. the QUIC bridge's
  /// `Chunks::next() -> Ok(None)`).
  fn observe_recv_fin(&mut self) {
    let TlsPhase::Established(bp) = &mut self.phase else {
      return;
    };
    *bp = match bp {
      BridgePhase::Active => BridgePhase::RecvClosed,
      BridgePhase::SendClosed => BridgePhase::BothClosed,
      BridgePhase::RecvClosed | BridgePhase::BothClosed | BridgePhase::Failed(_) => return,
    };
  }

  /// Idempotent retirement of both TLS halves: queue a `close_notify` (latched
  /// against a second call — `send_close_notify` is one-shot per rustls
  /// connection) and discard any decrypted-but-unconsumed inbound plaintext.
  /// Used by every failure transition, so a `BridgePhase::Failed` reap has the
  /// send half closed AND no buffered inbound plaintext by construction — the
  /// TLS analog of the QUIC bridge's `reset(0)` + `stop(0)` + `pending_out`
  /// clear (TLS has no bridge-side send buffer; `write_plaintext` encrypts
  /// straight into rustls, and a failed FSM's `enter_failed` clears its own
  /// `output_buf`, so the only bridge-reachable buffer to clear is the inbound
  /// plaintext rustls has decrypted but we have not yet fed to the `Stream`).
  fn retire_halves(&mut self) {
    if !self.close_notify_sent {
      self.records.send_close_notify();
      self.close_notify_sent = true;
    }
    let mut discard = Vec::new();
    self.records.read_plaintext(&mut discard);
  }

  /// Atomic FAILURE transition: retire both halves AND set
  /// `BridgePhase::Failed(reason)` in one step. The `is_terminal()` predicate
  /// is phase-authoritative, so a bridge that becomes `Failed` must already
  /// have its send half closed — otherwise a same-tick reap leaves the peer
  /// without a `close_notify`.
  ///
  /// Sticky — first failure wins; subsequent calls preserve the original cause
  /// for `drain_then_reap`'s `StreamErrored` notice. Idempotent retirement
  /// still runs (cheap, latched) so a recovery race that observes a prior
  /// `Failed` phase cannot leave the send half un-retired.
  fn fail_with_retire(&mut self, reason: BridgeFailure) {
    self.retire_halves();
    self.fail(reason);
  }

  /// FAILURE transition without retiring halves.
  ///
  /// Pre-FIN side-effect hygiene: if a `Stream` exists and the recv half has
  /// NOT yet been retired by peer-FIN observation (phase ∉ `RecvClosed` /
  /// `BothClosed`), discard the FSM's queued endpoint / lifecycle events. The
  /// `drain_payload_only` deferred-commit gate holds these events while the
  /// bridge is non-terminal pending peer FIN as protocol-level proof the
  /// exchange completed; `drain_then_reap` (the terminal D1 path) drains the
  /// queue unconditionally. Without this pre-FIN clear, a peer that sends one
  /// complete frame and then drops the connection (or whose `close_notify`
  /// never arrives) BEFORE the clean EOF would have its dispatched frame's side
  /// effects committed by `drain_then_reap` even though the exchange was never
  /// authorized. Mirrors `Stream::enter_failed`'s queue clearing on the
  /// FSM-failure path — same atomicity, applied at the transport-failure
  /// boundary. A `Handshaking` failure has no `Stream` and no queue, so the
  /// clear is skipped (the mTLS-reject path). `RecvClosed` / `BothClosed`
  /// failures preserve the queue: the clean EOF already authorized the
  /// dispatched events; the failure is orthogonal.
  fn fail(&mut self, reason: BridgeFailure) {
    if matches!(self.phase, TlsPhase::Established(BridgePhase::Failed(_))) {
      return;
    }
    if !matches!(
      self.phase,
      TlsPhase::Established(BridgePhase::RecvClosed | BridgePhase::BothClosed)
    ) {
      if let Some(stream) = self.stream.as_mut() {
        stream.discard_pending_events();
      }
    }
    self.phase = TlsPhase::Established(BridgePhase::Failed(reason));
  }

  /// The memberlist [`StreamId`] that correlates this bridge to its `Stream` —
  /// the observation seam for the coordinator / async driver shell to map a
  /// reliable exchange back to its `StreamId`. `None` during `Handshaking`
  /// (the `Stream` does not exist yet).
  #[allow(dead_code)]
  pub(crate) fn id(&self) -> Option<StreamId> {
    self.stream.as_ref().map(|s| s.id())
  }

  /// `true` once this bridge has queued its `close_notify` alert (clean
  /// `pump_out` half-close or a failure-path `retire_halves`). The coordinator
  /// reads this to emit one `TlsAction::Shutdown` per exchange after the final
  /// flush — the signal to half-close the TCP write side so the peer reads a
  /// clean EOF once it has drained our buffered ciphertext.
  pub(crate) fn close_notify_sent(&self) -> bool {
    self.close_notify_sent
  }

  /// `true` iff the bridge is in [`BridgePhase::Failed`] — observable shorthand
  /// for the `pump_out` leading-fatal guard and for `drain_then_reap`'s
  /// lifecycle-notice selection. Distinct from `Stream::is_failed()` (FSM-level
  /// failure, which cascades into the bridge via the failure transitions).
  fn is_phase_failed(&self) -> bool {
    matches!(self.phase, TlsPhase::Established(BridgePhase::Failed(_)))
  }

  /// The bridge's flush/lifetime deadline, folded into the unified `min` across
  /// active bridge / Endpoint timers in the coordinator's `poll_timeout`.
  ///
  /// During `Handshaking` (no `Stream`) this is the dial deadline — the only
  /// timer bounding the handshake. Once `Established`, returns the bridge's OWN
  /// snapshotted `deadline` `min` the inner stream's `poll_timeout()` (which
  /// goes `None` the moment `Stream::poll_transmit` flips an inbound-response /
  /// one-way-user-message phase to `Done`). Delegating solely to the inner
  /// stream would drop a `Done`-but-awaiting-peer-`close_notify` bridge out of
  /// the unified timer, so a peer that never sends `close_notify` would pin the
  /// bridge forever; the bridge deadline is therefore the lifetime authority,
  /// while a tighter inner deadline (e.g. the inbound-response `now + 5s`) is
  /// folded in via `min`. A terminal bridge is reaped this same tick and
  /// contributes no deadline.
  pub(crate) fn poll_timeout(&self) -> Option<Instant> {
    if self.is_terminal() {
      return None;
    }
    Some(match self.stream.as_ref().and_then(|s| s.poll_timeout()) {
      Some(inner) => inner.min(self.deadline),
      None => self.deadline,
    })
  }

  /// Force the bridge terminal with [`BridgeFailure::ConnectionLost`]. Used when
  /// the underlying TCP connection is lost: the byte pumps never observe the
  /// failure (the socket is gone), so the coordinator transitions the bridge
  /// through this entry and the same tick's inline D1 drain + `StreamErrored`
  /// reap runs as for any other transport error. No retirement: the socket is
  /// already gone, so a `close_notify` cannot be delivered.
  #[allow(dead_code)]
  pub(crate) fn fail_connection_lost(&mut self) {
    self.fail(BridgeFailure::ConnectionLost);
  }

  /// Pump outbound memberlist bytes through the TLS record layer.
  ///
  /// Handshake-deadline guard FIRST, before the no-`Stream` early return: a
  /// `Handshaking` bridge has no `Stream` and no FSM timer, so nothing else
  /// terminalizes it when its dial / `ACCEPT_HANDSHAKE_DEADLINE` budget elapses
  /// without the handshake completing (the empty-slice EOF anchor is a no-op
  /// while `process_new_packets` keeps succeeding, and the `is_done()`-gated
  /// flush-deadline path below needs a `Stream`). If `now >= self.deadline`
  /// while still `Handshaking`, fail to `Failed(Timeout)` with NO `Stream`
  /// minted so the coordinator's pre-`Stream` reap (`dial_failed` + `Close` +
  /// decrement) collects it — bounding a connect-refused / peer-silent dial AND
  /// a slowloris accept-side handshake stall. The `fail` transition has no
  /// queue to clear during `Handshaking`, matching the mTLS-reject path.
  ///
  /// Terminal-bridge guard NEXT: if a prior `handle_transport_data` made the
  /// bridge terminal this tick, or the inner `Stream` entered `Failed` on a
  /// previous tick (D1 reap still pending), every write path below must be
  /// unreachable — writing newly-yielded bytes would deliver a stale
  /// request/response (memberlist frames are length-delimited, so the peer
  /// applies a partial wire as a full message). The guard retires the halves,
  /// flips the phase to `Failed(Transport)`, and returns `Err(())`.
  ///
  /// The bridge-level flush deadline covers the `Done`-but-awaiting-peer-close
  /// gap: once `Stream::poll_transmit` flips a phase to `Done`, the FSM's
  /// `poll_timeout` is `None` and it can no longer self-enforce; if the bridge
  /// is not yet completion-terminal and `now >= self.deadline`, abandon the
  /// exchange (`Failed(Timeout)`) so the coordinator reaps it rather than
  /// leaking a bridge whose peer never sent `close_notify`.
  ///
  /// The send half queues `close_notify` exactly once (`close_notify_sent`
  /// guards a second call), on the first tick EITHER the memberlist `Stream`
  /// emitted its full request/response (`sent_any` — so a mid-exchange outbound
  /// request half-closes its send side *before* the stream is `Done`, so the
  /// peer can reply) OR the inner `Stream` is `Done` (so an inbound one-way
  /// `Message::UserData`, which reaches `Done` with NO outbound bytes and never
  /// arms `sent_any`, still half-closes and becomes reapable instead of leaking
  /// the bridge). It is never queued on an empty `poll_transmit` whose response
  /// buffer is merely not yet `stream_load_response`'d (that phase is
  /// `InboundSendingResponse`, not `Done`, with `sent_any` false), and it is
  /// gated against `Stream::is_failed()` / a failed phase so a timed-out or
  /// transport-errored stream cannot half-close cleanly on top of a failure.
  pub(crate) fn pump_out(&mut self, now: Instant) -> Result<(), ()> {
    // Handshake-deadline guard. A `Handshaking` bridge (no `Stream`, no FSM
    // timer) is otherwise never terminalized when its deadline elapses without
    // the handshake completing; fail it to `Failed(Timeout)` so the
    // coordinator's pre-`Stream` reap collects it. Runs before the no-`Stream`
    // early return, and only while `Handshaking` so the `Established` flush
    // deadline (the `is_done()`-gated path below) is unchanged.
    if matches!(self.phase, TlsPhase::Handshaking) && now >= self.deadline {
      self.fail(BridgeFailure::Timeout);
      return Err(());
    }

    if self.stream.is_none() {
      return Ok(());
    }

    // Terminal-bridge guard. A failed phase, or an FSM-failed inner stream,
    // means no further bytes may be written. Retire + fail atomically (the
    // failure transition is phase-authoritative) and return `Err`.
    let fsm_failed = self
      .stream
      .as_ref()
      .expect("stream is Some")
      .is_failed()
      .map(|e| e.to_string());
    if self.is_phase_failed() || fsm_failed.is_some() {
      let reason = match fsm_failed {
        Some(e) => BridgeFailure::Transport(e),
        None => BridgeFailure::Transport("bridge fatal".to_string()),
      };
      self.fail_with_retire(reason);
      return Err(());
    }

    // Bridge-level flush deadline — the `Done`-but-awaiting-peer-close gap.
    // The inner stream can no longer self-enforce its deadline once `Done`
    // (`poll_timeout` is `None`); enforce the bridge deadline so a peer that
    // never sends `close_notify` cannot pin the bridge.
    if now >= self.deadline
      && self.stream.as_ref().expect("stream is Some").is_done()
      && !self.is_terminal()
    {
      self.fail_with_retire(BridgeFailure::Timeout);
      return Err(());
    }

    // Drain plaintext from the FSM into the record layer. rustls buffers into
    // an unbounded send queue (set in the `TlsRecords` constructors), so there
    // is no `WriteError::Blocked` and no retained-tail loop: each yield is
    // fully encrypted in one call. The `poll_transmit` borrow on `stream` ends
    // before the `write_plaintext` borrow on `records` begins.
    let mut buf = Vec::new();
    loop {
      buf.clear();
      let yielded = self
        .stream
        .as_mut()
        .expect("stream is Some")
        .poll_transmit(now, &mut buf)
        .is_some();
      if !yielded {
        break;
      }
      // `poll_transmit` yielded this exchange's output: the send half now owes
      // a `close_notify`.
      self.sent_any = true;
      self.records.write_plaintext(&buf);
    }

    // FSM-failure detection AFTER `poll_transmit`: the FSM checks its own
    // deadline inside `poll_transmit` and transitions to `Failed(Timeout)` if
    // elapsed (yielding nothing). Mirror it into `BridgePhase::Failed` + retire
    // atomically so the next tick reaps a fully-retired bridge.
    let fsm_failed = self
      .stream
      .as_ref()
      .expect("stream is Some")
      .is_failed()
      .map(|e| e.to_string());
    if let Some(e) = fsm_failed {
      self.fail_with_retire(BridgeFailure::Transport(format!("stream failed: {e}")));
      return Err(());
    }

    // Queue `close_notify` exactly once. Reaching here guarantees
    // `poll_transmit` is exhausted, so an inbound response pre-`stream_load_response`
    // (phase `InboundSendingResponse`, `is_done()` false, `sent_any` false)
    // does not half-close before the reply is encrypted. A failed / fatal
    // stream MUST NOT half-close cleanly — the gate forbids it.
    let done = self.stream.as_ref().expect("stream is Some").is_done();
    let failed = self
      .stream
      .as_ref()
      .expect("stream is Some")
      .is_failed()
      .is_some();
    if !self.close_notify_sent && !self.is_phase_failed() && !failed && (self.sent_any || done) {
      self.records.send_close_notify();
      self.close_notify_sent = true;
      self.observe_send_fin();
    }

    Ok(())
  }

  /// `true` once the bridge has reached a terminal [`BridgePhase`] — either
  /// `BothClosed` (clean: both halves retired) or `Failed` (any failure path).
  /// The coordinator then stops pumping and runs [`Self::drain_then_reap`].
  ///
  /// Phase is the SINGLE source of truth. The failure transitions retire the
  /// send half (`close_notify`) and discard buffered inbound plaintext
  /// atomically BEFORE flipping the phase, so a `Failed`-phase bridge is
  /// retired by construction. No `Stream::is_failed()` fallback: every
  /// FSM-internal failure is observed by `pump_out` / `handle_transport_data`
  /// and cascaded into `Failed` via the atomic-retire-then-fail helpers;
  /// falling back to `Stream::is_failed()` for terminality would let a reap
  /// fire before the retirement step.
  pub(crate) fn is_terminal(&self) -> bool {
    matches!(
      self.phase,
      TlsPhase::Established(BridgePhase::BothClosed | BridgePhase::Failed(_))
    )
  }

  /// D1 drain-before-reap. In strict order:
  ///
  /// 1. drain every queued [`Stream::poll_endpoint_event`] into the
  ///    [`Endpoint`] (the push/pull reply / `ReliablePingAcked` lives here);
  /// 2. route each returned [`StreamCommand`] back to this stream
  ///    (`SendPushPullResponse` -> encode + load + flush our reply;
  ///    `Close` -> fail);
  /// 3. **only then** deliver the `StreamClosed` / `StreamErrored` lifecycle
  ///    notice.
  ///
  /// The caller removes the bridge **only after** this returns. The order is
  /// load-bearing: delivering the lifecycle notice before the payload events
  /// makes the `Endpoint` tear the exchange down before the join reply /
  /// reliable-ping ack is applied. A `Handshaking` bridge has no `Stream` and
  /// no events, so this is a no-op (the coordinator reaps a failed-handshake
  /// bridge directly).
  pub(crate) fn drain_then_reap(&mut self, ep: &mut Endpoint<I, A>, now: Instant) {
    if self.stream.is_none() {
      return;
    }
    while let Some(ev) = self
      .stream
      .as_mut()
      .expect("stream is Some")
      .poll_endpoint_event()
    {
      if let Some(cmd) = ep.handle_stream_event(ev, now) {
        match cmd {
          StreamCommand::SendPushPullResponse {
            local_states,
            user_data,
          } => {
            // `handle_stream_event` returns the response state UNENCODED with
            // the inbound stream's `output_buf` still empty: encode the
            // snapshot and load it before any of it can be transmitted, or the
            // peer is left with a half-applied merge (split-brain). The
            // bridge-level `self.deadline` is advanced to the SAME `now + 5s`
            // value `stream_load_response` writes into the inner stream so the
            // `Done`-but-awaiting-peer-close abandon does not fire on the stale
            // accept deadline before the fresh response window elapses.
            let response_deadline = now + core::time::Duration::from_secs(5);
            let encoded =
              Endpoint::<I, A>::encode_push_pull_response(&local_states, user_data, false);
            Endpoint::<I, A>::stream_load_response(
              self.stream.as_mut().expect("stream is Some"),
              encoded,
              response_deadline,
            );
            self.deadline = response_deadline;
            // Ignoring Err: `pump_out` failing here terminalizes the bridge,
            // which the lifecycle-notice selection below reflects as
            // `StreamErrored` — there is no separate action to take.
            let _ = self.pump_out(now);
          }
          StreamCommand::Close => {
            // Admission rejection (`MergeDelegate::notify_merge -> false`).
            // Atomic failure → the lifecycle notice surfaces as `StreamErrored`
            // carrying the rejection.
            self.fail_with_retire(BridgeFailure::AdmissionClosed);
          }
        }
      }
    }

    // Recv-half retirement is structurally guaranteed by [`BridgePhase`]:
    //   * `BothClosed` — `observe_recv_fin` already fired on the clean EOF.
    //   * `Failed(_)` — the failure transition retired the send half and
    //     discarded buffered inbound plaintext; the read side is retired by
    //     ceasing reads.
    // An adversarial peer that never sends `close_notify` does not produce
    // `BothClosed` — the bridge stays non-terminal until `self.deadline`
    // elapses, at which point `pump_out`'s flush-deadline path transitions to
    // `Failed(Timeout)`.
    let id = self.stream.as_ref().expect("stream is Some").id();
    let fsm_failed = self.stream.as_ref().expect("stream is Some").is_failed();
    let notice = match (&self.phase, fsm_failed) {
      (TlsPhase::Established(BridgePhase::Failed(reason)), _) => {
        let err = match reason {
          BridgeFailure::Timeout => "exchange deadline elapsed".to_string(),
          BridgeFailure::Transport(s) => format!("transport: {s}"),
          BridgeFailure::Decode => "decode failed".to_string(),
          BridgeFailure::ConnectionLost => "connection lost".to_string(),
          BridgeFailure::AdmissionClosed => "merge rejected by delegate".to_string(),
        };
        EndpointEvent::StreamErrored { id, err }
      }
      (_, Some(e)) => EndpointEvent::StreamErrored {
        id,
        err: e.to_string(),
      },
      _ => EndpointEvent::StreamClosed { id },
    };
    // Ignoring Err: the post-payload lifecycle notice
    // (`StreamClosed`/`StreamErrored`) returns no actionable `StreamCommand` —
    // the bridge is being reaped this tick and no outbound work can run after
    // the notice.
    let _ = ep.handle_stream_event(notice, now);
  }

  /// Drain a **non-terminal** stream's queued [`Stream::poll_endpoint_event`]
  /// into the [`Endpoint`] every tick, routing each returned [`StreamCommand`]
  /// with the **same** handling as [`Self::drain_then_reap`] — but **without**
  /// the post-loop lifecycle notice.
  ///
  /// Gated on `BridgePhase::RecvClosed`: a decoded frame's side effects
  /// (`PushPullRequestReceived`, `ReliablePingAcked`, `UserDataReceived`, …)
  /// queue on dispatch BEFORE the stream proves there are no trailing bytes.
  /// Holding the commit until the recv half observes the clean EOF means an
  /// adversarial `[valid_frame][trailing junk]` split across ticks fails the
  /// stream (via `enter_failed`, which clears the queue) before the chunk-1
  /// side effects are ever committed. The encode + load for an inbound
  /// push/pull response still runs here, though: without it the reply for an
  /// in-flight inbound exchange is never produced and the peer is left with a
  /// half-applied merge. Mirrors [`Self::drain_then_reap`] minus the lifecycle
  /// notice.
  pub(crate) fn drain_payload_only(&mut self, ep: &mut Endpoint<I, A>, now: Instant) {
    if !matches!(self.phase, TlsPhase::Established(BridgePhase::RecvClosed)) {
      return;
    }
    while let Some(ev) = self
      .stream
      .as_mut()
      .expect("stream is Some past the handshake window")
      .poll_endpoint_event()
    {
      if let Some(cmd) = ep.handle_stream_event(ev, now) {
        match cmd {
          StreamCommand::SendPushPullResponse {
            local_states,
            user_data,
          } => {
            let response_deadline = now + core::time::Duration::from_secs(5);
            let encoded =
              Endpoint::<I, A>::encode_push_pull_response(&local_states, user_data, false);
            Endpoint::<I, A>::stream_load_response(
              self.stream.as_mut().expect("stream is Some"),
              encoded,
              response_deadline,
            );
            self.deadline = response_deadline;
            // Ignoring Err: see `drain_then_reap` — a `pump_out` failure
            // terminalizes the bridge, which the next-tick reap reflects.
            let _ = self.pump_out(now);
          }
          StreamCommand::Close => {
            // Terminalize this tick so the coordinator's post-`drain_payload_only`
            // `is_terminal()` re-check reaps within the SAME tick rather than
            // pinning the bridge until its exchange deadline.
            self.fail_with_retire(BridgeFailure::AdmissionClosed);
          }
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
  };

  use bytes::Bytes;
  use rustls::pki_types::ServerName;
  use smol_str::SmolStr;

  use super::super::records::TlsRecords;
  use crate::{
    config::EndpointConfig,
    error::StreamError,
    event::Event,
    tls::crypto::tests::{test_client, test_server},
  };

  fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
  }

  /// Build a `Handshaking` client/server bridge pair sharing the accept-any
  /// test configs (real rustls handshake, deterministic — no test-only crypto
  /// hook; the determinism is the virtual clock).
  fn handshaking_pair(
    deadline: Instant,
  ) -> (
    TlsBridge<SmolStr, SocketAddr>,
    TlsBridge<SmolStr, SocketAddr>,
  ) {
    let client = TlsRecords::client(
      Arc::new(test_client()),
      ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();
    let server = TlsRecords::server(Arc::new(test_server())).unwrap();
    (
      TlsBridge::new(client, deadline),
      TlsBridge::new(server, deadline),
    )
  }

  /// Shuttle ciphertext both ways once: client out -> server in, server out ->
  /// client in. Returns `true` if either side produced bytes this round.
  fn shuttle(
    client: &mut TlsBridge<SmolStr, SocketAddr>,
    server: &mut TlsBridge<SmolStr, SocketAddr>,
    now: Instant,
  ) -> bool {
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      server.handle_transport_data(&c_out, now).unwrap();
    }
    let mut s_out = Vec::new();
    server.poll_transport_transmit(&mut s_out);
    if !s_out.is_empty() {
      client.handle_transport_data(&s_out, now).unwrap();
    }
    !c_out.is_empty() || !s_out.is_empty()
  }

  /// Run the handshake pump to completion on both sides.
  fn complete_handshake(
    client: &mut TlsBridge<SmolStr, SocketAddr>,
    server: &mut TlsBridge<SmolStr, SocketAddr>,
    now: Instant,
  ) {
    for _ in 0..64 {
      if !shuttle(client, server, now) {
        break;
      }
    }
    assert!(!client.is_handshaking() && !server.is_handshaking());
  }

  /// (a) The handshake pump shuttles ciphertext through both bridges until
  /// neither `is_handshaking()`. This is the pre-`Stream` window the
  /// coordinator runs before minting the `Stream`.
  #[test]
  fn handshake_pump_completes_both_sides() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
    assert!(client.is_handshaking() && server.is_handshaking());
    for _ in 0..64 {
      let mut c_out = Vec::new();
      client.poll_transport_transmit(&mut c_out);
      if !c_out.is_empty() {
        server.handle_transport_data(&c_out, now).unwrap();
      }
      let mut s_out = Vec::new();
      server.poll_transport_transmit(&mut s_out);
      if !s_out.is_empty() {
        client.handle_transport_data(&s_out, now).unwrap();
      }
      if c_out.is_empty() && s_out.is_empty() {
        break;
      }
    }
    assert!(!client.is_handshaking(), "client handshake completed");
    assert!(!server.is_handshaking(), "server handshake completed");
  }

  /// (a) `promote` installs the freshly-minted `Stream`, snapshots its exchange
  /// deadline, and transitions `Handshaking → Established(Active)`.
  #[test]
  fn promote_installs_stream_and_enters_established() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
    complete_handshake(&mut client, &mut server, now);

    let mut ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    let stream = ep.accept_stream(addr(7001), now);
    let want_deadline = stream
      .poll_timeout()
      .expect("inbound stream has a deadline");

    assert!(server.stream.is_none());
    server.promote(stream);
    assert!(server.stream.is_some(), "promote installed the Stream");
    assert!(
      matches!(server.phase, TlsPhase::Established(BridgePhase::Active)),
      "promote entered Established(Active)"
    );
    assert_eq!(
      server.deadline, want_deadline,
      "promote snapshotted the stream's exchange deadline"
    );
    assert!(
      !server.is_handshaking(),
      "an Established bridge is not handshaking"
    );
  }

  /// (b) A one-way `UserData` frame round-trips through the byte pump:
  /// outbound client `Stream::poll_transmit` -> `write_plaintext` ->
  /// ciphertext -> server `handle_transport_data` -> `read_plaintext` ->
  /// inbound `Stream::handle_data`. (c) close_notify both ways drives both
  /// bridges to `BothClosed`, then the terminal D1 reap surfaces the decoded
  /// `UserData` as `Event::UserPacket` on the server Endpoint.
  #[test]
  fn frame_round_trips_then_clean_close_reaps() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
    complete_handshake(&mut client, &mut server, now);

    // Outbound client: a one-way reliable user message.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7100)));
    let payload = Bytes::from_static(b"hello-tls");
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);

    // Inbound server: accept the exchange.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    let s_stream = ep_s.accept_stream(addr(7100), now);
    server.promote(s_stream);

    // Pump the request out of the client and into the server, shuttling
    // ciphertext (incl. the eventual close_notify) until both bridges reap.
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, now);
      // Drain non-terminal inbound side effects every tick (D1 contract).
      if !server.is_terminal() {
        server.drain_payload_only(&mut ep_s, now);
      }
      if client.is_terminal() && server.is_terminal() {
        break;
      }
      if !moved && client.is_terminal() && server.is_terminal() {
        break;
      }
    }

    assert!(
      matches!(client.phase, TlsPhase::Established(BridgePhase::BothClosed)),
      "client reached BothClosed (sent request + close_notify, saw peer close_notify), got {:?}",
      debug_phase(&client.phase)
    );
    assert!(
      matches!(server.phase, TlsPhase::Established(BridgePhase::BothClosed)),
      "server reached BothClosed, got {:?}",
      debug_phase(&server.phase)
    );

    // Terminal D1 reap on the server: the decoded UserData surfaces as
    // Event::UserPacket (clean close → StreamClosed lifecycle notice).
    server.drain_then_reap(&mut ep_s, now);
    let mut got = None;
    while let Some(ev) = ep_s.poll_event() {
      if let Event::UserPacket { data, .. } = ev {
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the round-tripped UserData frame surfaced on the server Endpoint"
    );
  }

  /// (b') A `UserData` frame whose plaintext exceeds rustls's 16 KiB
  /// received-plaintext limit, delivered to the server as ONE coalesced
  /// `handle_transport_data` call, must NOT panic: the bridge interleaves
  /// bounded record intake with plaintext draining (respecting rustls
  /// backpressure) so the whole frame decrypts and decodes into the `Stream`.
  /// Feeding the whole ciphertext in one shot would instead drive the record
  /// layer's `read_tls` into a buffer-full `Err`; the interleave keeps each
  /// intake step within the receive limit.
  #[test]
  fn coalesced_large_frame_decodes_without_panic() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
    complete_handshake(&mut client, &mut server, now);

    // Outbound client: a one-way reliable user message whose payload (48 KiB)
    // far exceeds the 16 KiB received-plaintext limit but is well under the
    // 64 MiB max_stream_frame_size — a VALID large frame.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7400)));
    let payload = Bytes::from((0..48 * 1024).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);

    // Inbound server: accept the exchange.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    let s_stream = ep_s.accept_stream(addr(7400), now);
    server.promote(s_stream);

    // Pump the request out of the client and COALESCE every ciphertext byte the
    // client emits into ONE buffer (the single-TCP-read worst case rustls
    // backpressure exposes), rather than shuttling chunk-by-chunk.
    let mut coalesced = Vec::new();
    for _ in 0..64 {
      client.pump_out(now).ok();
      let before = coalesced.len();
      client.poll_transport_transmit(&mut coalesced);
      if coalesced.len() == before {
        break;
      }
    }
    assert!(
      coalesced.len() > 16 * 1024,
      "the coalesced request ciphertext exceeds the received-plaintext limit, got {}",
      coalesced.len()
    );

    // Deliver the WHOLE coalesced request in ONE call. This is the path that
    // panicked before the interleave fix.
    server
      .handle_transport_data(&coalesced, now)
      .expect("a coalesced large frame is accepted, not a transport failure");

    // The server decoded the large UserData and queued it for D1 drain; finish
    // the close handshake so the terminal reap surfaces it.
    server.drain_payload_only(&mut ep_s, now);
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, now);
      if !server.is_terminal() {
        server.drain_payload_only(&mut ep_s, now);
      }
      if client.is_terminal() && server.is_terminal() {
        break;
      }
      if !moved {
        break;
      }
    }

    server.drain_then_reap(&mut ep_s, now);
    let mut got = None;
    while let Some(ev) = ep_s.poll_event() {
      if let Event::UserPacket { data, .. } = ev {
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the full >16 KiB UserData frame decoded intact from a single coalesced read"
    );
  }

  /// (b'') A coalesced read carrying the dialer's FINAL handshake flight
  /// (TLS 1.3 client `Finished`) immediately followed by a `UserData` frame
  /// whose plaintext exceeds rustls's 16 KiB received-plaintext limit, delivered
  /// to a server bridge that is STILL `Handshaking` (no `Stream` yet) in ONE
  /// `handle_transport_data` call. The coordinator-equivalent sequence —
  /// promote the freshly-minted `Stream` then pump the same tick — must
  /// reassemble the WHOLE frame into the `Stream` (decodes intact), not a
  /// truncated ~16 KiB prefix.
  ///
  /// This is the pre-promotion coalescing path: `process_new_packets` completes
  /// the handshake AND buffers the trailing app records in the SAME call, and
  /// the >16 KiB app frame trips rustls backpressure (`Intake::Pending`) before
  /// a `Stream` exists to drain into. The unconsumed ciphertext tail must be
  /// retained and replayed post-promotion.
  #[test]
  fn coalesced_handshake_final_plus_large_frame_pre_promotion_reassembles() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

    // Drive the handshake one flight at a time until the CLIENT has completed
    // its handshake (it has sent its `Finished`) but the SERVER has NOT yet
    // consumed that final flight — so the server is still `Handshaking` with no
    // `Stream`. Once the client finishes the loop breaks WITHOUT handing the
    // client's final flight to the server: it is withheld here and coalesced
    // with the large app frame below.
    for _ in 0..64 {
      if !client.is_handshaking() {
        break;
      }
      let mut c_out = Vec::new();
      client.poll_transport_transmit(&mut c_out);
      if !c_out.is_empty() {
        server.handle_transport_data(&c_out, now).unwrap();
      }
      let mut s_out = Vec::new();
      server.poll_transport_transmit(&mut s_out);
      if !s_out.is_empty() {
        client.handle_transport_data(&s_out, now).unwrap();
      }
      if c_out.is_empty() && s_out.is_empty() {
        break;
      }
    }
    assert!(
      !client.is_handshaking(),
      "client completed its handshake (sent Finished)"
    );
    assert!(
      server.is_handshaking(),
      "server has NOT yet consumed the client's final flight — still Handshaking"
    );

    // Mint + promote the CLIENT side (the dialer) with a large one-way user
    // message, then pump it so the client's `Finished` + the >16 KiB app frame
    // are produced together.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7500)));
    let payload = Bytes::from((0..48 * 1024).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);
    client.pump_out(now).expect("client pumps its request");

    // COALESCE the client's withheld `Finished` flight + the whole >16 KiB app
    // frame into ONE buffer (a single TCP read on the server).
    let mut coalesced = Vec::new();
    for _ in 0..64 {
      let before = coalesced.len();
      client.poll_transport_transmit(&mut coalesced);
      if coalesced.len() == before {
        break;
      }
    }
    assert!(
      coalesced.len() > 16 * 1024,
      "the coalesced [final flight][>16 KiB frame] exceeds the received-plaintext limit, got {}",
      coalesced.len()
    );

    // Deliver the WHOLE coalesced buffer to the still-`Handshaking` server in
    // ONE call. `process_new_packets` completes the handshake AND buffers the
    // app records; the >16 KiB frame trips backpressure before a `Stream` exists.
    server
      .handle_transport_data(&coalesced, now)
      .expect("the coalesced final-flight + large frame is accepted");
    assert!(
      !server.is_handshaking(),
      "the server's handshake completed inside the coalesced read"
    );

    // Coordinator-equivalent: mint + promote the inbound `Stream`, then pump the
    // SAME tick. The retained pre-promotion ciphertext tail must replay into the
    // freshly-promoted `Stream` so the WHOLE frame reassembles.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    let s_stream = ep_s.accept_stream(addr(7500), now);
    server.promote(s_stream);
    // Drive the replay exactly as the coordinator's post-mint `pump_bridges`
    // does: `replay_pending` feeds the retained tail through the established
    // interleave (no synthetic EOF). The full frame must reassemble into the
    // just-promoted `Stream`.
    server
      .replay_pending(now)
      .expect("post-promote replay of the retained tail decodes the frame");

    server.drain_payload_only(&mut ep_s, now);
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, now);
      if !server.is_terminal() {
        server.drain_payload_only(&mut ep_s, now);
      }
      if client.is_terminal() && server.is_terminal() {
        break;
      }
      if !moved {
        break;
      }
    }

    server.drain_then_reap(&mut ep_s, now);
    let mut got = None;
    while let Some(ev) = ep_s.poll_event() {
      if let Event::UserPacket { data, .. } = ev {
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the full >16 KiB frame coalesced with the final handshake flight decoded intact"
    );
  }

  /// (b''') A coalesced read carrying the dialer's FINAL handshake flight
  /// (TLS 1.3 client `Finished`), a SMALL first app frame (well under the 16 KiB
  /// received-plaintext limit), and the dialer's `close_notify`, delivered to a
  /// server bridge that is STILL `Handshaking` (no `Stream` yet) in ONE
  /// `handle_transport_data` call — with NO further transport read after.
  ///
  /// Unlike the large-frame sibling, the whole coalesced buffer is consumed by
  /// rustls in ONE `read_tls`/`process_new_packets` pass: the small frame never
  /// fills the received-plaintext buffer, so NO backpressure occurs and the
  /// handshake intake retains NO ciphertext tail (`pending_ciphertext` stays
  /// empty). The decrypted small frame is buffered inside `TlsRecords` and
  /// `peer_has_closed()` is already latched. After promote + `replay_pending`,
  /// the coordinator-equivalent post-mint pump MUST drain that buffered
  /// plaintext into the freshly-promoted `Stream` (the frame decodes) AND fire
  /// the recv-half close anchor — even though there is no retained tail. Before
  /// the fix `replay_pending` no-ops on an empty tail, so the buffered frame is
  /// never delivered and the close anchor never fires; with NO further transport
  /// read the exchange would stall to its deadline.
  #[test]
  fn coalesced_handshake_final_plus_small_frame_and_close_notify_pre_promotion_drains() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

    // Drive the handshake one flight at a time until the CLIENT has completed
    // its handshake (sent its `Finished`) but the SERVER has NOT yet consumed
    // that final flight — so the server is still `Handshaking` with no `Stream`.
    // The client's final flight is withheld here and coalesced with the small
    // app frame + `close_notify` below.
    for _ in 0..64 {
      if !client.is_handshaking() {
        break;
      }
      let mut c_out = Vec::new();
      client.poll_transport_transmit(&mut c_out);
      if !c_out.is_empty() {
        server.handle_transport_data(&c_out, now).unwrap();
      }
      let mut s_out = Vec::new();
      server.poll_transport_transmit(&mut s_out);
      if !s_out.is_empty() {
        client.handle_transport_data(&s_out, now).unwrap();
      }
      if c_out.is_empty() && s_out.is_empty() {
        break;
      }
    }
    assert!(
      !client.is_handshaking(),
      "client completed its handshake (sent Finished)"
    );
    assert!(
      server.is_handshaking(),
      "server has NOT yet consumed the client's final flight — still Handshaking"
    );

    // Mint + promote the CLIENT side (the dialer) with a SMALL one-way user
    // message, then pump it so the client's `Finished` + the small app frame +
    // the dialer's `close_notify` (a one-way message half-closes once its
    // request is sent) are produced together.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7600)));
    let payload = Bytes::from_static(b"small-coalesced-first-frame");
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);
    client.pump_out(now).expect("client pumps its request");
    assert!(
      client.close_notify_sent(),
      "a one-way user message half-closes after its request — close_notify queued"
    );

    // COALESCE the client's withheld `Finished` flight + the whole small app
    // frame + `close_notify` into ONE buffer (a single TCP read on the server).
    let mut coalesced = Vec::new();
    for _ in 0..64 {
      let before = coalesced.len();
      client.poll_transport_transmit(&mut coalesced);
      if coalesced.len() == before {
        break;
      }
    }
    assert!(
      coalesced.len() < 16 * 1024,
      "the coalesced [final flight][small frame][close_notify] is well under the \
       received-plaintext limit (no backpressure), got {}",
      coalesced.len()
    );

    // Deliver the WHOLE coalesced buffer to the still-`Handshaking` server in
    // ONE call. `process_new_packets` completes the handshake, buffers the small
    // app records, AND observes the `close_notify` — all in this single pass, so
    // NO backpressure trips and NO ciphertext tail is retained.
    server
      .handle_transport_data(&coalesced, now)
      .expect("the coalesced final-flight + small frame + close_notify is accepted");
    assert!(
      !server.is_handshaking(),
      "the server's handshake completed inside the coalesced read"
    );
    assert!(
      server.pending_ciphertext.is_empty(),
      "the small coalesced read is fully consumed in one pass — NO retained tail"
    );

    // Coordinator-equivalent: mint + promote the inbound `Stream`, then drive
    // the post-mint pump's `replay_pending` exactly as `pump_bridges` does. With
    // NO retained tail, the replay must STILL drain the buffered decrypted
    // plaintext into the just-promoted `Stream` and fire the recv-half close
    // anchor (`peer_has_closed()`), or the small frame never reaches the FSM.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    let s_stream = ep_s.accept_stream(addr(7600), now);
    server.promote(s_stream);
    server
      .replay_pending(now)
      .expect("post-promote replay drains the buffered small frame + close anchor");

    // The close anchor fired (peer `close_notify` observed): the recv half is
    // retired. With a one-way inbound user message reaching `Done` and the recv
    // half retired, the bridge converges to terminal so the D1 reap can run.
    server.drain_payload_only(&mut ep_s, now);
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, now);
      if !server.is_terminal() {
        server.drain_payload_only(&mut ep_s, now);
      }
      if client.is_terminal() && server.is_terminal() {
        break;
      }
      if !moved {
        break;
      }
    }
    assert!(
      server.is_terminal(),
      "the recv-half close anchor fired and the one-way exchange reaped, got {:?}",
      debug_phase(&server.phase)
    );

    server.drain_then_reap(&mut ep_s, now);
    let mut got = None;
    while let Some(ev) = ep_s.poll_event() {
      if let Event::UserPacket { data, .. } = ev {
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the small first frame coalesced with the final handshake flight reached the \
       Stream and decoded intact (drained post-promotion with no retained tail)"
    );
  }

  /// (d) mTLS reject: a handshake-time `TlsRecords` error (here a corrupted
  /// handshake flight, the same `process_new_packets` `Err` path a
  /// client-cert rejection takes) tears the bridge down with NO `Stream`
  /// minted and ZERO endpoint side effects. The bridge becomes terminal so the
  /// coordinator reaps it directly.
  #[test]
  fn handshake_failure_tears_down_with_no_stream_and_no_endpoint_events() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

    // Drive the client's first flight, then corrupt it before the server reads
    // it — the server's `process_new_packets` rejects the malformed record,
    // exactly as it rejects an unacceptable client identity.
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    assert!(!c_out.is_empty(), "client produced a ClientHello flight");
    for b in c_out.iter_mut() {
      *b ^= 0xff;
    }

    assert!(server.is_handshaking());
    let res = server.handle_transport_data(&c_out, now);
    assert!(res.is_err(), "the server rejected the corrupted flight");

    // No `Stream` was minted, the bridge is terminal, and no endpoint events
    // exist (there is no Endpoint involvement on the reject path at all).
    assert!(
      server.stream.is_none(),
      "no Stream minted on a handshake reject"
    );
    assert!(
      server.is_terminal(),
      "the bridge is terminal after the reject"
    );
    assert!(
      matches!(
        server.phase,
        TlsPhase::Established(BridgePhase::Failed(BridgeFailure::Transport(_)))
      ),
      "the reject is a Transport failure, got {:?}",
      debug_phase(&server.phase)
    );

    // `drain_then_reap` on a no-Stream bridge is a clean no-op (the coordinator
    // reaps a failed-handshake bridge without an FSM lifecycle notice).
    let mut ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    server.drain_then_reap(&mut ep, now);
    assert!(
      ep.poll_event().is_none(),
      "no endpoint events on the reject path"
    );
  }

  /// (e) Truncation: a TCP `read == 0` (zero-length `handle_transport_data`)
  /// mid-frame, with no peer `close_notify`, feeds `Stream::handle_data(&[])`,
  /// which the FSM rejects with `StreamError::PeerClosed` for a non-terminal
  /// awaiting phase. The bridge transitions to `Failed(Decode)` and is
  /// terminal.
  #[test]
  fn truncation_read_zero_mid_frame_fails_peer_closed() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
    complete_handshake(&mut client, &mut server, now);

    // Outbound client awaiting a response: a reliable ping fallback. After the
    // request is pumped, the inner FSM is `OutboundAwaitingResponse` — a
    // premature peer close is `PeerClosed`.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7200)));
    let sid = ep_c.start_reliable_ping(
      SmolStr::new("srv"),
      addr(7000),
      7,
      now + Duration::from_secs(5),
    );
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound ping stream");
    client.promote(c_stream);

    // Pump the request out so the FSM advances to OutboundAwaitingResponse.
    client.pump_out(now).ok();
    assert!(
      !client.is_terminal(),
      "client is mid-exchange awaiting the ack"
    );

    // TCP read == 0 before the ack arrives: truncation.
    let res = client.handle_transport_data(&[], now);
    assert!(res.is_err(), "a mid-frame read==0 fails the bridge");
    assert!(
      matches!(
        client.phase,
        TlsPhase::Established(BridgePhase::Failed(BridgeFailure::Decode))
      ),
      "truncation maps to Failed(Decode), got {:?}",
      debug_phase(&client.phase)
    );
    assert!(client.is_terminal(), "a truncated exchange is terminal");
    assert!(
      matches!(
        client.stream.as_ref().and_then(|s| s.is_failed()),
        Some(StreamError::PeerClosed)
      ),
      "the inner FSM failed with PeerClosed"
    );

    // The unused `server` still proves the handshake completed both ways.
    assert!(!server.is_handshaking());
  }

  /// (f) Handshake timeout: a `Handshaking` bridge whose deadline elapses
  /// WITHOUT the handshake completing is terminalized by `pump_out` with NO
  /// `Stream` minted, so the coordinator's pre-`Stream` reap collects it. This
  /// is the slowloris / connect-refused bound — without it a stalled handshake
  /// leaks the bridge (the empty-slice EOF anchor is a no-op while
  /// `process_new_packets` keeps succeeding, and the `is_done()`-gated flush
  /// deadline needs a `Stream` that does not exist yet).
  #[test]
  fn handshaking_bridge_times_out_at_deadline_with_no_stream() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    let (mut client, _server) = handshaking_pair(deadline);

    // Before the deadline a `pump_out` is a clean no-op (still Handshaking,
    // still surfacing the deadline as its only timer).
    assert!(client.is_handshaking());
    assert!(!client.is_terminal());
    client.pump_out(now).expect("pre-deadline pump is a no-op");
    assert!(
      client.is_handshaking(),
      "still handshaking before the deadline"
    );
    assert_eq!(
      client.poll_timeout(),
      Some(deadline),
      "a Handshaking bridge surfaces its dial deadline as the only timer"
    );

    // At the deadline `pump_out` terminalizes the stalled handshake.
    let res = client.pump_out(deadline);
    assert!(res.is_err(), "a stalled handshake at its deadline fails");
    assert!(
      client.is_terminal(),
      "the bridge is terminal after the handshake deadline elapses"
    );
    assert!(
      client.stream.is_none(),
      "no Stream is minted on a handshake timeout"
    );
    assert!(
      matches!(
        client.phase,
        TlsPhase::Established(BridgePhase::Failed(BridgeFailure::Timeout))
      ),
      "the handshake timeout maps to Failed(Timeout), got {:?}",
      debug_phase(&client.phase)
    );
    assert!(
      !client.is_handshaking(),
      "a timed-out bridge is no longer handshaking"
    );
    // A terminal bridge contributes no deadline (it is reaped this tick).
    assert_eq!(client.poll_timeout(), None);

    // `drain_then_reap` on a no-`Stream` bridge is a clean no-op — no FSM
    // lifecycle notice is owed for a handshake that never minted a `Stream`.
    let mut ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7300)));
    client.drain_then_reap(&mut ep, deadline);
    assert!(
      ep.poll_event().is_none(),
      "no endpoint events on the handshake-timeout path"
    );
  }

  /// Render a `TlsPhase` for assert messages without requiring `Debug` on the
  /// (private, accessor-only) enum.
  fn debug_phase(p: &TlsPhase) -> &'static str {
    match p {
      TlsPhase::Handshaking => "Handshaking",
      TlsPhase::Established(BridgePhase::Active) => "Established(Active)",
      TlsPhase::Established(BridgePhase::SendClosed) => "Established(SendClosed)",
      TlsPhase::Established(BridgePhase::RecvClosed) => "Established(RecvClosed)",
      TlsPhase::Established(BridgePhase::BothClosed) => "Established(BothClosed)",
      TlsPhase::Established(BridgePhase::Failed(_)) => "Established(Failed)",
    }
  }
}
