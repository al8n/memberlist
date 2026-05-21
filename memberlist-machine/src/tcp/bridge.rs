//! `memberlist::Stream` <-> `RawRecords` byte-pump for one reliable exchange.
//!
//! Phase: `Handshaking` (no `Stream` yet) → `Established(BridgePhase)` (the
//! byte pump runs and the half-close lifecycle tracks via the shared
//! [`BridgePhase`]). The `Stream` is minted by the coordinator at
//! `dial_succeeded` / `accept_stream` once
//! [`RawRecords::is_handshaking`](super::records::RawRecords::is_handshaking)
//! clears the bridge-mint gate. For the acceptor that gate clears once the
//! inbound label is read and validated (the bridge mustn't dispatch a merge
//! until the cluster is confirmed right); for the dialer it is open from
//! construction (classic memberlist sends `[label][request]` together — see
//! `memberlist-core/src/state.rs::push_pull_node` /
//! `memberlist-core/src/network.rs::reliable_encoder`), so a dialer bridge
//! mints its `Stream` in the same tick the request is queued and runs the
//! inbound response-label validation in-line on the established intake.
//!
//! Mirrors [`crate::tls::bridge::TlsBridge`] method-for-method, with the rustls
//! record layer replaced by the raw-passthrough
//! [`RawRecords`](super::records::RawRecords) codec. D1 (drain-before-reap) and
//! the atomic-failure / queue-clear discipline are reused verbatim from the TLS
//! bridge: every failure transition clears the FSM's queued endpoint events
//! before the terminal phase, so no queued merge / ack survives an aborted
//! exchange.
//!
//! # Transport anchors and the FIN-only half-close
//!
//! The TLS bridge anchors its half-close on in-band `close_notify`: its
//! `send_close_notify()` queues an alert that retires the send half, and the
//! peer's close is the `close_notify` latch (`peer_has_closed()`). Plain TCP has
//! NO in-band close record — `RawRecords::send_close_notify()` emits no bytes
//! and `RawRecords::peer_has_closed()` is always `false` (frozen `memberlist-net`
//! reliable close is the out-of-band TCP FIN). The two anchors therefore become:
//!
//! * **RECV close** is driven SOLELY by the transport `eof` (TCP `read == 0`)
//!   the coordinator passes into the byte pump. The TLS structure
//!   `fin = eof || records.peer_has_closed()` is kept verbatim — the second
//!   term is just always `false` for TCP — so an `eof` feeds
//!   `Stream::handle_data(&[], now)` and, on a clean accept, retires the recv
//!   half (`RecvClosed`); a premature `eof` mid-frame is the truncation path
//!   (`PeerClosed` → decode failure).
//! * **SEND close** still calls `RawRecords::send_close_notify()` and
//!   transitions the send half to `SendClosed`, exactly as the TLS bridge does —
//!   but since that call produces no bytes, our actual TCP FIN is the
//!   out-of-band shutdown-write the driver issues. The bridge records that its
//!   send half is retired in the [`Self::fin_sent`] latch (the structural analog
//!   of the TLS bridge's `close_notify_sent`) and surfaces it through
//!   [`Self::fin_owed`]; the coordinator (a later task) maps that to a driver
//!   shutdown-write / close action, mirroring how the TLS coordinator maps
//!   `close_notify_sent()` to `TlsAction::Shutdown`.
//!
//! There is no transport back-pressure inside [`RawRecords`] (it never returns
//! `Intake::Pending` — raw passthrough has no crypto buffer to fill), so the TLS
//! bridge's retained-ciphertext-tail handling collapses: [`Self::pending_inbound`]
//! is always empty and [`Self::replay_pending`] is the unconditional
//! drain-when-a-`Stream`-exists. The coalesced shape it still covers is a peer
//! that sends `[label][first request]` in one read before the coordinator mints
//! the `Stream`: `RawRecords` strips the label and buffers the first request as
//! inbound plaintext, which the post-promotion drain feeds into the `Stream`.

#![cfg(feature = "tcp")]

use std::time::Instant;

use super::records::{Intake, RawRecords};
use crate::{
  bridge_phase::{BridgeFailure, BridgePhase},
  endpoint::Endpoint,
  event::{EndpointEvent, StreamCommand, StreamId},
  stream::Stream,
};

/// Lifecycle of one composed `(memberlist Stream, RawRecords)` exchange.
/// `Handshaking` precedes the `Stream`; `Established` carries the shared
/// half-close [`BridgePhase`]. Newtype variant over `BridgePhase` (the
/// no-multi-field-tuple-variant rule).
enum TcpPhase {
  /// No `Stream` yet. For an acceptor this is the inbound-label window: bytes
  /// flow into `RawRecords` until the label is validated. For a dialer this
  /// is a single-tick state before the coordinator's
  /// `service_handshake_completions` mints the `Stream` (the dialer's
  /// `RawRecords::is_handshaking()` is already `false` at construction).
  Handshaking,
  /// `Stream` minted; the byte pump runs and the half-close lifecycle is
  /// tracked by the inner [`BridgePhase`].
  Established(BridgePhase),
}

/// Couples one reliable-exchange [`Stream`] to one [`RawRecords`], pumping
/// plaintext through the raw-passthrough record layer both ways. Built
/// `Handshaking` (pre-`Stream`); the coordinator promotes it to `Established`
/// once the label step settles and the `Stream` is minted. Accessor-only.
pub(crate) struct TcpBridge<I, A> {
  /// `None` until the label step settles and the coordinator mints the
  /// `Stream`. The `Handshaking` phase is exactly the `stream.is_none()` window.
  stream: Option<Stream<I, A>>,
  records: RawRecords,
  /// `true` once `Stream::poll_transmit` yielded this exchange's output bytes
  /// (so the deferred send-half close is owed). Mirrors the TLS bridge's
  /// `sent_any` and its inbound-response-not-yet-loaded subtlety: an inbound
  /// stream whose response has not been `stream_load_response`'d also yields
  /// `None` from `poll_transmit` (its phase is `InboundSendingResponse`, not
  /// `Done`), so closing the send half on the first empty poll would half-close
  /// before the reply is written. The owed close is taken only once
  /// `poll_transmit` yields nothing more.
  sent_any: bool,
  /// `true` once this bridge has retired its send half — the structural analog
  /// of the TLS bridge's `close_notify_sent`. For TLS the latch records that the
  /// `close_notify` alert was queued; here it records that our send-half close
  /// is owed to the driver as an out-of-band TCP FIN (a shutdown-write), since
  /// `RawRecords::send_close_notify()` emits no in-band bytes. Distinct from the
  /// `BridgePhase::SendClosed` transition (which records that the send half is
  /// retired): the two move in lockstep, but the latch is still needed because
  /// both `retire_halves` (failure path) and `pump_out` (clean path) may reach
  /// the close call and it must run at most once. Surfaced via [`Self::fin_owed`].
  fin_sent: bool,
  /// Vestigial for plain TCP: the TLS bridge retains the unconsumed ciphertext
  /// tail when its handshake completes mid-feed (TLS 1.3 coalesces the final
  /// flight with the first app records, and a >16 KiB first frame trips rustls's
  /// received-plaintext backpressure before a `Stream` exists). [`RawRecords`]
  /// has no crypto buffer to fill and never returns `Intake::Pending`, so its
  /// `handle_transport_data` always consumes all input — the coalesced
  /// `[label][first request]` is stripped of its label and surfaced as inbound
  /// plaintext in one call, leaving NO bridge-level tail to retain. This field
  /// therefore stays empty; it is kept so [`Self::replay_pending`] mirrors the
  /// TLS structure exactly (and a future generic extraction over "a Sans-I/O
  /// record unit" stays identical). The buffered first request still reaches the
  /// `Stream` via `replay_pending`'s unconditional drain.
  ///
  /// Companion: [`Self::pending_eof`] is the active sibling — it carries the
  /// pre-promote out-of-band TCP FIN signal across the promote boundary so
  /// the post-promotion drain honors it. Together they form the pre-promote
  /// carry-set.
  pending_inbound: Vec<u8>,
  /// Out-of-band TCP FIN latched while the bridge is still in `Handshaking`
  /// (pre-promote), so the post-promotion [`Self::replay_pending`] drain
  /// observes it. TLS uses the in-band `close_notify` latch on its records
  /// layer (`TlsRecords::peer_has_closed()`); plain TCP has no in-band close
  /// signal — the FIN is the driver's `read == 0`, a transport-level event the
  /// records layer cannot observe — so the latch lives on the bridge instead,
  /// keeping [`super::records::RawRecords`] strictly transport-agnostic.
  ///
  /// Set whenever a zero-length [`Self::handle_transport_data`] feed is
  /// delivered while `phase == Handshaking`; read by [`Self::replay_pending`]
  /// to seed `eof` into the post-promotion `pump_in_established` so the
  /// recv-half retirement (or the premature-EOF truncation path) fires on the
  /// SAME tick as the promote rather than waiting for a later transport read
  /// the peer (already half-closed) will never produce. Sticky like
  /// [`Self::fin_sent`] — once latched it is never cleared, so repeated
  /// replays observe the same EOF (idempotent).
  pending_eof: bool,
  phase: TcpPhase,
  /// The exchange deadline, snapshotted from the inner `Stream` at promotion
  /// (mirrors the TLS bridge's `deadline` field). During `Handshaking` the
  /// deadline is the dial / accept deadline, supplied by the coordinator, and is
  /// the only timer the bridge contributes (no `Stream` exists yet to fold in
  /// via `min`).
  deadline: Instant,
}

impl<I, A> TcpBridge<I, A>
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
  /// Build a `Handshaking` bridge wrapping a fresh `RawRecords`. The dial /
  /// accept deadline bounds the label exchange; the `Stream` (and its own
  /// exchange deadline) is installed by [`Self::promote`] once the label step
  /// settles.
  pub(crate) fn new(records: RawRecords, deadline: Instant) -> Self {
    Self {
      stream: None,
      records,
      sent_any: false,
      fin_sent: false,
      pending_inbound: Vec::new(),
      pending_eof: false,
      phase: TcpPhase::Handshaking,
      deadline,
    }
  }

  /// `true` while the bridge is still gating the `Stream` mint on its record
  /// layer. The acceptor stays handshaking until the inbound label is
  /// validated; the dialer never (its
  /// [`RawRecords::is_handshaking`](super::records::RawRecords::is_handshaking)
  /// is `false` from construction). Once `!records.is_handshaking()` and the
  /// coordinator calls [`Self::promote`] the bridge moves to `Established`;
  /// the phase is still `Handshaking` in that one-tick window, so both
  /// conjuncts gate the pre-`Stream` state.
  pub(crate) fn is_handshaking(&self) -> bool {
    matches!(self.phase, TcpPhase::Handshaking) && self.records.is_handshaking()
  }

  /// Feed bytes the driver read from the TCP connection. A zero-length slice is
  /// the TCP `read == 0` (peer half-closed the socket) anchor. Returns `Err(())`
  /// if the record layer rejected the bytes (label mismatch) — the bridge
  /// becomes terminal and the coordinator reaps it with NO `Stream` minted
  /// (during `Handshaking`) or via the `StreamErrored` D1 path (during
  /// `Established`).
  ///
  /// During `Handshaking` a `RawRecords` rejection just makes the bridge
  /// terminal: there is no `Stream` and no FSM queue to clear, and no endpoint
  /// side effect can have happened (this is the label-mismatch-reject path, the
  /// structural analog of the TLS bridge's mTLS-reject path).
  ///
  /// During `Established` the flow mirrors the TLS bridge's `pump_in_established`:
  /// a `RawRecords` rejection routes through the atomic-retire-then-fail helper
  /// (`BridgeFailure::Transport`); plaintext is drained and fed to
  /// `Stream::handle_data` (a decode `Err` → `BridgeFailure::Decode`); and the
  /// close anchor — the TCP `read == 0` EOF — feeds `Stream::handle_data(&[],
  /// now)`. A non-terminal-phase `Err` (`StreamError::PeerClosed`) is the
  /// truncation path → `BridgeFailure::Decode`; a clean `Ok(())` advances
  /// `observe_recv_fin()` → `RecvClosed`.
  ///
  /// A zero-length feed delivered while `phase == Handshaking` (the pre-promote
  /// window) latches [`Self::pending_eof`] so the post-promotion
  /// [`Self::replay_pending`] honors the out-of-band FIN. Without this latch a
  /// driver delivering `[label||first request]` + the same-tick TCP `read == 0`
  /// as two coordinator-level calls — bytes first, then the empty-slice EOF
  /// anchor — would have its EOF dropped (the empty-slice arm of
  /// [`Self::intake_handshaking`] is a no-op while the bridge is still
  /// pre-`Stream`), and the post-promote `pump_in_established` would never
  /// observe the recv-half FIN (`RawRecords::peer_has_closed()` is
  /// permanently `false`, since TCP close is out of band — see [`Self::pending_eof`]).
  pub(crate) fn handle_transport_data(&mut self, data: &[u8], now: Instant) -> Result<(), ()> {
    // Pre-`Stream` (label) window: shuttle bytes until the label settles,
    // retaining any tail for post-promotion replay (always empty for TCP — see
    // `pending_inbound`). Once promoted, the established intake feeds the record
    // layer and drains the surfaced plaintext into the `Stream`.
    if self.stream.is_none() {
      // Latch a same-tick out-of-band FIN delivered while still pre-`Stream` so
      // the post-promote `replay_pending` honors it (see `pending_eof`). The
      // bridge is still pre-`Stream` here, so the empty-slice intake itself is a
      // no-op and the latch is the only carrier of the EOF signal across the
      // promote boundary. Robust to ordering: a coalesced `[label||first request]`
      // delivered as `(bytes, eof=true)` reaches this branch with
      // `phase == Handshaking` regardless of whether `bytes` or `&[]` arrives
      // first (the empty-slice second call latches; the first call surfaces the
      // request as inbound plaintext, leaving the bridge still pre-`Stream` since
      // the mint runs in `service_handshake_completions`).
      if data.is_empty() {
        self.pending_eof = true;
      }
      return self.intake_handshaking(data, now);
    }

    // Established: a real transport read (a zero-length slice is the TCP
    // `read == 0` EOF anchor). A non-empty retained pre-promotion tail is
    // replayed FIRST, ahead of the newly-supplied data, so the request
    // reassembles in wire order. (`pending_inbound` is always empty for TCP, so
    // the second arm is vestigial — kept to mirror the TLS structure.)
    let eof = data.is_empty();
    if self.pending_inbound.is_empty() {
      self.pump_in_established(data, eof, now)
    } else {
      let mut combined = std::mem::take(&mut self.pending_inbound);
      combined.extend_from_slice(data);
      self.pump_in_established(&combined, eof, now)
    }
  }

  /// Pre-`Stream` intake. Feeds bytes into the record layer until either all
  /// supplied input is consumed or the label step settles mid-feed.
  ///
  /// A peer may coalesce its `[label]` prefix with its first application
  /// request into ONE transport read: `RawRecords::handle_transport_data` then
  /// settles the label (`is_handshaking()` flips false) AND surfaces the
  /// trailing request bytes as inbound plaintext in the SAME call. Unlike the
  /// TLS bridge there is no backpressure, so [`RawRecords`] consumes all input
  /// in one pass and the bridge retains NO tail (`pending_inbound` stays empty);
  /// the buffered request is drained into the just-minted `Stream` by
  /// [`Self::replay_pending`] post-promotion.
  ///
  /// An [`Intake::Failed`] is a label rejection (mismatch / double-label /
  /// over-long / non-UTF-8). During `Handshaking` it just terminalizes the
  /// bridge (no `Stream`, no queue, no endpoint side effect — the
  /// label-mismatch-reject path).
  fn intake_handshaking(&mut self, data: &[u8], now: Instant) -> Result<(), ()> {
    let mut offset = 0usize;
    loop {
      let intake = self.records.handle_transport_data(&data[offset..], now);
      let consumed = match intake {
        // Raw passthrough consumes all supplied input in one call.
        Intake::Done => {
          offset = data.len();
          true
        }
        // Never produced by `RawRecords` (no crypto backpressure). Treated as a
        // bounded-progress step for surface parity with the TLS record unit.
        Intake::Pending(n) => {
          offset += n;
          n > 0
        }
        // Label rejection: terminalize with NO `Stream` minted — the
        // label-mismatch-reject path (the TLS bridge's mTLS-reject analog).
        Intake::Failed => {
          self.fail(BridgeFailure::Transport("tcp label rejected".to_string()));
          return Err(());
        }
      };

      // The label step just settled inside this feed: any trailing request is
      // already surfaced as inbound plaintext inside `RawRecords`, drained by
      // `replay_pending` post-promotion. There is no bridge-level tail to retain
      // (raw passthrough consumes all input), so `pending_inbound` stays empty.
      if !self.records.is_handshaking() {
        if offset < data.len() {
          self.pending_inbound.extend_from_slice(&data[offset..]);
        }
        return Ok(());
      }

      // Still settling the label (a partial header was buffered): keep
      // shuttling. The `consumed` guard bounds the loop; an `Intake::Done` that
      // left us still handshaking means the partial header was buffered in full.
      if matches!(intake, Intake::Done) || !consumed {
        return Ok(());
      }
    }
  }

  /// Established intake: feed `input` through the record layer in bounded steps,
  /// draining surfaced plaintext into the `Stream` between steps, then map the
  /// close anchor. `eof` is the TCP `read == 0` marker (a zero-length real
  /// transport read); a retained-tail replay passes `eof = false` (the tail is
  /// buffered data, not an end-of-stream).
  ///
  /// [`RawRecords`] has no decrypted-plaintext buffer to fill and never returns
  /// `Intake::Pending`, so a single read is consumed in one pass — the TLS
  /// bridge's bounded-interleave loop collapses to one iteration here. The loop
  /// shape is kept verbatim (each iteration makes progress or breaks) so the two
  /// bridges stay structurally identical.
  fn pump_in_established(&mut self, input: &[u8], eof: bool, now: Instant) -> Result<(), ()> {
    let mut offset = 0usize;
    let mut decode_failed = false;
    loop {
      // An `Intake::Failed` is a label rejection — never produced once the label
      // step has settled (this is the `Established` path), but routed through the
      // atomic-retire-then-fail path defensively so no half-applied merge / ack
      // survives an aborted exchange.
      let intake = self.records.handle_transport_data(&input[offset..], now);
      let consumed = match intake {
        Intake::Done => {
          offset = input.len();
          true
        }
        // Never produced by `RawRecords`; treated as `Done` for the bounded-work
        // contract (the carried count would be bytes consumed before
        // backpressure). See `RawRecords::Intake`.
        Intake::Pending(n) => {
          offset += n;
          n > 0
        }
        Intake::Failed => {
          self.fail_with_retire(BridgeFailure::Transport("tcp record rejected".to_string()));
          return Err(());
        }
      };

      // Drain surfaced plaintext and feed it to the FSM. The borrow on `records`
      // ends before the borrow on `stream` begins (separate statements over
      // disjoint fields). Feeding plaintext in chunks is equivalent to one feed:
      // `Stream::handle_data` accumulates into its own `input_buf` and decodes a
      // complete frame as soon as it is buffered.
      let mut payload = Vec::new();
      let drained = self.records.read_plaintext(&mut payload);
      if !payload.is_empty()
        && self
          .stream
          .as_mut()
          .expect("stream is Some in the established intake")
          .handle_data(&payload, now)
          .is_err()
      {
        decode_failed = true;
      }

      // Terminate once all input is consumed; a decode failure also stops the
      // feed (the exchange is being torn down). Otherwise, if this step made no
      // progress at all, break to honor the bounded-work contract.
      if matches!(intake, Intake::Done) || decode_failed {
        break;
      }
      if !consumed && drained == 0 {
        break;
      }
    }

    // Close anchor. A TCP `read == 0` is the EOF marker (`records.peer_has_closed()`
    // is always `false` for plain TCP, so `eof` is the sole driver): feed
    // `Stream::handle_data(&[], now)`, the FSM's per-phase premature-vs-clean
    // EOF decision. A premature EOF (`StreamError::PeerClosed`) is the
    // truncation path → decode failure; a clean EOF (`Ok`) retires the recv half
    // below via `observe_recv_fin`.
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

  /// Drain any plaintext the record layer buffered pre-promotion through the
  /// established intake, SAME tick as the promotion. The coordinator drives this
  /// from its post-mint bridge pump (after [`Self::promote`]) so a first request
  /// coalesced with the peer's `[label]` prefix reaches the just-minted `Stream`
  /// without waiting for the next transport read.
  ///
  /// The TLS bridge distinguishes a retained large-frame ciphertext tail from a
  /// small frame rustls already buffered; for plain TCP there is no backpressure
  /// and so no retained tail (`pending_inbound` stays empty — see its docs). The
  /// drain still runs whenever a `Stream` exists, NOT only when a tail was
  /// retained: a peer that coalesced `[label][first request]` had that request
  /// stripped of its label and buffered as inbound plaintext inside
  /// [`RawRecords`] while the bridge was still `Handshaking`; feeding an EMPTY
  /// slice through [`Self::pump_in_established`] drains that buffered plaintext
  /// into the `Stream`. Gating on a non-empty tail would strand it until a later
  /// transport read that a request-awaiting-reply peer will never send.
  ///
  /// The replay seeds `eof` from the [`Self::pending_eof`] latch: a same-tick
  /// out-of-band FIN that arrived while the bridge was still `Handshaking` is
  /// honored here, so a coalesced `[label||first request]||FIN` delivery
  /// terminalizes on the same tick as the promote rather than stalling to the
  /// exchange deadline. A peer that has NOT yet half-closed leaves the latch
  /// `false`, so a push/pull request awaiting its response is not prematurely
  /// signaled EOF. The latch is sticky (never cleared), mirroring
  /// [`Self::fin_sent`] on the send side: `replay_pending` is idempotent, so a
  /// subsequent same-tick call observes the same EOF and the FSM's already-EOF
  /// state is a no-op. Returns `Err(())` if the drain terminalized the bridge
  /// (decode / record failure), mirroring [`Self::handle_transport_data`].
  pub(crate) fn replay_pending(&mut self, now: Instant) -> Result<(), ()> {
    if self.stream.is_none() {
      return Ok(());
    }
    // Feed the retained tail (if any) FIRST, then drain. `std::mem::take`
    // clears `pending_inbound` (always empty for TCP); an empty tail still
    // drains the buffered surfaced plaintext and honors the close anchor.
    let combined = std::mem::take(&mut self.pending_inbound);
    self.pump_in_established(&combined, self.pending_eof, now)
  }

  /// Drain bytes the record layer wants to write into `out`. Returns the byte
  /// count appended. Used by the coordinator's `collect_transmits`.
  pub(crate) fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    self.records.poll_transport_transmit(out)
  }

  /// Promote a `Handshaking` bridge to `Established` with the freshly-minted
  /// `Stream`. Snapshots the `Stream`'s exchange deadline as the bridge deadline
  /// (mirrors the TLS bridge's `expect`-on-`poll_timeout` invariant: a freshly
  /// dialed/accepted `Stream` is pre-`Done` with a `Some` deadline).
  pub(crate) fn promote(&mut self, stream: Stream<I, A>) {
    self.deadline = stream
      .poll_timeout()
      .expect("a freshly dialed/accepted Stream is pre-`Done` with a Some exchange deadline");
    self.stream = Some(stream);
    self.phase = TcpPhase::Established(BridgePhase::Active);
  }

  /// Drive the SEND-half transition: `Active → SendClosed`, or
  /// `RecvClosed → BothClosed`. No-op for already-`Failed`/-terminal states and
  /// during `Handshaking` (no `Stream` yet). Terminal states are sticky.
  ///
  /// Anchor: our send-half close (the out-of-band TCP FIN owed to the driver via
  /// [`Self::fin_owed`]), recorded by `RawRecords::send_close_notify()` — the
  /// no-op analog of the TLS bridge's `close_notify` alert.
  fn observe_send_fin(&mut self) {
    let TcpPhase::Established(bp) = &mut self.phase else {
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
  /// Anchor: a clean `Stream::handle_data(&[], now)` after the peer's TCP
  /// `read == 0` (the sole recv-close driver for plain TCP — there is no in-band
  /// `close_notify`).
  fn observe_recv_fin(&mut self) {
    let TcpPhase::Established(bp) = &mut self.phase else {
      return;
    };
    *bp = match bp {
      BridgePhase::Active => BridgePhase::RecvClosed,
      BridgePhase::SendClosed => BridgePhase::BothClosed,
      BridgePhase::RecvClosed | BridgePhase::BothClosed | BridgePhase::Failed(_) => return,
    };
  }

  /// Idempotent retirement of both halves: record the send-half close (latched
  /// against a second call) and discard any surfaced-but-unconsumed inbound
  /// plaintext. Used by every failure transition, so a `BridgePhase::Failed`
  /// reap has the send half closed AND no buffered inbound plaintext by
  /// construction. `RawRecords::send_close_notify()` is a no-op (the TCP FIN is
  /// the out-of-band shutdown-write the driver issues once it sees
  /// [`Self::fin_owed`]); the latch is what records that our send half is
  /// retired. A failed FSM's `enter_failed` clears its own `output_buf`, so the
  /// only bridge-reachable buffer to clear is the inbound plaintext the record
  /// layer surfaced but we have not yet fed to the `Stream`.
  fn retire_halves(&mut self) {
    if !self.fin_sent {
      self.records.send_close_notify();
      self.fin_sent = true;
    }
    let mut discard = Vec::new();
    self.records.read_plaintext(&mut discard);
  }

  /// Atomic FAILURE transition: retire both halves AND set
  /// `BridgePhase::Failed(reason)` in one step. The `is_terminal()` predicate is
  /// phase-authoritative, so a bridge that becomes `Failed` must already have its
  /// send half closed — otherwise a same-tick reap leaves the driver without the
  /// FIN signal.
  ///
  /// Sticky — first failure wins; subsequent calls preserve the original cause
  /// for `drain_then_reap`'s `StreamErrored` notice. Idempotent retirement still
  /// runs (cheap, latched) so a recovery race that observes a prior `Failed`
  /// phase cannot leave the send half un-retired.
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
  /// complete frame and then drops the connection BEFORE the clean EOF would
  /// have its dispatched frame's side effects committed by `drain_then_reap`
  /// even though the exchange was never authorized. Mirrors
  /// `Stream::enter_failed`'s queue clearing on the FSM-failure path — same
  /// atomicity, applied at the transport-failure boundary. A `Handshaking`
  /// failure has no `Stream` and no queue, so the clear is skipped (the
  /// label-mismatch-reject path). `RecvClosed` / `BothClosed` failures preserve
  /// the queue: the clean EOF already authorized the dispatched events; the
  /// failure is orthogonal.
  fn fail(&mut self, reason: BridgeFailure) {
    if matches!(self.phase, TcpPhase::Established(BridgePhase::Failed(_))) {
      return;
    }
    if !matches!(
      self.phase,
      TcpPhase::Established(BridgePhase::RecvClosed | BridgePhase::BothClosed)
    ) {
      if let Some(stream) = self.stream.as_mut() {
        stream.discard_pending_events();
      }
    }
    // Drop any bytes queued by `RawRecords` — the dialer's eager label
    // prefix (queued at construction by `RawRecords::dialer`), any
    // application bytes the FSM had written into the record layer before
    // this tick's failure, and an acceptor's lazy label prefix if it had
    // already validated its inbound label (lazy queue fired in
    // `RawRecords::handle_transport_data`'s `Accepted` branch) and then
    // failed mid-reply. A pre-validation acceptor failure
    // (`intake_handshaking` label-mismatch / handshake-deadline slow-loris)
    // sees an empty `records.outbound` because the lazy queue had not yet
    // fired, so the clear is a no-op on those paths — the acceptor's lazy
    // queue is the primary guard against pre-validation label disclosure;
    // this clear is the symmetric mid-exchange / dialer guard. A `Failed`
    // bridge has no business retaining outbound bytes: the coordinator's
    // reap path drains `records.outbound` into `out_transmit` via
    // `collect_bridge_transmits` AFTER `purge_transmit_for` has dropped any
    // already-drained chunks tagged with the exchange. Placing the clear
    // here covers EVERY failure transition uniformly —
    // `fail_with_retire` reaches `fail` via its own atomic
    // retire-then-fail step. Clean closes go through `pump_out` /
    // `observe_send_fin` and never call `fail`, so a same-tick clean
    // half-close cannot be observed clearing outbound here.
    self.records.clear_outbound();
    self.phase = TcpPhase::Established(BridgePhase::Failed(reason));
  }

  /// The memberlist [`StreamId`] that correlates this bridge to its `Stream` —
  /// the observation seam for the coordinator to map a reliable exchange back to
  /// its `StreamId`. `None` during `Handshaking` (the `Stream` does not exist
  /// yet).
  #[allow(dead_code)]
  pub(crate) fn id(&self) -> Option<StreamId> {
    self.stream.as_ref().map(|s| s.id())
  }

  /// `true` once this bridge has retired its send half (clean `pump_out`
  /// half-close or a failure-path `retire_halves`) and therefore OWES the driver
  /// an out-of-band TCP FIN (a shutdown-write on the connection's write side).
  ///
  /// The coordinator (a later task) reads this to emit one shutdown-write per
  /// exchange after the final flush — the signal that lets the peer read a clean
  /// EOF (its `read == 0`) once it has drained our buffered bytes. This is the
  /// structural analog of the TLS bridge's `close_notify_sent()`, which the TLS
  /// coordinator maps to `TlsAction::Shutdown`; the difference is purely that our
  /// send-half close is out-of-band (the TCP FIN) rather than an in-band
  /// `close_notify` record, so no bytes are emitted by the record layer for it.
  pub(crate) fn fin_owed(&self) -> bool {
    self.fin_sent
  }

  /// `true` iff the bridge is in [`BridgePhase::Failed`] — observable shorthand
  /// for the `pump_out` leading-fatal guard and for `drain_then_reap`'s
  /// lifecycle-notice selection. Distinct from `Stream::is_failed()` (FSM-level
  /// failure, which cascades into the bridge via the failure transitions).
  fn is_phase_failed(&self) -> bool {
    matches!(self.phase, TcpPhase::Established(BridgePhase::Failed(_)))
  }

  /// `true` iff this bridge has reached a failed terminal phase
  /// ([`BridgePhase::Failed`]). The coordinator uses this to distinguish a
  /// failed reap (stale pre-failure outbound chunks must be purged from
  /// `out_transmit` so they do not leak after the bridge is torn down) from a
  /// clean [`BridgePhase::BothClosed`] reap (response chunks queued by earlier
  /// pumps are legitimate wire bytes that must reach the peer).
  ///
  /// A `Handshaking` bridge failure (label rejection / handshake-deadline
  /// timeout) ALSO transitions to `Established(Failed(_))` — there is no
  /// distinct `Handshaking(Failed)` variant — so this predicate covers every
  /// failure transition the failed-bridge tests exercise (label-mismatch,
  /// handshake-deadline, dial-deadline, decode-fail, transport reset). Both
  /// [`Self::is_terminal`] (which also returns `true` for `BothClosed`) and
  /// `Stream::is_failed()` (which inspects the inner FSM's terminal error,
  /// independent of the bridge phase) are deliberately separate observations.
  pub(crate) fn is_failed(&self) -> bool {
    self.is_phase_failed()
  }

  /// The bridge's flush/lifetime deadline, folded into the unified `min` across
  /// active bridge / Endpoint timers in the coordinator's `poll_timeout`.
  ///
  /// During `Handshaking` (no `Stream`) this is the dial / accept deadline — the
  /// only timer bounding the label exchange. Once `Established`, returns the
  /// bridge's OWN snapshotted `deadline` `min` the inner stream's `poll_timeout()`
  /// (which goes `None` the moment `Stream::poll_transmit` flips an
  /// inbound-response / one-way-user-message phase to `Done`). Delegating solely
  /// to the inner stream would drop a `Done`-but-awaiting-peer-FIN bridge out of
  /// the unified timer, so a peer that never closes its send half would pin the
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
  /// already gone, so a FIN cannot be delivered.
  #[allow(dead_code)]
  pub(crate) fn fail_connection_lost(&mut self) {
    self.fail(BridgeFailure::ConnectionLost);
  }

  /// Pump outbound memberlist bytes through the record layer.
  ///
  /// Handshake-deadline guard FIRST, before the no-`Stream` early return: a
  /// `Handshaking` bridge has no `Stream` and no FSM timer, so nothing else
  /// terminalizes it when its dial / accept budget elapses without the label
  /// step settling (the empty-slice EOF anchor is a no-op, and the
  /// `is_done()`-gated flush-deadline path below needs a `Stream`). If
  /// `now >= self.deadline` while still `Handshaking`, fail to `Failed(Timeout)`
  /// with NO `Stream` minted so the coordinator's pre-`Stream` reap collects it
  /// — bounding a connect-refused / peer-silent dial AND a slowloris accept-side
  /// label stall. The `fail` transition has no queue to clear during
  /// `Handshaking`, matching the label-mismatch-reject path.
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
  /// exchange (`Failed(Timeout)`) so the coordinator reaps it rather than leaking
  /// a bridge whose peer never closed its send half.
  ///
  /// The send half is closed exactly once (`fin_sent` guards a second call), on
  /// the first tick EITHER the memberlist `Stream` emitted its full
  /// request/response (`sent_any` — so a mid-exchange outbound request
  /// half-closes its send side *before* the stream is `Done`, so the peer can
  /// reply) OR the inner `Stream` is `Done` (so an inbound one-way
  /// `Message::UserData`, which reaches `Done` with NO outbound bytes and never
  /// arms `sent_any`, still half-closes and becomes reapable instead of leaking
  /// the bridge). It is never taken on an empty `poll_transmit` whose response
  /// buffer is merely not yet `stream_load_response`'d (that phase is
  /// `InboundSendingResponse`, not `Done`, with `sent_any` false), and it is
  /// gated against `Stream::is_failed()` / a failed phase so a timed-out or
  /// transport-errored stream cannot half-close cleanly on top of a failure.
  pub(crate) fn pump_out(&mut self, now: Instant) -> Result<(), ()> {
    // Handshake-deadline guard. A `Handshaking` bridge (no `Stream`, no FSM
    // timer) is otherwise never terminalized when its deadline elapses without
    // the label step settling; fail it to `Failed(Timeout)` so the coordinator's
    // pre-`Stream` reap collects it. Runs before the no-`Stream` early return,
    // and only while `Handshaking` so the `Established` flush deadline (the
    // `is_done()`-gated path below) is unchanged.
    if matches!(self.phase, TcpPhase::Handshaking) && now >= self.deadline {
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
    // never closes its send half cannot pin the bridge.
    if now >= self.deadline
      && self.stream.as_ref().expect("stream is Some").is_done()
      && !self.is_terminal()
    {
      self.fail_with_retire(BridgeFailure::Timeout);
      return Err(());
    }

    // Drain plaintext from the FSM into the record layer. Raw passthrough
    // appends verbatim to an unbounded outbound buffer, so there is no
    // back-pressure and no retained-tail loop: each yield is queued in one call.
    // The `poll_transmit` borrow on `stream` ends before the `write_plaintext`
    // borrow on `records` begins.
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
      // its close.
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

    // Close the send half exactly once. Reaching here guarantees `poll_transmit`
    // is exhausted, so an inbound response pre-`stream_load_response` (phase
    // `InboundSendingResponse`, `is_done()` false, `sent_any` false) does not
    // half-close before the reply is written. A failed / fatal stream MUST NOT
    // half-close cleanly — the gate forbids it.
    let done = self.stream.as_ref().expect("stream is Some").is_done();
    let failed = self
      .stream
      .as_ref()
      .expect("stream is Some")
      .is_failed()
      .is_some();
    if !self.fin_sent && !self.is_phase_failed() && !failed && (self.sent_any || done) {
      self.records.send_close_notify();
      self.fin_sent = true;
      self.observe_send_fin();
    }

    Ok(())
  }

  /// `true` once the bridge has reached a terminal [`BridgePhase`] — either
  /// `BothClosed` (clean: both halves retired) or `Failed` (any failure path).
  /// The coordinator then stops pumping and runs [`Self::drain_then_reap`].
  ///
  /// Phase is the SINGLE source of truth. The failure transitions retire the
  /// send half and discard buffered inbound plaintext atomically BEFORE flipping
  /// the phase, so a `Failed`-phase bridge is retired by construction. No
  /// `Stream::is_failed()` fallback: every FSM-internal failure is observed by
  /// `pump_out` / `handle_transport_data` and cascaded into `Failed` via the
  /// atomic-retire-then-fail helpers; falling back to `Stream::is_failed()` for
  /// terminality would let a reap fire before the retirement step.
  pub(crate) fn is_terminal(&self) -> bool {
    matches!(
      self.phase,
      TcpPhase::Established(BridgePhase::BothClosed | BridgePhase::Failed(_))
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
  /// reliable-ping ack is applied. A `Handshaking` bridge has no `Stream` and no
  /// events, so this is a no-op (the coordinator reaps a failed-label bridge
  /// directly).
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
            // the inbound stream's `output_buf` still empty: encode the snapshot
            // and load it before any of it can be transmitted, or the peer is
            // left with a half-applied merge (split-brain). The bridge-level
            // `self.deadline` is advanced to the SAME `now + 5s` value
            // `stream_load_response` writes into the inner stream so the
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
    // An adversarial peer that never closes its send half does not produce
    // `BothClosed` — the bridge stays non-terminal until `self.deadline`
    // elapses, at which point `pump_out`'s flush-deadline path transitions to
    // `Failed(Timeout)`.
    let id = self.stream.as_ref().expect("stream is Some").id();
    let fsm_failed = self.stream.as_ref().expect("stream is Some").is_failed();
    let notice = match (&self.phase, fsm_failed) {
      (TcpPhase::Established(BridgePhase::Failed(reason)), _) => {
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
  /// stream (via `enter_failed`, which clears the queue) before the chunk-1 side
  /// effects are ever committed. The encode + load for an inbound push/pull
  /// response still runs here, though: without it the reply for an in-flight
  /// inbound exchange is never produced and the peer is left with a half-applied
  /// merge. Mirrors [`Self::drain_then_reap`] minus the lifecycle notice.
  pub(crate) fn drain_payload_only(&mut self, ep: &mut Endpoint<I, A>, now: Instant) {
    if !matches!(self.phase, TcpPhase::Established(BridgePhase::RecvClosed)) {
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
    time::{Duration, Instant},
  };

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::super::records::RawRecords;
  use crate::{config::EndpointConfig, error::StreamError, event::Event};

  fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
  }

  fn label(s: &str) -> Option<Vec<u8>> {
    Some(s.as_bytes().to_vec())
  }

  /// Build a `Handshaking` dialer/acceptor bridge pair over a shared label.
  /// The dialer queues its outbound `[12][len][label]` prefix eagerly at
  /// construction; the acceptor's prefix is queued lazily once its inbound
  /// label is validated (see `RawRecords` module docs). The dialer is never
  /// `is_handshaking()` (so it promotes immediately); the acceptor stays
  /// handshaking until it reads and validates the inbound label. Analog of
  /// the TLS test's `handshaking_pair`.
  fn handshaking_pair(
    cluster: &str,
    deadline: Instant,
  ) -> (
    TcpBridge<SmolStr, SocketAddr>,
    TcpBridge<SmolStr, SocketAddr>,
  ) {
    let dialer = RawRecords::dialer(label(cluster), false);
    let acceptor = RawRecords::acceptor(label(cluster), false);
    (
      TcpBridge::new(dialer, deadline),
      TcpBridge::new(acceptor, deadline),
    )
  }

  /// Pump each side's outbound label prefix across to its peer so both
  /// inbound-label validations settle. Symmetric with the bidirectional wire
  /// label on the reliable path. The dialer queues its `[12][len][label]`
  /// eagerly at construction; once delivered, the acceptor validates it and
  /// lazily queues its own outbound `[12][len][label]`, which this helper
  /// then drains back to the dialer. The acceptor stays handshaking until
  /// its inbound label is validated; the dialer is never handshaking (its
  /// inbound validation runs in-line on the established intake).
  fn complete_label_exchange(
    client: &mut TcpBridge<SmolStr, SocketAddr>,
    server: &mut TcpBridge<SmolStr, SocketAddr>,
    now: Instant,
  ) {
    let mut client_prefix = Vec::new();
    client.poll_transport_transmit(&mut client_prefix);
    assert!(!client_prefix.is_empty(), "dialer queued a label prefix");
    server
      .handle_transport_data(&client_prefix, now)
      .expect("acceptor accepts the matching label prefix");
    assert!(!server.is_handshaking(), "label settled on the acceptor");
    assert!(!client.is_handshaking(), "a dialer is never handshaking");
    let mut server_prefix = Vec::new();
    server.poll_transport_transmit(&mut server_prefix);
    assert!(
      !server_prefix.is_empty(),
      "acceptor symmetrically queued its outbound label prefix"
    );
    client
      .handle_transport_data(&server_prefix, now)
      .expect("dialer accepts the matching inbound label prefix");
  }

  /// Shuttle bytes both ways once, modeling the driver's per-connection move
  /// AND the out-of-band TCP FIN: any non-empty outbound bytes are delivered to
  /// the peer's `handle_transport_data`, and once a side `fin_owed()` its peer
  /// is delivered exactly one zero-length read (the TCP `read == 0` EOF anchor)
  /// — the out-of-band FIN that `RawRecords` cannot carry in-band. `c_fin` /
  /// `s_fin` latch the one-shot EOF delivery. Returns `true` if anything moved
  /// (bytes or a FIN) this round.
  fn shuttle(
    client: &mut TcpBridge<SmolStr, SocketAddr>,
    server: &mut TcpBridge<SmolStr, SocketAddr>,
    c_fin_delivered: &mut bool,
    s_fin_delivered: &mut bool,
    now: Instant,
  ) -> bool {
    // Ignoring Err on every `handle_transport_data` below: this helper models the
    // driver, which does not branch on the return — a terminalized bridge is
    // asserted via `is_terminal()` / `phase` at the call sites, exactly as the
    // real coordinator reaps on terminality rather than on the `Result`.
    let mut moved = false;

    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      let _ = server.handle_transport_data(&c_out, now);
      moved = true;
    }
    let mut s_out = Vec::new();
    server.poll_transport_transmit(&mut s_out);
    if !s_out.is_empty() {
      let _ = client.handle_transport_data(&s_out, now);
      moved = true;
    }

    // Deliver each side's out-of-band FIN to its peer exactly once, AFTER its
    // buffered bytes have crossed: a clean TCP close is "drain then FIN", so the
    // peer reads the bytes then a `read == 0`.
    if client.fin_owed() && !*c_fin_delivered {
      *c_fin_delivered = true;
      let _ = server.handle_transport_data(&[], now);
      moved = true;
    }
    if server.fin_owed() && !*s_fin_delivered {
      *s_fin_delivered = true;
      let _ = client.handle_transport_data(&[], now);
      moved = true;
    }
    moved
  }

  /// (b) A one-way `UserData` frame round-trips through the byte pump: outbound
  /// client `Stream::poll_transmit` -> `write_plaintext` -> bytes -> server
  /// `handle_transport_data` -> `read_plaintext` -> inbound `Stream::handle_data`.
  /// (c) The FIN-only half-close (each side's `read == 0` EOF) drives both
  /// bridges to `BothClosed`, then the terminal D1 reap surfaces the decoded
  /// `UserData` as `Event::UserPacket` on the server Endpoint. Analog of the TLS
  /// `frame_round_trips_then_clean_close_reaps`, with `close_notify` replaced by
  /// the TCP FIN.
  #[test]
  fn frame_round_trips_then_fin_close_reaps() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    complete_label_exchange(&mut client, &mut server, now);

    // Outbound client: a one-way reliable user message.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7100)));
    let payload = Bytes::from_static(b"hello-tcp");
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

    // Pump the request out of the client and into the server, shuttling bytes
    // and the out-of-band FIN until both bridges reap.
    let mut c_fin = false;
    let mut s_fin = false;
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, &mut c_fin, &mut s_fin, now);
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
      if !moved {
        break;
      }
    }

    assert!(
      matches!(client.phase, TcpPhase::Established(BridgePhase::BothClosed)),
      "client reached BothClosed (sent request + FIN, saw peer FIN), got {:?}",
      debug_phase(&client.phase)
    );
    assert!(
      matches!(server.phase, TcpPhase::Established(BridgePhase::BothClosed)),
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

  /// (d) Label-mismatch reject: an acceptor fed a wrong-label prefix tears the
  /// bridge down with NO `Stream` minted and ZERO endpoint side effects — the
  /// structural analog of the TLS bridge's mTLS-reject path. The bridge becomes
  /// terminal so the coordinator reaps it directly.
  #[test]
  fn label_mismatch_tears_down_with_no_stream_and_no_endpoint_events() {
    let now = Instant::now();
    // Acceptor expects `cluster-x`; the dialer presents `cluster-other`.
    let acceptor = RawRecords::acceptor(label("cluster-x"), false);
    let mut server = TcpBridge::<SmolStr, SocketAddr>::new(acceptor, now + Duration::from_secs(10));
    let dialer = RawRecords::dialer(label("cluster-other"), false);
    let mut client = TcpBridge::<SmolStr, SocketAddr>::new(dialer, now + Duration::from_secs(10));

    let mut prefix = Vec::new();
    client.poll_transport_transmit(&mut prefix);
    assert!(!prefix.is_empty(), "dialer produced a label prefix");

    assert!(server.is_handshaking());
    let res = server.handle_transport_data(&prefix, now);
    assert!(res.is_err(), "the server rejected the mismatched label");

    // No `Stream` was minted, the bridge is terminal, and no endpoint events
    // exist (there is no Endpoint involvement on the reject path at all).
    assert!(
      server.stream.is_none(),
      "no Stream minted on a label-mismatch reject"
    );
    assert!(
      server.is_terminal(),
      "the bridge is terminal after the reject"
    );
    assert!(
      matches!(
        server.phase,
        TcpPhase::Established(BridgePhase::Failed(BridgeFailure::Transport(_)))
      ),
      "the reject is a Transport failure, got {:?}",
      debug_phase(&server.phase)
    );

    // `drain_then_reap` on a no-Stream bridge is a clean no-op (the coordinator
    // reaps a failed-label bridge without an FSM lifecycle notice).
    let mut ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    server.drain_then_reap(&mut ep, now);
    assert!(
      ep.poll_event().is_none(),
      "no endpoint events on the reject path"
    );
  }

  /// Symmetric dialer-side reject: a wrong-cluster responder with
  /// `skip_inbound_label_check = true` could accept our dial, then answer with
  /// a DIFFERENT label. Without inbound validation on the dialer side the
  /// response would be treated as plaintext and merged into membership — the
  /// cross-cluster footgun. The dialer bridge's established intake validates
  /// the inbound response label and terminalizes on a mismatch, so no merge /
  /// ack can ever land.
  #[test]
  fn dialer_bridge_rejects_mismatched_inbound_response_label() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    // Dialer expects `cluster-x`; promote it with a real outbound exchange so
    // the established intake validates the inbound response label (rather
    // than the pre-promote handshaking intake).
    let dialer = RawRecords::dialer(label("cluster-x"), false);
    let mut client = TcpBridge::<SmolStr, SocketAddr>::new(dialer, deadline);
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7300)));
    let sid = ep_c.start_reliable_ping(
      SmolStr::new("srv"),
      addr(7000),
      7,
      now + Duration::from_secs(5),
    );
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the dialer's reliable-ping stream");
    client.promote(c_stream);
    client.pump_out(now).ok();
    // Drain whatever bytes the client queued (label + request).
    let mut tx = Vec::new();
    client.poll_transport_transmit(&mut tx);
    assert!(!tx.is_empty(), "dialer queued its [label||request] bytes");

    // Feed a wrong-label response. Under bidirectional validation this is
    // `Intake::Failed`; the bridge atomic-retires + terminalizes through the
    // `Established` `Transport` failure path (no merge / ack survives).
    let mut response = vec![12u8, 5];
    response.extend_from_slice(b"other");
    response.extend_from_slice(b"ack-bytes-that-would-never-be-trusted");
    let res = client.handle_transport_data(&response, now);
    assert!(res.is_err(), "the dialer rejects the wrong-label response");
    assert!(
      matches!(
        client.phase,
        TcpPhase::Established(BridgePhase::Failed(BridgeFailure::Transport(_)))
      ),
      "the reject is a Transport failure, got {:?}",
      debug_phase(&client.phase)
    );
    assert!(client.is_terminal());

    // The dialer never observed the response payload as plaintext: the inner
    // `Stream` is FSM-failed (no merge / ack survives), and the D1 reap on
    // the bridge runs without emitting any application-visible event for the
    // rejected exchange.
    client.drain_then_reap(&mut ep_c, now);
    while let Some(ev) = ep_c.poll_event() {
      // Only `DialRequested` may surface here (the inner endpoint also
      // queued one when the reliable-ping was started); the test cares that
      // NO membership-mutating event (a peer Joined/Updated/Left) appears
      // — those would only fire if the rejected label leaked.
      assert!(
        !matches!(
          ev,
          Event::NodeJoined(_) | Event::NodeUpdated(_) | Event::NodeLeft(_)
        ),
        "no membership mutation can survive a wrong-label response",
      );
    }
  }

  /// (f) Label-exchange timeout: a `Handshaking` bridge whose deadline elapses
  /// WITHOUT the label settling is terminalized by `pump_out` with NO `Stream`
  /// minted, so the coordinator's pre-`Stream` reap collects it. This is the
  /// slowloris / connect-refused bound — without it a stalled label exchange
  /// leaks the bridge (the empty-slice EOF anchor is a no-op while no inbound
  /// label arrives, and the `is_done()`-gated flush deadline needs a `Stream`
  /// that does not exist yet). Mirrors the TLS analog verbatim.
  #[test]
  fn handshaking_bridge_times_out_at_deadline_with_no_stream() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    // An acceptor that never receives its inbound label stays `Handshaking`.
    let acceptor = RawRecords::acceptor(label("cluster-x"), false);
    let mut server = TcpBridge::<SmolStr, SocketAddr>::new(acceptor, deadline);

    // Before the deadline a `pump_out` is a clean no-op (still Handshaking,
    // still surfacing the deadline as its only timer).
    assert!(server.is_handshaking());
    assert!(!server.is_terminal());
    server.pump_out(now).expect("pre-deadline pump is a no-op");
    assert!(
      server.is_handshaking(),
      "still handshaking before the deadline"
    );
    assert_eq!(
      server.poll_timeout(),
      Some(deadline),
      "a Handshaking bridge surfaces its accept deadline as the only timer"
    );

    // At the deadline `pump_out` terminalizes the stalled label exchange.
    let res = server.pump_out(deadline);
    assert!(
      res.is_err(),
      "a stalled label exchange at its deadline fails"
    );
    assert!(
      server.is_terminal(),
      "the bridge is terminal after the deadline elapses"
    );
    assert!(
      server.stream.is_none(),
      "no Stream is minted on a label-exchange timeout"
    );
    assert!(
      matches!(
        server.phase,
        TcpPhase::Established(BridgePhase::Failed(BridgeFailure::Timeout))
      ),
      "the timeout maps to Failed(Timeout), got {:?}",
      debug_phase(&server.phase)
    );
    assert!(
      !server.is_handshaking(),
      "a timed-out bridge is no longer handshaking"
    );
    // A terminal bridge contributes no deadline (it is reaped this tick).
    assert_eq!(server.poll_timeout(), None);

    // `drain_then_reap` on a no-`Stream` bridge is a clean no-op — no FSM
    // lifecycle notice is owed for a label exchange that never minted a `Stream`.
    let mut ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7000)));
    server.drain_then_reap(&mut ep, deadline);
    assert!(
      ep.poll_event().is_none(),
      "no endpoint events on the label-timeout path"
    );
  }

  /// (b''') A coalesced read carrying the dialer's `[label]` prefix immediately
  /// followed by a SMALL first app frame, delivered to a server bridge that is
  /// STILL `Handshaking` (no `Stream` yet) in ONE `handle_transport_data` call —
  /// with NO further transport read after. `RawRecords` settles the label AND
  /// surfaces the trailing request as inbound plaintext in that one pass (no
  /// backpressure, so NO retained tail). After promote + `replay_pending`, the
  /// coordinator-equivalent post-mint pump MUST drain that buffered plaintext
  /// into the freshly-promoted `Stream` (the frame decodes) — even though there
  /// is no retained tail. Analog of the TLS
  /// `coalesced_handshake_final_plus_small_frame_and_close_notify_pre_promotion_drains`.
  #[test]
  fn coalesced_label_plus_small_frame_pre_promotion_drains() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));

    // Mint + promote the CLIENT side (the dialer) with a SMALL one-way user
    // message, then pump it so the client's label prefix + the small app frame
    // are produced together (a one-way message half-closes once its request is
    // sent, so the client also owes its FIN).
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
      client.fin_owed(),
      "a one-way user message half-closes after its request — the FIN is owed"
    );

    // COALESCE the client's label prefix + the whole small app frame into ONE
    // buffer (a single TCP read on the server).
    let mut coalesced = Vec::new();
    client.poll_transport_transmit(&mut coalesced);
    assert!(
      coalesced.len() > 2,
      "the coalesced [label][small frame] carries both, got {}",
      coalesced.len()
    );

    // Deliver the WHOLE coalesced buffer to the still-`Handshaking` server in
    // ONE call. `RawRecords` settles the label, surfaces the small request as
    // inbound plaintext, and (no backpressure) retains NO tail.
    server
      .handle_transport_data(&coalesced, now)
      .expect("the coalesced label + small frame is accepted");
    assert!(
      !server.is_handshaking(),
      "the label settled inside the coalesced read"
    );
    assert!(
      server.pending_inbound.is_empty(),
      "the coalesced read is fully consumed in one pass — NO retained tail"
    );

    // Coordinator-equivalent: mint + promote the inbound `Stream`, then drive
    // the post-mint pump's `replay_pending` exactly as `pump_bridges` does. With
    // NO retained tail, the replay must STILL drain the buffered surfaced
    // plaintext into the just-promoted `Stream`, or the small frame never
    // reaches the FSM.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7600)));
    let s_stream = ep_s.accept_stream(addr(7600), now);
    server.promote(s_stream);
    server
      .replay_pending(now)
      .expect("post-promote replay drains the buffered small frame");

    // Finish: the client's FIN drives the server's recv-half close, and with a
    // one-way inbound user message reaching `Done` the bridge converges to
    // terminal so the D1 reap can run.
    server.drain_payload_only(&mut ep_s, now);
    let mut c_fin = false;
    let mut s_fin = false;
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, &mut c_fin, &mut s_fin, now);
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
      "the recv-half FIN fired and the one-way exchange reaped, got {:?}",
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
      "the small first frame coalesced with the label reached the Stream and \
       decoded intact (drained post-promotion with no retained tail)"
    );
  }

  /// A coalesced `[label||first request]` followed by a same-tick out-of-band
  /// TCP FIN, both observed BEFORE the bridge has been promoted (no `Stream`
  /// yet), MUST terminalize the acceptor on the SAME tick as the promote — not
  /// stall to the accept deadline.
  ///
  /// Without [`TcpBridge::pending_eof`] the FIN signal is dropped: the
  /// empty-slice second feed reaches a still-pre-`Stream` bridge whose
  /// pre-`Stream` intake is a no-op on empty input, and the post-promote
  /// [`TcpBridge::replay_pending`] passes `eof = false` to
  /// `pump_in_established`, so the recv-half FIN never fires. The latch
  /// captures the out-of-band EOF across the promote boundary so the
  /// recv-half retirement (or, for an `OutboundAwaitingResponse` peer, the
  /// truncation path) runs the same tick the `Stream` is minted.
  ///
  /// The TCP analog of TLS's in-band `close_notify` (latched in
  /// `TlsRecords::peer_has_closed()`): plain TCP has no in-band close signal,
  /// so the bridge holds the latch instead of the records layer, keeping
  /// [`super::records::RawRecords`] strictly transport-agnostic.
  #[test]
  fn coalesced_label_plus_small_frame_with_eof_pre_promotion_terminalizes() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));

    // Mint + promote the CLIENT and pump a one-way user-message + FIN, so the
    // client's outbound buffer carries `[label||frame]` and the client owes
    // its FIN.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("cli"), addr(7700)));
    let payload = Bytes::from_static(b"coalesced-with-eof");
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);
    client.pump_out(now).expect("client pumps its request");
    assert!(
      client.fin_owed(),
      "a one-way user-message half-closes after its request — the FIN is owed"
    );

    // Coalesce the client's `[label||frame]` into one buffer.
    let mut coalesced = Vec::new();
    client.poll_transport_transmit(&mut coalesced);
    assert!(coalesced.len() > 2);

    // Deliver the coalesced bytes then the same-tick FIN to the
    // still-`Handshaking` server, exactly as the coordinator splits a
    // `(bytes, eof=true)` driver call into a bytes feed then an empty-slice
    // EOF anchor.
    server
      .handle_transport_data(&coalesced, now)
      .expect("the coalesced label + small frame is accepted");
    assert!(!server.is_handshaking());
    // The empty-slice EOF reaches a still-pre-`Stream` server (the mint runs
    // in `service_handshake_completions`); the latch carries it across the
    // promote boundary.
    server
      .handle_transport_data(&[], now)
      .expect("the pre-promote empty-slice EOF is a no-op intake (and latches)");
    assert!(
      server.pending_eof,
      "pre-promote out-of-band FIN latched on the bridge"
    );

    // Mint + promote the server `Stream`, then run the post-mint replay drain
    // exactly as the coordinator's `pump_bridges` does. The replay seeds
    // `eof = pending_eof = true`, so the recv-half FIN fires for the one-way
    // user-message (its FSM is `Done` after the request decodes) and the
    // bridge converges to `BothClosed` without waiting for any later
    // transport read.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointConfig::new(SmolStr::new("srv"), addr(7700)));
    let s_stream = ep_s.accept_stream(addr(7700), now);
    server.promote(s_stream);
    server
      .replay_pending(now)
      .expect("post-promote replay drains the buffered frame and honors the latched EOF");

    // The recv-half FIN fired on the replay → `RecvClosed`; `pump_out` then
    // observes the inner FSM `Done` (one-way user-message) and retires the
    // send half → `BothClosed`. Same tick, no further transport reads.
    server
      .pump_out(now)
      .expect("server pump retires its send half");
    assert!(
      matches!(server.phase, TcpPhase::Established(BridgePhase::BothClosed)),
      "the latched pre-promote FIN drove RecvClosed → BothClosed this tick, \
       got {:?}",
      debug_phase(&server.phase)
    );
    assert!(server.is_terminal());

    // The decoded `UserData` still surfaces on the D1 reap (the recv-half
    // FIN authorized the dispatched event), so the exchange completes
    // successfully — not silently dropped.
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
      "the coalesced first frame still decoded; the latched EOF only governs \
       the recv-half close, not the payload"
    );
  }

  /// (e) Truncation: a TCP `read == 0` (zero-length `handle_transport_data`)
  /// mid-frame feeds `Stream::handle_data(&[])`, which the FSM rejects with
  /// `StreamError::PeerClosed` for a non-terminal awaiting phase. The bridge
  /// transitions to `Failed(Decode)` and is terminal. Analog of the TLS
  /// `truncation_read_zero_mid_frame_fails_peer_closed`, with the close already
  /// being the FIN (eof).
  #[test]
  fn truncation_read_zero_mid_frame_fails_peer_closed() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    complete_label_exchange(&mut client, &mut server, now);

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
        TcpPhase::Established(BridgePhase::Failed(BridgeFailure::Decode))
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

    // The unused `server` still proves the label settled.
    assert!(!server.is_handshaking());
  }

  /// Render a `TcpPhase` for assert messages without requiring `Debug` on the
  /// (private, accessor-only) enum.
  fn debug_phase(p: &TcpPhase) -> &'static str {
    match p {
      TcpPhase::Handshaking => "Handshaking",
      TcpPhase::Established(BridgePhase::Active) => "Established(Active)",
      TcpPhase::Established(BridgePhase::SendClosed) => "Established(SendClosed)",
      TcpPhase::Established(BridgePhase::RecvClosed) => "Established(RecvClosed)",
      TcpPhase::Established(BridgePhase::BothClosed) => "Established(BothClosed)",
      TcpPhase::Established(BridgePhase::Failed(_)) => "Established(Failed)",
    }
  }
}
