//! Per-probe state machine. Each in-flight probe is one [`Probe`] keyed
//! by sequence number in `Endpoint::probes`.
//!
//! The FSM advances through:
//!
//! ```text
//!   AwaitingDirectAck { deadline }
//!         │ Ack (handle_ack)        │ deadline elapsed (handle_timeout)
//!         ▼                         ▼  fan out IndirectPings AND (if enabled)
//!     Success                   open the reliable ping CONCURRENTLY
//!                                   ▼
//!                               AwaitingIndirect { expected_nacks,
//!                                                  indirect_peers, nacked_by,
//!                                                  reliable_stream_id, deadline }
//!                                   │ indirect/direct-relayed Ack (handle_ack)
//!                                   │   OR ReliablePingAcked          → Success
//!                                   │ deadline elapsed (handle_timeout)
//!                                   │   → Failure (target → Suspect)
//! ```
//!
//! The reliable-stream fallback ping is NOT a separate sequential phase.
//! Mirroring memberlist-core `handle_remote_failure`, when the direct ack
//! times out the reliable ping is opened **concurrently** with the indirect
//! fan-out and races them within the single cumulative `AwaitingIndirect`
//! deadline (`= sent + probe_timeout`). The probe succeeds on whichever
//! arrives first (an indirect/direct-relayed Ack OR `ReliablePingAcked`);
//! it fails — suspecting the target — only when that one deadline elapses.
//! There is deliberately no extra per-stream timeout added to a failing
//! probe — the old serialized `AwaitingReliableFallback` phase widened
//! the failure envelope from one probe_timeout to
//! direct+indirect+stream_timeout.

use crate::Instant;
use core::time::Duration;
use std::sync::Arc;

use memberlist_wire::typed::NodeState;

/// Source of a probe — distinguishes failure-detection probes (default) from
/// application-level pings issued via `Endpoint::ping`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ProbeKind {
  /// SWIM failure-detection probe; outcome updates Awareness and may
  /// transition the target to Suspect.
  Detection,
  /// Application-level ping; outcome emits `Event::PingCompleted` on
  /// success and is silent on failure.
  Ping,
}

/// State of one in-flight probe, keyed in `Endpoint::probes` by sequence number.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct Probe<I, A> {
  /// The peer being probed.
  pub(crate) target: Arc<NodeState<I, A>>,
  /// When the initial Ping was sent (used for RTT measurement).
  pub(crate) sent_at: Instant,
  /// What kind of probe this is.
  pub(crate) kind: ProbeKind,
  /// The absolute authoritative FAILURE deadline, captured ONCE at probe
  /// creation (NOT recomputed from a later `now`). Mirrors memberlist-core
  /// `probe_node`: Detection uses `sent_at + awareness.scale_timeout(
  /// probe_interval)` — the Lifeguard-scaled SWIM period snapshotted at
  /// `sent`, so a degraded (high health score) node waits proportionally
  /// longer before suspecting; Ping uses `sent_at + probe_timeout`
  /// (direct-only, memberlist-core `Memberlist::ping`). The caller
  /// computes it (only the Endpoint has `awareness` + `probe_interval`)
  /// and stores it here so every deadline decision reads one fixed value.
  pub(crate) failure_deadline: Instant,
  /// Current FSM phase.
  pub(crate) phase: ProbePhase<A>,
}

/// Direct-ack sub-window payload. See [`ProbePhase::AwaitingDirectAck`].
///
/// Extracted as a named struct because the FSM enum uses newtype variants
/// only (no struct variants).
#[derive(Debug)]
pub(crate) struct AwaitingDirectAck {
  /// End of the direct-ack wait. If it elapses without a direct Ack,
  /// escalate to indirect + concurrent reliable fallback (Detection) or
  /// fail (direct-only Ping).
  pub(crate) deadline: Instant,
}

/// Indirect / reliable-fallback race payload.
/// See [`ProbePhase::AwaitingIndirect`].
#[derive(Debug)]
pub(crate) struct AwaitingIndirect<A> {
  /// Number of indirect peers we sent IndirectPing to (memberlist-core
  /// `expected_nacks = nodes.len()`). Equals `indirect_peers.len()` by
  /// construction; kept explicit to mirror upstream and feed the
  /// Lifeguard severity `expected_nacks - nacked_by.len()`.
  pub(crate) expected_nacks: usize,
  /// Source addresses of the indirect peers we actually sent an
  /// IndirectPing to. A Nack is counted only if its `from` is in this
  /// allowlist — a Nack carries just the sequence number, so an off-path
  /// node that guesses `seq` (or a stale/forged Nack) must not be able to
  /// mark the indirect probe answered. The Nack's transport source address
  /// is the only responder identity available, so the allowlist is keyed
  /// by address, not id.
  pub(crate) indirect_peers: smallvec::SmallVec<[A; 4]>,
  /// Distinct indirect peers (by source address) that have returned a
  /// Nack within the deadline. `len()` is the effective nack count; a
  /// duplicate or late (>= `failure_deadline`) Nack is ignored so it
  /// cannot inflate the count and suppress the Lifeguard health penalty
  /// (`probe_terminate_failure` computes severity from
  /// `expected_nacks - nacked_by.len()`).
  pub(crate) nacked_by: smallvec::SmallVec<[A; 4]>,
  /// The reliable-ping fallback stream opened **concurrently** with the
  /// indirect fan-out (mirrors memberlist-core spawning the TCP ping
  /// alongside the indirect pings, bounded by the same deadline).
  /// `None` when reliable ping is disabled for the target, or once a
  /// reliable dial/ping failure has retired it (the indirect path keeps
  /// racing the deadline regardless — a fallback failure does NOT fail
  /// the probe early, matching `fallback_tx.send(false)` upstream).
  pub(crate) reliable_stream_id: Option<crate::event::StreamId>,
  /// Cumulative deadline (after which we transition the target to Suspect).
  pub(crate) deadline: Instant,
}

/// Current phase of a [`Probe`] FSM. Newtype variants only — each payload
/// is its own struct.
#[derive(Debug)]
#[allow(dead_code, clippy::enum_variant_names)]
pub(crate) enum ProbePhase<A> {
  /// Waiting for a direct Ack from the target. If the deadline elapses
  /// without one, transition to `AwaitingIndirect` (sending IndirectPings
  /// to k random peers).
  AwaitingDirectAck(AwaitingDirectAck),

  /// Waiting for an Ack-relay (from any indirect peer that reached the
  /// target), the concurrently-opened reliable-ping `ReliablePingAcked`,
  /// or for the single cumulative deadline to elapse (→ suspect).
  AwaitingIndirect(AwaitingIndirect<A>),
}

/// Successful-probe payload. See [`ProbeOutcome::Success`].
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ProbeSuccess<I, A> {
  /// The probed peer.
  pub(crate) target: Arc<NodeState<I, A>>,
  /// Round-trip time.
  pub(crate) rtt: core::time::Duration,
  /// Ack payload carried by the response.
  pub(crate) payload: bytes::Bytes,
  /// What kind of probe this was.
  pub(crate) kind: ProbeKind,
}

/// Failed-probe payload. See [`ProbeOutcome::Failure`].
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ProbeFailure<I, A> {
  /// The probed peer.
  pub(crate) target: Arc<NodeState<I, A>>,
  /// What kind of probe this was.
  pub(crate) kind: ProbeKind,
  /// `Some((expected, seen))` if we entered AwaitingIndirect and the
  /// failure happened there; `None` if the failure happened on the direct
  /// path (no indirect was attempted, e.g., empty cluster).
  pub(crate) nack_stats: Option<(usize, usize)>,
}

/// Terminal outcome of a probe. Newtype variants only — each payload is
/// its own struct.
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum ProbeOutcome<I, A> {
  /// Probe succeeded — target is healthy.
  Success(ProbeSuccess<I, A>),
  /// Probe failed — target should be marked Suspect (Detection) or
  /// silently dropped (Ping).
  Failure(ProbeFailure<I, A>),
}

impl<I, A> Probe<I, A> {
  /// Construct a fresh probe in `AwaitingDirectAck`.
  ///
  /// - the direct-ack sub-window deadline is derived from
  ///   `sent_at + probe_timeout` (the SAME formula as
  ///   [`direct_deadline`](Self::direct_deadline));
  /// - `failure_deadline` is the absolute authoritative failure instant,
  ///   computed ONCE by the caller (it needs `awareness` +
  ///   `probe_interval`, which only the Endpoint has) and stored as-is —
  ///   Detection: `sent_at + awareness.scale_timeout(probe_interval)`;
  ///   Ping: `sent_at + probe_timeout`.
  #[allow(dead_code)]
  pub(crate) fn new_direct(
    target: Arc<NodeState<I, A>>,
    sent_at: Instant,
    kind: ProbeKind,
    probe_timeout: Duration,
    failure_deadline: Instant,
  ) -> Self {
    Self {
      target,
      sent_at,
      kind,
      failure_deadline,
      phase: ProbePhase::AwaitingDirectAck(AwaitingDirectAck {
        deadline: sent_at + probe_timeout,
      }),
    }
  }

  // ─── Authoritative probe deadlines (single source of truth) ───────────────
  //
  // Both are anchored ABSOLUTELY to `sent_at` (the instant the probe
  // started), never to a later injected `now`, so a late `handle_timeout`
  // callback and packet-vs-timer event ordering cannot move them.
  // Every deadline decision in the probe subsystem MUST go through these —
  // `start_probe`/`ping` (construction + AckEntry), `advance_probe_fsm`
  // (escalate vs terminate), `probe_fan_out_indirect` (cumulative + the
  // concurrent reliable fallback), and `complete_probe_success` (the
  // ack-too-late cutoff).

  /// End of the direct-ack sub-window. After this a `Detection` probe
  /// escalates to indirect + concurrent reliable fallback; a `Ping`
  /// (direct-only, mirrors memberlist-core `Memberlist::ping`) fails.
  /// Equals the `AwaitingDirectAck` phase deadline by construction.
  /// Always the *unscaled* `probe_timeout` — memberlist-core's direct UDP
  /// wait is `opts.probe_timeout`, NOT the awareness-scaled interval.
  #[allow(dead_code)]
  pub(crate) fn direct_deadline(&self, probe_timeout: Duration) -> Instant {
    self.sent_at + probe_timeout
  }

  /// The single authoritative FAILURE deadline: the instant at/after which
  /// timeout terminates the probe and an Ack (direct, indirect-relayed, or
  /// reliable-fallback) can no longer rescue it — regardless of phase or
  /// of how we got here (packet vs timer). Returns the value snapshotted
  /// at construction (memberlist-core computes `sent + probe_interval`
  /// once at `sent`, where `probe_interval` is the awareness-scaled SWIM
  /// period for Detection, or `probe_timeout` for a direct-only Ping).
  #[allow(dead_code)]
  pub(crate) fn failure_deadline(&self) -> Instant {
    self.failure_deadline
  }

  /// Earliest actionable deadline for this probe (used by
  /// `Endpoint::poll_timeout` to schedule the next `handle_timeout`).
  ///
  /// While `AwaitingDirectAck` the FSM has TWO actionable instants — the
  /// direct sub-window end (`deadline`, == `direct_deadline()`) and the
  /// authoritative `failure_deadline`. Normally `failure_deadline` is
  /// later (probe_interval ≥ probe_timeout) so this is just the direct
  /// deadline; but if `scale_timeout(probe_interval) < probe_timeout` the
  /// failure deadline comes FIRST and must be the wake — the probe
  /// terminates then rather than uselessly waiting out a direct sub-window
  /// longer than its whole budget. `AwaitingIndirect`'s stored deadline
  /// already IS `failure_deadline`.
  #[allow(dead_code)]
  pub(crate) fn deadline(&self) -> Instant {
    match &self.phase {
      ProbePhase::AwaitingDirectAck(p) => p.deadline.min(self.failure_deadline),
      ProbePhase::AwaitingIndirect(p) => p.deadline,
    }
  }
}
