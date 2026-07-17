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
//! deadline — `Probe::failure_deadline`, i.e. `sent +
//! awareness.scale_timeout(probe_interval)`, the awareness-scaled SWIM
//! period snapshotted at `sent`. The probe succeeds on whichever arrives
//! first (an indirect/direct-relayed Ack OR `ReliablePingAcked`); it
//! fails — suspecting the target — only when that one deadline elapses.
//! There is deliberately no extra per-stream timeout added to a failing
//! probe — the old serialized `AwaitingReliableFallback` phase widened
//! the failure envelope from one cumulative deadline to
//! direct+indirect+stream_timeout.

use crate::Instant;
use core::time::Duration;
use std::sync::Arc;

use crate::{event::StreamId, typed::NodeState};

use smallvec_wrapper::SmallVec;

/// Source of a probe — distinguishes failure-detection probes (default) from
/// application-level pings issued via `Endpoint::ping`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub(crate) struct Probe<I, A> {
  /// The peer being probed.
  pub(crate) target: Arc<NodeState<I, A>>,
  /// The target's incarnation snapshotted at probe creation. On a failed
  /// Detection probe the target is suspected at THIS incarnation, not whatever
  /// it currently holds: if it refuted (raised its incarnation) in place while
  /// the probe was in flight, `process_suspect` drops the stale suspicion via
  /// its `inc < local_inc` guard. Mirrors Go memberlist `probeNode`, which
  /// suspects the snapshot `node.Incarnation`, not a re-read of the live map.
  pub(crate) target_incarnation: u32,
  /// The probed membership INSTANCE, captured as the target's generation token
  /// at probe creation (see [`Member::generation`](crate::members::Member::generation)).
  /// A failed Detection probe suspects the target only while the current member
  /// under that id still carries THIS generation. The generation changes when a
  /// record is replaced by a different instance — a fresh id, or a Left/Dead id
  /// reclaimed at any address or incarnation (including an EQUAL one, which
  /// `reset_nodes` removal + re-admit produces and an `(address, incarnation)`
  /// check cannot distinguish) — so binding to it prevents a stale probe from
  /// suspecting a replacement that merely reuses the id. `0` is the "unset"
  /// sentinel (a probe of an untracked target, e.g. the public `ping`); an unset
  /// snapshot matches no tracked member, so it can never suspect one.
  pub(crate) target_generation: u64,
  /// When the initial Ping was sent (used for RTT measurement).
  pub(crate) sent_at: Instant,
  /// What kind of probe this is.
  pub(crate) kind: ProbeKind,
  /// Whether at least one probe datagram/dial was ever dispatched for this
  /// probe — MONOTONIC: set `true` at each dispatch initiation (the direct
  /// `Ping` queuing, any `IndirectPing` queuing, or the reliable-ping dial
  /// opening) and NEVER cleared. A node id is unbounded, so a large id can make
  /// the direct `Ping` AND every `IndirectPing` exceed `gossip_mtu` and be
  /// dropped; with reliable ping unavailable, NOTHING is sent. A deterministic
  /// local MTU limit is not peer loss, so `probe_terminate_failure` reads this
  /// flag directly — from dispatch HISTORY, not the mutable `reliable_stream_id`
  /// (which a fallback retirement, `dial_failed` / `ReliablePingFailed`, clears
  /// while the probe is still live) — and applies the awareness penalty +
  /// suspicion only when it is true; otherwise it aborts cleanly. `false` at
  /// construction.
  pub(crate) dispatched: bool,
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
  pub(crate) indirect_peers: SmallVec<A>,
  /// Distinct indirect peers (by source address) that have returned a
  /// Nack within the deadline. `len()` is the effective nack count; a
  /// duplicate or late (>= `failure_deadline`) Nack is ignored so it
  /// cannot inflate the count and suppress the Lifeguard health penalty
  /// (`probe_terminate_failure` computes severity from
  /// `expected_nacks - nacked_by.len()`).
  pub(crate) nacked_by: SmallVec<A>,
  /// The reliable-ping fallback stream opened **concurrently** with the
  /// indirect fan-out (mirrors memberlist-core spawning the TCP ping
  /// alongside the indirect pings, bounded by the same deadline).
  /// `None` when reliable ping is disabled for the target, or once a
  /// reliable dial/ping failure has retired it (the indirect path keeps
  /// racing the deadline regardless — a fallback failure does NOT fail
  /// the probe early, matching `fallback_tx.send(false)` upstream).
  pub(crate) reliable_stream_id: Option<StreamId>,
  /// Cumulative deadline (after which we transition the target to Suspect).
  pub(crate) deadline: Instant,
}

/// Current phase of a [`Probe`] FSM. Newtype variants only — each payload
/// is its own struct.
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
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

impl<I, A> Probe<I, A> {
  /// Construct a fresh probe in `AwaitingDirectAck`.
  ///
  /// - `target_incarnation` is the target's incarnation at this instant; a
  ///   later Detection failure suspects that snapshot incarnation (see the
  ///   [`target_incarnation`](Self::target_incarnation) field);
  /// - `target_generation` is the target's membership-instance token at this
  ///   instant (`0` if untracked); a later Detection failure suspects only
  ///   while the live member still carries it (see the
  ///   [`target_generation`](Self::target_generation) field);
  /// - the direct-ack sub-window deadline is derived from
  ///   `sent_at + probe_timeout` (the SAME formula as
  ///   [`direct_deadline`](Self::direct_deadline));
  /// - `failure_deadline` is the absolute authoritative failure instant,
  ///   computed ONCE by the caller (it needs `awareness` +
  ///   `probe_interval`, which only the Endpoint has) and stored as-is —
  ///   Detection: `sent_at + awareness.scale_timeout(probe_interval)`;
  ///   Ping: `sent_at + probe_timeout`.
  pub(crate) fn new_direct(
    target: Arc<NodeState<I, A>>,
    target_incarnation: u32,
    target_generation: u64,
    sent_at: Instant,
    kind: ProbeKind,
    probe_timeout: Duration,
    failure_deadline: Instant,
  ) -> Self {
    Self {
      target,
      target_incarnation,
      target_generation,
      sent_at,
      kind,
      // No datagram has been dispatched at construction time; the initiator
      // (`start_probe` / `ping`) records the actual direct send afterward.
      dispatched: false,
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
  pub(crate) fn deadline(&self) -> Instant {
    match &self.phase {
      ProbePhase::AwaitingDirectAck(p) => p.deadline.min(self.failure_deadline),
      ProbePhase::AwaitingIndirect(p) => p.deadline,
    }
  }
}
