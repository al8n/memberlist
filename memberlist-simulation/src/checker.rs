//! Invariant checkers for the VOPR driver.
//!
//! Each checker observes the cluster every tick and returns a [`CheckResult`].
//! A `Violation` carries a one-line reason; the driver panics with the seed +
//! tick so the failure replays.

use std::{collections::{HashMap, HashSet}, net::SocketAddr};

use memberlist_proto::typed::State;
use smol_str::SmolStr;

use crate::Cluster;

/// The outcome of one invariant check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckResult {
  /// The invariant holds.
  Ok,
  /// The invariant is violated, with a human-readable reason.
  Violation(SmolStr),
}

impl CheckResult {
  /// Construct a violation from any string-like reason.
  pub fn violation(reason: impl Into<SmolStr>) -> Self {
    Self::Violation(reason.into())
  }

  /// True if the invariant holds.
  pub fn is_ok(&self) -> bool {
    matches!(self, Self::Ok)
  }

  /// True if the invariant is violated.
  pub fn is_violation(&self) -> bool {
    matches!(self, Self::Violation(_))
  }

  /// The violation reason, if any.
  pub fn reason(&self) -> Option<&str> {
    match self {
      Self::Ok => None,
      Self::Violation(r) => Some(r.as_str()),
    }
  }
}

/// One per-mutation history transition: a distinct `(state, incarnation)` value
/// that a single `(observer, subject)` row passed through *during* one
/// [`Cluster::step`](crate::Cluster::step), in chronological order.
///
/// The history checkers track per-`(observer, subject)` state transitions, so a
/// row that passes through an illegal intermediate value and is then overwritten
/// by a legal one within the SAME step would be masked if only the post-step
/// value were sampled (e.g. an in-place resurrection `Dead@I -> Alive@<=I`
/// followed by a legal refute `-> Alive@>I` carried by a later message of the
/// same compound datagram, or a second datagram at the same deadline). The
/// harness records every distinct value the row took and feeds the sequence to
/// the checkers via `observe_transitions`, so the illegal intermediate is
/// observed rather than overwritten.
///
/// `state`/`incarnation` are `None` together when the row became absent;
/// `pruned` is `true` only when `reset_nodes` actually removed the entry this
/// step (the same discriminator [`Cluster::was_pruned_this_step`] feeds the
/// post-step path), so a legal post-prune disappearance clears the baseline
/// while an illegitimate one retains it.
#[derive(Debug, Clone)]
pub struct Transition {
  observer: SocketAddr,
  subject: SmolStr,
  state: Option<State>,
  incarnation: Option<u32>,
  pruned: bool,
}

impl Transition {
  /// Record one transition of `subject`'s row as seen by `observer`.
  pub fn new(
    observer: SocketAddr,
    subject: SmolStr,
    state: Option<State>,
    incarnation: Option<u32>,
    pruned: bool,
  ) -> Self {
    Self { observer, subject, state, incarnation, pruned }
  }

  /// The observer whose view changed.
  pub fn observer(&self) -> SocketAddr {
    self.observer
  }

  /// The subject whose `(state, incarnation)` changed in the observer's view.
  pub fn subject(&self) -> &SmolStr {
    &self.subject
  }

  /// The post-transition state, or `None` if the row became absent.
  pub fn state(&self) -> Option<State> {
    self.state
  }

  /// The post-transition incarnation, or `None` if the row became absent.
  pub fn incarnation(&self) -> Option<u32> {
    self.incarnation
  }

  /// Whether `reset_nodes` actually pruned this entry on the step that produced
  /// the transition.
  pub fn pruned(&self) -> bool {
    self.pruned
  }
}

/// One stored observation: a state and its incarnation.
///
/// A terminal (Dead/Left) baseline is expired by the ACTUAL prune event —
/// surfaced per step by [`Cluster::was_pruned_this_step`] — not by elapsed time.
/// The machine prunes a terminal entry lazily (`reset_nodes` runs once per probe
/// round-robin pass), so the prune and the post-prune re-learn can land in
/// separate steps with the intervening `None` never sampled; and the prune can
/// fire well after `gossip_to_the_dead_time` has elapsed. `Dead@I -> Alive@<=I`
/// is the same observation whether the machine legally pruned-then-re-learned or
/// (buggily) accepted in place — the discriminator is whether `reset_nodes`
/// actually removed the entry, which the harness observes directly. Keying on
/// the real prune both eliminates false positives (a legal post-prune re-join is
/// always accompanied by `pruned == true`) and closes a real false negative: a
/// buggy in-place resurrection after `gossip_to_the_dead_time` but before the
/// lazy prune carries `pruned == false`, so it now fires.
#[derive(Debug, Clone, Copy)]
struct Sample {
  state: State,
  incarnation: u32,
}

/// Checks that incarnation numbers are monotone non-decreasing for a
/// continuously-present node as seen by a single observer. A subject the machine
/// actually pruned and re-learned (possibly at a lower incarnation) resets the
/// baseline.
#[derive(Debug, Default)]
pub struct IncarnationMonotonicChecker {
  seen: HashMap<(SocketAddr, SmolStr), Sample>,
}

impl IncarnationMonotonicChecker {
  /// Create a checker with no prior observations.
  pub fn new() -> Self {
    Self::default()
  }

  /// Feed one observation. `pruned` is `true` when `reset_nodes` removed this
  /// (observer, subject) entry during the step being observed. All-`None` means
  /// the subject is absent: the baseline is cleared if `pruned` (a legitimate
  /// prune), otherwise retained (an illegitimate disappearance must not erase
  /// the baseline).
  pub fn observe_one(
    &mut self,
    observer: SocketAddr,
    subject: &SmolStr,
    incarnation: Option<u32>,
    state: Option<State>,
    pruned: bool,
  ) -> CheckResult {
    let key = (observer, subject.clone());
    match (incarnation, state) {
      (Some(inc), Some(st)) => {
        // A prune this step means the current observation is a legal post-prune
        // re-learn, which may legitimately restart at a lower incarnation.
        let live_prior = if pruned { None } else { self.seen.get(&key).copied() };
        if let Some(prev) = live_prior {
          if inc < prev.incarnation {
            return CheckResult::violation(format!(
              "incarnation regressed at {observer} for {subject}: {} -> {inc}",
              prev.incarnation
            ));
          }
        }
        // Carry the running max while continuously present; a pruned (or
        // absent) baseline restarts from the current incarnation.
        let max_inc = live_prior.map_or(inc, |p| p.incarnation.max(inc));
        self.seen.insert(key, Sample { state: st, incarnation: max_inc });
        CheckResult::Ok
      }
      _ => {
        // Clear the baseline ONLY on an actual prune; retain it on any other
        // disappearance — a non-terminal member vanishing, or a terminal one the
        // machine has not yet pruned — so a later regression still fires. An
        // observer restart clears explicitly via clear_observer.
        if pruned {
          self.seen.remove(&key);
        }
        CheckResult::Ok
      }
    }
  }

  /// Clear all stored baselines for `observer`. Called when the observer node
  /// restarts (crash + rejoin) — its member view is fresh and old baselines no
  /// longer apply.
  pub fn clear_observer(&mut self, observer: SocketAddr) {
    self.seen.retain(|(obs, _), _| obs != &observer);
  }

  /// Observe the whole cluster snapshot.
  pub fn observe(&mut self, c: &Cluster) -> CheckResult {
    for &observer in c.addrs() {
      for subject in c.ids() {
        let inc = c.get_node_incarnation(observer, subject);
        let st = c.member_liveness(observer, subject);
        let pruned = c.was_pruned_this_step(observer, subject);
        let r = self.observe_one(observer, subject, inc, st, pruned);
        if r.is_violation() {
          return r;
        }
      }
    }
    CheckResult::Ok
  }

  /// Seed a single observer's current snapshot as the baseline. Called after a
  /// restart clears the observer's baselines: the restarted endpoint's fresh
  /// self-view must become the baseline (`pruned = false`, no prior after the
  /// clear ⇒ nothing fires) so the next regressing observation is checked
  /// against it rather than silently adopted as the first observation.
  pub fn observe_observer(&mut self, c: &Cluster, observer: SocketAddr) -> CheckResult {
    for subject in c.ids() {
      let inc = c.get_node_incarnation(observer, subject);
      let st = c.member_liveness(observer, subject);
      let r = self.observe_one(observer, subject, inc, st, false);
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }

  /// Feed every per-mutation [`Transition`] of one step, in order. Each
  /// transition runs through [`observe_one`](Self::observe_one) exactly as the
  /// post-step snapshot would, so an illegal intermediate value masked by a
  /// later same-step mutation is still observed. Returns the first violation.
  pub fn observe_transitions(&mut self, transitions: &[Transition]) -> CheckResult {
    for t in transitions {
      let r = self.observe_one(t.observer, &t.subject, t.incarnation, t.state, t.pruned);
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }
}

/// One meta per incarnation: within a single incarnation of a subject, the full
/// server tuple (meta, protocol_version, delegate_version) is immutable
/// cluster-wide. Two observers reporting different tuples for the same
/// (subject, incarnation) is a violation.
#[derive(Debug, Default)]
pub struct MetaPerIncarnationChecker {
  seen: HashMap<(SmolStr, u32), (Vec<u8>, u8, u8)>,
}

impl MetaPerIncarnationChecker {
  /// Create a new checker with no prior observations.
  pub fn new() -> Self {
    Self::default()
  }

  /// Feed one observed tuple for `(subject, incarnation)`.
  pub fn observe_one(
    &mut self,
    subject: &SmolStr,
    incarnation: u32,
    tuple: (Vec<u8>, u8, u8),
  ) -> CheckResult {
    let key = (subject.clone(), incarnation);
    match self.seen.get(&key) {
      Some(prev) if *prev != tuple => CheckResult::violation(format!(
        "meta divergence for {subject}@{incarnation}: {prev:?} vs {tuple:?}"
      )),
      Some(_) => CheckResult::Ok,
      None => {
        self.seen.insert(key, tuple);
        CheckResult::Ok
      }
    }
  }

  /// Observe the whole cluster snapshot: for every (observer, subject) pair,
  /// read the gossip-tracked server tuple and check for divergence within the
  /// same incarnation.
  ///
  /// Returns the first violation found, or [`CheckResult::Ok`].
  pub fn observe(&mut self, c: &Cluster) -> CheckResult {
    for &observer in c.addrs() {
      for subject in c.ids() {
        // Only check meta for Alive states. Suspect, Dead, and Left states
        // can bump the gossip-tracked incarnation via their respective wire
        // messages without updating the server arc's meta, so the
        // (gossip_inc, meta) pair is only atomically set by an Alive message.
        let liveness = c.member_liveness(observer, subject);
        if !matches!(liveness, Some(State::Alive)) {
          continue;
        }
        if let (Some(tuple), Some(inc)) = (
          c.member_meta(observer, subject),
          c.get_node_incarnation(observer, subject),
        ) {
          let r = self.observe_one(subject, inc, tuple);
          if r.is_violation() {
            return r;
          }
        }
      }
    }
    CheckResult::Ok
  }
}

/// No false resurrection: a subject seen `Dead`/`Left` at incarnation `I` may
/// return to `Alive` only at a strictly higher incarnation — UNLESS the machine
/// actually pruned the terminal entry this step, after which a fresh Alive is a
/// legal re-join. Absent a prune, an `Alive@<=I` after a terminal state is a real
/// resurrection.
#[derive(Debug, Default)]
pub struct NoResurrectionChecker {
  last: HashMap<(SocketAddr, SmolStr), Sample>,
}

impl NoResurrectionChecker {
  /// Create a checker with no prior observations.
  pub fn new() -> Self {
    Self::default()
  }

  /// Feed one observation. `pruned` is `true` when `reset_nodes` removed this
  /// (observer, subject) entry during the step being observed. All-`None` means
  /// absent: the baseline is cleared if `pruned`, otherwise retained.
  pub fn observe_one(
    &mut self,
    observer: SocketAddr,
    subject: &SmolStr,
    state: Option<State>,
    incarnation: Option<u32>,
    pruned: bool,
  ) -> CheckResult {
    let key = (observer, subject.clone());
    match (state, incarnation) {
      (Some(st), Some(inc)) => {
        if let Some(prev) = self.last.get(&key).copied() {
          let was_terminal = matches!(prev.state, State::Dead | State::Left);
          // A prune this step makes the current Alive a legal post-prune
          // re-learn; only an unpruned terminal baseline constrains it.
          if was_terminal
            && !pruned
            && matches!(st, State::Alive)
            && inc <= prev.incarnation
          {
            return CheckResult::violation(format!(
              "false resurrection at {observer} for {subject}: {:?}@{} -> Alive@{inc}",
              prev.state, prev.incarnation
            ));
          }
        }
        self.last.insert(key, Sample { state: st, incarnation: inc });
        CheckResult::Ok
      }
      _ => {
        // Clear the baseline ONLY on an actual prune; retain it on any other
        // disappearance — a non-terminal member vanishing, or a terminal one the
        // machine has not yet pruned — so a later resurrection still fires. An
        // observer restart clears explicitly via clear_observer.
        if pruned {
          self.last.remove(&key);
        }
        CheckResult::Ok
      }
    }
  }

  /// Clear all stored baselines for `observer`. Called when the observer node
  /// restarts — its member view is fresh and old baselines no longer apply.
  pub fn clear_observer(&mut self, observer: SocketAddr) {
    self.last.retain(|(obs, _), _| obs != &observer);
  }

  /// Observe the whole cluster snapshot.
  pub fn observe(&mut self, c: &Cluster) -> CheckResult {
    for &observer in c.addrs() {
      for subject in c.ids() {
        let st = c.member_liveness(observer, subject);
        let inc = c.get_node_incarnation(observer, subject);
        let pruned = c.was_pruned_this_step(observer, subject);
        let r = self.observe_one(observer, subject, st, inc, pruned);
        if r.is_violation() {
          return r;
        }
      }
    }
    CheckResult::Ok
  }

  /// Seed a single observer's current snapshot as the baseline. Called after a
  /// restart clears the observer's baselines: the restarted endpoint's fresh
  /// self-view must become the baseline (`pruned = false`, no prior after the
  /// clear ⇒ nothing fires) so the next resurrecting observation is checked
  /// against it rather than silently adopted as the first observation.
  pub fn observe_observer(&mut self, c: &Cluster, observer: SocketAddr) -> CheckResult {
    for subject in c.ids() {
      let st = c.member_liveness(observer, subject);
      let inc = c.get_node_incarnation(observer, subject);
      let r = self.observe_one(observer, subject, st, inc, false);
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }

  /// Feed every per-mutation [`Transition`] of one step, in order, so an illegal
  /// intermediate resurrection masked by a later same-step refute is observed.
  /// Returns the first violation.
  pub fn observe_transitions(&mut self, transitions: &[Transition]) -> CheckResult {
    for t in transitions {
      let r = self.observe_one(t.observer, &t.subject, t.state, t.incarnation, t.pruned);
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }
}

/// Illegal state-pair transitions for one subject at one observer:
///   - `Dead@I  -> Suspect@I`  — a dead node is not re-suspected at the same incarnation
///   - `Left@I  -> Alive@<=I`  — a departed node does not return alive without a higher incarnation
///   - `Left    -> Suspect`    — a node that announced departure is not suspected
///
/// A terminal baseline the machine actually pruned this step no longer
/// constrains the re-learned entry; an unpruned disappearance retains it.
#[derive(Debug, Default)]
pub struct IllegalPairChecker {
  last: HashMap<(SocketAddr, SmolStr), Sample>,
}

impl IllegalPairChecker {
  /// Create a checker with no prior observations.
  pub fn new() -> Self {
    Self::default()
  }

  /// Feed one observation. `pruned` is `true` when `reset_nodes` removed this
  /// (observer, subject) entry during the step being observed. All-`None` means
  /// absent: the baseline is cleared if `pruned`, otherwise retained.
  pub fn observe_one(
    &mut self,
    observer: SocketAddr,
    subject: &SmolStr,
    state: Option<State>,
    incarnation: Option<u32>,
    pruned: bool,
  ) -> CheckResult {
    let key = (observer, subject.clone());
    match (state, incarnation) {
      (Some(st), Some(inc)) => {
        if let Some(prev) = self.last.get(&key).copied() {
          // A prune this step makes the current observation a legal post-prune
          // re-learn; only an unpruned baseline constrains the transition.
          if !pruned {
            let illegal = match (prev.state, st) {
              (State::Dead, State::Suspect) => inc == prev.incarnation,
              (State::Left, State::Alive) => inc <= prev.incarnation,
              (State::Left, State::Suspect) => true,
              _ => false,
            };
            if illegal {
              return CheckResult::violation(format!(
                "illegal state pair at {observer} for {subject}: {:?}@{} -> {st:?}@{inc}",
                prev.state, prev.incarnation
              ));
            }
          }
        }
        self.last.insert(key, Sample { state: st, incarnation: inc });
        CheckResult::Ok
      }
      _ => {
        // Clear the baseline ONLY on an actual prune; retain it on any other
        // disappearance — a non-terminal member vanishing, or a terminal one the
        // machine has not yet pruned — so a later illegal transition still fires.
        // An observer restart clears explicitly via clear_observer.
        if pruned {
          self.last.remove(&key);
        }
        CheckResult::Ok
      }
    }
  }

  /// Clear all stored baselines for `observer`. Called when the observer node
  /// restarts — its member view is fresh and old baselines no longer apply.
  pub fn clear_observer(&mut self, observer: SocketAddr) {
    self.last.retain(|(obs, _), _| obs != &observer);
  }

  /// Observe the whole cluster snapshot.
  pub fn observe(&mut self, c: &Cluster) -> CheckResult {
    for &observer in c.addrs() {
      for subject in c.ids() {
        let st = c.member_liveness(observer, subject);
        let inc = c.get_node_incarnation(observer, subject);
        let pruned = c.was_pruned_this_step(observer, subject);
        let r = self.observe_one(observer, subject, st, inc, pruned);
        if r.is_violation() {
          return r;
        }
      }
    }
    CheckResult::Ok
  }

  /// Seed a single observer's current snapshot as the baseline. Called after a
  /// restart clears the observer's baselines: the restarted endpoint's fresh
  /// self-view must become the baseline (`pruned = false`, no prior after the
  /// clear ⇒ nothing fires) so the next illegal transition is checked against
  /// it rather than silently adopted as the first observation.
  pub fn observe_observer(&mut self, c: &Cluster, observer: SocketAddr) -> CheckResult {
    for subject in c.ids() {
      let st = c.member_liveness(observer, subject);
      let inc = c.get_node_incarnation(observer, subject);
      let r = self.observe_one(observer, subject, st, inc, false);
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }

  /// Feed every per-mutation [`Transition`] of one step, in order, so an illegal
  /// intermediate state pair masked by a later same-step transition is observed.
  /// Returns the first violation.
  pub fn observe_transitions(&mut self, transitions: &[Transition]) -> CheckResult {
    for t in transitions {
      let r = self.observe_one(t.observer, &t.subject, t.state, t.incarnation, t.pruned);
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }
}

/// Self-incarnation monotonicity: a node's own incarnation never decreases.
#[derive(Debug, Default)]
pub struct SelfIncarnationChecker {
  max_self: HashMap<SocketAddr, u32>,
}

impl SelfIncarnationChecker {
  /// Create a new checker with no prior observations.
  pub fn new() -> Self {
    Self::default()
  }

  /// Feed one self-incarnation reading for `host`.
  pub fn observe_one(&mut self, host: SocketAddr, incarnation: u32) -> CheckResult {
    if let Some(&prev) = self.max_self.get(&host) {
      if incarnation < prev {
        return CheckResult::violation(format!(
          "self-incarnation regressed at {host}: {prev} -> {incarnation}"
        ));
      }
    }
    self.max_self.insert(host, incarnation);
    CheckResult::Ok
  }

  /// Observe every node's own incarnation.
  ///
  /// Returns the first violation found, or [`CheckResult::Ok`].
  pub fn observe(&mut self, c: &Cluster) -> CheckResult {
    for &host in c.addrs() {
      if let Some(inc) = c.local_incarnation(host) {
        let r = self.observe_one(host, inc);
        if r.is_violation() {
          return r;
        }
      }
    }
    CheckResult::Ok
  }
}

/// Boundedness: a host never knows more members than were introduced into the
/// cluster, and its broadcast queue stays under a generous retransmit-derived
/// backstop. The queue bound is intentionally loose — it catches runaway
/// growth, not transient gossip spikes.
#[derive(Debug, Default)]
pub struct BoundednessChecker {
  retransmit_mult: u32,
}

impl BoundednessChecker {
  /// Create a new checker with the given retransmit multiplier.
  pub fn new(retransmit_mult: u32) -> Self {
    Self { retransmit_mult }
  }

  /// Feed one host's counts: members it knows, nodes ever introduced, and its
  /// broadcast queue length.
  pub fn observe_one(
    &mut self,
    host: SocketAddr,
    num_members: usize,
    introduced: usize,
    queue_len: usize,
  ) -> CheckResult {
    if num_members > introduced {
      return CheckResult::violation(format!(
        "member count out of bounds at {host}: knows {num_members} > {introduced} introduced"
      ));
    }
    let limit =
      memberlist_proto::broadcast::retransmit_limit(self.retransmit_mult, introduced.max(1) as u32)
        .max(1) as usize;
    // Deliberately loose: one queued broadcast per introduced node, each held
    // for `limit` retransmits, times a slack factor. Catches unbounded growth
    // without firing on transient gossip spikes.
    let bound = introduced.max(1) * limit * 8;
    if queue_len > bound {
      return CheckResult::violation(format!(
        "broadcast queue out of bounds at {host}: {queue_len} > {bound}"
      ));
    }
    CheckResult::Ok
  }

  /// Observe the whole cluster.
  ///
  /// Returns the first violation found, or [`CheckResult::Ok`].
  pub fn observe(&mut self, c: &Cluster) -> CheckResult {
    let introduced = c.ids().len();
    for &host in c.addrs() {
      let r =
        self.observe_one(host, c.num_members(host), introduced, c.broadcast_queue_len(host));
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }
}

/// Self-liveness: while a node is running it never sees itself `Dead` or `Left`.
#[derive(Debug, Default)]
pub struct SelfLivenessChecker;

impl SelfLivenessChecker {
  /// Create a new checker with no prior observations.
  pub fn new() -> Self {
    Self
  }

  /// Feed one self-liveness reading: is the node running, and what state does it
  /// hold for itself.
  pub fn observe_one(
    &mut self,
    host: SocketAddr,
    running: bool,
    self_state: Option<State>,
  ) -> CheckResult {
    if running {
      if let Some(st) = self_state {
        if matches!(st, State::Dead | State::Left) {
          return CheckResult::violation(format!(
            "node {host} sees itself {st:?} while running"
          ));
        }
      }
    }
    CheckResult::Ok
  }

  /// Observe every running node's view of itself.
  ///
  /// Returns the first violation found, or [`CheckResult::Ok`].
  pub fn observe(&mut self, c: &Cluster) -> CheckResult {
    for &host in c.addrs() {
      let running = c.is_running(host);
      let self_state = c.local_id(host).and_then(|id| c.member_liveness(host, &id));
      let r = self.observe_one(host, running, self_state);
      if r.is_violation() {
        return r;
      }
    }
    CheckResult::Ok
  }
}

/// Liveness convergence oracle, evaluated at the calm-window end. The cluster
/// has converged when every node that did not gracefully leave is `Alive` at
/// every such (live) observer, at an agreed incarnation, and every left node is
/// `Left` or absent everywhere. Disagreement on the dead set is tolerated
/// (asynchronous GC); only the alive set and the terminal-ness of left nodes
/// are gated. Stateless — call `check` at quiescence.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConvergenceChecker;

impl ConvergenceChecker {
  /// Create a new convergence oracle.
  pub fn new() -> Self {
    Self
  }

  /// Check convergence; `left` is the set of gracefully-departed node addresses.
  ///
  /// Returns the first violation found, or [`CheckResult::Ok`] if converged.
  pub fn check(&self, c: &Cluster, left: &HashSet<SocketAddr>) -> CheckResult {
    // Live (observer, id) pairs: every node that did not gracefully leave.
    let live: Vec<(SocketAddr, &SmolStr)> = c
      .addrs()
      .iter()
      .zip(c.ids())
      .filter(|(a, _)| !left.contains(a))
      .map(|(&a, id)| (a, id))
      .collect();

    // Harness truth overrides stale membership views: a node still darkened
    // (crashed and never restarted) is down, no matter how many peers still hold
    // an Alive row for it. The calm phase restarts every crashed node, so a
    // still-crashed non-left node is itself a convergence failure — checked
    // before the member rows so a stale Alive cannot certify a down node.
    for (addr, id) in &live {
      if c.is_crashed(*addr) {
        return CheckResult::violation(format!(
          "not converged: crashed node {id} ({addr}) was not restored to the cluster"
        ));
      }
    }

    // Alive-set agreement: each live subject is Alive at every live observer,
    // all live observers agree on its incarnation, AND each observer binds the
    // subject ID to its REGISTERED address. The address check closes a routing
    // hole: a cluster that agrees `n1` is Alive@1 but routes it to a wrong/stale
    // address has not actually converged — it would send to the wrong peer.
    for (subject_addr, subject) in &live {
      let mut agreed: Option<u32> = None;
      for (observer, _) in &live {
        match c.member_liveness(*observer, subject) {
          Some(State::Alive) => {
            match (agreed, c.get_node_incarnation(*observer, subject)) {
              (None, inc) => agreed = inc,
              (Some(a), Some(v)) if a != v => {
                return CheckResult::violation(format!(
                  "not converged: observers disagree on {subject} incarnation ({a} vs {v})"
                ));
              }
              _ => {}
            }
            // The observed Alive node must point at the subject's registered
            // address. A mismatch means the ID→address binding diverged.
            if let Some(ns) = c.member(*observer, subject) {
              let observed_addr = *ns.address_ref();
              if observed_addr != *subject_addr {
                return CheckResult::violation(format!(
                  "not converged: {observer} binds {subject} to {observed_addr}, expected {subject_addr}"
                ));
              }
            }
          }
          other => {
            return CheckResult::violation(format!(
              "not converged: live node {subject} is {other:?} at {observer} (expected Alive)"
            ));
          }
        }
      }
    }

    // Departed nodes are terminal at every live observer: Left, Dead (still
    // within the GC window), or absent (GC'd). A departed node seen as
    // Alive or Suspect is the only pathological case — it would mean the
    // node is incorrectly considered part of the active membership.
    // Dead is accepted because the graceful-leave Death broadcast can be
    // lost during a chaotic partition phase; a Dead entry for a left node
    // is terminal and will GC normally.
    let left_ids: Vec<&SmolStr> = c
      .addrs()
      .iter()
      .zip(c.ids())
      .filter(|(a, _)| left.contains(a))
      .map(|(_, id)| id)
      .collect();
    for (observer, _) in &live {
      for left_id in &left_ids {
        match c.member_liveness(*observer, left_id) {
          None | Some(State::Left) | Some(State::Dead) => {}
          Some(other) => {
            return CheckResult::violation(format!(
              "not converged: departed node {left_id} is {other:?} at {observer} (expected Left/Dead/absent)"
            ));
          }
        }
      }
    }

    CheckResult::Ok
  }
}
