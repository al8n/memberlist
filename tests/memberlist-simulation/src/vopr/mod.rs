//! A VOPR-style deterministic adversarial driver for the SWIM machine.
//!
//! [`run_vopr`] is a pure function of `(seed, ticks)`: it builds a cluster, then
//! each tick applies a seed-chosen mix of network chaos (drop/duplicate/jitter),
//! partition/heal, ticks, and runs every safety checker. A violation panics with
//! `seed` + `tick` so the failure replays via [`run_vopr_one`].

use std::{
  collections::{BTreeSet, HashSet},
  net::SocketAddr,
  time::Duration,
};

/// Default `suspicion_max_timeout_mult` from `EndpointOptions` — the maximum
/// multiplier applied to the suspicion timeout. Mirrors the value in
/// `memberlist-proto/src/config.rs` (`suspicion_max_timeout_mult: 6`).
const SUSPICION_MAX_MULT: u32 = 6;

/// How long to let the just-healed topology settle before re-seeding any node
/// still isolated from the cluster. `enter_calm`'s heal/restart/shutdown churn
/// must clear first, or a re-seed join races it and is lost. Once settled, a
/// single fault-free push-pull join converges the node (its own periodic
/// push-pull finishes once it knows one peer), so the calm phase re-seeds
/// exactly once: a node still isolated afterwards is a genuine liveness wedge the
/// convergence check surfaces, not something a blind retry should mask.
const RESEED_DELAY: Duration = Duration::from_secs(2);

/// A protocol-derived upper bound on how long, in virtual time, a healed
/// cluster of `n` nodes needs to re-converge. The formula is generous on
/// purpose: the calm loop exits early on convergence, so a large floor only
/// affects genuinely non-converging seeds (a real liveness wedge, not a
/// timing miss). The formula accounts for darkened left-node failure detection
/// (Suspect→Dead takes up to suspicion_max) and limited force-join help
/// (isolated nodes only), so organic gossip convergence may approach the full
/// suspicion window.
fn convergence_floor(
  push_pull: Duration,
  probe_interval: Duration,
  suspicion_mult: u32,
  suspicion_max_mult: u32,
  gossip_interval: Duration,
  n: usize,
) -> Duration {
  let log10n = (n as f64).log10().max(1.0);
  let suspicion_max =
    probe_interval.mul_f64(suspicion_mult as f64 * log10n * suspicion_max_mult as f64);
  let gossip = gossip_interval * ((n as f64).log2().ceil() as u32 + 1);
  // Two full suspicion windows: one to failure-detect darkened leavers, one
  // for the remaining re-convergence. Plus one push-pull and gossip rounds.
  push_pull + 2 * suspicion_max + gossip
}

use rand::{RngExt, SeedableRng, rngs::SmallRng};
use smol_str::{SmolStr, format_smolstr};

use crate::{
  Alive, Cluster, Meta, Node, State,
  checker::{
    BoundednessChecker, ConvergenceChecker, IllegalPairChecker, IncarnationMonotonicChecker,
    MetaPerIncarnationChecker, NoResurrectionChecker, SelfIncarnationChecker, SelfLivenessChecker,
  },
};

/// A summary of one [`run_vopr`] run — the schedule it actually explored.
#[derive(Debug, Default, Clone)]
pub struct VoprReport {
  /// Total ticks executed.
  pub ticks: usize,
  /// Cluster size for this run.
  pub n: usize,
  /// Actual datagrams dropped by the probabilistic drop fault (the
  /// `drop_per_mille` roll fired). Counts real dropped datagrams, not
  /// rate-enabled ticks; excludes crash/partition drops.
  pub total_drops: u64,
  /// Actual datagrams duplicated by the probabilistic duplicate fault (the
  /// `duplicate_per_mille` roll fired). Counts real duplicated datagrams, not
  /// rate-enabled ticks.
  pub total_duplicates: u64,
  /// Partition windows opened.
  pub total_partitions: u64,
  /// Distinct partition topologies (node-set cuts) actually exercised. Keyed by
  /// cluster size and the canonical group assignment, so the campaign can prove
  /// partitions explore more than one fixed split rather than always cutting the
  /// same nodes onto the same sides.
  pub partition_topologies: BTreeSet<u32>,
  /// Nodes crashed (hard stop).
  pub total_crashes: u64,
  /// Crashed nodes restarted.
  pub total_restarts: u64,
  /// Nodes that gracefully left.
  pub total_leaves: u64,
  /// (observer, subject) samples observed as Suspect across the run
  /// — non-vacuity coverage proof that the campaign exercised Suspect state.
  pub total_suspect_samples: u64,
  /// (observer, subject) samples observed as Dead across the run
  /// — non-vacuity coverage proof that the campaign drove nodes to Dead.
  pub total_dead_samples: u64,
  /// (observer, subject) samples observed as Left across the run
  /// — non-vacuity coverage proof that the campaign drove nodes to Left.
  pub total_left_samples: u64,
  /// Ticks executed in the post-chaos calm window (quiesce-to-convergence).
  pub calm_ticks: u64,
  /// Whether the cluster converged within the calm window (liveness gate).
  pub converged: bool,
}

/// Run one seeded simulation for `ticks` ticks. Panics with `seed`+`tick` on any
/// invariant violation.
///
/// The run has two phases:
///
/// **Chaos phase** — `ticks` ticks of seed-chosen fault injection (drops,
/// duplicates, jitter, partitions, crashes, restarts, graceful leaves). Safety
/// checkers run throughout. `report.ticks` always equals the `ticks` argument.
///
/// **Calm phase** — faults are zeroed, every partition is healed, and every
/// still-crashed node is restarted. The cluster then runs for up to one
/// protocol-derived convergence floor of virtual time so the membership state
/// can stabilise after chaos. Safety checkers stay live.
pub fn run_vopr(seed: u64, ticks: usize) -> VoprReport {
  let mut v = Vopr::new(seed);
  let mut c = v.build_cluster();
  let mut report = VoprReport {
    ticks,
    n: v.n,
    ..Default::default()
  };

  // Seed the three history checkers' baselines from the bootstrap snapshot, so
  // the pre-loop `alive_node` state is their baseline. Per step they then
  // consume the per-mutation transition log (which continues from this row);
  // the remaining checkers and the oracle stay post-step.
  v.seed_history_baselines(&c);

  // Chaos phase: seed-chosen fault injection.
  for tick in 0..ticks {
    // Clear the per-tick transition log at the tick boundary so it spans BOTH
    // the chooser-phase membership mutations (`apply_actions` may call
    // `c.leave`, which records the leaver's `Alive -> Left`) AND the mutations
    // `c.step` records. `check_safety` then reads the whole tick's log before
    // the next tick clears it.
    c.clear_history_transitions();
    v.apply_actions(&mut c, &mut report);
    c.step();
    v.check_safety(&c, seed, tick);
    v.tally_coverage(&c, &mut report);
  }

  // Calm phase: heal + restart-all + zero faults, then run until the protocol
  // convergence floor of virtual time has elapsed. Safety stays live.
  let calm_restarted = v.enter_calm(&mut c);
  let floor = convergence_floor(
    Duration::from_secs(30),     // push_pull_interval
    Duration::from_millis(1000), // probe_interval
    4,                           // suspicion_mult
    SUSPICION_MAX_MULT,
    Duration::from_millis(200), // gossip_interval
    v.n,
  );
  let calm_start = c.now();
  let reseed_at = calm_start + RESEED_DELAY;
  let mut reseeded = false;
  let max_calm_ticks = 100_000usize; // backstop; the floor is the real bound
  let mut calm = 0usize;
  let mut tick = ticks;
  let conv = ConvergenceChecker::new();
  while c.now().saturating_duration_since(calm_start) < floor && calm < max_calm_ticks {
    // Same tick-boundary clear as the chaos phase. The calm phase injects no
    // chooser-phase membership mutations, so each calm tick's log is exactly
    // that tick's step transitions — but the clear is still required because
    // `step` only appends, never clears.
    c.clear_history_transitions();
    let progressed = c.step();
    v.check_safety(&c, seed, tick);
    v.tally_coverage(&c, &mut report);
    report.calm_ticks += 1;
    tick += 1;
    calm += 1;
    // Once the just-healed topology has settled, re-seed exactly once: every
    // calm-restarted node unconditionally (its single intended join) plus any
    // other still-isolated node. A node that stays isolated afterwards is a real
    // liveness wedge the convergence check surfaces, not something a retry masks.
    if !reseeded && c.now() >= reseed_at {
      v.reseed_isolated(&mut c, &calm_restarted);
      reseeded = true;
    }
    // When step returns false AND the clock did not advance (no next deadline
    // was found), the cluster is truly idle — no pending timers or in-flight
    // messages remain. Exit early rather than spin to the backstop cap.
    let elapsed = c.now().saturating_duration_since(calm_start);
    if !progressed && elapsed == Duration::ZERO {
      break;
    }
  }

  // Record the ACTUAL fault counts accumulated by the network over the whole
  // run (chaos + calm). These are real dropped/duplicated datagrams, not
  // rate-enabled ticks, so the non-vacuity assertions stay meaningful and a
  // masked-off (or broken) fault class reports zero.
  report.total_drops = c.fault_drops();
  report.total_duplicates = c.fault_duplicates();

  // Gate that the cluster actually converged within the floor. A failure here
  // is a genuine liveness wedge: the protocol did not re-converge after healing.
  if let Some(reason) = conv.check(&c, &v.left).reason() {
    dump_divergence(&c);
    panic!("VOPR liveness violation: seed {seed} did not converge within the calm floor: {reason}");
  }
  report.converged = true;

  report
}

/// Replay one seed at a fixed tick budget (the campaign default).
pub fn run_vopr_one(seed: u64) -> VoprReport {
  run_vopr(seed, 1500)
}

struct Vopr {
  rng: SmallRng,
  n: usize,
  addrs: Vec<SocketAddr>,
  ids: Vec<SmolStr>,
  incarnation: IncarnationMonotonicChecker,
  resurrection: NoResurrectionChecker,
  meta: MetaPerIncarnationChecker,
  self_incarnation: SelfIncarnationChecker,
  self_liveness: SelfLivenessChecker,
  illegal_pair: IllegalPairChecker,
  boundedness: BoundednessChecker,
  /// Crashed nodes that have not yet been restarted.
  gone: HashSet<SocketAddr>,
  /// Nodes that crashed and were subsequently restarted.
  restarted: HashSet<SocketAddr>,
  /// Nodes that gracefully left (terminal — not rejoined).
  left: HashSet<SocketAddr>,
}

impl Vopr {
  fn new(seed: u64) -> Self {
    let mut rng = SmallRng::seed_from_u64(seed);
    let n = rng.random_range(2..=8usize);
    let base = 40000u16;
    let addrs: Vec<SocketAddr> = (0..n)
      .map(|i| format!("127.0.0.1:{}", base + i as u16).parse().unwrap())
      .collect();
    let ids: Vec<SmolStr> = (0..n).map(|i| format_smolstr!("v{i}")).collect();
    Self {
      rng,
      n,
      addrs,
      ids,
      incarnation: IncarnationMonotonicChecker::new(),
      resurrection: NoResurrectionChecker::new(),
      meta: MetaPerIncarnationChecker::new(),
      self_incarnation: SelfIncarnationChecker::new(),
      self_liveness: SelfLivenessChecker::new(),
      illegal_pair: IllegalPairChecker::new(),
      boundedness: BoundednessChecker::new(4),
      gone: HashSet::new(),
      restarted: HashSet::new(),
      left: HashSet::new(),
    }
  }

  fn build_cluster(&mut self) -> Cluster {
    let mut c = Cluster::new();
    c.seed_faults(self.rng.random::<u64>());
    c.set_latency(Duration::from_millis(self.rng.random_range(0..=80)));
    for (i, (id, &a)) in self.ids.iter().zip(&self.addrs).enumerate() {
      // Each node gets a distinct 1-byte meta derived from its index so the
      // MetaPerIncarnationChecker operates on non-empty, non-uniform values.
      let node_meta = Meta::try_from(&[i as u8][..]).expect("1-byte meta is always valid");
      c.add_node_with(id.clone(), a, |opts| opts.with_initial_meta(node_meta));
    }
    // Bootstrap node0 with all peers. Use alive_node (with correct meta) so the
    // MetaPerIncarnationChecker sees a consistent (meta, incarnation) tuple from
    // the start — inject_alive always produces an empty-meta Alive.
    for i in 1..self.n {
      let peer_meta = Meta::try_from(&[i as u8][..]).expect("1-byte meta is always valid");
      let alive = Alive::new(1, Node::new(self.ids[i].clone(), self.addrs[i])).with_meta(peer_meta);
      c.alive_node(self.addrs[0], alive, true);
    }
    c
  }

  fn apply_actions(&mut self, c: &mut Cluster, report: &mut VoprReport) {
    // Re-roll the chaos knobs each tick. The draws are unconditional so the
    // schedule stays stable across the MEMBERLIST_VOPR_NO_* shrink masks.
    let drop = self.rng.random_range(0..=600u32);
    let dup = self.rng.random_range(0..=150u32);
    let jit = self.rng.random_range(0..=120u64);
    c.set_drop_per_mille(masked("DROP", drop));
    c.set_duplicate_per_mille(masked("DUP", dup));
    c.set_jitter(Duration::from_millis(masked("JITTER", jit as u32) as u64));

    // Lifecycle + topology chaos — every roll AND candidate index drawn
    // unconditionally so the MEMBERLIST_VOPR_NO_* masks suppress effects, not
    // the schedule.
    let do_partition = self.rng.random_bool(0.02);
    let do_heal = self.rng.random_bool(0.05);
    let do_crash = self.rng.random_bool(0.01);
    let do_restart = self.rng.random_bool(0.05);
    let do_leave = self.rng.random_bool(0.005);
    let cand = self.rng.random_range(0..self.n);
    let restart_pick = self.rng.random_range(0..self.n);
    // Per-node side assignment for a partition: bit i selects node i's group.
    // The range [1, 2^n - 2] guarantees both groups are non-empty (at least one
    // set bit and one clear bit). Drawn here, unconditionally, so masking
    // PARTITION suppresses the cut, not the schedule, and so each partition
    // window can isolate a different node set rather than always splitting at
    // the midpoint.
    let partition_mask = self.rng.random_range(1..=((1u32 << self.n) - 2));

    // Install the partition now, but defer its coverage recording until the
    // tick's lifecycle actions below have settled the live set (see the end of
    // this method).
    let mut partition_cut: Option<u32> = None;
    if !masked_off("PARTITION") && do_partition {
      self.install_partition(c, partition_mask);
      report.total_partitions += 1;
      partition_cut = Some(partition_mask);
    } else if do_heal {
      c.heal();
    }

    // Crash a live node, keeping at least 2 live.
    if !masked_off("CRASH") && do_crash {
      let addr = self.addrs[cand];
      if self.is_live(addr) && self.live_count() > 2 {
        c.crash(addr);
        self.gone.insert(addr);
        report.total_crashes += 1;
      }
    }

    // Restart a gone node at a higher incarnation. The restarted node starts
    // with a fresh membership view, so all checker baselines for it as an
    // *observer* are stale and must be cleared.
    if !masked_off("CRASH") && do_restart && !self.gone.is_empty() {
      let gone: Vec<SocketAddr> = self
        .addrs
        .iter()
        .copied()
        .filter(|a| self.gone.contains(a))
        .collect();
      let addr = gone[restart_pick % gone.len()];
      // A restart only fails when the superseding incarnation would leave the
      // safe range (unreachable here — the VOPR's incarnations stay small); if
      // it ever did, the node stays in `gone` rather than the run panicking.
      if c.restart(addr) {
        self.gone.remove(&addr);
        self.restarted.insert(addr);
        self.incarnation.clear_observer(addr);
        self.resurrection.clear_observer(addr);
        self.illegal_pair.clear_observer(addr);
        // Re-establish the restarted observer's fresh self-view as the baseline;
        // restart records no transition, so the clear above would otherwise leave
        // the next mutation for this observer unchecked.
        self.seed_history_baselines_for(c, addr);
        report.total_restarts += 1;
      }
    }

    // Graceful leave (terminal), keeping at least 2 live.
    if !masked_off("LEAVE") && do_leave {
      let addr = self.addrs[cand];
      if self.is_live(addr) && self.live_count() > 2 && c.leave(addr).is_ok() {
        self.left.insert(addr);
        report.total_leaves += 1;
      }
    }

    // Record partition coverage only now, after this tick's crash/restart/leave
    // have settled the live set, and just before `c.step`. A cut that a same-tick
    // crash or leave rendered inert (every live node on one side) yields no key;
    // a same-tick restart that revived a node is reflected in the live set.
    if let Some(mask) = partition_cut {
      if let Some(key) = self.effective_live_cut_key(mask) {
        report.partition_topologies.insert(key);
      }
    }
  }

  /// Quiesce the cluster: heal every partition, restart every still-crashed
  /// node (so the live set is whole again), and zero all probabilistic fault
  /// rates. Graceful-left nodes stay Left — a leave is terminal. After this
  /// the only remaining dynamics are the protocol settling.
  ///
  /// Left nodes are darkened (shutdown) to model the process exit that follows
  /// a graceful leave. During chaos their Dead-self broadcast had the
  /// opportunity to propagate; now their endpoint goes silent so peers that
  /// missed the broadcast will probe → no ack → Suspect → Dead → convergence.
  ///
  /// Crash-restarted nodes come back WITHOUT an auto-join, so the single
  /// post-settle reseed is their only join attempt. Their addresses are returned
  /// so the reseed can force exactly one join for each — never suppressed by
  /// connectivity they may pick up organically — making a join that completes
  /// without converging a node observable rather than masked.
  fn enter_calm(&mut self, c: &mut Cluster) -> Vec<SocketAddr> {
    c.heal();
    // Iterate in addrs order (a fixed Vec) for a deterministic restart sequence.
    let still_gone: Vec<SocketAddr> = self
      .addrs
      .iter()
      .copied()
      .filter(|a| self.gone.contains(a))
      .collect();
    let mut calm_restarted = Vec::new();
    for addr in still_gone {
      // Unreachable headroom-exhaustion aside (the VOPR keeps incarnations
      // small), a node that cannot restart stays in `gone` rather than panicking
      // the run; the post-calm convergence check then surfaces it as a wedge.
      // Restart WITHOUT an auto-join so the single post-settle reseed is this
      // node's only join — a restart join that completed without converging it
      // must not be masked by a later second attempt.
      if c.restart_without_join(addr) {
        self.gone.remove(&addr);
        self.restarted.insert(addr);
        calm_restarted.push(addr);
        self.incarnation.clear_observer(addr);
        self.resurrection.clear_observer(addr);
        self.illegal_pair.clear_observer(addr);
        // Re-establish the restarted observer's fresh self-view as the baseline;
        // restart records no transition, so the clear above would otherwise leave
        // the next mutation for this observer unchecked.
        self.seed_history_baselines_for(c, addr);
      }
    }
    c.set_drop_per_mille(0);
    c.set_duplicate_per_mille(0);
    c.set_jitter(Duration::ZERO);

    // Left nodes have announced their departure during chaos; now model the
    // process exit so a peer that missed the leave broadcast failure-detects
    // them (a still-acking left node would otherwise be held Alive forever).
    // Iterate in addrs order for a deterministic shutdown sequence.
    let departed: Vec<SocketAddr> = self
      .addrs
      .iter()
      .copied()
      .filter(|a| self.left.contains(a))
      .collect();
    for addr in departed {
      c.shutdown(addr);
    }
    calm_restarted
  }

  /// Issue a rejoin (push-pull `join`) toward a CONNECTED live peer that knows
  /// the cluster (the best-connected, by member count) for every node that needs
  /// one: each `calm_restarted` node UNCONDITIONALLY (its single intended join,
  /// not suppressed by a peer it may have learned organically), plus any other
  /// live node still isolated — one that sees no *other* live node as `Alive`. A
  /// darkened leaver still showing `Alive` does NOT count as connectivity, so a
  /// node connected only to a leaver is re-seeded rather than isolating itself.
  ///
  /// The seed is chosen preferentially from connected nodes that are NOT
  /// themselves calm-restarted, so every calm-restarted node is a non-seed and
  /// receives its forced join; raw member count alone is unsafe, since a node
  /// with many stale or departed rows can top it while being isolated. When the
  /// only viable seed is itself calm-restarted (every connected node restarted,
  /// or the all-isolated fallback), it is cross-joined to another live node so it
  /// too gets exactly one intended join. Returns the addresses joined.
  ///
  /// A restarted node re-converges only via a successful push-pull join: peers
  /// prune the crashed node and never contact it back, and a node with an empty
  /// table initiates no push-pull of its own. The calm phase calls this once,
  /// after the post-chaos churn has settled (see `RESEED_DELAY`), so the join is
  /// not lost to the heal/restart/shutdown activity at calm entry.
  fn reseed_isolated(&self, c: &mut Cluster, calm_restarted: &[SocketAddr]) -> Vec<SocketAddr> {
    let live: Vec<SocketAddr> = self
      .addrs
      .iter()
      .copied()
      .filter(|a| self.is_live(*a))
      .collect();
    // Classify each live node as connected (sees some OTHER live node as Alive)
    // or isolated, up front so the join loop below can borrow `c` mutably.
    let connected: Vec<bool> = live
      .iter()
      .map(|&addr| {
        self
          .addrs
          .iter()
          .zip(self.ids.iter())
          .any(|(&peer, peer_id)| {
            peer != addr
              && self.is_live(peer)
              && c.member_liveness(addr, peer_id) == Some(State::Alive)
          })
      })
      .collect();
    let members: Vec<usize> = live.iter().map(|&a| c.num_members(a)).collect();
    // Prefer a connected seed that is NOT itself calm-restarted, so every
    // calm-restarted node is a non-seed and receives its forced join. Fall back
    // to any connected node, then any live node (the all-isolated cross-join
    // base). Raw member count alone is unsafe: a node with many stale or departed
    // rows can top it while being isolated.
    let seed_idx = (0..live.len())
      .filter(|&i| connected[i] && !calm_restarted.contains(&live[i]))
      .max_by_key(|&i| members[i])
      .or_else(|| {
        (0..live.len())
          .filter(|&i| connected[i])
          .max_by_key(|&i| members[i])
      })
      .or_else(|| (!live.is_empty()).then_some(0));
    let Some(seed_idx) = seed_idx else {
      return Vec::new();
    };
    let seed = live[seed_idx];
    let mut joined = Vec::new();
    for (i, &addr) in live.iter().enumerate() {
      // A calm-restarted node gets its single intended join unconditionally —
      // never suppressed by connectivity it may have picked up organically, so a
      // join that completes without merging is surfaced, not masked. Any other
      // node is reseeded only when isolated.
      if i != seed_idx && (calm_restarted.contains(&addr) || !connected[i]) {
        c.join(addr, seed);
        joined.push(addr);
      }
    }
    // If the chosen seed is itself calm-restarted — only when every connected
    // node is restarted, or in the all-isolated fallback — it would otherwise be
    // the one calm-restarted node without a forced join. Cross-join it to another
    // live node so it, too, gets exactly one intended join (it also re-converges
    // from its joiners' inbound push-pull).
    if calm_restarted.contains(&seed) {
      if let Some(other) = live.iter().copied().find(|&a| a != seed) {
        c.join(seed, other);
        joined.push(seed);
      }
    }
    joined
  }

  /// Install a two-group network partition: bit `i` of `mask` selects node `i`'s
  /// side (the draw range guarantees both groups non-empty). This isolates an
  /// arbitrary node subset — singletons, asymmetric minorities, differing
  /// component memberships — not just a fixed midpoint cut. Coverage is recorded
  /// separately, after the tick's lifecycle actions settle the live set.
  fn install_partition(&self, c: &mut Cluster, mask: u32) {
    let mut group_a: Vec<SocketAddr> = Vec::new();
    let mut group_b: Vec<SocketAddr> = Vec::new();
    for (i, &addr) in self.addrs.iter().enumerate() {
      if mask & (1 << i) != 0 {
        group_a.push(addr);
      } else {
        group_b.push(addr);
      }
    }
    c.partition(&group_a, &group_b);
  }

  /// The canonical coverage key for the EFFECTIVE LIVE cut `mask` produces over
  /// the nodes live *now*, or `None` if the mask leaves every live node on one
  /// side (an inert partition). Restricting to the current live set — evaluated
  /// after the tick's crash/restart/leave — keeps a cut that a same-tick
  /// lifecycle change rendered inert from inflating coverage. A partition is
  /// symmetric, so the live side and its complement fold to the smaller; the
  /// live set is folded into the key so distinct live sets stay distinct.
  fn effective_live_cut_key(&self, mask: u32) -> Option<u32> {
    let mut live_set = 0u32;
    let mut live_a = 0u32;
    for (i, &addr) in self.addrs.iter().enumerate() {
      if !self.is_live(addr) {
        continue;
      }
      live_set |= 1 << i;
      if mask & (1 << i) != 0 {
        live_a |= 1 << i;
      }
    }
    let live_b = live_set & !live_a;
    (live_a != 0 && live_b != 0).then(|| {
      let canon = live_a.min(live_b);
      (live_set << 8) | canon
    })
  }

  /// A node is live if it has neither crashed-without-restart nor left.
  fn is_live(&self, addr: SocketAddr) -> bool {
    !self.gone.contains(&addr) && !self.left.contains(&addr)
  }

  fn live_count(&self) -> usize {
    self.addrs.iter().filter(|&&a| self.is_live(a)).count()
  }

  /// Count Suspect/Dead/Left samples this tick from LIVE PEER observers only —
  /// the campaign's non-vacuity proof that these states are genuinely exercised.
  /// A crashed or departed observer holds a frozen, stale view, and a node's own
  /// self-row turns `Left` synchronously the moment it calls `leave`; counting
  /// either would let the coverage gates pass with no live peer ever observing
  /// the state on another node. So restrict to observers that are live and not
  /// the subject.
  fn tally_coverage(&self, c: &Cluster, report: &mut VoprReport) {
    for (observer, observer_id) in self.addrs.iter().copied().zip(self.ids.iter()) {
      if !self.is_live(observer) {
        continue;
      }
      for subject in &self.ids {
        if subject == observer_id {
          continue;
        }
        match c.member_liveness(observer, subject) {
          Some(State::Suspect) => report.total_suspect_samples += 1,
          Some(State::Dead) => report.total_dead_samples += 1,
          Some(State::Left) => report.total_left_samples += 1,
          _ => {}
        }
      }
    }
  }

  /// Seed the three history checkers' baselines from the current cluster
  /// snapshot. Called once before the chaos loop so the bootstrap state is the
  /// baseline; thereafter the per-step transition log continues from it.
  fn seed_history_baselines(&mut self, c: &Cluster) {
    self.incarnation.observe(c);
    self.resurrection.observe(c);
    self.illegal_pair.observe(c);
  }

  /// Re-seed the three history checkers' baselines for a single observer from
  /// its current snapshot. Called immediately after `clear_observer(addr)` on a
  /// restart: `clear_observer` wipes the crashed view but `restart` records no
  /// transition, so without this the restarted endpoint's fresh self row
  /// (`Alive@next_inc`) and re-learned view would never become a baseline — the
  /// first subsequent mutation for `(addr, *)` would be silently adopted instead
  /// of checked. After the clear there is no prior for `addr`, so seeding fires
  /// nothing; it just installs the post-restart baseline.
  fn seed_history_baselines_for(&mut self, c: &Cluster, observer: SocketAddr) {
    self.incarnation.observe_observer(c, observer);
    self.resurrection.observe_observer(c, observer);
    self.illegal_pair.observe_observer(c, observer);
  }

  fn check_safety(&mut self, c: &Cluster, seed: u64, tick: usize) {
    // The three HISTORY checkers consume the per-mutation transition log so an
    // illegal intermediate transition overwritten by a later same-step mutation
    // is still observed. The remaining checkers and the convergence oracle are
    // final-state invariants, so they stay on the post-step snapshot.
    let transitions = c.history_transitions();
    for r in [
      self.incarnation.observe_transitions(transitions),
      self.resurrection.observe_transitions(transitions),
      self.illegal_pair.observe_transitions(transitions),
      self.meta.observe(c),
      self.self_incarnation.observe(c),
      self.self_liveness.observe(c),
      self.boundedness.observe(c),
    ] {
      if let Some(reason) = r.reason() {
        dump_divergence(c);
        panic!("VOPR safety violation: seed {seed} tick {tick}: {reason}");
      }
    }
  }
}

/// Print every observer's view of every subject's (state, incarnation) — the
/// failure context printed just before a safety panic.
fn dump_divergence(c: &Cluster) {
  eprintln!("=== VOPR divergence dump ===");
  for &observer in c.addrs() {
    for subject in c.ids() {
      let st = c.member_liveness(observer, subject);
      let inc = c.get_node_incarnation(observer, subject);
      eprintln!("  {observer} sees {subject}: {st:?} @ {inc:?}");
    }
  }
}

fn masked(class: &str, v: u32) -> u32 {
  if masked_off(class) { 0 } else { v }
}

fn masked_off(class: &str) -> bool {
  std::env::var(format!("MEMBERLIST_VOPR_NO_{class}")).is_ok()
}

#[cfg(test)]
mod tests;
