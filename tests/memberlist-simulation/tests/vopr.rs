//! The VOPR seed sweep.
use std::collections::BTreeSet;

use memberlist_simulation::vopr::{run_vopr, run_vopr_one};

#[derive(Debug, Default)]
struct Totals {
  drops: u64,
  dups: u64,
  partitions: u64,
  crashes: u64,
  restarts: u64,
  leaves: u64,
  suspect: u64,
  dead: u64,
  left: u64,
  calm_ticks: u64,
  seeds_with_calm_window: u64,
  /// Union across the sweep of every distinct partition topology exercised.
  partition_topologies: BTreeSet<u32>,
}

fn run_campaign(seeds: std::ops::Range<u64>, ticks: usize) -> Totals {
  let mut t = Totals::default();
  for seed in seeds {
    let r = run_vopr(seed, ticks);
    t.drops += r.total_drops;
    t.dups += r.total_duplicates;
    t.partitions += r.total_partitions;
    t.crashes += r.total_crashes;
    t.restarts += r.total_restarts;
    t.leaves += r.total_leaves;
    t.suspect += r.total_suspect_samples;
    t.dead += r.total_dead_samples;
    t.left += r.total_left_samples;
    t.calm_ticks += r.calm_ticks;
    if r.calm_ticks > 0 {
      t.seeds_with_calm_window += 1;
    }
    t.partition_topologies.extend(r.partition_topologies.iter().copied());
  }
  t
}

#[test]
fn single_seed_runs_clean() {
  let report = run_vopr(1, 3000);
  assert!(report.ticks == 3000);
  // At least some chaos actually happened (non-vacuity).
  assert!(report.total_drops + report.total_duplicates + report.total_partitions > 0,
    "the run must actually inject faults: {report:?}");
}

#[test]
fn replay_is_deterministic() {
  let a = run_vopr(7, 1500);
  let b = run_vopr_one(7);
  assert_eq!(a.total_drops, b.total_drops);
}

/// Routine safety + liveness sweep. A fast subset for `cargo test`; the
/// exhaustive run is `full_campaign` (ignored by default).
#[test]
fn safety_seed_sweep() {
  // 3000 ticks/seed is the convergence floor for the 0..64 band: a Leaving/Left
  // node is gossip-inert, so the chaos band needs this much headroom for the
  // quiesce phase to settle every seed (the full campaign uses 5000).
  let totals = run_campaign(0..64, 3000);
  assert!(totals.drops > 0 && totals.dups > 0 && totals.partitions > 0,
    "network faults must fire across the sweep: {totals:?}");
  assert!(totals.partition_topologies.len() > 1,
    "partitions must exercise more than one distinct topology: {totals:?}");
  assert!(totals.crashes > 0, "the sweep must crash nodes: {totals:?}");
  assert!(totals.restarts > 0, "the sweep must restart crashed nodes: {totals:?}");
  assert!(totals.leaves > 0, "the sweep must gracefully leave nodes: {totals:?}");
  assert!(totals.suspect > 0, "a live peer must observe Suspect: {totals:?}");
  assert!(totals.dead > 0, "a live peer must observe Dead: {totals:?}");
  assert!(totals.left > 0, "a live peer must observe Left: {totals:?}");
  assert!(totals.calm_ticks > 0, "the calm/quiesce phase must run: {totals:?}");
}

/// The exhaustive campaign: a contiguous band of seeds, each run for many chaos
/// ticks followed by a quiesce-to-convergence. Heavy (minutes) — run explicitly
/// with `--ignored`, ideally a release build:
///
/// ```text
/// cargo test --release -p memberlist-simulation --test vopr full_campaign -- --ignored
/// ```
///
/// The band and tick budget default to `0..4096` × 5000 and are overridable via
/// the `VOPR_SEED_START` / `VOPR_SEED_END` / `VOPR_TICKS` environment variables,
/// so CI can rotate to a fresh band each run.
#[test]
#[ignore = "exhaustive multi-minute campaign; run explicitly (release recommended)"]
fn full_campaign() {
  let start = env_u64("VOPR_SEED_START", 0);
  let end = env_u64("VOPR_SEED_END", 4096);
  let ticks = env_usize("VOPR_TICKS", 5000);
  let totals = run_campaign(start..end, ticks);
  assert!(totals.drops > 0 && totals.dups > 0 && totals.partitions > 0, "{totals:?}");
  assert!(totals.partition_topologies.len() > 1,
    "partitions must exercise more than one distinct topology: {totals:?}");
  assert!(totals.crashes > 0 && totals.restarts > 0, "crash/restart must fire: {totals:?}");
  assert!(totals.leaves > 0, "leaves must fire: {totals:?}");
  assert!(totals.suspect > 0 && totals.dead > 0 && totals.left > 0,
    "a live peer must observe Suspect, Dead, and Left: {totals:?}");
  assert!(totals.seeds_with_calm_window > 0, "calm windows must run: {totals:?}");
}

fn env_u64(key: &str, default: u64) -> u64 {
  std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
  std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[test]
#[ignore = "single-seed replay for debugging a reported failure"]
fn replay() {
  let seed: u64 = std::env::var("VOPR_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
  run_vopr(seed, 5000);
}
