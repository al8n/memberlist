use super::*;

fn sock(p: u16) -> SocketAddr {
  format!("127.0.0.1:{p}").parse().unwrap()
}

/// A 4-node driver with a known address list and no departures, for exercising
/// the partition-coverage helper in isolation.
fn driver() -> Vopr {
  let mut v = Vopr::new(0);
  v.n = 4;
  v.addrs = (0..4u16).map(|i| sock(40000 + i)).collect();
  v.ids = (0..4).map(|i| format_smolstr!("v{i}")).collect();
  v.gone.clear();
  v.left.clear();
  v
}

#[test]
fn effective_cut_records_when_both_sides_live() {
  let v = driver();
  // Node 0 alone on side A, nodes 1..=3 on side B: both sides hold live nodes.
  assert!(v.effective_live_cut_key(0b0001).is_some());
}

#[test]
fn effective_cut_is_none_when_a_same_tick_crash_strands_one_side() {
  let mut v = driver();
  // Node 0 is the only node on side A of this cut, so the cut is effective…
  assert!(v.effective_live_cut_key(0b0001).is_some());
  // …until a same-tick crash of node 0 (modeled by inserting it into `gone`
  // before the post-lifecycle recording) leaves side A with no live node. The
  // cut is now inert and must not be recorded — this is the ordering the
  // deferred recording in `apply_actions` guarantees.
  v.gone.insert(v.addrs[0]);
  assert!(v.effective_live_cut_key(0b0001).is_none());
  // A graceful leave strands a side the same way.
  let mut w = driver();
  w.left.insert(w.addrs[0]);
  assert!(w.effective_live_cut_key(0b0001).is_none());
}

#[test]
fn effective_cut_dedups_symmetric_masks() {
  let v = driver();
  // A mask and its complement describe the same undirected cut.
  let key = v.effective_live_cut_key(0b0011);
  assert_eq!(key, v.effective_live_cut_key(0b1100));
  assert!(key.is_some());
}

#[test]
fn effective_cut_distinguishes_different_live_sets() {
  let mut v = driver();
  let full = v.effective_live_cut_key(0b0001);
  // Removing a live node from side B changes the live set, hence the key, even
  // though node 0 is still the isolated side.
  v.gone.insert(v.addrs[3]);
  let fewer = v.effective_live_cut_key(0b0001);
  assert!(full.is_some() && fewer.is_some());
  assert_ne!(full, fewer);
}

/// A calm-restarted node that has already learned one peer (so the bare
/// isolation predicate would skip it) still receives its forced reseed join —
/// and if that join cannot merge, the node stays short of the cluster, so the
/// convergence check surfaces the wedge instead of organic gossip masking it.
#[test]
fn forced_reseed_of_connected_calm_restart_surfaces_a_nonmerging_join() {
  // A cluster of >= 4 so the restarted node, its injected peer, and a distinct
  // best-connected seed are all separate, and the full size exceeds the node's
  // {self, injected peer} count.
  let seed = (0u64..)
    .find(|&s| Vopr::new(s).n >= 4)
    .expect("a seed with n >= 4 exists");
  let mut v = Vopr::new(seed);
  let mut c = v.build_cluster();
  for _ in 0..400 {
    if !c.step() {
      break;
    }
  }
  let x = v.addrs[0];
  let y = v.addrs[1];
  let y_id = v.ids[1].clone();
  // Crash and restart X without a rejoin: it knows only itself.
  c.crash(x);
  assert!(c.restart_without_join(x));
  // X learns one live peer organically before the reseed -> "connected".
  c.inject_alive(x, y_id.clone(), y, 1);
  assert_eq!(c.member_liveness(x, &y_id), Some(State::Alive));
  // X rejects every merge and every gossiped alive, so neither the forced
  // reseed push-pull nor organic gossip can teach it the rest of the cluster:
  // a join that completes without merging leaves it short of the cluster.
  c.reject_merges(x);
  c.reject_alives(x);
  let joined = v.reseed_isolated(&mut c, &[x]);
  // The force must issue X exactly one reseed join even though it is connected;
  // this assertion fails if `calm_restarted` no longer overrides the isolation
  // predicate.
  assert!(
    joined.contains(&x),
    "a connected calm-restarted node must be force-joined"
  );
  for _ in 0..600 {
    if !c.step() {
      break;
    }
  }
  // The reseed join completed but did not merge, so X is still short of the
  // cluster — a wedge the convergence check would flag, not mask.
  assert!(
    c.num_members(x) < v.n,
    "a non-merging reseed must leave the node isolated"
  );
}

/// Every calm-restarted node receives exactly one join — including one selected
/// as the seed, which is cross-joined to another live node rather than exempted.
#[test]
fn every_calm_restart_is_joined_including_the_seed() {
  let seed = (0u64..)
    .find(|&s| Vopr::new(s).n >= 3)
    .expect("a seed with n >= 3 exists");
  let mut v = Vopr::new(seed);
  let mut c = v.build_cluster();
  for _ in 0..400 {
    if !c.step() {
      break;
    }
  }
  // Treat every live node as calm-restarted: the chosen seed is itself
  // calm-restarted, so it must be cross-joined, not exempted.
  let all: Vec<SocketAddr> = v.addrs.clone();
  let joined = v.reseed_isolated(&mut c, &all);
  for &addr in &all {
    assert!(
      joined.contains(&addr),
      "every calm-restarted node, including the seed, must be joined"
    );
  }
}

/// `enter_calm` reports the nodes it restarted (without a rejoin), and the
/// single forced reseed converges such a node.
#[test]
fn enter_calm_reports_restarts_and_reseed_converges_them() {
  let mut v = Vopr::new(7);
  let mut c = v.build_cluster();
  for _ in 0..400 {
    if !c.step() {
      break;
    }
  }
  let x = v.addrs[0];
  assert_eq!(c.num_members(x), v.n);
  c.crash(x);
  v.gone.insert(x);
  let restarted = v.enter_calm(&mut c);
  assert!(
    restarted.contains(&x),
    "enter_calm must report the node it restarted"
  );
  assert_eq!(
    c.num_members(x),
    1,
    "a restart-without-join node knows only itself"
  );
  v.reseed_isolated(&mut c, &restarted);
  for _ in 0..600 {
    if !c.step() {
      break;
    }
  }
  assert_eq!(
    c.num_members(x),
    v.n,
    "the forced reseed converges the restarted node"
  );
}
