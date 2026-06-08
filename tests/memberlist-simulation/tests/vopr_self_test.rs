//! The mutation self-test: every checker must catch a deliberately-broken input,
//! proving the inject -> observe -> detect loop bites.
use memberlist_simulation::checker::{CheckResult, IncarnationMonotonicChecker};
use smol_str::SmolStr;
use std::net::SocketAddr;

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

#[test]
fn checkresult_distinguishes_ok_from_violation() {
  assert!(CheckResult::Ok.is_ok());
  let v = CheckResult::violation("incarnation regressed");
  assert!(v.is_violation());
  assert_eq!(v.reason(), Some("incarnation regressed"));
}

#[test]
fn inline_monotonicity_checker_catches_a_regression() {
  // A trivial stand-in for the real incarnation-monotonicity checker: it must flag a decrease.
  fn check_seq(incs: &[u32]) -> CheckResult {
    let mut max = 0u32;
    for &i in incs {
      if i < max { return CheckResult::violation("incarnation regressed"); }
      max = max.max(i);
    }
    CheckResult::Ok
  }
  assert!(check_seq(&[1, 2, 3, 5]).is_ok());
  assert!(check_seq(&[1, 5, 3]).is_violation(), "the checker must bite on a regression");
}

#[test]
fn incarnation_monotonic_checker_bites_on_regression() {
  use memberlist_proto::typed::State;
  let mut ck = IncarnationMonotonicChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  // Continuously-Alive entry (no prune): 5 -> 3 is a real regression.
  assert!(ck.observe_one(obs, &sub, Some(5), Some(State::Alive), false).is_ok());
  assert!(ck.observe_one(obs, &sub, Some(3), Some(State::Alive), false).is_violation(),
    "5 -> 3 within a live entry must fire");
}

#[test]
fn incarnation_monotonic_checker_allows_prune_rejoin() {
  use memberlist_proto::typed::State;
  let mut ck = IncarnationMonotonicChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  // Dead@5, then a prune-and-re-learn (pruned=true) at a lower incarnation ->
  // legal: the actual prune reset the baseline.
  assert!(ck.observe_one(obs, &sub, Some(5), Some(State::Dead), false).is_ok());
  assert!(ck.observe_one(obs, &sub, Some(1), Some(State::Alive), true).is_ok(),
    "a post-prune re-learn at a lower incarnation is legal");
  // But Dead@5 -> Alive@1 WITHOUT a prune is a real regression (an in-place
  // resurrection the machine never pruned).
  let mut ck2 = IncarnationMonotonicChecker::new();
  assert!(ck2.observe_one(obs, &sub, Some(5), Some(State::Dead), false).is_ok());
  assert!(ck2.observe_one(obs, &sub, Some(1), Some(State::Alive), false).is_violation(),
    "absent a prune a 5 -> 1 drop is a real regression");
}

#[test]
fn no_resurrection_checker_bites_without_prune() {
  use memberlist_simulation::checker::NoResurrectionChecker;
  use memberlist_proto::typed::State;
  let mut ck = NoResurrectionChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  // Dead@5 then Alive@5 with NO prune is an in-place resurrection — the gap the
  // time heuristic would have missed once gossip_to_the_dead_time elapsed.
  assert!(ck.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  assert!(ck.observe_one(obs, &sub, Some(State::Alive), Some(5), false).is_violation(),
    "Dead@5 -> Alive@5 without a prune is a real resurrection");
}

#[test]
fn no_resurrection_checker_allows_realive_after_prune() {
  use memberlist_simulation::checker::NoResurrectionChecker;
  use memberlist_proto::typed::State;
  let mut ck = NoResurrectionChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  // Higher-incarnation refute is always legal, prune or not.
  assert!(ck.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  assert!(ck.observe_one(obs, &sub, Some(State::Alive), Some(6), false).is_ok());
  // Equal-incarnation re-alive ACCOMPANYING a prune is a legal post-prune re-join.
  let mut ck2 = NoResurrectionChecker::new();
  assert!(ck2.observe_one(obs, &sub, Some(State::Left), Some(1), false).is_ok());
  assert!(ck2.observe_one(obs, &sub, Some(State::Alive), Some(1), true).is_ok(),
    "Left@1 -> Alive@1 on the prune step is a legal re-introduction");
}

#[test]
fn meta_per_incarnation_checker_bites_on_divergent_meta() {
  use memberlist_simulation::checker::MetaPerIncarnationChecker;
  let mut ck = MetaPerIncarnationChecker::new();
  let sub: SmolStr = "b".into();
  assert!(ck.observe_one(&sub, 3, (vec![1, 2, 3], 0, 0)).is_ok());
  assert!(ck.observe_one(&sub, 3, (vec![9, 9, 9], 0, 0)).is_violation(),
    "two different metas at the same incarnation must fire");
}

#[test]
fn meta_per_incarnation_checker_allows_same_tuple_and_new_incarnation() {
  use memberlist_simulation::checker::MetaPerIncarnationChecker;
  let mut ck = MetaPerIncarnationChecker::new();
  let sub: SmolStr = "b".into();
  assert!(ck.observe_one(&sub, 3, (vec![1, 2, 3], 0, 0)).is_ok());
  assert!(ck.observe_one(&sub, 3, (vec![1, 2, 3], 0, 0)).is_ok()); // same tuple, same inc
  assert!(ck.observe_one(&sub, 4, (vec![9, 9, 9], 1, 1)).is_ok()); // new incarnation
}

#[test]
fn self_incarnation_checker_bites_on_decrease() {
  use memberlist_simulation::checker::SelfIncarnationChecker;
  let mut ck = SelfIncarnationChecker::new();
  let h = addr(1);
  assert!(ck.observe_one(h, 5).is_ok());
  assert!(ck.observe_one(h, 3).is_violation(), "self-incarnation 5->3 must fire");
}

#[test]
fn self_incarnation_checker_allows_nondecrease() {
  use memberlist_simulation::checker::SelfIncarnationChecker;
  let mut ck = SelfIncarnationChecker::new();
  let h = addr(1);
  assert!(ck.observe_one(h, 5).is_ok());
  assert!(ck.observe_one(h, 5).is_ok());
  assert!(ck.observe_one(h, 7).is_ok());
}

#[test]
fn self_liveness_checker_bites_on_self_dead_while_running() {
  use memberlist_simulation::checker::SelfLivenessChecker;
  use memberlist_proto::typed::State;
  let mut ck = SelfLivenessChecker::new();
  let h = addr(1);
  assert!(ck.observe_one(h, true, Some(State::Alive)).is_ok());
  assert!(ck.observe_one(h, true, Some(State::Dead)).is_violation(),
    "a running node seeing itself Dead must fire");
}

#[test]
fn self_liveness_checker_allows_self_left_when_not_running() {
  use memberlist_simulation::checker::SelfLivenessChecker;
  use memberlist_proto::typed::State;
  let mut ck = SelfLivenessChecker::new();
  let h = addr(1);
  assert!(ck.observe_one(h, false, Some(State::Left)).is_ok(),
    "Left after a graceful leave (not running) is correct");
}

#[test]
fn illegal_pair_checker_bites_on_dead_to_suspect_same_incarnation() {
  use memberlist_simulation::checker::IllegalPairChecker;
  use memberlist_proto::typed::State;
  let mut ck = IllegalPairChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  assert!(ck.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  assert!(ck.observe_one(obs, &sub, Some(State::Suspect), Some(5), false).is_violation(),
    "Dead@5 -> Suspect@5 without a prune is illegal");
}

#[test]
fn illegal_pair_checker_allows_normal_and_post_prune_transitions() {
  use memberlist_simulation::checker::IllegalPairChecker;
  use memberlist_proto::typed::State;
  let mut ck = IllegalPairChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  assert!(ck.observe_one(obs, &sub, Some(State::Alive), Some(5), false).is_ok());
  assert!(ck.observe_one(obs, &sub, Some(State::Suspect), Some(5), false).is_ok());
  assert!(ck.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  // Left@1 -> Alive@1 ON the prune step is a legal re-introduction, not illegal.
  let mut ck2 = IllegalPairChecker::new();
  assert!(ck2.observe_one(obs, &sub, Some(State::Left), Some(1), false).is_ok());
  assert!(ck2.observe_one(obs, &sub, Some(State::Alive), Some(1), true).is_ok(),
    "post-prune Left@1 -> Alive@1 is legal");
}

#[test]
fn boundedness_checker_bites_on_excess_members() {
  use memberlist_simulation::checker::BoundednessChecker;
  let mut ck = BoundednessChecker::new(4);
  let h = addr(1);
  assert!(ck.observe_one(h, 3, 5, 0).is_ok());        // 3 known <= 5 introduced
  assert!(ck.observe_one(h, 6, 5, 0).is_violation(),
    "knowing 6 members when only 5 were introduced must fire");
}

#[test]
fn boundedness_checker_allows_normal_counts_and_queue() {
  use memberlist_simulation::checker::BoundednessChecker;
  let mut ck = BoundednessChecker::new(4);
  let h = addr(1);
  assert!(ck.observe_one(h, 5, 5, 10).is_ok());        // full membership, small queue
}

/// Two live observers holding a third node Alive at DIFFERENT incarnations
/// must be flagged as not converged. This verifies that the ConvergenceChecker
/// actually exercises its per-subject incarnation-agreement path — the path
/// that catches the reachable-liveness wedge where a gracefully-left node
/// keeps ACKing probes (holding it Alive at the probing peer) while the rest
/// of the cluster has advanced its incarnation.
#[test]
fn convergence_checker_bites_on_incarnation_disagreement() {
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use std::collections::HashSet;
  let a0 = addr(35000); let a1 = addr(35001); let a2 = addr(35002);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node("n1".into(), a1);
  c.add_node("n2".into(), a2);
  // a0 sees n2 as Alive@1; a1 sees n2 as Alive@2 (higher incarnation after a
  // refute). No steps between the two injects so they never reconcile.
  c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n2".into(), a2, 2);
  // Also introduce n0/n1 at each observer so the live set is full.
  c.inject_alive(a0, "n1".into(), a1, 1);
  c.inject_alive(a1, "n0".into(), a0, 1);
  c.inject_alive(a2, "n0".into(), a0, 1);
  c.inject_alive(a2, "n1".into(), a1, 1);
  // No steps: incarnation split is intentionally frozen.
  let conv = ConvergenceChecker::new();
  assert!(
    conv.check(&c, &HashSet::new()).is_violation(),
    "observers disagreeing on n2 incarnation (1 vs 2) must be flagged as not converged"
  );
}

#[test]
fn convergence_checker_passes_on_settled_cluster() {
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use std::collections::HashSet;
  let a0 = addr(34000); let a1 = addr(34001); let a2 = addr(34002);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0); c.add_node("n1".into(), a1); c.add_node("n2".into(), a2);
  c.inject_alive(a0, "n1".into(), a1, 1); c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1); c.inject_alive(a1, "n2".into(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1); c.inject_alive(a2, "n1".into(), a1, 1);
  for _ in 0..400 { c.step(); }
  let conv = ConvergenceChecker::new();
  assert!(conv.check(&c, &HashSet::new()).is_ok(), "a settled all-alive mesh has converged");
}

#[test]
fn convergence_checker_bites_on_down_live_node() {
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use std::collections::HashSet;
  let a0 = addr(34100); let a1 = addr(34101); let a2 = addr(34102);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0); c.add_node("n1".into(), a1); c.add_node("n2".into(), a2);
  c.inject_alive(a0, "n1".into(), a1, 1); c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1); c.inject_alive(a1, "n2".into(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1); c.inject_alive(a2, "n1".into(), a1, 1);
  for _ in 0..400 { c.step(); }
  // Crash n2 but treat it as live (not left): peers will see it Dead → not converged.
  c.crash(a2);
  for _ in 0..4000 { c.step(); }
  let conv = ConvergenceChecker::new();
  assert!(conv.check(&c, &HashSet::new()).is_violation(),
    "a down node that is still in the live set means the cluster has not converged");
}

#[test]
fn convergence_checker_allows_left_node_as_terminal() {
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use std::collections::HashSet;
  let a0 = addr(34200); let a1 = addr(34201); let a2 = addr(34202);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0); c.add_node("n1".into(), a1); c.add_node("n2".into(), a2);
  c.inject_alive(a0, "n1".into(), a1, 1); c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1); c.inject_alive(a1, "n2".into(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1); c.inject_alive(a2, "n1".into(), a1, 1);
  for _ in 0..400 { c.step(); }
  c.leave(a2).expect("n2 is running and can leave");
  for _ in 0..2000 { c.step(); }
  let mut left = HashSet::new(); left.insert(a2);
  let conv = ConvergenceChecker::new();
  assert!(conv.check(&c, &left).is_ok(),
    "a departed node seen Left/absent everywhere is converged, not a violation");
}

#[test]
fn convergence_checker_bites_on_departed_node_seen_alive() {
  // A gracefully-left node seen as Alive at a live observer is a real violation:
  // it means the leave gossip was reverted or the node re-joined without the
  // harness registering it.
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use std::collections::HashSet;
  let a0 = addr(34300); let a1 = addr(34301); let a2 = addr(34302);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0); c.add_node("n1".into(), a1); c.add_node("n2".into(), a2);
  c.inject_alive(a0, "n1".into(), a1, 1); c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1); c.inject_alive(a1, "n2".into(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1); c.inject_alive(a2, "n1".into(), a1, 1);
  for _ in 0..400 { c.step(); }
  // Declare n2 as left in the oracle but don't actually leave or crash it;
  // the cluster still sees it as Alive everywhere.
  let mut left = HashSet::new(); left.insert(a2);
  let conv = ConvergenceChecker::new();
  assert!(conv.check(&c, &left).is_violation(),
    "a departed node seen Alive at observers must fire as not converged");
}

#[test]
fn checker_clear_observer_resets_baselines_for_restarted_node() {
  // When an observer node restarts, all checker baselines for it as an
  // observer must be cleared. A restarted node has a fresh membership view,
  // so stale Dead/Left baselines from the old incarnation must not fire.
  use memberlist_simulation::checker::{IllegalPairChecker, IncarnationMonotonicChecker, NoResurrectionChecker};
  use memberlist_proto::typed::State;
  let obs = addr(1); let sub: SmolStr = "b".into();

  // IncarnationMonotonic: after clear_observer the restarted observer can
  // re-learn at any incarnation, with no prune signal.
  let mut s1 = IncarnationMonotonicChecker::new();
  assert!(s1.observe_one(obs, &sub, Some(5), Some(State::Dead), false).is_ok());
  s1.clear_observer(obs);
  assert!(s1.observe_one(obs, &sub, Some(1), Some(State::Alive), false).is_ok(),
    "post-restart observer should accept any incarnation after clear_observer");

  // NoResurrection: after clear_observer a stale Dead baseline no longer blocks an equal-inc Alive.
  let mut s2 = NoResurrectionChecker::new();
  assert!(s2.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  s2.clear_observer(obs);
  assert!(s2.observe_one(obs, &sub, Some(State::Alive), Some(5), false).is_ok(),
    "post-restart observer should not fire on a stale Dead baseline");

  // IllegalPair: after clear_observer a stale Dead baseline no longer blocks re-suspected.
  let mut s6 = IllegalPairChecker::new();
  assert!(s6.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  s6.clear_observer(obs);
  assert!(s6.observe_one(obs, &sub, Some(State::Suspect), Some(5), false).is_ok(),
    "post-restart observer should not fire on a stale Dead baseline");
}

/// A terminal baseline that DISAPPEARS without an actual prune must be retained,
/// so a subsequent equal-incarnation Alive is still caught as a resurrection. If
/// the machine drops a Dead/Left entry without `reset_nodes` pruning it (a bug)
/// and re-accepts Alive@<=I, clearing the baseline on the intervening `None`
/// would mask the resurrection — this verifies it does not.
#[test]
fn no_resurrection_checker_retains_baseline_through_unpruned_absence() {
  use memberlist_simulation::checker::NoResurrectionChecker;
  use memberlist_proto::typed::State;
  let obs = addr(1); let sub: SmolStr = "b".into();

  // ILLEGITIMATE: Dead@5, then an absence with pruned=false, then Alive@5
  // (pruned=false). The baseline must be retained through the unpruned absence,
  // so the equal-incarnation re-alive fires.
  let mut ck = NoResurrectionChecker::new();
  assert!(ck.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  assert!(ck.observe_one(obs, &sub, None, None, false).is_ok(),
    "the unpruned disappearance itself is not a violation");
  assert!(ck.observe_one(obs, &sub, Some(State::Alive), Some(5), false).is_violation(),
    "an unpruned drop-then-resurrect must still fire: the baseline is retained");

  // LEGITIMATE: Dead@5, then an absence with pruned=true, then Alive@5
  // (pruned=false). The actual prune cleared the baseline, so the fresh re-join
  // is Ok (no false positive).
  let mut ck2 = NoResurrectionChecker::new();
  assert!(ck2.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  assert!(ck2.observe_one(obs, &sub, None, None, true).is_ok(),
    "a prune clears the baseline");
  assert!(ck2.observe_one(obs, &sub, Some(State::Alive), Some(5), false).is_ok(),
    "a post-prune equal-incarnation re-join is a legal re-introduction, not a resurrection");
}

/// The convergence oracle must verify the ID→address binding, not just that
/// observers agree a node is Alive at an agreed incarnation. A cluster that
/// agrees `n1` is Alive@1 but routes it to a WRONG address has not converged.
#[test]
fn convergence_checker_bites_on_wrong_address_binding() {
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use memberlist_proto::typed::Dead;
  use std::collections::HashSet;
  let a0 = addr(34400); let a1 = addr(34401); let a2 = addr(34402);
  let a_wrong = addr(34999); // not a registered node address
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0); c.add_node("n1".into(), a1); c.add_node("n2".into(), a2);
  c.inject_alive(a0, "n1".into(), a1, 1); c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1); c.inject_alive(a1, "n2".into(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1); c.inject_alive(a2, "n1".into(), a1, 1);
  for _ in 0..400 { c.step(); }

  // Drive a0's view of n1 to State::Left via a self-marked Dead (node == from),
  // at the same incarnation the cluster holds (1). A Left entry is immediately
  // address-reclaimable, so the next Alive at a wrong address is ADOPTED rather
  // than rejected as a NodeConflict.
  c.dead_node(a0, Dead::new(1, "n1".into(), "n1".into()));
  assert_eq!(c.member_liveness(a0, &"n1".into()), Some(memberlist_proto::typed::State::Left),
    "the self-marked Dead must drive n1 to Left at a0");
  // Inject Alive@1 for n1 at the WRONG address. Adopting the address skips the
  // incarnation gate, so the applied incarnation stays 1 — every observer still
  // agrees n1 is Alive@1, isolating the address-binding check from the
  // incarnation-agreement check.
  c.inject_alive(a0, "n1".into(), a_wrong, 1);
  assert_eq!(c.member_liveness(a0, &"n1".into()), Some(memberlist_proto::typed::State::Alive),
    "the adopting Alive must re-alive n1 at a0");
  assert_eq!(
    c.member(a0, &"n1".into()).map(|ns| *ns.address_ref()),
    Some(a_wrong),
    "a0 must now bind n1 to the wrong address (precondition for the check to bite)"
  );

  let conv = ConvergenceChecker::new();
  let r = conv.check(&c, &HashSet::new());
  assert!(r.is_violation(),
    "an observer binding n1 to the wrong address must fire as not converged: {r:?}");
  assert!(r.reason().is_some_and(|s| s.contains("binds") && s.contains("n1")),
    "the violation must name the wrong-address binding: {r:?}");
}

/// A non-terminal (Alive/Suspect) baseline must be retained through a
/// disappearance. The machine never prunes a non-terminal entry, so the absence
/// is illegitimate (pruned=false) and the baseline must survive so a later
/// incarnation regression still fires.
#[test]
fn incarnation_monotonic_retains_non_terminal_baseline_through_absence() {
  use memberlist_simulation::checker::IncarnationMonotonicChecker;
  use memberlist_proto::typed::State;
  let mut ck = IncarnationMonotonicChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  // Step 1: observe Alive@5 — establishes the baseline.
  assert!(ck.observe_one(obs, &sub, Some(5), Some(State::Alive), false).is_ok());
  // Step 2: non-terminal disappearance with pruned=false — must be silently retained.
  assert!(ck.observe_one(obs, &sub, None, None, false).is_ok(),
    "the disappearance itself is not a violation");
  // Step 3: Alive@3 — the retained baseline catches the 5->3 regression.
  assert!(ck.observe_one(obs, &sub, Some(3), Some(State::Alive), false).is_violation(),
    "unpruned disappearance must retain baseline; Alive@3 after Alive@5 is a regression");
}

/// A terminal (Dead) baseline the machine actually prunes (pruned=true on the
/// absence) is cleared, and the subsequent lower-incarnation re-join must be
/// accepted as a legal re-introduction (no false positive).
#[test]
fn incarnation_monotonic_clears_terminal_baseline_on_prune() {
  use memberlist_simulation::checker::IncarnationMonotonicChecker;
  use memberlist_proto::typed::State;
  let mut ck = IncarnationMonotonicChecker::new();
  let obs = addr(1); let sub: SmolStr = "b".into();
  // Step 1: observe Dead@5 — establishes a terminal baseline.
  assert!(ck.observe_one(obs, &sub, Some(5), Some(State::Dead), false).is_ok());
  // Step 2: absence with pruned=true — the machine pruned it, baseline cleared.
  assert!(ck.observe_one(obs, &sub, None, None, true).is_ok(),
    "a prune is not a violation");
  // Step 3: Alive@3 — baseline was cleared, legal re-join at any incarnation.
  assert!(ck.observe_one(obs, &sub, Some(3), Some(State::Alive), false).is_ok(),
    "the prune cleared the baseline; fresh re-join at lower incarnation is legal");
}

/// The wrong-address check must NOT fire on a settled cluster where every
/// observer binds each ID to its registered address (no false positive).
#[test]
fn convergence_checker_passes_address_binding_on_settled_cluster() {
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use std::collections::HashSet;
  let a0 = addr(34500); let a1 = addr(34501); let a2 = addr(34502);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0); c.add_node("n1".into(), a1); c.add_node("n2".into(), a2);
  c.inject_alive(a0, "n1".into(), a1, 1); c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1); c.inject_alive(a1, "n2".into(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1); c.inject_alive(a2, "n1".into(), a1, 1);
  for _ in 0..400 { c.step(); }
  let conv = ConvergenceChecker::new();
  assert!(conv.check(&c, &HashSet::new()).is_ok(),
    "a settled mesh with correct ID→address bindings has converged");
}

/// The oracle must reject a node that is darkened (crashed, not restarted) even
/// while peers still hold a stale Alive row for it — convergence is harness
/// truth, not a popularity vote among stale membership views.
#[test]
fn convergence_rejects_a_crashed_node_still_seen_alive() {
  use memberlist_simulation::{Cluster, checker::ConvergenceChecker};
  use std::collections::HashSet;
  let a0 = addr(34600); let a1 = addr(34601); let a2 = addr(34602);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0); c.add_node("n1".into(), a1); c.add_node("n2".into(), a2);
  c.inject_alive(a0, "n1".into(), a1, 1); c.inject_alive(a0, "n2".into(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1); c.inject_alive(a1, "n2".into(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1); c.inject_alive(a2, "n1".into(), a1, 1);
  for _ in 0..200 { c.step(); }
  let conv = ConvergenceChecker::new();
  assert!(conv.check(&c, &HashSet::new()).is_ok(), "an all-up mesh has converged");

  // Crash n2 but do NOT step: peers still hold n2 Alive@1 (a stale view). The
  // oracle must reject convergence on harness truth, not the stale Alive rows.
  c.crash(a2);
  let r = conv.check(&c, &HashSet::new());
  assert!(r.is_violation(),
    "a crashed node still seen Alive by peers must fail convergence immediately: {r:?}");
  assert!(r.reason().is_some_and(|s| s.contains("crashed") && s.contains("n2")),
    "the violation must name the crashed node: {r:?}");
}

