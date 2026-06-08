//! Same-step history masking: an (observer, subject) row that passes through an
//! illegal `(state, incarnation)` intermediate and is then overwritten by a
//! LEGAL value within the SAME step must still be caught. The fix samples the
//! three history checkers (incarnation-monotonic, no-resurrection, illegal-pair)
//! at the per-mutation boundary via the per-step transition log, not the
//! post-step snapshot, so the illegal intermediate is observed rather than
//! masked by the final value.
//!
//! The frozen machine is correct and will not actually perform an in-place
//! illegal transition, so a real cluster cannot be coaxed into producing the
//! masked sequence. These tests therefore feed the transition SEQUENCE directly
//! to each checker via `observe_transitions` — exactly the ordered values the
//! harness records for a compound datagram (or two same-deadline datagrams) —
//! and assert, per checker, that the masked illegal intermediate FIRES through
//! `observe_transitions` while the post-step-equivalent (the final value alone)
//! does NOT, proving the masking was a real false-negative that per-mutation
//! sampling closes. The end-to-end proof that the recorder actually surfaces a
//! compound-internal intermediate lives in the `network.rs`
//! `recorder_captures_compound_internal_intermediate` unit test.

use memberlist_proto::typed::State;
use memberlist_simulation::checker::{
  IllegalPairChecker, IncarnationMonotonicChecker, NoResurrectionChecker, Transition,
};
use smol_str::SmolStr;
use std::net::SocketAddr;

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

/// `Dead@5 -> Alive@5 (illegal in-place resurrection) -> Alive@6 (legal refute)`
/// within one step. The post-step snapshot only sees the legal `Alive@6`; the
/// transition log surfaces the masked `Alive@5`, which the no-resurrection
/// checker must catch.
#[test]
fn no_resurrection_catches_masked_intermediate() {
  let obs = addr(1);
  let sub: SmolStr = "s".into();

  // Control: post-step-equivalent — only the final legal value is observed.
  // A Dead@5 -> Alive@6 refute is legal, so the post-step path does NOT fire.
  // This is the false-negative the old once-per-step snapshot suffered.
  let mut control = NoResurrectionChecker::new();
  assert!(control.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok());
  assert!(
    control.observe_one(obs, &sub, Some(State::Alive), Some(6), false).is_ok(),
    "post-step Dead@5 -> Alive@6 is a legal refute: the masking hides the illegal intermediate"
  );

  // Per-mutation: the transition log carries the masked Alive@5 before Alive@6.
  let mut ck = NoResurrectionChecker::new();
  assert!(ck.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok(), "baseline Dead@5");
  let log = [
    Transition::new(obs, sub.clone(), Some(State::Alive), Some(5), false),
    Transition::new(obs, sub.clone(), Some(State::Alive), Some(6), false),
  ];
  let r = ck.observe_transitions(&log);
  assert!(
    r.is_violation(),
    "the masked Dead@5 -> Alive@5 intermediate must fire via the transition log: {r:?}"
  );
  assert!(
    r.reason().is_some_and(|s| s.contains("resurrection")),
    "the violation must name the resurrection: {r:?}"
  );
}

/// `Alive@5 -> Alive@3 (illegal incarnation regression) -> Alive@7 (legal bump)`
/// within one step. The post-step snapshot only sees `Alive@7` (>= 5, legal);
/// the transition log surfaces the masked `Alive@3` regression.
#[test]
fn incarnation_monotonic_catches_masked_regression() {
  let obs = addr(1);
  let sub: SmolStr = "s".into();

  // Control: Alive@5 -> Alive@7 is monotone, so the post-step path is silent.
  let mut control = IncarnationMonotonicChecker::new();
  assert!(control.observe_one(obs, &sub, Some(5), Some(State::Alive), false).is_ok());
  assert!(
    control.observe_one(obs, &sub, Some(7), Some(State::Alive), false).is_ok(),
    "post-step Alive@5 -> Alive@7 is monotone: the masking hides the intermediate regression"
  );

  // Per-mutation: the transition log carries the masked Alive@3 before Alive@7.
  let mut ck = IncarnationMonotonicChecker::new();
  assert!(ck.observe_one(obs, &sub, Some(5), Some(State::Alive), false).is_ok(), "baseline Alive@5");
  let log = [
    Transition::new(obs, sub.clone(), Some(State::Alive), Some(3), false),
    Transition::new(obs, sub.clone(), Some(State::Alive), Some(7), false),
  ];
  let r = ck.observe_transitions(&log);
  assert!(
    r.is_violation(),
    "the masked Alive@5 -> Alive@3 regression must fire via the transition log: {r:?}"
  );
  assert!(
    r.reason().is_some_and(|s| s.contains("regress")),
    "the violation must name the regression: {r:?}"
  );
}

/// `Left@5 -> Suspect@5 (illegal: a departed node is not suspected) -> Alive@6
/// (legal re-introduction at a higher incarnation)` within one step. The
/// post-step snapshot only sees `Alive@6` (Left@5 -> Alive@6 is legal); the
/// transition log surfaces the masked illegal `Left -> Suspect` pair.
#[test]
fn illegal_pair_catches_masked_intermediate() {
  let obs = addr(1);
  let sub: SmolStr = "s".into();

  // Control: Left@5 -> Alive@6 (higher incarnation) is a legal re-introduction,
  // so the post-step path is silent — the masking hides the Left -> Suspect.
  let mut control = IllegalPairChecker::new();
  assert!(control.observe_one(obs, &sub, Some(State::Left), Some(5), false).is_ok());
  assert!(
    control.observe_one(obs, &sub, Some(State::Alive), Some(6), false).is_ok(),
    "post-step Left@5 -> Alive@6 is a legal re-introduction: the masking hides the bad pair"
  );

  // Per-mutation: the transition log carries the masked Suspect@5 before Alive@6.
  let mut ck = IllegalPairChecker::new();
  assert!(ck.observe_one(obs, &sub, Some(State::Left), Some(5), false).is_ok(), "baseline Left@5");
  let log = [
    Transition::new(obs, sub.clone(), Some(State::Suspect), Some(5), false),
    Transition::new(obs, sub.clone(), Some(State::Alive), Some(6), false),
  ];
  let r = ck.observe_transitions(&log);
  assert!(
    r.is_violation(),
    "the masked Left@5 -> Suspect@5 pair must fire via the transition log: {r:?}"
  );
  assert!(
    r.reason().is_some_and(|s| s.contains("illegal state pair")),
    "the violation must name the illegal pair: {r:?}"
  );
}

/// A legal compound-internal sequence must NOT fire: an empty change set and a
/// monotone `Dead@5 -> (pruned) absent -> Alive@1` post-prune re-learn are both
/// legal, so `observe_transitions` stays silent (no false positive from the new
/// per-mutation path).
#[test]
fn observe_transitions_no_false_positive_on_legal_sequence() {
  let obs = addr(1);
  let sub: SmolStr = "s".into();

  // No-resurrection: a prune clears the baseline, so a post-prune lower-inc
  // re-learn is a legal re-introduction even though it would otherwise look like
  // a resurrection.
  let mut ck = NoResurrectionChecker::new();
  assert!(ck.observe_one(obs, &sub, Some(State::Dead), Some(5), false).is_ok(), "baseline Dead@5");
  let log = [
    Transition::new(obs, sub.clone(), None, None, true), // an actual prune
    Transition::new(obs, sub.clone(), Some(State::Alive), Some(1), false), // legal re-join
  ];
  assert!(
    ck.observe_transitions(&log).is_ok(),
    "a pruned-then-re-learned sequence is a legal re-introduction, not a resurrection"
  );

  // An empty transition log is trivially clean.
  let mut ck2 = IncarnationMonotonicChecker::new();
  assert!(ck2.observe_transitions(&[]).is_ok(), "an empty transition log is clean");
}
