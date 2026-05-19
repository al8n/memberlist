//! Integration tests for scenario helpers: gossip, probe, and suspect/dead.

use memberlist_simulation::scenarios::gossip::three_node_gossip;
use smol_str::SmolStr;

fn addr(port: u16) -> std::net::SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

/// After n1 gossips its knowledge of n2 and n3, both n2 and n3 should
/// eventually learn about each other and about n1.
#[test]
fn gossip_propagates_n1_knowledge_to_n2_and_n3() {
  let a1 = addr(19301);
  let a2 = addr(19302);
  let a3 = addr(19303);
  let c = three_node_gossip(a1, a2, a3, 500);

  for (host_addr, host_name) in [(a2, "n2"), (a3, "n3")] {
    for peer in ["n1", "n2", "n3"] {
      if peer == host_name {
        continue;
      }
      assert!(
        c.member(host_addr, &SmolStr::new(peer)).is_some(),
        "{host_name} should know {peer} after gossip"
      );
    }
  }
}

/// After a direct probe where the target is reachable, the prober's view
/// should still record the target as Alive.
#[test]
fn probe_success_target_stays_alive() {
  use memberlist_simulation::scenarios::probe::probe_success;
  use memberlist_wire::typed::State;

  let prober = addr(19401);
  let target = addr(19402);
  let c = probe_success(prober, target);

  let state = c.member_liveness(prober, &SmolStr::new("target"));
  assert_eq!(
    state,
    Some(State::Alive),
    "target should still be Alive after successful probe"
  );
}

/// After probing an unresponsive target (no real endpoint) with an indirect
/// helper that also cannot reach it, the prober should mark target Suspect
/// or Dead.
#[test]
fn indirect_probe_marks_unresponsive_target_suspect() {
  use memberlist_simulation::scenarios::probe::indirect_probe_dead_target;
  use memberlist_wire::typed::State;

  let prober = addr(19501);
  let helper = addr(19502);
  let target = addr(19503); // no real endpoint
  let c = indirect_probe_dead_target(prober, helper, target);

  let state = c.member_liveness(prober, &SmolStr::new("target"));
  assert!(
    matches!(state, Some(State::Suspect) | Some(State::Dead)),
    "target should be Suspect or Dead after indirect probe timeout, got {state:?}"
  );
}

/// After injecting a Suspect and waiting past the suspicion timeout, the
/// ghost node should be Dead.
#[test]
fn suspect_dead_transition() {
  use memberlist_simulation::scenarios::suspect_dead::suspect_then_dead;
  use memberlist_wire::typed::State;

  let observer = addr(19601);
  let ghost = addr(19699);
  let c = suspect_then_dead(observer, SmolStr::new("ghost"), ghost);

  let state = c.member_liveness(observer, &SmolStr::new("ghost"));
  assert_eq!(
    state,
    Some(State::Dead),
    "ghost should be Dead after suspicion timeout"
  );
}
