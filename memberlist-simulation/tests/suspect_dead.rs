//! Scenario: dropped probe → suspect → dead transition.
//!
//! Test 1 (`dropped_probe_leads_to_suspect`): node1 probes node2, but the
//! probe datagram is dropped. After the probe timeout, node1 marks node2
//! Suspect.
//!
//! Test 2 (`suspect_transitions_to_dead_after_timeout`): a Suspect is
//! injected directly into node1's view of a ghost node. After the suspicion
//! timer (suspicion_mult * probe_interval = 4s) elapses, node1 marks the
//! ghost Dead.

use memberlist_proto::typed::State;
use memberlist_simulation::Cluster;
use smol_str::SmolStr;
use std::time::Duration;

fn addr(port: u16) -> std::net::SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

/// Partition the network so the probe Ping never reaches target and the
/// target's refute can never return. A 2-node / zero-indirect probe still
/// arms the reliable fallback concurrently and races the single cumulative
/// deadline (direct probe_timeout, then another probe_timeout for the
/// indirect+fallback window) before suspecting — it does NOT short-circuit
/// to Suspect on the direct timeout alone. `advance()` does not process
/// streams, so the partitioned fallback never completes; the probe
/// terminates → Suspect once the cumulative deadline elapses (two FSM
/// ticks: Direct→Indirect, then Indirect→terminate).
#[test]
fn dropped_probe_leads_to_suspect() {
  let mut c = Cluster::new();
  let a1 = addr(19001);
  let a2 = addr(19002);
  c.add_node(SmolStr::new("prober"), a1);
  c.add_node(SmolStr::new("target"), a2);

  // Bootstrap prober's knowledge of target.
  c.inject_alive(a1, SmolStr::new("target"), a2, 1);

  // Isolate prober from target: all datagrams between them are dropped,
  // so the Ping never arrives and the target can never refute via Alive.
  c.partition(&[a1], &[a2]);

  // Manually kick off a probe round on prober (bypasses scheduler).
  let launched = c.trigger_probe(a1);
  assert!(launched, "probe should launch — prober knows target");

  // Tick 1 — past the direct probe_timeout (500 ms): AwaitingDirectAck
  // deadline elapsed → fan-out. No indirect peers (2 nodes), but reliable
  // ping is enabled, so the probe arms the concurrent fallback and enters
  // AwaitingIndirect with a fresh cumulative deadline (now + probe_timeout)
  // rather than failing immediately.
  c.advance(Duration::from_millis(600));
  assert!(
    matches!(
      c.member_liveness(a1, &SmolStr::new("target")),
      Some(State::Alive)
    ),
    "target must NOT be suspected on the direct timeout alone — the \
     reliable fallback must race the cumulative deadline first"
  );

  // Tick 2 — past the cumulative deadline: the partitioned reliable
  // fallback never made contact (advance() processes no streams) →
  // AwaitingIndirect deadline elapsed → probe_terminate_failure →
  // process_suspect → target transitions to Suspect.
  c.advance(Duration::from_millis(600));

  // Check state immediately: no step() calls so no network messages have
  // been exchanged and no refute cycle can have completed.
  let state = c.member_liveness(a1, &SmolStr::new("target"));
  assert!(
    state.is_some() && !matches!(state, Some(State::Alive)),
    "target should NOT be Alive after dropped probe, got {state:?}"
  );
}

/// Inject a Suspect for a ghost node and wait for the suspicion timer to
/// expire. After suspicion_mult * probe_interval (4 * 1s = 4s default) the
/// node should transition to Dead.
#[test]
fn suspect_transitions_to_dead_after_timeout() {
  let mut c = Cluster::new();
  let a1 = addr(19101);
  let ghost_addr = addr(19199);

  c.add_node(SmolStr::new("n1"), a1);

  // First make the ghost node alive (so it is tracked by the state machine).
  c.inject_alive(a1, SmolStr::new("ghost"), ghost_addr, 1);

  // Inject a Suspect for the ghost, attributed to the local node.
  c.inject_suspect(a1, SmolStr::new("ghost"), SmolStr::new("n1"), 1);

  // Verify ghost is now Suspect.
  let state_after_suspect = c.member_liveness(a1, &SmolStr::new("ghost"));
  assert_eq!(
    state_after_suspect,
    Some(State::Suspect),
    "ghost should be Suspect right after inject, got {state_after_suspect:?}"
  );

  // Advance well past the suspicion timeout (suspicion_mult * probe_interval
  // = 4 * 1000 ms = 4s). With suspicion k=0 (single node), the min timeout
  // equals the max timeout, so the suspicion fires immediately at min.
  c.advance(Duration::from_secs(5));
  for _ in 0..30 {
    c.step();
  }

  let state = c.member_liveness(a1, &SmolStr::new("ghost"));
  assert_eq!(
    state,
    Some(State::Dead),
    "ghost should be Dead after suspicion timeout, got {state:?}"
  );
}
