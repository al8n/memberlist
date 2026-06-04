//! A crashed node is declared Dead at its peers; a restart at a higher
//! incarnation re-converges to Alive.
use memberlist_simulation::checker::{IncarnationMonotonicChecker, NoResurrectionChecker, Transition};
use memberlist_simulation::{Cluster, EndpointOptions, Event, Meta, Reliability};
use memberlist_proto::typed::State;
use smol_str::SmolStr;
use std::{net::SocketAddr, time::Duration};

fn addr(p: u16) -> SocketAddr { format!("127.0.0.1:{p}").parse().unwrap() }

/// Drain `host`'s events and report whether any `UserPacket` carrying exactly
/// `payload` arrived (the observable for a delivered unreliable `send`).
fn received_user_payload(c: &mut Cluster, host: SocketAddr, payload: &[u8]) -> bool {
  let mut got = false;
  while let Some(ev) = c.poll_event(host) {
    if let Event::UserPacket(p) = ev {
      if p.reliability() == Reliability::Unreliable && p.data_ref().as_ref() == payload {
        got = true;
      }
    }
  }
  got
}

#[test]
fn packet_to_crashed_node_is_dropped_at_enqueue() {
  let a0 = addr(33100);
  let a1 = addr(33101);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node("n1".into(), a1);
  c.inject_alive(a0, "n1".into(), a1, 1);
  c.inject_alive(a1, "n0".into(), a0, 1);
  for _ in 0..200 { c.step(); }

  c.crash(a1);
  // n0 tries to send to the crashed n1 while it is down.
  c.send(a0, a1, bytes::Bytes::from_static(b"downtime"));
  assert_eq!(c.queued_to(a1), 0,
    "a packet to a crashed node must be dropped at enqueue, never queued for post-restart delivery");

  // Restart before any such packet could have been delivered; the fresh endpoint
  // must reconverge without ever receiving a downtime packet (none exist).
  assert!(c.restart(a1), "restart at a small incarnation must succeed");
  for _ in 0..500 { c.step(); }
}

#[test]
fn crashed_node_dies_then_restart_reconverges() {
  let a0 = addr(33000);
  let a1 = addr(33001);
  let a2 = addr(33002);
  let id1: SmolStr = "n1".into();
  let id2: SmolStr = "n2".into();
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node(id1.clone(), a1);
  c.add_node(id2.clone(), a2);
  // Bootstrap full mesh from n0.
  c.inject_alive(a0, id1.clone(), a1, 1);
  c.inject_alive(a0, id2.clone(), a2, 1);
  c.inject_alive(a1, "n0".into(), a0, 1);
  c.inject_alive(a1, id2.clone(), a2, 1);
  c.inject_alive(a2, "n0".into(), a0, 1);
  c.inject_alive(a2, id1.clone(), a1, 1);
  // Let it settle, capture n2's incarnation as seen by n0.
  for _ in 0..200 { c.step(); }
  let inc_before = c.get_node_incarnation(a0, &id2);

  // Crash n2; step long enough for n0's suspicion to mature to Dead.
  c.crash(a2);
  for _ in 0..4000 { c.step(); }
  let st = c.member_liveness(a0, &id2);
  assert!(matches!(st, Some(State::Dead) | None),
    "after crash, n0 must see n2 Dead or pruned, got {st:?}");

  // Restart n2 (fresh, higher incarnation) and let it rejoin.
  assert!(c.restart(a2), "restart at a small incarnation must succeed");
  for _ in 0..2000 { c.step(); }
  let st2 = c.member_liveness(a0, &id2);
  let inc_after = c.get_node_incarnation(a0, &id2);
  assert_eq!(st2, Some(State::Alive), "after restart, n0 must see n2 Alive again, got {st2:?}");
  assert!(inc_after > inc_before,
    "restart must supersede the dead incarnation: before={inc_before:?} after={inc_after:?}");
}

/// A crashed node is a dead process: it cannot SEND either. Neither an
/// application `send` nor a `leave` fanout originating at the down node may
/// inject a single datagram into the network — the symmetric counterpart to
/// "a packet to a crashed node is dropped at enqueue".
#[test]
fn crashed_source_cannot_inject_datagram() {
  let a0 = addr(33200);
  let a1 = addr(33201);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node("n1".into(), a1);
  c.inject_alive(a0, "n1".into(), a1, 1);
  c.inject_alive(a1, "n0".into(), a0, 1);
  for _ in 0..200 { c.step(); }

  // Crash n0 (the SOURCE).
  c.crash(a0);

  // An application send from the down n0 must escape nothing.
  c.send(a0, a1, bytes::Bytes::from_static(b"from-the-grave"));
  assert_eq!(c.queued_to(a1), 0,
    "a crashed source must not enqueue a datagram (application send)");

  // A leave fanout from the down n0 must also escape nothing. `leave` on a
  // crashed node may early-return (its endpoint is frozen, not removed); it
  // must not panic and must not queue a Dead self-broadcast to any peer.
  let _ = c.leave(a0);
  assert_eq!(c.queued_to(a1), 0,
    "a crashed source must not enqueue a datagram (leave fanout)");
}

/// An established push-pull stream must NOT complete across a partition opened
/// after it was dialed. Reaping the cut virtual stream stops it being pumped,
/// so the anti-entropy response never crosses the cut and a fact known only to
/// the responder is never learned by the initiator over that stream.
#[test]
fn push_pull_does_not_cross_partition_opened_mid_stream() {
  let a0 = addr(33300);
  let a1 = addr(33301);
  let id_secret: SmolStr = "secret".into();
  let a_secret = addr(33399);
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node("n1".into(), a1);
  // Bootstrap the pair so they know each other (but NOT the secret node).
  c.inject_alive(a0, "n1".into(), a1, 1);
  c.inject_alive(a1, "n0".into(), a0, 1);
  for _ in 0..200 { c.step(); }

  // Teach ONLY a1 about a third node. The only way a0 can learn `secret` here
  // is via a1's push-pull RESPONSE (no gossip path is given a chance: we cut
  // the link before any pumping and never heal).
  c.inject_alive(a1, id_secret.clone(), a_secret, 1);
  assert!(c.member(a0, &id_secret).is_none(),
    "precondition: a0 must not yet know the secret node");

  // Initiate a push-pull a0 -> a1, then cut the link BEFORE stepping (so the
  // stream is reaped before a single response byte is pumped across).
  c.trigger_push_pull(a0, a1);
  c.partition(&[a0], &[a1]);
  for _ in 0..300 { c.step(); }

  // The exchange could not complete across the cut: a0 never learned the fact
  // that only a1's post-cut response would have carried.
  assert!(c.member(a0, &id_secret).is_none(),
    "a push-pull response must not cross a partition opened mid-stream (anti-entropy leak)");
}

/// A datagram enqueued BEFORE a partition (and held back by latency) must not
/// cross a link that is cut at the moment it matures. The delivery-time
/// partition re-check in `deliver_ready` drops it. The control run — identical
/// but without the cut — proves the assertion is non-vacuous (the datagram
/// would otherwise have been delivered).
#[test]
fn delayed_datagram_does_not_cross_partition_opened_after_enqueue() {
  let a0 = addr(33500);
  let a1 = addr(33501);
  let payload: &[u8] = b"delayed";

  // ── Control: no partition → the delayed datagram IS delivered. ──
  {
    let mut c = Cluster::new();
    c.add_node("n0".into(), a0);
    c.add_node("n1".into(), a1);
    // Latency holds the datagram in the queue so its delivery is in the future.
    c.set_latency(Duration::from_millis(50));

    // Enqueue at the current instant (deliver_at = now + 50ms). No partition is
    // active at enqueue, so it is genuinely queued — not dropped on the spot.
    c.send(a0, a1, bytes::Bytes::from_static(b"delayed"));
    assert_eq!(c.queued_to(a1), 1,
      "control: the delayed datagram must be queued (deliver_at in the future)");

    // Advance past deliver_at; with no cut it is delivered.
    c.advance(Duration::from_millis(60));
    assert!(received_user_payload(&mut c, a1, payload),
      "control: a delayed datagram with no partition must be delivered");
  }

  // ── Cut after enqueue, before delivery → the datagram is DROPPED. ──
  {
    let mut c = Cluster::new();
    c.add_node("n0".into(), a0);
    c.add_node("n1".into(), a1);
    c.set_latency(Duration::from_millis(50));

    // Enqueue while the link is whole (so it lands in the queue), THEN cut the
    // link before its deliver_at. The matured datagram must hit the
    // delivery-time partition check and be dropped, never reaching a1.
    c.send(a0, a1, bytes::Bytes::from_static(b"delayed"));
    assert_eq!(c.queued_to(a1), 1,
      "the datagram must be queued before the cut (else the test is vacuous)");
    c.partition(&[a0], &[a1]);

    c.advance(Duration::from_millis(60));
    assert!(!received_user_payload(&mut c, a1, payload),
      "a datagram must NOT cross a partition that opened after it was enqueued");
  }
}

/// Drive a ping `pinger → target` and return the payload the target attached to
/// its Ack (via `PingCompleted`), or `None` if no ack returned in `steps`.
fn ping_ack_payload(
  c: &mut Cluster,
  pinger: SocketAddr,
  target_id: &str,
  target_addr: SocketAddr,
  steps: usize,
) -> Option<bytes::Bytes> {
  c.trigger_ping(pinger, target_id.into(), target_addr);
  for _ in 0..steps {
    c.step();
    while let Some(ev) = c.poll_event(pinger) {
      if let Event::PingCompleted(p) = ev {
        return Some(p.payload_ref().clone());
      }
    }
  }
  None
}

/// The configured Ack payload (the `PingDelegate::ack_payload` behavior) is a
/// delegate-level config, so — like the admission policy — it must survive a
/// crash/restart. A real restarted process re-installs its delegates from
/// config; the harness must reincarnate the same behavior, not a bare endpoint
/// with an empty ack payload.
#[test]
fn restart_preserves_ack_payload() {
  let a0 = addr(33600);
  let a1 = addr(33601);
  let id1: SmolStr = "n1".into();
  let want: &[u8] = b"keepme";

  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node(id1.clone(), a1);
  // n1 attaches `keepme` to every Ack it sends.
  c.set_ack_payload(a1, bytes::Bytes::from_static(b"keepme"));
  // Bootstrap the pair so n0 can ping n1.
  c.inject_alive(a0, id1.clone(), a1, 1);
  c.inject_alive(a1, "n0".into(), a0, 1);
  for _ in 0..200 { c.step(); }

  // Precondition: before any crash, n0's ping to n1 returns the payload.
  let before = ping_ack_payload(&mut c, a0, "n1", a1, 200);
  assert_eq!(before.as_deref(), Some(want),
    "precondition: n1's configured ack payload must round-trip before the crash");

  // Crash and restart n1.
  c.crash(a1);
  for _ in 0..4000 { c.step(); }
  assert!(c.restart(a1), "restart at a small incarnation must succeed");
  for _ in 0..2000 { c.step(); }

  // The fresh endpoint must still attach the SAME configured payload — proving
  // `restart` re-installed the ack-payload behavior (without the fix the fresh
  // endpoint would carry an empty payload and this would fail).
  let after = ping_ack_payload(&mut c, a0, "n1", a1, 400);
  assert_eq!(after.as_deref(), Some(want),
    "restart must preserve the node's configured ack payload");
}

/// A datagram already in flight when its SENDER crashes must still be
/// delivered. A real UDP datagram, once transmitted, is in the network
/// regardless of what happens to the sender afterward.
#[test]
fn in_flight_datagram_from_crashed_sender_is_delivered() {
  let a0 = addr(33700);
  let a1 = addr(33701);
  let payload: &[u8] = b"in-flight";
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node("n1".into(), a1);
  // 50 ms latency holds the datagram in the queue after enqueue.
  c.set_latency(Duration::from_millis(50));

  // Enqueue the datagram; it is in the network, deliver_at is 50 ms away.
  c.send(a0, a1, bytes::Bytes::from_static(b"in-flight"));
  assert_eq!(c.queued_to(a1), 1,
    "precondition: datagram must be queued before the sender crashes");

  // Crash the SENDER before the datagram matures.
  c.crash(a0);
  assert_eq!(c.queued_to(a1), 1,
    "crashing the sender must not retract the already-queued datagram");

  // Advance past deliver_at; the datagram must reach a1.
  c.advance(Duration::from_millis(60));
  assert!(received_user_payload(&mut c, a1, payload),
    "an in-flight datagram from a crashed sender must still be delivered");
}

/// A restart must reincarnate the SAME node: its configured initial meta and
/// its installed admission policy (here, reject-alives) must both survive the
/// crash/restart, not be reset to the bare default.
#[test]
fn restart_preserves_meta_and_policy() {
  let a0 = addr(33400);
  let a1 = addr(33401);
  let id1: SmolStr = "n1".into();
  let meta1 = Meta::try_from("role=db").unwrap();

  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node_with(id1.clone(), a1, |cfg: EndpointOptions<_, _>| {
    cfg.with_initial_meta(meta1.clone())
  });
  // n1 rejects every inbound Alive (models a config.Alive that always vetoes).
  c.reject_alives(a1);

  // n1 joins n0; n1's push-pull REQUEST carries its configured meta, which n0
  // merges (n0's MergeDelegate auto-accepts). n1's own reject-alives gate only
  // affects what n1 ADMITS, not what it advertises, so n0 learns the meta.
  c.join(a1, a0);
  for _ in 0..400 { c.step(); }
  let meta_before = c.member_meta(a0, &id1).map(|(m, _, _)| m);
  assert_eq!(meta_before.as_deref(), Some(b"role=db".as_ref()),
    "precondition: n0 must learn n1's configured meta before the crash");

  // Crash and restart n1.
  c.crash(a1);
  for _ in 0..4000 { c.step(); }
  assert!(c.restart(a1), "restart at a small incarnation must succeed");
  for _ in 0..2000 { c.step(); }

  // (a) The restarted node re-advertises its ORIGINAL meta, not the default.
  let meta_after = c.member_meta(a0, &id1).map(|(m, _, _)| m);
  assert_eq!(meta_after.as_deref(), Some(b"role=db".as_ref()),
    "restart must preserve the node's configured initial meta");

  // (b) The reject-alives policy is still active on the fresh endpoint: an
  // injected Alive about a brand-new peer at a higher incarnation is vetoed,
  // so n1 never learns the peer.
  let intruder: SmolStr = "intruder".into();
  let a_intruder = addr(33499);
  c.inject_alive(a1, intruder.clone(), a_intruder, 9);
  assert!(c.member(a1, &intruder).is_none(),
    "restart must re-install the reject-alives policy: a vetoed Alive must not be admitted");
}

/// The VOPR chooser calls `leave` OUTSIDE `step`, marking the leaver's local
/// member `Left` — a membership mutation that must be recorded into the per-tick
/// transition log so the history checkers update the leaver's self-baseline.
/// Without it the baseline stays `Alive` and a later same-incarnation
/// resurrection at the leaver's own row would be compared against the stale
/// `Alive` and missed.
///
/// The machine keeps a left node `Left` (a self-`Alive` is suppressed once
/// `Left`), so a REAL same-incarnation re-alive of the leaver's own self-view is
/// not reachable through the cluster. This test therefore proves the contract in
/// two faithful parts (mirroring how the same-step masking tests prove the
/// recorders): (1) the live cluster's `leave` actually records the leaver's
/// `Alive -> Left` transition into `history_transitions`; (2) feeding that
/// recorded log to a `NoResurrectionChecker` seeded `Alive` advances its baseline
/// to `Left`, after which a synthetic same-incarnation `Alive` at that row FIRES
/// — the resurrection the recorded transition now lets the checker catch.
#[test]
fn chooser_leave_records_terminal_transition_for_history_checkers() {
  let a0 = addr(33600);
  let a1 = addr(33601);
  let id0: SmolStr = "n0".into();
  let mut c = Cluster::new();
  c.add_node(id0.clone(), a0);
  c.add_node("n1".into(), a1);
  c.inject_alive(a0, "n1".into(), a1, 1);
  c.inject_alive(a1, id0.clone(), a0, 1);
  for _ in 0..200 {
    c.step();
  }

  // Precondition: n0 sees its OWN self as Alive at some incarnation.
  let self_inc = c.get_node_incarnation(a0, &id0).expect("n0 must know its own incarnation");
  assert_eq!(
    c.member_liveness(a0, &id0),
    Some(State::Alive),
    "precondition: n0's self-view must be Alive before the leave"
  );

  // Isolate this tick's log (as the VOPR loop does at the tick boundary), then
  // perform the chooser-phase graceful leave OUTSIDE `step`.
  c.clear_history_transitions();
  c.leave(a0).expect("leave of a live node succeeds");

  // (1) n0's own `Alive -> Left` transition is recorded.
  assert_eq!(
    c.member_liveness(a0, &id0),
    Some(State::Left),
    "leave must mark n0's self-view Left"
  );
  let leave_transition = c
    .history_transitions()
    .iter()
    .find(|t| t.observer() == a0 && t.subject() == &id0)
    .expect("the chooser leave must record n0's self transition into the per-tick log");
  assert_eq!(
    leave_transition.state(),
    Some(State::Left),
    "the recorded leave transition must carry the terminal Left state"
  );
  assert_eq!(
    leave_transition.incarnation(),
    Some(self_inc),
    "the recorded leave transition carries the leaver's current incarnation"
  );
  assert!(
    !leave_transition.pruned(),
    "a leave is not a prune: the transition must not be flagged pruned"
  );

  // (2) The recorded transition advances the checker's baseline to Left, so a
  // later same-incarnation Alive at n0's own row is now caught. Seed the checker
  // Alive (the pre-leave baseline the VOPR campaign holds), replay the recorded
  // log, then a synthetic same-incarnation resurrection must FIRE.
  let mut ck = NoResurrectionChecker::new();
  assert!(
    ck.observe_one(a0, &id0, Some(State::Alive), Some(self_inc), false).is_ok(),
    "pre-leave baseline is Alive"
  );
  assert!(
    ck.observe_transitions(c.history_transitions()).is_ok(),
    "the recorded leave log is itself legal (Alive -> Left is not a resurrection)"
  );
  let resurrection = [Transition::new(a0, id0.clone(), Some(State::Alive), Some(self_inc), false)];
  let r = ck.observe_transitions(&resurrection);
  assert!(
    r.is_violation(),
    "with the leave recorded, a same-incarnation Alive at the leaver's row must fire: {r:?}"
  );
  assert!(
    r.reason().is_some_and(|s| s.contains("resurrection")),
    "the violation must name the resurrection: {r:?}"
  );
}

/// Restart sibling of the leave history-baseline fix: after a restart the VOPR
/// chooser clears the restarted observer's checker baselines, but `restart`
/// records no transition, so the restarted endpoint's fresh self row
/// `Alive@next_inc` must be re-seeded as the baseline (via `observe_observer`)
/// or the next mutation on that row is silently adopted as the first
/// observation instead of checked.
///
/// This proves the re-seed installs the baseline: with `observe_observer` the
/// restarted observer's self-incarnation is the baseline, so a later regressing
/// `Alive@<next_inc` on its own row FIRES; a control checker that clears WITHOUT
/// re-seeding swallows the same regression as a first observation.
#[test]
fn restart_reseeds_observer_history_baseline() {
  let a0 = addr(33700);
  let a1 = addr(33701);
  let id0: SmolStr = "n0".into();
  let mut c = Cluster::new();
  c.add_node(id0.clone(), a0);
  c.add_node("n1".into(), a1);
  c.inject_alive(a0, "n1".into(), a1, 1);
  c.inject_alive(a1, id0.clone(), a0, 1);
  for _ in 0..200 {
    c.step();
  }

  // Crash n0 and mature it to Dead/pruned at n1, then restart it. The restart
  // supersedes the dead incarnation, so n0's fresh self-view is Alive at some
  // next_inc >= 2.
  c.crash(a0);
  for _ in 0..4000 {
    c.step();
  }
  assert!(c.restart(a0), "restart at a small incarnation must succeed");
  for _ in 0..2000 {
    c.step();
  }
  assert_eq!(
    c.member_liveness(a0, &id0),
    Some(State::Alive),
    "after restart n0's self-view must be Alive"
  );
  let next_inc = c
    .get_node_incarnation(a0, &id0)
    .expect("restarted n0 must know its own incarnation");
  assert!(
    next_inc >= 2,
    "restart must supersede the pre-crash incarnation: next_inc={next_inc}"
  );

  // A synthetic regression on n0's own self row: an incarnation strictly below
  // the post-restart baseline.
  let regress =
    [Transition::new(a0, id0.clone(), Some(State::Alive), Some(next_inc - 1), false)];

  // Control: clear the observer (as the chooser does) but do NOT re-seed. With
  // no baseline the regression is the first observation and is silently
  // swallowed — the bug this fix closes.
  let mut control = IncarnationMonotonicChecker::new();
  control.observe(&c);
  control.clear_observer(a0);
  assert!(
    control.observe_transitions(&regress).is_ok(),
    "without the re-seed, the first observation after the clear is adopted, not checked"
  );

  // Fixed: clear then re-seed the single observer (exactly what
  // `seed_history_baselines_for` does after `clear_observer`). The fresh
  // Alive@next_inc self row is now the baseline, so the regression FIRES.
  let mut fixed = IncarnationMonotonicChecker::new();
  fixed.observe(&c);
  fixed.clear_observer(a0);
  assert!(
    fixed.observe_observer(&c, a0).is_ok(),
    "re-seeding a fresh observer with no prior baseline cannot itself fire"
  );
  let r = fixed.observe_transitions(&regress);
  assert!(
    r.is_violation(),
    "with the re-seed, a later sub-baseline incarnation on the restarted self row must fire: {r:?}"
  );
  assert!(
    r.reason().is_some_and(|s| s.contains("regress")),
    "the violation must name the incarnation regression: {r:?}"
  );
}

/// A node seeded at the accepted upper boundary of the initial-incarnation range
/// cannot be restarted: superseding it would need `MAX/2 + 1`, which the builder
/// rejects as outside the reserved lower half. `restart` must refuse gracefully
/// rather than panic inside `with_initial_incarnation`.
#[test]
fn restart_refuses_when_superseding_would_exceed_safe_range() {
  let a0 = addr(33600);
  let a1 = addr(33601);
  let id1: SmolStr = "n1".into();
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node_with(id1.clone(), a1, |opts| opts.with_initial_incarnation(u32::MAX / 2));
  c.inject_alive(a0, id1.clone(), a1, u32::MAX / 2);
  for _ in 0..100 { c.step(); }

  c.crash(a1);
  assert!(!c.restart(a1),
    "restart must refuse when the superseding incarnation would leave the safe range");
}

/// A peer can observe a node at an incarnation in the upper half of the u32
/// range (a crafted or corrupt gossip). Restarting that node would need an even
/// higher incarnation, outside the safe range, so `restart` must refuse rather
/// than panic.
#[test]
fn restart_refuses_upper_half_observed_incarnation() {
  let a0 = addr(33610);
  let a1 = addr(33611);
  let id1: SmolStr = "n1".into();
  let mut c = Cluster::new();
  c.add_node("n0".into(), a0);
  c.add_node(id1.clone(), a1);
  c.inject_alive(a0, id1.clone(), a1, u32::MAX / 2 + 5);
  c.crash(a1);
  assert!(!c.restart(a1),
    "restart must refuse when an observed upper-half incarnation cannot be superseded");
}
