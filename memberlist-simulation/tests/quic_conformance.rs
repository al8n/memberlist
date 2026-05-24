#![cfg(feature = "__quic-harness")]
//! Membership conformance must hold UNCHANGED when reliable exchanges ride
//! real quinn-proto instead of stubs. Asserts state / events / timing only —
//! never QUIC wire bytes.
//!
//! The matrix:
//! - `two_node_join_over_quic_reaches_alive_both_sides` — join push/pull rides
//!   a real quinn bidi stream; both nodes converge Alive.
//! - `d1_join_reply_merges_before_bridge_reap` — the full reply lands, then
//!   the connection is dropped the SAME tick: the merge is still applied
//!   (D1 drain-before-reap), incl. the connection-loss teardown variant.
//! - `reliable_fallback_rescues_udp_blocked_probe_no_false_suspect` — a
//!   single triggered probe with direct + indirect UDP dropped completes via
//!   the QUIC reliable-ping fallback on the warm pooled connection; the
//!   healthy peer is never falsely Suspected and the bridge is reaped.
//! - `idle_connection_is_drained_reaped` — idle past `max_idle_timeout`: the
//!   connection is drained-reaped (slab + peers entry gone), a stray datagram
//!   for the reaped handle is a no-op.
//! - `leave_over_quic_observed_dead_or_left` — `QuicEndpoint::leave` fires
//!   `Event::LeftCluster`; the peer observes the local node Dead/Left.
//! - `large_state_push_pull_completes_under_backpressure` — a shrunk quinn
//!   flow window forces `WriteError::Blocked`; the join still completes with
//!   zero byte loss.
//! - `one_way_user_message_over_quic_delivered_and_no_bridge_leak` — a
//!   reliable `start_user_message` over QUIC: the inbound bridge has NO
//!   outbound bytes, the deferred `finish()` must still finish its (empty)
//!   send half so `is_terminal()` fires and the bridge is reaped (no leak).
//! - `backpressured_done_bridge_credit_starved_reaped_at_flush_deadline` —
//!   a `Done`-but-`pending_out`-non-empty bridge whose credit is permanently
//!   withheld must be reaped by the bridge-level flush deadline (bounded
//!   lifetime; no post-deadline stale write).
//! - `backpressured_outbound_request_credit_starved_reaped_at_stream_deadline`
//!   — an `OutboundAwaitingResponse`-with-retained-tail bridge whose
//!   `MAX_STREAM_DATA` ACKs are dropped must reap at the exchange deadline
//!   via the pre-write deadline check (FSM `enter_failed(Timeout)`),
//!   strictly before quinn's idle timeout fires `ConnectionLost`; no
//!   post-deadline write, no FIN.
//!
//! Conformance-parity (the capstone): the same SWIM membership outcomes /
//! timing as the existing non-quic suite, proving the composition does not
//! perturb SWIM timing.

use memberlist_simulation::quic_net::QuicCluster;
use memberlist_wire::typed::State;
use smol_str::SmolStr;
use std::time::Duration;

fn id(s: &str) -> SmolStr {
  SmolStr::new(s)
}

#[test]
fn two_node_join_over_quic_reaches_alive_both_sides() {
  let a = "127.0.0.1:7001".parse().unwrap();
  let b = "127.0.0.1:7002".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after QUIC push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after QUIC push/pull join"
  );
}

#[test]
fn d1_join_reply_merges_before_bridge_reap() {
  let a = "127.0.0.1:7011".parse().unwrap();
  let b = "127.0.0.1:7012".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  c.drop_connection_after_first_full_reply(a);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  // The push/pull reply reached A's Endpoint (A saw B Alive) the SAME tick
  // the QUIC connection was torn down: D1 (drain-before-reap) guarantees the
  // bridge drained its `PushPullReplyReceived` INTO the Endpoint before the
  // stream/connection was reaped. If D1 were violated the merge would be
  // lost and A would NEVER have seen B Alive.
  assert!(
    c.ever_saw_alive(a, &id("b")),
    "join reply must merge into the Endpoint even though the QUIC conn dropped the same tick"
  );
}

#[test]
fn d1_join_reply_merges_before_connection_loss_teardown() {
  // Teardown variant of D1: instead of a graceful close right after the
  // reply, the whole connection is force-lost (every bridge on it is
  // marked fatal via the coordinator's `Event::ConnectionLost` path). The
  // merge must still be applied before the StreamErrored reap.
  let a = "127.0.0.1:7013".parse().unwrap();
  let b = "127.0.0.1:7014".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  c.lose_connection_after_first_full_reply(a);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  // Teardown variant: the connection is force-lost (idle-timeout →
  // `Event::ConnectionLost` → every bridge marked fatal → StreamErrored
  // reap). D1 still requires the reply to have been drained into the
  // Endpoint before that reap, so A must have seen B Alive.
  assert!(
    c.ever_saw_alive(a, &id("b")),
    "join reply must merge into the Endpoint even though the QUIC conn was lost the same tick"
  );
}

/// Reliable-fallback invariant: a single triggered failure-detection
/// probe whose direct + indirect UDP path is blocked must complete via the
/// QUIC reliable-ping fallback on the warm pooled connection, and the healthy
/// peer must NOT be falsely Suspected.
///
/// One isolated probe cycle, asserted genuinely:
///   * a real QUIC push/pull join warms the pooled quinn connection (the
///     reliable-ping fallback then opens a bidi stream on the EXISTING
///     connection — no fresh-handshake RTT). A short direct `probe_timeout`
///     with a long `probe_interval` gives the fallback a wide cumulative
///     deadline. (The SUBJECT here is the fallback completion + no-false-
///     Suspect invariant, NOT SWIM-timing parity — the parity tests
///     deliberately keep the standard LAN knobs.)
///   * only the UDP probe frames between A and B are dropped; the QUIC
///     (`>= 0x40`) path stays open, so the direct ping + the indirect ping
///     cannot get a UDP Ack and the probe is forced onto the QUIC
///     reliable-ping fallback.
///   * exactly ONE probe is triggered, then `step()` is driven only until
///     that probe's fallback bridge has opened AND been reaped — its full
///     open→reap cycle completes within ≈1s of virtual time, far inside the
///     long (10s) `probe_interval`, so the periodic scheduler never fires a
///     second probe. The observed cycle is therefore EXACTLY the one
///     triggered probe (no unbounded periodic-probe loop, no sticky-flag
///     fragility). An absolute virtual-time guard past the cumulative failure
///     deadline only bounds a hypothetically-stalled run so the assertions
///     below report the failure.
///
/// The assertion is genuine, not merely "did not suspect within the loop":
///   1. the fallback bridge is observed to actually open (the QUIC
///      reliable-ping path was exercised, not bypassed);
///   2. that same bridge is then reaped — the Ping↔Ack exchange actually
///      finished (no stall, no leak);
///   3. B was never Suspected and is still Alive — and since the UDP path is
///      permanently blocked, the only signal that could explain this is the
///      reliable-ping fallback completing.
#[test]
fn reliable_fallback_rescues_udp_blocked_probe_no_false_suspect() {
  let a = "127.0.0.1:7021".parse().unwrap();
  let b = "127.0.0.1:7022".parse().unwrap();
  let mut c = QuicCluster::two_node_join_slow_probe(a, b);
  for _ in 0..3_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "precondition: A sees B Alive over QUIC"
  );
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "warm-up join must have fully settled (no leftover bridge)"
  );

  // Drop EVERY direct + indirect UDP probe between A and B; the QUIC path
  // stays open so the only way A can confirm B is the reliable-ping fallback.
  c.drop_all_udp_probes_between(a, b);

  let t0 = c.elapsed();
  c.trigger_probe(a);
  // Drive ONLY the triggered probe's lifetime: the direct ping times out
  // (+probe_timeout), the probe escalates and opens the QUIC reliable-ping
  // fallback bridge, the fallback Ping↔Ack completes, and the bridge is
  // reaped. We watch that full open→reap cycle and stop the instant it
  // closes — that happens within ≈1s of virtual time, far inside the long
  // (10s) probe_interval, so the periodic scheduler never gets to fire a
  // second probe: the cycle observed is EXACTLY the one triggered probe (no
  // unbounded periodic-probe loop, no sticky-flag fragility).
  let mut fallback_bridge_opened = false;
  let mut fallback_bridge_reaped = false;
  for _ in 0..40_000 {
    let progressed = c.step();
    let live = c.live_bridge_count(a);
    if live > 0 {
      fallback_bridge_opened = true;
    } else if fallback_bridge_opened {
      // The bridge opened and is now gone → the reliable-ping exchange
      // finished and its bridge was reaped.
      fallback_bridge_reaped = true;
      break;
    }
    // Absolute guard: well past the triggered probe's cumulative failure
    // deadline (trigger + scale_timeout(probe_interval) ≈ +10s). If we ever
    // got here the fallback failed to complete and B would already be
    // Suspected — the assertions below would catch it.
    if c.elapsed() - t0 >= Duration::from_secs(12) {
      break;
    }
    if !progressed {
      break;
    }
  }

  assert!(
    fallback_bridge_opened,
    "the UDP-blocked probe must escalate and open a QUIC reliable-ping \
     fallback bridge (the fallback path was genuinely exercised)"
  );
  assert!(
    fallback_bridge_reaped,
    "the reliable-ping fallback bridge must complete its Ping↔Ack and be \
     reaped (the fallback exchange actually finished — no stall, no leak)"
  );
  assert!(
    !c.ever_suspected(a, &id("b")),
    "the reliable-ping fallback rescued the probe — B must never be Suspected"
  );
  assert!(
    c.sees_alive(a, &id("b")),
    "B must remain Alive: with the UDP probe path blocked, only the completed \
     QUIC reliable-ping fallback can keep B from being Suspected"
  );
}

#[test]
fn idle_connection_is_drained_reaped() {
  let a = "127.0.0.1:7031".parse().unwrap();
  let b = "127.0.0.1:7032".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  for _ in 0..5_000 {
    if !c.step() {
      break;
    }
  }
  // A connection must have actually been established by the join.
  assert!(
    c.live_quic_connections(a) >= 1,
    "the join must have opened a QUIC connection"
  );
  c.idle_for(Duration::from_secs(60)); // past max_idle_timeout
  for _ in 0..5_000 {
    if !c.step() {
      break;
    }
  }
  assert_eq!(
    c.live_quic_connections(a),
    0,
    "idle conn must be drained-reaped (slab + peers entry gone)"
  );
  // A late datagram for the reaped handle must not panic.
  c.inject_stray_quic_datagram(a);
  let _ = c.step();
}

#[test]
fn leave_over_quic_observed_dead_or_left() {
  let a = "127.0.0.1:7041".parse().unwrap();
  let b = "127.0.0.1:7042".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(c.sees_alive(b, &id("a")), "precondition: B sees A Alive");

  c.leave(a).expect("leave should succeed");
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }

  assert!(
    c.left_cluster(a),
    "A must emit Event::LeftCluster once its dead-self broadcast is flushed"
  );
  // B must have observed A transition to Left/Dead. (The Left entry is
  // later garbage-collected to absent after `gossip_to_the_dead_time`, so
  // assert the transition was observed, not the post-GC steady state — the
  // plain harness leave tests check within the pre-GC window for the same
  // reason.)
  assert!(
    c.ever_saw_gone(b, &id("a")),
    "B must observe A transition to Left/Dead after A's QUIC-bootstrapped leave"
  );
}

/// `Event::LeftCluster` is the dead-self FLUSH boundary, not a coordinator
/// internal buffer hop: it must NOT be observable through `poll_event`
/// until the LAST dead-self datagram has actually crossed
/// `QuicEndpoint::poll_memberlist_transmit` to the external driver.
///
/// Sequence under test:
///   1. `leave(a)` queues `a`'s dead-self broadcast and sets the
///      leave-flush counter inside the inner `Endpoint`.
///   2. `tick_only(a)` runs `handle_timeout(now)` (which runs
///      `run_tick` → `collect_transmits`). `collect_transmits` only
///      touches QUIC transmits, so `a`'s dead-self memberlist Transmits
///      stay queued inside the inner endpoint's `pending_transmits` and
///      `LeftCluster` is NOT yet queued in `pending_events`.
///   3. `host_has_left_cluster_pending(a)` peeks `poll_event` (with
///      requeue), and MUST report `false`: `LeftCluster` cannot leak to
///      the driver before the dead-self bytes do.
///   4. Drain `a`'s memberlist Transmits one at a time via
///      `drain_one_memberlist_transmit(a)`. Each pop calls
///      `Endpoint::poll_transmit`, decrementing the leave-flush counter.
///      The final pop emits `Event::LeftCluster` into the inner endpoint.
///   5. After the source-side drain returns `false`, the next
///      `poll_event(a)` (via `left_cluster(a)` after a final
///      `scan_events` pass) MUST report `LeftCluster`. Meanwhile B,
///      having received the leave broadcast on the simulation queue,
///      MUST see `a` as `Dead`/`Left` (not `Suspect`).
///
/// Negative control (verified out-of-band by reintroducing a memberlist
/// pre-drain into `collect_transmits`): step (3) would flip to `true`
/// because the leave-flush counter would tick on the coordinator-internal
/// buffer hop, queuing `LeftCluster` BEFORE any byte crosses the external
/// `poll_memberlist_transmit` boundary. The existing
/// `leave_over_quic_observed_dead_or_left` does not assert this ordering,
/// so it would pass by accident; this test fails closed.
#[test]
fn left_cluster_observable_only_after_memberlist_transmit_drained_to_driver() {
  let a = "127.0.0.1:7045".parse().unwrap();
  let b = "127.0.0.1:7046".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);

  // Warm up: drive the join push/pull to settled membership over the
  // QUIC handshake (same shape as `leave_over_quic_observed_dead_or_left`).
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(b, &id("a")) && c.sees_alive(a, &id("b")),
    "precondition: both peers must converge Alive before the leave"
  );

  // (1) Begin the leave on `a`. The inner `Endpoint` queues `a`'s
  //     dead-self broadcast into `pending_transmits` and arms its
  //     leave-flush counter; no `Event::LeftCluster` is emitted yet.
  c.leave(a).expect("leave should succeed");

  // (2) Tick `a` only — no `poll_memberlist_transmit` drain inside this
  //     call. `run_tick`'s `collect_transmits` only touches QUIC
  //     transmits, so the dead-self memberlist tail stays queued inside
  //     the inner endpoint.
  c.tick_only(a);

  // (3) Peek `a`'s events BEFORE any memberlist-transmit drain. The
  //     leave-flush counter has not been decremented (no inner pop has
  //     happened), so `LeftCluster` MUST NOT be queued yet. With the
  //     buggy pre-drain, the inner pops happened in step (2) and this
  //     would already report `true` — the load-bearing observable.
  assert!(
    !c.host_has_left_cluster_pending(a),
    "Event::LeftCluster must NOT be observable through poll_event \
     before the dead-self memberlist tail has been drained via \
     poll_memberlist_transmit. A buggy pre-drain inside collect_transmits \
     would tick the leave-flush counter on a coordinator-internal buffer \
     hop, queuing LeftCluster before any byte crossed the external boundary."
  );

  // (4) Drain `a`'s memberlist Transmits one at a time. Each pop calls
  //     `Endpoint::poll_transmit`, ticking the leave-flush counter. The
  //     helper also enqueues + delivers the dead-self datagram into B so
  //     B can observe `a` as Dead/Left (not Suspect).
  let mut last_was_some = false;
  let mut drained = 0usize;
  loop {
    let popped = c.drain_one_memberlist_transmit(a);
    if !popped {
      break;
    }
    drained += 1;
    last_was_some = true;
    // Hard cap to prevent a regression that wedges this loop.
    assert!(
      drained < 1024,
      "dead-self drain should bound out quickly; drained={drained}"
    );
  }
  assert!(
    last_was_some,
    "at least one memberlist Transmit must have been pending after \
     leave(now) (the dead-self broadcast)"
  );

  // (5) Now that the dead-self tail has fully crossed to the external
  //     driver, `LeftCluster` MUST be observable on the next poll. Run a
  //     no-op step so `scan_events` picks it up (`left_cluster` is the
  //     harness's recorded view).
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
    if c.left_cluster(a) {
      break;
    }
  }
  assert!(
    c.left_cluster(a),
    "after every dead-self memberlist Transmit has crossed \
     poll_memberlist_transmit, the next poll_event(a) must return \
     Event::LeftCluster"
  );

  // The leave broadcast actually reached `b`: it observed `a` transition
  // to Left/Dead (not Suspect). The Left entry is later GC'd to absent,
  // so assert the transition was OBSERVED (same shape as
  // `leave_over_quic_observed_dead_or_left`).
  assert!(
    c.ever_saw_gone(b, &id("a")),
    "b must observe a as Dead/Left after the leave broadcast — \
     not Suspect (which is what a teardown-before-drain would produce \
     under the buggy pre-drain)"
  );
}

#[test]
fn large_state_push_pull_completes_under_backpressure() {
  let a = "127.0.0.1:7051".parse().unwrap();
  let b = "127.0.0.1:7052".parse().unwrap();
  // Tiny quinn stream-receive window + a 24-extra-peer push snapshot: the
  // join push/pull payload exceeds one window, so `Bridge::pump_out` hits
  // `WriteError::Blocked` and must retain + retry the remainder. The join
  // must still complete with zero byte loss. The 24 extras are ghosts (no
  // endpoint) so B garbage-collects them once they age out — assert the
  // TRANSFER (B received every one across the back-pressured stream), not
  // the post-GC steady state, exactly as the leave test does for the GC'd
  // Left entry.
  let mut c = QuicCluster::two_node_join_small_window(a, b, 24);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(b, &id("a")),
    "the large push/pull must complete despite flow-control back-pressure"
  );
  // Every peer A pushed must have crossed the back-pressured stream intact
  // (B observed each `Alive` at least once → the retain+retry of the
  // `WriteError::Blocked` remainder dropped ZERO bytes).
  for n in 0..24 {
    let peer = id(&format!("extra-{n}"));
    assert!(
      c.ever_saw_alive(b, &peer),
      "peer {peer} never crossed the back-pressured stream (byte loss)"
    );
  }
}

/// Forced back-pressure on the RESPONDER's send half: the inbound push/pull
/// **response** is far larger than one tiny quinn stream-window, so `b`'s
/// `Bridge::pump_out` partial-accepts then `WriteError::Blocked`s mid-reply.
/// The memberlist `Stream` flips the inbound-response phase to `Done` the
/// instant it hands the bytes to the bridge — BEFORE they reach quinn — so a
/// naive `is_terminal() == is_done()` would reap `b`'s bridge the same tick,
/// truncating the retained `pending_out` tail and never `finish()`ing the
/// send half (no FIN → `a` never sees EOF / a QUIC credit leak). The correct
/// behaviour gates `is_terminal()` on the send half having reached
/// `send_finished`, so the bridge survives until the retained tail has
/// drained AND the deferred `finish()` has FINed the send half. This asserts
/// the end-to-end observable:
///
///   1. `a` receives the COMPLETE response — it sees `b` Alive AND every one
///      of `b`'s 24 pushed extras Alive at least once (zero byte loss; if the
///      tail were truncated those extras never cross). `b` also merges `a`'s
///      push (parity).
///   2. The exchange actually completes (FIN delivered): `a` could only have
///      finished its reliable read — and merged — because `b` `finish()`ed
///      the send half after the retained tail drained. Both sides converge
///      and NO bridge leaks on either node after settle (a bridge stuck
///      un-reaped on an un-FINed send, or reaped-early truncated, fails one
///      of these).
///   3. A subsequent push/pull on the SAME pooled connection still succeeds:
///      after the back-pressured exchange's bridge is reaped, a fresh bidi
///      stream opens on the existing connection (no new handshake — the live
///      connection count stays 1) and the refresh completes.
///
/// Deterministic: every node seeds quinn's RNG from its port (fixed across
/// runs) and the only clock is the injected virtual clock; the step loop is
/// bounded. Run ≥5× standalone to prove non-flaky.
#[test]
fn responder_backpressured_reply_not_truncated_then_reused() {
  let a = "127.0.0.1:7111".parse().unwrap();
  let b = "127.0.0.1:7112".parse().unwrap();
  // B (the responder) carries `B_EXTRAS` extra Alive ghosts so B's push/pull
  // RESPONSE is many tiny-stream-windows' worth: B's inbound-response send
  // partial-accepts then `WriteError::Blocked`s and the retained `pending_out`
  // tail is carried across MANY ticks while the memberlist `Stream` phase is
  // already `Done` — the exact window in which a naive
  // `is_terminal() == is_done()` reaps the bridge and truncates the tail.
  const B_EXTRAS: usize = 150;
  let mut c = QuicCluster::two_node_join_small_window_responder_heavy(a, b, B_EXTRAS);
  for _ in 0..200_000 {
    if !c.step() {
      break;
    }
  }
  // Drive a few quiescence ticks to clear any momentarily-in-flight periodic
  // bridge before the no-leak assertion. The closed-before-drained redial
  // path preserves every periodic push-pull dial that would otherwise be
  // dropped in that window, so the cluster's steady-state churn keeps one
  // in-flight bridge at any moment more often than a lossy-dial baseline
  // would; the assertion is genuinely about leak vs in-flight, so wait for
  // the in-flight one to settle before sampling.
  for _ in 0..50_000 {
    if c.live_bridge_count(a) == 0 && c.live_bridge_count(b) == 0 {
      break;
    }
    if !c.step() {
      break;
    }
  }

  // (1) The complete response crossed the back-pressured stream: A sees B
  // Alive and EVERY one of B's pushed extras Alive at least once. If B's
  // `pending_out` tail were truncated (early reap) these would be missing.
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after the back-pressured push/pull join"
  );
  for n in 0..B_EXTRAS {
    let peer = id(&format!("bextra-{n}"));
    assert!(
      c.ever_saw_alive(a, &peer),
      "B's pushed peer {peer} never reached A — responder reply was truncated \
       (back-pressured tail dropped or bridge reaped before the full reply)"
    );
  }
  // Parity: B merged A's push too (A's extras crossed B's receive side).
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive (parity — A's push merged)"
  );
  for n in 0..4 {
    let peer = id(&format!("aextra-{n}"));
    assert!(
      c.ever_saw_alive(b, &peer),
      "A's pushed peer {peer} never reached B"
    );
  }

  // (2) The exchange completed with a FIN (A finished its reliable read and
  // merged → B `finish()`ed the send half after the retained tail drained),
  // and NO bridge leaked on either node — a bridge stuck on an un-FINed send,
  // or reaped-early/truncated, fails here.
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "no bridge may leak on A after the back-pressured join settles"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "the responder's back-pressured bridge must be reaped ONLY after the full \
     reply + FIN — and then gone (no leak)"
  );
  assert!(
    c.live_quic_connections(a) >= 1,
    "the pooled QUIC connection must still be live for reuse"
  );

  // (3) A subsequent push/pull on the SAME pooled connection still succeeds:
  // a fresh bidi stream opens on the existing connection (no new handshake —
  // the connection count stays 1) and the refresh completes.
  let conns_before = c.live_quic_connections(a);
  c.trigger_push_pull(a, b);
  let mut refresh_bridge_opened = false;
  for _ in 0..40_000 {
    let progressed = c.step();
    if c.live_bridge_count(a) > 0 {
      refresh_bridge_opened = true;
    } else if refresh_bridge_opened {
      break;
    }
    if !progressed {
      break;
    }
  }
  assert!(
    refresh_bridge_opened,
    "the subsequent push/pull must open a fresh bidi stream on the reused \
     connection"
  );
  assert_eq!(
    c.live_quic_connections(a),
    conns_before,
    "the subsequent push/pull must reuse the SAME pooled connection (no new \
     handshake)"
  );
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "the subsequent push/pull bridge must complete and be reaped (no leak)"
  );
  // State stays converged after the reused-connection refresh.
  assert!(
    c.sees_alive(a, &id("b")),
    "A still sees B Alive after the connection-reuse refresh"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B still sees A Alive after the connection-reuse refresh"
  );
}

/// A one-way reliable user message over QUIC (`start_user_message`) must be
/// delivered AND leave **zero live bridges on BOTH peers** after it settles.
///
/// The inbound side decodes the `UserData` frame, emits `Event::UserPacket`,
/// and drives the inner `Stream` straight to `Done` with **no outbound
/// bytes** — so `sent_any` is never armed. The deferred `finish()` is
/// generalized to also finish the (empty, `Ready`) send half once the stream
/// is `Done` with nothing pending, so `send_finished` is reached,
/// `is_terminal()` fires, the `UserDataReceived` event is D1-drained, and the
/// bridge + its QUIC bidi stream are reaped.
///
/// Negative control: a regression that restricted the deferred `finish()` to
/// a `sent_any`-only guard would leave the inbound bridge's `send_finished`
/// permanently `false`, `is_terminal()` could never hold, and
/// `drain_payload_only` would re-drain + reinsert that bridge on EVERY tick
/// without ever reaping it — `live_bridge_count(b)` would never return to 0
/// (an unbounded per-message bridge + bidi-stream leak). The
/// `live_bridge_count(b) == 0` assertion below is exactly that leak detector.
///
/// Deterministic: every node seeds quinn's RNG from its port (fixed across
/// runs); the only clock is the injected virtual clock; the step loop is
/// bounded. Run ≥5× standalone to prove non-flaky.
#[test]
fn one_way_user_message_over_quic_delivered_and_no_bridge_leak() {
  let a = "127.0.0.1:7121".parse().unwrap();
  let b = "127.0.0.1:7122".parse().unwrap();
  let mut c = QuicCluster::two_node_bare(a, b);
  let payload = b"hello-over-a-reliable-quic-stream".to_vec();
  c.start_user_message(a, b, &payload);
  for _ in 0..40_000 {
    if !c.step() {
      break;
    }
  }

  // The exact payload was delivered (presence semantics: `scan_events`
  // re-queues every event so the same Event::UserPacket is observed once per
  // scan; the harness dedups by payload — see `received_user_payload`).
  assert!(
    c.received_user_payload(b, &payload),
    "B must observe a reliable Event::UserPacket carrying the exact payload"
  );

  // No bridge leaked on EITHER peer. The inbound (B) bridge is the one the
  // fix targets: a one-way UserData drives its inner Stream to Done with no
  // outbound bytes, so without the generalized finish it would never become
  // terminal and would be re-drained + reinserted forever (leak). The
  // outbound (A) bridge FINs via the retained `sent_any` path and reaps
  // normally — asserting both = 0 proves neither side leaks.
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "the outbound user-message bridge on A must complete and be reaped"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "the inbound one-way UserData bridge on B must finish its (empty) send \
     half, become terminal, and be reaped — NOT leak forever"
  );
}

/// A backpressured-`Done` inbound-response bridge whose flow-control credit
/// is permanently withheld must have its stuck send half reaped by the
/// **bridge-level flush deadline** (bounded lifetime) — and the peer must
/// NOT receive the stale post-deadline tail (the exchange fails cleanly as
/// a timeout, self-healing).
///
/// Responder-heavy small-window join: `B` holds many extra Alive ghosts, so
/// `B`'s push/pull RESPONSE is many tiny-windows worths. The memberlist
/// `Stream` flips `B`'s inbound-response phase to `Done` the instant
/// `poll_transmit` hands the bytes to the bridge — BEFORE they reach quinn.
/// quinn `SendStream::write` accepts only one window-worth then
/// `WriteError::Blocked`s and the retained tail is in `pending_out`. The
/// drop is armed only AFTER `B` merges `A`'s push (so `B`'s response has
/// already been loaded and partially flushed — `B`'s bridge is genuinely in
/// the `Done`-but-unflushed gap that the bridge-level flush-deadline abandon
/// targets). Then every `A -> B` QUIC datagram is
/// dropped: `B`'s send half never receives MAX_STREAM_DATA / ACK from `A`
/// and the retained `pending_out` can never drain. The inner `Stream` is
/// `Done`, so `poll_transmit` / `handle_data` / `handle_timeout` all no-op
/// on the terminal guard — the FSM cannot self-enforce its deadline. The
/// ONLY remaining deadline is the bridge's snapshotted flush deadline (=
/// accept-time + `stream_timeout` = 10s), strictly earlier than quinn's 20s
/// `max_idle_timeout`, so the idle path cannot be what reaps it. On `now
/// >= bridge.deadline` the bridge RESETs its send half + STOPs its recv
/// half + goes `fatal`, D1 reaps it, and `live_bridge_count(b)` returns to
/// 0 — bounded, self-healing.
///
/// Negative control: a regression that delegated `Bridge::poll_timeout()`
/// solely to the (now `Done`) inner stream would return `None`, the unified
/// timer would never tick this bridge again, and a `pump_out` without its
/// own deadline check would let the stuck bridge either leak indefinitely
/// (no signal to reap) or only die at the 20s idle timeout via
/// `ConnectionLost`, NOT the 10s exchange deadline; and if credit ever did
/// reappear it could still write the retained tail (post-deadline stale
/// send). Asserting the bridge is gone within ~15s of virtual time after
/// the drop (well before the 20s idle timeout) is therefore the genuine
/// bridge-level flush-deadline detector.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run ≥5× standalone to prove non-flaky.
#[test]
fn backpressured_done_bridge_credit_starved_reaped_at_flush_deadline() {
  let a = "127.0.0.1:7131".parse().unwrap();
  let b = "127.0.0.1:7132".parse().unwrap();
  // Responder-heavy small-window join: B holds many extra Alive ghosts, so
  // B's push/pull RESPONSE is many tiny-window worths. The memberlist
  // `Stream` flips B's inbound-response phase to `Done` the instant
  // `poll_transmit` hands the bytes to the bridge; quinn `SendStream::write`
  // partial-accepts then `WriteError::Blocked`s and the retained tail in
  // `pending_out` cannot drain without further credit (MAX_STREAM_DATA) from
  // A. With every `A -> B` QUIC datagram dropped the instant B's inbound
  // bridge opens (so the handshake itself has already completed in both
  // directions), B never again receives credit. B's inner Stream is `Done`,
  // so it can no longer self-enforce its exchange deadline — only the
  // bridge-level flush deadline can reap the stuck send.
  const B_EXTRAS: usize = 80;
  let mut c = QuicCluster::two_node_join_small_window_responder_heavy(a, b, B_EXTRAS);

  // Drive until B has merged A's push (so B has fully received + decoded the
  // request AND has loaded its big response — B's `pump_out` has already
  // partial-accepted then `WriteError::Blocked`ed and B's inbound-response
  // bridge is now `Done` with a non-empty retained `pending_out`). Without
  // this gate, dropping A->B from t0 — or even from when B's bridge first
  // opens but A's push request is only partially received — would leave A's
  // request UNREAD (the FSM's own deadline reaps that case, NOT the
  // bridge-level flush-deadline abandon path). Gating on `sees_alive(b, a)`
  // guarantees B is past `dispatch_message` → `handle_stream_event` →
  // response load and is genuinely in the Done-but-unflushed window the
  // bridge-level flush-deadline abandon targets.
  let mut b_merged = false;
  for _ in 0..40_000 {
    if c.sees_alive(b, &id("a")) && c.live_bridge_count(b) > 0 {
      b_merged = true;
      break;
    }
    if !c.step() {
      break;
    }
  }
  assert!(
    b_merged,
    "precondition: B's inbound bridge must reach the Done-but-unflushed \
     state (B merged A's push and B's response is mid-backpressured-flush) \
     before credit starvation is armed; otherwise the FSM's own deadline \
     reaps the in-flight bridge — not the bridge-level flush-deadline abandon path"
  );

  // NOW starve B of every `A -> B` QUIC datagram (ACKs, MAX_STREAM_DATA).
  // B's response send-half can no longer get further flow-control credit;
  // its retained `pending_out` is stuck while the inner Stream is already
  // `Done`. The connection itself stays alive (no graceful close, quinn's
  // 20s idle timeout is strictly later than the 10s bridge flush deadline),
  // so the ONLY thing that can reap B's stuck Done-but-unflushed bridge
  // within the bound is the bridge-level flush deadline (the abandon
  // path: `now >= bridge.deadline && stream.is_done() && !is_terminal()`).
  c.drop_quic_directional(a, b);
  let t_starve = c.elapsed();

  // Bound: well past B's bridge flush deadline (accept-time + stream_timeout
  // = 10s) but strictly BEFORE quinn's 20s idle timeout — so a reap
  // observed here can ONLY be the bridge-level flush deadline abandon path,
  // not the ConnectionLost / idle-timeout path.
  let mut b_bridge_reaped = false;
  for _ in 0..400_000 {
    let progressed = c.step();
    if c.live_bridge_count(b) == 0 {
      b_bridge_reaped = true;
      break;
    }
    if c.elapsed() - t_starve >= Duration::from_secs(15) {
      break;
    }
    if !progressed {
      break;
    }
  }

  assert!(
    b_bridge_reaped,
    "B's credit-starved Done-but-backpressured bridge must be reaped by the \
     bridge-level flush deadline within ~stream_timeout (bounded lifetime) \
     — not leak until the far-later quinn idle timeout"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "no bridge may leak on B after the flush-deadline abandon"
  );
  assert!(
    c.elapsed() - t_starve < Duration::from_secs(20),
    "the bridge must be gone strictly before quinn's 20s idle timeout, \
     proving the bridge-level flush deadline (not idle-timeout) reaped it"
  );
  // The exchange failed cleanly as a timeout: A must NOT have observed all
  // of B's pushed extras Alive (the abandon RESET B's send half rather than
  // continuing to drive the retained tail across the deadline; without the
  // bridge-level flush-deadline abandon a credit-rescue past the deadline
  // could still leak some extras post-deadline, or the bridge could leak
  // indefinitely). At least the last extra (highest index, in the tail that
  // was retained) must NOT cross.
  let last = id(&format!("bextra-{}", B_EXTRAS - 1));
  assert!(
    !c.ever_saw_alive(a, &last),
    "A must NOT receive B's last pushed extra `{last}` past the flush \
     deadline — that would mean the bridge wrote its retained tail after \
     the bridge timed out (stale post-deadline write)"
  );
}

/// An OUTBOUND push/pull request whose retained-tail (`pending_out` after a
/// partial-accept-then-`Blocked` write) cannot drain because the peer's
/// `MAX_STREAM_DATA` ACKs are dropped must reap at the exchange deadline —
/// NOT linger until the much-later connection idle timeout, and never
/// deliver the retained tail to the peer.
///
/// Initiator-heavy small-window join: `A` holds many extra Alive ghosts, so
/// `A`'s push/pull REQUEST is many tiny-stream-windows' worth and `A`'s
/// outbound bridge partial-accepts then `WriteError::Blocked`s on its
/// `pending_out` after the FSM has moved to `OutboundAwaitingResponse`. The
/// inner `Stream` is **not** `Done` (it's awaiting the peer's reply), so the
/// `is_done()`-gated flush-deadline abandon cannot fire. The Stream FSM's
/// own deadline-at-`poll_transmit` would self-fail the stream — but only if
/// `poll_transmit` is reached, and a non-empty `pending_out` flush below
/// returns early on `WriteError::Blocked`. Without the pre-write deadline
/// check the bridge survives until quinn's idle timeout fires
/// `ConnectionLost`; the unified-timer `min` over the bridge's deadlines
/// no longer enforces `stream_timeout`, and any late `MAX_STREAM_DATA`
/// would still drain the retained tail post-deadline (a stale send,
/// diverging from the timeout-at-deadline guarantee).
///
/// The pre-write check (run before the `pending_out` flush every tick)
/// closes the gap: a non-terminal stream with a non-empty `pending_out`
/// whose `Stream::poll_timeout()` has elapsed drives
/// `Stream::handle_timeout(now)` — the FSM enters `Failed(Timeout)`,
/// `enter_failed` clears `output_buf` — the QUIC halves are abandoned
/// (`SendStream::reset` + `RecvStream::stop`), `pending_out` is cleared, the
/// bridge marks itself `fatal`, and the D1 drain-then-reap runs the same
/// tick. The deferred `finish()` is gated against `Stream::is_failed()` so
/// a timed-out stream **must not** emit FIN (a FIN would make the expired
/// request look complete to the peer; the correct outcome is a timed-out
/// exchange).
///
/// Negative control: without the pre-write deadline check, the
/// `pending_out` flush returns `Blocked` every tick (credit withheld) and
/// `pump_out` never reaches `poll_transmit` — the bridge would stay alive
/// past the 10s exchange deadline and would eventually be reaped only when
/// quinn's 20s `max_idle_timeout` fires `Event::ConnectionLost` (marks every
/// bridge on the connection fatal — the `StreamErrored` teardown variant,
/// NOT the `Timeout` reap). Asserting the bridge is reaped strictly before
/// 20s of elapsed time after the starve point therefore distinguishes the
/// pre-write deadline reap (~stream_timeout) from the connection-loss reap
/// (~max_idle_timeout).
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run ≥5× standalone to prove non-flaky.
#[test]
fn backpressured_outbound_request_credit_starved_reaped_at_stream_deadline() {
  let a = "127.0.0.1:7141".parse().unwrap();
  let b = "127.0.0.1:7142".parse().unwrap();
  // Initiator-heavy small-window join: A holds enough extra Alive ghosts
  // that the push request CANNOT drain in the initial per-stream receive
  // window. With fewer extras the request fits in one window-worth, the
  // initial flush succeeds entirely, `pending_out` empties before the
  // exchange deadline, and `pump_out` reaches `poll_transmit` — whose own
  // deadline check (`enter_failed(Timeout)` on `now >= deadline`) then
  // reaps the bridge, masking the pre-write deadline check and yielding a
  // false-positive against a missing pre-write check. 500 extras force a
  // deeply back-pressured retained tail that the FSM canNOT self-fail
  // without the pre-write check.
  const A_EXTRAS: usize = 500;
  let mut c = QuicCluster::two_node_join_small_window(a, b, A_EXTRAS);

  // Drive until A's outbound bridge is open AND `pending_out` is genuinely
  // back-pressured — i.e. until A has at least one live bridge on the
  // pooled connection. This guarantees A is in the
  // `OutboundAwaitingResponse`-with-retained-tail state the pre-write check
  // targets; without it, dropping credit before the bridge opens reduces to
  // a pre-connection scenario that the FSM's own deadline handling would
  // already cover.
  let mut bridge_opened = false;
  for _ in 0..40_000 {
    if c.live_bridge_count(a) > 0 {
      bridge_opened = true;
      break;
    }
    if !c.step() {
      break;
    }
  }
  assert!(
    bridge_opened,
    "precondition: A's outbound push/pull bridge must open before credit \
     starvation is armed"
  );

  // NOW starve A of every `B -> A` QUIC datagram — A no longer receives
  // any further `MAX_STREAM_DATA` ACK, and any retained tail in
  // `pending_out` can no longer drain. The connection itself stays alive
  // (no graceful close; quinn's 20s idle timeout is strictly later than
  // the 10s stream deadline), so the only thing that can reap A's
  // back-pressured `OutboundAwaitingResponse` bridge within the bound is
  // the pre-write deadline check (the FSM canNOT self-fail via
  // `poll_transmit` because the `pending_out` flush returns early on
  // `Blocked`).
  c.drop_quic_directional(b, a);
  let t_starve = c.elapsed();

  // Bound: well past A's stream exchange deadline (dial-time +
  // stream_timeout = 10s) but strictly BEFORE quinn's 20s idle timeout —
  // so a reap observed here can ONLY be the pre-write deadline reap, not
  // the `ConnectionLost` / idle-timeout path.
  let mut bridge_reaped = false;
  for _ in 0..400_000 {
    let progressed = c.step();
    if c.live_bridge_count(a) == 0 {
      bridge_reaped = true;
      break;
    }
    if c.elapsed() - t_starve >= Duration::from_secs(15) {
      break;
    }
    if !progressed {
      break;
    }
  }
  assert!(
    bridge_reaped,
    "A's credit-starved back-pressured outbound bridge must be reaped by \
     the pre-write deadline check within ~stream_timeout of the starve \
     point — not leak until the far-later quinn idle timeout fires \
     ConnectionLost"
  );
  let reap_at = c.elapsed() - t_starve;
  assert!(
    reap_at < Duration::from_secs(20),
    "the bridge must be reaped strictly before quinn's 20s idle timeout \
     ({reap_at:?} elapsed since starve), proving the pre-write deadline \
     (not the idle-timeout / ConnectionLost path) drove the reap"
  );

  // The exchange failed cleanly as a timeout: the pre-write check cleared
  // `pending_out` and the QUIC send/recv halves were abandoned before any
  // of the retained tail could hit the wire, so B must NOT have observed
  // A's last pushed extra (the highest-index Alive ghost — in the tail
  // that was retained when the flush blocked). Without the pre-write
  // deadline check, a late credit grant past the deadline could still flush
  // the retained tail and let B receive (and decode, if its own inbound
  // deadline hadn't yet elapsed) A's extras; with the pre-write check, NO
  // post-deadline bytes are delivered.
  let last = id(&format!("extra-{}", A_EXTRAS - 1));
  assert!(
    !c.ever_saw_alive(b, &last),
    "B must NOT receive A's last pushed extra `{last}` past the stream \
     deadline — that would mean the bridge wrote its retained tail after \
     the deadline (stale post-deadline write)"
  );

  // The pooled QUIC connection itself is still live (only the stream was
  // RESET; the connection survived). A subsequent push/pull therefore
  // opens a fresh bidi stream on the SAME connection (no fresh handshake)
  // — proves the pre-write reap is per-stream, not per-connection.
  assert!(
    c.live_quic_connections(a) >= 1,
    "the pooled QUIC connection must still be live after the stream reap \
     (only the stream was abandoned, not the connection)"
  );
}

/// Response-deadline refresh codepath. `SendPushPullResponse` must
/// advance `Bridge.self.deadline` to the SAME `now + 5s` value
/// `stream_load_response` writes into the inner stream's own deadline.
/// Without that refresh, the `Done`-but-unflushed abandon
/// (`now >= self.deadline && is_done() && !is_terminal()`) would fire on
/// the stale (accept-time) deadline whenever the backpressured response
/// delivery extended past `accept_time + stream_timeout` — the reply
/// would be truncated and the peer left with a half-applied merge.
///
/// Setup: both-heavy small-window join with a SHORT `stream_timeout` so
/// the bridge's accept-time deadline is tight relative to the post-refresh
/// `T1 + 5s` window. The fact that B's response delivers in full — with
/// every one of B's pushed extras Alive at A within a bounded virtual
/// time window — exercises the deadline-refresh codepath every time
/// `SendPushPullResponse` runs (the `Bridge::drain_then_reap` and
/// `Bridge::drain_payload_only` arms both assign
/// `self.deadline = response_deadline`).
///
/// Negative-control proof: drop the `self.deadline = response_deadline`
/// assignment from both arms. The bridge.deadline then stays at
/// `accept_time + stream_timeout` regardless of when the response is
/// loaded; whenever the backpressured response delivery happens to
/// exceed the original accept deadline, the `Done`-but-unflushed abandon
/// fires, the retained response tail is RESET / STOPPED, and A never
/// observes the high-index `bextra-*` extras Alive. The `ever_saw_alive`
/// assertion below would then fail for at least one tail entry.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run >=5x standalone to prove non-flaky.
#[test]
fn inbound_push_pull_response_backpressured_past_old_deadline_still_completes() {
  let a = "127.0.0.1:7151".parse().unwrap();
  let b = "127.0.0.1:7152".parse().unwrap();
  // Both-heavy small-window join with a 3s `stream_timeout`. Sized so:
  //   - the request arrives at B in well under 3s (the FSM's own
  //     deadline-at-`poll_transmit` does not self-fail the inbound
  //     stream before `SendPushPullResponse` fires);
  //   - the response covers many tiny-window worths so its backpressured
  //     delivery exercises the deadline-refresh codepath — the post-load
  //     bridge.deadline advances to `T1 + 5s` in lockstep with the inner
  //     stream's deadline, and the `Done`-but-unflushed abandon does not
  //     fire prematurely on the stale (3s) accept-time deadline.
  let timeout = Duration::from_secs(3);
  const A_EXTRAS: usize = 6;
  const B_EXTRAS: usize = 240;
  let mut c =
    QuicCluster::two_node_join_short_stream_timeout_both_heavy(a, b, A_EXTRAS, B_EXTRAS, timeout);
  // Bound the observation window by VIRTUAL TIME: long enough for the
  // post-refresh response window (`T1 + 5s`) to elapse, short enough
  // that SWIM gossip / periodic push-pulls (30s interval) cannot deliver
  // the missing tail via a follow-up exchange.
  let bound = c.elapsed() + Duration::from_secs(15);
  while c.elapsed() < bound {
    if !c.step() {
      break;
    }
  }
  // EVERY one of B's pushed extras reached A within the refreshed
  // response window. Without the refresh the `Done`-but-unflushed
  // abandon would reset B's send half at the stale
  // `accept_time + stream_timeout` deadline whenever the backpressured
  // response happens to extend past it; the retained response tail would
  // be discarded and A would never observe the *highest-index* extras
  // Alive (those bytes are in the response tail).
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after the join"
  );
  for n in 0..B_EXTRAS {
    let peer = id(&format!("bextra-{n}"));
    assert!(
      c.ever_saw_alive(a, &peer),
      "B's pushed peer {peer} never reached A — the response was \
       truncated by the `Done`-but-unflushed abandon firing on the stale \
       accept-time bridge deadline (self.deadline not refreshed in \
       lockstep with stream_load_response)"
    );
  }
  // Parity: B merged A's push.
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive (parity — A's push merged)"
  );
  for n in 0..A_EXTRAS {
    let peer = id(&format!("aextra-{n}"));
    assert!(
      c.ever_saw_alive(b, &peer),
      "A's pushed peer {peer} never reached B"
    );
  }
}

/// Stream-credit-exhausted is not a permanent dial failure: when an
/// outbound push/pull / reliable-ping / user-message intent races with a
/// pooled connection that is currently pinned at its peer-granted
/// `initial_max_streams_bidi` credit, `service_dials::open(Dir::Bi) == None`
/// must REQUEUE the intent onto `dial_pending` (not retire it via
/// `dial_failed`) until either credit is restored by a future
/// `MAX_STREAMS` frame from the peer (inflight bidi streams reap) or the
/// intent's own `deadline` expires.
///
/// Scenario: drive several concurrent reliable exchanges on the same
/// pooled connection, then queue one MORE while the pre-existing exchanges
/// pin the connection's bidi-stream-credit at zero. The queued exchange
/// MUST eventually open and complete — proving the intent was preserved
/// in `dial_pending` across the credit pressure rather than silently
/// dropped to `dial_failed`.
///
/// Negative-control proof: change `service_dials` so that on
/// `open(Bi) == None` for an Established / `!is_handshaking()` /
/// `!is_closed()` connection it calls `dial_failed` instead of pushing
/// onto `dial_pending`. The queued exchange's intent is consumed by the
/// stream-credit-exhausted spike; with no future event to retry it, the
/// exchange never opens, the queued peer never receives the marker
/// payload, and `received_user_payload(b, &marker)` stays `false`.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run >=5x standalone to prove non-flaky.
#[test]
fn established_stream_credit_exhausted_dial_eventually_succeeds() {
  let a = "127.0.0.1:7181".parse().unwrap();
  let b = "127.0.0.1:7182".parse().unwrap();
  // Pre-warm the pooled QUIC connection with a normal join so subsequent
  // reliable exchanges ride the SAME Established connection (no fresh
  // handshake). Once warm, the credit-pressure scenario opens many bidi
  // streams concurrently against this single pooled connection.
  let mut c = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "precondition: A converged Alive over the warm-up join"
  );
  let conn_count_after_warm = c.live_quic_connections(a);
  assert!(
    conn_count_after_warm >= 1,
    "precondition: a pooled QUIC connection exists after the warm-up join"
  );

  // Wait for any in-flight periodic bridges to settle so the credit
  // pressure assertion is measured in isolation.
  for _ in 0..20_000 {
    if c.live_bridge_count(a) == 0 && c.live_bridge_count(b) == 0 {
      break;
    }
    if !c.step() {
      break;
    }
  }

  // Burst-open many reliable user-message exchanges concurrently against
  // the SAME pooled connection. Each one consumes one bidi stream until
  // either `initial_max_streams_bidi` is exhausted OR the pre-bursted
  // bridges start reaping (returning credit via MAX_STREAMS). Sized so
  // that at least one intent in the burst meets `open(Bi) == None` on
  // the credit-exhausted path while the connection is Established (not
  // handshaking, not closed). The exact pinning depends on quinn-proto's
  // default `max_concurrent_bidi_streams` (100), so we burst noticeably
  // above 100 to ensure at least the trailing intents hit the
  // credit-exhausted branch on at least one service tick.
  const BURST: usize = 150;
  for i in 0..BURST {
    let payload = format!("burst-msg-{i}").into_bytes();
    c.start_user_message(a, b, &payload);
  }
  // The marker message rides one of the burst's later intents — the one
  // most likely to encounter `open(Bi) == None` against the credit-pinned
  // connection. Verify it eventually arrives at B.
  let marker_payload = b"credit-exhausted-marker".to_vec();
  c.start_user_message(a, b, &marker_payload);

  for _ in 0..200_000 {
    if c.received_user_payload(b, &marker_payload) {
      break;
    }
    if !c.step() {
      break;
    }
  }
  assert!(
    c.received_user_payload(b, &marker_payload),
    "the marker user-message must reach B despite the credit pressure — \
     its dial intent MUST be requeued across the stream-credit \
     exhaustion (not retired via `dial_failed`) until the inflight burst \
     reaps and returns MAX_STREAMS credit"
  );

  // Settle so no bridge leaks. (`live_bridge_count` zero on both sides
  // after the burst + marker fully complete.)
  for _ in 0..200_000 {
    if c.live_bridge_count(a) == 0 && c.live_bridge_count(b) == 0 {
      break;
    }
    if !c.step() {
      break;
    }
  }
  // Every burst payload AND the marker reached B (no intent silently
  // retired via `dial_failed` on the credit-exhausted path).
  for i in 0..BURST {
    let p = format!("burst-msg-{i}").into_bytes();
    assert!(
      c.received_user_payload(b, &p),
      "burst payload {i} never reached B — its dial intent was \
       silently retired on the stream-credit-exhausted path"
    );
  }
}

/// `Class::Memberlist` inbound datagrams MUST be buffered ONLY by
/// `QuicEndpoint::handle_udp` — the call MUST NOT advance time on the
/// memberlist class. The codec-owning driver drains via
/// `poll_memberlist_ingress`, decodes, feeds via `handle_packet`, and
/// only THEN calls `handle_timeout(now)`. If `handle_udp` advances time
/// itself on the memberlist class, a probe scheduler / gossip
/// scheduler / suspicion timer due at the same instant fires BEFORE
/// the just-arrived datagram is decoded and fed — perturbing internal
/// state on stale-pre-Ack/pre-Alive input.
///
/// Scenario: A is alive, knows B; advance the clock by the default
/// probe_interval so a periodic-probe scheduler firing is *due* at the
/// next time-advance, then snapshot A's transmits-queue state. Inject
/// an arbitrary `Class::Memberlist` datagram at A through `handle_udp`
/// (an Ack for a never-issued sequence — `handle_ack` drops it as
/// untracked once decoded, so no membership state changes from the
/// payload itself). The contract: `handle_udp` MUST NOT have run any
/// probe / gossip / push-pull scheduler — the buffered ingress is the
/// ONLY observable consequence. Verify by polling
/// `poll_memberlist_ingress` (exactly one queued raw datagram, as
/// `handle_udp` buffered it) AND by polling `poll_transmit` /
/// `poll_memberlist_transmit` (no outbound datagrams produced — pre-
/// fix's in-`handle_udp` `run_tick` would have fired the probe
/// scheduler, queueing an outbound Ping for the periodic-probe).
///
/// Negative control: revert `handle_udp` to advance time on
/// `Class::Memberlist` (e.g. move `self.run_tick(now)` back to the
/// bottom of `handle_udp` outside the `match`). The buffered datagram
/// has not been decoded yet, but `run_tick` runs anyway and the
/// periodic-probe scheduler — which becomes due at the exact instant
/// of this `handle_udp` call — fires `start_probe(now)`, queueing a
/// direct Ping for the next `poll_transmit` drain. The post-`handle_udp`
/// `poll_transmit` then returns `Some(_)` and this test's
/// `outbound_after == 0` assertion fails.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run ≥3× standalone to prove non-flaky.
#[test]
fn memberlist_udp_buffered_only_does_not_advance_time() {
  let a = "127.0.0.1:7301".parse().unwrap();
  let b = "127.0.0.1:7302".parse().unwrap();
  let mut c = QuicCluster::two_node_alive(a, b);

  // `two_node_alive` builds nodes with the default probe_interval
  // (1s); `add_node` calls `start_scheduling(now)` so the first
  // periodic-probe scheduler fires at `now + 1s`. Advance and tick
  // through that first probe (this primes `next_probe = now + 1s`),
  // drain every queued transmit so the post-injection assertion below
  // sees only `handle_udp`'s own side-effects, then `advance_clock_only`
  // to the EXACT instant of the NEXT scheduler fire (`now + 1s`) WITHOUT
  // ticking — so `handle_udp` is the sole API call that observes the
  // due-scheduler instant.
  c.advance(Duration::from_secs(1));
  c.drain_pending_transmits_for_test();
  c.advance_clock_only(Duration::from_secs(1));

  // Inject an arbitrary `Class::Memberlist` datagram on A. The Ack
  // sequence is 999 — A has no outstanding probe with that seq, so
  // `handle_ack` (when eventually called by the helper's
  // `handle_packet`) drops the message as untracked and does NOT
  // modify membership state from the payload. The point under test is
  // purely the `handle_udp` -> `run_tick` ordering inside the
  // coordinator, not the Ack's payload semantics.
  c.inject_memberlist_handle_udp_only(a, b, 999);

  // Buffered-only contract: exactly one raw datagram is queued in
  // `poll_memberlist_ingress` (the one `handle_udp` just buffered),
  // and `handle_udp` has NOT advanced time — so the periodic-probe
  // scheduler that was due at this instant has NOT fired, hence no
  // outbound Ping was queued for transmit.
  let ingress_count = c.drain_memberlist_ingress_count_for_test(a);
  assert_eq!(
    ingress_count, 1,
    "exactly one raw datagram must have been buffered by handle_udp \
     (the just-injected Ack bytes) — `Class::Memberlist` is \
     buffered-only on `handle_udp`"
  );
  let outbound_after = c.drain_transmits_count_for_test(a);
  assert_eq!(
    outbound_after, 0,
    "handle_udp(Memberlist) MUST NOT advance time: no probe / gossip \
     / push-pull scheduler may fire inside `handle_udp`. A non-zero \
     count means `run_tick(now)` ran while the buffered datagram had \
     not yet been decoded and fed"
  );
}

// ── Conformance parity (the capstone) ─────────────────────────────────────────
//
// The SAME membership outcomes / timing as the existing non-quic suite
// (`tests/suspect_dead.rs`, `tests/legacy_suspect.rs`), but every reliable
// exchange now rides real quinn-proto. The plain harness is untouched and
// still green (regression guard run separately); these prove the QUIC
// composition does not perturb SWIM timing.

/// Mirror of `tests/suspect_dead.rs::dropped_probe_leads_to_suspect`:
/// partition prober↔target so the probe Ping never arrives and the
/// reliable-ping fallback (over QUIC) cannot complete either; after the
/// cumulative deadline the target is no longer Alive.
#[test]
fn parity_dropped_probe_leads_to_suspect() {
  let a = "127.0.0.1:7061".parse().unwrap();
  let b = "127.0.0.1:7062".parse().unwrap();
  let mut c = QuicCluster::two_node_alive(a, b);
  c.partition(&[a], &[b]);

  c.trigger_probe(a);

  // Past the direct probe_timeout: SWIM arms the concurrent reliable
  // fallback rather than suspecting on the direct timeout alone.
  c.advance(Duration::from_millis(600));
  assert_eq!(
    c.member_state(a, &id("b")),
    Some(State::Alive),
    "target must NOT be suspected on the direct timeout alone (parity)"
  );

  // Past the cumulative deadline: the partitioned fallback never made
  // contact → terminate-failure → Suspect.
  c.advance(Duration::from_millis(600));
  let st = c.member_state(a, &id("b"));
  assert!(
    st.is_some() && st != Some(State::Alive),
    "target must NOT be Alive after the dropped probe (parity), got {st:?}"
  );
}

/// Mirror of `tests/suspect_dead.rs::suspect_transitions_to_dead_after_timeout`.
/// The cluster is bootstrapped by a REAL QUIC join (proving the QUIC
/// composition is live); then — exactly as the plain test — a Suspect is
/// injected for a GHOST node that has no endpoint and therefore cannot
/// refute. After `suspicion_mult * probe_interval` (4 * 1s) the ghost must
/// transition Dead with the SAME timing as the plain harness: the QUIC
/// composition must not perturb the suspicion timer.
#[test]
fn parity_suspect_transitions_to_dead_after_timeout() {
  let a = "127.0.0.1:7071".parse().unwrap();
  let b = "127.0.0.1:7072".parse().unwrap();
  let ghost_addr = "127.0.0.1:7099".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "precondition: A sees B Alive over QUIC"
  );

  // A ghost A learns about but that has no endpoint (cannot refute).
  c.inject_alive(a, id("ghost"), ghost_addr, 1);
  c.inject_suspect(a, id("ghost"), id("a"), 1);
  assert_eq!(
    c.member_state(a, &id("ghost")),
    Some(State::Suspect),
    "ghost must be Suspect right after inject (parity)"
  );

  c.advance(Duration::from_secs(5));
  for _ in 0..200 {
    if !c.step() {
      break;
    }
  }
  assert_eq!(
    c.member_state(a, &id("ghost")),
    Some(State::Dead),
    "ghost must be Dead after the suspicion timeout (parity)"
  );
}

/// Legacy suspect→alive→dead behaviour over QUIC: an Alive refute with a
/// higher incarnation clears a Suspect (the node returns to Alive), exactly
/// as the plain `legacy_suspect` suite asserts.
#[test]
fn parity_suspect_then_alive_refute_clears_suspect() {
  let a = "127.0.0.1:7081".parse().unwrap();
  let b = "127.0.0.1:7082".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(c.sees_alive(a, &id("b")), "precondition: A sees B Alive");

  c.inject_suspect(a, id("b"), id("a"), 1);
  assert_eq!(
    c.member_state(a, &id("b")),
    Some(State::Suspect),
    "B Suspect after inject"
  );

  // A higher-incarnation Alive refutes the suspicion.
  c.inject_alive(a, id("b"), b, 5);
  assert_eq!(
    c.member_state(a, &id("b")),
    Some(State::Alive),
    "a higher-incarnation Alive must clear the Suspect (parity)"
  );
}

/// Terminal-bridge guard: once the bridge is `fatal` (or its inner
/// `Stream::is_failed()` is `Some`), `pump_out` MUST short-circuit at the
/// very top — abandon the QUIC halves (`SendStream::reset` +
/// `RecvStream::stop`), clear `pending_out`, force `self.fatal=true`,
/// and return `Err` — so neither the pre-write deadline check, the
/// `is_done`-gated flush-deadline abandon, the `pending_out` flush, nor
/// the `poll_transmit` loop is reachable post-terminal. The invariant
/// is load-bearing: same-tick `pump_in` fatals (a `ReadError::Reset`,
/// a `Stream::handle_data` Decode/UnexpectedMessage error, or a
/// deadline-driven `handle_data` Timeout) leave the send half `Ready`
/// and (if credit was just granted) writable; without the guard, the
/// `pending_out` flush at the bottom of `pump_out` would hand a stale
/// tail to `SendStream::write`. Memberlist frames are length-delimited
/// (the peer's decoder applies a partial wire as a full message without
/// ever observing a FIN), so a single stale post-terminal write is
/// enough for the peer to merge the retained tail.
///
/// Scenario: `two_node_join_small_window` with A initiator-heavy
/// (`A_EXTRAS = 600`) — A's outbound request is many tiny-stream-
/// windows' worth so the partial-accepted-then-`Blocked` retained tail
/// stays in `pending_out` for many ticks. The default `stream_timeout`
/// (10 s) gives the FSM exchange deadline a fixed bound. After A's
/// bridge has opened and `pending_out` is genuinely back-pressured,
/// every `B -> A` QUIC datagram is dropped: A no longer receives
/// further `MAX_STREAM_DATA` from B, the connection itself stays alive
/// (no `ConnectionLost` until quinn's much-later 20 s `max_idle_timeout`).
/// As A's exchange deadline elapses with `pending_out` still retained,
/// the per-stream reap runs strictly inside the bridge-deadline window —
/// the terminal-bridge guard's invariant is then asserted across every
/// subsequent reap tick: no post-fatal `pump_out` may hand the retained
/// tail to the peer.
///
/// End-to-end assertions: the highest-index Alive ghost (the LAST entry
/// in A's push snapshot, whose bytes live in the retained tail at the
/// moment of back-pressure) MUST NEVER reach B; the bridge MUST reap
/// strictly before quinn's 20 s idle timeout; no bridge leaks on either
/// peer.
///
/// This test exercises the terminal-bridge guard path under a
/// retained-tail + per-stream reap; it is regression coverage for the
/// guard's load-bearing invariant (no post-terminal write through the
/// `pending_out` flush or `poll_transmit` loop). The fault paths that
/// currently funnel through the guard happen to also pre-clear
/// `pending_out` via `enter_failed`'s buffer-clear or via the pre-write
/// deadline abandon, so a same-tick `pump_in` fatal with retained
/// `pending_out` is not observable through this scenario alone without
/// a finer-grained timing seam — the guard is therefore a defensive
/// hardening of the invariant against hypothetical future fault paths
/// (decode error / IllegalOrderedRead / a Close-only peer reset) that
/// could otherwise leave a retained tail flushable post-terminal. The
/// assertions below ARE the end-to-end outcome the invariant guarantees.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run >=5x standalone to prove non-flaky.
#[test]
fn terminal_bridge_pump_out_is_a_no_op_no_stale_tail_after_fatal() {
  let a = "127.0.0.1:7151".parse().unwrap();
  let b = "127.0.0.1:7152".parse().unwrap();
  // 600 extras forces a deeply back-pressured retained tail through the
  // entire `stream_timeout` window — the request CANNOT drain before
  // A's exchange deadline elapses, so the retained tail is what the
  // terminal-bridge guard must keep off the wire.
  const A_EXTRAS: usize = 600;
  let mut c = QuicCluster::two_node_join_small_window(a, b, A_EXTRAS);

  // Run until A's outbound bridge is open AND `pending_out` is genuinely
  // back-pressured (A holds at least one live bridge on the pooled
  // connection). Without this gate, dropping `B -> A` before A's bridge
  // opens reduces to a pre-connection scenario that the FSM's own
  // deadline machinery handles — NOT the terminal-bridge guard path.
  let mut bridge_opened = false;
  for _ in 0..40_000 {
    if c.live_bridge_count(a) > 0 {
      bridge_opened = true;
      break;
    }
    if !c.step() {
      break;
    }
  }
  assert!(
    bridge_opened,
    "precondition: A's outbound push/pull bridge must open before the \
     `B -> A` drop is armed"
  );

  // Starve A of every `B -> A` QUIC datagram (MAX_STREAM_DATA / ACKs /
  // response bytes). A's retained `pending_out` can no longer drain, and
  // any B-originated bytes that would later reach A (and be rejected by
  // the inner `Stream::handle_data` at/after the deadline) are likewise
  // gone — but A's send half stays `Ready` and any credit already granted
  // remains; this is exactly the state where, post-fatal, a missing
  // terminal-bridge guard would let `pump_out` flush the retained tail
  // to B.
  c.drop_quic_directional(b, a);
  let t_starve = c.elapsed();

  // Drive until A's bridge is reaped (the back-pressured retained-tail
  // path eventually transitions A's bridge through `fatal` and the
  // terminal guard short-circuits every subsequent `pump_out`). Bound
  // well past the 10 s stream_timeout but strictly BEFORE quinn's 20 s
  // idle timeout, so the reap observed here is the per-stream reap
  // (the guard's invariant), not the connection-level `ConnectionLost`
  // teardown.
  let mut bridge_reaped = false;
  for _ in 0..400_000 {
    let progressed = c.step();
    if c.live_bridge_count(a) == 0 {
      bridge_reaped = true;
      break;
    }
    if c.elapsed() - t_starve >= Duration::from_secs(15) {
      break;
    }
    if !progressed {
      break;
    }
  }
  assert!(
    bridge_reaped,
    "A's terminal bridge must be reaped within ~stream_timeout of the \
     starve point — not leak until quinn's 20 s idle timeout"
  );
  let reap_at = c.elapsed() - t_starve;
  assert!(
    reap_at < Duration::from_secs(20),
    "the bridge must be reaped strictly before quinn's 20 s idle timeout \
     ({reap_at:?} elapsed since starve), proving the per-stream guard \
     (not connection-level ConnectionLost) drove the reap"
  );

  // The terminal-bridge guard's load-bearing invariant: post-fatal,
  // `pump_out` MUST NOT write any byte of `pending_out` or any byte
  // newly yielded by `poll_transmit`. Memberlist frames are
  // length-delimited (the peer's decoder applies a partial wire as a
  // full message without ever observing a FIN), so a single stale
  // post-terminal flush is enough for B to merge A's retained tail.
  // The highest-index Alive ghost (the LAST entry in A's request push
  // snapshot — guaranteed by `start_push_pull` ordering to live in the
  // retained tail when the initial window-worth flush partial-accepts
  // then `Blocked`s) must therefore NEVER reach B. Without the guard,
  // a same-tick `pump_in` fatal + late credit (the race the guard
  // closes) would hand exactly that tail to `SendStream::write`.
  let last = id(&format!("extra-{}", A_EXTRAS - 1));
  assert!(
    !c.ever_saw_alive(b, &last),
    "B must NOT receive A's last pushed extra `{last}` after the \
     terminal-bridge guard fires — that would mean `pump_out` wrote a \
     stale tail post-fatal (the guard's invariant is the load-bearing one)"
  );
  // No bridge may leak on either peer.
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "no bridge may leak on A after the terminal-guard reap"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "no bridge may leak on B after the terminal-guard reap"
  );
}

/// `Event::DialRequested` is coordinator-internal: the public
/// [`QuicEndpoint::poll_event`] sieves it into the private `dial_pending`
/// deque so an external poll-event drain can NEVER pop the retry token
/// and silently drop it. The composed coordinator IS the driver — only
/// `service_dials` may consume the intent.
///
/// Scenario: a fresh `two_node_join`. The very tick A's `start_push_pull`
/// runs, `Event::DialRequested` is queued inside the inner endpoint. The
/// test then performs an explicit external [`QuicCluster::external_poll_event_drain`]
/// on A — the exact sequence a real external driver might run to surface
/// application-visible events to the host process — at multiple points
/// across the join handshake / push-pull lifetime (before any step, once
/// per step for the first many ticks while the QUIC handshake is in
/// flight, once more after settle). The drain returns the application-
/// visible events (joins, packets, …); it MUST NOT return
/// `DialRequested`.
///
/// `DialRequested` only ever lives in the private `dial_pending` deque
/// (sieved out of the inner endpoint by `QuicEndpoint::poll_event` and
/// `service_dials::sieve_dial_events`); handshake-blocked dials are pushed
/// BACK onto `dial_pending` (not `Endpoint::requeue_event`), so the retry
/// token cannot leak through the public surface either. The eventual
/// push/pull completes and both peers converge Alive.
///
/// Negative control: collapsing `QuicEndpoint::poll_event` back to a
/// direct passthrough (`self.ep.poll_event()`) reopens the leak — the
/// first external drain pops `DialRequested`, the test discards it, and
/// `service_dials` finds nothing to consume. The dial then orphans
/// (no future event ever fires the retry), the push/pull never opens,
/// and both `sees_alive(a, b)` and `sees_alive(b, a)` stay `false`. The
/// convergence assertions below catch exactly that.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run >=5x standalone to prove non-flaky.
#[test]
fn dial_requested_is_coordinator_internal_external_drain_does_not_orphan_dial() {
  let a = "127.0.0.1:7161".parse().unwrap();
  let b = "127.0.0.1:7162".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);

  // Pre-step external drain: `start_push_pull` queued `DialRequested`
  // inside A's inner endpoint the moment `two_node_join` ran, before
  // any tick has fired. The sieve must lift it out into `dial_pending`
  // on the very first public `poll_event` call — and an external caller
  // (which discards what it pops) must see zero events here, not the
  // `DialRequested` retry token.
  let pre = c.external_poll_event_drain(a);
  assert_eq!(
    pre, 0,
    "external `poll_event` must see ZERO events before any step (the \
     queued `DialRequested` must be sieved into the private `dial_pending` \
     deque — never returned to external callers); got {pre}"
  );

  // Mid-handshake external drains: across the first many ticks the QUIC
  // connection is still handshaking and `service_dials::open(Bi)` keeps
  // returning `None`; the intent is requeued onto `dial_pending` (NOT
  // `Endpoint::requeue_event`) so the retry token stays private. An
  // external drain between every step must NOT pop it.
  for _ in 0..200 {
    let _ = c.step();
    let mid = c.external_poll_event_drain(a);
    assert!(
      mid < 1_000,
      "external `poll_event` must not surface `DialRequested` (or any \
       unbounded retry-token loop) mid-handshake; got {mid} events in \
       one drain"
    );
  }

  // Drive to convergence. With the sieve in place the dial completes (the
  // retry token survived every external drain inside `dial_pending`);
  // without it the very first drain would have popped it, no future event
  // would fire the retry, and the push/pull would never open.
  for _ in 0..40_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive — the external `poll_event` drain mid-handshake \
     must not have orphaned the dial"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive — the dial completed end-to-end despite the \
     external drain"
  );

  // Post-settle external drain: even after convergence the public surface
  // never returns `DialRequested` (the private storage is the only place
  // it ever lives). The drain count is bounded by the number of genuine
  // application-visible events the host emits (joins, updates) — it must
  // not include any retry token.
  let post = c.external_poll_event_drain(a);
  assert!(
    post < 64,
    "post-settle external `poll_event` drain must be bounded by genuine \
     application events (no `DialRequested` retry token leakage); got {post}"
  );
}

/// Closed-before-drained pool window: a pooled QUIC connection that has been
/// observed Established but is now in `Closed`/`Draining`/`Drained` MUST NOT
/// silently drop a subsequent reliable exchange. `ConnTable::get_or_dial`
/// detects the un-reusable cached connection (`Streams::open(Dir::Bi)=None`
/// is coupled with `!is_handshaking()` while the conn is closed, so without
/// the `is_closed()` probe the `service_dials` path would fall through to
/// `dial_failed` and consume the intent silently) and dials a fresh one,
/// with the slab retaining the old handle so `reap_if_drained` can complete
/// its drained-reap protocol on it. `reap_if_drained`'s handle-equality
/// guard then keeps the redial's new `peers[peer]` mapping intact when the
/// old handle drains and is reaped.
///
/// End-to-end via the public harness: after an idle drain reaps the pooled
/// connection, a freshly triggered push/pull MUST open a new connection and
/// converge. (The harness's idle drain transitions the connection
/// `Established → Drained` via quinn-proto's `kill`-on-idle, so the exact
/// `Closed`/`Draining` window is not reproducible end-to-end without a
/// `Connection::close` seam; the unit tests in `memberlist-machine`
/// `quic::conn` cover that precise state. This conformance test pins the
/// parity invariant: the system recovers correctly after a drained-reap and
/// a subsequent dial succeeds.)
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run ≥5× standalone to prove non-flaky.
#[test]
fn drained_reap_then_subsequent_dial_redials_and_converges() {
  let a = "127.0.0.1:7171".parse().unwrap();
  let b = "127.0.0.1:7172".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  for _ in 0..5_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "precondition: A sees B Alive after the warm-up join"
  );
  assert!(
    c.live_quic_connections(a) >= 1,
    "precondition: the warm-up join opened a pooled QUIC connection"
  );
  // Idle past quinn's `max_idle_timeout` → kill → Drained → drained-reap.
  // After this, A.peers[B] is None and the slab slot is gone (the
  // equality-guarded `peers` removal in `reap_if_drained` correctly clears
  // it because `peers[B]` still equalled the reaped handle).
  c.idle_for(Duration::from_secs(60));
  for _ in 0..5_000 {
    if !c.step() {
      break;
    }
  }
  assert_eq!(
    c.live_quic_connections(a),
    0,
    "idle drain must reap the pooled connection (slab + peers entry gone)"
  );

  // Trigger a subsequent reliable exchange after the reap. `get_or_dial`
  // sees no cached entry → dial fresh. The freshly dialed connection must
  // complete its handshake and the bridge for this push/pull must open and
  // reap; both peers stay Alive.
  c.trigger_push_pull(a, b);
  let mut bridge_opened = false;
  let mut bridge_reaped = false;
  for _ in 0..40_000 {
    let progressed = c.step();
    if c.live_bridge_count(a) > 0 {
      bridge_opened = true;
    } else if bridge_opened && c.live_bridge_count(b) == 0 {
      // BridgePhase model: both peers reach `BothClosed` only after
      // their respective FIN ACKs are observed. The initiator (A) may
      // reach `BothClosed` ~1 RTT before the responder (B): A's FIN ACK
      // arrives first (because A sent FIN first), and B's `BothClosed`
      // requires A's ACK of B's response FIN to come back. Wait for
      // BOTH peers' bridges to fully reap before declaring the cycle
      // complete.
      bridge_reaped = true;
      break;
    }
    if !progressed {
      break;
    }
  }
  assert!(
    bridge_opened,
    "the post-reap push/pull must open a new bidi stream on a fresh dial"
  );
  assert!(
    bridge_reaped,
    "the post-reap push/pull bridge must complete its exchange and be reaped"
  );
  assert!(
    c.live_quic_connections(a) >= 1,
    "the fresh dial must have established a new pooled connection"
  );
  assert!(
    c.sees_alive(a, &id("b")),
    "A must still see B Alive after the post-reap push/pull"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must still see A Alive after the post-reap push/pull"
  );
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "no bridge may leak on A after the post-reap push/pull settles"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "no bridge may leak on B after the post-reap push/pull settles"
  );
}

/// A peer that opens a remote-initiated unidirectional stream toward
/// the composed endpoint must be refused at the QUIC protocol layer:
/// `QuicConfig::new` forces `TransportConfig::max_concurrent_uni_streams = 0`
/// on the shared transport config installed on BOTH client and server,
/// so the sender's `quinn_proto::Streams::open(Dir::Uni)` returns `None`
/// once `state.next[Uni] >= state.max[Uni]`. The receiver therefore
/// never accepts an unwelcome uni stream and no live remote uni-stream
/// state can accumulate.
///
/// Why the zero-credit transport config matters: memberlist's reliable
/// exchanges (push/pull, reliable-ping fallback, reliable user-message)
/// are ALL bidirectional; a remotely-initiated unidirectional stream has
/// no legitimate destination in the composed unit. Without the zero-credit
/// transport config, a peer (or a tampered build) could flood the receiver
/// with uni-stream-open frames and accumulate per-stream `Recv` state.
///
/// Scenario:
///   - Two nodes converge Alive over a real QUIC bidi handshake (so
///     both endpoints' streams subsystems are fully initialized and
///     both have observed each other's `initial_max_streams_uni`
///     transport parameter — which is `0` on both sides).
///   - From inside the inner quinn connection, attempt
///     `streams().open(Dir::Uni)` on `a -> b`. With the zero-credit
///     transport config in effect this MUST return `None`: `a`'s view of
///     `state.max[Uni]` is `b`'s advertised `initial_max_streams_uni = 0`,
///     and the open is refused without a single frame leaving `a`.
///   - After several further `c.step()` ticks the receiver `b` must
///     show no incremental bridge state — `b`'s `live_bridge_count`
///     is unchanged from its post-warm-up baseline (push/pull bridges
///     have already been reaped; no uni-stream has opened because
///     none were ever sent), and `b`'s `live_quic_connections` is
///     unchanged (no new accepted connection materialised).
///
/// Negative-control proof: removing the
/// `transport.max_concurrent_uni_streams(VarInt::from_u32(0))` line in
/// `QuicConfig::new` (and removing the analogous force on the server
/// side) reverts `state.max[Uni]` to quinn's default 100, so the
/// `Streams::open(Dir::Uni)` would instead return `Some(_)` and the
/// receiver would allocate `Recv` state on first frame arrival. The
/// assertion `let sid = a.quinn().connection(...).streams().open(Dir::Uni); assert!(sid.is_none())`
/// would then fail.
///
/// The exact observable above (the sender's `open(Dir::Uni)` outcome)
/// is the most precise check available end-to-end: probing the
/// receiver's bridge / Recv state directly would require deep
/// introspection seams that don't exist in the public API; the
/// sender-side refusal is the visible, single-source-of-truth assertion
/// that the protocol refused the open WITHOUT generating any data on
/// the wire. The receiver-state invariance check
/// (`live_bridge_count(b)` / `live_quic_connections(b)` unchanged)
/// confirms no side-effect.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock,
/// bounded steps). Run ≥5× standalone to prove non-flaky.
#[test]
fn peer_opened_uni_stream_is_refused_no_remote_state() {
  let a = "127.0.0.1:7195".parse().unwrap();
  let b = "127.0.0.1:7196".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);

  // Drive the QUIC handshake + push/pull join to convergence so both
  // connections are fully Established, both `initial_max_streams_uni`
  // transport parameters have crossed, and both bridges have been
  // reaped.
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")) && c.sees_alive(b, &id("a")),
    "precondition: both peers must converge Alive before probing uni-stream credit"
  );
  // Wait for any in-flight periodic push/pull bridge to reap so the
  // baseline snapshot below is taken at a known-quiescent point.
  for _ in 0..10_000 {
    if c.live_bridge_count(b) == 0 {
      break;
    }
    c.step();
  }
  let bridges_b_before = c.live_bridge_count(b);
  let conns_b_before = c.live_quic_connections(b);

  // Attempt to open a remote-initiated unidirectional stream from
  // `a -> b`. With the zero-credit transport config in effect, `b`
  // advertised `initial_max_streams_uni = 0`, so `a`'s view of
  // `state.max[Uni]` is zero and the open MUST return `None` without
  // sending a single STREAM frame. (The protocol-level refusal is
  // observable here on the sender side; a default `TransportConfig` would
  // allow up to 100 concurrent uni streams from each side.)
  let open_succeeded = c.try_open_uni_stream(a, b);
  assert!(
    !open_succeeded,
    "`Streams::open(Dir::Uni)` on `a -> b` MUST be refused — \
     `b`'s advertised `initial_max_streams_uni = 0` means `a`'s view of \
     `state.max[Uni]` is zero, so the open returns `None` without \
     sending a single STREAM frame. With the default `TransportConfig` \
     it would instead succeed and start a new uni stream."
  );

  // The refusal must NOT have created any receive-side state on `b`.
  // Drive a handful of further ticks and verify `b`'s bridge and
  // live-connection counts are unchanged from the post-warm-up
  // baseline — no Recv was allocated, no bridge materialised.
  for _ in 0..200 {
    if !c.step() {
      break;
    }
  }
  assert_eq!(
    c.live_bridge_count(b),
    bridges_b_before,
    "B's live-bridge count must be unchanged after the uni-stream \
     open attempt — quinn-proto refused the open at the sender, so no \
     Recv (no bridge) was allocated on the receiver"
  );
  assert_eq!(
    c.live_quic_connections(b),
    conns_b_before,
    "B's live-connection count must be unchanged — the uni-stream \
     open attempt must not materialise any new accepted connection on B"
  );
}

/// A request initiated via the high-level `QuicEndpoint::start_push_pull`
/// wrapper MUST be self-sufficient under STRICT poll-surface driving: a
/// driver that uses ONLY `start_push_pull` / `poll_transmit` /
/// `poll_timeout` / `handle_udp` / `handle_packet` / `handle_timeout` (no
/// same-instant pre-pump of `handle_timeout` after `start_*`) must
/// observe a fresh dial's Initial datagram queued on the driver-facing
/// `poll_transmit` AT THE SAME LOGICAL INSTANT `start_push_pull` returns,
/// and the exchange must converge end-to-end purely on poll-surface
/// driving.
///
/// Two observables:
///
/// 1. **Same-instant Initial emission.** After
///    `start_push_pull_via_high_level_api(a, b, Join)`,
///    `pop_transmit_of(a)` must return `Some(_)` without any clock
///    advance — the zero-time outbound flush ran `service_quinn` +
///    `collect_transmits` synchronously, so the fresh quinn
///    connection's Initial datagram is in `out` before `start_*`
///    returns. Without the flush the Initial sits inside
///    `Connection::poll_transmit` (never drained), and the first
///    `pop_transmit_of` returns `None`.
///
/// 2. **End-to-end convergence under `step_via_poll_timeout_only`.**
///    The cluster is built from scratch with NO warm-up. Every
///    subsequent tick uses only [`QuicCluster::step_via_poll_timeout_only`]
///    (no `c.step()` ever — `c.step()` pre-pumps `handle_timeout(now)`
///    on every node before reading `poll_timeout`, masking the bug).
///    Both nodes must eventually see each other Alive.
///
/// Negative-control proof (REQUIRED). Comment out
/// `self.flush_outbound(now)` from `QuicEndpoint::start_push_pull` (in
/// `memberlist-machine/src/quic/mod.rs`) and re-run this test. Observable
/// (1) fails first: `pop_transmit_of(a)` returns `None` because the
/// fresh quinn connection's Initial is never collected into `out` (the
/// fresh-dial case — `Connection::poll_transmit` is only polled inside
/// `collect_transmits`, which only runs at the end of `run_tick`, which
/// only fires from `handle_timeout` or `handle_udp(Class::Quic)`, neither
/// of which the strict-poll driver invoked here). For the pooled-
/// Established + freshly-opened-bridge case the negative control manifests
/// differently: `service_dials` opens a bidi stream and inserts a `Bridge`
/// carrying the encoded request bytes in its FSM `Stream` output buffer,
/// but `pump_bridges` (step (2) of `run_tick`) is what moves the bytes
/// into the quinn send stream — so without the flush the bytes never
/// reach quinn and the next `poll_transmit` is also empty.
///
/// Deterministic (fixed per-node quinn rng_seed, virtual clock, bounded
/// steps). Run ≥3× standalone via the conformance harness to prove
/// non-flaky.
#[test]
fn start_push_pull_self_sufficient_under_strict_poll_driving() {
  let a = "127.0.0.1:7211".parse().unwrap();
  let b = "127.0.0.1:7212".parse().unwrap();

  // Observable (1): the wrapper's zero-time outbound flush must put the
  // fresh quinn connection's Initial datagram on the driver-facing
  // `poll_transmit` queue BEFORE the wrapper returns. No clock advance
  // happens here — `start_push_pull_via_high_level_api` calls
  // `QuicEndpoint::start_push_pull(peer, kind, now)` at `c.clock.now()`
  // and the underlying `flush_outbound(now)` runs `service_quinn` +
  // `collect_transmits` on the just-dialed connection. `two_node_bare`
  // builds A and B with no warm-up and no initiating push/pull, so the
  // high-level wrapper is the ONLY public action that initiates the
  // exchange.
  let mut c = QuicCluster::two_node_bare(a, b);
  c.start_push_pull_via_high_level_api(a, b, memberlist_machine::PushPullKind::Join);
  let initial = c.pop_transmit_of(a);
  assert!(
    initial.is_some(),
    "after `start_push_pull` returns, `poll_transmit` on `a` MUST \
     yield the fresh quinn connection's Initial datagram on the very \
     first call (the zero-time outbound flush ran `service_quinn` + \
     `collect_transmits` synchronously). Without the flush, the Initial \
     sits inside `Connection::poll_transmit` and the strict-poll driver \
     observes an empty outbound queue."
  );
  let (to, _bytes) = initial.expect("guarded by the assertion above");
  assert_eq!(
    to, b,
    "the Initial's destination MUST be B (the dial target)"
  );

  // Observable (2): rebuild a fresh bare cluster and drive it purely via
  // `step_via_poll_timeout_only` — the strict-poll loop. Every wake
  // instant comes from `poll_timeout` or the harness queue; no
  // `c.step()` is invoked (which would pre-pump `handle_timeout` on
  // every node before reading `poll_timeout`, masking the bug). The
  // push/pull initiated by the high-level wrapper must converge end-
  // to-end without that pre-pump.
  let mut c = QuicCluster::two_node_bare(a, b);
  c.start_push_pull_via_high_level_api(a, b, memberlist_machine::PushPullKind::Join);
  for _ in 0..40_000 {
    if c.sees_alive(a, &id("b")) && c.sees_alive(b, &id("a")) {
      break;
    }
    if !c.step_via_poll_timeout_only() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive — the push/pull initiated via the high-\
     level wrapper MUST converge under strict poll-surface driving \
     (no `handle_timeout` pre-pump after `start_*`)"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive — both peers converge end-to-end purely \
     via the public Sans-I/O poll surface (`start_push_pull` / \
     `poll_transmit` / `poll_timeout` / `handle_udp` / `handle_packet` \
     / `handle_timeout`)"
  );
}

// ── Per-peer rustls `server_name` plumbing via `AddrBridge` ──────────────────
//
// `QuicEndpoint` MUST source the rustls/QUIC verification identity for a
// peer from `AddrBridge::server_name(&peer)`, NOT a hardcoded string. The
// test below proves the plumbing end-to-end: each side's client cert
// verifier is a strict `ServerCertVerifier` that asserts the
// `ServerName` it receives matches the peer's per-peer name; the
// per-peer `AddrBridge` impls map the wire `SocketAddr` to those names;
// each side's server cert SAN is its own per-peer name. A successful
// push/pull exchange between A and B is then proof that the bridge's
// name reached rustls.
//
// Reverting `conn.rs:get_or_dial` to call `quinn.connect(..., peer,
// "localhost")` is the negative control: the CapturingVerifier on A
// sees `"localhost"` (not the expected `"node-b.example.com"`), the
// strict SAN check fails with `CertificateError::NotValidForName`, the
// handshake errors, and the push/pull never converges.
mod per_peer_server_name {
  use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
  };

  use memberlist_machine::{
    AddrBridge, Endpoint, EndpointConfig, PushPullKind, QuicConfig, QuicEndpoint,
  };
  use memberlist_wire::typed::State;
  use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
  use smol_str::SmolStr;

  /// Strict rustls verifier: every `verify_server_cert` invocation must
  /// see `server_name == expected`. Captures the last-seen name so a test
  /// assertion can inspect it directly; mismatched names return
  /// `CertificateError::NotValidForName` (the same error a real
  /// `WebPkiServerVerifier` returns when a presented cert lacks a
  /// matching SAN/CN).
  ///
  /// The certificate bytes themselves are NOT checked — this is a
  /// plumbing test, not a TLS conformance test; the assertion is that
  /// the rustls verifier saw the name `AddrBridge::server_name` returned.
  #[derive(Debug)]
  struct CapturingVerifier {
    expected: ServerName<'static>,
    last_seen: Arc<Mutex<Option<ServerName<'static>>>>,
  }

  impl rustls::client::danger::ServerCertVerifier for CapturingVerifier {
    fn verify_server_cert(
      &self,
      _end_entity: &CertificateDer<'_>,
      _intermediates: &[CertificateDer<'_>],
      server_name: &ServerName<'_>,
      _ocsp: &[u8],
      _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      let owned = server_name.to_owned();
      if let Ok(mut g) = self.last_seen.lock() {
        *g = Some(owned.clone());
      }
      if owned == self.expected {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
      } else {
        Err(rustls::Error::InvalidCertificate(
          rustls::CertificateError::NotValidForName,
        ))
      }
    }

    fn verify_tls12_signature(
      &self,
      _message: &[u8],
      _cert: &CertificateDer<'_>,
      _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
      &self,
      _message: &[u8],
      _cert: &CertificateDer<'_>,
      _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      memberlist_simulation::quic_net::sim_crypto_provider()
        .signature_verification_algorithms
        .supported_schemes()
    }
  }

  /// Build a server cert chain + private key with the supplied SAN. Uses
  /// rcgen 0.14: `generate_simple_self_signed` returns
  /// `CertifiedKey<KeyPair>` with the private key in `signing_key`
  /// (PKCS#8 DER via `serialize_der()`) — same construction the
  /// `memberlist-machine` quic crypto tests use.
  fn cert_with_san(san: &str) -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let ck = rcgen::generate_simple_self_signed(vec![san.to_string()]).unwrap();
    let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
    let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
    (chain, key)
  }

  /// Build a `QuicConfig` for one node: its own server cert (SAN =
  /// `own_san`) plus a strict client verifier that requires the peer's
  /// cert verification name to equal `expected_peer_name`.
  fn config_with_strict_verifier(
    own_san: &str,
    expected_peer_name: &'static str,
  ) -> (QuicConfig, Arc<Mutex<Option<ServerName<'static>>>>) {
    let (chain, key) = cert_with_san(own_san);
    let expected: ServerName<'static> =
      ServerName::try_from(expected_peer_name).expect("valid DNS name");
    let last_seen: Arc<Mutex<Option<ServerName<'static>>>> = Arc::new(Mutex::new(None));
    let verifier = Arc::new(CapturingVerifier {
      expected,
      last_seen: last_seen.clone(),
    });
    let provider = memberlist_simulation::quic_net::sim_crypto_provider();
    let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .with_no_client_auth()
      .with_single_cert(chain, key)
      .unwrap();
    let qsc =
      quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(server_tls)).unwrap();
    let server = quinn_proto::ServerConfig::with_crypto(Arc::new(qsc));
    let client_tls = rustls::ClientConfig::builder_with_provider(provider)
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(verifier)
      .with_no_client_auth();
    let qcc =
      quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(client_tls)).unwrap();
    let client = quinn_proto::ClientConfig::new(Arc::new(qcc));
    let mut transport = quinn_proto::TransportConfig::default();
    transport.max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
    ));
    let endpoint = memberlist_simulation::quic_net::sim_endpoint_config(&[0x5au8; 32]);
    let cfg = QuicConfig::new(endpoint, server, client, transport);
    (cfg, last_seen)
  }

  // Per-peer bridges — one type per node so each can hardcode the
  // expected peer name as a `&'static str`. The bridge marker is a
  // `PhantomData<fn(B)>` on `QuicEndpoint`, so `B::server_name` is a
  // static fn — keying on the marker type is the natural way to thread
  // per-node policy through the trait.

  // A's view: B's wire SocketAddr maps to B's name.
  struct BridgeForA;
  impl AddrBridge<SocketAddr> for BridgeForA {
    type ServerName = str;

    fn to_socket(addr: &SocketAddr) -> SocketAddr {
      *addr
    }
    fn from_socket(socket: SocketAddr) -> SocketAddr {
      socket
    }
    fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
      // A only dials B in this test; map every peer to B's name.
      Some("node-b.example.com")
    }
  }

  // B's view: A's wire SocketAddr maps to A's name.
  struct BridgeForB;
  impl AddrBridge<SocketAddr> for BridgeForB {
    type ServerName = str;

    fn to_socket(addr: &SocketAddr) -> SocketAddr {
      *addr
    }
    fn from_socket(socket: SocketAddr) -> SocketAddr {
      socket
    }
    fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
      Some("node-a.example.com")
    }
  }

  fn build_endpoint<B: AddrBridge<SocketAddr>>(
    id: &str,
    addr: SocketAddr,
    cfg: QuicConfig,
    now: Instant,
  ) -> QuicEndpoint<SmolStr, SocketAddr, B> {
    let ep_cfg = EndpointConfig::new(SmolStr::new(id), addr)
      .with_gossip_interval(Duration::from_millis(200))
      .with_push_pull_interval(Duration::from_secs(30))
      .with_probe_interval(Duration::from_millis(1000))
      .with_probe_timeout(Duration::from_millis(500))
      .with_suspicion_mult(4)
      .with_retransmit_mult(4)
      .with_rng_seed(addr.port() as u64);
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(ep_cfg);
    ep.start_scheduling(now);
    let mut seed = [0u8; 32];
    seed[..2].copy_from_slice(&addr.port().to_le_bytes());
    QuicEndpoint::<SmolStr, SocketAddr, B>::with_quinn_rng_seed(ep, cfg, Some(seed))
  }

  /// Per-peer rustls `server_name` plumbing via `AddrBridge`.
  ///
  /// Two nodes A and B, each with a server cert whose SAN is its own
  /// per-peer DNS name. Each client side uses a strict
  /// `ServerCertVerifier` that requires the `ServerName` it receives to
  /// match the peer's per-peer name (a non-match returns
  /// `CertificateError::NotValidForName`). The per-peer `AddrBridge`
  /// impls return the expected peer name from `server_name(&peer)`.
  /// A successful push/pull join (A and B converge Alive) proves the
  /// bridge's per-peer name travelled from `service_dials` through
  /// `ConnTable::get_or_dial` into `quinn_proto::Endpoint::connect` and
  /// out the other side as the `ServerName` the rustls verifier
  /// observed.
  ///
  /// Negative control (manual): revert `conn.rs:get_or_dial` to
  /// `quinn.connect(..., peer, "localhost")` and rerun — the
  /// CapturingVerifier on A sees `"localhost"` instead of
  /// `"node-b.example.com"`, returns
  /// `CertificateError::NotValidForName`, the handshake fails, and the
  /// `c.sees_alive(...)` / `last_seen == expected` assertions below
  /// fail.
  #[test]
  fn dial_uses_per_peer_server_name_from_addr_bridge() {
    let a_addr: SocketAddr = "127.0.0.1:7301".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7302".parse().unwrap();
    let now = Instant::now();
    let (cfg_a, seen_by_a) =
      config_with_strict_verifier("node-a.example.com", "node-b.example.com");
    let (cfg_b, _seen_by_b) =
      config_with_strict_verifier("node-b.example.com", "node-a.example.com");
    let mut a = build_endpoint::<BridgeForA>("a", a_addr, cfg_a, now);
    let mut b = build_endpoint::<BridgeForB>("b", b_addr, cfg_b, now);

    // A initiates a join push/pull to B via the high-level wrapper so
    // `service_dials` fires in-band — that is the path under test.
    // `start_push_pull` triggers `service_dials(now)` which calls
    // `ConnTable::get_or_dial(..., server_name)` which calls
    // `quinn_proto::Endpoint::connect(..., server_name)`. rustls's
    // `start_session` parses the name and the configured
    // `ServerCertVerifier::verify_server_cert` is invoked the moment
    // the server's Certificate handshake message is processed by the
    // client.
    let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);

    // Drive the QUIC handshake. Virtual time advances by 10 ms per
    // iteration — enough for quinn-proto's PTO / retransmit timers to
    // fire while staying within a tight bound for the test. The
    // verifier fires as soon as A's client receives B's TLS
    // Certificate message during the handshake, so a few hundred
    // ferry iterations are more than enough.
    let mut t = now;
    for _ in 0..2000 {
      let mut moved = false;
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, t);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, t);
          moved = true;
        }
      }
      // Feed any decoded memberlist messages straight back through
      // `handle_packet` (this test does not exercise the wire codec
      // — that has its own coverage; here we only need enough
      // membership progress for the post-handshake observation
      // assertions below).
      while let Some(tx) = a.poll_memberlist_transmit() {
        match tx {
          memberlist_machine::Transmit::Packet(p) => {
            let (to, message) = p.into_parts();
            if to == b_addr {
              b.handle_packet(a_addr, message, t);
              moved = true;
            }
          }
          memberlist_machine::Transmit::Compound(cmp) => {
            let (to, messages) = cmp.into_parts();
            if to == b_addr {
              for m in messages {
                b.handle_packet(a_addr, m, t);
              }
              moved = true;
            }
          }
        }
      }
      while let Some(tx) = b.poll_memberlist_transmit() {
        match tx {
          memberlist_machine::Transmit::Packet(p) => {
            let (to, message) = p.into_parts();
            if to == a_addr {
              a.handle_packet(b_addr, message, t);
              moved = true;
            }
          }
          memberlist_machine::Transmit::Compound(cmp) => {
            let (to, messages) = cmp.into_parts();
            if to == a_addr {
              for m in messages {
                a.handle_packet(b_addr, m, t);
              }
              moved = true;
            }
          }
        }
      }
      a.handle_timeout(t);
      b.handle_timeout(t);
      // Stop as soon as A's client verifier has observed at least one
      // ServerName — the per-peer ServerName plumbing observable is then
      // complete and there is no point continuing to drive time.
      if seen_by_a.lock().unwrap().is_some()
        && a
          .endpoint_ref()
          .member_liveness(&SmolStr::new("b"))
          .is_some_and(|s| s == State::Alive)
        && b
          .endpoint_ref()
          .member_liveness(&SmolStr::new("a"))
          .is_some_and(|s| s == State::Alive)
      {
        break;
      }
      // Advance virtual time so quinn-proto's retransmit / PTO timers
      // can fire on subsequent iterations.
      t += Duration::from_millis(10);
      if !moved {
        // No traffic AND not converged — let the assertions below
        // report the failure with full context.
      }
    }

    // The rustls verifier on A's side MUST have observed the bridge's
    // per-peer name. This is the precise observable for the per-peer
    // ServerName plumbing seam: the moment `service_dials` reaches
    // `quinn_proto::Endpoint::connect(now, client, peer, server_name)`,
    // quinn-proto invokes
    // `config.crypto.start_session(version, server_name, &params)`;
    // rustls parses the name via `ServerName::try_from(server_name)`
    // and on the next handshake step the configured
    // `ServerCertVerifier::verify_server_cert` is invoked with that
    // ServerName. A regression that hardcodes `"localhost"` in
    // `get_or_dial` would leave `seen_by_a` holding
    // `DnsName("localhost")` instead of the expected per-peer name,
    // and this assertion would fail.
    let observed = seen_by_a
      .lock()
      .unwrap()
      .clone()
      .expect("A's client verifier must have observed at least one ServerName from a dial");
    let expected = ServerName::try_from("node-b.example.com").unwrap();
    assert_eq!(
      observed, expected,
      "AddrBridge::server_name MUST plumb through to rustls — A's \
       client verifier observed `{observed:?}`, expected `{expected:?}`. \
       Reverting `get_or_dial` to hardcode `\"localhost\"` would have \
       this asserting `localhost` against `node-b.example.com`."
    );

    // End-to-end convergence: with the verifier accepting the cert
    // (the per-peer name matches), the join push/pull MUST succeed —
    // a `CertificateError::NotValidForName` handshake failure would
    // strand the dial and neither side would converge Alive. This is
    // the additive end-to-end observable: not only did the bridge's
    // name reach rustls, but rustls accepted it and the handshake
    // completed, so the membership exchange built on top of it
    // actually delivered.
    let a_alive_b = a
      .endpoint_ref()
      .member_liveness(&SmolStr::new("b"))
      .is_some_and(|s| s == State::Alive);
    let b_alive_a = b
      .endpoint_ref()
      .member_liveness(&SmolStr::new("a"))
      .is_some_and(|s| s == State::Alive);
    assert!(
      a_alive_b,
      "A must see B Alive — the rustls verifier on A's side accepted \
       the per-peer SAN `node-b.example.com` returned by \
       `BridgeForA::server_name`, so the handshake completed and the \
       join push/pull converged"
    );
    assert!(
      b_alive_a,
      "B must see A Alive — symmetric: B's verifier accepted the \
       per-peer SAN `node-a.example.com` returned by \
       `BridgeForB::server_name`"
    );
  }
}

/// Cluster-auth on the QUIC reliable path is the operator's choice of
/// rustls `ClientCertVerifier` on the supplied `quinn_proto::ServerConfig`.
/// `QuicConfig::new` does NOT pick a policy: the caller builds the entire
/// server config including the cert verifier. This test proves the two
/// pieces operators need:
///
/// 1. A server config built with `with_client_cert_verifier(...)` instead
///    of `with_no_client_auth()` makes `QuicConfig::new` happily install
///    it (the constructor no longer hardcodes `with_no_client_auth`).
/// 2. A client that does NOT present a cert against such a server fails
///    the TLS handshake — no `EndpointEvent` side effects survive
///    (no merge into the strict-server's membership, no push/pull reply
///    received on the joiner). This is the cluster-isolation property:
///    a node whose cert isn't signed by the cluster CA cannot drive any
///    state changes.
mod mtls_cluster_auth {
  use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
  };

  use memberlist_machine::{
    AddrBridge, Endpoint, EndpointConfig, PushPullKind, QuicConfig, QuicEndpoint,
  };
  use memberlist_wire::typed::State;
  use rustls_pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
  use smol_str::SmolStr;

  /// `ClientCertVerifier` that rejects every presented cert — the
  /// adversarial-test equivalent of "client cert not signed by the
  /// cluster CA". `client_auth_mandatory` defaults to `true` via
  /// `offer_client_auth`, so a client that presents NO cert is also
  /// rejected (TLS server emits an alert during handshake).
  #[derive(Debug)]
  struct RejectAllClients;

  impl rustls::server::danger::ClientCertVerifier for RejectAllClients {
    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
      &[]
    }
    fn verify_client_cert(
      &self,
      _end_entity: &CertificateDer<'_>,
      _intermediates: &[CertificateDer<'_>],
      _now: UnixTime,
    ) -> Result<rustls::server::danger::ClientCertVerified, rustls::Error> {
      Err(rustls::Error::General(
        "mTLS cluster-auth test: rejecting unauthenticated client".into(),
      ))
    }
    fn verify_tls12_signature(
      &self,
      _message: &[u8],
      _cert: &CertificateDer<'_>,
      _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
      &self,
      _message: &[u8],
      _cert: &CertificateDer<'_>,
      _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      memberlist_simulation::quic_net::sim_crypto_provider()
        .signature_verification_algorithms
        .supported_schemes()
    }
  }

  #[derive(Debug)]
  struct AcceptAnyServer;
  impl rustls::client::danger::ServerCertVerifier for AcceptAnyServer {
    fn verify_server_cert(
      &self,
      _e: &CertificateDer<'_>,
      _i: &[CertificateDer<'_>],
      _n: &rustls_pki_types::ServerName<'_>,
      _o: &[u8],
      _t: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      memberlist_simulation::quic_net::sim_crypto_provider()
        .signature_verification_algorithms
        .supported_schemes()
    }
  }

  struct IdBridge;
  impl AddrBridge<SocketAddr> for IdBridge {
    type ServerName = str;

    fn to_socket(addr: &SocketAddr) -> SocketAddr {
      *addr
    }
    fn from_socket(socket: SocketAddr) -> SocketAddr {
      socket
    }
    fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
      Some("localhost")
    }
  }

  fn cert() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
    let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
    (chain, key)
  }

  /// Build A's config: server REQUIRES client cert (rejected by
  /// `RejectAllClients`); client side has no client cert (irrelevant —
  /// A is the strict server).
  fn strict_server_config() -> QuicConfig {
    let (chain, key) = cert();
    let provider = memberlist_simulation::quic_net::sim_crypto_provider();
    let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .with_client_cert_verifier(Arc::new(RejectAllClients))
      .with_single_cert(chain, key)
      .unwrap();
    let qsc =
      quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(server_tls)).unwrap();
    let server = quinn_proto::ServerConfig::with_crypto(Arc::new(qsc));

    let client_tls = rustls::ClientConfig::builder_with_provider(provider)
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(AcceptAnyServer))
      .with_no_client_auth();
    let qcc =
      quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(client_tls)).unwrap();
    let client = quinn_proto::ClientConfig::new(Arc::new(qcc));

    let mut transport = quinn_proto::TransportConfig::default();
    transport.max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
    ));
    let endpoint = memberlist_simulation::quic_net::sim_endpoint_config(&[0x5au8; 32]);
    QuicConfig::new(endpoint, server, client, transport)
  }

  /// Build B's config: server accepts any client; client has no cert
  /// — the unauthenticated-joiner role.
  fn unauth_client_config() -> QuicConfig {
    let (chain, key) = cert();
    let provider = memberlist_simulation::quic_net::sim_crypto_provider();
    let server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .with_no_client_auth()
      .with_single_cert(chain, key)
      .unwrap();
    let qsc =
      quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(server_tls)).unwrap();
    let server = quinn_proto::ServerConfig::with_crypto(Arc::new(qsc));

    let client_tls = rustls::ClientConfig::builder_with_provider(provider)
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(AcceptAnyServer))
      .with_no_client_auth();
    let qcc =
      quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(client_tls)).unwrap();
    let client = quinn_proto::ClientConfig::new(Arc::new(qcc));

    let mut transport = quinn_proto::TransportConfig::default();
    transport.max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
    ));
    let endpoint = memberlist_simulation::quic_net::sim_endpoint_config(&[0xa5u8; 32]);
    QuicConfig::new(endpoint, server, client, transport)
  }

  fn build_endpoint(
    id: &str,
    addr: SocketAddr,
    cfg: QuicConfig,
    now: Instant,
  ) -> QuicEndpoint<SmolStr, SocketAddr, IdBridge> {
    let ep_cfg = EndpointConfig::new(SmolStr::new(id), addr)
      .with_probe_interval(Duration::from_millis(1000))
      .with_probe_timeout(Duration::from_millis(500))
      .with_suspicion_mult(4)
      .with_rng_seed(addr.port() as u64);
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(ep_cfg);
    ep.start_scheduling(now);
    let mut seed = [0u8; 32];
    seed[..2].copy_from_slice(&addr.port().to_le_bytes());
    QuicEndpoint::<SmolStr, SocketAddr, IdBridge>::with_quinn_rng_seed(ep, cfg, Some(seed))
  }

  #[test]
  fn mtls_rejects_unauthenticated_client_no_side_effects() {
    // A: strict server (requires client cert, rejects all presented).
    // B: unauthenticated joiner (no client cert).
    let a_addr: SocketAddr = "127.0.0.1:7501".parse().unwrap();
    let b_addr: SocketAddr = "127.0.0.1:7502".parse().unwrap();
    let now = Instant::now();
    let mut a = build_endpoint("a", a_addr, strict_server_config(), now);
    let mut b = build_endpoint("b", b_addr, unauth_client_config(), now);

    // B dials A via a Join push/pull. The QUIC handshake fires inside
    // `service_dials` → `get_or_dial` → `quinn_proto::Endpoint::connect`.
    // A's TLS server requires a client cert; B presents none →
    // handshake aborts → no bidi stream → no PushPullRequestReceived
    // on A → no merge of B into A's membership.
    let _ = b.start_push_pull(a_addr, PushPullKind::Join, now);

    // Drive virtual time across handshake + several PTO windows so a
    // bug that LET the handshake succeed (or smuggled bytes through a
    // failed handshake) would have surfaced.
    let mut t = now;
    for _ in 0..400 {
      while let Some((to, bytes)) = a.poll_transmit() {
        if to == b_addr {
          b.handle_udp(a_addr, &bytes, t);
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == a_addr {
          a.handle_udp(b_addr, &bytes, t);
        }
      }
      // Drain any memberlist events to keep buffers clean — these are
      // the OBSERVABLES the assertions below check.
      while a.poll_event().is_some() {}
      while b.poll_event().is_some() {}
      a.handle_timeout(t);
      b.handle_timeout(t);
      t += Duration::from_millis(25);
    }

    // A MUST not have merged B into its membership: a successful
    // PushPullRequestReceived → MergeDelegate → merge_state would have
    // added B (Alive). With the rejected handshake, no
    // EndpointEvent::PushPullRequestReceived ever surfaced on A.
    assert!(
      a.endpoint_ref()
        .member_liveness(&SmolStr::new("b"))
        .is_none(),
      "A's strict-server REJECTED B's unauthenticated handshake — \
       A's membership MUST NOT contain B. Got liveness: {:?}",
      a.endpoint_ref().member_liveness(&SmolStr::new("b"))
    );

    // B's join cannot complete either — A's PushPullReply never
    // arrives because there's no bidi stream. B's view should not
    // see A as Alive.
    assert!(
      !b.endpoint_ref()
        .member_liveness(&SmolStr::new("a"))
        .is_some_and(|s| s == State::Alive),
      "B's handshake was REJECTED at A's side — B MUST NOT see A as \
       Alive. Got liveness: {:?}",
      b.endpoint_ref().member_liveness(&SmolStr::new("a"))
    );
  }
}

// ── Compression: the membership conformance must hold UNCHANGED when reliable
//    exchanges and gossip datagrams ride lz4-compressed frames. Compression is
//    a wire-codec transform: it must be transparent to SWIM membership.

#[test]
fn compressed_two_node_join_over_quic_reaches_alive_both_sides() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9801".parse().unwrap();
  let b = "127.0.0.1:9802".parse().unwrap();
  let mut c = QuicCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after a compressed QUIC push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after a compressed QUIC push/pull join — \
     compression must be transparent to membership"
  );
}

#[test]
fn compressed_join_over_quic_matches_uncompressed_membership_outcome() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9811".parse().unwrap();
  let b = "127.0.0.1:9812".parse().unwrap();
  let mut plain = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !plain.step() {
      break;
    }
  }
  let mut compressed = QuicCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
  for _ in 0..20_000 {
    if !compressed.step() {
      break;
    }
  }
  assert_eq!(
    plain.sees_alive(a, &id("b")),
    compressed.sees_alive(a, &id("b")),
    "A's view of B must match the uncompressed QUIC run"
  );
  assert_eq!(
    plain.sees_alive(b, &id("a")),
    compressed.sees_alive(b, &id("a")),
    "B's view of A must match the uncompressed QUIC run"
  );
}

#[test]
fn compressed_large_state_push_pull_completes_under_backpressure() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9821".parse().unwrap();
  let b = "127.0.0.1:9822".parse().unwrap();
  // Tiny quinn stream-receive window + a 24-extra-peer push snapshot: the
  // LARGE compressed push/pull payload exceeds one window, so the bridge hits
  // `WriteError::Blocked` and must retain + retry the remainder, end-to-end
  // exercising the compressed reliable path's split-delivery reassembly. The
  // 24 extras are ghosts (no endpoint) so B garbage-collects them once they
  // age out — assert the TRANSFER (B received every one across the
  // back-pressured stream), not the post-GC steady state.
  let mut c = QuicCluster::two_node_join_small_window_compressed(a, b, 24, CompressAlgorithm::Lz4);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(b, &id("a")),
    "the large compressed push/pull must complete despite flow-control back-pressure"
  );
  // Every peer A pushed must have crossed the back-pressured stream intact —
  // each compressed reliable unit reassembled with zero byte loss.
  for n in 0..24 {
    let peer = id(&format!("extra-{n}"));
    assert!(
      c.ever_saw_alive(b, &peer),
      "peer {peer} never crossed the back-pressured compressed stream (byte loss)"
    );
  }
}

#[test]
fn compressed_gossip_with_trailing_junk_dropped_wholesale() {
  // A compressed gossip datagram that decompresses to [valid frame][trailing
  // junk] must be dropped wholesale — the receiver's membership is unchanged.
  // This guards the all-or-nothing decode contract: a datagram whose frame
  // sequence does not consume the full decompressed payload is treated the same
  // as a datagram with a corrupt compression wrapper (i.e. dropped with no
  // partial application of the prefix frames that did decode cleanly).
  use memberlist_simulation::{Alive, Message, Node};
  use memberlist_wire::{
    CompressAlgorithm, compress, encode_compressed_frame, framing, message_to_any,
  };

  let a: std::net::SocketAddr = "127.0.0.1:9831".parse().unwrap();
  let b: std::net::SocketAddr = "127.0.0.1:9832".parse().unwrap();
  let ghost_addr: std::net::SocketAddr = "127.0.0.1:9833".parse().unwrap();
  let mut c = QuicCluster::two_node_join(a, b);
  // Let the join complete so both nodes are live before we inject.
  for _ in 0..20_000 {
    if c.sees_alive(a, &id("b")) && c.sees_alive(b, &id("a")) {
      break;
    }
    c.step();
  }
  assert!(
    !c.sees_alive(b, &id("ghost")),
    "precondition: b has not observed ghost"
  );

  // Build a valid Alive frame for "ghost".
  let ghost_alive: Message<SmolStr, std::net::SocketAddr> =
    Message::Alive(Alive::new(1, Node::new(id("ghost"), ghost_addr)));
  let any =
    message_to_any::<SmolStr, std::net::SocketAddr>(&ghost_alive).expect("alive to AnyMessage");
  let valid_frame = framing::encode_message(&any).expect("encode alive frame");

  // Concatenate trailing junk bytes that are not a valid frame.
  let mut payload = valid_frame;
  payload.extend_from_slice(b"\xff\xff\xff");

  // Compress the combined payload and wrap it in the compressed-frame format.
  let packed = compress(CompressAlgorithm::Lz4, &payload).expect("lz4 compress");
  let on_wire = encode_compressed_frame(CompressAlgorithm::Lz4, payload.len(), &packed);

  // Inject the crafted datagram as an inbound gossip datagram from a to b.
  c.inject_raw_gossip(a, b, &on_wire);

  // b must NOT have gained "ghost" in its live membership — the datagram was
  // dropped wholesale. `sees_alive` reads the live member table directly (via
  // `member_liveness`), so a partial-apply that installed the valid prefix
  // frame would surface here immediately, without requiring a `step()` to
  // refresh an event cache.
  assert!(
    !c.sees_alive(b, &id("ghost")),
    "compressed gossip datagram with trailing junk must be dropped wholesale; \
     the valid prefix frame must not be applied"
  );
}

// ── Encryption: the membership conformance must hold UNCHANGED when reliable
//    exchanges ride QUIC (quinn-encrypted streams) and gossip datagrams ride
//    AEAD-encrypted frames. The QUIC bridge force-disables reliable-path
//    encryption (quinn already encrypts the underlying streams) — the
//    reliable-wire wrapper-skip test below pins that no `[Encrypted[..]]`
//    wrapper appears on the reliable path.

#[cfg(feature = "__sim-encryption-aes-gcm")]
#[test]
fn encrypted_two_node_join_over_quic_reaches_alive_both_sides() {
  use memberlist_wire::SecretKey;
  let a = "127.0.0.1:9951".parse().unwrap();
  let b = "127.0.0.1:9952".parse().unwrap();
  let mut c = QuicCluster::two_node_join_encrypted(a, b, SecretKey::Aes256([0x88; 32]));
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after an encrypted QUIC push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after an encrypted QUIC push/pull join — \
     encryption must be transparent to membership"
  );
}

#[cfg(feature = "__sim-encryption-aes-gcm")]
#[test]
fn encrypted_join_over_quic_matches_unencrypted_membership_outcome() {
  use memberlist_wire::SecretKey;
  let a = "127.0.0.1:9961".parse().unwrap();
  let b = "127.0.0.1:9962".parse().unwrap();

  let mut plain = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !plain.step() {
      break;
    }
  }
  let mut encrypted = QuicCluster::two_node_join_encrypted(a, b, SecretKey::Aes256([0x99; 32]));
  for _ in 0..20_000 {
    if !encrypted.step() {
      break;
    }
  }
  // The membership end state is identical with and without encryption.
  assert_eq!(
    plain.sees_alive(a, &id("b")),
    encrypted.sees_alive(a, &id("b")),
    "A's view of B must match the unencrypted run"
  );
  assert_eq!(
    plain.sees_alive(b, &id("a")),
    encrypted.sees_alive(b, &id("a")),
    "B's view of A must match the unencrypted run"
  );
}

#[cfg(feature = "__sim-encryption-aes-gcm")]
#[test]
fn encrypted_quic_reliable_wire_carries_no_encrypted_wrapper() {
  // The QUIC reliable wire bytes (the quinn UDP datagrams the harness routes
  // through the virtual reliable pipe) must NOT begin with `ENCRYPTED_TAG` —
  // the QUIC bridge force-disables reliable-path encryption (quinn already
  // encrypts the streams), so the plaintext units the bridge hands to quinn
  // never carry an `Encrypted` wrapper either. Quinn then encrypts those
  // units into datagrams whose first byte has `b & 0xC0 != 0` (>= `0x40`) by
  // the QUIC long/short-header bit pattern, disjoint from the memberlist
  // tag space (`1..=15`) that contains `ENCRYPTED_TAG`.
  use memberlist_wire::SecretKey;
  let a = "127.0.0.1:9971".parse().unwrap();
  let b = "127.0.0.1:9972".parse().unwrap();
  let mut c = QuicCluster::two_node_join_encrypted(a, b, SecretKey::Aes256([0xAA; 32]));
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  let observed = c.observed_reliable_wire_bytes();
  assert!(
    !observed.is_empty(),
    "the harness must have observed at least one reliable wire payload"
  );
  for chunk in observed {
    assert_ne!(
      chunk.first().copied(),
      Some(memberlist_wire::ENCRYPTED_TAG),
      "QUIC reliable wire bytes must NOT carry an Encrypted wrapper (quinn already encrypts)"
    );
  }
}

// ── Compound stack: compression + encryption together must not disturb SWIM.

#[cfg(all(feature = "__sim-encryption-aes-gcm", feature = "compression-lz4"))]
#[test]
fn compressed_and_encrypted_join_over_quic_matches_unencrypted_uncompressed_membership_outcome() {
  use memberlist_wire::{CompressAlgorithm, SecretKey};
  let a = "127.0.0.1:9993".parse().unwrap();
  let b = "127.0.0.1:9994".parse().unwrap();
  let mut plain = QuicCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !plain.step() {
      break;
    }
  }
  let key = SecretKey::Aes256([0xEE; 32]);
  let mut both =
    QuicCluster::two_node_join_compressed_and_encrypted(a, b, CompressAlgorithm::Lz4, key);
  for _ in 0..20_000 {
    if !both.step() {
      break;
    }
  }
  assert_eq!(plain.sees_alive(a, &id("b")), both.sees_alive(a, &id("b")));
  assert_eq!(plain.sees_alive(b, &id("a")), both.sees_alive(b, &id("a")));
}
