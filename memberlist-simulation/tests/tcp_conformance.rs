#![cfg(feature = "tcp")]
//! Membership conformance must hold UNCHANGED when reliable exchanges ride
//! plain TCP with a wire-label cluster gate instead of stubs. Asserts state /
//! events / timing only — never plain-TCP wire bytes.
//!
//! The matrix mirrors the sibling TLS conformance suite method-for-method,
//! dropping mTLS (which has no analogue in the label-gated TCP coordinator)
//! and adding two label-policy tests:
//! - `two_node_join_over_tcp_reaches_alive_both_sides` — join push/pull rides a
//!   real plain-TCP connection with matching labels; both nodes converge Alive.
//! - `push_pull_convergence_over_tcp` — a periodic (non-join) push/pull merges
//!   both directions.
//! - `reliable_fallback_rescues_udp_blocked_probe_no_false_suspect` — direct +
//!   indirect UDP probes dropped: the probe completes via the TCP reliable-ping
//!   fallback and the healthy peer is NEVER falsely Suspected.
//! - `leave_over_tcp_observed_dead_or_left` — `TcpEndpoint::leave` fires
//!   `Event::LeftCluster`; the peer observes the local node Dead/Left.
//! - `large_state_push_pull_completes_under_tcp_backpressure` — a tiny virtual
//!   TCP receive window forces the writer to drain across many ticks; the join
//!   still completes with zero byte loss.
//! - `large_response_coalesced_read_completes` — the responder's push/pull
//!   reply is delivered as ONE coalesced TCP read (uncapped window) and the
//!   join still converges.
//! - `large_request_coalesced_with_label_prefix_completes` — the DIALER's
//!   large join request is delivered to the responder COALESCED with the
//!   one-time label prefix in one transport read while the responder is still
//!   in its label window. The responder must retain the unconsumed plaintext
//!   tail across the post-label mint and replay it the same tick so the full
//!   request reassembles and both sides converge. The plain-TCP analogue of
//!   the TLS pre-promotion coalescing path; the label prefix plays the role
//!   TLS's final handshake flight plays.
//! - `small_request_coalesced_with_label_no_fin_completes` — the DIALER's
//!   small join request is delivered to the responder COALESCED with the
//!   one-time label prefix in one transport read while the responder's read==0
//!   FIN anchor is withheld. The whole small read is consumed in one pass —
//!   no backpressure, no retained tail — yet the decrypted request sits
//!   buffered. With the FIN withheld the responder must drain that buffered
//!   request on the SAME tick it mints + promotes its `Stream` to merge and
//!   reply. The structural analogue of the TLS `close_notify`-then-no-FIN
//!   small-request case.
//! - `truncation_mid_frame_does_not_merge` — the peer half-closes the TCP
//!   connection mid-frame (read==0 before a frame completes): the partial
//!   frame never reaches `Done`, so no merge is applied.
//! - `connect_refused_dial_fails_no_membership_change` — the TCP connect is
//!   refused: no `Stream`, no decode, no merge.
//! - `label_match_join_succeeds_both_sides` — both nodes carry the same label;
//!   the label-check path passes and the join converges. The happy-path
//!   pair of the label-mismatch test.
//! - `acceptor_label_mismatch_rejected_no_side_effects` — the responder
//!   carries a different label: the inbound check on the RESPONDER rejects
//!   the dialer's stream before any memberlist payload is decoded. NO merge,
//!   NO ack, NO suspect / dead side effects. The plain-TCP structural
//!   analogue of the TLS `mtls_rejects_unauthenticated_client_no_side_effects`
//!   test.
//! - `dialer_label_mismatch_rejects_response_no_membership_change` — the
//!   symmetric dialer-side reject: an unlabeled dialer reaches a responder
//!   with `skip_inbound_label_check = true` (so the responder accepts the
//!   unlabeled request and replies), but the responder's reply carries its
//!   own label `[12][len][cluster-b]...`. Per the inbound truth table a
//!   labeled `[12]` header on an unlabeled receiver is rejected as a double
//!   label, so the dialer NEVER merges the responder. Proves the
//!   bidirectional wire-label invariant catches cross-cluster traffic on
//!   the response leg too, faithful to memberlist-core's
//!   `read_message` → `decoder.with_label(...)` on `state.rs::push_pull_node`.
//!   The responder MAY merge the dialer (its `skip_inbound_label_check`
//!   suppresses the inbound check), but the DIALER-side reject is what the
//!   bidirectional invariant adds — and what this test asserts.
//!
//! Parity (the capstone): the existing suspect/dead scenarios pass UNCHANGED
//! through `TcpEndpoint` (the `parity_*` tests below mirror
//! `tests/suspect_dead.rs`), and the existing non-tcp suite is byte-unchanged
//! and still green under the parity gate `cargo test -p memberlist-simulation`.

use memberlist_simulation::tcp_net::TcpCluster;
use memberlist_wire::typed::State;
use smol_str::SmolStr;
use std::time::Duration;

fn id(s: &str) -> SmolStr {
  SmolStr::new(s)
}

#[test]
fn two_node_join_over_tcp_reaches_alive_both_sides() {
  let a = "127.0.0.1:9001".parse().unwrap();
  let b = "127.0.0.1:9002".parse().unwrap();
  let mut c = TcpCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after TCP push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after TCP push/pull join"
  );
}

#[test]
fn push_pull_convergence_over_tcp() {
  let a = "127.0.0.1:9011".parse().unwrap();
  let b = "127.0.0.1:9012".parse().unwrap();
  let mut c = TcpCluster::two_node_alive(a, b);
  c.trigger_push_pull(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(c.sees_alive(a, &id("b")));
  assert!(c.sees_alive(b, &id("a")));
}

#[test]
fn reliable_fallback_rescues_udp_blocked_probe_no_false_suspect() {
  let a = "127.0.0.1:9021".parse().unwrap();
  let b = "127.0.0.1:9022".parse().unwrap();
  let mut c = TcpCluster::two_node_join_slow_probe(a, b);
  // Warm the membership first so a is Alive about b, then block UDP probes.
  for _ in 0..5_000 {
    if c.sees_alive(a, &id("b")) && c.sees_alive(b, &id("a")) {
      break;
    }
    c.step();
  }
  c.drop_all_udp_probes_between(a, b);
  c.trigger_probe(a);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    !c.ever_suspected(a, &id("b")),
    "fallback ack absorbed in step (2) before probe handle_timeout — no false Suspect"
  );
  assert_eq!(c.live_bridge_count(a), 0, "fallback bridge reaped");
}

#[test]
fn leave_over_tcp_observed_dead_or_left() {
  let a = "127.0.0.1:9041".parse().unwrap();
  let b = "127.0.0.1:9042".parse().unwrap();
  let mut c = TcpCluster::two_node_join(a, b);
  for _ in 0..10_000 {
    if c.sees_alive(a, &id("b")) && c.sees_alive(b, &id("a")) {
      break;
    }
    c.step();
  }
  c.leave(a).unwrap();
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(c.left_fired(a), "a fired Event::LeftCluster");
  assert!(c.ever_saw_gone(b, &id("a")), "b observed a Dead/Left");
}

#[test]
fn large_state_push_pull_completes_under_tcp_backpressure() {
  let a = "127.0.0.1:9051".parse().unwrap();
  let b = "127.0.0.1:9052".parse().unwrap();
  // a's push snapshot is pre-loaded with many extra members; the virtual TCP
  // receive window is tiny, so the bytes drain across many ticks.
  let mut c = TcpCluster::two_node_join_small_window(a, b, 200);
  for _ in 0..50_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "join completes despite TCP backpressure (zero byte loss)"
  );
  assert!(
    c.sees_alive(b, &id("extra-0")),
    "every pushed peer crossed intact"
  );
  assert!(c.sees_alive(b, &id("extra-199")));
}

#[test]
fn large_response_coalesced_read_completes() {
  let a = "127.0.0.1:9055".parse().unwrap();
  let b = "127.0.0.1:9056".parse().unwrap();
  // b (the responder) is pre-loaded with 1500 extra members and the receive
  // window is UNCAPPED, so b's push/pull RESPONSE crosses to a as ONE coalesced
  // TCP read whose plaintext spans a large reassembly buffer. The intake loop
  // must reassemble the buffer in bounded passes without panicking on
  // backpressure; break as soon as the merge has crossed (the join completes
  // early) rather than running to idle, which would burn many ticks probing
  // the non-existent extra peers.
  let mut c = TcpCluster::two_node_join_large_response_coalesced(a, b, 1500);
  for _ in 0..20_000 {
    if c.sees_alive(a, &id("b"))
      && c.sees_alive(a, &id("extra-0"))
      && c.sees_alive(a, &id("extra-1499"))
      && c.sees_alive(b, &id("a"))
    {
      break;
    }
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "join completes from a single coalesced large response read"
  );
  assert!(
    c.sees_alive(a, &id("extra-0")),
    "the responder's first extra peer crossed in the coalesced response"
  );
  assert!(
    c.sees_alive(a, &id("extra-1499")),
    "the responder's last extra peer crossed in the coalesced response"
  );
  assert!(c.sees_alive(b, &id("a")), "b merged a's push");
}

#[test]
fn large_request_coalesced_with_label_prefix_completes() {
  let a = "127.0.0.1:9057".parse().unwrap();
  let b = "127.0.0.1:9058".parse().unwrap();
  // a (the dialer) is pre-loaded with 1500 extra members and the receive
  // window is UNCAPPED. a writes the one-time label prefix and the whole
  // large join request into ONE coalesced byte buffer delivered to b in ONE
  // transport read while b is STILL in its label window (it has not yet
  // promoted its inbound `Stream`). b's bridge must strip the label, buffer
  // the trailing plaintext, and replay it the same tick it mints + promotes
  // its `Stream`; a dropped tail would truncate b's request, so b would never
  // merge a (and a would never learn b). Break as soon as the merge has
  // crossed both ways to avoid burning ticks probing the non-existent extras.
  let mut c = TcpCluster::two_node_join_large_request_coalesced(a, b, 1500);
  for _ in 0..20_000 {
    if c.sees_alive(b, &id("a"))
      && c.sees_alive(b, &id("extra-0"))
      && c.sees_alive(b, &id("extra-1499"))
      && c.sees_alive(a, &id("b"))
    {
      break;
    }
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(b, &id("a")),
    "b merged a's push from a request coalesced with a's one-time label prefix"
  );
  assert!(
    c.sees_alive(b, &id("extra-0")),
    "the dialer's first extra peer crossed in the coalesced request"
  );
  assert!(
    c.sees_alive(b, &id("extra-1499")),
    "the dialer's last extra peer crossed in the coalesced request"
  );
  assert!(
    c.sees_alive(a, &id("b")),
    "a learned b from the join reply (the exchange completed both ways)"
  );
}

#[test]
fn small_request_coalesced_with_label_no_fin_completes() {
  let a = "127.0.0.1:9059".parse().unwrap();
  let b = "127.0.0.1:9060".parse().unwrap();
  // a (the dialer) sends a SMALL join request (no preloaded extras) and the
  // receive window is UNCAPPED. a writes the one-time label prefix and the
  // small join request into ONE coalesced byte buffer delivered to b in ONE
  // transport read; b's inbound pipe WITHHOLDS its read==0 FIN anchor, so b
  // gets no later transport read to lean on. b must drain the buffered
  // request on the SAME tick it mints + promotes its inbound `Stream` (the
  // post-promotion drain); otherwise b never sees the request → never merges
  // a → never replies, and a never learns b (the join is the only path).
  // The small (no-retained-tail) sibling of the large-request-coalesced case,
  // structurally analogous to the TLS `close_notify`-then-no-FIN scenario.
  let mut c = TcpCluster::two_node_join_small_coalesced_no_fin(a, b);
  for _ in 0..20_000 {
    if c.sees_alive(b, &id("a")) && c.sees_alive(a, &id("b")) {
      break;
    }
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(b, &id("a")),
    "b merged a's push from a small request coalesced with the label prefix, \
     drained post-promotion with no retained tail and no later read==0"
  );
  assert!(
    c.sees_alive(a, &id("b")),
    "a learned b from the join reply (the exchange completed both ways)"
  );
}

#[test]
fn truncation_mid_frame_does_not_merge() {
  let a = "127.0.0.1:9061".parse().unwrap();
  let b = "127.0.0.1:9062".parse().unwrap();
  // The truncation constructor sets a TINY virtual-TCP receive window so the
  // dialer's coalesced `[label||request]` buffer is fragmented across
  // multiple TCP reads (plain TCP has no application-level record framing
  // on the wire, unlike TLS where the handshake and application records
  // naturally split the writes). The companion `half_close_tcp_after_first_record`
  // hook arms b's pipe to read==0 after the FIRST fragment; the rest of the
  // request never reaches b → the frame decode never reaches `Done` → no
  // merge is applied.
  let mut c = TcpCluster::two_node_join_truncated_mid_frame(a, b);
  c.half_close_tcp_after_first_record(b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    !c.ever_saw_alive(a, &id("b")),
    "a never merges a truncated reply"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "the truncated acceptor bridge must be reaped (no leaks); a 'bridge stays alive forever' regression would silently satisfy ever_saw_alive=false",
  );
}

#[test]
fn connect_refused_dial_fails_no_membership_change() {
  let a = "127.0.0.1:9065".parse().unwrap();
  let b = "127.0.0.1:9066".parse().unwrap();
  // `a` starts knowing nothing about `b` (no bootstrap alive entry), so the
  // TCP push/pull join is the ONLY path for either node to learn the other.
  // UDP gossip cannot bridge them: `a` has no membership entry for `b` to
  // gossip, and `b` has no entry for `a`, so neither side can emit an Alive
  // datagram about the other before the join merges.
  let mut c = TcpCluster::two_node_join(a, b);
  // Refuse `a`'s TCP connect to `b`: the dialer pipe is marked `reset`
  // immediately, the empty-slice EOF anchor surfaces on the next tick, and
  // the bridge drives `dial_failed` through its teardown — no `Stream` is
  // ever established, no push/pull frame is decoded, and no merge is applied.
  c.refuse_connect_between(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    !c.ever_saw_alive(a, &id("b")),
    "refused connect must not produce a membership merge on the dialer"
  );
  assert!(
    !c.ever_saw_alive(b, &id("a")),
    "refused connect must not produce a membership merge on the acceptor"
  );
  // The dialer's stalled bridge IS reaped at its dial deadline.
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "the dialer's stalled bridge is reaped at its deadline (no leak)"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "no acceptor bridge survives a connect that never arrived"
  );
}

#[test]
fn label_match_join_succeeds_both_sides() {
  // Happy-path label parity: both nodes carry the SAME default label
  // (`two_node_join` builds both with the harness default), the inbound
  // check passes, and the join converges. The pair of
  // `label_mismatch_rejected_no_side_effects`.
  let a = "127.0.0.1:9070".parse().unwrap();
  let b = "127.0.0.1:9071".parse().unwrap();
  let mut c = TcpCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "matching labels must allow the join to converge (A→B)"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "matching labels must allow the join to converge (B→A)"
  );
}

#[test]
fn acceptor_label_mismatch_rejected_no_side_effects() {
  // The responder carries a different label than the dialer; the inbound
  // label-check on the RESPONDER rejects the dialer's stream before any
  // payload is decoded. NO merge runs on either side and NO suspect / dead
  // side effects fire on the in-memory membership of either node. The
  // plain-TCP structural analogue of
  // `mtls_rejects_unauthenticated_client_no_side_effects` — the
  // cluster-isolation gate is the wire label rather than mutual auth, but
  // the observable contract is identical: the rejected dial produces no
  // merge / Stream / payload event.
  let a = "127.0.0.1:9031".parse().unwrap();
  let b = "127.0.0.1:9032".parse().unwrap();
  let mut c = TcpCluster::two_node_join_label_mismatch_responder(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    !c.ever_saw_alive(b, &id("a")),
    "rejected peer never merged into b"
  );
  assert!(
    !c.ever_saw_alive(a, &id("b")),
    "a's join reply never arrived"
  );
  assert!(
    !c.ever_suspected(a, &id("b")),
    "rejected dial produces no Suspect on the dialer"
  );
  assert!(
    !c.ever_suspected(b, &id("a")),
    "rejected dial produces no Suspect on the acceptor"
  );
  assert!(
    !c.ever_saw_gone(a, &id("b")),
    "rejected dial produces no Dead/Left on the dialer"
  );
  assert!(
    !c.ever_saw_gone(b, &id("a")),
    "rejected dial produces no Dead/Left on the acceptor"
  );
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "the dialer's bridge is reaped after the rejection (no leak)"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "the responder's label-rejection bridge is reaped (no leak)"
  );
}

#[test]
fn dialer_label_mismatch_rejects_response_no_membership_change() {
  // Symmetric dialer-side reject. `a` is built UNLABELED; `b` is labeled
  // `cluster-b` with `skip_inbound_label_check = true` so it suppresses the
  // missing-but-expected mismatch and accepts `a`'s unlabeled request as
  // plaintext. `b` therefore processes the request and merges `a` (the
  // acceptor-side merge runs before the response is sent — there's no way
  // to prevent that without rejecting `a`'s inbound). But `b`'s reply
  // carries `b`'s own outbound label `[12][len][cluster-b]...`, and an
  // unlabeled `a` rejects a labeled inbound as a double label (per the
  // truth table in `memberlist::codec`): `a` MUST NOT merge `b`. Proves
  // bidirectional wire-label validation catches cross-cluster traffic on
  // the response leg too, faithful to memberlist-core's `read_message` →
  // `decoder.with_label(...)` on `state.rs::push_pull_node`.
  //
  // The asymmetric merge here is intrinsic to the truth-table semantics
  // (`skip_inbound_label_check` only suppresses missing-but-expected, not
  // labeled-mismatch). The DIALER-side reject is what the test asserts;
  // the acceptor-side merge is a property of `b`'s suppressed policy.
  let a = "127.0.0.1:9033".parse().unwrap();
  let b = "127.0.0.1:9034".parse().unwrap();
  let mut c = TcpCluster::two_node_join_label_mismatch_responder_reply(a, b);
  // Cut UDP gossip across the (a, b) link so the only way `a` could learn
  // `b` is via the TCP response — what the dialer-side label-validation
  // gate is supposed to reject. Without this isolation `b` (which has
  // merged `a` from the inbound TCP request) would gossip its own state
  // to `a` over plain UDP and `a` would learn `b` indirectly, hiding the
  // dialer-side reject behind a parallel UDP merge.
  c.partition(&[a], &[b]);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    !c.ever_saw_alive(a, &id("b")),
    "a must NEVER merge b's wrong-cluster response"
  );
  assert!(
    !c.ever_suspected(a, &id("b")),
    "rejected response produces no Suspect on the dialer"
  );
  assert!(
    !c.ever_saw_gone(a, &id("b")),
    "rejected response produces no Dead/Left on the dialer"
  );
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "the dialer's bridge is reaped after rejecting the response (no leak)"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "the responder's bridge is reaped (no leak)"
  );
}

// ── Parity: the existing suspect/dead scenarios, but reliable exchanges ride
//    real plain TCP. Gossip rides UDP unchanged, so the SWIM transitions /
//    timing must match the plain harness exactly. Mirror of
//    `tests/suspect_dead.rs`.

/// Mirror of `tests/suspect_dead.rs::dropped_probe_leads_to_suspect`:
/// partition prober↔target so the probe Ping never arrives and the
/// reliable-ping fallback (over TCP) cannot complete either; after the
/// cumulative deadline the target is no longer Alive.
#[test]
fn parity_dropped_probe_leads_to_suspect() {
  let a = "127.0.0.1:9071".parse().unwrap();
  let b = "127.0.0.1:9072".parse().unwrap();
  let mut c = TcpCluster::two_node_alive(a, b);
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
/// The cluster is bootstrapped by a REAL TCP join (proving the TCP
/// composition is live); then — exactly as the plain test — a Suspect is
/// injected for a GHOST node that has no endpoint and therefore cannot
/// refute. After `suspicion_mult * probe_interval` (4 * 1s) the ghost must
/// transition Dead with the SAME timing as the plain harness: the TCP
/// composition must not perturb the suspicion timer.
#[test]
fn parity_suspect_transitions_to_dead_after_timeout() {
  let a = "127.0.0.1:9081".parse().unwrap();
  let b = "127.0.0.1:9082".parse().unwrap();
  let ghost_addr = "127.0.0.1:9099".parse().unwrap();
  let mut c = TcpCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "precondition: A sees B Alive over TCP"
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

// ── Compression: the membership conformance must hold UNCHANGED when reliable
//    exchanges and gossip datagrams ride lz4-compressed frames. Compression is
//    a wire-codec transform: it must be transparent to SWIM membership.

#[test]
fn compressed_two_node_join_over_tcp_reaches_alive_both_sides() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9601".parse().unwrap();
  let b = "127.0.0.1:9602".parse().unwrap();
  let mut c = TcpCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after a compressed TCP push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after a compressed TCP push/pull join — \
     compression must be transparent to membership"
  );
}

#[test]
fn compressed_join_matches_uncompressed_membership_outcome() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9611".parse().unwrap();
  let b = "127.0.0.1:9612".parse().unwrap();

  let mut plain = TcpCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !plain.step() {
      break;
    }
  }
  let mut compressed = TcpCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
  for _ in 0..20_000 {
    if !compressed.step() {
      break;
    }
  }
  // The membership end state is identical with and without compression.
  assert_eq!(
    plain.sees_alive(a, &id("b")),
    compressed.sees_alive(a, &id("b")),
    "A's view of B must match the uncompressed run"
  );
  assert_eq!(
    plain.sees_alive(b, &id("a")),
    compressed.sees_alive(b, &id("a")),
    "B's view of A must match the uncompressed run"
  );
}

#[test]
fn compressed_large_state_push_pull_completes_under_tcp_backpressure() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9621".parse().unwrap();
  let b = "127.0.0.1:9622".parse().unwrap();
  // a's push snapshot is pre-loaded with 200 extra members and the virtual TCP
  // receive window is tiny: the LARGE compressed push/pull snapshot is
  // fragmented across many reads, end-to-end exercising the compressed
  // reliable path's split-delivery reassembly. The join must still complete
  // with zero byte loss — every compressed reliable unit reassembles intact.
  let mut c = TcpCluster::two_node_join_small_window_compressed(a, b, 200, CompressAlgorithm::Lz4);
  for _ in 0..50_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "compressed join completes despite TCP backpressure (zero byte loss)"
  );
  assert!(
    c.sees_alive(b, &id("extra-0")),
    "every pushed peer crossed intact through the compressed reliable path"
  );
  assert!(c.sees_alive(b, &id("extra-199")));
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
    compress, encode_compressed_frame, framing, message_to_any, CompressAlgorithm,
  };

  let a: std::net::SocketAddr = "127.0.0.1:9651".parse().unwrap();
  let b: std::net::SocketAddr = "127.0.0.1:9652".parse().unwrap();
  // Use an isolated two-node cluster (no join handshake): b is the receiver,
  // a is the peer whose membership state the crafted datagram would install.
  let ghost_addr: std::net::SocketAddr = "127.0.0.1:9653".parse().unwrap();
  let mut c = TcpCluster::two_node_join(a, b);
  // Let the join complete so both nodes are live and their membership tables
  // are stable before we inject the crafted datagram.
  for _ in 0..20_000 {
    if c.sees_alive(a, &id("b")) && c.sees_alive(b, &id("a")) {
      break;
    }
    c.step();
  }
  // b must not know "ghost" before the crafted datagram.
  assert!(
    !c.sees_alive(b, &id("ghost")),
    "precondition: b has not observed ghost"
  );

  // Build a valid Alive frame for "ghost" — this is what the valid prefix
  // of the crafted datagram carries.
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
//    exchanges and gossip datagrams ride AEAD-encrypted frames. Encryption is
//    a wire-codec transform: it must be transparent to SWIM membership.

#[cfg(feature = "__sim-encryption-aes-gcm")]
#[test]
fn encrypted_two_node_join_over_tcp_reaches_alive_both_sides() {
  use memberlist_wire::SecretKey;
  let a = "127.0.0.1:9901".parse().unwrap();
  let b = "127.0.0.1:9902".parse().unwrap();
  let key = SecretKey::Aes256([0x42; 32]);
  let mut c = TcpCluster::two_node_join_encrypted(a, b, key);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after an encrypted TCP push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after an encrypted TCP push/pull join — \
     encryption must be transparent to membership"
  );
}

#[cfg(feature = "__sim-encryption-aes-gcm")]
#[test]
fn encrypted_join_over_tcp_matches_unencrypted_membership_outcome() {
  use memberlist_wire::SecretKey;
  let a = "127.0.0.1:9911".parse().unwrap();
  let b = "127.0.0.1:9912".parse().unwrap();

  let mut plain = TcpCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !plain.step() {
      break;
    }
  }
  let key = SecretKey::Aes256([0x99; 32]);
  let mut encrypted = TcpCluster::two_node_join_encrypted(a, b, key);
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

// ── Compound stack: compression + encryption together must not disturb SWIM.
//    The wire layout must be [Encrypted[[Compressed][frame]]] — encryption is
//    the outer wrapper, compression is the inner wrapper.

#[cfg(all(feature = "__sim-encryption-aes-gcm", feature = "compression-lz4"))]
#[test]
fn compressed_and_encrypted_join_over_tcp_matches_unencrypted_uncompressed_membership_outcome() {
  use memberlist_wire::{CompressAlgorithm, SecretKey};
  let a = "127.0.0.1:9981".parse().unwrap();
  let b = "127.0.0.1:9982".parse().unwrap();
  let mut plain = TcpCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !plain.step() {
      break;
    }
  }
  let key = SecretKey::Aes256([0xBB; 32]);
  let mut both =
    TcpCluster::two_node_join_compressed_and_encrypted(a, b, CompressAlgorithm::Lz4, key);
  for _ in 0..20_000 {
    if !both.step() {
      break;
    }
  }
  assert_eq!(plain.sees_alive(a, &id("b")), both.sees_alive(a, &id("b")));
  assert_eq!(plain.sees_alive(b, &id("a")), both.sees_alive(b, &id("a")));
}

#[cfg(all(feature = "__sim-encryption-aes-gcm", feature = "compression-lz4"))]
#[test]
fn compressed_and_encrypted_wire_layout_is_outer_encrypted_inner_compressed() {
  // Direct codec assertion: with both transforms enabled the unit's payload
  // begins with ENCRYPTED_TAG (outer) and the round-trip recovers the
  // original bytes, verifying that encrypt(compress(frame)) ordering holds.
  // For a small payload the leading varint fits in one byte, so unit[1] is
  // the first byte of the payload.
  use memberlist_wire::{
    encode_reliable_unit_with_encryption, take_reliable_unit_with_encryption, CompressAlgorithm,
    CompressionOptions, EncryptionOptions, Keyring, SecretKey,
  };
  let comp = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let enc = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0xCC; 32])));
  let framed = b"a payload large enough to compress and to encrypt".repeat(8);
  let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
  // The leading varint is a single byte for payloads < 128 B after the varint;
  // the byte at index 1 is the first byte of the encoded payload.
  assert_eq!(
    unit[1],
    memberlist_wire::ENCRYPTED_TAG,
    "outer wrapper is Encrypted (ENCRYPTED_TAG comes before the Compressed wrapper)"
  );
  // Round-trip: decrypt + decompress recovers the original frame sequence.
  let (back, _) = take_reliable_unit_with_encryption(&unit, &enc, 16 * 1024 * 1024)
    .expect("decode ok")
    .expect("complete unit");
  assert_eq!(
    back, framed,
    "round-trip recovers the original frame sequence"
  );
}
