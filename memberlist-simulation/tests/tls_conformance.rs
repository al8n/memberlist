#![cfg(feature = "__tls-harness")]
//! Membership conformance must hold UNCHANGED when reliable exchanges ride
//! real rustls-over-TCP instead of stubs. Asserts state / events / timing only
//! — never TLS wire bytes.
//!
//! The matrix:
//! - `two_node_join_over_tls_reaches_alive_both_sides` — join push/pull rides a
//!   real TLS-over-TCP connection; both nodes converge Alive.
//! - `push_pull_convergence_over_tls` — a periodic (non-join) push/pull merges
//!   both directions.
//! - `reliable_fallback_rescues_udp_blocked_probe_no_false_suspect` — direct +
//!   indirect UDP probes dropped: the probe completes via the TLS reliable-ping
//!   fallback and the healthy peer is NEVER falsely Suspected (the
//!   step-(2)-before-step-(3) ack-absorption ordering).
//! - `mtls_rejects_unauthenticated_client_no_side_effects` — a client with no
//!   cluster-CA cert fails the mutual handshake: NO `Stream` is minted, ZERO
//!   endpoint side effects (no merge / UserDataReceived / ack), the bridge is
//!   torn down.
//! - `leave_over_tls_observed_dead_or_left` — `TlsEndpoint::leave` fires
//!   `Event::LeftCluster`; the peer observes the local node Dead/Left.
//! - `large_state_push_pull_completes_under_tcp_backpressure` — a tiny virtual
//!   TCP receive window forces the writer to drain across many ticks; the join
//!   still completes with zero byte loss.
//! - `large_response_coalesced_read_completes` — the responder's push/pull
//!   reply exceeds rustls's 16 KiB received-plaintext limit and is delivered as
//!   ONE coalesced TCP read (uncapped window): the coordinator interleaves
//!   bounded record intake with plaintext draining (no panic on rustls
//!   backpressure) and the join converges. The masked path the small-window
//!   test does not cover.
//! - `large_request_coalesced_with_handshake_flight_completes` — the DIALER's
//!   >16 KiB join request is delivered to the responder COALESCED with the
//!   dialer's final TLS flight in one transport read, while the responder is
//!   still completing the handshake (no `Stream` yet). The responder must retain
//!   the unconsumed ciphertext tail across the handshake-completion mint and
//!   replay it the same tick so the full request reassembles and both sides
//!   converge. The pre-promotion analogue of the large-response case.
//! - `small_request_coalesced_with_handshake_flight_no_fin_completes` — the
//!   DIALER's SMALL join request is delivered to the responder COALESCED with
//!   the dialer's final TLS flight + `close_notify` in one transport read, while
//!   the responder is still handshaking. The whole small read is consumed in one
//!   pass, so NO backpressure trips and NO ciphertext tail is retained — yet the
//!   decrypted request sits buffered in the record layer. With the responder's
//!   read==0 FIN anchor withheld (no later transport read), the responder must
//!   drain that buffered request on the SAME tick it mints + promotes its
//!   `Stream` to merge and reply. The small (no-retained-tail) sibling of the
//!   large-request case.
//! - `truncation_mid_frame_does_not_merge` — the peer half-closes the TCP
//!   connection mid-frame (read==0 before a frame completes): the partial frame
//!   never reaches `Done`, so no merge is applied.
//!
//! Parity (the capstone): the existing suspect/dead scenarios pass UNCHANGED
//! through `TlsEndpoint` (the `parity_*` tests below mirror
//! `tests/suspect_dead.rs`), and the existing non-tls suite is byte-unchanged
//! and still green under the parity gate `cargo test -p memberlist-simulation`.

use memberlist_simulation::tls_net::TlsCluster;
use memberlist_wire::typed::State;
use smol_str::SmolStr;
use std::time::Duration;

fn id(s: &str) -> SmolStr {
  SmolStr::new(s)
}

#[test]
fn two_node_join_over_tls_reaches_alive_both_sides() {
  let a = "127.0.0.1:8001".parse().unwrap();
  let b = "127.0.0.1:8002".parse().unwrap();
  let mut c = TlsCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after TLS push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after TLS push/pull join"
  );
}

#[test]
fn push_pull_convergence_over_tls() {
  let a = "127.0.0.1:8011".parse().unwrap();
  let b = "127.0.0.1:8012".parse().unwrap();
  let mut c = TlsCluster::two_node_alive(a, b);
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
  let a = "127.0.0.1:8021".parse().unwrap();
  let b = "127.0.0.1:8022".parse().unwrap();
  let mut c = TlsCluster::two_node_join_slow_probe(a, b);
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
fn mtls_rejects_unauthenticated_client_no_side_effects() {
  let a = "127.0.0.1:8031".parse().unwrap();
  let b = "127.0.0.1:8032".parse().unwrap();
  // `b` requires a cluster-CA client cert; `a` presents none → handshake fails.
  let mut c = TlsCluster::two_node_join_mtls_required_responder(a, b);
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
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "a's failed-handshake bridge torn down"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "b minted no Stream and tore the bridge down"
  );
}

#[test]
fn leave_over_tls_observed_dead_or_left() {
  let a = "127.0.0.1:8041".parse().unwrap();
  let b = "127.0.0.1:8042".parse().unwrap();
  let mut c = TlsCluster::two_node_join(a, b);
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
  let a = "127.0.0.1:8051".parse().unwrap();
  let b = "127.0.0.1:8052".parse().unwrap();
  // a's push snapshot is pre-loaded with many extra members; the virtual TCP
  // receive window is tiny, so the ciphertext drains across many ticks.
  let mut c = TlsCluster::two_node_join_small_window(a, b, 200);
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
  let a = "127.0.0.1:8055".parse().unwrap();
  let b = "127.0.0.1:8056".parse().unwrap();
  // b (the responder) is pre-loaded with 1500 extra members and the receive
  // window is UNCAPPED, so b's push/pull RESPONSE crosses to a as ONE coalesced
  // TCP read whose plaintext spans several 16 KiB TLS records. The intake loop
  // therefore sees rustls's buffer-full backpressure with ciphertext still
  // unconsumed — the exact condition that panicked before the interleave fix,
  // and the case the small-window large_state test masks. Break as soon as the
  // merge has crossed (the join completes early) rather than running to idle,
  // which would otherwise burn many ticks probing the non-existent extra peers.
  let mut c = TlsCluster::two_node_join_large_response_coalesced(a, b, 1500);
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
    "join completes from a single coalesced >16 KiB response read"
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
fn large_request_coalesced_with_handshake_flight_completes() {
  let a = "127.0.0.1:8057".parse().unwrap();
  let b = "127.0.0.1:8058".parse().unwrap();
  // a (the dialer) is pre-loaded with 1500 extra members and the receive window
  // is UNCAPPED. a mints + pumps its outbound Stream the SAME tick its handshake
  // completes, so a's final TLS flight (client Finished) and the whole >16 KiB
  // join request drain into ONE coalesced ciphertext buffer delivered to b in
  // ONE transport read WHILE b IS STILL HANDSHAKING (b has not consumed a's
  // Finished). b's record layer completes the handshake AND buffers the trailing
  // app records in the same call, and the >16 KiB request trips backpressure
  // before b has a Stream. b must retain the unconsumed tail across the
  // handshake-completion mint and replay it the same tick; a dropped tail would
  // truncate b's request, so b would never merge a (and a would never learn b,
  // since the join is the only path). Break as soon as the merge has
  // crossed both ways to avoid burning ticks probing the non-existent extras.
  let mut c = TlsCluster::two_node_join_large_request_coalesced(a, b, 1500);
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
    "b merged a's push from a request coalesced with a's final handshake flight"
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
fn small_request_coalesced_with_handshake_flight_no_fin_completes() {
  let a = "127.0.0.1:8059".parse().unwrap();
  let b = "127.0.0.1:8060".parse().unwrap();
  // a (the dialer) sends a SMALL join request (no preloaded extras) and the
  // receive window is UNCAPPED. a mints + pumps its outbound Stream the SAME
  // tick its handshake completes, so a's final TLS flight (client Finished),
  // its small join request, and its close_notify drain into ONE coalesced
  // ciphertext buffer delivered to b in ONE transport read WHILE b is STILL
  // HANDSHAKING. The whole small read is consumed by rustls in ONE pass — NO
  // backpressure, so b retains NO ciphertext tail — yet the decrypted request
  // sits in b's received-plaintext buffer and peer_has_closed() is latched.
  //
  // b's inbound pipe WITHHOLDS its read==0 FIN anchor, so b gets no later
  // transport read to lean on: it must drain the buffered request on the SAME
  // tick it mints + promotes its inbound Stream to merge a's push and reply.
  // The post-promotion drain must run even on an empty tail: otherwise b
  // never sees the request → never merges a → never replies, and a never learns
  // b (the join is the only path). This is the small sibling of the
  // large-request-coalesced case. Break as soon as the merge crosses both ways.
  let mut c = TlsCluster::two_node_join_small_coalesced_no_fin(a, b);
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
    "b merged a's push from a small request coalesced with a's final handshake \
     flight, drained post-promotion with no retained tail and no later read==0"
  );
  assert!(
    c.sees_alive(a, &id("b")),
    "a learned b from the join reply (the exchange completed both ways)"
  );
}

#[test]
fn truncation_mid_frame_does_not_merge() {
  let a = "127.0.0.1:8061".parse().unwrap();
  let b = "127.0.0.1:8062".parse().unwrap();
  let mut c = TlsCluster::two_node_join(a, b);
  c.half_close_tcp_after_first_record(b); // b half-closes mid-frame on the inbound exchange
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    !c.ever_saw_alive(a, &id("b")),
    "a never merges a truncated reply"
  );
}

#[test]
fn connect_refused_dial_fails_no_membership_change() {
  let a = "127.0.0.1:8065".parse().unwrap();
  let b = "127.0.0.1:8066".parse().unwrap();
  // `a` starts knowing nothing about `b` (no bootstrap alive entry), so the
  // TLS push/pull join is the ONLY path for either node to learn the other.
  // UDP gossip cannot bridge them: `a` has no membership entry for `b` to
  // gossip, and `b` has no entry for `a`, so neither side can emit an Alive
  // datagram about the other before the join merges.
  let mut c = TlsCluster::two_node_join(a, b);
  // Refuse `a`'s TCP connect to `b`: the dialer pipe is marked `reset`
  // immediately, the empty-slice anchor surfaces on the next tick, and the
  // bridge drives `dial_failed` through its teardown — no `Stream` is ever
  // established, no push/pull frame is decoded, and no merge is applied.
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
  // The dialer's stalled `Handshaking` bridge IS reaped at its dial deadline:
  // the empty-slice reset anchor is a no-op while the handshake is in flight
  // (rustls `process_new_packets` succeeds on an empty feed), so only the
  // bridge-level handshake deadline terminalizes it. A leak here is the
  // connect-refused / slowloris bridge leak.
  assert_eq!(
    c.live_bridge_count(a),
    0,
    "the dialer's stalled handshake bridge is reaped at its deadline (no leak)"
  );
  assert_eq!(
    c.live_bridge_count(b),
    0,
    "no acceptor bridge survives a connect that never arrived"
  );
}

// ── Parity: the existing suspect/dead scenarios, but reliable exchanges ride
//    real TLS-over-TCP. Gossip rides UDP unchanged, so the SWIM transitions /
//    timing must match the plain harness exactly. Mirror of
//    `tests/suspect_dead.rs`.

/// Mirror of `tests/suspect_dead.rs::dropped_probe_leads_to_suspect`:
/// partition prober↔target so the probe Ping never arrives and the
/// reliable-ping fallback (over TLS) cannot complete either; after the
/// cumulative deadline the target is no longer Alive.
#[test]
fn parity_dropped_probe_leads_to_suspect() {
  let a = "127.0.0.1:8071".parse().unwrap();
  let b = "127.0.0.1:8072".parse().unwrap();
  let mut c = TlsCluster::two_node_alive(a, b);
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
/// The cluster is bootstrapped by a REAL TLS join (proving the TLS composition
/// is live); then — exactly as the plain test — a Suspect is injected for a
/// GHOST node that has no endpoint and therefore cannot refute. After
/// `suspicion_mult * probe_interval` (4 * 1s) the ghost must transition Dead
/// with the SAME timing as the plain harness: the TLS composition must not
/// perturb the suspicion timer.
#[test]
fn parity_suspect_transitions_to_dead_after_timeout() {
  let a = "127.0.0.1:8081".parse().unwrap();
  let b = "127.0.0.1:8082".parse().unwrap();
  let ghost_addr = "127.0.0.1:8099".parse().unwrap();
  let mut c = TlsCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "precondition: A sees B Alive over TLS"
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
fn compressed_two_node_join_over_tls_reaches_alive_both_sides() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9701".parse().unwrap();
  let b = "127.0.0.1:9702".parse().unwrap();
  let mut c = TlsCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
  for _ in 0..20_000 {
    if !c.step() {
      break;
    }
  }
  assert!(
    c.sees_alive(a, &id("b")),
    "A must see B Alive after a compressed TLS push/pull join"
  );
  assert!(
    c.sees_alive(b, &id("a")),
    "B must see A Alive after a compressed TLS push/pull join — \
     compression must be transparent to membership"
  );
}

#[test]
fn compressed_join_over_tls_matches_uncompressed_membership_outcome() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9711".parse().unwrap();
  let b = "127.0.0.1:9712".parse().unwrap();
  let mut plain = TlsCluster::two_node_join(a, b);
  for _ in 0..20_000 {
    if !plain.step() {
      break;
    }
  }
  let mut compressed = TlsCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
  for _ in 0..20_000 {
    if !compressed.step() {
      break;
    }
  }
  assert_eq!(
    plain.sees_alive(a, &id("b")),
    compressed.sees_alive(a, &id("b")),
    "A's view of B must match the uncompressed TLS run"
  );
  assert_eq!(
    plain.sees_alive(b, &id("a")),
    compressed.sees_alive(b, &id("a")),
    "B's view of A must match the uncompressed TLS run"
  );
}

#[test]
fn compressed_large_state_push_pull_completes_under_tcp_backpressure() {
  use memberlist_wire::CompressAlgorithm;
  let a = "127.0.0.1:9721".parse().unwrap();
  let b = "127.0.0.1:9722".parse().unwrap();
  // a's push snapshot is pre-loaded with 200 extra members and the virtual TCP
  // receive window is tiny: the LARGE compressed push/pull snapshot is
  // fragmented across many reads, end-to-end exercising the compressed
  // reliable path's split-delivery reassembly through the TLS record layer.
  // The join must still complete with zero byte loss — every compressed
  // reliable unit reassembles intact.
  let mut c = TlsCluster::two_node_join_small_window_compressed(a, b, 200, CompressAlgorithm::Lz4);
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

  let a: std::net::SocketAddr = "127.0.0.1:9731".parse().unwrap();
  let b: std::net::SocketAddr = "127.0.0.1:9732".parse().unwrap();
  let ghost_addr: std::net::SocketAddr = "127.0.0.1:9733".parse().unwrap();
  let mut c = TlsCluster::two_node_join(a, b);
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
