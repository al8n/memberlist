//! Coordinator ([`super::StreamEndpoint`]) and bridge
//! ([`super::bridge::StreamBridge`]) edge-path coverage, driven through the
//! plain-TCP record layer (`RawRecords`) and, for the handshake-gated paths
//! that only a real TLS handshake reaches, the `Labeled<TlsRecords>` layer.
//!
//! These exercise the scattered error / no-op / immediate-due branches the
//! `tcp::tests` and `tls::tests` driver-parity suites do not reach: the
//! gossip-ingress backstop, the `poll_timeout` immediate-due folds, the
//! half-close `observe_*` no-op arms, the pre-`Stream` EOF reject, and the
//! TLS dial-retired terminal-event emission.

#[cfg(feature = "tcp")]
mod tcp {
  use crate::Instant;
  use core::{net::SocketAddr, time::Duration};

  use bytes::Bytes;
  use smol_str::SmolStr;

  use std::collections::HashMap;

  use crate::{
    RawRecords,
    event::{DialAbortReason, Event, ExchangeKind, ExchangeStatus, PushPullKind, StreamId},
    streams::{
      LabelOptions, StreamAction, StreamEndpoint,
      bridge::StreamBridge,
      test_support::{
        addr, endpoint, handshaking_pair as shared_handshaking_pair, label, test_peer_to_socket,
        test_sni_provider,
      },
    },
  };

  fn coord(port: u16) -> StreamEndpoint<SmolStr, SocketAddr, RawRecords> {
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    StreamEndpoint::new(
      endpoint(port),
      cfg,
      test_sni_provider(),
      test_peer_to_socket(),
    )
  }

  /// Build a `Handshaking` dialer/acceptor `RawRecords` bridge pair over a
  /// shared cluster label (the same delegation `tcp::bridge`'s tests use).
  fn handshaking_pair(
    cluster: &str,
    deadline: Instant,
  ) -> (
    StreamBridge<SmolStr, SocketAddr, RawRecords>,
    StreamBridge<SmolStr, SocketAddr, RawRecords>,
  ) {
    shared_handshaking_pair(
      deadline,
      || RawRecords::dialer(label(cluster), false),
      || RawRecords::acceptor(label(cluster), false),
    )
  }

  /// Pump each side's outbound label prefix across so both inbound-label
  /// validations settle (mirrors `tcp::bridge::complete_label_exchange`).
  fn complete_label_exchange(
    client: &mut StreamBridge<SmolStr, SocketAddr, RawRecords>,
    server: &mut StreamBridge<SmolStr, SocketAddr, RawRecords>,
    now: Instant,
  ) {
    let mut client_prefix = Vec::new();
    client.poll_transport_transmit(&mut client_prefix);
    server
      .handle_transport_data(&client_prefix, now)
      .expect("acceptor accepts the matching label prefix");
    let mut server_prefix = Vec::new();
    server.poll_transport_transmit(&mut server_prefix);
    client
      .handle_transport_data(&server_prefix, now)
      .expect("dialer accepts the matching inbound label prefix");
  }

  /// A `requeue_event(DialRequested)` on a LEFT coordinator drops the intent:
  /// the `!is_running()` guard returns before pushing to `dial_pending`, so no
  /// bridge is built and no Connect surfaces. Covers the leaving-node early
  /// return in `requeue_event`'s `DialRequested` arm.
  #[test]
  fn requeue_dial_requested_after_leave_is_dropped() {
    let now = Instant::now();
    let mut coord = coord(7100);
    coord.leave(now).expect("leave from a running node");

    // A held DialRequested re-queued after leave must NOT restart a dial.
    coord.requeue_event(
      Event::DialRequested(crate::event::DialRequested::new(
        StreamId::from_raw(0),
        addr(7000),
        now + Duration::from_secs(5),
      )),
      now,
    );
    coord.service_dials(now);

    assert_eq!(
      coord.live_bridge_count(),
      0,
      "a left node builds no bridge for a re-queued DialRequested",
    );
    assert!(
      coord.poll_action().is_none(),
      "a left node surfaces no Connect for a re-queued DialRequested",
    );
  }

  /// The gossip-ingress backstop: once `mem_ingress` holds
  /// `MAX_MEM_INGRESS_DATAGRAMS` buffered datagrams, a further `handle_gossip`
  /// is dropped (the `gossip_ingress_dropped` metric increments and the
  /// datagram is not buffered) rather than growing the buffer without bound.
  #[test]
  fn handle_gossip_drops_past_ingress_cap() {
    let now = Instant::now();
    let mut coord = coord(7101);

    // The cap is 8192 (MAX_MEM_INGRESS_DATAGRAMS). Fill exactly to the cap.
    const CAP: usize = 8192;
    for _ in 0..CAP {
      coord.handle_gossip(addr(7000), b"g", now);
    }
    assert_eq!(
      coord.pending_memberlist_ingress(),
      CAP,
      "the buffer fills to the cap",
    );

    // The next datagram is dropped: the buffer does not grow past the cap.
    coord.handle_gossip(addr(7000), b"overflow", now);
    assert_eq!(
      coord.pending_memberlist_ingress(),
      CAP,
      "a datagram past the cap is dropped, not buffered",
    );
  }

  /// `poll_timeout` folds an immediate-due wake derived from `last_now` over an
  /// ALREADY-`Some` best when an unattempted pending dial exists AND a live
  /// bridge contributed a (future) deadline first. Exercises the
  /// `best.map_or(anchor, |b| b.min(anchor))` `min` branch of the
  /// `has_unattempted` term (line where best is already `Some` from the bridge).
  #[test]
  fn poll_timeout_unattempted_dial_min_folds_over_existing_best() {
    let now = Instant::now();
    let mut coord = coord(7102);

    // One in-band dial → a live Active bridge whose future deadline sets `best`
    // to `Some` before the dial-pending term runs.
    coord
      .start_user_message(addr(7000), Bytes::from_static(b"a"), now)
      .expect("issued while running");
    assert!(
      coord.live_bridge_count() >= 1,
      "the dial built a live bridge"
    );

    // A raw dial sieved into dial_pending (unattempted) so `has_unattempted`
    // holds and `last_now` (anchored by start_user_message) folds in via `min`.
    coord
      .endpoint_mut()
      .start_push_pull(addr(7004), PushPullKind::Join, now);
    while coord.poll_event().is_some() {}

    let t = coord
      .poll_timeout()
      .expect("a live bridge + a pending dial contribute a deadline");
    assert!(
      t <= now,
      "an unattempted pending dial folds an immediate-due wake over the \
         bridge's future deadline via min, got a future {t:?}",
    );
  }

  /// `push_teardown` accepts each non-Connect teardown variant (the debug-assert
  /// `matches!` holds) and `poll_action` surfaces them in producer order behind
  /// any queued Connect. Drives the `Shutdown`/`Close`/`Abort` teardown-variant
  /// arms of the `push_teardown` debug-assert.
  #[test]
  fn push_teardown_accepts_every_teardown_variant() {
    use crate::streams::{ExchangeId, ExchangeRef};
    let mut coord = coord(7103);
    let id = ExchangeId::new(5);
    coord.push_teardown(StreamAction::Shutdown(ExchangeRef::new(id)));
    coord.push_teardown(StreamAction::Close(ExchangeRef::new(id)));
    coord.push_teardown(StreamAction::Abort(ExchangeRef::new(id)));

    let mut kinds = Vec::new();
    while let Some(a) = coord.poll_action() {
      kinds.push(match a {
        StreamAction::Shutdown(_) => "Shutdown",
        StreamAction::Close(_) => "Close",
        StreamAction::Abort(_) => "Abort",
        StreamAction::Connect(_) => "Connect",
      });
    }
    assert_eq!(
      kinds,
      ["Shutdown", "Close", "Abort"],
      "every pushed teardown surfaces in producer order",
    );
  }

  /// A `Stream`-less bridge — one whose label / handshake step has not settled
  /// — drives the half-close `observe_*` early-return arms indirectly: the
  /// handshaking acceptor's `pump_out` never reaches `observe_send_fin`, so its
  /// FIN stays un-owed. Asserts the no-op shape: a fresh acceptor bridge that
  /// received nothing owes no FIN and is not terminal.
  #[test]
  fn handshaking_bridge_pump_owes_no_fin() {
    let now = Instant::now();
    let (_client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    // Pump the still-handshaking acceptor: no `Stream`, so the early
    // `self.stream.is_none()` return fires and no send-half transition runs.
    server.pump_out(now).expect("a handshaking pump is a no-op");
    assert!(!server.fin_owed(), "a handshaking bridge owes no FIN");
    assert!(
      !server.is_terminal(),
      "a handshaking bridge is not terminal"
    );
  }

  /// A full clean push/pull exchange drives both half-close transitions to
  /// `BothClosed`, sweeping `observe_send_fin` (`Active -> SendClosed`) and
  /// `observe_recv_fin` (`SendClosed -> BothClosed`, then the already-closed
  /// `return` arm on the second EOF). Verifies both bridges reap with the
  /// inbound merge applied and the recv-half no-op arm taken on a redundant EOF.
  #[test]
  fn clean_exchange_drives_both_observe_arms_to_bothclosed() {
    use crate::{
      bridge_phase::LinkState, config::EndpointOptions, endpoint::Endpoint,
      streams::phase::BridgePhase,
    };
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    complete_label_exchange(&mut client, &mut server, now);

    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7600)));
    let sid = ep_c.start_push_pull(addr(7000), PushPullKind::Join, now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound push/pull stream");
    client.promote(c_stream);

    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
    let s_stream = ep_s
      .accept_stream(addr(7600), now)
      .expect("node is running");
    server.promote(s_stream);

    let mut c_fin = false;
    let mut s_fin = false;
    for _ in 0..128 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      // Shuttle bytes and one-shot FINs both directions.
      let mut moved = false;
      let mut c_out = Vec::new();
      client.poll_transport_transmit(&mut c_out);
      if !c_out.is_empty() {
        let _ = server.handle_transport_data(&c_out, now);
        moved = true;
      }
      let mut s_out = Vec::new();
      server.poll_transport_transmit(&mut s_out);
      if !s_out.is_empty() {
        let _ = client.handle_transport_data(&s_out, now);
        moved = true;
      }
      if client.fin_owed() && !c_fin {
        c_fin = true;
        // Deliver the dialer FIN twice: the first drives the acceptor's recv
        // transition, the redundant second exercises the already-closed
        // `observe_recv_fin` `return` arm.
        let _ = server.handle_transport_data(&[], now);
        let _ = server.handle_transport_data(&[], now);
        moved = true;
      }
      if server.fin_owed() && !s_fin {
        s_fin = true;
        let _ = client.handle_transport_data(&[], now);
        let _ = client.handle_transport_data(&[], now);
        moved = true;
      }
      if !client.is_terminal() {
        client.drain_payload_only(&mut ep_c, now);
      }
      if !server.is_terminal() {
        server.drain_payload_only(&mut ep_s, now);
      }
      if client.is_terminal() && server.is_terminal() {
        break;
      }
      if !moved {
        break;
      }
    }

    assert!(
      matches!(
        server.phase_ref(),
        BridgePhase::Established(LinkState::BothClosed)
      ),
      "the acceptor reached BothClosed via observe_send_fin + observe_recv_fin",
    );
    assert!(
      matches!(
        client.phase_ref(),
        BridgePhase::Established(LinkState::BothClosed)
      ),
      "the dialer reached BothClosed",
    );

    server.drain_then_reap(&mut ep_s, now);
    assert!(
      ep_s.member(&SmolStr::new("cli")).is_some(),
      "the acceptor merged the dialer's view after the clean reap",
    );
  }

  /// A pre-`Stream` EOF delivered while the acceptor's inbound label has NOT yet
  /// validated (`records.is_handshaking()` is still true) fails the bridge with
  /// `ConnectionLost`: the peer half-closed before establishing the exchange, so
  /// the handshake can never complete. Covers the `is_handshaking()` arm of the
  /// pre-`Stream` empty-slice branch in `handle_transport_data`.
  #[test]
  fn pre_stream_eof_while_handshaking_fails_connection_lost() {
    use crate::{bridge_phase::LinkState, streams::phase::BridgePhase};
    let now = Instant::now();
    let (_client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    // The acceptor has received no inbound label, so it is still handshaking.
    assert!(server.is_handshaking(), "acceptor starts handshaking");

    // Deliver a bare transport EOF (read == 0) before any label arrives.
    let res = server.handle_transport_data(&[], now);
    assert!(res.is_err(), "a pre-label EOF fails the acceptor bridge");
    assert!(
      matches!(
        server.phase_ref(),
        BridgePhase::Established(LinkState::Failed(_))
      ),
      "a pre-`Stream` EOF while handshaking terminalizes the bridge",
    );
    assert!(server.is_terminal(), "the bridge is terminal after the EOF");
  }

  /// A partial label prefix (fewer bytes than the full `[12][len][label]`)
  /// leaves the acceptor still handshaking after the record layer consumes all
  /// supplied bytes: `intake_handshaking` buffers the partial header and returns
  /// without minting. Drives the `Intake::Done`-but-still-handshaking break in
  /// `intake_handshaking`.
  #[test]
  fn partial_label_prefix_stays_handshaking() {
    let now = Instant::now();
    let (_client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    // Deliver only the first byte of the label frame (the LABELED_TAG); the
    // length byte and label body are withheld.
    server
      .handle_transport_data(&[12u8], now)
      .expect("a partial label header is buffered, not rejected");
    assert!(
      server.is_handshaking(),
      "a partial label leaves the acceptor handshaking (no mint yet)",
    );
    assert!(!server.is_terminal(), "a partial label is not a failure");
  }

  /// A bridge's `poll_timeout` returns `None` once it is terminal (the reap is
  /// this same tick, so it contributes no deadline to the coordinator's unified
  /// `min`). Drives the `is_terminal()` early-`None` arm of the bridge
  /// `poll_timeout`.
  #[test]
  fn terminal_bridge_contributes_no_timeout() {
    let now = Instant::now();
    let (_client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    // A handshaking bridge has a (handshake-deadline) timeout.
    assert!(
      server.poll_timeout().is_some(),
      "a live handshaking bridge contributes its accept deadline",
    );
    // Reject a wrong-cluster label → terminal.
    let mut wrong = vec![12u8, 7];
    wrong.extend_from_slice(b"other-x");
    let _ = server.handle_transport_data(&wrong, now);
    assert!(server.is_terminal());
    assert!(
      server.poll_timeout().is_none(),
      "a terminal bridge contributes no deadline",
    );
  }

  /// A second `handle_transport_data` after the bridge has already terminalized
  /// is a no-op (the terminal-ingress stop returns `Ok(())` without re-entering
  /// the intake), so a post-failure network read cannot commit further events.
  /// Covers the terminal guards at the `handle_transport_data` /
  /// `pump_in_established` entry.
  #[test]
  fn post_terminal_transport_data_is_ignored() {
    let now = Instant::now();
    let (_client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));

    // Reject a wrong-cluster label → the acceptor terminalizes.
    let mut wrong = vec![12u8, 7];
    wrong.extend_from_slice(b"other-x");
    let first = server.handle_transport_data(&wrong, now);
    assert!(first.is_err(), "the label mismatch terminalizes the bridge");
    assert!(server.is_terminal());

    // A further read on the terminal bridge is silently ignored (Ok, no panic,
    // still terminal).
    let second = server.handle_transport_data(b"more bytes", now);
    assert!(
      second.is_ok(),
      "a terminal bridge accepts no further bytes and returns Ok",
    );
    assert!(server.is_terminal(), "the bridge stays terminal");
  }

  /// The membership forwarders that mutate inner-endpoint state and the
  /// `last_now` anchor: `start_probe`, `handle_suspect`, and `ping` each set
  /// `last_now` so a subsequent `poll_timeout` has an anchor. Asserts each
  /// forwards (no panic) and the endpoint observes the effect.
  #[test]
  fn start_probe_anchors_last_now_for_immediate_reap_wake() {
    let now = Instant::now();
    let mut coord = coord(7104);
    // Seed an alive peer so a probe has a target.
    coord.handle_alive(
      addr(7000),
      crate::typed::Alive::new(1, crate::Node::new(SmolStr::new("p"), addr(7000))),
      now,
    );
    // start_probe forwards and anchors last_now.
    let _started = coord.start_probe(now);
    // A pending dial sieved unattempted now resolves its immediate-due wake
    // against the anchored last_now.
    coord
      .endpoint_mut()
      .start_push_pull(addr(7000), PushPullKind::Join, now);
    while coord.poll_event().is_some() {}
    let t = coord.poll_timeout().expect("a deadline source exists");
    assert!(
      t <= now,
      "the probe anchored last_now for the immediate-due wake"
    );
  }

  /// A runtime `set_encryption_options` that fails a live insecure-transport
  /// bridge sets the `policy_reap_pending` latch, and `poll_timeout` then folds
  /// an immediate-due wake (`last_now`) over an ALREADY-`Some` best contributed
  /// by the inner endpoint's scheduler timer. Drives the `policy_reap_pending`
  /// `best.map_or(anchor, |b| b.min(anchor))` `min` branch (best already Some).
  #[cfg(feature = "aes-gcm")]
  #[test]
  fn poll_timeout_policy_reap_min_folds_over_scheduler_timer() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let mut coord = coord(7105);
    // Arm the periodic schedulers so `ep.poll_timeout()` returns a FUTURE
    // instant — the `Some` best the policy-reap term then folds over.
    coord.start_scheduling(now);

    // Build a live insecure-transport bridge under the default disabled policy.
    coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);
    while coord.poll_action().is_some() {}
    assert!(
      coord.live_bridge_count() >= 1,
      "the dial built a live bridge"
    );

    // A key rotation fails the bridge (insecure transport) and latches the reap.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    // The scheduler set a future `best`; the policy-reap latch folds last_now
    // (== now, an immediate-due wake) over it via min.
    let t = coord
      .poll_timeout()
      .expect("the scheduler timer always contributes a deadline");
    assert!(
      t <= now,
      "the policy-reap latch folds an immediate-due wake over the scheduler's \
         future timer, got {t:?}",
    );
  }

  /// A runtime `set_encryption_options` enqueues an `Abort` for each
  /// newly-failed insecure-transport bridge (the teardown the failure cascade
  /// owes the driver), which `poll_action` then surfaces. Exercises the
  /// failed-bridge teardown-enqueue path of `set_encryption_options` and the
  /// subsequent reap.
  #[cfg(feature = "aes-gcm")]
  #[test]
  fn set_encryption_options_enqueues_abort_for_failed_bridge() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let mut coord = coord(7106);
    coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);
    let exchange = match coord.poll_action().expect("the dial surfaces a Connect") {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {other:?}"),
    };
    // Drain the dialer's queued label bytes so the per-exchange teardown gate
    // does not withhold the Abort behind them.
    while coord.poll_transport_transmit().is_some() {}

    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    // The policy change synchronously enqueued an Abort for the failed bridge.
    let abort_seen = core::iter::from_fn(|| coord.poll_action())
      .any(|a| matches!(a, StreamAction::Abort(r) if r.id() == exchange));
    assert!(
      abort_seen,
      "set_encryption_options enqueues an Abort for the failed insecure bridge",
    );

    // The latched reap clears the bridge on the next tick.
    coord.handle_timeout(now);
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the policy-failed bridge is reaped",
    );
  }

  /// `accept_connection` admission-gates inbound exchanges against the optional
  /// `max_inbound_streams` ceiling: once the live inbound bridge count reaches
  /// the cap, a further accept returns `None` (and the
  /// `inbound_streams_rejected` metric increments) so a peer cannot grow inbound
  /// bridge state without bound. Drives the over-cap reject arm of
  /// `accept_connection`.
  #[test]
  fn accept_connection_rejects_over_max_inbound_streams() {
    use crate::{config::EndpointOptions, endpoint::Endpoint};

    let now = Instant::now();
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    // Cap inbound exchanges at 1.
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
      EndpointOptions::new(SmolStr::new("srv"), addr(7108)).with_max_inbound_streams(Some(1)),
    );
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // First inbound accept is admitted (fills the single slot).
    let _first = coord
      .accept_connection(addr(7000), now)
      .expect("the first inbound exchange is admitted under the cap");
    assert_eq!(coord.live_bridge_count(), 1);

    // A second inbound accept exceeds the cap and is rejected: no bridge built.
    assert!(
      coord.accept_connection(addr(7001), now).is_none(),
      "an inbound accept past max_inbound_streams must be rejected",
    );
    assert_eq!(
      coord.live_bridge_count(),
      1,
      "the rejected accept built no bridge",
    );
  }

  /// `queue_user_broadcast_ranked` enqueues at the requested priority tier and
  /// increments the inner endpoint's user-broadcast queue length. Mirrors the
  /// inner `Endpoint::queue_user_broadcast_ranked` contract: rank 0 is the
  /// highest priority tier; an out-of-range rank saturates to the lowest tier
  /// rather than being rejected.
  #[test]
  fn queue_user_broadcast_ranked_forwards_to_inner_endpoint() {
    let mut coord = coord(7108);

    assert_eq!(
      coord.endpoint_ref().user_broadcast_queue_len(),
      0,
      "fresh coordinator has an empty broadcast queue",
    );
    coord
      .queue_user_broadcast_ranked(0, Bytes::from_static(b"high-priority"))
      .expect("in-budget payload enqueues at rank 0");
    assert_eq!(
      coord.endpoint_ref().user_broadcast_queue_len(),
      1,
      "one ranked broadcast lands in the inner endpoint's queue",
    );
    coord
      .queue_user_broadcast_ranked(1, Bytes::from_static(b"lower-priority"))
      .expect("in-budget payload enqueues at rank 1");
    assert_eq!(
      coord.endpoint_ref().user_broadcast_queue_len(),
      2,
      "both ranked broadcasts land in the inner endpoint's queue",
    );
  }

  /// A `set_compression_options` update fans the new policy out to every live
  /// bridge WITHOUT a failure cascade (compression is non-security): the bridge
  /// stays alive and adopts the new options. Drives the
  /// `set_compression_options` per-bridge `get_mut` loop body.
  #[cfg(feature = "lz4")]
  #[test]
  fn set_compression_options_fans_out_without_failing_bridges() {
    use crate::{CompressAlgorithm, CompressionOptions};

    let now = Instant::now();
    let mut coord = coord(7107);
    coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);
    while coord.poll_action().is_some() {}
    let before = coord.live_bridge_count();
    assert!(before >= 1, "the dial built a live bridge");

    let comp = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(64);
    coord.set_compression_options(comp);

    assert_eq!(
      coord.live_bridge_count(),
      before,
      "a compression-policy change never fails or reaps a live bridge",
    );
  }

  /// STR-A001: a `start_reliable_ping` whose probe deadline has already elapsed
  /// is retired inside `service_dials`' expired-intent gate — no connection is
  /// opened — and, because the kind is start-originated (`ReliablePing`),
  /// surfaces exactly one `Event::DialAborted{ReliablePing, DeadlineElapsed}`
  /// keyed by the returned `StreamId`. The probe FSM's own fallback retirement
  /// is preserved (`dial_failed` still routes for `ReliablePing`).
  #[test]
  fn reliable_ping_elapsed_deadline_emits_dialaborted_deadline_elapsed() {
    let now = Instant::now();
    let mut coord = coord(7108);
    // `deadline == now` is already elapsed at the `now >= deadline` gate.
    let sid = coord.start_reliable_ping(SmolStr::new("p"), addr(7000), 7, now, now);

    let mut aborts = Vec::new();
    while let Some(ev) = coord.poll_event() {
      if let Event::DialAborted(a) = ev {
        aborts.push((a.stream_id(), a.kind(), a.reason()));
      }
    }
    assert_eq!(
      aborts.as_slice(),
      &[(
        sid,
        ExchangeKind::ReliablePing,
        DialAbortReason::DeadlineElapsed
      )],
      "exactly one DialAborted, keyed by the returned StreamId",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "no bridge is built for an already-elapsed reliable-ping dial",
    );
    assert!(
      coord.poll_action().is_none(),
      "no Connect surfaces for an expired reliable-ping dial",
    );
  }

  /// STR-A001 kind-gating non-vacuity: a `DialRequested` fed straight into the
  /// inner endpoint (bypassing the `start_*` wrappers, so no
  /// `pending_outbound_kinds` entry — kind `None`) whose dial fails pre-`Connect`
  /// emits NO `DialAborted`. The abort is reported only for start-originated
  /// ids; machine-scheduled dials self-heal on their schedulers.
  #[test]
  fn kind_none_dial_failing_pre_connect_emits_no_dialaborted() {
    let now = Instant::now();
    let mut coord = coord(7109);
    // A real inner intent + `DialRequested`, allocated OUTSIDE the wrappers, so
    // `pending_outbound_kinds` never gains an entry for its id.
    coord
      .endpoint_mut()
      .start_push_pull(addr(7000), PushPullKind::Join, now);
    // Sieve the `DialRequested` into the private dial deque.
    while coord.poll_event().is_some() {}

    // Service the dial well past its deadline so it retires pre-`Connect`.
    let later = now + Duration::from_secs(86_400);
    coord.service_dials(later);

    let mut saw_abort = false;
    while let Some(ev) = coord.poll_event() {
      if matches!(ev, Event::DialAborted(_)) {
        saw_abort = true;
      }
    }
    assert!(
      !saw_abort,
      "a kind-None (machine-scheduled) dial failure emits no DialAborted",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the expired kind-None dial built no bridge",
    );
    assert!(
      coord.poll_action().is_none(),
      "no Connect surfaces for the expired kind-None dial",
    );
  }

  /// Leave terminality (drained-Connect path): two outbound exchanges whose
  /// `Connect` the driver has already drained (observed) are, on `leave()`,
  /// terminalized as exactly one `ExchangeCompleted(Failed, kind)` each — the
  /// driver holds each `eid`, so its parked waiter resolves at leave — plus one
  /// `Abort` teardown; NO `DialAborted`.
  #[test]
  fn leave_after_draining_connects_terminalizes_each_unsent_exchange_as_failed() {
    let now = Instant::now();
    let mut coord = coord(7130);
    let um_sid = coord
      .start_user_message(addr(7001), Bytes::from_static(b"m"), now)
      .expect("issued while running");
    let pp_sid = coord.start_push_pull(addr(7002), PushPullKind::Join, now);

    // Drain (observe) the Connect actions, recording each StreamId -> ExchangeId.
    let mut eid_by_sid = HashMap::new();
    while let Some(a) = coord.poll_action() {
      if let StreamAction::Connect(info) = a {
        eid_by_sid.insert(info.stream_id(), info.id());
      }
    }
    assert_eq!(eid_by_sid.len(), 2, "both dials surfaced a Connect");

    coord.leave(now).expect("leave from a running node");

    let mut completed = Vec::new();
    let mut aborts = 0;
    while let Some(ev) = coord.poll_event() {
      match ev {
        Event::ExchangeCompleted(c) => completed.push((c.eid(), c.outcome(), c.kind())),
        Event::DialAborted(_) => aborts += 1,
        _ => {}
      }
    }
    assert_eq!(
      aborts, 0,
      "a drained-Connect exchange completes, never DialAborted"
    );
    completed.sort_by_key(|(eid, _, _)| eid.get());
    // Exactly one Failed completion per exchange, carrying the originating kind.
    assert_eq!(
      completed.len(),
      2,
      "exactly one terminal per drained exchange"
    );
    let um_eid = eid_by_sid[&um_sid];
    let pp_eid = eid_by_sid[&pp_sid];
    assert!(
      completed.iter().any(|&(e, s, k)| e == um_eid
        && s == ExchangeStatus::Failed
        && k == ExchangeKind::UserMessage),
      "the user-message exchange completed Failed with its kind",
    );
    assert!(
      completed.iter().any(|&(e, s, k)| e == pp_eid
        && s == ExchangeStatus::Failed
        && k == ExchangeKind::PushPull),
      "the push/pull exchange completed Failed with its kind",
    );

    // One Abort teardown per cancelled exchange; nothing further.
    let mut abort_actions = 0;
    while let Some(a) = coord.poll_action() {
      match a {
        StreamAction::Abort(_) => abort_actions += 1,
        StreamAction::Connect(_) => panic!("a left node surfaces no Connect"),
        _ => {}
      }
    }
    assert_eq!(
      abort_actions, 2,
      "one Abort teardown per cancelled exchange"
    );
    assert_eq!(coord.live_bridge_count(), 0, "no bridge survives the leave");
  }

  /// Leave terminality (queued-Connect path): two outbound exchanges whose
  /// `Connect` the driver has NEVER drained are, on `leave()`, terminalized as
  /// exactly one `DialAborted { Leaving }` each (keyed by the originating
  /// `StreamId`); NO `ExchangeCompleted`, and no `Connect` ever surfaces.
  #[test]
  fn leave_without_draining_connects_aborts_each_with_leaving_reason() {
    let now = Instant::now();
    let mut coord = coord(7131);
    let um_sid = coord
      .start_user_message(addr(7001), Bytes::from_static(b"m"), now)
      .expect("issued while running");
    let pp_sid = coord.start_push_pull(addr(7002), PushPullKind::Join, now);

    // Do NOT drain the Connect actions: they stay queued until leave.
    coord.leave(now).expect("leave from a running node");

    let mut completed = 0;
    let mut aborts = Vec::new();
    while let Some(ev) = coord.poll_event() {
      match ev {
        Event::ExchangeCompleted(_) => completed += 1,
        Event::DialAborted(a) => aborts.push((a.stream_id(), a.kind(), a.reason())),
        _ => {}
      }
    }
    assert_eq!(
      completed, 0,
      "a still-queued Connect is DialAborted, never completed"
    );
    assert_eq!(aborts.len(), 2, "exactly one DialAborted per started id");
    assert!(
      aborts.contains(&(um_sid, ExchangeKind::UserMessage, DialAbortReason::Leaving)),
      "the user-message id was aborted with the Leaving reason",
    );
    assert!(
      aborts.contains(&(pp_sid, ExchangeKind::PushPull, DialAbortReason::Leaving)),
      "the push/pull id was aborted with the Leaving reason",
    );

    while let Some(a) = coord.poll_action() {
      assert!(
        !matches!(a, StreamAction::Connect(_)),
        "a left node surfaces no Connect for a still-queued dial",
      );
    }
    assert_eq!(coord.live_bridge_count(), 0, "no bridge survives the leave");
  }

  /// Totality invariant across a mixed interleaving: every kind-bearing
  /// `StreamId` receives EXACTLY ONE machine-emitted terminal at leave — an
  /// `ExchangeCompleted(Failed)` for an exchange whose `Connect` was drained, a
  /// `DialAborted(Leaving)` for one whose `Connect` was still queued.
  #[test]
  fn leave_emits_exactly_one_terminal_per_kind_bearing_stream_id() {
    let now = Instant::now();
    let mut coord = coord(7140);

    // Two exchanges whose Connect the driver drains (observes) before leave.
    let drained = [
      coord
        .start_user_message(addr(7001), Bytes::from_static(b"a"), now)
        .expect("issued while running"),
      coord.start_push_pull(addr(7002), PushPullKind::Join, now),
    ];
    let mut eid_by_sid = HashMap::new();
    while let Some(a) = coord.poll_action() {
      if let StreamAction::Connect(info) = a {
        eid_by_sid.insert(info.stream_id(), info.id());
      }
    }

    // Two more started AFTER that drain: their Connect stays queued at leave.
    let queued = [
      coord
        .start_user_message(addr(7003), Bytes::from_static(b"c"), now)
        .expect("issued while running"),
      coord.start_push_pull(addr(7004), PushPullKind::Refresh, now),
    ];

    coord.leave(now).expect("leave from a running node");

    let mut completed_eids = Vec::new();
    let mut aborted_sids = Vec::new();
    while let Some(ev) = coord.poll_event() {
      match ev {
        Event::ExchangeCompleted(c) if c.outcome() == ExchangeStatus::Failed => {
          completed_eids.push(c.eid());
        }
        Event::DialAborted(a) if a.reason() == DialAbortReason::Leaving => {
          aborted_sids.push(a.stream_id());
        }
        _ => {}
      }
    }

    // Each drained sid: exactly one ExchangeCompleted(Failed) via its eid, and
    // never a DialAborted.
    for sid in drained {
      let eid = eid_by_sid[&sid];
      assert_eq!(
        completed_eids.iter().filter(|&&e| e == eid).count(),
        1,
        "a drained-Connect id completes exactly once",
      );
      assert!(
        !aborted_sids.contains(&sid),
        "a drained-Connect id is not also DialAborted",
      );
    }
    // Each queued sid: exactly one DialAborted(Leaving); its eid was never
    // allocated to the driver, so it cannot appear as a completion.
    for sid in queued {
      assert_eq!(
        aborted_sids.iter().filter(|&&s| s == sid).count(),
        1,
        "a queued-Connect id is aborted exactly once",
      );
      assert!(
        !eid_by_sid.contains_key(&sid),
        "a queued-Connect id never surfaced an eid",
      );
    }
    // Exactly four terminals — one per started kind-bearing StreamId.
    assert_eq!(
      completed_eids.len() + aborted_sids.len(),
      4,
      "exactly one machine terminal per kind-bearing StreamId",
    );
  }

  /// Build a coordinator whose inner endpoint holds `m_id` in `State::Suspect`
  /// at incarnation `inc`, armed with the suspicion's own deadline. The setup
  /// events (the `NodeJoined(m_id)` from seeding it Alive, the `Suspect`
  /// broadcast) are drained so the caller observes only what the tick under
  /// test produces.
  fn coord_with_suspect_member(
    coord_port: u16,
    m_id: &SmolStr,
    m_addr: SocketAddr,
    inc: u32,
    now: Instant,
  ) -> StreamEndpoint<SmolStr, SocketAddr, RawRecords> {
    use crate::typed::{Alive, Suspect};
    let mut coord = coord(coord_port);
    coord.handle_alive(
      m_addr,
      Alive::new(inc, crate::Node::new(m_id.clone(), m_addr)),
      now,
    );
    coord.handle_suspect(
      m_addr,
      Suspect::new(inc, m_id.clone(), SmolStr::new("accuser")),
      now,
    );
    while coord.poll_event().is_some() {}
    let _ = coord.endpoint_mut().drain_broadcasts();
    coord
  }

  /// A real dialer's coalesced `[label || push/pull request]` bytes, where the
  /// push advertises `m_id` at `m_inc` as `State::Alive`. The dialer's
  /// membership is seeded so its push/pull request carries that exact record —
  /// the refutation (or, for a stale incarnation, non-refutation) the acceptor
  /// merges when its handshake settles.
  fn dialer_push_pull_advertising_alive(
    dialer_port: u16,
    m_id: &SmolStr,
    m_addr: SocketAddr,
    m_inc: u32,
    coord_addr: SocketAddr,
    now: Instant,
  ) -> Vec<u8> {
    use crate::typed::Alive;
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut dialer: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
      endpoint(dialer_port),
      cfg,
      test_sni_provider(),
      test_peer_to_socket(),
    );
    dialer.handle_alive(
      m_addr,
      Alive::new(m_inc, crate::Node::new(m_id.clone(), m_addr)),
      now,
    );
    let _ = dialer.start_push_pull(coord_addr, PushPullKind::Join, now);
    let _ = dialer.poll_action();
    let mut bytes = Vec::new();
    while let Some((_id, _peer, chunk)) = dialer.poll_transport_transmit() {
      bytes.extend_from_slice(&chunk);
    }
    bytes
  }

  /// A same-tick-settled handshake carrying a superseding `Alive` cancels a
  /// suspicion whose deadline expires on that same tick as a silent
  /// `Suspect -> Alive` — not a spurious `Dead -> Alive` flap. The buffered
  /// refutation is applied (mint + replay) BEFORE `Endpoint::handle_timeout`
  /// fires the suspicion, so no `NodeLeft`/`Dead` is ever synthesized.
  #[test]
  fn same_tick_refutation_cancels_suspicion_no_flap() {
    use crate::{
      event::Event,
      typed::{Message, State},
    };
    let now = Instant::now();
    let m = SmolStr::new("m-node");
    let m_addr = addr(7303);
    let inc = 5u32;
    let coord_addr = addr(7300);

    let mut coord = coord_with_suspect_member(7300, &m, m_addr, inc, now);
    assert_eq!(
      coord.endpoint_ref().member_liveness(&m),
      Some(State::Suspect),
      "M starts Suspect",
    );

    // The suspicion is armed at `now`; its deadline is well within this margin
    // (the small cluster gives `k == 0`, so the timer is fixed at the sub-minute
    // `min`). The connection arrives fresh on the wake at `t >= deadline`: the
    // peer's push/pull request coalesced with its handshake tail, delivered on a
    // driver wake at or after the suspicion deadline — ordinary scheduling.
    let t = now + Duration::from_secs(30);
    let blob = dialer_push_pull_advertising_alive(7304, &m, m_addr, inc + 1, coord_addr, t);
    let exchange = coord
      .accept_connection(m_addr, t)
      .expect("connection admitted while running");
    coord.handle_transport_data(exchange, &blob, true, t);

    assert_eq!(
      coord.endpoint_ref().member_liveness(&m),
      Some(State::Alive),
      "M ends Alive: the refutation cancelled the suspicion",
    );
    assert_eq!(
      coord.endpoint_ref().node_incarnation(&m),
      Some(inc + 1),
      "M carries the refuting incarnation",
    );

    let mut node_left_m = false;
    let mut node_joined_m = false;
    while let Some(ev) = coord.poll_event() {
      match ev {
        Event::NodeLeft(ns) if ns.id_ref() == &m => node_left_m = true,
        Event::NodeJoined(ns) if ns.id_ref() == &m => node_joined_m = true,
        _ => {}
      }
    }
    assert!(
      !node_left_m,
      "no NodeLeft(M): the suspicion was cancelled, never fired",
    );
    assert!(
      !node_joined_m,
      "no NodeJoined(M): a Suspect -> Alive cancel is not a rejoin",
    );

    let broadcasts = coord.endpoint_mut().drain_broadcasts();
    assert!(
      broadcasts.iter().any(|msg| matches!(
        msg,
        Message::Alive(a) if a.node_ref().id_ref() == &m && a.incarnation() == inc + 1
      )),
      "the refutation Alive(M, inc+1) is broadcast",
    );
    assert!(
      !broadcasts
        .iter()
        .any(|msg| matches!(msg, Message::Dead(d) if d.node_ref() == &m)),
      "no Dead(M) is broadcast",
    );
  }

  /// Liveness control: with no refutation on the wire, a suspicion still fires
  /// `Dead` on the tick its deadline expires — the reorder does not suppress a
  /// genuine failure. Same setup as the flap-cancel test, minus the inbound.
  #[test]
  fn suspicion_still_fires_without_refutation() {
    use crate::{
      event::Event,
      typed::{Message, State},
    };
    let now = Instant::now();
    let m = SmolStr::new("m-node");
    let m_addr = addr(7303);
    let inc = 5u32;

    let mut coord = coord_with_suspect_member(7310, &m, m_addr, inc, now);
    assert_eq!(
      coord.endpoint_ref().member_liveness(&m),
      Some(State::Suspect),
      "M starts Suspect",
    );

    // The same wake time the refutation tests use; here nothing refutes, so the
    // suspicion deadline (well within this margin) fires on the tick.
    let t = now + Duration::from_secs(30);
    coord.handle_timeout(t);

    assert_eq!(
      coord.endpoint_ref().member_liveness(&m),
      Some(State::Dead),
      "M transitions Dead once the suspicion deadline elapses",
    );

    let mut node_left_m = false;
    while let Some(ev) = coord.poll_event() {
      if let Event::NodeLeft(ns) = ev
        && ns.id_ref() == &m
      {
        node_left_m = true;
      }
    }
    assert!(
      node_left_m,
      "NodeLeft(M) is emitted on the suspicion expiry"
    );

    let broadcasts = coord.endpoint_mut().drain_broadcasts();
    assert!(
      broadcasts
        .iter()
        .any(|msg| matches!(msg, Message::Dead(d) if d.node_ref() == &m)),
      "Dead(M) is broadcast on the suspicion expiry",
    );
  }

  /// Control: a stale/equal-incarnation `Alive` carried in the same-tick
  /// handshake does NOT refute the suspicion — the older/equal-incarnation
  /// guard rejects it — so the suspicion survives the mint + replay and still
  /// fires `Dead` when `handle_timeout` runs later in the same tick.
  #[test]
  fn stale_refutation_does_not_suppress_suspicion() {
    use crate::{
      event::Event,
      typed::{Message, State},
    };
    let now = Instant::now();
    let m = SmolStr::new("m-node");
    let m_addr = addr(7303);
    let inc = 5u32;
    let coord_addr = addr(7320);

    let mut coord = coord_with_suspect_member(7320, &m, m_addr, inc, now);

    // The handshake carries Alive(M, inc) — equal to the suspicion incarnation,
    // so the merge's `<= local_incarnation` guard drops it. The connection
    // arrives fresh on the wake at `t >= deadline`.
    let t = now + Duration::from_secs(30);
    let blob = dialer_push_pull_advertising_alive(7321, &m, m_addr, inc, coord_addr, t);
    let exchange = coord
      .accept_connection(m_addr, t)
      .expect("connection admitted while running");
    coord.handle_transport_data(exchange, &blob, true, t);

    assert_eq!(
      coord.endpoint_ref().member_liveness(&m),
      Some(State::Dead),
      "the stale Alive does not refute; the suspicion still fires Dead",
    );

    let mut node_left_m = false;
    while let Some(ev) = coord.poll_event() {
      if let Event::NodeLeft(ns) = ev
        && ns.id_ref() == &m
      {
        node_left_m = true;
      }
    }
    assert!(
      node_left_m,
      "NodeLeft(M) is emitted: the suspicion survived"
    );

    let broadcasts = coord.endpoint_mut().drain_broadcasts();
    assert!(
      broadcasts
        .iter()
        .any(|msg| matches!(msg, Message::Dead(d) if d.node_ref() == &m)),
      "Dead(M) is broadcast: the stale Alive did not suppress the suspicion",
    );
  }
}

/// STR-A001 test 3: a record layer whose `dialer` constructor fails drives the
/// `R::dialer` `Err` arm of `service_dials`, surfacing a
/// `DialAborted{.., RecordLayer}`. Uses a minimal record layer whose only
/// reachable methods are `dial_context` (Ok) and `dialer` (Err); the dial fails
/// before any bridge is built, so the transport I/O methods are never called.
#[cfg(any(feature = "tcp", feature = "tls"))]
mod fallible {
  use crate::Instant;

  use bytes::Bytes;
  use core::net::SocketAddr;
  use smol_str::SmolStr;

  use crate::{
    event::{DialAbortReason, Event, ExchangeKind},
    streams::{
      StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
      transport::{Intake, StreamTransport},
    },
  };

  /// A record layer whose dialer construction always fails. Only `dial_context`
  /// and `dialer` are reachable in the record-layer-construction-failure path;
  /// every transport method is unreachable because no bridge is ever built.
  struct FallibleDialer;

  impl StreamTransport for FallibleDialer {
    type Options = ();
    type DialContext = ();
    type ConstructError = &'static str;

    fn dial_context<A>(_addr: &A, _server_name: Option<&str>) -> Result<(), &'static str> {
      Ok(())
    }

    fn dialer(_opts: &(), _ctx: ()) -> Result<Self, &'static str> {
      Err("test: dialer construction always fails")
    }

    fn acceptor(_opts: &()) -> Result<Self, &'static str> {
      Err("test: acceptor construction always fails")
    }

    fn handle_transport_data(&mut self, _input: &[u8], _now: Instant) -> Intake {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn poll_transport_transmit(&mut self, _out: &mut std::vec::Vec<u8>) -> usize {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn is_handshaking(&self) -> bool {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn read_plaintext(&mut self, _out: &mut std::vec::Vec<u8>) -> usize {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn write_plaintext(&mut self, _plaintext: &[u8]) -> bool {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn send_close_notify(&mut self) {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn peer_has_closed(&self) -> bool {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn clear_outbound(&mut self) {
      unreachable!("no bridge is built when the dialer fails")
    }
    fn is_secure() -> bool {
      false
    }
  }

  #[test]
  fn dialer_construction_failure_emits_dialaborted_record_layer() {
    let now = Instant::now();
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, FallibleDialer> = StreamEndpoint::new(
      endpoint(7120),
      (),
      test_sni_provider(),
      test_peer_to_socket(),
    );

    let sid = coord
      .start_user_message(addr(7000), Bytes::from_static(b"x"), now)
      .expect("issued while running");

    let mut aborts = Vec::new();
    while let Some(ev) = coord.poll_event() {
      if let Event::DialAborted(a) = ev {
        aborts.push((a.stream_id(), a.kind(), a.reason()));
      }
    }
    assert_eq!(
      aborts.as_slice(),
      &[(sid, ExchangeKind::UserMessage, DialAbortReason::RecordLayer)],
      "a dialer-construction failure surfaces one DialAborted with RecordLayer",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "no bridge is built when the dialer constructor fails",
    );
    assert!(
      coord.poll_action().is_none(),
      "no Connect surfaces for a failed dialer construction",
    );
  }
}

#[cfg(feature = "tls")]
mod tls {
  use crate::Instant;
  use core::{net::SocketAddr, time::Duration};

  use smol_str::SmolStr;
  use std::sync::Arc;

  use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified},
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    version::TLS13,
  };

  use bytes::Bytes;

  use crate::{
    TlsOptions, TlsRecords,
    event::{DialAbortReason, Event, ExchangeKind, ExchangeStatus, PushPullKind},
    streams::{
      LabelOptions, Labeled, StreamAction, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
  };

  // The `tls::options::tests` cert helpers live behind a private `mod options`,
  // so they are not reachable from this module; replicate the minimal
  // self-signed-server + accept-any-client rustls bundle the TLS suite uses.
  fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
  }

  fn self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
    let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
    (chain, key)
  }

  /// Accept-any server-cert verifier — test only.
  #[derive(Debug)]
  struct AnyServer(Arc<CryptoProvider>);
  impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
      &self,
      _e: &CertificateDer<'_>,
      _i: &[CertificateDer<'_>],
      _n: &ServerName<'_>,
      _o: &[u8],
      _t: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
      Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
      Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
      Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }

  fn test_server() -> rustls::ServerConfig {
    let (chain, key) = self_signed();
    rustls::ServerConfig::builder_with_provider(provider())
      .with_protocol_versions(&[&TLS13])
      .unwrap()
      .with_no_client_auth()
      .with_single_cert(chain, key)
      .unwrap()
  }

  fn test_client() -> rustls::ClientConfig {
    let p = provider();
    rustls::ClientConfig::builder_with_provider(p.clone())
      .with_protocol_versions(&[&TLS13])
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(AnyServer(p)))
      .with_no_client_auth()
  }

  fn tls_coord(port: u16) -> StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> {
    let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
    StreamEndpoint::new(
      endpoint(port),
      cfg,
      test_sni_provider(),
      test_peer_to_socket(),
    )
  }

  /// A TLS dial whose intent is retired by the inner endpoint BEFORE its
  /// `Stream` is minted, while the bridge is still inside its (future) dial
  /// deadline, drives the `dial_succeeded(None)` branch of
  /// `service_handshake_completions` WITH `meta.kind = Some` — surfacing a
  /// terminal `Event::ExchangeCompleted(Failed)` carrying the originating
  /// `ExchangeKind::PushPull` (the `pending_outbound_kinds` entry the
  /// `start_push_pull` wrapper stamps).
  ///
  /// The TLS dialer stays `Handshaking` after `start_push_pull` (the TLS
  /// handshake is still pending), unlike a plain-TCP dialer that promotes
  /// in-band. The intent is retired up front (`dial_failed`); the TLS handshake
  /// is then driven to completion against a bare server record layer via
  /// `handle_transport_data`. Each intermediate tick's
  /// `service_handshake_completions` requeues the still-handshaking bridge (no
  /// `dial_succeeded` call), and the tick on which the handshake settles calls
  /// `dial_succeeded(sid, now)` -> `None` (the intent is gone), entering the
  /// fail-and-emit branch.
  #[test]
  fn tls_dial_retired_emits_failed_exchange_completed_with_kind() {
    use crate::{error::StreamError, streams::StreamTransport};

    let now = Instant::now();
    let mut coord = tls_coord(7300);
    let server_addr = addr(7000);

    // The wrapper stamps kind = PushPull, dials, and flushes; the TLS dialer is
    // still handshaking, so the bridge stays unminted (mint = Some).
    let sid = coord.start_push_pull(server_addr, PushPullKind::Join, now);
    let exchange = match coord.poll_action().expect("the dial surfaces a Connect") {
      crate::streams::StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {other:?}"),
    };
    while coord.poll_action().is_some() {}

    // Retire the inner endpoint's dial intent up front. Until the handshake
    // settles the bridge stays in `unminted` and `dial_succeeded` is never
    // called, so the retirement only takes effect on the settling tick.
    coord
      .endpoint_mut()
      .dial_failed(sid, StreamError::DialFailed("test injection".into()), now);

    // Bare `Labeled<TlsRecords>` server (label `None`, matching the dialer) to
    // complete the dialer's handshake — the label rides inside TLS, so this is
    // just the crypto peer.
    let server_cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
    let mut server =
      <Labeled<TlsRecords> as StreamTransport>::acceptor(&server_cfg).expect("bare TLS server");

    let mut completed = None;
    'drive: for _ in 0..64 {
      // Dialer -> server: drain the coordinator's outbound ciphertext and feed
      // it to the bare server.
      let mut to_server = Vec::new();
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        if id == exchange {
          to_server.extend_from_slice(&bytes);
        }
      }
      if !to_server.is_empty() {
        let _ = server.handle_transport_data(&to_server, now);
      }
      // Server -> dialer: feed the server's flight back through the coordinator,
      // which runs a tick (and `service_handshake_completions`) each call.
      let mut to_dialer = Vec::new();
      server.poll_transport_transmit(&mut to_dialer);
      if !to_dialer.is_empty() {
        coord.handle_transport_data(exchange, &to_dialer, false, now);
      }
      // The settling tick reaped the bridge through `dial_succeeded(None)`.
      while let Some(ev) = coord.poll_event() {
        if let Event::ExchangeCompleted(c) = ev {
          completed = Some(c);
          break 'drive;
        }
      }
      if coord.live_bridge_count() == 0 {
        break;
      }
      if to_server.is_empty() && to_dialer.is_empty() {
        break;
      }
    }

    let completed = completed
      .expect("the dial-retired branch emits a terminal ExchangeCompleted carrying the kind");
    assert_eq!(
      completed.outcome(),
      ExchangeStatus::Failed,
      "a retired dial completes as Failed",
    );
    assert_eq!(
      completed.kind(),
      ExchangeKind::PushPull,
      "the failed completion carries the start_push_pull kind",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the bridge is reaped on the dial-retired path",
    );
  }

  /// Completeness of the leave-cancellation sweep: a drained-`Connect` outbound
  /// TLS exchange whose ClientHello has ALREADY flushed (`out_transmit` empty)
  /// while the bridge is still an unminted `PendingMint::Outbound` — the peer
  /// crash-stopped after the ClientHello, sending no handshake response and no
  /// FIN — is terminalized PROMPTLY by `leave()`, with NO dependence on the
  /// stream deadline. The sweep's second predicate (unminted `PendingMint::
  /// Outbound`, not just non-empty `out_transmit`) catches it: exactly one
  /// `ExchangeCompleted(Failed)`, the handshaking bridge torn down, an `Abort`
  /// queued, and no duplicate terminal.
  #[test]
  fn leave_terminalizes_flushed_unminted_tls_handshake_exchange() {
    let now = Instant::now();
    let mut coord = tls_coord(7330);
    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Join, now);
    let exchange = match coord.poll_action().expect("the dial surfaces a Connect") {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {other:?}"),
    };
    while coord.poll_action().is_some() {}

    // Drain the flushed ClientHello so `out_transmit` is empty; feed NO server
    // response, so the bridge stays Handshaking (unminted `PendingMint::Outbound`).
    let mut hello_bytes = 0usize;
    while let Some((_id, _peer, bytes)) = coord.poll_transport_transmit() {
      hello_bytes += bytes.len();
    }
    assert!(hello_bytes > 0, "the TLS dialer flushed a ClientHello");
    assert_eq!(
      coord.live_bridge_count(),
      1,
      "the handshaking bridge is live (unminted) before leave",
    );

    // Leave WITHOUT advancing time or firing a timeout.
    coord.leave(now).expect("leave from a running node");

    let mut completed = Vec::new();
    let mut aborts = 0;
    while let Some(ev) = coord.poll_event() {
      match ev {
        Event::ExchangeCompleted(c) => completed.push((c.eid(), c.outcome(), c.kind())),
        Event::DialAborted(_) => aborts += 1,
        _ => {}
      }
    }
    assert_eq!(
      aborts, 0,
      "a drained-Connect exchange completes, never DialAborted"
    );
    assert_eq!(
      completed.as_slice(),
      &[(exchange, ExchangeStatus::Failed, ExchangeKind::PushPull)],
      "one immediate Failed completion for the flushed-but-unminted TLS exchange",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the handshaking bridge is torn down by the leave cancel",
    );

    // The teardown surfaces an Abort (the half-open TLS connection is not left
    // live); no `Connect` and no duplicate terminal.
    let mut aborted = false;
    while let Some(a) = coord.poll_action() {
      match a {
        StreamAction::Abort(r) if r.id() == exchange => aborted = true,
        StreamAction::Connect(_) => panic!("a left node surfaces no Connect"),
        _ => {}
      }
    }
    assert!(
      aborted,
      "leave queues an Abort to tear the half-open TLS connection down"
    );
    let mut duplicate_terminals = 0;
    while let Some(ev) = coord.poll_event() {
      if matches!(ev, Event::ExchangeCompleted(_) | Event::DialAborted(_)) {
        duplicate_terminals += 1;
      }
    }
    assert_eq!(
      duplicate_terminals, 0,
      "no duplicate terminal after the leave"
    );
  }

  /// The `leave_silent` twin (the driver's hard-shutdown teardown path): the same
  /// flushed-but-unminted TLS exchange is cancelled and its handshaking bridge
  /// torn down (the connection is not left live), but NO application terminal is
  /// emitted — the shutdown drain reaps the parked waiter with its own `Shutdown`
  /// outcome, which a leave-cancel terminal would preempt.
  #[test]
  fn leave_silent_cancels_flushed_unminted_tls_handshake_without_terminal() {
    let now = Instant::now();
    let mut coord = tls_coord(7331);
    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Join, now);
    let exchange = match coord.poll_action().expect("the dial surfaces a Connect") {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {other:?}"),
    };
    while coord.poll_action().is_some() {}
    while coord.poll_transport_transmit().is_some() {}
    assert_eq!(
      coord.live_bridge_count(),
      1,
      "the handshaking bridge is live before leave_silent",
    );

    coord.leave_silent(now).expect("silent leave");

    let mut terminals = 0;
    while let Some(ev) = coord.poll_event() {
      if matches!(ev, Event::ExchangeCompleted(_) | Event::DialAborted(_)) {
        terminals += 1;
      }
    }
    assert_eq!(terminals, 0, "leave_silent emits no application terminal");
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "leave_silent still tears the handshaking bridge down",
    );
    let mut aborted = false;
    while let Some(a) = coord.poll_action() {
      if let StreamAction::Abort(r) = a
        && r.id() == exchange
      {
        aborted = true;
      }
    }
    assert!(aborted, "leave_silent still queues the Abort teardown");
  }

  /// A TLS dial whose SNI provider returns `None` is rejected at
  /// `TlsRecords::dial_context` inside `service_dials`, retiring the intent via
  /// the pre-`ExchangeMeta` `dial_failed` path and draining the
  /// `pending_outbound_kinds` entry (so no leak), with no bridge allocated.
  #[test]
  fn tls_dial_context_rejection_retires_without_bridge() {
    let now = Instant::now();
    let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
    let sni: Box<dyn Fn(&SocketAddr) -> Option<String> + Send + Sync> = Box::new(|_| None);
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
      StreamEndpoint::new(endpoint(7301), cfg, sni, test_peer_to_socket());

    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);
    assert_eq!(
      coord.pending_outbound_kinds_len(),
      0,
      "the kind entry drains on the dial_context-failure exit",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "no bridge is built when the per-peer SNI is rejected",
    );
  }

  /// Drives a TLS acceptor to `Established(Active)` with NO application bytes
  /// via a bare client `TlsRecords`, then asserts the
  /// `bridge_is_established_pre_fin` accessor reports the pre-FIN established
  /// state (the precondition the failed-reap regression relies on). Covers the
  /// `matches!` in `bridge_is_established_pre_fin`.
  #[test]
  fn tls_acceptor_reaches_established_pre_fin() {
    let now = Instant::now();
    let mut acceptor = tls_coord(7302);
    let dialer_addr = addr(7303);
    let exchange = acceptor
      .accept_connection(dialer_addr, now)
      .expect("test: connection admitted");

    let mut client = TlsRecords::client(
      Arc::new(test_client()),
      ServerName::try_from("localhost").unwrap(),
    )
    .expect("client TlsRecords");
    for _ in 0..64 {
      let mut c_out = Vec::new();
      client.poll_transport_transmit(&mut c_out);
      if !c_out.is_empty() {
        acceptor.handle_transport_data(exchange, &c_out, false, now);
      }
      let mut s_out = Vec::new();
      while let Some((_id, _peer, bytes)) = acceptor.poll_transport_transmit() {
        s_out.extend_from_slice(&bytes);
      }
      if !s_out.is_empty() {
        client
          .handle_transport_data(&s_out)
          .expect("the client consumes the server flight");
      }
      while acceptor.poll_action().is_some() {}
      if c_out.is_empty() && s_out.is_empty() {
        break;
      }
    }

    assert_eq!(
      acceptor.bridge_is_established_pre_fin(exchange),
      Some(true),
      "the TLS acceptor reached Established(Active) with no FIN owed",
    );
    // A non-existent exchange id reports `None` (the accessor's `get_mut` miss).
    let missing = crate::streams::ExchangeId::new(9999);
    assert_eq!(
      acceptor.bridge_is_established_pre_fin(missing),
      None,
      "an unknown exchange id has no bridge to inspect",
    );
  }

  /// STR-A001 test 1: a `start_user_message` whose per-peer SNI provider returns
  /// `None` returns `Ok(sid)` (the id is allocated before the fallible dial
  /// setup), then the dial is rejected at `TlsRecords::dial_context` inside
  /// `service_dials`. Exactly one `DialAborted{UserMessage, DialContext}` keyed
  /// by `sid` surfaces; no `Connect`, no transmit, no retained exchange
  /// metadata, and no leaked `pending_outbound_kinds` entry.
  #[test]
  fn sni_none_user_message_emits_dialaborted_dialcontext() {
    let now = Instant::now();
    let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
    let sni: Box<dyn Fn(&SocketAddr) -> Option<String> + Send + Sync> = Box::new(|_| None);
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
      StreamEndpoint::new(endpoint(7320), cfg, sni, test_peer_to_socket());

    let sid = coord
      .start_user_message(addr(7000), Bytes::from_static(b"x"), now)
      .expect("start_user_message returns Ok while running");

    assert!(
      coord.poll_action().is_none(),
      "no Connect surfaces for an SNI-rejected user-message dial",
    );
    assert!(
      coord.poll_transport_transmit().is_none(),
      "no bytes transmit for an SNI-rejected dial",
    );

    let mut aborts = Vec::new();
    while let Some(ev) = coord.poll_event() {
      if let Event::DialAborted(a) = ev {
        aborts.push((a.stream_id(), a.kind(), a.reason()));
      }
    }
    assert_eq!(
      aborts.as_slice(),
      &[(sid, ExchangeKind::UserMessage, DialAbortReason::DialContext)],
      "exactly one DialAborted (UserMessage, DialContext) keyed by the returned id",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "no exchange metadata / bridge is retained after the rejected dial",
    );
    assert_eq!(
      coord.pending_outbound_kinds_len(),
      0,
      "the pending_outbound_kinds entry drained on the dial_context-failure exit",
    );
  }

  /// STR-A001 test 4: a mixed batch of two `start_user_message`s — the first
  /// peer's SNI resolves (`Some`), the second's does not (`None`) — surfaces
  /// exactly one `Connect` (the first id) and exactly one `DialAborted` (the
  /// second id), with disjoint ids. Proves the abort is per-id and only for the
  /// dial that actually failed pre-`Connect`.
  #[test]
  fn mixed_sni_batch_emits_one_connect_and_one_dialaborted() {
    let now = Instant::now();
    let cfg = LabelOptions::new_in(None, TlsOptions::new(test_server(), test_client()));
    // Resolve SNI for port 7001 only; port 7002 gets `None`.
    let sni: Box<dyn Fn(&SocketAddr) -> Option<String> + Send + Sync> =
      Box::new(|a: &SocketAddr| (a.port() == 7001).then(|| "localhost".to_string()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, Labeled<TlsRecords>> =
      StreamEndpoint::new(endpoint(7321), cfg, sni, test_peer_to_socket());

    let ok_sid = coord
      .start_user_message(addr(7001), Bytes::from_static(b"a"), now)
      .expect("issued while running");
    let bad_sid = coord
      .start_user_message(addr(7002), Bytes::from_static(b"b"), now)
      .expect("issued while running");
    assert_ne!(ok_sid, bad_sid, "the two starts allocate disjoint ids");

    let mut connects = Vec::new();
    while let Some(action) = coord.poll_action() {
      if let StreamAction::Connect(info) = action {
        connects.push(info.stream_id());
      }
    }
    assert_eq!(
      connects.as_slice(),
      &[ok_sid],
      "exactly one Connect, for the SNI-resolved (first) id",
    );

    let mut aborts = Vec::new();
    while let Some(ev) = coord.poll_event() {
      if let Event::DialAborted(a) = ev {
        aborts.push((a.stream_id(), a.kind(), a.reason()));
      }
    }
    assert_eq!(
      aborts.as_slice(),
      &[(
        bad_sid,
        ExchangeKind::UserMessage,
        DialAbortReason::DialContext
      )],
      "exactly one DialAborted, for the SNI-rejected (second) id",
    );
  }

  /// STR-A001 test 5: `leave()` then `start_push_pull` — the inner endpoint
  /// hands back an inert id (no dial initiated while not running) — surfaces one
  /// `DialAborted{PushPull, NotRunning}` and leaves `pending_outbound_kinds`
  /// empty (the wrapper does NOT stage a not-running dial, closing the latent
  /// lingering-entry wart).
  #[test]
  fn not_running_push_pull_emits_dialaborted_not_running() {
    let now = Instant::now();
    let mut coord = tls_coord(7322);
    coord.leave(now).expect("leave from a running node");

    let sid = coord.start_push_pull(addr(7000), PushPullKind::Join, now);

    let mut aborts = Vec::new();
    while let Some(ev) = coord.poll_event() {
      if let Event::DialAborted(a) = ev {
        aborts.push((a.stream_id(), a.kind(), a.reason()));
      }
    }
    assert_eq!(
      aborts.as_slice(),
      &[(sid, ExchangeKind::PushPull, DialAbortReason::NotRunning)],
      "one DialAborted (PushPull, NotRunning) keyed by the inert id",
    );
    assert_eq!(
      coord.pending_outbound_kinds_len(),
      0,
      "a not-running dial stages no pending_outbound_kinds entry",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "no bridge is built while not running",
    );
  }

  /// A reaped exchange's still-queued `Connect` must never surface:
  /// `poll_action` orders every queued `Connect` before any teardown, so a
  /// `Connect` left in `pending_connects` across a deadline reap would open a
  /// transport socket for an exchange the coordinator has already removed,
  /// before the `Abort` that tells the driver to tear the connection back
  /// down.
  ///
  /// The TLS dialer stays `Handshaking` after `start_push_pull` (no server
  /// response is fed), so the bridge is still unminted when its dial deadline
  /// elapses. The `Connect` is deliberately left undrained — the driver has
  /// not yet called `poll_action` when `handle_timeout` fires at the elapsed
  /// deadline, which pumps the bridge, fails it on the handshake-deadline
  /// guard, and reaps it through the GENERIC `reap_bridge` path (not the
  /// dial-retired `dial_succeeded(None)` branch other tests cover).
  #[test]
  fn deadline_reaped_dial_surfaces_only_abort_no_stale_connect() {
    let now = Instant::now();
    let mut coord = tls_coord(7332);
    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Join, now);

    assert_eq!(
      coord.live_bridge_count(),
      1,
      "the dial built one handshaking bridge",
    );

    // Advance past the bridge's dial deadline (the default `stream_timeout`)
    // WITHOUT ever draining the queued Connect via `poll_action`, then run
    // the timer tick that reaps it.
    let later = now + Duration::from_secs(30);
    coord.handle_timeout(later);

    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the deadline-elapsed bridge is reaped",
    );

    let mut saw_abort = false;
    while let Some(action) = coord.poll_action() {
      match action {
        StreamAction::Connect(_) => {
          panic!("a reaped exchange's stale Connect must never surface")
        }
        StreamAction::Abort(_) => saw_abort = true,
        _ => {}
      }
    }
    assert!(
      saw_abort,
      "the deadline-elapsed reap emits an Abort teardown",
    );
  }
}
