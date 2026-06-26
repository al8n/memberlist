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

  use crate::{
    RawRecords,
    event::{Event, PushPullKind, StreamId},
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
}

#[cfg(feature = "tls")]
mod tls {
  use crate::Instant;
  use core::net::SocketAddr;

  use smol_str::SmolStr;
  use std::sync::Arc;

  use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified},
    crypto::CryptoProvider,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    version::TLS13,
  };

  use crate::{
    TlsOptions, TlsRecords,
    event::{Event, ExchangeKind, ExchangeStatus, PushPullKind},
    streams::{
      LabelOptions, Labeled, StreamEndpoint,
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
}
