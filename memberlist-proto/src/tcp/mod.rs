//! Plain-TCP reliable record layer for the generic Sans-I/O stream transport.
//!
//! The plain-TCP reliable path is the cluster-label decorator over a pure byte
//! pipe: [`RawRecords`] is [`crate::streams::Labeled`] wrapping
//! [`crate::streams::Passthrough`] (see [`records`]). A one-time
//! `[LABELED_TAG=12][len][label]` frame (byte-compatible with the frozen
//! `memberlist-proto` label frame) is written once at stream start by both
//! sides, then bytes pass through verbatim — there is no transport-level
//! encryption. A [`crate::streams::StreamEndpoint`] parameterised with
//! `R = RawRecords` carries reliable membership exchanges over a per-exchange
//! plain TCP connection (plain UDP carries the unreliable gossip on a separate
//! socket). TLS's in-band `close_notify` half-close anchor is replaced by the
//! out-of-band TCP FIN (`shutdown(write)`).
//!
//! The plain-TCP options bundle is [`crate::streams::LabelOptions`]`<()>`:
//! construct via [`LabelOptions::new_in`] (panics on invalid label) or
//! [`LabelOptions::try_new_in`] (checked).

#[cfg(test)]
mod bridge;
#[cfg(test)]
mod conn;
mod options;
mod records;

pub use records::RawRecords;

#[cfg(test)]
mod tests {
  use crate::Instant;
  use core::{net::SocketAddr, time::Duration};

  use bytes::Bytes;
  use smol_str::SmolStr;

  use crate::streams::LabelOptions;

  use super::records::RawRecords;
  use crate::{
    config::EndpointOptions,
    endpoint::Endpoint,
    event::{Event, PushPullKind, StreamId},
    streams::{
      ConnectInfo, ExchangeId, ExchangeRef, StreamAction, StreamEndpoint,
      test_support::{addr, endpoint, test_peer_to_socket, test_sni_provider},
    },
  };

  /// Public-constructor signature check. Mirrors
  /// `tls::tests::tls_endpoint_type_is_constructible_signature`; behavioural
  /// coverage lives in the sim harness.
  #[test]
  fn tcp_endpoint_type_is_constructible_signature() {
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());
    assert_eq!(coord.live_bridge_count(), 0);
  }

  /// `StreamAction` accessor parity: each variant's `as_*` returns `Some` only
  /// for itself. The accessors are the only seam the driver can pattern-match
  /// against without owning the enum's privacy.
  #[test]
  fn tcp_action_accessors_match_only_their_variant() {
    let id = ExchangeId::new(0);
    let connect = StreamAction::Connect(ConnectInfo::new(id, addr(7000), StreamId::from_raw(0)));
    assert!(connect.as_connect().is_some());
    assert!(connect.as_shutdown().is_none());
    assert!(connect.as_close().is_none());

    let shutdown = StreamAction::Shutdown(ExchangeRef::new(id));
    assert!(shutdown.as_shutdown().is_some());
    assert!(shutdown.as_connect().is_none());
    assert!(shutdown.as_close().is_none());

    let close = StreamAction::Close(ExchangeRef::new(id));
    assert!(close.as_close().is_some());
    assert!(close.as_connect().is_none());
    assert!(close.as_shutdown().is_none());
  }

  /// `start_push_pull` sieves the `Event::DialRequested` it emits into the
  /// private dial queue (it must NEVER leak through `poll_event`) and the
  /// in-band `service_dials` + `flush_outbound` surfaces the `Connect` action
  /// and the dialer's label prefix in the SAME call. The strict-poll
  /// self-sufficiency seam: a driver using only the public poll surface sees
  /// the dial proceed without a separate `handle_timeout` pre-pump.
  #[test]
  fn start_push_pull_dials_in_band_and_does_not_leak_dial_requested() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    let connect = coord
      .poll_action()
      .expect("the first Connect action surfaced in-band");
    let exchange_id = match &connect {
      StreamAction::Connect(c) => {
        assert_eq!(c.peer(), addr(7000));
        c.id()
      }
      other => panic!("expected Connect, got {:?}", action_kind(other)),
    };

    // No DialRequested leaks through poll_event.
    while let Some(ev) = coord.poll_event() {
      assert!(
        !matches!(ev, Event::DialRequested(..)),
        "DialRequested must NEVER leak through poll_event",
      );
    }

    // The dialer's label prefix is surfaced on the same tick.
    let (id, peer, bytes) = coord
      .poll_transport_transmit()
      .expect("the dialer queued its one-time label prefix in-band");
    assert_eq!(id, exchange_id);
    assert_eq!(peer, addr(7000));
    assert!(
      bytes.starts_with(&[12u8]),
      "label prefix begins with LABELED_TAG=12",
    );
    assert!(coord.live_bridge_count() >= 1);
  }

  /// The `Connect` action a `start_*` dial produces carries the originating
  /// `StreamId` — the very `StreamId` that `start_*` returned. This is the
  /// call-scoped correlation token drivers key their reliable-send / join
  /// capture on: `service_dials` drains a SHARED dial deque, so the peer address
  /// alone cannot tell one command's Connect apart from an unrelated same-peer
  /// dial flushed for another subsystem.
  #[test]
  fn connect_action_carries_originating_stream_id() {
    let now = Instant::now();
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());

    // start_user_message: the returned StreamId equals the Connect's stream_id().
    {
      let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
        endpoint(7100),
        cfg.clone(),
        test_sni_provider(),
        test_peer_to_socket(),
      );
      let payload = Bytes::from_static(b"hello-tcp");
      let sid = coord.start_user_message(addr(7000), payload, now);
      let connect = coord
        .poll_action()
        .expect("Connect surfaced for the outbound user-message dial");
      match connect {
        StreamAction::Connect(info) => {
          assert_eq!(
            info.stream_id(),
            sid,
            "Connect.stream_id() must equal the StreamId start_user_message returned",
          );
          assert_eq!(info.peer(), addr(7000));
        }
        other => panic!("expected Connect, got {:?}", action_kind(&other)),
      }
    }

    // start_push_pull: same threading.
    {
      let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
        endpoint(7101),
        cfg,
        test_sni_provider(),
        test_peer_to_socket(),
      );
      let sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);
      let connect = coord
        .poll_action()
        .expect("Connect surfaced for the outbound push/pull dial");
      match connect {
        StreamAction::Connect(info) => {
          assert_eq!(
            info.stream_id(),
            sid,
            "Connect.stream_id() must equal the StreamId start_push_pull returned",
          );
        }
        other => panic!("expected Connect, got {:?}", action_kind(&other)),
      }
    }
  }

  /// Two back-to-back `start_user_message` dials to the SAME peer each tag their
  /// Connect with a DISTINCT originating `StreamId` (the very ids `start_*`
  /// returned), so a driver that drains BOTH Connects in one pass can still
  /// attribute each to the exact `start_*` it issued — the peer address, shared
  /// by both, cannot. This is the precise property the drivers' StreamId-keyed
  /// capture relies on.
  #[test]
  fn same_peer_dials_carry_distinct_stream_ids() {
    let now = Instant::now();
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
      endpoint(7100),
      cfg,
      test_sni_provider(),
      test_peer_to_socket(),
    );

    let sid_a = coord.start_user_message(addr(7000), Bytes::from_static(b"a"), now);
    let sid_b = coord.start_user_message(addr(7000), Bytes::from_static(b"b"), now);
    assert_ne!(sid_a, sid_b, "each dial gets a fresh StreamId");

    let mut seen: std::collections::HashMap<StreamId, SocketAddr> =
      std::collections::HashMap::new();
    while let Some(action) = coord.poll_action() {
      if let StreamAction::Connect(info) = action {
        assert!(
          seen.insert(info.stream_id(), info.peer()).is_none(),
          "each StreamId tags at most one Connect",
        );
      }
    }
    assert_eq!(seen.get(&sid_a), Some(&addr(7000)));
    assert_eq!(seen.get(&sid_b), Some(&addr(7000)));
    assert_eq!(
      seen.len(),
      2,
      "both same-peer dials surfaced distinct Connects"
    );
  }

  /// FIN-emit gating: `fin_owed()` flips a single [`StreamAction::Shutdown`] for
  /// the exchange, latched by `ExchangeMeta::fin_emitted` against a re-emit.
  /// Mirrors the TLS coordinator's `maybe_emit_shutdown` analog, with
  /// `fin_owed()` (out-of-band TCP FIN) replacing `close_notify_sent()`
  /// (in-band TLS alert) as the gate.
  ///
  /// Drives a real push/pull exchange to the half-close anchor: the dialer's
  /// `Stream::poll_transmit` yields its request bytes; once exhausted with
  /// `sent_any = true` the bridge calls `send_close_notify()` (a no-op on
  /// TCP), latches `fin_sent`, and `fin_owed()` flips true. The coordinator's
  /// `maybe_emit_shutdown` then pushes one Shutdown action. A second tick
  /// pumping the bridge MUST NOT emit a duplicate (the `fin_emitted` latch).
  #[test]
  fn fin_owed_emits_one_shutdown_per_exchange() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // Start an outbound user-message exchange. The dialer label step settles
    // at construction (no inbound to read), the Stream is minted in the
    // same in-band flush, and the request bytes are queued — but the send
    // half is NOT yet retired (peer hasn't been heard from). On the next
    // `handle_timeout` the bridge pump observes `sent_any = true` with no
    // further yields and retires the send half, flipping `fin_owed()` true.
    let payload = Bytes::from_static(b"hello-tcp");
    let _sid = coord.start_user_message(addr(7000), payload, now);

    // Drain the in-band Connect, the dialer's label prefix, and the first
    // request bytes (still no Shutdown — `pump_out` has not seen poll_transmit
    // exhausted yet in this same tick because the request is the first yield).
    let connect = coord
      .poll_action()
      .expect("Connect surfaced for the outbound user-message dial");
    let exchange_id = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };

    // Drain everything queued in-band by the start_* call.
    while coord.poll_transport_transmit().is_some() {}

    // One subsequent tick: `pump_out` re-enters with the Stream's
    // poll_transmit exhausted and `sent_any = true`, calls
    // `records.send_close_notify()` (no-op on TCP) and `observe_send_fin()`,
    // latching `fin_sent`. The coordinator observes `fin_owed()` and pushes
    // one Shutdown.
    coord.handle_timeout(now + Duration::from_millis(1));

    let shutdown = coord
      .poll_action()
      .expect("fin_owed flips true → one Shutdown emitted");
    let shutdown_ref = match shutdown {
      StreamAction::Shutdown(r) => r,
      other => panic!("expected Shutdown, got {:?}", action_kind(&other)),
    };
    assert_eq!(
      shutdown_ref.id(),
      exchange_id,
      "Shutdown names the same exchange we dialed",
    );

    // A second tick MUST NOT re-emit Shutdown for the same exchange. The
    // bridge's `fin_owed()` stays `true` (the latch is sticky), so the
    // ExchangeMeta::fin_emitted gate is what prevents the duplicate. A
    // mutation that removes the latch check would emit a second Shutdown
    // here — the duplicate-Shutdown assertion below is the mutation gate.
    coord.handle_timeout(now + Duration::from_millis(2));

    while let Some(act) = coord.poll_action() {
      assert!(
        !matches!(act, StreamAction::Shutdown(_)),
        "fin_emitted latch must suppress a duplicate Shutdown for the same exchange (got {:?})",
        action_kind(&act),
      );
    }
  }

  /// Tick step (2) (bridge pump + endpoint-event drain) MUST run strictly
  /// before step (3) (`Endpoint::handle_timeout`): an inbound reply that lands
  /// the same tick a deadline elapses on must reach the `Endpoint` before the
  /// timer-driven cleanup fires, or a live exchange is wrongly closed. This
  /// test exercises the ordering directly: a configured `handle_timeout(now)`
  /// runs the full tick without panic (the inner ordering is preserved by
  /// `run_tick` regardless of whether a bridge is present). Behavioural
  /// coverage of the ordering on a live exchange is the sim harness's job.
  #[test]
  fn handle_timeout_drives_pump_before_endpoint_timeout() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // Repeatedly ticking with no bridges is safe and a no-op on the bridge
    // pump — the ordering invariant means the inner endpoint advances cleanly.
    coord.handle_timeout(now);
    coord.handle_timeout(now + Duration::from_millis(1));
    assert_eq!(coord.live_bridge_count(), 0);
  }

  /// `accept_connection` allocates a fresh `ExchangeId`, installs an acceptor
  /// bridge bounded by `ACCEPT_HANDSHAKE_DEADLINE`, and does NOT emit a
  /// Connect/Shutdown/Close action (the driver already has the inbound
  /// socket).
  #[test]
  fn accept_connection_installs_acceptor_bridge_without_emitting_actions() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    let id = coord.accept_connection(addr(7000), now);
    assert_eq!(coord.live_bridge_count(), 1);
    // The handle's monotonic-id property is exercised in StreamConns tests;
    // this assertion only proves the bridge was inserted.
    let _ = id;

    assert!(
      coord.poll_action().is_none(),
      "accept_connection MUST NOT enqueue Connect/Shutdown/Close",
    );
    assert!(coord.poll_transport_transmit().is_none());
  }

  /// A `handle_timeout` whose membership-timer step (step 3) emits a fresh
  /// `Event::DialRequested` — the canonical case is the reliable-fallback
  /// ping the SWIM probe arms on direct timeout — MUST surface the
  /// dialer's `Connect` action AND its first request bytes on the SAME
  /// tick. The dialer's [`RawRecords`](records::RawRecords) is not
  /// handshaking from construction, so step (4)'s
  /// `service_handshake_completions` (which runs BEFORE step (5)'s
  /// `service_dials`) never sees the freshly-inserted bridge; step (5.5)'s
  /// extra `service_handshake_completions` promotes it before step (5.6)'s
  /// `pump_bridges` transmits the request.
  ///
  /// Without step (5.5), a driver that wakes solely on [`Self::poll_timeout`]
  /// — the strict-poll discipline — would not see the request bytes appear
  /// until the bridge's exchange deadline (the only timer remaining once
  /// `Endpoint::poll_timeout` and the bridge's own `poll_timeout` are
  /// drained), by which point [`crate::stream::Stream::handle_data`] would
  /// reject them as timed out and the fallback ping would never reach the
  /// peer.
  ///
  /// Mutation gate: deleting step (5.5) from `run_tick` fails the
  /// `poll_transport_transmit().is_some()` assertion below — the first
  /// post-tick `poll_transport_transmit` would yield `None` because the
  /// bridge is still unpromoted at `pump_bridges` time and has no Stream to
  /// drive `poll_transmit` for.
  #[test]
  fn timer_emitted_dial_request_bytes_appear_same_tick() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // Inject a DialRequested directly into the private dial queue — the
    // shape `Endpoint::handle_timeout` would produce when SWIM arms the
    // reliable-fallback ping inside step (3). Use `start_reliable_ping`
    // on the inner endpoint and then drive the run_tick path WITHOUT the
    // flush_outbound that `start_*` wrappers normally run, so only the
    // `handle_timeout` → run_tick path is exercised.
    let sid = coord.endpoint_mut().start_reliable_ping(
      SmolStr::new("srv"),
      addr(7000),
      7,
      now + Duration::from_secs(5),
    );
    // The returned StreamId is not consulted; the test asserts on the
    // coordinator's pumped state below.
    let _ = sid;

    // ONE handle_timeout(now): step (3) sieves the DialRequested → dial_pending,
    // step (5) `service_dials` builds the dialer bridge, step (5.5)
    // `service_handshake_completions` promotes it (the dialer is not
    // handshaking from construction), step (5.6) `pump_bridges` drives
    // `Stream::poll_transmit` and queues the request bytes.
    coord.handle_timeout(now);

    let mut connect_exchange = None;
    while let Some(act) = coord.poll_action() {
      if let StreamAction::Connect(c) = act {
        connect_exchange = Some(c.id());
        break;
      }
    }
    let connect_exchange = connect_exchange.expect("Connect action surfaced this tick");
    let (id, _peer, bytes) = coord
      .poll_transport_transmit()
      .expect("the dialer's [label||request] bytes are on the wire this tick");
    assert_eq!(id, connect_exchange);
    assert!(
      bytes.starts_with(&[12u8]),
      "first byte is LABELED_TAG (the one-time label prefix)"
    );
    assert!(
      bytes.len() > 2 + "cluster-x".len(),
      "bytes carry both the label prefix AND a non-empty request tail (got {} bytes)",
      bytes.len()
    );
  }

  /// A driver may deliver a peer's `[label||first request]` plus the same-tick
  /// out-of-band TCP FIN as ONE coordinator call
  /// (`handle_transport_data(id, bytes, eof=true, now)`). The coordinator splits
  /// that into a bytes feed followed by an empty-slice EOF anchor; the bridge
  /// is still `Handshaking` for BOTH feeds (the `Stream` is minted by step (4)
  /// `service_handshake_completions`, not by the bytes feed itself). The
  /// pre-promote FIN must therefore be latched on the bridge so the post-mint
  /// [`crate::streams::bridge::StreamBridge::replay_pending`] honors it; otherwise
  /// the bridge promotes, drains the buffered request without an EOF, never
  /// retires its recv half, stays in `Established(SendClosed)` (the one-way
  /// user-message reaches FSM `Done` and the bridge half-closes its send
  /// side), and is held until the accept deadline elapses — a single coalesced
  /// exchange stalls 10 s on a peer that half-closes after the request.
  ///
  /// The structural analog of the TLS coordinator's coalesced
  /// `[handshake_final||first_record]||close_notify` round trip, except TCP's
  /// FIN is out of band so the latch lives on the bridge (not on the records
  /// layer — `RawRecords::peer_has_closed()` is permanently `false`).
  #[test]
  fn coalesced_label_request_with_eof_terminalizes_same_tick_or_soon() {
    let now = Instant::now();
    let server_addr = addr(7400);
    let dialer_addr = addr(7401);

    // Server coordinator. The acceptor bridge is installed by
    // `accept_connection`; the dialer's coalesced bytes arrive via
    // `handle_transport_data` and the same-tick EOF rides the same call.
    let server_ep = endpoint(server_addr.port());
    let cfg_s = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut server: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(server_ep, cfg_s, test_sni_provider(), test_peer_to_socket());
    let server_exchange = server.accept_connection(dialer_addr, now);
    assert_eq!(server.live_bridge_count(), 1);

    // Dialer coordinator. Starting a one-way user-message produces the
    // `[label||frame]` blob on the very first `poll_transport_transmit` (the
    // dialer's label step settles at construction and `start_user_message`
    // flushes outbound in-band).
    let dialer_ep = endpoint(dialer_addr.port());
    let cfg_d = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut dialer: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(dialer_ep, cfg_d, test_sni_provider(), test_peer_to_socket());
    let payload = Bytes::from_static(b"coalesced-with-eof");
    let _sid = dialer.start_user_message(server_addr, payload.clone(), now);
    // The user message reaches Done on the dialer this tick, so a second
    // `handle_timeout` retires its send half and `Shutdown` is emitted; we
    // only need the bytes for this assertion.
    dialer.handle_timeout(now);

    // Coalesce every byte the dialer produced this tick (the label prefix
    // and the user-message frame) into one buffer.
    let mut coalesced = Vec::new();
    while let Some((_id, _peer, bytes)) = dialer.poll_transport_transmit() {
      coalesced.extend_from_slice(&bytes);
    }
    assert!(
      coalesced.len() > 2,
      "the dialer produced both the label prefix and the user-message frame, \
       got {} bytes",
      coalesced.len()
    );

    // Deliver the WHOLE coalesced blob with the same-tick out-of-band FIN in
    // ONE coordinator call — the exact shape the bug is about.
    server.handle_transport_data(server_exchange, &coalesced, true, now);
    // One follow-up tick at the SAME `now` so any post-mint pump that ran in
    // step (5.5) has flushed and reaping has had a tick to surface `Close`.
    server.handle_timeout(now);

    // The buffered user-message frame surfaced on the server `Endpoint` (the
    // recv-half FIN authorized the dispatched event; without the latch the
    // event would still surface, but only after the 10 s accept deadline).
    let mut got = None;
    while let Some(ev) = server.poll_event() {
      if let Event::UserPacket(p) = ev {
        got = Some(p.into_parts().1);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the coalesced user-message frame surfaced on the server Endpoint",
    );

    // The bridge reached `BothClosed` and was reaped this same tick: the
    // coordinator emitted exactly one `Close` for this exchange (preceded by
    // a `Shutdown`, since the bridge retired its send half on the same tick
    // it observed the recv FIN), and `live_bridge_count()` is back to zero.
    // The teardown for this exchange is withheld behind its bytes — drain the
    // transmit queue first, then collect the actions.
    let mut saw_close_for_exchange = false;
    let mut close_count = 0usize;
    loop {
      let mut made_progress = false;
      while let Some(act) = server.poll_action() {
        if let StreamAction::Close(r) = act {
          if r.id() == server_exchange {
            saw_close_for_exchange = true;
          }
          close_count += 1;
        }
        made_progress = true;
      }
      while server.poll_transport_transmit().is_some() {
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }
    assert!(
      saw_close_for_exchange,
      "the coordinator reaped the bridge and emitted Close for the exchange",
    );
    assert_eq!(close_count, 1, "exactly one Close per exchange");
    assert_eq!(
      server.live_bridge_count(),
      0,
      "the bridge was reaped — no leaked exchange held to the accept deadline",
    );
  }

  /// Within-tick action ordering: a `Shutdown` enqueued for an existing
  /// bridge BEFORE a `Connect` enqueued for a brand-new dial in the same tick
  /// MUST surface from [`StreamEndpoint::poll_action`] as `[Connect, Shutdown]`,
  /// not the FIFO insertion order. A naive driver that drains actions in
  /// poll-order, writes bytes, and stops on the first non-`Connect` would
  /// otherwise pop the `Shutdown`, drain transmit (which carries bytes for
  /// the NEW exchange under same-tick promote), and orphan-write to a peer
  /// the `Connect` had not yet opened.
  ///
  /// Mutation gate: reverting [`StreamEndpoint::poll_action`] to a single-queue
  /// FIFO pop fails the first assertion below — the first action returned
  /// would be the `Shutdown` (insertion order) rather than the `Connect`.
  #[test]
  fn same_tick_shutdown_old_and_connect_new_emit_connect_first() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // Simulate step (2) `pump_bridges` enqueuing a `Shutdown` for an
    // existing bridge by pushing one directly to the teardown queue (the
    // same producer site `maybe_emit_shutdown` uses). The `ExchangeId` value
    // is irrelevant for the ordering assertion — only the variant ordering
    // is.
    let existing = ExchangeId::new(42);
    coord.push_teardown(StreamAction::Shutdown(ExchangeRef::new(existing)));

    // Now drive a brand-new dial through the same tick the `Shutdown` is
    // queued in: `start_push_pull` runs `service_dials` in-band, which
    // pushes a `Connect` to the connect queue. After this call the
    // coordinator has BOTH a queued `Shutdown` (for the existing exchange)
    // AND a queued `Connect` (for the brand-new exchange).
    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    let first = coord
      .poll_action()
      .expect("a Connect was enqueued by the in-band service_dials");
    let new_exchange = match first {
      StreamAction::Connect(c) => {
        assert_eq!(
          c.peer(),
          addr(7000),
          "the in-band dial's Connect names the new peer",
        );
        c.id()
      }
      other => panic!(
        "first poll_action MUST surface the Connect before any Shutdown / \
         Close enqueued the same tick (got {:?})",
        action_kind(&other),
      ),
    };
    assert_ne!(
      new_exchange, existing,
      "the new dial's exchange id is distinct from the existing one",
    );

    // The very next poll_action surfaces the Shutdown for the existing
    // exchange (teardowns drain only after every queued Connect has been
    // drained).
    let second = coord
      .poll_action()
      .expect("the queued Shutdown surfaces after the Connect");
    match second {
      StreamAction::Shutdown(r) => assert_eq!(
        r.id(),
        existing,
        "the trailing Shutdown still names the existing exchange",
      ),
      other => panic!(
        "second poll_action MUST be the queued Shutdown (got {:?})",
        action_kind(&other),
      ),
    }
  }

  /// Within-tick action ordering, `Close` variant: a `Close` enqueued for a
  /// terminal-reap of an old bridge BEFORE a `Connect` enqueued for a
  /// brand-new dial in the same tick MUST surface as `[Connect, Close]`.
  /// Mirrors [`same_tick_shutdown_old_and_connect_new_emit_connect_first`]
  /// for the `Close` producer site
  /// ([`StreamEndpoint::reap_bridge`] and the dial-deadline-elapsed branch of
  /// [`StreamEndpoint::service_handshake_completions`]).
  ///
  /// Mutation gate: reverting [`StreamEndpoint::poll_action`] to a single-queue
  /// FIFO pop fails the first assertion below — the first action returned
  /// would be the `Close` (insertion order).
  #[test]
  fn same_tick_close_and_new_connect_emit_connect_first() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    let reaped = ExchangeId::new(99);
    coord.push_teardown(StreamAction::Close(ExchangeRef::new(reaped)));

    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    let first = coord
      .poll_action()
      .expect("a Connect was enqueued by the in-band service_dials");
    let new_exchange = match first {
      StreamAction::Connect(c) => {
        assert_eq!(c.peer(), addr(7000));
        c.id()
      }
      other => panic!(
        "first poll_action MUST surface the Connect before any Shutdown / \
         Close enqueued the same tick (got {:?})",
        action_kind(&other),
      ),
    };
    assert_ne!(
      new_exchange, reaped,
      "the new dial's exchange id is distinct from the reaped one",
    );

    let second = coord
      .poll_action()
      .expect("the queued Close surfaces after the Connect");
    match second {
      StreamAction::Close(r) => assert_eq!(
        r.id(),
        reaped,
        "the trailing Close still names the reaped exchange",
      ),
      other => panic!(
        "second poll_action MUST be the queued Close (got {:?})",
        action_kind(&other),
      ),
    }
  }

  /// A naive driver — one that calls [`StreamEndpoint::poll_action`] and
  /// [`StreamEndpoint::poll_transport_transmit`] in a single loop, with no
  /// explicit phase partitioning — MUST observe every byte tagged with an
  /// exchange's [`ExchangeId`] BEFORE it observes that exchange's
  /// [`StreamAction::Shutdown`] / [`StreamAction::Close`]. Applying the teardown
  /// first would issue TCP `shutdown(write)` on a socket whose last bytes
  /// have not yet been written; the send half closes and the trailing write
  /// fails, orphaning the response.
  ///
  /// Setup: a one-way outbound `start_user_message` — the dialer's request
  /// bytes and its half-close anchor both land in the SAME tick. The
  /// request is drained from the stream's `poll_transmit` (sent_any flips
  /// true) and the next iteration exhausts it; `pump_out` then calls
  /// `send_close_notify` + `observe_send_fin`, flipping `fin_owed` true,
  /// and `maybe_emit_shutdown` enqueues the `Shutdown` — all before
  /// `finalize_tick` collects the bytes into `out_transmit`.
  ///
  /// Mutation gate: dropping the per-exchange transmit-pending guard from
  /// [`StreamEndpoint::poll_action`] makes a naive drain emit the `Shutdown`
  /// in the same iteration as the bytes (the `Shutdown` surfaces from the
  /// first `poll_action` sweep BEFORE the bytes are drained), failing the
  /// "last byte observed before Shutdown" assertion below.
  #[test]
  fn naive_drain_loop_writes_last_bytes_before_shutdown() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // One-way user-message: the dialer's request and its half-close anchor
    // both land in the in-band `flush_outbound` tick.
    let payload = Bytes::from_static(b"orphan-canary");
    let _sid = coord.start_user_message(addr(7000), payload, now);

    // Naive driver loop: drain actions, drain transmits, repeat until idle.
    // Record what was observed per iteration so we can prove the LAST byte
    // for the exchange was observed BEFORE the exchange's `Shutdown`.
    let mut bytes_observed: Vec<(usize, ExchangeId)> = Vec::new();
    let mut actions_observed: Vec<(usize, StreamAction)> = Vec::new();
    let mut iter = 0usize;
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push((iter, action));
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        assert!(!bytes.is_empty(), "transmit chunks are never empty");
        bytes_observed.push((iter, id));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
      iter += 1;
    }

    // The Connect was emitted first, in iteration 0.
    let (connect_iter, connect_id) = actions_observed
      .iter()
      .find_map(|(i, a)| a.as_connect().map(|c| (*i, c.id())))
      .expect("Connect was emitted for the dialed user-message exchange");
    assert_eq!(
      connect_iter, 0,
      "Connect surfaces in the first poll_action sweep",
    );

    // The Shutdown was emitted for the same exchange.
    let (shutdown_iter, shutdown_id) = actions_observed
      .iter()
      .find_map(|(i, a)| a.as_shutdown().map(|r| (*i, r.id())))
      .expect("Shutdown was emitted once the send half retired");
    assert_eq!(
      shutdown_id, connect_id,
      "Shutdown names the same exchange the Connect opened",
    );

    // The LAST byte tagged with the exchange was observed in some iteration.
    let last_byte_iter = bytes_observed
      .iter()
      .rfind(|(_, id)| *id == connect_id)
      .map(|(i, _)| *i)
      .expect("the dialer queued bytes for the exchange");

    // The load-bearing assertion: the LAST byte for the exchange was
    // observed in an iteration STRICTLY BEFORE the Shutdown for that
    // exchange. With the per-exchange transmit-pending gate this holds by
    // construction; without it, the Shutdown surfaces in the same iteration
    // as the bytes (or earlier), and a naive `apply(action)` loop would
    // shutdown(write) the socket before writing the trailing bytes.
    assert!(
      last_byte_iter < shutdown_iter,
      "last byte for exchange {:?} was observed in iter {} but Shutdown \
       surfaced in iter {} — a naive driver would orphan the bytes",
      connect_id,
      last_byte_iter,
      shutdown_iter,
    );
  }

  /// An exchange whose deadline elapses BEFORE the driver has drained its
  /// outbound bytes must not leak those bytes after the bridge has been
  /// reaped. The transmit queue is eagerly populated by
  /// [`StreamEndpoint::pump_bridges`] one tick (the bridge's pre-failure label /
  /// request chunk lands in `out_transmit`); a later tick fires
  /// [`StreamEndpoint::handle_timeout`] past the bridge's deadline, the
  /// `pump_out` deadline path fails the bridge, and
  /// [`StreamEndpoint::reap_bridge`] enqueues `Close`. Without the
  /// [`StreamEndpoint::purge_transmit_for`] step, a driver doing the natural
  /// "drain actions, drain transmits, repeat" loop would emit the stale
  /// pre-failure bytes (the teardown gate withholds `Close` while
  /// `out_transmit` still has the exchange's chunk, so the bytes surface
  /// first) — delivering membership state from an exchange the local node
  /// has already failed, or sending a probe to a peer the local node has
  /// already given up on.
  ///
  /// Setup: dial an outbound push/pull at `now`, drain its `Connect` (so
  /// the natural drain loop below sees only the post-failure ordering of
  /// transmit vs. teardown), and advance the clock past the bridge's
  /// exchange deadline. The bridge's deadline is the exchange deadline
  /// carried on `Event::DialRequested` (`now + stream_timeout`, default
  /// 10s), so `now + 11s` is strictly past it.
  ///
  /// Mutation gate: reverting the `purge_transmit_for(id)` call in
  /// [`StreamEndpoint::reap_bridge`] fails the "no stale bytes" assertion
  /// below — the dialer's `[label, request]` chunk surfaces from the
  /// natural drain loop ahead of the released `Close`.
  #[test]
  fn timed_out_exchange_purges_stale_transmit_before_close() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Dial an outbound push/pull. The in-band `service_dials` +
    // `flush_outbound` builds and promotes the bridge in the SAME tick and
    // queues `[label, request]` into `out_transmit` (see `run_tick` step
    // (5.5)).
    let _sid = coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);

    // (2) Drain the `Connect` to keep the natural drain loop below from
    // racing it against the post-failure bytes / `Close` ordering. The
    // gate has no Connects-vs-bytes interaction; pulling the `Connect` out
    // here isolates the assertion to the stale-byte / `Close` ordering.
    let connect = coord.poll_action();
    let exchange = match connect {
      Some(StreamAction::Connect(c)) => c.id(),
      other => panic!(
        "first poll_action MUST surface the dial's Connect (got {:?})",
        other.as_ref().map(action_kind),
      ),
    };
    // Do NOT drain `poll_transport_transmit` here — the queued
    // `[label, request]` chunk for the soon-to-fail exchange is exactly
    // the stale byte payload the purge must drop.
    assert!(
      coord.exchange_has_pending_bytes(exchange),
      "pre-failure invariant: the dialer queued bytes for the exchange",
    );

    // (3) Advance past the bridge's exchange deadline. The default
    // `stream_timeout` is 10s and the dial deadline is `now + 10s`;
    // pumping at `now + 11s` is strictly past it.
    let deadline_elapsed = now + Duration::from_secs(11);
    coord.handle_timeout(deadline_elapsed);

    // (4) Natural drain loop. With the purge in place, the gate releases
    // the `Close` immediately (`out_transmit` for the exchange is empty
    // after the reap), and no stale bytes for the failed exchange surface
    // from `poll_transport_transmit`.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (5) Load-bearing assertion: NO bytes for the failed exchange
    // surfaced. Mutation gate: dropping `purge_transmit_for` from
    // `reap_bridge` lets the dialer's pre-failure `[label, request]` chunk
    // surface here.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "stale bytes for the timed-out exchange {:?} must not be emitted; \
       got {:?}",
      exchange,
      stale,
    );

    // (6) Load-bearing assertion: `Abort` for the failed exchange did
    // surface — the gate must release the teardown once the purge empties
    // the exchange's queue, else the bridge would leak (no teardown ever
    // fires). A timed-out exchange is FAILED, so the reap emits `Abort` (the
    // driver discards its stale buffered bytes) rather than `Close`.
    let abort_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      abort_for_exchange,
      "Abort for the timed-out exchange {:?} must surface from the natural \
       drain loop after the purge releases the gate; got {:?}",
      exchange,
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// An acceptor bridge whose inbound label is REJECTED (a wrong-cluster
  /// peer dials this node) must not leak its local label prefix to the peer
  /// before the coordinator emits `Close`. The acceptor's `RawRecords`
  /// queues `[12][len][local_label]` in its outbound buffer at
  /// construction; without the bridge's failure-retirement clearing that
  /// buffer, the reap path's `collect_bridge_transmits` drains it into
  /// `out_transmit` AFTER `purge_transmit_for` runs — and a driver doing
  /// the natural "drain actions, drain transmits, repeat" loop emits the
  /// local cluster label on the wire to a peer whose dial was just
  /// rejected.
  ///
  /// Setup: `accept_connection` builds the acceptor; one
  /// `handle_transport_data` delivers a wrong-cluster `[12][len][other]`
  /// header that `intake_handshaking` rejects (`self.fail(Transport)`); the
  /// natural drain loop then observes the resulting `Close` with NO bytes
  /// for the exchange.
  ///
  /// Mutation gate: removing `self.records.clear_outbound()` from
  /// `StreamBridge::fail` fails the "no stale bytes" assertion below — the
  /// leaked chunk is the local label `[12][len][cluster-x]` queued at
  /// `accept_connection` time.
  #[test]
  fn failed_acceptor_label_mismatch_no_bytes_before_close() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Driver accepts an inbound TCP connection from a wrong-cluster
    // peer. The acceptor stays Handshaking until its inbound label is
    // validated; its lazy label gate has queued no outbound prefix yet, so
    // nothing is owed to the wire.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(
      coord.poll_transport_transmit().is_none(),
      "accept_connection emits no transmits in-band",
    );

    // (2) Wrong-cluster peer sends its own labeled header. The acceptor's
    // label gate returns a reject (label mismatch) → the decorator surfaces
    // `Intake::Failed`, the bridge enters `Failed(Transport)` via
    // `intake_handshaking`'s `self.fail(...)`, and the driver-facing
    // `run_tick` runs through to the reap.
    let mut wrong = vec![12u8, 7];
    wrong.extend_from_slice(b"other-x");
    coord.handle_transport_data(exchange, &wrong, false, now);

    // (3) Natural drain loop: drain actions, drain transmits, repeat
    // until idle. Record everything observed for this exchange.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (4) Load-bearing assertion: NO bytes for the failed exchange
    // surfaced — the local label `[12][len][cluster-x]` queued at
    // `accept_connection` was cleared by `StreamBridge::fail`'s
    // `records.clear_outbound()` before the reap could drain it.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "stale outbound bytes for the rejected acceptor {:?} must not be \
       emitted to the wrong-cluster peer; got {:?}",
      exchange,
      stale,
    );

    // (5) Load-bearing assertion: `Abort` for the failed exchange did
    // surface — the bridge is retired (its inbound label was rejected), so
    // the coordinator MUST signal the driver to RST the socket and discard
    // its stale outbound bytes.
    let abort_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      abort_for_exchange,
      "Abort for the rejected acceptor {:?} must surface; got {:?}",
      exchange,
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// An acceptor whose `ACCEPT_HANDSHAKE_DEADLINE` elapses with NO inbound
  /// bytes (the slow-loris that connects but never sends a label) must not
  /// leak its local label prefix to that peer on the reap. The
  /// handshake-deadline guard in `StreamBridge::pump_out` fires
  /// `self.fail(Timeout)`, and the bridge's `records.clear_outbound()`
  /// drops the queued `[12][len][local_label]` before the coordinator's
  /// reap path can collect it.
  ///
  /// Setup: `accept_connection` builds the acceptor; NO
  /// `handle_transport_data` is delivered (the peer connects then stalls).
  /// One `handle_timeout(now + ACCEPT_HANDSHAKE_DEADLINE + 1s)` drives the
  /// handshake-deadline reap.
  ///
  /// Mutation gate: removing `self.records.clear_outbound()` from
  /// `StreamBridge::fail` fails the "no stale bytes" assertion — the local
  /// label leaks to the stalled peer at reap.
  #[test]
  fn failed_accept_handshake_timeout_no_bytes_before_close() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Driver accepts an inbound connection; the acceptor's lazy label
    // gate queues no outbound prefix until its inbound label validates.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(
      coord.poll_transport_transmit().is_none(),
      "accept_connection emits no transmits in-band",
    );

    // (2) Slow-loris peer: NO `handle_transport_data` is delivered. The
    // bridge's deadline is `now + ACCEPT_HANDSHAKE_DEADLINE` (10s by
    // default); ticking at `now + 11s` is strictly past it.
    let deadline_elapsed = now + Duration::from_secs(11);
    coord.handle_timeout(deadline_elapsed);

    // (3) Natural drain loop. The handshake-deadline guard fired
    // (`self.fail(Timeout)`), `clear_outbound` ran, and the reap's
    // `collect_bridge_transmits` drained an empty buffer.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (4) Load-bearing assertion: NO bytes for the stalled exchange
    // surfaced.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "stale outbound bytes for the slow-loris acceptor {:?} must not be \
       emitted on the handshake-deadline reap; got {:?}",
      exchange,
      stale,
    );

    // (5) Load-bearing assertion: `Abort` for the failed exchange did
    // surface — the slow-loris acceptor timed out (FAILED), so the reap
    // emits `Abort` to RST the socket and discard its stale bytes.
    let abort_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      abort_for_exchange,
      "Abort for the slow-loris acceptor {:?} must surface; got {:?}",
      exchange,
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// A driver that runs one `handle_timeout` tick on a freshly-accepted
  /// connection — BEFORE any inbound bytes arrive — must observe NO
  /// outbound bytes for that exchange. The acceptor's outbound label prefix
  /// is queued LAZILY at inbound-label validation, so it is empty across the
  /// pump and `finalize_tick`'s `collect_bridge_transmits` drains nothing for this
  /// exchange. Faithful to `memberlist-core/src/network.rs::handle_conn`'s
  /// `read_message`-before-`send_message` ordering: an acceptor reveals
  /// cluster identity only after the dialer has proven its own.
  ///
  /// Mutation gate: switching the acceptor's [`crate::streams::label::LabelGate`]
  /// to an eager (dialer-style) outbound-prefix queue makes this test fail
  /// with the leaked `[12][len][cluster-x]` bytes surfacing on the first pump.
  #[test]
  fn accept_connection_then_handle_timeout_no_bytes_before_inbound_validation() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Driver accepts an inbound TCP connection; the acceptor is
    // Handshaking with an EMPTY `records.outbound` (lazy queue not yet
    // fired). No same-tick transmits surface from `accept_connection`.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(
      coord.poll_transport_transmit().is_none(),
      "accept_connection emits no transmits in-band",
    );

    // (2) One driver tick with NO inbound bytes — models a scanner /
    // wrong-cluster / slow-loris peer that completes only the TCP
    // handshake. The bridge stays Handshaking, the lazy queue stays
    // dormant, `pump_bridges` finds an empty outbound buffer, and
    // `finalize_tick` collects nothing for the exchange.
    coord.handle_timeout(now);

    // (3) Drain everything the driver can observe.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (4) Load-bearing assertion: NO bytes for the still-handshaking
    // acceptor surfaced — the local label `[12][9][cluster-x]` is still
    // unqueued because inbound validation has not happened.
    let leaked: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      leaked.is_empty(),
      "handshaking acceptor {:?} must not leak the local cluster label to a \
       peer that has not validated its inbound label; got {:?}",
      exchange,
      leaked,
    );

    // (5) Sanity: no `Close` either — the bridge is still alive, waiting
    // for the dialer's labeled request (bounded by ACCEPT_HANDSHAKE_DEADLINE).
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      !close_for_exchange,
      "the bridge is still Handshaking; no Close should fire yet",
    );
  }

  /// Once a valid inbound label arrives, the acceptor's lazy outbound
  /// label queue fires and the next pump surfaces the `[12][len][label]`
  /// prefix on the wire — proving the asymmetric-queue redesign still
  /// emits the labeled reply when cluster identity is confirmed. Paired
  /// with `accept_connection_then_handle_timeout_no_bytes_before_inbound_validation`
  /// for the full "silent until validated, then labeled" invariant.
  #[test]
  fn accept_connection_then_valid_inbound_then_outbound_label_emitted() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Accept the connection; nothing on the wire yet.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(coord.poll_transport_transmit().is_none());

    // (2) Deliver a matching labeled header from the peer; the acceptor's
    // label gate accepts it, the lazy outbound-prefix queue fires inside the
    // gate's accepted branch, and the acceptor's outbound prefix now holds
    // the `[12][9][cluster-x]` bytes.
    let mut inbound = vec![12u8, 9];
    inbound.extend_from_slice(b"cluster-x");
    coord.handle_transport_data(exchange, &inbound, false, now);

    // (3) One tick to run the pump + collect bytes.
    coord.handle_timeout(now);

    // (4) Drain the queue and confirm the acceptor's outbound label
    // prefix surfaced for the exchange.
    let mut bytes_for_exchange: Vec<Bytes> = Vec::new();
    while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
      if id == exchange {
        bytes_for_exchange.push(bytes);
      }
    }
    assert!(
      !bytes_for_exchange.is_empty(),
      "lazy queue must fire on inbound validation; expected the acceptor's \
       labeled reply prefix to surface on the wire",
    );
    let first = &bytes_for_exchange[0];
    assert!(
      first.len() >= 2 + 9,
      "first chunk must contain at least the full label prefix, got len={}",
      first.len(),
    );
    assert_eq!(first[0], 12, "leading byte is LABELED_TAG");
    assert_eq!(
      first[1] as usize,
      "cluster-x".len(),
      "declared label length"
    );
    assert_eq!(
      &first[2..2 + 9],
      b"cluster-x",
      "label bytes match the configured cluster label",
    );
  }

  /// A clean acceptor exchange whose inbound `[label]` and `[request][FIN]`
  /// arrive in TWO separate transport reads must deliver BOTH its outbound
  /// label prefix AND its push/pull response to the wire — the
  /// [`StreamEndpoint::reap_bridge`] purge must not drop the legitimately-queued
  /// label.
  ///
  /// Setup: a dialer/acceptor pair on shared in-process coordinators. The
  /// dialer produces a real `[label]||[push/pull request]` blob via
  /// `start_push_pull`. We feed the LABEL portion to the acceptor with
  /// `eof=false` (first read), tick to drain the acceptor's lazy label into
  /// `out_transmit`, then feed the REQUEST tail with `eof=true` (second
  /// read), and tick once more to let the acceptor's response reach
  /// `out_transmit` and the bridge reach `BothClosed` for the clean reap.
  ///
  /// With the `is_failed()` gate on [`StreamEndpoint::reap_bridge`]'s
  /// [`StreamEndpoint::purge_transmit_for`] step, the clean reap preserves
  /// BOTH the label chunk (queued during the first tick) and the response
  /// chunk (added by the reap's `collect_bridge_transmits`). Without the
  /// gate the purge drops the label chunk and the peer sees a labelless
  /// response — its own inbound-label check then rejects the reply.
  ///
  /// Mutation gate: dropping the `if br.is_failed()` gate in
  /// `reap_bridge` (unconditional purge) fails the
  /// `response_bytes_after_reap_present` assertion below — `second_chunks`
  /// is empty because the purge dropped the label that earlier pumps had
  /// queued, leaving the reap with the response chunk only, and the queued
  /// label is gone.
  #[test]
  fn clean_acceptor_split_inbound_label_then_request_preserves_full_response_on_wire() {
    let now = Instant::now();
    let server_addr = addr(7500);
    let dialer_addr = addr(7501);

    // (1) Build a real dialer coordinator and ask it to start a push/pull.
    // The in-band `service_dials` + `flush_outbound` builds the dialer's
    // bridge, surfaces `Connect`, and queues `[label||push_pull_request]`
    // into `out_transmit`.
    let dialer_ep = endpoint(dialer_addr.port());
    let cfg_d = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut dialer: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(dialer_ep, cfg_d, test_sni_provider(), test_peer_to_socket());
    let _sid = dialer.start_push_pull(server_addr, PushPullKind::Join, now);
    // Drain the `Connect` so the natural drain loop below sees only bytes.
    // The action's payload is asserted on by `.expect(...)`; only the
    // binding is discarded.
    let _ = dialer
      .poll_action()
      .expect("dialer's first poll_action is the Connect");
    let mut dialer_bytes = Vec::new();
    while let Some((_id, _peer, bytes)) = dialer.poll_transport_transmit() {
      dialer_bytes.extend_from_slice(&bytes);
    }
    // The dialer produced `[12][9][cluster-x]` followed by an encoded
    // push/pull request frame. The label prefix is exactly 11 bytes; the
    // request tail is what remains.
    assert!(
      dialer_bytes.len() > 11,
      "dialer produced label + request, got {} bytes",
      dialer_bytes.len(),
    );
    assert_eq!(dialer_bytes[0], 12, "LABELED_TAG byte");
    assert_eq!(dialer_bytes[1] as usize, "cluster-x".len());
    let label_only = &dialer_bytes[..11];
    let request_tail = &dialer_bytes[11..];

    // (2) Build the acceptor coordinator and accept the connection. The
    // bridge is Handshaking; the lazy outbound label has NOT fired yet.
    let server_ep = endpoint(server_addr.port());
    let cfg_s = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut server: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(server_ep, cfg_s, test_sni_provider(), test_peer_to_socket());
    let server_exchange = server.accept_connection(dialer_addr, now);

    // (3) First transport read: ONLY the dialer's label prefix, no FIN.
    // The acceptor's inbound label validates, the lazy outbound label fires
    // inside the label gate's accepted branch, and the acceptor's outbound
    // prefix now holds `[12][9][cluster-x]`. The Stream is not minted yet
    // because the bytes-only feed leaves the bridge Handshaking until the
    // next `run_tick`'s `service_handshake_completions`.
    server.handle_transport_data(server_exchange, label_only, false, now);

    // (4) One tick to promote and drain the acceptor's lazy label into
    // `out_transmit`. The bridge transitions Handshaking → Active and the
    // pump drains the queued label prefix into the coordinator's transmit
    // queue. The acceptor's lazy-queued label NOW sits in `out_transmit`
    // tagged with `server_exchange` — and we do NOT drain it yet (the
    // load-bearing invariant: a driver that ticks the coordinator multiple
    // times before reaching its `poll_transport_transmit` loop must still
    // receive the full reply).
    server.handle_timeout(now);
    assert!(
      server.exchange_has_pending_bytes(server_exchange),
      "the acceptor's lazy-queued label must be sitting in out_transmit \
       between ticks (the pre-condition for a clean reap with already-queued \
       bytes)",
    );

    // (5) Second transport read: the request tail + the same-tick FIN
    // anchor. The Stream FSM decodes the push/pull request, the endpoint
    // generates a response (the `SendPushPullResponse` command, encoded
    // and loaded by `drain_then_reap`), the bridge observes the recv-half
    // FIN → `RecvClosed`, then `pump_out` half-closes the send half →
    // `BothClosed`. The next `run_tick` reaps the bridge.
    //
    // Crucially, we do this BEFORE draining `out_transmit` — the label
    // chunk from step (4) is still in the queue. The reap that follows
    // will trigger `purge_transmit_for(server_exchange)` — gated on
    // `is_failed()` so an unconditional purge does not drop the legitimate
    // label chunk queued in step (4); the `is_failed` gate is the mutation
    // gate.
    server.handle_transport_data(server_exchange, request_tail, true, now);

    // (6) One tick at the same `now` so the post-mint pump flushes and the
    // reaper runs. The `BothClosed` reap is where `purge_transmit_for` fires
    // — its `is_failed` gate must preserve the legitimate label chunk.
    server.handle_timeout(now);

    // (7) Drain transmit AFTER the reap. Two chunks must surface in this
    // exchange — the label prefix queued in step (4) AND the response
    // body added by the reap's `collect_bridge_transmits` — so the
    // dialer reassembles a well-formed labeled reply.
    let mut all_chunks: Vec<Bytes> = Vec::new();
    while let Some((id, _peer, bytes)) = server.poll_transport_transmit() {
      if id == server_exchange {
        all_chunks.push(bytes);
      }
    }

    // Load-bearing assertions:
    //
    // (a) The label chunk from step (4) survived the clean reap — the
    //     first chunk must lead with `LABELED_TAG=12`. The mutation gate
    //     (`reap_bridge` purges unconditionally) fails this assertion: the
    //     purge dropped the label chunk, so the first surviving chunk
    //     starts with the response body's leading byte (the push/pull
    //     framing header), NOT the label tag.
    //
    // (b) At least two chunks surfaced — the label and the response are
    //     two separate `out_transmit` entries (label queued in tick (4),
    //     response added by the reap in tick (6)) and the natural drain
    //     loop pops them in queue order. The mutation gate drops the
    //     label, leaving exactly one chunk (the response) and failing
    //     this assertion.
    assert!(
      all_chunks.len() >= 2,
      "the full wire reply spans TWO out_transmit entries (label from \
       tick (4), response from the reap in tick (6)); the unconditional \
       purge mutation drops the label, leaving only the response. got \
       {} chunks: {:?}",
      all_chunks.len(),
      all_chunks.iter().map(Bytes::len).collect::<Vec<_>>(),
    );
    assert_eq!(
      all_chunks[0][0], 12,
      "the first chunk must lead with LABELED_TAG (the acceptor's outbound \
       label prefix); the mutation gate causes this to fail because the \
       purge dropped the label and the first surviving chunk is the \
       push/pull response body's framing byte",
    );
    assert_eq!(
      all_chunks[0][1] as usize,
      "cluster-x".len(),
      "the label chunk's declared length matches the configured label",
    );

    // (8) The clean reap must emit `Close` for the exchange. The teardown
    // gate releases `Close` once `out_transmit` for the exchange has been
    // drained (step (7) drained the queue, so the gate is unblocked).
    let mut actions: Vec<StreamAction> = Vec::new();
    while let Some(a) = server.poll_action() {
      actions.push(a);
    }
    let close_for_exchange = actions
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == server_exchange);
    assert!(
      close_for_exchange,
      "the clean reap must emit Close for the exchange; got {:?}",
      actions.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (9) The bridge was reaped — no stragglers held to the accept
    // deadline.
    assert_eq!(
      server.live_bridge_count(),
      0,
      "the bridge was reaped on the clean exchange completion",
    );
  }

  /// A dialer whose stream intent is retired by the inner endpoint
  /// (deadline elapsed inside `dial_succeeded`, or an explicit external
  /// `dial_failed`) BEFORE the coordinator could mint a `Stream` MUST NOT
  /// leak the eager-queued local cluster label, AND its still-queued
  /// `Connect` action MUST NOT surface to the driver. The
  /// [`StreamEndpoint::service_handshake_completions`] `dial_succeeded`-returns-
  /// `None` branch is the reap site for this scenario: without the four-step
  /// fail-and-purge — bridge `fail_connection_lost` (clears
  /// `records.outbound`), `purge_transmit_for`,
  /// `purge_pending_connect_for`, then drop the bridge with NO
  /// `collect_bridge_transmits` — a driver doing the natural drain loop would
  /// dequeue `Connect` (Connects surface before teardowns), open the
  /// socket, write the label, and then close it: a wrong-cluster peer
  /// observes the local label even though the dial intent was never
  /// supposed to reach it.
  ///
  /// Setup constructs the failed state directly. We invoke the inner
  /// `start_push_pull` so a `DialRequested` event + a `pending_stream_intent`
  /// exist, then drive `service_dials` so the dialer bridge is inserted
  /// (mint = `Some(Outbound)`, `records.outbound` carries `[12][len][label]`,
  /// `Connect` is queued in `pending_connects`). We then call
  /// `ep.dial_failed` to retire the intent — the natural API does not
  /// reach this branch through `start_push_pull`'s in-band flush, which
  /// promotes the dialer bridge in the SAME tick before any intent
  /// retirement is possible, so the direct injection is the only way to
  /// exercise the branch deterministically. A subsequent `flush_outbound`
  /// drives `service_handshake_completions`, which calls
  /// `dial_succeeded(stream_id, now)`, observes `None`, and runs the
  /// fail-and-purge reap.
  ///
  /// Mutation gate: removing the `bridge.fail_connection_lost()` call from
  /// the `dial_succeeded(None)` branch lets the dialer's eager-queued
  /// `[12][len][cluster-x]` survive in `records.outbound` (`clear_outbound`
  /// never runs). Removing the `purge_pending_connect_for(id)` call lets the
  /// queued `Connect` surface, so the natural drain loop opens the socket.
  /// Restoring the `collect_bridge_transmits(id, &mut br)` call
  /// unconditionally routes the just-cleared-or-pre-leak label into
  /// `out_transmit`. Any of those mutations fails the assertions below.
  #[test]
  fn failed_dial_no_label_leak_no_connect_emitted() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Create the dial intent in the inner endpoint. This queues a
    // `DialRequested` event in `coord.ep.pending_events` and inserts the
    // intent into `pending_stream_intents`. We deliberately invoke the
    // inner endpoint directly rather than `coord.start_push_pull` so the
    // coordinator's same-tick in-band flush (which would promote the
    // dialer bridge and clear its `mint`) does not run yet.
    let stream_id = coord
      .endpoint_mut()
      .start_push_pull(addr(7000), PushPullKind::Refresh, now);

    // (2) Sieve the `DialRequested` into `dial_pending`, then drive
    // `service_dials` to insert the dialer bridge with
    // `mint = Some(Outbound(stream_id))` and eager-queue
    // `[12][9]["cluster-x"]` into `records.outbound`. A `Connect` for
    // the exchange lands in `pending_connects`. The bridge is NOT
    // promoted yet — that happens in
    // `service_handshake_completions`, which we drive in step (4).
    coord.sieve_dial_events();
    coord.service_dials(now);

    // Discover the exchange handle of the just-inserted dialer bridge.
    // Exactly one bridge exists at this point.
    let ids = coord.exchange_ids();
    assert_eq!(ids.len(), 1, "service_dials inserted exactly one bridge");
    let exchange = ids[0];

    // (3) Retire the dial intent directly through the inner endpoint.
    // After this, `dial_succeeded(stream_id, now)` returns `None` (the
    // intent is gone), which is the trigger for the `dial_succeeded(None)`
    // branch in `service_handshake_completions`.
    coord.endpoint_mut().dial_failed(
      stream_id,
      crate::error::StreamError::DialFailed("test injection".into()),
      now,
    );

    // (4) Drive the same in-band flush `start_push_pull`'s wrapper would
    // have run: `pump_bridges` → `service_handshake_completions` →
    // `pump_bridges` → `finalize_tick`. The Handshaking bridge's
    // `pump_out` does NOT fail it (the bridge deadline is still in the
    // future), so the bridge survives `pump_bridges` and the SHC step
    // calls `dial_succeeded(stream_id, now)` → `None`, entering the
    // fail-and-purge reap path.
    coord.flush_outbound(now);

    // (5) Natural drain loop: drain actions and transmits until idle.
    // Record everything for assertion.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    loop {
      let mut made_progress = false;
      while let Some(action) = coord.poll_action() {
        actions_observed.push(action);
        made_progress = true;
      }
      while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
        bytes_observed.push((id, bytes));
        made_progress = true;
      }
      if !made_progress {
        break;
      }
    }

    // (6) Load-bearing assertion: NO bytes for the failed exchange
    // surfaced. The leaked chunk a missing fix would emit is the
    // dialer's eager `[12][9]["cluster-x"]` prefix.
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "failed dial must not emit any bytes for the exchange; got {:?}",
      stale,
    );

    // (7) Load-bearing assertion: NO `Connect` for the failed exchange
    // surfaced. The Connect was queued by `service_dials` and must be
    // purged by `purge_pending_connect_for` so the driver does not open
    // a socket for an exchange the coordinator has already failed.
    let connect_for_exchange = actions_observed.iter().any(|a| match a {
      StreamAction::Connect(info) => info.id() == exchange,
      _ => false,
    });
    assert!(
      !connect_for_exchange,
      "failed dial must not emit Connect for the exchange; got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (8) `Abort` for the failed exchange surfaces — the contract is to
    // emit `Abort` (the dial FAILED) so a driver that had already drained
    // `Connect` from a prior tick can RST the open socket and discard any
    // bytes it queued for the exchange. In this test the Connect was purged
    // so a driver's `Abort` is a no-op for an unopened socket, but the action
    // presence is the documented behaviour.
    let abort_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      abort_for_exchange,
      "Abort for the failed exchange must surface; got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (9) The bridge was reaped and the exchange forgotten.
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the bridge was reaped on the dial-failure path",
    );
  }

  #[cfg(feature = "compression-lz4")]
  #[test]
  fn stream_endpoint_gossip_compression_roundtrips() {
    use crate::{CompressAlgorithm, CompressionOptions};
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let opts = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(64);
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::with_compression(ep, cfg, test_sni_provider(), test_peer_to_socket(), opts);
    let datagram = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(8);
    let on_wire = coord.compress_gossip(&datagram);
    assert!(
      on_wire.len() < datagram.len(),
      "compressible datagram must shrink on the wire"
    );
    let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
    assert_eq!(back, datagram);
  }

  #[cfg(feature = "compression-lz4")]
  #[test]
  fn stream_endpoint_over_mtu_compressed_gossip_is_rejected() {
    use crate::{CompressAlgorithm, CompressionOptions};
    let ep = endpoint(7102);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let opts = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(64);
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::with_compression(ep, cfg, test_sni_provider(), test_peer_to_socket(), opts);
    // A wrapper whose orig_len exceeds the gossip MTU cannot be produced by
    // any compliant coordinator. Build one synthetically and assert it is
    // rejected without touching the body (the bomb guard checks orig_len
    // before allocation).
    let over_mtu = coord.gossip_mtu() + 1;
    let frame = crate::encode_compressed_frame(CompressAlgorithm::Lz4, over_mtu, b"x");
    assert!(
      coord.decrypt_gossip(&frame).is_err(),
      "a compressed gossip frame claiming orig_len > gossip_mtu must be rejected"
    );
  }

  #[cfg(feature = "compression-lz4")]
  #[test]
  fn stream_endpoint_compressed_gossip_never_inflates() {
    use crate::{CompressAlgorithm, CompressionOptions};
    let ep = endpoint(7101);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    // Low threshold so the compressor attempts compression for all sizes in
    // the sweep, exercising the don't-expand else branch for sizes where the
    // wrapper header overhead erases the raw saving.
    let opts = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(1);
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::with_compression(ep, cfg, test_sni_provider(), test_peer_to_socket(), opts);
    for len in 1..=1500 {
      // Mostly-varying pattern with a short repeated motif: the backend's
      // saving is small and varies with `len`, ensuring the don't-expand
      // else branch is exercised for some sizes while still round-tripping
      // for all.
      let input: Vec<u8> = (0..len)
        .map(|i| {
          let base = (i % 7) as u8;
          base ^ ((i / 7) as u8).wrapping_mul(3)
        })
        .collect();
      let compressed = coord.compress_gossip(&input);
      assert!(
        compressed.len() <= input.len(),
        "compress_gossip inflated datagram at len={len}: {} > {}",
        compressed.len(),
        input.len(),
      );
      let back = coord.decrypt_gossip(&compressed).expect("decrypt failed");
      assert_eq!(
        back, input,
        "compress_gossip round-trip failed at len={len}",
      );
    }
  }

  /// `EndpointOptions::with_gossip_mtu` propagates all the way through to the
  /// composed `StreamEndpoint` coordinator: `coord.gossip_mtu()` returns the
  /// configured value, NOT the legacy 1400 constant.
  #[test]
  fn stream_endpoint_gossip_mtu_is_propagated_from_config() {
    let cfg_ep = EndpointOptions::new(SmolStr::new("local"), addr(7240)).with_gossip_mtu(1200);
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_ep);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());
    assert_eq!(
      coord.gossip_mtu(),
      1200,
      "StreamEndpoint::gossip_mtu must read the configured value (not the \
       legacy 1400 constant) so the FSM's plaintext-budget bound tracks \
       EndpointOptions",
    );
  }

  /// Wire-byte guarantee for encrypted gossip under a configured `gossip_mtu`:
  /// a near-budget plaintext frame's on-wire ciphertext stays within
  /// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`. This documents the explicit
  /// semantic that the configured `gossip_mtu` bounds the FSM's plaintext
  /// budget; the on-wire datagram MAY exceed it by exactly the AEAD wrapper
  /// overhead (30 bytes = 14-byte header + 16-byte auth tag) when
  /// encryption is enabled. Operators on tight path-MTU networks size
  /// `gossip_mtu` accordingly.
  ///
  /// Mutation gate: if a future change forgets to size buffers for the
  /// wrapper-overhead slack (or, conversely, lets the configured budget be
  /// silently overshot by more than the wrapper), this test catches it.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn encrypted_gossip_wire_bytes_within_configured_mtu_plus_overhead() {
    use crate::{ENCRYPTED_WRAPPER_OVERHEAD, EncryptionOptions, Keyring, SecretKey};
    let cfg_ep = EndpointOptions::new(SmolStr::new("local"), addr(7241)).with_gossip_mtu(1200);
    let ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_ep);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
    // A plaintext datagram at the configured gossip-MTU ceiling.
    let plaintext = vec![0xab; 1200];
    let on_wire = coord
      .encrypt_gossip(&plaintext)
      .expect("aes-gcm primary -> encrypt succeeds");
    assert!(
      on_wire.len() <= 1200 + ENCRYPTED_WRAPPER_OVERHEAD,
      "encrypted on-wire datagram MUST stay within \
       gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD; \
       got on_wire={} > 1200 + {} = {}",
      on_wire.len(),
      ENCRYPTED_WRAPPER_OVERHEAD,
      1200 + ENCRYPTED_WRAPPER_OVERHEAD,
    );
    assert_eq!(
      on_wire.len(),
      1200 + ENCRYPTED_WRAPPER_OVERHEAD,
      "AES-GCM is a streaming AEAD: a 1200-byte plaintext inflates by \
       EXACTLY ENCRYPTED_WRAPPER_OVERHEAD (30 bytes) — header + 12-byte \
       nonce + 16-byte auth tag",
    );
    let back = coord.decrypt_gossip(&on_wire).expect("roundtrip decrypt");
    assert_eq!(back, plaintext, "roundtrip preserves the plaintext bytes");
  }

  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn stream_endpoint_gossip_encryption_roundtrip() {
    use crate::{EncryptionOptions, Keyring, SecretKey};
    let ep = endpoint(7200);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let kr = Keyring::new(SecretKey::Aes256([0x42; 32]));
    let opts = EncryptionOptions::new().with_keyring(kr);
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
    let datagram = b"a gossip body".to_vec();
    let on_wire = coord.encrypt_gossip(&datagram).expect("encrypt");
    assert_ne!(on_wire, datagram, "encrypted bytes differ from plaintext");
    assert_eq!(
      on_wire[0],
      crate::ENCRYPTED_TAG,
      "outbound starts with the Encrypted wrapper tag"
    );
    let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
    assert_eq!(back, datagram);
  }

  #[test]
  fn stream_endpoint_gossip_encryption_disabled_is_byte_identical() {
    let ep = endpoint(7201);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());
    let datagram = b"a gossip body".to_vec();
    let on_wire = coord.encrypt_gossip(&datagram).expect("identity");
    assert_eq!(on_wire, datagram, "no keyring -> identity transform");
    let back = coord.decrypt_gossip(&on_wire).expect("identity");
    assert_eq!(back, datagram);
  }

  /// Strict-mode rejection MUST fire at the coordinator's public ingress
  /// API. A configured keyring + a leading tag that is not `Encrypted` is
  /// an unauthenticated plaintext Ping/Ack/Alive frame; passing it through
  /// to `handle_gossip` would bypass strict-mode entirely. The
  /// coordinator's single canonical ingress helper `decrypt_gossip` routes
  /// through `unwrap_transforms_with_encryption`, which applies the
  /// strict-mode entry check before any wrapper decoding, so cluster
  /// authentication holds without depending on driver discipline.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn decrypt_gossip_rejects_plaintext_when_encryption_enabled() {
    use crate::{
      EncryptionOptions, FrameError, Keyring, MessageTag, SecretKey, encode_plain_frame,
    };
    let ep = endpoint(7205);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
    // A plain Ping frame — its leading byte is `MessageTag::Ping`, not
    // `Encrypted`, so the strict-mode entry check inside
    // `unwrap_transforms_with_encryption` MUST reject before any decoding.
    // Body shape does not matter for the leading-byte check.
    let plain_ping = encode_plain_frame(MessageTag::Ping, b"opaque-body").expect("encode");
    let result = coord.decrypt_gossip(&plain_ping);
    assert!(
      matches!(result, Err(FrameError::Encryption(_))),
      "decrypt_gossip MUST reject a plaintext datagram while encryption \
       is enabled — got {result:?}",
    );
  }

  #[cfg(all(
    feature = "encryption-aes-gcm",
    not(feature = "encryption-chacha20-poly1305")
  ))]
  #[test]
  fn stream_endpoint_encrypt_gossip_returns_err_on_unsupported_backend() {
    // A keyring whose primary requires a backend the binary was not built
    // with (here: ChaCha20-Poly1305 under an aes-gcm-only build) must
    // surface as Err, NOT silently emit plaintext.
    use crate::{EncryptionError, EncryptionOptions, Keyring, SecretKey};
    let ep = endpoint(7202);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let kr = Keyring::new(SecretKey::ChaCha20Poly1305([0x42; 32]));
    let opts = EncryptionOptions::new().with_keyring(kr);
    let coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts);
    let datagram = b"this gossip must not go out plaintext".to_vec();
    let err = coord
      .encrypt_gossip(&datagram)
      .expect_err("missing backend must surface as Err, not silent plaintext");
    assert!(
      matches!(err, EncryptionError::UnsupportedAlgorithm(_)),
      "got {err:?}"
    );
  }

  #[test]
  fn raw_records_is_secure_returns_false() {
    use crate::streams::StreamTransport;
    assert!(
      !RawRecords::is_secure(),
      "plain TCP is not transport-encrypted"
    );
  }

  /// A runtime [`StreamEndpoint::set_encryption_options`] update MUST propagate
  /// to every live reliable bridge, not just `self.encryption`. Without the
  /// fan-out, a peer holding a pre-update reliable exchange continues to send
  /// plaintext units on that bridge — the bridge holds a clone of the prior
  /// (disabled) `EncryptionOptions`, so its strict-mode entry check in
  /// `unwrap_transforms_with_encryption` never fires and the
  /// unauthenticated-plaintext-ingress gap reopens.
  ///
  /// The test opens an outbound exchange with encryption DISABLED (so the
  /// bridge is constructed under the disabled policy), then calls
  /// `set_encryption_options(enabled)` and asserts the live bridge's
  /// effective encryption is now ENABLED.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_propagates_to_live_bridges() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7203);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // Drive an outbound dial so the coordinator builds a bridge UNDER the
    // disabled-encryption policy (the default constructor leaves
    // `self.encryption` disabled, so `start_push_pull` → `service_dials` clones
    // a disabled options into the new bridge).
    coord.start_push_pull(addr(7000), PushPullKind::Refresh, now);
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };

    assert_eq!(
      coord.bridge_encryption_enabled(exchange),
      Some(false),
      "before set_encryption_options the bridge clone is disabled",
    );

    // Publish an enabled policy at runtime — every live bridge MUST observe it.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    assert_eq!(
      coord.bridge_encryption_enabled(exchange),
      Some(true),
      "set_encryption_options must fan out to every live bridge — without \
       propagation a peer's pre-update reliable exchange would keep accepting \
       plaintext on the bridge's stale (disabled) EncryptionOptions clone",
    );
    // And the endpoint's own field is also updated, so any subsequent bridge
    // built from the coordinator sees the new policy.
    assert!(
      coord.encryption_options().is_enabled(),
      "the endpoint's stored EncryptionOptions reflects the runtime update",
    );
  }

  /// The builder variant [`StreamEndpoint::with_encryption`] MUST route
  /// through [`StreamEndpoint::set_encryption_options`] so the bridge-fan-out
  /// is shared. A naive `self.encryption = encryption; self` would leave every
  /// live bridge holding its prior (disabled) `EncryptionOptions` clone — the
  /// same unauthenticated-plaintext-ingress gap the in-place setter closes.
  ///
  /// Mirror of `set_encryption_options_propagates_to_live_bridges` but uses
  /// `coord = coord.with_encryption(opts)` to exercise the builder path.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn with_encryption_builder_propagates_to_live_bridges() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7204);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    coord.start_push_pull(addr(7001), PushPullKind::Refresh, now);
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };

    assert_eq!(
      coord.bridge_encryption_enabled(exchange),
      Some(false),
      "before with_encryption the bridge clone is disabled",
    );

    // Builder variant: rebuild the coordinator value with the enabled policy.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord = coord.with_encryption(opts);

    assert_eq!(
      coord.bridge_encryption_enabled(exchange),
      Some(true),
      "with_encryption must fan out to every live bridge — the builder \
       variant must share set_encryption_options' propagation, otherwise a \
       pre-update reliable exchange would keep accepting plaintext on a \
       stale (disabled) EncryptionOptions clone",
    );
    assert!(
      coord.encryption_options().is_enabled(),
      "the endpoint's stored EncryptionOptions reflects the builder update",
    );
  }

  /// On an INSECURE transport (`RawRecords::is_secure() == false`) a runtime
  /// [`StreamEndpoint::set_encryption_options`] update MUST drop any queued
  /// bytes the bridge encoded under the prior policy: those bytes are
  /// plaintext, and emitting them on the wire after the operator publishes
  /// a new policy would leak plaintext post-enablement (the SWIM push/pull
  /// request may carry user-defined node metadata). The fix fails the
  /// bridge, which clears the records-layer outbound buffer; the
  /// coordinator's post-iteration purge drops the already-drained chunks
  /// from [`StreamEndpoint::out_transmit`]; the FSM retries the exchange
  /// under a fresh bridge built under the new policy.
  ///
  /// Drives the queued-plaintext leak scenario:
  /// (1) bridge built under disabled encryption;
  /// (2) `start_push_pull` encodes the request as `[unit_len][plaintext]`
  ///     and queues it via `records.write_plaintext`, which `pump_bridges`
  ///     drains into `out_transmit`;
  /// (3) operator publishes an enabled policy;
  /// (4) natural drain loop MUST observe NO bytes for the affected
  ///     exchange, AND the bridge MUST be in `BridgePhase::Failed`.
  ///
  /// Mutation gate: if `StreamBridge::set_encryption` does NOT `fail` the
  /// bridge on insecure transport, the bridge keeps running and lets the
  /// pre-update plaintext surface from `poll_transport_transmit` after
  /// `set_encryption_options` — the failure transition + outbound purge is
  /// load-bearing for the policy-change contract.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_drops_queued_plaintext_when_enabling_on_raw_records() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7205);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Open a push/pull exchange under disabled encryption. The in-band
    // `service_dials` + `flush_outbound` builds the dialer bridge, promotes
    // it (the dialer's `RawRecords::is_handshaking()` is `false` from
    // construction), runs `pump_out` which encodes the request as a reliable
    // unit and `write_plaintext`s it into the records layer, then
    // `collect_transmits` drains it into `out_transmit`.
    coord.start_push_pull(addr(7002), PushPullKind::Refresh, now);

    // (2) Drain the `Connect` so the natural loop below sees only the
    // post-update ordering of transmit vs. teardown. The exchange is still
    // alive at this point — the pre-update plaintext chunk for the same
    // exchange is in `out_transmit`.
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };
    assert!(
      coord.exchange_has_pending_bytes(exchange),
      "pre-update invariant: the dialer queued plaintext bytes for the \
       exchange (these are the bytes that would leak post-enablement)",
    );
    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(false),
      "pre-update invariant: the bridge is still running",
    );

    // (3) Publish an enabled policy. On an insecure transport this must
    // fail the bridge AND purge `out_transmit`.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(true),
      "an insecure-transport bridge MUST fail on a runtime encryption-policy \
       change so the SWIM FSM retries the exchange under a fresh bridge \
       constructed under the new policy",
    );
    assert!(
      !coord.exchange_has_pending_bytes(exchange),
      "the coordinator MUST purge any already-drained plaintext chunks tagged \
       with the failed bridge — without the purge, a driver polling \
       `poll_transport_transmit` before the next pump would emit pre-update \
       plaintext on the wire post-enablement",
    );

    // (4) Natural drain loop. With the bridge failed, `out_transmit` purged,
    // and an `Abort` synchronously enqueued by `set_encryption_options`, no
    // bytes for the exchange surface and the `Abort` is reachable from
    // `poll_action` WITHOUT an external `handle_timeout` kick. A terminal
    // bridge returns no per-bridge timeout, and an idle endpoint may have
    // no scheduler timeout at all — relying on the natural `pump_bridges`
    // reap to surface the `Abort` would leave it unreachable through the
    // documented driver interface.
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    while let Some(action) = coord.poll_action() {
      actions_observed.push(action);
    }
    while let Some((id, _peer, bytes)) = coord.poll_transport_transmit() {
      bytes_observed.push((id, bytes));
    }
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "no bytes for the failed exchange {:?} may reach the wire after \
       set_encryption_options publishes the new policy; got {:?}",
      exchange,
      stale,
    );
    let abort_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      abort_for_exchange,
      "the failed bridge MUST surface an Abort action so the driver tears down \
       the affected transport connection and discards the stale plaintext; \
       got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// The builder variant [`StreamEndpoint::with_encryption`] routes through
  /// [`StreamEndpoint::set_encryption_options`], so the queued-plaintext-drop
  /// behavior MUST apply to the builder path as well. Mirrors
  /// [`set_encryption_options_drops_queued_plaintext_when_enabling_on_raw_records`]
  /// but uses `coord = coord.with_encryption(opts)`.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn with_encryption_drops_queued_plaintext_when_enabling_on_raw_records() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7206);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    coord.start_push_pull(addr(7003), PushPullKind::Refresh, now);
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };
    assert!(
      coord.exchange_has_pending_bytes(exchange),
      "pre-update invariant: the dialer queued plaintext bytes for the exchange",
    );

    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord = coord.with_encryption(opts);

    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(true),
      "with_encryption (builder) must share set_encryption_options' \
       insecure-transport failure path — otherwise the same plaintext-leak \
       gap reopens on the builder",
    );
    assert!(
      !coord.exchange_has_pending_bytes(exchange),
      "with_encryption (builder) must share set_encryption_options' \
       transmit-queue purge",
    );

    // The builder must also share `set_encryption_options`' synchronous
    // `Abort` enqueue: the teardown surfaces from `poll_action` WITHOUT an
    // external `handle_timeout` kick. A terminal bridge returns no
    // per-bridge timeout, so an `Abort` that only fires on the next reap is
    // unreachable from the documented driver interface.
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    while let Some(action) = coord.poll_action() {
      actions_observed.push(action);
    }
    let abort_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      abort_for_exchange,
      "with_encryption (builder) must surface an Abort action for the failed \
       bridge directly from poll_action; got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// A `start_push_pull` enqueues a `Connect` action. If the driver has not
  /// yet drained that `Connect` when the operator publishes a new encryption
  /// policy on an insecure transport, the bridge fails and its queued
  /// `Connect` MUST be purged — otherwise a driver doing the natural
  /// "drain actions, drain transmits, repeat" loop would open a transport
  /// socket for a bridge the coordinator has already failed, then drain
  /// the same exchange's `Abort` and tear it back down (wasted work, and
  /// the eager-queued local label sitting in the bridge's record-layer
  /// outbound buffer was already cleared by the failure transition — but a
  /// driver should not even attempt the dial).
  ///
  /// Mirror invariant of [`set_encryption_options_drops_queued_plaintext_when_enabling_on_raw_records`]
  /// in the action-queue dimension: that test asserts no plaintext BYTES
  /// surface after the policy change; this one asserts no stale `Connect`
  /// ACTION surfaces.
  ///
  /// Mutation gate: removing `purge_pending_connect_for(id)` from
  /// `set_encryption_options` makes the natural drain loop observe the
  /// stale `Connect` before the `Abort`.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_purges_pending_connect_for_failed_bridge() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7207);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) `start_push_pull` enqueues a `Connect` for the dial. Do NOT drain
    // it — the regression scenario is precisely that the driver had not yet
    // observed `Connect` when the operator published the new policy.
    coord.start_push_pull(addr(7004), PushPullKind::Refresh, now);
    assert_eq!(
      coord.live_bridge_count(),
      1,
      "the dial inserts exactly one bridge whose `Connect` is queued and \
       MUST be purged by step (2)",
    );

    // (2) Publish the enabling policy. The bridge fails and its queued
    // `Connect` MUST be purged.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    // (3) Drain `poll_action` and assert: NO `Connect` is observed (the
    // purge worked), and an `Abort` IS observed (the synchronous enqueue
    // worked, so the driver can tear down any state it accrued for the
    // exchange and discard its stale bytes — the bridge FAILED).
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    while let Some(action) = coord.poll_action() {
      actions_observed.push(action);
    }
    let stale_connect = actions_observed
      .iter()
      .any(|a| matches!(a, StreamAction::Connect(_)));
    assert!(
      !stale_connect,
      "a `Connect` for the now-failed bridge MUST be purged from the action \
       queue so a driver doing the natural drain loop does not open a \
       transport socket for an exchange the coordinator has already failed; \
       got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
    let abort_observed = actions_observed
      .iter()
      .any(|a| matches!(a, StreamAction::Abort(_)));
    assert!(
      abort_observed,
      "the failed bridge MUST surface an `Abort` so the driver can clean up \
       any state it accrued for the exchange and discard its stale buffered \
       bytes (a driver that had already drained `Connect` from a prior tick \
       would otherwise leak the open socket / flush stale bytes); got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// A reapply of the SAME effective `EncryptionOptions` must be a no-op:
  /// no live bridge should be failed, no queued bytes should be purged, no
  /// `Abort` should be enqueued. A config reconciler that republishes the
  /// current configuration on a timer is a normal operational pattern; the
  /// pre-guard implementation would tear down every reliable exchange on
  /// every reapply for no security gain (the bridge clone already runs
  /// under these exact options).
  ///
  /// The test opens an exchange under an enabled policy, then republishes
  /// the same options. The bridge MUST remain alive AND its queued bytes
  /// MUST stay in `out_transmit`.
  ///
  /// Mutation gate: removing the entry-equality short-circuit from
  /// `set_encryption_options` makes the insecure-transport fail-cascade run
  /// on the reapply — the bridge transitions to `Failed`, the queued
  /// plaintext is purged, and an `Abort` is enqueued (the same outputs the
  /// real policy-change tests assert when transitioning DISABLED → ENABLED).
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_is_noop_when_reapplying_same_policy() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7208);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let key = SecretKey::Aes256([0x42; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket())
        .with_encryption(opts.clone());

    // Open a push/pull exchange under the enabled policy; the bridge holds a
    // clone of `opts`. The dial encodes the request as an encrypted unit and
    // drains it into `out_transmit`.
    coord.start_push_pull(addr(7005), PushPullKind::Refresh, now);
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };
    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(false),
      "pre-reapply invariant: the bridge is running under the enabled policy",
    );
    let bytes_pre = coord.exchange_has_pending_bytes(exchange);
    assert!(
      bytes_pre,
      "pre-reapply invariant: the dialer queued bytes for the exchange",
    );

    // Reapply the IDENTICAL options. The no-op guard MUST skip the
    // fail-cascade: bridge stays alive, queued bytes stay queued, no `Abort`
    // is enqueued.
    coord.set_encryption_options(opts.clone());

    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(false),
      "a no-op reapply of the same EncryptionOptions MUST NOT fail the live \
       bridge — the bridge clone already runs under these exact options, so \
       re-failing it is pure availability loss",
    );
    assert!(
      coord.exchange_has_pending_bytes(exchange),
      "a no-op reapply MUST NOT purge the bridge's queued bytes (the \
       purge runs only after a real policy-change failure)",
    );

    // Drain the action queue and assert NO `Abort` for the exchange surfaced —
    // the only action that survives is the original `Connect` (already
    // drained above) and possibly its companions; specifically no `Abort`
    // for this exchange should be present.
    let mut actions_observed: Vec<StreamAction> = Vec::new();
    while let Some(action) = coord.poll_action() {
      actions_observed.push(action);
    }
    let abort_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      !abort_for_exchange,
      "a no-op reapply MUST NOT enqueue an Abort — the bridge is still alive; \
       got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
    // And the endpoint's stored options are still the same enabled policy.
    assert_eq!(
      coord.encryption_options(),
      &opts,
      "the no-op reapply still publishes the (identical) options as the \
       endpoint's current configuration",
    );
  }

  /// A runtime policy change that fails one or more bridges MUST make the
  /// failed bridges reachable to the next reap WITHOUT requiring an
  /// unrelated external event to advance time. A terminal bridge contributes
  /// no per-bridge timeout of its own, and an idle endpoint may have no
  /// scheduler timeout at all — without an immediate wake the bridge would
  /// sit in `conns` until some unrelated event triggered a tick.
  ///
  /// `set_encryption_options` sets the `policy_reap_pending` latch on a
  /// real failure; `poll_timeout` folds `last_now` into the returned `min`
  /// so the driver wakes immediately. The driver's `handle_timeout`
  /// reaches the natural `pump_bridges` reap, removes the failed bridge
  /// from `conns`, and clears the latch.
  ///
  /// The test opens an exchange under disabled encryption, enables
  /// encryption (which fails the insecure-transport bridge), and asserts:
  /// (a) `poll_timeout` returns an immediate-due wake even though no other
  ///     timer is scheduled;
  /// (b) one `handle_timeout(now)` later, the bridge is gone from `conns`.
  ///
  /// Mutation gate: removing the `policy_reap_pending` fold from
  /// `poll_timeout` makes the wake unreachable (`poll_timeout` returns
  /// `None` between the policy change and the bridge's exchange deadline),
  /// so the bridge would linger until the deadline elapses — a user-visible
  /// stall on the driver-observable timer.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_wakes_immediately_to_reap_failed_bridge() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7209);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Open an exchange under disabled encryption. The in-band
    // `start_push_pull` calls `service_dials` + `flush_outbound`, sets
    // `last_now`, and creates the bridge.
    coord.start_push_pull(addr(7006), PushPullKind::Refresh, now);
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };
    assert_eq!(
      coord.live_bridge_count(),
      1,
      "pre-update invariant: exactly one bridge is live",
    );

    // (2) Publish an enabling policy. The insecure-transport bridge fails;
    // `set_encryption_options` purges its transmit + connect queues and
    // enqueues an `Abort`. It also sets the `policy_reap_pending` latch so
    // `poll_timeout` returns an immediate-due wake.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    // Drain the synchronous `Abort` (already reachable from `poll_action`).
    let mut actions: Vec<StreamAction> = Vec::new();
    while let Some(a) = coord.poll_action() {
      actions.push(a);
    }
    let abort_observed = actions
      .iter()
      .filter_map(|a| a.as_abort())
      .any(|r| r.id() == exchange);
    assert!(
      abort_observed,
      "the failed bridge surfaces an Abort directly from poll_action \
       (synchronous enqueue); got {:?}",
      actions.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (3) The failed bridge is still in `conns` — the reap runs in the next
    // `pump_bridges`. Assert that `poll_timeout` returns an immediate-due
    // wake (`<= now`) so a driver doing the natural "wait until
    // poll_timeout" loop sees the wake at once.
    let wake = coord
      .poll_timeout()
      .expect("the policy-change latch makes poll_timeout return Some");
    assert!(
      wake <= now,
      "set_encryption_options sets policy_reap_pending so the wake is \
       immediate-due (<= last_now); got wake={:?} vs last_now={:?}",
      wake,
      now,
    );

    // (4) Advance the clock with `handle_timeout(now)`. The pump_bridges
    // call inside `run_tick` reaps the terminal (Failed) bridge, removes it
    // from `conns`, and the run_tick epilogue clears the latch.
    coord.handle_timeout(now);
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "handle_timeout's pump_bridges MUST reap the policy-failed bridge; \
       leaving it in conns would defeat the immediate-reap contract",
    );
    // The latch must clear so subsequent `poll_timeout` calls do not loop
    // forever returning an immediate wake.
    let post_wake = coord.poll_timeout();
    if let Some(t) = post_wake {
      assert!(
        t > now,
        "after the reap, the policy_reap_pending latch is cleared and \
         poll_timeout no longer returns an immediate wake; got {:?} <= now",
        t,
      );
    }
  }

  /// Mirror of `set_encryption_options_wakes_immediately_to_reap_failed_bridge`
  /// for the inbound-accept-first ordering: a brand-new endpoint whose VERY
  /// FIRST operation is [`StreamEndpoint::accept_connection`] (e.g. a server
  /// that accepts an inbound TCP connection before it has ever started its own
  /// probe / push-pull / gossip cycle) must still surface the policy-reap
  /// wake when [`StreamEndpoint::set_encryption_options`] subsequently fails
  /// the freshly-inserted server bridge.
  ///
  /// The `policy_reap_pending` latch's wake is folded into `poll_timeout` ONLY
  /// when `last_now` is `Some(_)` (the known-past anchor). If
  /// `accept_connection` does NOT anchor `last_now`, the latch is set but the
  /// fold finds `last_now == None` and the wake never reaches the driver — the
  /// failed bridge would linger in `conns` until its accept-handshake deadline
  /// elapsed.
  ///
  /// Mutation gate: removing `self.last_now = Some(now)` from
  /// `accept_connection` makes `poll_timeout` return `None` here (the failed
  /// bridge is in `Failed`, so its own `poll_timeout` returns `None`; the
  /// inner endpoint has no timers; `last_now == None` voids the policy latch
  /// fold). The driver would never wake to reap the bridge.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_immediate_reap_works_after_inbound_accept_as_first_op() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7900);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) FIRST operation on a brand-new endpoint: accept an inbound
    // connection. This inserts a server-side bridge into `conns` and anchors
    // `last_now = Some(now)` (the known-past anchor the policy-reap fold in
    // `poll_timeout` requires).
    let exchange = coord.accept_connection(addr(7800), now);
    assert_eq!(
      coord.live_bridge_count(),
      1,
      "accept_connection installs a server-side bridge",
    );

    // (2) Publish an enabling policy. The insecure-transport bridge fails;
    // `set_encryption_options` purges its transmit + connect queues and
    // enqueues an `Abort`. It also sets the `policy_reap_pending` latch so
    // `poll_timeout` returns an immediate-due wake.
    coord.set_encryption_options(
      EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32]))),
    );
    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(true),
      "the inbound bridge fails on the enabling policy update",
    );

    // (3) The failed bridge is still in `conns` — the reap runs in the next
    // `pump_bridges`. Assert that `poll_timeout` returns an immediate-due
    // wake (`<= now`) so a driver doing the natural "wait until
    // poll_timeout" loop sees the wake at once.
    //
    // Without the `accept_connection` anchor, `last_now == None` and the
    // policy-latch fold is a no-op; the only remaining `poll_timeout`
    // candidate is the bridge's own timer — but the bridge is `Failed`, so
    // its `poll_timeout` returns `None`. `poll_timeout` would return `None`.
    let wake = coord
      .poll_timeout()
      .expect("policy_reap_pending must surface as an immediate-due timeout");
    assert!(
      wake <= now,
      "the wake must be immediate-due (<= last_now); got wake={:?} vs now={:?}",
      wake,
      now,
    );

    // (4) `handle_timeout(now)` reaches `pump_bridges`, reaps the terminal
    // bridge, removes it from `conns`, and clears the latch.
    coord.handle_timeout(now);
    assert_eq!(
      coord.bridge_is_failed(exchange),
      None,
      "the failed bridge must be reaped out of conns after handle_timeout",
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "no bridges remain after the policy-reap",
    );
  }

  /// A bridge that cleanly reaped (left `conns`) can leave its final response
  /// chunk sitting in [`StreamEndpoint::out_transmit`] tagged with the now-gone
  /// exchange — the clean-reap path deliberately does NOT
  /// [`StreamEndpoint::purge_transmit_for`] (see
  /// `clean_acceptor_split_inbound_label_then_request_preserves_full_response_on_wire`)
  /// because the label + response chunks split across two queue entries are
  /// both legitimate wire bytes the driver still owes the peer. If the
  /// operator then publishes an enabling encryption policy BEFORE the driver
  /// drains those bytes, the per-bridge fail-and-purge loop in
  /// [`StreamEndpoint::set_encryption_options`] cannot reach the orphaned
  /// chunks (the bridge is not in `conns` to be iterated) and the driver's
  /// next [`StreamEndpoint::poll_transport_transmit`] emits PLAINTEXT bytes
  /// on the wire under the new policy.
  ///
  /// The fix drops the entire `out_transmit` queue when the policy change
  /// fails at least one bridge — `any_failed` is only true on an insecure
  /// transport (`R::is_secure() == false`), so the heavy-handed clear is
  /// bounded to the path where the orphan can leak.
  ///
  /// Setup mirrors `clean_acceptor_split_inbound_label_then_request_preserves_full_response_on_wire`:
  /// drive a real dialer/acceptor exchange to clean reap on the SERVER, then
  /// observe that both the acceptor's label chunk (queued in an earlier tick)
  /// and the response chunk (queued by the reap) are sitting in the server's
  /// `out_transmit` AFTER the bridge has left `conns`. A subsequent
  /// `set_encryption_options(enabled)` MUST purge those orphaned chunks.
  ///
  /// Mutation gate: removing `self.out_transmit.clear()` from the
  /// `set_encryption_options` policy-change branch fails the final assertion
  /// — the orphaned bytes survive and a driver doing the natural drain loop
  /// would emit pre-update plaintext after the new policy publishes.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_purges_orphaned_transmit_from_reaped_bridges() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let server_addr = addr(7600);
    let dialer_addr = addr(7601);

    // (1) Build the dialer coordinator and produce a real `[label||request]`
    // blob with `start_push_pull`.
    let dialer_ep = endpoint(dialer_addr.port());
    let cfg_d = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut dialer: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(dialer_ep, cfg_d, test_sni_provider(), test_peer_to_socket());
    let _sid = dialer.start_push_pull(server_addr, PushPullKind::Join, now);
    // Drain the `Connect` so the byte-collection loop below sees only the
    // wire payload. The `expect(...)` asserts the action's existence; the
    // binding is discarded.
    let _ = dialer
      .poll_action()
      .expect("dialer's first poll_action is the Connect");
    let mut dialer_bytes = Vec::new();
    while let Some((_id, _peer, bytes)) = dialer.poll_transport_transmit() {
      dialer_bytes.extend_from_slice(&bytes);
    }
    assert!(
      dialer_bytes.len() > 11,
      "dialer produced label + request, got {} bytes",
      dialer_bytes.len(),
    );
    let label_only = &dialer_bytes[..11];
    let request_tail = &dialer_bytes[11..];

    // (2) Build the server coordinator under DISABLED encryption (the policy
    // we will later flip to enabled) and accept the connection.
    let server_ep = endpoint(server_addr.port());
    let cfg_s = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut server: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(server_ep, cfg_s, test_sni_provider(), test_peer_to_socket());
    let server_exchange = server.accept_connection(dialer_addr, now);

    // (3) Split-read the dialer's label, tick to drain the acceptor's lazy
    // label into `out_transmit`, then feed the request tail with FIN. After
    // the next tick the bridge cleanly reaps (`BothClosed`) and the response
    // is added by the reap path — the server's `out_transmit` now holds the
    // acceptor's label chunk (queued in tick (4) below) AND the response
    // chunk (queued by the reap), tagged with `server_exchange` even though
    // the bridge itself is gone from `conns`.
    server.handle_transport_data(server_exchange, label_only, false, now);
    server.handle_timeout(now);
    server.handle_transport_data(server_exchange, request_tail, true, now);
    server.handle_timeout(now);

    assert_eq!(
      server.bridge_is_failed(server_exchange),
      None,
      "the bridge cleanly reaped — it is no longer in `conns` (the \
       precondition for the orphaned-out_transmit gap this test exercises)",
    );
    assert!(
      server.exchange_has_pending_bytes(server_exchange),
      "the orphan precondition: the reaped bridge's response chunk is \
       sitting in `out_transmit` tagged with the gone exchange. Without this \
       the test exercises nothing.",
    );

    // (4) Publish an enabling encryption policy. The per-bridge fail-and-purge
    // loop iterates `self.conns`, which the reaped bridge is no longer in;
    // without the orphan-clear branch the chunks remain queued.
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    server.set_encryption_options(opts);

    // (5) The orphaned bytes MUST be gone. The natural drain loop must NOT
    // surface any bytes for `server_exchange`.
    assert!(
      !server.exchange_has_pending_bytes(server_exchange),
      "set_encryption_options on an insecure transport MUST purge orphaned \
       `out_transmit` chunks from reaped bridges — without the clear, a \
       driver polling `poll_transport_transmit` after the operator publishes \
       the new policy would emit pre-update plaintext bytes belonging to \
       an exchange whose bridge is already gone from `conns`",
    );
    let mut bytes_observed: Vec<(ExchangeId, Bytes)> = Vec::new();
    while let Some((id, _peer, bytes)) = server.poll_transport_transmit() {
      bytes_observed.push((id, bytes));
    }
    let stale: Vec<&Bytes> = bytes_observed
      .iter()
      .filter_map(|(eid, b)| (*eid == server_exchange).then_some(b))
      .collect();
    assert!(
      stale.is_empty(),
      "no bytes for the reaped exchange {:?} may reach the wire after the \
       new policy publishes; got {:?}",
      server_exchange,
      stale,
    );
  }

  /// Inbound gossip is buffered raw into [`StreamEndpoint::mem_ingress`] by
  /// [`StreamEndpoint::handle_gossip`] and decrypted at drain time by
  /// [`StreamEndpoint::decrypt_gossip`], which reads the coordinator's CURRENT
  /// `self.encryption`. A datagram queued under one policy is therefore
  /// decrypted under whatever policy is in effect when the driver calls
  /// [`StreamEndpoint::poll_memberlist_ingress`] + `decrypt_gossip`. Without
  /// the policy-change purge, the asymmetry breaks both directions:
  ///
  /// * A plaintext datagram queued while strict-mode was ON (the keyring
  ///   would reject it at drain) is ACCEPTED if the operator disables
  ///   encryption before drain.
  /// * A ciphertext datagram queued while disabled is REJECTED if the operator
  ///   enables strict-mode before drain.
  ///
  /// Gossip is lossy and self-healing, so dropping the buffered datagrams on
  /// every effective policy change is correct — the next gossip round delivers
  /// fresh datagrams under the new policy.
  ///
  /// Test angle: queue a plaintext datagram under disabled encryption, flip
  /// to enabled, and assert that [`StreamEndpoint::poll_memberlist_ingress`]
  /// returns `None`. Without the `mem_ingress.clear()` the queued plaintext
  /// would survive into the strict-mode regime and `decrypt_gossip` on the
  /// drained bytes would reject them — already a defect because the codec
  /// layer would surface a spurious decrypt error for a datagram that was
  /// validly accepted on its arrival policy.
  ///
  /// Mutation gate: removing `self.mem_ingress.clear()` from
  /// `set_encryption_options` makes `poll_memberlist_ingress` return the
  /// pre-policy-change buffered datagram.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn set_encryption_options_purges_buffered_gossip_on_policy_change() {
    use crate::{EncryptionOptions, Keyring, SecretKey};

    let now = Instant::now();
    let ep = endpoint(7700);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Queue a plaintext gossip datagram under disabled encryption. The
    // bytes are buffered raw in `mem_ingress`; the coordinator does NOT
    // decode them in-crate.
    let peer = addr(7701);
    let datagram = [1u8, 2, 3, 4, 5];
    coord.handle_gossip(peer, &datagram, now);
    assert!(
      coord.poll_memberlist_ingress().is_some(),
      "pre-condition: a queued gossip datagram is observable through \
       poll_memberlist_ingress before the policy change",
    );
    // Re-queue (the assertion above drained the queue).
    coord.handle_gossip(peer, &datagram, now);

    // (2) Publish an enabling encryption policy. The `mem_ingress` queue must
    // be cleared — the queued datagram was accepted on a disabled-encryption
    // policy and `decrypt_gossip` reading `self.encryption` after the flip
    // would surface it under the new strict-mode regime (either accepting
    // an unauthenticated datagram in the disabled→enabled direction the test
    // exercises, or rejecting an authenticated datagram in the
    // enabled→disabled mirror direction).
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    coord.set_encryption_options(opts);

    assert!(
      coord.poll_memberlist_ingress().is_none(),
      "set_encryption_options MUST purge the `mem_ingress` queue on a real \
       policy change — without the clear, the queued datagram would survive \
       across the policy flip and be decrypted/rejected under the new policy \
       it was never validated against",
    );
  }

  /// A bridge failed by a runtime encryption-policy change
  /// ([`crate::bridge_phase::BridgeFailure::EncryptionPolicyChanged`]) stays
  /// in [`StreamEndpoint::conns`] until the next [`pump_bridges`] iteration
  /// reaps it. Inbound transport bytes for that exchange may be delivered by
  /// the driver in the window between
  /// [`StreamEndpoint::set_encryption_options`] and the wake-latch reap —
  /// a network read the driver had already issued, or an in-flight TCP
  /// segment that arrives between the policy publication and the reap call.
  ///
  /// [`StreamBridge::handle_transport_data`] MUST short-circuit any such
  /// post-failure ingress: feeding the bytes through
  /// [`StreamBridge::pump_in_established`] would route them through the
  /// records layer, decode them as a reliable unit, and pass the surfaced
  /// plaintext to [`Stream::handle_data`]. A valid response frame applied
  /// after the bridge is failed would queue an
  /// [`crate::EndpointEvent::PushPullReplyReceived`] (or analog) that
  /// [`StreamBridge::drain_then_reap`] then routes through
  /// [`Endpoint::handle_stream_event`], committing membership state from an
  /// exchange the local node has already declared dead.
  ///
  /// Test angle: build a dialer push/pull, drain its `Connect` and request
  /// bytes, publish an enabling encryption policy (fails the bridge),
  /// craft a reliable unit encoded under the NEW policy carrying a synthetic
  /// peer ("carol"), and deliver those bytes through
  /// [`StreamEndpoint::handle_transport_data`] BEFORE any
  /// [`StreamEndpoint::handle_timeout`] reap. Without the guard, the
  /// post-failure ingress would surface as an [`Event::NodeJoined`] for carol
  /// and [`Endpoint::num_members`] would grow to 2. With the guard, the
  /// bridge's `handle_transport_data` no-ops on its terminal phase, the
  /// reliable unit is never decoded, no stream events are queued, and the
  /// only effect is the queued [`crate::StreamAction::Abort`] the
  /// policy-change path already enqueued synchronously.
  ///
  /// Mutation gate: removing the terminal-phase guard from
  /// [`StreamBridge::handle_transport_data`] makes [`Endpoint::num_members`]
  /// grow to 2 here AND surfaces an [`Event::NodeJoined`] for the synthetic
  /// peer — the [`crate::typed::PushNodeState`] reaches
  /// `merge_state` via the drain-then-reap path and is committed.
  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn policy_failed_bridge_rejects_ingress_before_reap() {
    use crate::{
      CompressionOptions, EncryptionOptions, Keyring, SecretKey,
      encode_reliable_unit_with_encryption,
      typed::{PushNodeState, State},
    };
    use bytes::Bytes;

    use crate::endpoint::Endpoint;

    let now = Instant::now();
    let ep = endpoint(7800);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) Open a dialer push/pull under disabled encryption. `start_push_pull`
    // runs `service_dials` + `flush_outbound`, which builds the bridge,
    // promotes it (RawRecords dialer is non-handshaking from construction),
    // and pumps the request bytes out into `out_transmit`. The dialer's
    // stream FSM advances to `OutboundAwaitingResponse` once `poll_transmit`
    // drains the output buffer inside that flush.
    coord.start_push_pull(addr(7801), PushPullKind::Refresh, now);
    let connect = coord
      .poll_action()
      .expect("the dial surfaces a Connect action");
    let exchange = match connect {
      StreamAction::Connect(c) => c.id(),
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    };
    // Drain the dialer's queued request bytes so they do not interfere with
    // the post-policy-change observations below. The bytes are the dialer's
    // own push/pull request — the regression scenario is about INBOUND
    // bytes arriving for the same exchange, not the outbound queue.
    while coord.poll_transport_transmit().is_some() {}

    // Pre-condition invariants: the bridge is alive, the local endpoint has
    // exactly one tracked member (itself), and no Event::NodeJoined is queued.
    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(false),
      "pre-update invariant: the bridge is still running",
    );
    let pre_members = coord.endpoint_mut().num_members();
    assert_eq!(
      pre_members, 1,
      "pre-update invariant: only the local node is tracked (got {})",
      pre_members,
    );

    // (2) Build the NEW encryption policy and craft a reliable unit encoded
    // under that policy carrying a synthetic peer ("carol"). The unit
    // decodes successfully on a bridge that holds the NEW policy — the
    // exact shape an adversary (or a peer that already received the new
    // policy) would put on the wire between policy publication and the
    // wake-latch reap, when in-flight bytes can race the reap.
    let new_opts =
      EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let carol_addr = addr(7802);
    let carol = PushNodeState::new(1, SmolStr::new("carol"), carol_addr, State::Alive);
    let response = Endpoint::<SmolStr, SocketAddr>::encode_push_pull_response(
      core::slice::from_ref(&carol),
      Bytes::new(),
      false,
    );
    let unit =
      encode_reliable_unit_with_encryption(&CompressionOptions::new(), &new_opts, &response)
        .expect("aes-gcm primary -> encrypt succeeds for a well-formed PushPull response");
    // The dialer's records layer still validates its INBOUND label before
    // surfacing plaintext (the dialer is non-handshaking from construction,
    // but its label gate's inbound label starts unvalidated — see
    // [`crate::streams::label::LabelGate`] on the dialer's in-line inbound
    // label check). Prepend the label frame so the records layer accepts the
    // unit on the first call rather than rejecting on a wrong-cluster
    // label-mismatch — without the prefix the records layer would
    // `Intake::Failed` the bytes and the bridge's `pump_in_established` would
    // short-circuit BEFORE reaching `Stream::handle_data`, masking the
    // regression this test asserts.
    let label = b"cluster-x";
    let mut framed = Vec::with_capacity(2 + label.len() + unit.len());
    framed.push(12u8); // LABELED_TAG
    framed.push(label.len() as u8);
    framed.extend_from_slice(label);
    framed.extend_from_slice(&unit);

    // (3) Publish the enabling policy. The insecure-transport bridge fails
    // with `BridgeFailure::EncryptionPolicyChanged`; the bridge stays in
    // `conns` until the next `pump_bridges` iteration. From this point the
    // bridge is terminal but reachable.
    coord.set_encryption_options(new_opts);
    assert_eq!(
      coord.bridge_is_failed(exchange),
      Some(true),
      "an insecure-transport bridge MUST fail on a runtime encryption-policy \
       change so the post-failure ingress guard has a Failed phase to gate on",
    );

    // (4) Deliver the inbound reliable unit BEFORE any reap. Without the
    // terminal-phase guard at `StreamBridge::handle_transport_data`, this
    // bytes feed would reach `pump_in_established`, decode under the
    // bridge's NEW encryption (which it now holds post-`set_encryption`),
    // queue a `PushPullReplyReceived` event on the still-`OutboundAwaitingResponse`
    // FSM, and `drain_then_reap` (run by `run_tick` at the end of
    // `handle_transport_data`) would route that event through
    // `Endpoint::handle_stream_event` -> `merge_state` -> `process_alive`,
    // committing "carol" to the membership table.
    coord.handle_transport_data(exchange, &framed, false, now);

    // (5) The post-failure ingress was rejected: `num_members` is unchanged
    // and no `Event::NodeJoined` for carol surfaces from `poll_event`. The
    // bridge was reaped by the same-call `run_tick`, so the `Abort` action
    // is reachable from `poll_action`.
    let post_members = coord.endpoint_mut().num_members();
    assert_eq!(
      post_members, pre_members,
      "post-failure ingress MUST NOT mutate membership state — `num_members` \
       grew from {} to {}, which means the reliable unit reached \
       `Stream::handle_data` and the `PushPullReplyReceived` event applied \
       via `drain_then_reap`. The terminal-phase guard at \
       `StreamBridge::handle_transport_data` is missing or ineffective.",
      pre_members, post_members,
    );
    while let Some(ev) = coord.endpoint_mut().poll_event() {
      assert!(
        !matches!(ev, Event::NodeJoined(_)),
        "no `Event::NodeJoined` may surface from the post-failure ingress; \
         got {:?}",
        ev,
      );
    }
  }

  /// The `StreamEndpoint` membership/lifecycle forwarders delegate to the
  /// inner [`Endpoint`]. Drive each once on a real coordinator so the
  /// pass-through surface is covered and the `last_now` anchoring side effect
  /// is exercised. Behavioural depth lives in the endpoint's own tests + the
  /// sim harness; this guards the thin forwarding layer.
  #[test]
  fn stream_endpoint_membership_forwarders_delegate_to_endpoint() {
    use crate::{
      node::Node,
      typed::{Alive, Meta, Suspect},
    };

    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // Pure read-only accessors.
    assert_eq!(coord.resolve_peer_socket(&addr(7000)), addr(7000));
    // No compression configured by default → the algorithm is None.
    assert!(coord.compression().algorithm().is_none());
    assert!(coord.max_stream_frame_size() > 0);

    // Lifecycle: before scheduling and before any leave, the endpoint is
    // running.
    coord.start_scheduling(now);
    assert!(coord.is_running());

    // Seed an alive peer via the bootstrap forwarder, then assert membership
    // grew — `handle_alive` reached the inner endpoint.
    let alive = Alive::new(1, Node::new(SmolStr::new("peer-a"), addr(7001)));
    coord.handle_alive(addr(7001), alive, now);
    assert!(
      coord
        .endpoint_ref()
        .member(&SmolStr::new("peer-a"))
        .is_some(),
      "handle_alive forwarded the alive into the inner endpoint",
    );

    // Suspect forwarder: inject a suspicion on the seeded peer (does not panic
    // and reaches the inner endpoint).
    let suspect = Suspect::new(1, SmolStr::new("peer-a"), SmolStr::new("n-7100"));
    coord.handle_suspect(addr(7001), suspect, now);

    // Metadata + broadcast + probe forwarders.
    coord
      .update_meta(Meta::empty())
      .expect("update_meta forwards to the running endpoint");
    coord
      .queue_user_broadcast(Bytes::from_static(b"bcast"))
      .expect("queue_user_broadcast forwards");
    coord
      .set_local_state_snapshot(Bytes::from_static(b"snap"))
      .expect("set_local_state_snapshot forwards");
    coord
      .set_ack_payload(Bytes::from_static(b"ack-extra"))
      .expect("set_ack_payload forwards");
    // `start_probe` returns whether a probe target was selected; either bool
    // is a valid outcome — the point is the forwarder runs.
    let _ = coord.start_probe(now);

    // Directed unreliable user packets.
    coord
      .send_user_packet(addr(7001), Bytes::from_static(b"u1"))
      .expect("send_user_packet forwards");
    coord
      .send_user_packets(
        addr(7001),
        &[Bytes::from_static(b"u2"), Bytes::from_static(b"u3")],
      )
      .expect("send_user_packets forwards");

    // An application ping returns a correlation token.
    let _ping_id = coord.ping(Node::new(SmolStr::new("peer-a"), addr(7001)), now);

    // `poll_memberlist_transmit` drains the inner endpoint's outbound gossip;
    // at least the seeded alive / broadcasts produced some transmit traffic.
    let mut transmits = 0usize;
    while coord.poll_memberlist_transmit().is_some() {
      transmits += 1;
    }
    assert!(
      transmits > 0,
      "the seeded membership produced gossip transmits"
    );

    // `poll_event`'s DialRequested-sieve arm: a DialRequested emitted by the
    // inner endpoint must be sieved into the private dial deque, never returned
    // to the caller. Inject one through the inner endpoint, then drain events.
    let _intent =
      coord
        .endpoint_mut()
        .start_push_pull(addr(7009), crate::event::PushPullKind::Refresh, now);
    while let Some(ev) = coord.poll_event() {
      assert!(
        !matches!(ev, Event::DialRequested(_)),
        "poll_event sieves DialRequested into the private deque",
      );
    }

    // `leave` forwards to the inner endpoint and transitions the lifecycle out
    // of running.
    coord
      .leave(now)
      .expect("leave forwards to the running endpoint");
    assert!(
      !coord.is_running(),
      "after leave the endpoint is no longer running",
    );
  }

  /// `requeue_event` routes a `DialRequested` into the private dial queue
  /// (surfacing a `Connect` via the normal poll path) and delegates every
  /// other variant to the inner endpoint's queue (re-observed via
  /// `poll_event`). Covers both arms of the match.
  #[test]
  fn requeue_event_routes_dial_requested_privately_and_others_to_endpoint() {
    use crate::event::DialRequested;

    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // (1) A requeued DialRequested is routed DIRECTLY into the private dial
    // deque (the `Event::DialRequested` arm of `requeue_event`): it must NOT
    // resurface through poll_event, AND it anchors an immediate-due
    // `poll_timeout` wake (the freshly-sieved, never-attempted entry) so a
    // caller advancing solely by `poll_timeout` cannot orphan it.
    let dial = DialRequested::new(
      StreamId::from_raw(0),
      addr(7000),
      now + Duration::from_secs(10),
    );
    coord.requeue_event(Event::DialRequested(dial), now);
    while let Some(ev) = coord.poll_event() {
      assert!(
        !matches!(ev, Event::DialRequested(_)),
        "a requeued DialRequested is serviced privately, never re-surfaced",
      );
    }
    // The never-attempted private dial entry forces an immediate-due wake
    // (`<= now`), proving the DialRequested arm landed it in `dial_pending`.
    let wake = coord
      .poll_timeout()
      .expect("the requeued dial intent contributes a poll_timeout wake");
    assert!(
      wake <= now,
      "a freshly-requeued, never-attempted dial intent forces an immediate-due \
       wake so a poll_timeout-only driver services it",
    );

    // (2) A non-DialRequested event delegates to the inner endpoint's queue and
    // is observable via poll_event. Build a `NodeJoined` directly, requeue it,
    // and confirm the delegation arm round-trips it back through poll_event.
    use crate::typed::{NodeState, State};
    use std::sync::Arc;
    let ns = Arc::new(NodeState::new(
      SmolStr::new("peer-j"),
      addr(7003),
      State::Alive,
    ));
    coord.requeue_event(Event::NodeJoined(ns), now);
    let mut re_observed = false;
    while let Some(ev) = coord.poll_event() {
      if matches!(ev, Event::NodeJoined(_)) {
        re_observed = true;
      }
    }
    assert!(
      re_observed,
      "a requeued non-DialRequested event delegates to the inner endpoint and \
       resurfaces through poll_event",
    );
  }

  /// `start_reliable_ping` (the public reliable-fallback dial entry point)
  /// surfaces a `Connect` in-band and threads the originating `StreamId`,
  /// exactly like the push/pull / user-message wrappers.
  #[test]
  fn start_reliable_ping_dials_in_band() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    let sid = coord.start_reliable_ping(
      SmolStr::new("peer-b"),
      addr(7000),
      9,
      now + Duration::from_secs(5),
      now,
    );
    let connect = coord
      .poll_action()
      .expect("the reliable-ping dial surfaces a Connect in-band");
    match connect {
      StreamAction::Connect(info) => {
        assert_eq!(info.peer(), addr(7000));
        assert_eq!(
          info.stream_id(),
          sid,
          "Connect.stream_id() equals the StreamId start_reliable_ping returned",
        );
      }
      other => panic!("expected Connect, got {:?}", action_kind(&other)),
    }
    // The dialer's label + ping request bytes emerge on the same tick.
    let (_id, _peer, bytes) = coord
      .poll_transport_transmit()
      .expect("the reliable-ping dialer queued its label + request");
    assert!(bytes.starts_with(&[12u8]), "leads with LABELED_TAG");
  }

  /// `handle_dial_failed` for a live outbound exchange fails the bridge via
  /// `fail_dial_retired` and the resulting reap surfaces an `Abort` (the
  /// failed-terminal teardown). A one-way user-message whose dial the driver
  /// reports failed must terminalize as a genuine failure, never a benign EOF.
  #[test]
  fn handle_dial_failed_fails_bridge_and_aborts() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // Dial a one-way user message so a live bridge + Connect exist.
    let _sid = coord.start_user_message(addr(7000), Bytes::from_static(b"x"), now);
    let exchange = match coord.poll_action() {
      Some(StreamAction::Connect(c)) => c.id(),
      other => panic!(
        "expected Connect, got {:?}",
        other.as_ref().map(action_kind)
      ),
    };
    // Drain the in-band request bytes so the teardown gate is not withheld
    // behind them after the failure.
    while coord.poll_transport_transmit().is_some() {}
    assert!(coord.live_bridge_count() >= 1);

    // The driver reports the dial failed. The bridge fails (dial retired) and
    // the reap emits an Abort for the exchange.
    coord.handle_dial_failed(exchange, now);

    let mut actions: Vec<StreamAction> = Vec::new();
    loop {
      let mut progressed = false;
      while let Some(a) = coord.poll_action() {
        actions.push(a);
        progressed = true;
      }
      while coord.poll_transport_transmit().is_some() {
        progressed = true;
      }
      if !progressed {
        break;
      }
    }
    assert!(
      actions
        .iter()
        .filter_map(StreamAction::as_abort)
        .any(|r| r.id() == exchange),
      "a dial-failed exchange is reaped with Abort; got {:?}",
      actions.iter().map(action_kind).collect::<Vec<_>>(),
    );
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the dial-failed bridge was reaped",
    );
  }

  /// `set_compression_options` fans the new policy out to every live bridge's
  /// `set_compression` (non-security: no failure cascade, no purge). Dial an
  /// exchange first so there is a live bridge to receive the update.
  #[cfg(feature = "compression-lz4")]
  #[test]
  fn set_compression_options_fans_out_to_live_bridges() {
    use crate::{CompressAlgorithm, CompressionOptions};

    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = LabelOptions::new_in(Some(b"cluster-x".to_vec()), ());
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, RawRecords> =
      StreamEndpoint::new(ep, cfg, test_sni_provider(), test_peer_to_socket());

    // A live outbound bridge.
    let _sid = coord.start_user_message(addr(7000), Bytes::from_static(b"x"), now);
    while coord.poll_action().is_some() {}
    while coord.poll_transport_transmit().is_some() {}
    assert!(coord.live_bridge_count() >= 1);

    // Apply a non-default compression policy — fans out to the live bridge
    // (and updates the coordinator's own `compression`).
    let comp = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(8);
    coord.set_compression_options(comp);
    assert_eq!(
      coord.compression().algorithm(),
      Some(CompressAlgorithm::Lz4),
      "the coordinator's compression policy updated",
    );
    // A second call with default (disabled) compression also fans out without
    // failing the live bridge (the non-security no-cascade path).
    coord.set_compression_options(CompressionOptions::new());
    assert!(coord.compression().algorithm().is_none());
    assert!(
      coord.live_bridge_count() >= 1,
      "a compression-policy change never fails a live bridge",
    );
  }

  fn action_kind(a: &StreamAction) -> &'static str {
    match a {
      StreamAction::Connect(_) => "Connect",
      StreamAction::Shutdown(_) => "Shutdown",
      StreamAction::Close(_) => "Close",
      StreamAction::Abort(_) => "Abort",
    }
  }
}
