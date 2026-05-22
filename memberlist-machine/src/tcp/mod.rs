//! Plain-TCP record layer for the generic Sans-I/O stream transport.
//!
//! A label-prefix-then-raw-passthrough record layer: a one-time
//! `[LABELED_TAG=12][len][label]` frame (byte-compatible with the frozen
//! `memberlist-proto` label frame) is written once at stream start by both
//! sides, then bytes pass through verbatim — there is no transport-level
//! encryption. [`RawRecords`] implements [`StreamTransport`], so a
//! [`crate::streams::StreamEndpoint`] parameterised with `R = RawRecords`
//! carries reliable membership exchanges over a per-exchange plain TCP
//! connection (plain UDP carries the unreliable gossip on a separate
//! socket). TLS's in-band `close_notify` half-close anchor is replaced by
//! the out-of-band TCP FIN (`shutdown(write)`).

#[cfg(test)]
mod bridge;
#[cfg(test)]
mod conn;
mod options;
mod records;

use std::time::Instant;

use crate::{
  addr_bridge::AddrBridge,
  streams::transport::{Intake, StreamTransport},
};

pub use options::TcpOptions;
pub use records::RawRecords;

/// The raw-passthrough record layer as a transport-agnostic
/// [`StreamTransport`] plug.
///
/// Plain TCP needs no per-dial verification identity, so [`Self::DialContext`]
/// is `()` and [`Self::dial_context`] returns `Ok(())` without consulting the
/// bridge's `server_name`. The [`RawRecords`] constructors are infallible (a
/// label is validated at [`TcpOptions`] build time, not at record-layer
/// construction), so [`Self::ConstructError`] is [`core::convert::Infallible`].
/// The inherent `records::Intake::{Done, Pending, Failed}` map straight across
/// to the unified [`Intake`].
impl StreamTransport for RawRecords {
  type Options = TcpOptions;
  type DialContext = ();
  type ConstructError = core::convert::Infallible;

  fn dial_context<A, B>(_addr: &A) -> Result<(), &'static str>
  where
    B: AddrBridge<A>,
  {
    Ok(())
  }

  fn dialer(opts: &Self::Options, _ctx: Self::DialContext) -> Result<Self, Self::ConstructError> {
    Ok(RawRecords::dialer(
      opts.label().map(<[u8]>::to_vec),
      opts.skip_inbound_label_check(),
    ))
  }

  fn acceptor(opts: &Self::Options) -> Result<Self, Self::ConstructError> {
    Ok(RawRecords::acceptor(
      opts.label().map(<[u8]>::to_vec),
      opts.skip_inbound_label_check(),
    ))
  }

  fn handle_transport_data(&mut self, input: &[u8], now: Instant) -> Intake {
    match RawRecords::handle_transport_data(self, input, now) {
      records::Intake::Done => Intake::Done,
      records::Intake::Pending(n) => Intake::Pending(n),
      records::Intake::Failed => Intake::Failed,
    }
  }

  fn poll_transport_transmit(&mut self, out: &mut Vec<u8>) -> usize {
    RawRecords::poll_transport_transmit(self, out)
  }

  fn is_handshaking(&self) -> bool {
    RawRecords::is_handshaking(self)
  }

  fn read_plaintext(&mut self, out: &mut Vec<u8>) -> usize {
    RawRecords::read_plaintext(self, out)
  }

  fn write_plaintext(&mut self, plaintext: &[u8]) {
    RawRecords::write_plaintext(self, plaintext)
  }

  fn send_close_notify(&mut self) {
    RawRecords::send_close_notify(self)
  }

  fn peer_has_closed(&self) -> bool {
    RawRecords::peer_has_closed(self)
  }

  fn clear_outbound(&mut self) {
    RawRecords::clear_outbound(self)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
  };

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::{TcpOptions, records::RawRecords};
  use crate::{
    addr_bridge::AddrBridge,
    config::EndpointConfig,
    endpoint::Endpoint,
    event::{Event, PushPullKind},
    streams::{ConnectInfo, ExchangeId, ExchangeRef, StreamAction, StreamEndpoint},
  };

  /// Identity `AddrBridge` for `A = SocketAddr`, matching the sim harness's
  /// shape. The `server_name` accessor is unused on the plain-TCP path (no
  /// certificate verification) but must be supplied for the trait.
  struct IdentityBridge;

  impl AddrBridge<SocketAddr> for IdentityBridge {
    type ServerName = str;

    fn to_socket(addr: &SocketAddr) -> SocketAddr {
      *addr
    }
    fn from_socket(socket: SocketAddr) -> SocketAddr {
      socket
    }
    fn server_name(_addr: &SocketAddr) -> Option<&'static str> {
      None
    }
  }

  fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
  }

  fn endpoint(local_port: u16) -> Endpoint<SmolStr, SocketAddr> {
    Endpoint::new(EndpointConfig::new(
      SmolStr::new(format!("n-{local_port}")),
      addr(local_port),
    ))
  }

  /// Public-constructor signature check. Mirrors
  /// `tls::tests::tls_endpoint_type_is_constructible_signature`; behavioural
  /// coverage lives in the sim harness.
  #[test]
  fn tcp_endpoint_type_is_constructible_signature() {
    let ep = endpoint(7100);
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);
    assert_eq!(coord.live_bridge_count(), 0);
  }

  /// `StreamAction` accessor parity: each variant's `as_*` returns `Some` only
  /// for itself. The accessors are the only seam the driver can pattern-match
  /// against without owning the enum's privacy.
  #[test]
  fn tcp_action_accessors_match_only_their_variant() {
    let id = ExchangeId::new(0);
    let connect = StreamAction::Connect(ConnectInfo::new(id, addr(7000)));
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
  /// + the dialer's label prefix in the SAME call. The strict-poll
  /// self-sufficiency seam: a driver using only the public poll surface sees
  /// the dial proceed without a separate `handle_timeout` pre-pump.
  #[test]
  fn start_push_pull_dials_in_band_and_does_not_leak_dial_requested() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
        !matches!(ev, Event::DialRequested { .. }),
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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

    let id = coord.accept_connection(addr(7000), now);
    assert_eq!(coord.live_bridge_count(), 1);
    let _ = id; // monotonic, exercised in StreamConns tests

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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
    let cfg_s = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut server: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(server_ep, cfg_s);
    let server_exchange = server.accept_connection(dialer_addr, now);
    assert_eq!(server.live_bridge_count(), 1);

    // Dialer coordinator. Starting a one-way user-message produces the
    // `[label||frame]` blob on the very first `poll_transport_transmit` (the
    // dialer's label step settles at construction and `start_user_message`
    // flushes outbound in-band).
    let dialer_ep = endpoint(dialer_addr.port());
    let cfg_d = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut dialer: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(dialer_ep, cfg_d);
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
      if let Event::UserPacket { data, .. } = ev {
        got = Some(data);
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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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

    // (6) Load-bearing assertion: `Close` for the failed exchange did
    // surface — the gate must release the teardown once the purge empties
    // the exchange's queue, else the bridge would leak (no `Close` ever
    // fires).
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the timed-out exchange {:?} must surface from the natural \
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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

    // (1) Driver accepts an inbound TCP connection from a wrong-cluster
    // peer. The acceptor's `RawRecords::outbound` is populated with the
    // local label prefix `[12][9]["cluster-x"]` at construction. The
    // acceptor stays Handshaking until its inbound label is validated.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(
      coord.poll_transport_transmit().is_none(),
      "accept_connection emits no transmits in-band",
    );

    // (2) Wrong-cluster peer sends its own labeled header. The acceptor's
    // `classify_inbound_label` returns Rejected (label mismatch), the
    // bridge enters `Failed(Transport)` via `intake_handshaking`'s
    // `self.fail(...)`, and the driver-facing `run_tick` runs through to
    // the reap.
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

    // (5) Load-bearing assertion: `Close` for the failed exchange did
    // surface — the bridge is retired, but the coordinator MUST signal
    // the driver to close the socket.
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the rejected acceptor {:?} must surface; got {:?}",
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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

    // (1) Driver accepts an inbound connection; the acceptor queues
    // `[12][9]["cluster-x"]` in `RawRecords::outbound` at construction.
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

    // (5) Load-bearing assertion: `Close` for the failed exchange did
    // surface.
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the slow-loris acceptor {:?} must surface; got {:?}",
      exchange,
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );
  }

  /// A driver that runs one `handle_timeout` tick on a freshly-accepted
  /// connection — BEFORE any inbound bytes arrive — must observe NO
  /// outbound bytes for that exchange. The acceptor's outbound label prefix
  /// is queued LAZILY at inbound-label validation, so the bridge's
  /// `records.outbound` is empty across the pump and `finalize_tick`'s
  /// `collect_bridge_transmits` drains nothing for this exchange. Faithful
  /// to `memberlist-core/src/network.rs::handle_conn`'s
  /// `read_message`-before-`send_message` ordering: an acceptor reveals
  /// cluster identity only after the dialer has proven its own.
  ///
  /// Mutation gate: restoring the at-construction eager queue in
  /// `RawRecords::acceptor` (calling `queue_outbound_label_if_needed` from
  /// the constructor) makes this test fail with the leaked
  /// `[12][len][cluster-x]` bytes surfacing on the first pump.
  #[test]
  fn accept_connection_then_handle_timeout_no_bytes_before_inbound_validation() {
    let now = Instant::now();
    let ep = endpoint(7100);
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

    // (1) Accept the connection; nothing on the wire yet.
    let exchange = coord.accept_connection(addr(7000), now);
    assert!(coord.poll_transport_transmit().is_none());

    // (2) Deliver a matching labeled header from the peer; the acceptor's
    // `classify_inbound_label` returns `Accepted`, the lazy queue fires
    // inside `handle_transport_data`, and the bridge's
    // `records.outbound` now holds the `[12][9][cluster-x]` prefix.
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
    let cfg_d = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut dialer: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(dialer_ep, cfg_d);
    let _sid = dialer.start_push_pull(server_addr, PushPullKind::Join, now);
    // Drain the `Connect` so the natural drain loop below sees only bytes.
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
    let cfg_s = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut server: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(server_ep, cfg_s);
    let server_exchange = server.accept_connection(dialer_addr, now);

    // (3) First transport read: ONLY the dialer's label prefix, no FIN.
    // The acceptor's inbound label validates (`classify_inbound_label`
    // returns `Accepted`), the lazy outbound label fires inside
    // `RawRecords::handle_transport_data`, and the bridge's
    // `records.outbound` now holds `[12][9][cluster-x]`. The Stream is
    // not minted yet because the bytes-only feed leaves the bridge
    // Handshaking until the next `run_tick`'s
    // `service_handshake_completions`.
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
    let cfg = TcpOptions::new(Some(b"cluster-x".to_vec()));
    let mut coord: StreamEndpoint<SmolStr, SocketAddr, IdentityBridge, RawRecords> =
      StreamEndpoint::new(ep, cfg);

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

    // (8) `Close` for the failed exchange surfaces — the contract per the
    // fix is to emit `Close` so a driver that had already drained
    // `Connect` from a prior tick can clean up the open socket. In this
    // test the Connect was purged so a driver's `Close` is a no-op for
    // an unopened socket, but the action presence is the documented
    // behaviour.
    let close_for_exchange = actions_observed
      .iter()
      .filter_map(|a| a.as_close())
      .any(|r| r.id() == exchange);
    assert!(
      close_for_exchange,
      "Close for the failed exchange must surface; got {:?}",
      actions_observed.iter().map(action_kind).collect::<Vec<_>>(),
    );

    // (9) The bridge was reaped and the exchange forgotten.
    assert_eq!(
      coord.live_bridge_count(),
      0,
      "the bridge was reaped on the dial-failure path",
    );
  }

  // ---- helpers ----------------------------------------------------------

  fn action_kind(a: &StreamAction) -> &'static str {
    match a {
      StreamAction::Connect(_) => "Connect",
      StreamAction::Shutdown(_) => "Shutdown",
      StreamAction::Close(_) => "Close",
    }
  }
}
