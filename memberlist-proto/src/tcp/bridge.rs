//! Tests for the generic `memberlist::Stream` <-> record-layer byte-pump
//! ([`crate::streams::bridge::StreamBridge`]) wrapping the raw-passthrough
//! plain-TCP record layer ([`RawRecords`](super::records::RawRecords)).

// Symbols the `#[cfg(test)] mod tests` glob-imports via `use super::*`. Gated
// so the tests-only module has no non-test-build footprint.
#[cfg(test)]
use crate::{
  bridge_phase::{BridgeFailure, BridgePhase},
  endpoint::Endpoint,
};

#[cfg(test)]
mod tests {
  use super::*;
  use crate::Instant;
  use core::{net::SocketAddr, time::Duration};

  use bytes::Bytes;
  use smol_str::SmolStr;

  use super::super::records::RawRecords;
  use crate::{
    config::EndpointOptions,
    error::StreamError,
    event::Event,
    streams::{
      bridge::StreamBridge,
      phase::StreamPhase,
      test_support::{
        TEST_RELIABLE_MAX, addr, handshaking_pair as shared_handshaking_pair, label, phase_label,
      },
    },
  };

  /// Build a `Handshaking` dialer/acceptor `RawRecords` bridge pair over a
  /// shared cluster label. Delegates to the shared `handshaking_pair` with
  /// the TCP-specific dialer/acceptor constructors.
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

  /// Pump each side's outbound label prefix across to its peer so both
  /// inbound-label validations settle. Symmetric with the bidirectional wire
  /// label on the reliable path. The dialer queues its `[12][len][label]`
  /// eagerly at construction; once delivered, the acceptor validates it and
  /// lazily queues its own outbound `[12][len][label]`, which this helper
  /// then drains back to the dialer. The acceptor stays handshaking until
  /// its inbound label is validated; the dialer is never handshaking (its
  /// inbound validation runs in-line on the established intake).
  fn complete_label_exchange(
    client: &mut StreamBridge<SmolStr, SocketAddr, RawRecords>,
    server: &mut StreamBridge<SmolStr, SocketAddr, RawRecords>,
    now: Instant,
  ) {
    let mut client_prefix = Vec::new();
    client.poll_transport_transmit(&mut client_prefix);
    assert!(!client_prefix.is_empty(), "dialer queued a label prefix");
    server
      .handle_transport_data(&client_prefix, now)
      .expect("acceptor accepts the matching label prefix");
    assert!(!server.is_handshaking(), "label settled on the acceptor");
    assert!(!client.is_handshaking(), "a dialer is never handshaking");
    let mut server_prefix = Vec::new();
    server.poll_transport_transmit(&mut server_prefix);
    assert!(
      !server_prefix.is_empty(),
      "acceptor symmetrically queued its outbound label prefix"
    );
    client
      .handle_transport_data(&server_prefix, now)
      .expect("dialer accepts the matching inbound label prefix");
  }

  /// Shuttle bytes both ways once, modeling the driver's per-connection move
  /// AND the out-of-band TCP FIN: any non-empty outbound bytes are delivered to
  /// the peer's `handle_transport_data`, and once a side `fin_owed()` its peer
  /// is delivered exactly one zero-length read (the TCP `read == 0` EOF anchor)
  /// — the out-of-band FIN that `RawRecords` cannot carry in-band. `c_fin` /
  /// `s_fin` latch the one-shot EOF delivery. Returns `true` if anything moved
  /// (bytes or a FIN) this round.
  fn shuttle(
    client: &mut StreamBridge<SmolStr, SocketAddr, RawRecords>,
    server: &mut StreamBridge<SmolStr, SocketAddr, RawRecords>,
    c_fin_delivered: &mut bool,
    s_fin_delivered: &mut bool,
    now: Instant,
  ) -> bool {
    // Ignoring Err on every `handle_transport_data` below: this helper models the
    // driver, which does not branch on the return — a terminalized bridge is
    // asserted via `is_terminal()` / `phase` at the call sites, exactly as the
    // real coordinator reaps on terminality rather than on the `Result`.
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

    // Deliver each side's out-of-band FIN to its peer exactly once, AFTER its
    // buffered bytes have crossed: a clean TCP close is "drain then FIN", so the
    // peer reads the bytes then a `read == 0`.
    if client.fin_owed() && !*c_fin_delivered {
      *c_fin_delivered = true;
      let _ = server.handle_transport_data(&[], now);
      moved = true;
    }
    if server.fin_owed() && !*s_fin_delivered {
      *s_fin_delivered = true;
      let _ = client.handle_transport_data(&[], now);
      moved = true;
    }
    moved
  }

  /// (b) A one-way `UserData` frame round-trips through the byte pump: outbound
  /// client `Stream::poll_transmit` -> `write_plaintext` -> bytes -> server
  /// `handle_transport_data` -> `read_plaintext` -> inbound `Stream::handle_data`.
  /// (c) The FIN-only half-close (each side's `read == 0` EOF) drives both
  /// bridges to `BothClosed`, then the terminal D1 reap surfaces the decoded
  /// `UserData` as `Event::UserPacket` on the server Endpoint. Analog of the TLS
  /// `frame_round_trips_then_clean_close_reaps`, with `close_notify` replaced by
  /// the TCP FIN.
  #[test]
  fn frame_round_trips_then_fin_close_reaps() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    complete_label_exchange(&mut client, &mut server, now);

    // Outbound client: a one-way reliable user message.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7100)));
    let payload = Bytes::from_static(b"hello-tcp");
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);

    // Inbound server: accept the exchange.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
    let s_stream = ep_s.accept_stream(addr(7100), now);
    server.promote(s_stream);

    // Pump the request out of the client and into the server, shuttling bytes
    // and the out-of-band FIN until both bridges reap.
    let mut c_fin = false;
    let mut s_fin = false;
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, &mut c_fin, &mut s_fin, now);
      // Drain non-terminal inbound side effects every tick (D1 contract).
      if !server.is_terminal() {
        server.drain_payload_only(&mut ep_s, now);
      }
      if client.is_terminal() && server.is_terminal() {
        break;
      }
      if !moved && client.is_terminal() && server.is_terminal() {
        break;
      }
      if !moved {
        break;
      }
    }

    assert!(
      matches!(
        client.phase_ref(),
        StreamPhase::Established(BridgePhase::BothClosed)
      ),
      "client reached BothClosed (sent request + FIN, saw peer FIN), got {:?}",
      phase_label(client.phase_ref())
    );
    assert!(
      matches!(
        server.phase_ref(),
        StreamPhase::Established(BridgePhase::BothClosed)
      ),
      "server reached BothClosed, got {:?}",
      phase_label(server.phase_ref())
    );

    // Terminal D1 reap on the server: the decoded UserData surfaces as
    // Event::UserPacket (clean close → StreamClosed lifecycle notice).
    server.drain_then_reap(&mut ep_s, now);
    let mut got = None;
    while let Some(ev) = ep_s.poll_event() {
      if let Event::UserPacket(p) = ev {
        let (_, data, _) = p.into_parts();
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the round-tripped UserData frame surfaced on the server Endpoint"
    );
  }

  /// (d) Label-mismatch reject: an acceptor fed a wrong-label prefix tears the
  /// bridge down with NO `Stream` minted and ZERO endpoint side effects — the
  /// structural analog of the TLS bridge's mTLS-reject path. The bridge becomes
  /// terminal so the coordinator reaps it directly.
  #[test]
  fn label_mismatch_tears_down_with_no_stream_and_no_endpoint_events() {
    let now = Instant::now();
    // Acceptor expects `cluster-x`; the dialer presents `cluster-other`.
    let acceptor = RawRecords::acceptor(label("cluster-x"), false);
    let mut server = StreamBridge::<SmolStr, SocketAddr, RawRecords>::new(
      acceptor,
      now + Duration::from_secs(10),
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      TEST_RELIABLE_MAX,
    );
    let dialer = RawRecords::dialer(label("cluster-other"), false);
    let mut client = StreamBridge::<SmolStr, SocketAddr, RawRecords>::new(
      dialer,
      now + Duration::from_secs(10),
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      TEST_RELIABLE_MAX,
    );

    let mut prefix = Vec::new();
    client.poll_transport_transmit(&mut prefix);
    assert!(!prefix.is_empty(), "dialer produced a label prefix");

    assert!(server.is_handshaking());
    let res = server.handle_transport_data(&prefix, now);
    assert!(res.is_err(), "the server rejected the mismatched label");

    // No `Stream` was minted, the bridge is terminal, and no endpoint events
    // exist (there is no Endpoint involvement on the reject path at all).
    assert!(
      server.stream_is_none(),
      "no Stream minted on a label-mismatch reject"
    );
    assert!(
      server.is_terminal(),
      "the bridge is terminal after the reject"
    );
    assert!(
      matches!(
        server.phase_ref(),
        StreamPhase::Established(BridgePhase::Failed(BridgeFailure::Transport(_)))
      ),
      "the reject is a Transport failure, got {:?}",
      phase_label(server.phase_ref())
    );

    // `drain_then_reap` on a no-Stream bridge is a clean no-op (the coordinator
    // reaps a failed-label bridge without an FSM lifecycle notice).
    let mut ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
    server.drain_then_reap(&mut ep, now);
    assert!(
      ep.poll_event().is_none(),
      "no endpoint events on the reject path"
    );
  }

  /// Symmetric dialer-side reject: a wrong-cluster responder with
  /// `skip_inbound_label_check = true` could accept our dial, then answer with
  /// a DIFFERENT label. Without inbound validation on the dialer side the
  /// response would be treated as plaintext and merged into membership — the
  /// cross-cluster footgun. The dialer bridge's established intake validates
  /// the inbound response label and terminalizes on a mismatch, so no merge /
  /// ack can ever land.
  #[test]
  fn dialer_bridge_rejects_mismatched_inbound_response_label() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    // Dialer expects `cluster-x`; promote it with a real outbound exchange so
    // the established intake validates the inbound response label (rather
    // than the pre-promote handshaking intake).
    let dialer = RawRecords::dialer(label("cluster-x"), false);
    let mut client = StreamBridge::<SmolStr, SocketAddr, RawRecords>::new(
      dialer,
      deadline,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      TEST_RELIABLE_MAX,
    );
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7300)));
    let sid = ep_c.start_reliable_ping(
      SmolStr::new("srv"),
      addr(7000),
      7,
      now + Duration::from_secs(5),
    );
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the dialer's reliable-ping stream");
    client.promote(c_stream);
    client.pump_out(now).ok();
    // Drain whatever bytes the client queued (label + request).
    let mut tx = Vec::new();
    client.poll_transport_transmit(&mut tx);
    assert!(!tx.is_empty(), "dialer queued its [label||request] bytes");

    // Feed a wrong-label response. Under bidirectional validation this is
    // `Intake::Failed`; the bridge atomic-retires + terminalizes through the
    // `Established` `Transport` failure path (no merge / ack survives).
    let mut response = vec![12u8, 5];
    response.extend_from_slice(b"other");
    response.extend_from_slice(b"ack-bytes-that-would-never-be-trusted");
    let res = client.handle_transport_data(&response, now);
    assert!(res.is_err(), "the dialer rejects the wrong-label response");
    assert!(
      matches!(
        client.phase_ref(),
        StreamPhase::Established(BridgePhase::Failed(BridgeFailure::Transport(_)))
      ),
      "the reject is a Transport failure, got {:?}",
      phase_label(client.phase_ref())
    );
    assert!(client.is_terminal());

    // The dialer never observed the response payload as plaintext: the inner
    // `Stream` is FSM-failed (no merge / ack survives), and the D1 reap on
    // the bridge runs without emitting any application-visible event for the
    // rejected exchange.
    client.drain_then_reap(&mut ep_c, now);
    while let Some(ev) = ep_c.poll_event() {
      // Only `DialRequested` may surface here (the inner endpoint also
      // queued one when the reliable-ping was started); the test cares that
      // NO membership-mutating event (a peer Joined/Updated/Left) appears
      // — those would only fire if the rejected label leaked.
      assert!(
        !matches!(
          ev,
          Event::NodeJoined(_) | Event::NodeUpdated(_) | Event::NodeLeft(_)
        ),
        "no membership mutation can survive a wrong-label response",
      );
    }
  }

  /// (f) Label-exchange timeout: a `Handshaking` bridge whose deadline elapses
  /// WITHOUT the label settling is terminalized by `pump_out` with NO `Stream`
  /// minted, so the coordinator's pre-`Stream` reap collects it. This is the
  /// slowloris / connect-refused bound — without it a stalled label exchange
  /// leaks the bridge (the empty-slice EOF anchor is a no-op while no inbound
  /// label arrives, and the `is_done()`-gated flush deadline needs a `Stream`
  /// that does not exist yet). Mirrors the TLS analog verbatim.
  #[test]
  fn handshaking_bridge_times_out_at_deadline_with_no_stream() {
    let now = Instant::now();
    let deadline = now + Duration::from_secs(10);
    // An acceptor that never receives its inbound label stays `Handshaking`.
    let acceptor = RawRecords::acceptor(label("cluster-x"), false);
    let mut server = StreamBridge::<SmolStr, SocketAddr, RawRecords>::new(
      acceptor,
      deadline,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      TEST_RELIABLE_MAX,
    );

    // Before the deadline a `pump_out` is a clean no-op (still Handshaking,
    // still surfacing the deadline as its only timer).
    assert!(server.is_handshaking());
    assert!(!server.is_terminal());
    server.pump_out(now).expect("pre-deadline pump is a no-op");
    assert!(
      server.is_handshaking(),
      "still handshaking before the deadline"
    );
    assert_eq!(
      server.poll_timeout(),
      Some(deadline),
      "a Handshaking bridge surfaces its accept deadline as the only timer"
    );

    // At the deadline `pump_out` terminalizes the stalled label exchange.
    let res = server.pump_out(deadline);
    assert!(
      res.is_err(),
      "a stalled label exchange at its deadline fails"
    );
    assert!(
      server.is_terminal(),
      "the bridge is terminal after the deadline elapses"
    );
    assert!(
      server.stream_is_none(),
      "no Stream is minted on a label-exchange timeout"
    );
    assert!(
      matches!(
        server.phase_ref(),
        StreamPhase::Established(BridgePhase::Failed(BridgeFailure::Timeout))
      ),
      "the timeout maps to Failed(Timeout), got {:?}",
      phase_label(server.phase_ref())
    );
    assert!(
      !server.is_handshaking(),
      "a timed-out bridge is no longer handshaking"
    );
    // A terminal bridge contributes no deadline (it is reaped this tick).
    assert_eq!(server.poll_timeout(), None);

    // `drain_then_reap` on a no-`Stream` bridge is a clean no-op — no FSM
    // lifecycle notice is owed for a label exchange that never minted a `Stream`.
    let mut ep: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
    server.drain_then_reap(&mut ep, deadline);
    assert!(
      ep.poll_event().is_none(),
      "no endpoint events on the label-timeout path"
    );
  }

  /// (b''') A coalesced read carrying the dialer's `[label]` prefix immediately
  /// followed by a SMALL first app frame, delivered to a server bridge that is
  /// STILL `Handshaking` (no `Stream` yet) in ONE `handle_transport_data` call —
  /// with NO further transport read after. `RawRecords` settles the label AND
  /// surfaces the trailing request as inbound plaintext in that one pass (no
  /// backpressure, so NO retained tail). After promote + `replay_pending`, the
  /// coordinator-equivalent post-mint pump MUST drain that buffered plaintext
  /// into the freshly-promoted `Stream` (the frame decodes) — even though there
  /// is no retained tail. Analog of the TLS
  /// `coalesced_handshake_final_plus_small_frame_and_close_notify_pre_promotion_drains`.
  #[test]
  fn coalesced_label_plus_small_frame_pre_promotion_drains() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));

    // Mint + promote the CLIENT side (the dialer) with a SMALL one-way user
    // message, then pump it so the client's label prefix + the small app frame
    // are produced together (a one-way message half-closes once its request is
    // sent, so the client also owes its FIN).
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7600)));
    let payload = Bytes::from_static(b"small-coalesced-first-frame");
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);
    client.pump_out(now).expect("client pumps its request");
    assert!(
      client.fin_owed(),
      "a one-way user message half-closes after its request — the FIN is owed"
    );

    // COALESCE the client's label prefix + the whole small app frame into ONE
    // buffer (a single TCP read on the server).
    let mut coalesced = Vec::new();
    client.poll_transport_transmit(&mut coalesced);
    assert!(
      coalesced.len() > 2,
      "the coalesced [label][small frame] carries both, got {}",
      coalesced.len()
    );

    // Deliver the WHOLE coalesced buffer to the still-`Handshaking` server in
    // ONE call. `RawRecords` settles the label, surfaces the small request as
    // inbound plaintext, and (no backpressure) retains NO tail.
    server
      .handle_transport_data(&coalesced, now)
      .expect("the coalesced label + small frame is accepted");
    assert!(
      !server.is_handshaking(),
      "the label settled inside the coalesced read"
    );
    assert!(
      server.pending_inbound_is_empty(),
      "the coalesced read is fully consumed in one pass — NO retained tail"
    );

    // Coordinator-equivalent: mint + promote the inbound `Stream`, then drive
    // the post-mint pump's `replay_pending` exactly as `pump_bridges` does. With
    // NO retained tail, the replay must STILL drain the buffered surfaced
    // plaintext into the just-promoted `Stream`, or the small frame never
    // reaches the FSM.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7600)));
    let s_stream = ep_s.accept_stream(addr(7600), now);
    server.promote(s_stream);
    server
      .replay_pending(now)
      .expect("post-promote replay drains the buffered small frame");

    // Finish: the client's FIN drives the server's recv-half close, and with a
    // one-way inbound user message reaching `Done` the bridge converges to
    // terminal so the D1 reap can run.
    server.drain_payload_only(&mut ep_s, now);
    let mut c_fin = false;
    let mut s_fin = false;
    for _ in 0..64 {
      client.pump_out(now).ok();
      server.pump_out(now).ok();
      let moved = shuttle(&mut client, &mut server, &mut c_fin, &mut s_fin, now);
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
      server.is_terminal(),
      "the recv-half FIN fired and the one-way exchange reaped, got {:?}",
      phase_label(server.phase_ref())
    );

    server.drain_then_reap(&mut ep_s, now);
    let mut got = None;
    while let Some(ev) = ep_s.poll_event() {
      if let Event::UserPacket(p) = ev {
        let (_, data, _) = p.into_parts();
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the small first frame coalesced with the label reached the Stream and \
       decoded intact (drained post-promotion with no retained tail)"
    );
  }

  /// A coalesced `[label||first request]` followed by a same-tick out-of-band
  /// TCP FIN, both observed BEFORE the bridge has been promoted (no `Stream`
  /// yet), MUST terminalize the acceptor on the SAME tick as the promote — not
  /// stall to the accept deadline.
  ///
  /// Without [`StreamBridge::pending_eof`] the FIN signal is dropped: the
  /// empty-slice second feed reaches a still-pre-`Stream` bridge whose
  /// pre-`Stream` intake is a no-op on empty input, and the post-promote
  /// [`StreamBridge::replay_pending`] passes `eof = false` to
  /// `pump_in_established`, so the recv-half FIN never fires. The latch
  /// captures the out-of-band EOF across the promote boundary so the
  /// recv-half retirement (or, for an `OutboundAwaitingResponse` peer, the
  /// truncation path) runs the same tick the `Stream` is minted.
  ///
  /// The TCP analog of TLS's in-band `close_notify` (latched in
  /// `TlsRecords::peer_has_closed()`): plain TCP has no in-band close signal,
  /// so the bridge holds the latch instead of the records layer, keeping
  /// [`super::records::RawRecords`] strictly transport-agnostic.
  #[test]
  fn coalesced_label_plus_small_frame_with_eof_pre_promotion_terminalizes() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));

    // Mint + promote the CLIENT and pump a one-way user-message + FIN, so the
    // client's outbound buffer carries `[label||frame]` and the client owes
    // its FIN.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7700)));
    let payload = Bytes::from_static(b"coalesced-with-eof");
    let sid = ep_c.start_user_message(addr(7000), payload.clone(), now);
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound stream");
    client.promote(c_stream);
    client.pump_out(now).expect("client pumps its request");
    assert!(
      client.fin_owed(),
      "a one-way user-message half-closes after its request — the FIN is owed"
    );

    // Coalesce the client's `[label||frame]` into one buffer.
    let mut coalesced = Vec::new();
    client.poll_transport_transmit(&mut coalesced);
    assert!(coalesced.len() > 2);

    // Deliver the coalesced bytes then the same-tick FIN to the
    // still-`Handshaking` server, exactly as the coordinator splits a
    // `(bytes, eof=true)` driver call into a bytes feed then an empty-slice
    // EOF anchor.
    server
      .handle_transport_data(&coalesced, now)
      .expect("the coalesced label + small frame is accepted");
    assert!(!server.is_handshaking());
    // The empty-slice EOF reaches a still-pre-`Stream` server (the mint runs
    // in `service_handshake_completions`); the latch carries it across the
    // promote boundary.
    server
      .handle_transport_data(&[], now)
      .expect("the pre-promote empty-slice EOF is a no-op intake (and latches)");
    assert!(
      server.pending_eof(),
      "pre-promote out-of-band FIN latched on the bridge"
    );

    // Mint + promote the server `Stream`, then run the post-mint replay drain
    // exactly as the coordinator's `pump_bridges` does. The replay seeds
    // `eof = pending_eof = true`, so the recv-half FIN fires for the one-way
    // user-message (its FSM is `Done` after the request decodes) and the
    // bridge converges to `BothClosed` without waiting for any later
    // transport read.
    let mut ep_s: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7700)));
    let s_stream = ep_s.accept_stream(addr(7700), now);
    server.promote(s_stream);
    server
      .replay_pending(now)
      .expect("post-promote replay drains the buffered frame and honors the latched EOF");

    // The recv-half FIN fired on the replay → `RecvClosed`; `pump_out` then
    // observes the inner FSM `Done` (one-way user-message) and retires the
    // send half → `BothClosed`. Same tick, no further transport reads.
    server
      .pump_out(now)
      .expect("server pump retires its send half");
    assert!(
      matches!(
        server.phase_ref(),
        StreamPhase::Established(BridgePhase::BothClosed)
      ),
      "the latched pre-promote FIN drove RecvClosed → BothClosed this tick, \
       got {:?}",
      phase_label(server.phase_ref())
    );
    assert!(server.is_terminal());

    // The decoded `UserData` still surfaces on the D1 reap (the recv-half
    // FIN authorized the dispatched event), so the exchange completes
    // successfully — not silently dropped.
    server.drain_then_reap(&mut ep_s, now);
    let mut got = None;
    while let Some(ev) = ep_s.poll_event() {
      if let Event::UserPacket(p) = ev {
        let (_, data, _) = p.into_parts();
        got = Some(data);
      }
    }
    assert_eq!(
      got.as_deref(),
      Some(payload.as_ref()),
      "the coalesced first frame still decoded; the latched EOF only governs \
       the recv-half close, not the payload"
    );
  }

  /// (e) Truncation: a TCP `read == 0` (zero-length `handle_transport_data`)
  /// mid-frame feeds `Stream::handle_data(&[])`, which the FSM rejects with
  /// `StreamError::PeerClosed` for a non-terminal awaiting phase. The bridge
  /// transitions to `Failed(Decode)` and is terminal. Analog of the TLS
  /// `truncation_read_zero_mid_frame_fails_peer_closed`, with the close already
  /// being the FIN (eof).
  #[test]
  fn truncation_read_zero_mid_frame_fails_peer_closed() {
    let now = Instant::now();
    let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
    complete_label_exchange(&mut client, &mut server, now);

    // Outbound client awaiting a response: a reliable ping fallback. After the
    // request is pumped, the inner FSM is `OutboundAwaitingResponse` — a
    // premature peer close is `PeerClosed`.
    let mut ep_c: Endpoint<SmolStr, SocketAddr> =
      Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7200)));
    let sid = ep_c.start_reliable_ping(
      SmolStr::new("srv"),
      addr(7000),
      7,
      now + Duration::from_secs(5),
    );
    let c_stream = ep_c
      .dial_succeeded(sid, now)
      .expect("dial_succeeded mints the outbound ping stream");
    client.promote(c_stream);

    // Pump the request out so the FSM advances to OutboundAwaitingResponse.
    client.pump_out(now).ok();
    assert!(
      !client.is_terminal(),
      "client is mid-exchange awaiting the ack"
    );

    // TCP read == 0 before the ack arrives: truncation.
    let res = client.handle_transport_data(&[], now);
    assert!(res.is_err(), "a mid-frame read==0 fails the bridge");
    assert!(
      matches!(
        client.phase_ref(),
        StreamPhase::Established(BridgePhase::Failed(BridgeFailure::Decode))
      ),
      "truncation maps to Failed(Decode), got {:?}",
      phase_label(client.phase_ref())
    );
    assert!(client.is_terminal(), "a truncated exchange is terminal");
    assert!(
      matches!(client.stream_is_failed(), Some(StreamError::PeerClosed)),
      "the inner FSM failed with PeerClosed"
    );

    // The unused `server` still proves the label settled.
    assert!(!server.is_handshaking());
  }

  #[cfg(feature = "compression-lz4")]
  #[test]
  fn stream_reliable_unit_accumulation_roundtrips() {
    use crate::{CompressAlgorithm, CompressionOptions, encode_reliable_unit, take_reliable_unit};
    let opts = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(8);
    let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
    let unit = encode_reliable_unit(&opts, &framed);
    // The receiver accumulates the unit and drains one complete frame.
    let mut accum = unit.clone();
    let (back, consumed) = take_reliable_unit(&accum, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("a complete unit is present");
    accum.drain(..consumed);
    assert_eq!(back, framed);
    assert!(accum.is_empty(), "the whole unit was consumed");
  }

  #[cfg(feature = "compression-lz4")]
  #[test]
  fn stream_reliable_unit_split_across_two_reads_buffers_then_completes() {
    // REGRESSION TEST for the split-wrapper bug: a byte stream does not
    // preserve write/read boundaries, so a single compressed unit may arrive
    // in two reads. The accumulation buffer must hold the first partial read
    // (no frame yet) and complete on the second, decompressing to the
    // original. Feeding the unit whole would have decoded fine; the bug was
    // that a SPLIT unit tore the exchange down.
    use crate::{CompressAlgorithm, CompressionOptions, encode_reliable_unit, take_reliable_unit};
    let opts = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(8);
    let framed = b"the quick brown fox jumps over the lazy dog".repeat(32);
    let unit = encode_reliable_unit(&opts, &framed);
    assert!(unit.len() > 4, "unit is large enough to split");
    let split = unit.len() / 2;

    let mut accum: Vec<u8> = Vec::new();
    // First read: the front half. No complete unit yet — buffer it.
    accum.extend_from_slice(&unit[..split]);
    assert!(
      take_reliable_unit(&accum, 16 * 1024 * 1024)
        .expect("a partial unit is not an error")
        .is_none(),
      "the first partial read must buffer, not decode"
    );
    // Second read: the remainder. Now a complete unit drains.
    accum.extend_from_slice(&unit[split..]);
    let (back, consumed) = take_reliable_unit(&accum, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("the unit is complete after the second read");
    accum.drain(..consumed);
    assert_eq!(back, framed, "the split unit decompresses to the original");
    assert!(accum.is_empty());
  }

  #[test]
  fn stream_reliable_unit_disabled_is_byte_identical() {
    // Disabled compression: the unit payload is the framed bytes verbatim,
    // and the round-trip is byte-identical end to end (no wrapper).
    use crate::{CompressionOptions, encode_reliable_unit, take_reliable_unit};
    let opts = CompressionOptions::new();
    let framed = b"plain reliable frame bytes that are not compressed".to_vec();
    let unit = encode_reliable_unit(&opts, &framed);
    let (back, consumed) = take_reliable_unit(&unit, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("a complete unit");
    assert_eq!(back, framed);
    assert_eq!(consumed, unit.len());
  }

  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn stream_reliable_unit_encrypted_roundtrip() {
    use crate::{
      EncryptionOptions, Keyring, SecretKey, encode_reliable_unit_with_encryption,
      take_reliable_unit_with_encryption,
    };
    let comp = crate::CompressionOptions::new();
    let enc = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
    let framed = b"reliable encrypted payload".to_vec();
    let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
    let (back, _consumed) = take_reliable_unit_with_encryption(&unit, &enc, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("complete unit");
    assert_eq!(back, framed);
  }

  #[cfg(feature = "encryption-aes-gcm")]
  #[test]
  fn stream_bridge_reliable_skips_encryption_when_is_secure() {
    // RawRecords is plain TCP — it never claims confidentiality, so the
    // bridge MUST apply the configured encryption on the reliable path. The
    // TLS-side mirror lives in tls/bridge.rs (the bridge force-disables the
    // EncryptionOptions on a TLS record layer, because TLS already encrypts).
    use crate::streams::transport::StreamTransport;
    assert!(!RawRecords::is_secure(), "RawRecords is plain TCP");
  }

  #[cfg(all(feature = "compression-lz4", feature = "encryption-aes-gcm"))]
  #[test]
  fn stream_reliable_unit_encrypted_then_compressed_roundtrip() {
    use crate::{
      CompressAlgorithm, CompressionOptions, EncryptionOptions, Keyring, SecretKey,
      encode_reliable_unit_with_encryption, take_reliable_unit_with_encryption,
    };
    let comp = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(8);
    let enc = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x99; 32])));
    let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
    let unit = encode_reliable_unit_with_encryption(&comp, &enc, &framed).expect("encode");
    let (back, _consumed) = take_reliable_unit_with_encryption(&unit, &enc, 16 * 1024 * 1024)
      .expect("decode ok")
      .expect("complete unit");
    assert_eq!(back, framed);
  }
}
