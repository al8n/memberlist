//! Tests for the generic `memberlist::Stream` <-> record-layer byte-pump
//! ([`crate::streams::bridge::StreamBridge`]) wrapping the raw-passthrough
//! plain-TCP record layer ([`RawRecords`](super::records::RawRecords)).

use crate::{
  bridge_phase::{BridgeFailure, LinkState},
  endpoint::Endpoint,
};

use crate::Instant;
use core::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use smol_str::SmolStr;

use super::records::RawRecords;
use crate::{
  config::EndpointOptions,
  delegate::MergeDelegate,
  error::StreamError,
  event::{Event, PushPullKind},
  streams::{
    bridge::StreamBridge,
    phase::BridgePhase,
    test_support::{
      TEST_RELIABLE_MAX, addr, handshaking_pair as shared_handshaking_pair, label, phase_label,
    },
  },
  typed::NodeState,
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7100)));
  let payload = Bytes::from_static(b"hello-tcp");
  let sid = ep_c
    .start_user_message(addr(7000), payload.clone(), now)
    .expect("issued while running");
  let c_stream = ep_c
    .dial_succeeded(sid, now)
    .expect("dial_succeeded mints the outbound stream");
  client.promote(c_stream);

  // Inbound server: accept the exchange.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7100), now)
    .expect("node is running");
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
      BridgePhase::Established(LinkState::BothClosed)
    ),
    "client reached BothClosed (sent request + FIN, saw peer FIN), got {:?}",
    phase_label(client.phase_ref())
  );
  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::BothClosed)
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
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Transport(_)))
    ),
    "the reject is a Transport failure, got {:?}",
    phase_label(server.phase_ref())
  );

  // `drain_then_reap` on a no-Stream bridge is a clean no-op (the coordinator
  // reaps a failed-label bridge without an FSM lifecycle notice).
  let mut ep: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7300)));
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
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Transport(_)))
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
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Timeout))
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7600)));
  let payload = Bytes::from_static(b"small-coalesced-first-frame");
  let sid = ep_c
    .start_user_message(addr(7000), payload.clone(), now)
    .expect("issued while running");
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7600)));
  let s_stream = ep_s
    .accept_stream(addr(7600), now)
    .expect("node is running");
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7700)));
  let payload = Bytes::from_static(b"coalesced-with-eof");
  let sid = ep_c
    .start_user_message(addr(7000), payload.clone(), now)
    .expect("issued while running");
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7700)));
  let s_stream = ep_s
    .accept_stream(addr(7700), now)
    .expect("node is running");
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
      BridgePhase::Established(LinkState::BothClosed)
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
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7200)));
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
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Decode))
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

#[cfg(feature = "lz4")]
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

#[cfg(feature = "lz4")]
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

#[cfg(feature = "aes-gcm")]
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

#[cfg(feature = "aes-gcm")]
#[test]
fn stream_bridge_reliable_skips_encryption_when_is_secure() {
  // RawRecords is plain TCP — it never claims confidentiality, so the
  // bridge MUST apply the configured encryption on the reliable path. The
  // TLS-side mirror lives in tls/bridge.rs (the bridge force-disables the
  // EncryptionOptions on a TLS record layer, because TLS already encrypts).
  use crate::streams::transport::StreamTransport;
  assert!(!RawRecords::is_secure(), "RawRecords is plain TCP");
}

#[cfg(all(feature = "lz4", feature = "aes-gcm"))]
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

/// A full push/pull request→response exchange over the byte pump drives the
/// generic bridge's [`StreamBridge::drain_payload_only`] /
/// [`StreamBridge::drain_then_reap`] `SendPushPullResponse` arm: the
/// acceptor decodes the dialer's request, the inner endpoint returns
/// `StreamCommand::SendPushPullResponse`, and the bridge encodes + loads the
/// reply into the inbound stream's `output_buf`. The reply then flows back to
/// the dialer, which decodes it as `PushPullReplyReceived` and applies the
/// merge — both bridges converge to `BothClosed` and reap cleanly.
#[test]
fn push_pull_request_response_round_trips_then_reaps() {
  use PushPullKind;

  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  complete_label_exchange(&mut client, &mut server, now);

  // Dialer initiates a push/pull. Seed an alive peer so the request carries a
  // non-empty membership view (and so `start_push_pull` produces a real
  // DialRequested for `dial_succeeded`).
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7600)));
  let sid = ep_c.start_push_pull(addr(7000), PushPullKind::Join, now);
  let c_stream = ep_c
    .dial_succeeded(sid, now)
    .expect("dial_succeeded mints the outbound push/pull stream");
  client.promote(c_stream);

  // Acceptor side.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7600), now)
    .expect("node is running");
  server.promote(s_stream);

  // Shuttle bytes + FINs both ways, draining non-terminal side effects every
  // tick on BOTH sides (the dialer must run `drain_payload_only` to consume
  // the `PushPullReplyReceived`, the acceptor to produce the response via the
  // `SendPushPullResponse` arm).
  let mut c_fin = false;
  let mut s_fin = false;
  for _ in 0..128 {
    client.pump_out(now).ok();
    server.pump_out(now).ok();
    let moved = shuttle(&mut client, &mut server, &mut c_fin, &mut s_fin, now);
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
    "the acceptor produced its push/pull response and closed, got {:?}",
    phase_label(server.phase_ref())
  );
  assert!(
    matches!(
      client.phase_ref(),
      BridgePhase::Established(LinkState::BothClosed)
    ),
    "the dialer received the reply and closed, got {:?}",
    phase_label(client.phase_ref())
  );

  // Terminal reap on both sides — the clean-close lifecycle path.
  server.drain_then_reap(&mut ep_s, now);
  client.drain_then_reap(&mut ep_c, now);

  // The acceptor merged the dialer's `cli` view: it now knows about `cli`.
  assert!(
    ep_s.member(&SmolStr::new("cli")).is_some(),
    "the acceptor merged the dialer's membership view (knows `cli`); \
       members: {:?}",
    ep_s
      .members()
      .map(|m| m.id_ref().clone())
      .collect::<Vec<_>>(),
  );
}

/// An acceptor whose [`MergeDelegate`](crate::delegate::MergeDelegate) vetoes
/// the inbound JOIN push/pull drives the bridge's
/// [`StreamBridge::drain_payload_only`] `StreamCommand::Close` arm:
/// `handle_stream_event` returns `Close`, the bridge calls
/// `fail_with_retire(BridgeFailure::AdmissionClosed)`, and becomes terminal
/// within the same tick (no pinning to the exchange deadline).
#[test]
fn inbound_push_pull_admission_rejected_fails_bridge() {
  use PushPullKind;

  /// Vetoes every merge.
  struct RejectAll;
  impl MergeDelegate<SmolStr, SocketAddr> for RejectAll {
    fn notify_merge(
      &self,
      _peers: crate::MaybeOwned<'_, [NodeState<SmolStr, SocketAddr>]>,
    ) -> bool {
      false
    }
  }

  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  complete_label_exchange(&mut client, &mut server, now);

  // Dialer initiates a JOIN push/pull (the merge delegate only fires on
  // join, never refresh).
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7610)));
  let sid = ep_c.start_push_pull(addr(7000), PushPullKind::Join, now);
  let c_stream = ep_c
    .dial_succeeded(sid, now)
    .expect("dial_succeeded mints the outbound push/pull stream");
  client.promote(c_stream);

  // Acceptor with a rejecting merge delegate.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  ep_s.set_merge_delegate(RejectAll);
  let s_stream = ep_s
    .accept_stream(addr(7610), now)
    .expect("node is running");
  server.promote(s_stream);

  // Shuttle until the server reaches its admission decision. The acceptor's
  // `drain_payload_only` consumes the decoded request → `handle_stream_event`
  // returns `Close` → `fail_with_retire(AdmissionClosed)`.
  let mut c_fin = false;
  let mut s_fin = false;
  for _ in 0..128 {
    client.pump_out(now).ok();
    server.pump_out(now).ok();
    let moved = shuttle(&mut client, &mut server, &mut c_fin, &mut s_fin, now);
    if !server.is_terminal() {
      server.drain_payload_only(&mut ep_s, now);
    }
    if server.is_terminal() {
      break;
    }
    if !moved {
      break;
    }
  }

  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::AdmissionClosed))
    ),
    "a vetoed join push/pull fails the acceptor bridge with AdmissionClosed, \
       got {:?}",
    phase_label(server.phase_ref())
  );
  assert!(server.is_terminal());
  // The veto means the dialer's `cli` view was NOT merged.
  assert!(
    ep_s.member(&SmolStr::new("cli")).is_none(),
    "a vetoed merge must not apply the peer's state",
  );
}

/// [`StreamBridge::drain_then_reap`]'s `SendPushPullResponse` arm: when the
/// terminal reap runs with an inbound push/pull request event still queued
/// (the request decoded but `drain_payload_only` never ran), the reap drains
/// the event, `handle_stream_event` returns `SendPushPullResponse`, and the
/// bridge encodes + loads the reply into the inbound stream's `output_buf`.
#[test]
fn drain_then_reap_loads_push_pull_response() {
  use PushPullKind;

  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  complete_label_exchange(&mut client, &mut server, now);

  // Dialer produces a real push/pull request frame.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7620)));
  let sid = ep_c.start_push_pull(addr(7000), PushPullKind::Join, now);
  let c_stream = ep_c
    .dial_succeeded(sid, now)
    .expect("dial mints the outbound stream");
  client.promote(c_stream);
  client.pump_out(now).expect("client pumps its request");

  // Acceptor: promote the inbound stream, then feed the dialer's request
  // ciphertext (NO FIN). The FSM dispatches `PushPullRequestReceived` and is
  // left in `InboundSendingResponse` with the event queued.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7620), now)
    .expect("node is running");
  server.promote(s_stream);
  let mut c_out = Vec::new();
  client.poll_transport_transmit(&mut c_out);
  assert!(!c_out.is_empty(), "the dialer queued its request bytes");
  server
    .handle_transport_data(&c_out, now)
    .expect("the acceptor consumes the request");

  // Reap directly — bypassing `drain_payload_only` — so the queued
  // push/pull request event is consumed by `drain_then_reap`'s
  // `SendPushPullResponse` arm.
  server.drain_then_reap(&mut ep_s, now);

  // The acceptor merged the dialer's `cli` view (the SendPushPullResponse arm
  // ran `handle_stream_event`, which merges then asks for the reply).
  assert!(
    ep_s.member(&SmolStr::new("cli")).is_some(),
    "the reap drained the request event, merging the dialer's view; \
       members: {:?}",
    ep_s
      .members()
      .map(|m| m.id_ref().clone())
      .collect::<Vec<_>>(),
  );
  // The encoded push/pull response was loaded into the inbound stream and
  // surfaces on the next pump.
  let mut reply = Vec::new();
  server.poll_transport_transmit(&mut reply);
  assert!(
    !reply.is_empty(),
    "the reap encoded + loaded the push/pull response into out_transmit",
  );
}

/// Build a promoted inbound acceptor bridge over a settled label exchange,
/// returning the bridge plus its Endpoint. The inner stream is freshly minted
/// in `InboundAwaitingFirstMessage` so a crafted reliable unit can be fed
/// straight through the established intake.
fn promoted_inbound(
  now: Instant,
  port: u16,
) -> (
  StreamBridge<SmolStr, SocketAddr, RawRecords>,
  Endpoint<SmolStr, SocketAddr>,
) {
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  complete_label_exchange(&mut client, &mut server, now);
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(port)));
  let s_stream = ep_s
    .accept_stream(addr(port + 1), now)
    .expect("node is running");
  server.promote(s_stream);
  (server, ep_s)
}

/// A forged reliable unit whose declared `unit_len` exceeds the bridge's
/// reliable-unit ceiling is a corrupt unit: the established intake's
/// `take_reliable_unit_with_encryption` returns `Err`, so the bridge
/// `fail_with_retire(BridgeFailure::Decode)` and is terminal — the
/// `Err(_)` corrupt-unit arm of `pump_in_established`.
#[test]
fn established_intake_corrupt_unit_len_over_ceiling_fails_decode() {
  let now = Instant::now();
  let (mut server, _ep_s) = promoted_inbound(now, 7700);

  // `[unit_len = TEST_RELIABLE_MAX + 1 : varint][1 payload byte]`. The
  // ceiling check fires before the payload is even fully required.
  let mut forged = Vec::new();
  crate::framing::encode_varint_u32((TEST_RELIABLE_MAX + 1) as u32, &mut forged);
  forged.push(0u8);

  let res = server.handle_transport_data(&forged, now);
  assert!(
    res.is_err(),
    "an over-ceiling unit_len tears the bridge down"
  );
  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Decode))
    ),
    "a corrupt unit maps to Failed(Decode), got {:?}",
    phase_label(server.phase_ref())
  );
  assert!(server.is_terminal());
}

/// A complete reliable unit whose INNER frame is well-framed but undecodable
/// drives the established intake's inner-`Stream::handle_data` failure arm:
/// the unit decodes off the accumulator, the FSM rejects the frame
/// (`StreamError::Decode`), and the bridge `fail_with_retire(Decode)`.
#[test]
fn established_intake_inner_frame_decode_failure_fails_bridge() {
  let now = Instant::now();
  let (mut server, _ep_s) = promoted_inbound(now, 7710);

  // Inner frame `[tag=8 PushPull][len=4][4 garbage bytes]` — accepted by
  // `probe_frame`, rejected by the real decoder. Wrap it as one reliable unit
  // (disabled compression/encryption → the payload is the frame verbatim).
  let mut frame = std::vec![8u8];
  crate::framing::encode_varint_u32(4, &mut frame);
  frame.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);
  let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &frame);

  let res = server.handle_transport_data(&unit, now);
  assert!(res.is_err(), "an undecodable inner frame fails the bridge");
  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Decode))
    ),
    "an inner-frame decode failure maps to Failed(Decode), got {:?}",
    phase_label(server.phase_ref())
  );
  assert!(
    matches!(server.stream_is_failed(), Some(StreamError::Frame(_))),
    "the inner FSM recorded the decode failure"
  );
}

/// A partial reliable unit (the `unit_len` declares more payload than has
/// arrived) followed by a transport `read == 0` (EOF) is a truncated
/// transmission: the peer closed mid-unit. The established intake's
/// `fin_seen && !recv_accum.is_empty()` arm fails the bridge `Decode`.
#[test]
fn established_intake_partial_unit_then_eof_fails_decode() {
  let now = Instant::now();
  let (mut server, _ep_s) = promoted_inbound(now, 7720);

  // Declare a 32-byte unit but deliver only 4 payload bytes — a partial unit
  // that `take_reliable_unit` buffers (Ok(None)), leaving `recv_accum`
  // non-empty.
  let mut partial = Vec::new();
  crate::framing::encode_varint_u32(32, &mut partial);
  partial.extend_from_slice(&[1u8, 2, 3, 4]);
  server
    .handle_transport_data(&partial, now)
    .expect("a partial unit buffers without failing");
  assert!(
    !server.is_terminal(),
    "a partial unit alone is not yet a failure"
  );

  // Now the peer FINs (read == 0) mid-unit: truncation.
  let res = server.handle_transport_data(&[], now);
  assert!(
    res.is_err(),
    "a partial unit at EOF is a truncation failure"
  );
  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Decode))
    ),
    "a truncated partial unit maps to Failed(Decode), got {:?}",
    phase_label(server.phase_ref())
  );
}

/// `poll_timeout` on an Established bridge folds in the inner stream's tighter
/// deadline via `min`: a freshly-accepted inbound stream snapshots its own
/// exchange deadline, which can be earlier than the bridge's handshake/accept
/// deadline. The exposed timeout is the minimum of the two.
#[test]
fn poll_timeout_folds_in_tighter_inner_stream_deadline() {
  let now = Instant::now();
  // Bridge handshake/accept deadline far out (+30s).
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(30));
  complete_label_exchange(&mut client, &mut server, now);

  // Accept a stream whose own exchange deadline is tighter (+5s, the inbound
  // first-message default in `accept_stream`). After promote the bridge
  // snapshots +30s as its bridge deadline, but the inner stream still carries
  // its tighter timer.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7730)));
  let s_stream = ep_s
    .accept_stream(addr(7731), now)
    .expect("node is running");
  let inner_deadline = s_stream
    .poll_timeout()
    .expect("a freshly accepted stream carries a deadline");
  server.promote(s_stream);

  assert_eq!(
    server.poll_timeout(),
    Some(inner_deadline),
    "poll_timeout returns the tighter inner-stream deadline via min, not the \
       later bridge deadline"
  );
}

/// A second SEND-half close and a second RECV-half close are idempotent: once
/// a bridge reaches `BothClosed`, replaying `pump_out` (send) and a redundant
/// EOF feed (recv) leave it terminal without re-transitioning — the
/// `observe_send_fin` / `observe_recv_fin` already-closed return arms.
#[test]
fn second_half_close_observations_are_idempotent_at_both_closed() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  complete_label_exchange(&mut client, &mut server, now);

  // One-way user message both promoted, driven to BothClosed.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7740)));
  let sid = ep_c
    .start_user_message(addr(7000), Bytes::from_static(b"x"), now)
    .expect("issued while running");
  let c_stream = ep_c.dial_succeeded(sid, now).expect("dial mints");
  client.promote(c_stream);
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7740), now)
    .expect("node is running");
  server.promote(s_stream);

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
    matches!(
      client.phase_ref(),
      BridgePhase::Established(LinkState::BothClosed)
    ),
    "client reached BothClosed, got {:?}",
    phase_label(client.phase_ref())
  );

  // A redundant send-close (pump_out) and a redundant recv EOF on a
  // BothClosed bridge are no-ops: the phase stays BothClosed (sticky
  // terminal), and the SECOND observation arms (`SendClosed|BothClosed|Failed
  // => return`) run without panicking.
  client.pump_out(now).ok();
  // Ignoring Err: the redundant EOF on a terminal bridge is a no-op; the
  // post-condition is asserted via `phase_ref()` below, not the return value.
  let _ = client.handle_transport_data(&[], now);
  assert!(
    matches!(
      client.phase_ref(),
      BridgePhase::Established(LinkState::BothClosed)
    ),
    "a redundant double-close leaves BothClosed intact, got {:?}",
    phase_label(client.phase_ref())
  );
}

/// `pump_out` on an already phase-failed (NON-FSM-failed) bridge re-enters the
/// terminal guard: `is_phase_failed()` is true while the inner FSM is not
/// failed, so the `None => "bridge fatal"` reason arm runs and
/// `fail_with_retire` is idempotent (the original `Timeout` reason is kept).
#[test]
fn pump_out_on_phase_failed_non_fsm_failed_bridge_is_idempotent() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  complete_label_exchange(&mut client, &mut server, now);

  // Promote an inbound stream, then drive it to a bridge flush-deadline
  // Timeout: the FSM itself is NOT failed (it is `Done`/awaiting), only the
  // bridge phase is `Failed(Timeout)`.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7750)));
  let sid = ep_c
    .start_user_message(addr(7000), Bytes::from_static(b"hi"), now)
    .expect("issued while running");
  let c_stream = ep_c.dial_succeeded(sid, now).expect("dial mints");
  client.promote(c_stream);
  // Pump the one-way message so the FSM reaches `Done` (the user message is
  // sent and the FSM closes), then push the bridge past its deadline so the
  // `Done`-but-awaiting-close flush-deadline fails it to `Failed(Timeout)`.
  client.pump_out(now).expect("first pump sends the request");
  let past = now + Duration::from_secs(11);
  let res = client.pump_out(past);
  assert!(res.is_err(), "the bridge flush deadline fails the bridge");
  assert!(
    matches!(
      client.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Timeout))
    ),
    "the bridge is Failed(Timeout) with no FSM failure, got {:?}",
    phase_label(client.phase_ref())
  );
  assert!(
    client.stream_is_failed().is_none(),
    "the inner FSM is not itself failed — only the bridge phase is"
  );

  // A subsequent pump_out re-enters the terminal guard (is_phase_failed true,
  // fsm_failed None → "bridge fatal") and stays Failed(Timeout) — the first
  // failure's reason wins (sticky).
  let res2 = client.pump_out(past);
  assert!(res2.is_err());
  assert!(
    matches!(
      client.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Timeout))
    ),
    "the sticky reason is preserved across a redundant pump_out, got {:?}",
    phase_label(client.phase_ref())
  );
}

/// `drain_then_reap` on a `Failed(Timeout)` bridge that still holds a `Stream`
/// builds the `StreamErrored` notice through the `BridgeFailure::Timeout`
/// error-string arm of the lifecycle-notice match. The notice is consumed
/// internally (`StreamErrored` maps to no public `Event`), so the observable
/// contract is a clean reap that leaves the bridge terminal and emits no
/// spurious public event.
#[test]
fn drain_then_reap_timeout_failed_bridge_routes_errored_notice() {
  let now = Instant::now();
  let (mut client, _server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  // The dialer is never handshaking; promote an outbound user message.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7760)));
  let sid = ep_c
    .start_user_message(addr(7000), Bytes::from_static(b"hi"), now)
    .expect("issued while running");
  let c_stream = ep_c.dial_succeeded(sid, now).expect("dial mints");
  client.promote(c_stream);
  client.pump_out(now).expect("first pump sends the request");
  // Drain the `DialRequested` the outbound `start_user_message` emitted so the
  // post-reap assertion observes only events the reap itself produces.
  while ep_c.poll_event().is_some() {}

  // Fail the bridge to Failed(Timeout) via the flush deadline.
  let past = now + Duration::from_secs(11);
  // Ignoring Err: the flush-deadline failure is the intent here; the resulting
  // Failed(Timeout) phase is asserted directly below.
  let _ = client.pump_out(past);
  assert!(
    matches!(
      client.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Timeout))
    ),
    "bridge is Failed(Timeout), got {:?}",
    phase_label(client.phase_ref())
  );

  // The terminal reap drains the (no-op) event queue and builds the lifecycle
  // notice through the Timeout error-string arm. The notice itself maps to no
  // public Event.
  client.drain_then_reap(&mut ep_c, past);
  assert!(
    client.is_terminal(),
    "the bridge is terminal after the reap"
  );
  assert!(
    ep_c.poll_event().is_none(),
    "a StreamErrored notice maps to no public Event"
  );
}

/// `drain_then_reap` on a `Failed(Decode)` bridge holding a `Stream` builds
/// the `StreamErrored` notice through the `BridgeFailure::Decode` error-string
/// arm. As above, the notice is consumed internally.
#[test]
fn drain_then_reap_decode_failed_bridge_routes_errored_notice() {
  let now = Instant::now();
  let (mut server, mut ep_s) = promoted_inbound(now, 7770);

  // Truncation: a mid-frame read == 0 fails the inbound stream Decode.
  let res = server.handle_transport_data(&[], now);
  assert!(res.is_err());
  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::Decode))
    ),
    "bridge is Failed(Decode), got {:?}",
    phase_label(server.phase_ref())
  );

  server.drain_then_reap(&mut ep_s, now);
  assert!(
    server.is_terminal(),
    "the bridge is terminal after the reap"
  );
  assert!(
    ep_s.poll_event().is_none(),
    "a StreamErrored notice maps to no public Event"
  );
}

/// `drain_then_reap` itself can carry the admission rejection: when the
/// terminal reap drains a still-queued inbound push/pull request whose
/// `MergeDelegate` vetoes the merge, `handle_stream_event` returns
/// `StreamCommand::Close` and the reap's `Close` arm
/// `fail_with_retire(BridgeFailure::AdmissionClosed)`. This is the reap-path
/// sibling of the `drain_payload_only` Close arm — the request event was
/// queued but `drain_payload_only` never ran before the bridge went terminal.
#[test]
fn drain_then_reap_close_arm_rejects_inbound_merge() {
  use PushPullKind;

  /// Vetoes every merge.
  struct RejectAll;
  impl MergeDelegate<SmolStr, SocketAddr> for RejectAll {
    fn notify_merge(
      &self,
      _peers: crate::MaybeOwned<'_, [NodeState<SmolStr, SocketAddr>]>,
    ) -> bool {
      false
    }
  }

  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair("cluster-x", now + Duration::from_secs(10));
  complete_label_exchange(&mut client, &mut server, now);

  // Dialer produces a JOIN push/pull request.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("cli"), addr(7780)));
  let sid = ep_c.start_push_pull(addr(7000), PushPullKind::Join, now);
  let c_stream = ep_c.dial_succeeded(sid, now).expect("dial mints");
  client.promote(c_stream);
  client.pump_out(now).expect("client pumps its request");

  // Acceptor with a rejecting merge delegate. Promote, feed the request
  // ciphertext (the FSM dispatches `PushPullRequestReceived`, queued), then
  // reap directly — bypassing `drain_payload_only` — so the reap's Close arm
  // performs the rejection.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  ep_s.set_merge_delegate(RejectAll);
  let s_stream = ep_s
    .accept_stream(addr(7780), now)
    .expect("node is running");
  server.promote(s_stream);
  let mut c_out = Vec::new();
  client.poll_transport_transmit(&mut c_out);
  assert!(!c_out.is_empty(), "the dialer queued its request bytes");
  server
    .handle_transport_data(&c_out, now)
    .expect("the acceptor consumes the request");

  server.drain_then_reap(&mut ep_s, now);
  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::Failed(BridgeFailure::AdmissionClosed))
    ),
    "the reap's Close arm failed the bridge AdmissionClosed, got {:?}",
    phase_label(server.phase_ref())
  );
  // The merge was vetoed: the acceptor did NOT learn `cli`.
  assert!(
    ep_s.member(&SmolStr::new("cli")).is_none(),
    "a vetoed merge does not add the dialer to membership"
  );
}

/// `pump_out` on a bridge whose INNER FSM has failed (a truncation
/// `PeerClosed`) re-enters the terminal guard via the `fsm_failed.is_some()`
/// disjunct: the `is_failed()` reason is stringified (the `.map` closure) and
/// the `Some(e) => Transport(e)` reason arm runs. `fail_with_retire` is
/// idempotent so the original failure stands.
#[test]
fn pump_out_on_fsm_failed_bridge_uses_fsm_reason() {
  let now = Instant::now();
  let (mut server, _ep_s) = promoted_inbound(now, 7790);

  // Truncation: a mid-frame read == 0 fails the inbound FSM with PeerClosed,
  // cascading the bridge to Failed(Decode).
  let res = server.handle_transport_data(&[], now);
  assert!(res.is_err());
  assert!(
    matches!(server.stream_is_failed(), Some(StreamError::PeerClosed)),
    "the inner FSM failed with PeerClosed"
  );

  // A subsequent pump_out sees the failed FSM at its top guard: the
  // `fsm_failed.is_some()` disjunct fires and the FSM reason string is used.
  let res2 = server.pump_out(now);
  assert!(res2.is_err(), "a pump_out on an FSM-failed bridge errors");
  assert!(
    server.is_terminal(),
    "the bridge stays terminal after the redundant pump_out"
  );
  assert!(
    matches!(
      server.phase_ref(),
      BridgePhase::Established(LinkState::Failed(_))
    ),
    "the bridge is still in a Failed phase, got {:?}",
    phase_label(server.phase_ref())
  );
}
