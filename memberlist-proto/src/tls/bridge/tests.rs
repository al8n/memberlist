use super::*;
use crate::Instant;
use core::{net::SocketAddr, time::Duration};
use std::sync::Arc;

use bytes::Bytes;
use rustls::pki_types::ServerName;
use smol_str::SmolStr;

use super::super::records::TlsRecords;
use crate::{
  config::EndpointOptions,
  error::StreamError,
  event::Event,
  streams::{
    bridge::StreamBridge,
    phase::StreamPhase,
    test_support::{addr, handshaking_pair as shared_handshaking_pair, phase_label},
  },
  tls::options::tests::{test_client, test_server},
};

/// Build a `Handshaking` client/server bridge pair sharing the accept-any
/// test configs (real rustls handshake, deterministic — no test-only crypto
/// hook; the determinism is the virtual clock).
fn handshaking_pair(
  deadline: Instant,
) -> (
  StreamBridge<SmolStr, SocketAddr, TlsRecords>,
  StreamBridge<SmolStr, SocketAddr, TlsRecords>,
) {
  shared_handshaking_pair(
    deadline,
    || {
      TlsRecords::client(
        Arc::new(test_client()),
        ServerName::try_from("localhost").unwrap(),
      )
      .unwrap()
    },
    || TlsRecords::server(Arc::new(test_server())).unwrap(),
  )
}

/// Shuttle ciphertext both ways once: client out -> server in, server out ->
/// client in. Returns `true` if either side produced bytes this round.
fn shuttle(
  client: &mut StreamBridge<SmolStr, SocketAddr, TlsRecords>,
  server: &mut StreamBridge<SmolStr, SocketAddr, TlsRecords>,
  now: Instant,
) -> bool {
  let mut c_out = Vec::new();
  client.poll_transport_transmit(&mut c_out);
  if !c_out.is_empty() {
    server.handle_transport_data(&c_out, now).unwrap();
  }
  let mut s_out = Vec::new();
  server.poll_transport_transmit(&mut s_out);
  if !s_out.is_empty() {
    client.handle_transport_data(&s_out, now).unwrap();
  }
  !c_out.is_empty() || !s_out.is_empty()
}

/// Run the handshake pump to completion on both sides.
fn complete_handshake(
  client: &mut StreamBridge<SmolStr, SocketAddr, TlsRecords>,
  server: &mut StreamBridge<SmolStr, SocketAddr, TlsRecords>,
  now: Instant,
) {
  for _ in 0..64 {
    if !shuttle(client, server, now) {
      break;
    }
  }
  assert!(!client.is_handshaking() && !server.is_handshaking());
}

/// (a) The handshake pump shuttles ciphertext through both bridges until
/// neither `is_handshaking()`. This is the pre-`Stream` window the
/// coordinator runs before minting the `Stream`.
#[test]
fn handshake_pump_completes_both_sides() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
  assert!(client.is_handshaking() && server.is_handshaking());
  for _ in 0..64 {
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      server.handle_transport_data(&c_out, now).unwrap();
    }
    let mut s_out = Vec::new();
    server.poll_transport_transmit(&mut s_out);
    if !s_out.is_empty() {
      client.handle_transport_data(&s_out, now).unwrap();
    }
    if c_out.is_empty() && s_out.is_empty() {
      break;
    }
  }
  assert!(!client.is_handshaking(), "client handshake completed");
  assert!(!server.is_handshaking(), "server handshake completed");
}

/// (a) `promote` installs the freshly-minted `Stream`, snapshots its exchange
/// deadline, and transitions `Handshaking → Established(Active)`.
#[test]
fn promote_installs_stream_and_enters_established() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
  complete_handshake(&mut client, &mut server, now);

  let mut ep: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let stream = ep.accept_stream(addr(7001), now).expect("node is running");
  let want_deadline = stream
    .poll_timeout()
    .expect("inbound stream has a deadline");

  assert!(server.stream_is_none());
  server.promote(stream);
  assert!(server.stream_is_some(), "promote installed the Stream");
  assert!(
    matches!(
      server.phase_ref(),
      StreamPhase::Established(BridgePhase::Active)
    ),
    "promote entered Established(Active)"
  );
  assert_eq!(
    server.deadline(),
    want_deadline,
    "promote snapshotted the stream's exchange deadline"
  );
  assert!(
    !server.is_handshaking(),
    "an Established bridge is not handshaking"
  );
}

/// (b) A one-way `UserData` frame round-trips through the byte pump:
/// outbound client `Stream::poll_transmit` -> `write_plaintext` ->
/// ciphertext -> server `handle_transport_data` -> `read_plaintext` ->
/// inbound `Stream::handle_data`. (c) close_notify both ways drives both
/// bridges to `BothClosed`, then the terminal D1 reap surfaces the decoded
/// `UserData` as `Event::UserPacket` on the server Endpoint.
#[test]
fn frame_round_trips_then_clean_close_reaps() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
  complete_handshake(&mut client, &mut server, now);

  // Outbound client: a one-way reliable user message.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7100)));
  let payload = Bytes::from_static(b"hello-tls");
  let sid = ep_c
    .start_user_message(addr(7000), payload.clone(), now)
    .expect("issued while running");
  let c_stream = ep_c
    .dial_succeeded(sid, now)
    .expect("dial_succeeded mints the outbound stream");
  client.promote(c_stream);

  // Inbound server: accept the exchange.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7100), now)
    .expect("node is running");
  server.promote(s_stream);

  // Pump the request out of the client and into the server, shuttling
  // ciphertext (incl. the eventual close_notify) until both bridges reap.
  for _ in 0..64 {
    client.pump_out(now).ok();
    server.pump_out(now).ok();
    let moved = shuttle(&mut client, &mut server, now);
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
  }

  assert!(
    matches!(
      client.phase_ref(),
      StreamPhase::Established(BridgePhase::BothClosed)
    ),
    "client reached BothClosed (sent request + close_notify, saw peer close_notify), got {:?}",
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

/// (b') A `UserData` frame whose plaintext exceeds rustls's 16 KiB
/// received-plaintext limit, delivered to the server as ONE coalesced
/// `handle_transport_data` call, must NOT panic: the bridge interleaves
/// bounded record intake with plaintext draining (respecting rustls
/// backpressure) so the whole frame decrypts and decodes into the `Stream`.
/// Feeding the whole ciphertext in one shot would instead drive the record
/// layer's `read_tls` into a buffer-full `Err`; the interleave keeps each
/// intake step within the receive limit.
#[test]
fn coalesced_large_frame_decodes_without_panic() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
  complete_handshake(&mut client, &mut server, now);

  // Outbound client: a one-way reliable user message whose payload (48 KiB)
  // far exceeds the 16 KiB received-plaintext limit but is well under the
  // 64 MiB max_stream_frame_size — a VALID large frame.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7400)));
  let payload = Bytes::from((0..48 * 1024).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
  let sid = ep_c
    .start_user_message(addr(7000), payload.clone(), now)
    .expect("issued while running");
  let c_stream = ep_c
    .dial_succeeded(sid, now)
    .expect("dial_succeeded mints the outbound stream");
  client.promote(c_stream);

  // Inbound server: accept the exchange.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7400), now)
    .expect("node is running");
  server.promote(s_stream);

  // Pump the request out of the client and COALESCE every ciphertext byte the
  // client emits into ONE buffer (the single-TCP-read worst case rustls
  // backpressure exposes), rather than shuttling chunk-by-chunk.
  let mut coalesced = Vec::new();
  for _ in 0..64 {
    client.pump_out(now).ok();
    let before = coalesced.len();
    client.poll_transport_transmit(&mut coalesced);
    if coalesced.len() == before {
      break;
    }
  }
  assert!(
    coalesced.len() > 16 * 1024,
    "the coalesced request ciphertext exceeds the received-plaintext limit, got {}",
    coalesced.len()
  );

  // Deliver the WHOLE coalesced request in ONE call. This is the path that
  // panicked before the interleave fix.
  server
    .handle_transport_data(&coalesced, now)
    .expect("a coalesced large frame is accepted, not a transport failure");

  // The server decoded the large UserData and queued it for D1 drain; finish
  // the close handshake so the terminal reap surfaces it.
  server.drain_payload_only(&mut ep_s, now);
  for _ in 0..64 {
    client.pump_out(now).ok();
    server.pump_out(now).ok();
    let moved = shuttle(&mut client, &mut server, now);
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
    "the full >16 KiB UserData frame decoded intact from a single coalesced read"
  );
}

/// (b'') A coalesced read carrying the dialer's FINAL handshake flight
/// (TLS 1.3 client `Finished`) immediately followed by a `UserData` frame
/// whose plaintext exceeds rustls's 16 KiB received-plaintext limit, delivered
/// to a server bridge that is STILL `Handshaking` (no `Stream` yet) in ONE
/// `handle_transport_data` call. The coordinator-equivalent sequence —
/// promote the freshly-minted `Stream` then pump the same tick — must
/// reassemble the WHOLE frame into the `Stream` (decodes intact), not a
/// truncated ~16 KiB prefix.
///
/// This is the pre-promotion coalescing path: `process_new_packets` completes
/// the handshake AND buffers the trailing app records in the SAME call, and
/// the >16 KiB app frame trips rustls backpressure (`Intake::Pending`) before
/// a `Stream` exists to drain into. The unconsumed ciphertext tail must be
/// retained and replayed post-promotion.
#[test]
fn coalesced_handshake_final_plus_large_frame_pre_promotion_reassembles() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

  // Drive the handshake one flight at a time until the CLIENT has completed
  // its handshake (it has sent its `Finished`) but the SERVER has NOT yet
  // consumed that final flight — so the server is still `Handshaking` with no
  // `Stream`. Once the client finishes the loop breaks WITHOUT handing the
  // client's final flight to the server: it is withheld here and coalesced
  // with the large app frame below.
  for _ in 0..64 {
    if !client.is_handshaking() {
      break;
    }
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      server.handle_transport_data(&c_out, now).unwrap();
    }
    let mut s_out = Vec::new();
    server.poll_transport_transmit(&mut s_out);
    if !s_out.is_empty() {
      client.handle_transport_data(&s_out, now).unwrap();
    }
    if c_out.is_empty() && s_out.is_empty() {
      break;
    }
  }
  assert!(
    !client.is_handshaking(),
    "client completed its handshake (sent Finished)"
  );
  assert!(
    server.is_handshaking(),
    "server has NOT yet consumed the client's final flight — still Handshaking"
  );

  // Mint + promote the CLIENT side (the dialer) with a large one-way user
  // message, then pump it so the client's `Finished` + the >16 KiB app frame
  // are produced together.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7500)));
  let payload = Bytes::from((0..48 * 1024).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
  let sid = ep_c
    .start_user_message(addr(7000), payload.clone(), now)
    .expect("issued while running");
  let c_stream = ep_c
    .dial_succeeded(sid, now)
    .expect("dial_succeeded mints the outbound stream");
  client.promote(c_stream);
  client.pump_out(now).expect("client pumps its request");

  // COALESCE the client's withheld `Finished` flight + the whole >16 KiB app
  // frame into ONE buffer (a single TCP read on the server).
  let mut coalesced = Vec::new();
  for _ in 0..64 {
    let before = coalesced.len();
    client.poll_transport_transmit(&mut coalesced);
    if coalesced.len() == before {
      break;
    }
  }
  assert!(
    coalesced.len() > 16 * 1024,
    "the coalesced [final flight][>16 KiB frame] exceeds the received-plaintext limit, got {}",
    coalesced.len()
  );

  // Deliver the WHOLE coalesced buffer to the still-`Handshaking` server in
  // ONE call. `process_new_packets` completes the handshake AND buffers the
  // app records; the >16 KiB frame trips backpressure before a `Stream` exists.
  server
    .handle_transport_data(&coalesced, now)
    .expect("the coalesced final-flight + large frame is accepted");
  assert!(
    !server.is_handshaking(),
    "the server's handshake completed inside the coalesced read"
  );

  // Coordinator-equivalent: mint + promote the inbound `Stream`, then pump the
  // SAME tick. The retained pre-promotion ciphertext tail must replay into the
  // freshly-promoted `Stream` so the WHOLE frame reassembles.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7500), now)
    .expect("node is running");
  server.promote(s_stream);
  // Drive the replay exactly as the coordinator's post-mint `pump_bridges`
  // does: `replay_pending` feeds the retained tail through the established
  // interleave (no synthetic EOF). The full frame must reassemble into the
  // just-promoted `Stream`.
  server
    .replay_pending(now)
    .expect("post-promote replay of the retained tail decodes the frame");

  server.drain_payload_only(&mut ep_s, now);
  for _ in 0..64 {
    client.pump_out(now).ok();
    server.pump_out(now).ok();
    let moved = shuttle(&mut client, &mut server, now);
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
    "the full >16 KiB frame coalesced with the final handshake flight decoded intact"
  );
}

/// (b''') A coalesced read carrying the dialer's FINAL handshake flight
/// (TLS 1.3 client `Finished`), a SMALL first app frame (well under the 16 KiB
/// received-plaintext limit), and the dialer's `close_notify`, delivered to a
/// server bridge that is STILL `Handshaking` (no `Stream` yet) in ONE
/// `handle_transport_data` call — with NO further transport read after.
///
/// Unlike the large-frame sibling, the whole coalesced buffer is consumed by
/// rustls in ONE `read_tls`/`process_new_packets` pass: the small frame never
/// fills the received-plaintext buffer, so NO backpressure occurs and the
/// handshake intake retains NO ciphertext tail (`pending_ciphertext` stays
/// empty). The decrypted small frame is buffered inside `TlsRecords` and
/// `peer_has_closed()` is already latched. After promote + `replay_pending`,
/// the coordinator-equivalent post-mint pump MUST drain that buffered
/// plaintext into the freshly-promoted `Stream` (the frame decodes) AND fire
/// the recv-half close anchor — even though there is no retained tail. Before
/// the fix `replay_pending` no-ops on an empty tail, so the buffered frame is
/// never delivered and the close anchor never fires; with NO further transport
/// read the exchange would stall to its deadline.
#[test]
fn coalesced_handshake_final_plus_small_frame_and_close_notify_pre_promotion_drains() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

  // Drive the handshake one flight at a time until the CLIENT has completed
  // its handshake (sent its `Finished`) but the SERVER has NOT yet consumed
  // that final flight — so the server is still `Handshaking` with no `Stream`.
  // The client's final flight is withheld here and coalesced with the small
  // app frame + `close_notify` below.
  for _ in 0..64 {
    if !client.is_handshaking() {
      break;
    }
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      server.handle_transport_data(&c_out, now).unwrap();
    }
    let mut s_out = Vec::new();
    server.poll_transport_transmit(&mut s_out);
    if !s_out.is_empty() {
      client.handle_transport_data(&s_out, now).unwrap();
    }
    if c_out.is_empty() && s_out.is_empty() {
      break;
    }
  }
  assert!(
    !client.is_handshaking(),
    "client completed its handshake (sent Finished)"
  );
  assert!(
    server.is_handshaking(),
    "server has NOT yet consumed the client's final flight — still Handshaking"
  );

  // Mint + promote the CLIENT side (the dialer) with a SMALL one-way user
  // message, then pump it so the client's `Finished` + the small app frame +
  // the dialer's `close_notify` (a one-way message half-closes once its
  // request is sent) are produced together.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7600)));
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
    "a one-way user message half-closes after its request — close_notify queued"
  );

  // COALESCE the client's withheld `Finished` flight + the whole small app
  // frame + `close_notify` into ONE buffer (a single TCP read on the server).
  let mut coalesced = Vec::new();
  for _ in 0..64 {
    let before = coalesced.len();
    client.poll_transport_transmit(&mut coalesced);
    if coalesced.len() == before {
      break;
    }
  }
  assert!(
    coalesced.len() < 16 * 1024,
    "the coalesced [final flight][small frame][close_notify] is well under the \
       received-plaintext limit (no backpressure), got {}",
    coalesced.len()
  );

  // Deliver the WHOLE coalesced buffer to the still-`Handshaking` server in
  // ONE call. `process_new_packets` completes the handshake, buffers the small
  // app records, AND observes the `close_notify` — all in this single pass, so
  // NO backpressure trips and NO ciphertext tail is retained.
  server
    .handle_transport_data(&coalesced, now)
    .expect("the coalesced final-flight + small frame + close_notify is accepted");
  assert!(
    !server.is_handshaking(),
    "the server's handshake completed inside the coalesced read"
  );
  assert!(
    server.pending_inbound_is_empty(),
    "the small coalesced read is fully consumed in one pass — NO retained tail"
  );

  // Coordinator-equivalent: mint + promote the inbound `Stream`, then drive
  // the post-mint pump's `replay_pending` exactly as `pump_bridges` does. With
  // NO retained tail, the replay must STILL drain the buffered decrypted
  // plaintext into the just-promoted `Stream` and fire the recv-half close
  // anchor (`peer_has_closed()`), or the small frame never reaches the FSM.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7600), now)
    .expect("node is running");
  server.promote(s_stream);
  server
    .replay_pending(now)
    .expect("post-promote replay drains the buffered small frame + close anchor");

  // The close anchor fired (peer `close_notify` observed): the recv half is
  // retired. With a one-way inbound user message reaching `Done` and the recv
  // half retired, the bridge converges to terminal so the D1 reap can run.
  server.drain_payload_only(&mut ep_s, now);
  for _ in 0..64 {
    client.pump_out(now).ok();
    server.pump_out(now).ok();
    let moved = shuttle(&mut client, &mut server, now);
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
    "the recv-half close anchor fired and the one-way exchange reaped, got {:?}",
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
    "the small first frame coalesced with the final handshake flight reached the \
       Stream and decoded intact (drained post-promotion with no retained tail)"
  );
}

/// (d) mTLS reject: a handshake-time `TlsRecords` error (here a corrupted
/// handshake flight, the same `process_new_packets` `Err` path a
/// client-cert rejection takes) tears the bridge down with NO `Stream`
/// minted and ZERO endpoint side effects. The bridge becomes terminal so the
/// coordinator reaps it directly.
#[test]
fn handshake_failure_tears_down_with_no_stream_and_no_endpoint_events() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

  // Drive the client's first flight, then corrupt it before the server reads
  // it — the server's `process_new_packets` rejects the malformed record,
  // exactly as it rejects an unacceptable client identity.
  let mut c_out = Vec::new();
  client.poll_transport_transmit(&mut c_out);
  assert!(!c_out.is_empty(), "client produced a ClientHello flight");
  for b in c_out.iter_mut() {
    *b ^= 0xff;
  }

  assert!(server.is_handshaking());
  let res = server.handle_transport_data(&c_out, now);
  assert!(res.is_err(), "the server rejected the corrupted flight");

  // No `Stream` was minted, the bridge is terminal, and no endpoint events
  // exist (there is no Endpoint involvement on the reject path at all).
  assert!(
    server.stream_is_none(),
    "no Stream minted on a handshake reject"
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
  // reaps a failed-handshake bridge without an FSM lifecycle notice).
  let mut ep: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  server.drain_then_reap(&mut ep, now);
  assert!(
    ep.poll_event().is_none(),
    "no endpoint events on the reject path"
  );
}

/// (e) Truncation: a TCP `read == 0` (zero-length `handle_transport_data`)
/// mid-frame, with no peer `close_notify`, feeds `Stream::handle_data(&[])`,
/// which the FSM rejects with `StreamError::PeerClosed` for a non-terminal
/// awaiting phase. The bridge transitions to `Failed(Decode)` and is
/// terminal.
#[test]
fn truncation_read_zero_mid_frame_fails_peer_closed() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));
  complete_handshake(&mut client, &mut server, now);

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

  // The unused `server` still proves the handshake completed both ways.
  assert!(!server.is_handshaking());
}

/// (f) Handshake timeout: a `Handshaking` bridge whose deadline elapses
/// WITHOUT the handshake completing is terminalized by `pump_out` with NO
/// `Stream` minted, so the coordinator's pre-`Stream` reap collects it. This
/// is the slowloris / connect-refused bound — without it a stalled handshake
/// leaks the bridge (the empty-slice EOF anchor is a no-op while
/// `process_new_packets` keeps succeeding, and the `is_done()`-gated flush
/// deadline needs a `Stream` that does not exist yet).
#[test]
fn handshaking_bridge_times_out_at_deadline_with_no_stream() {
  let now = Instant::now();
  let deadline = now + Duration::from_secs(10);
  let (mut client, _server) = handshaking_pair(deadline);

  // Before the deadline a `pump_out` is a clean no-op (still Handshaking,
  // still surfacing the deadline as its only timer).
  assert!(client.is_handshaking());
  assert!(!client.is_terminal());
  client.pump_out(now).expect("pre-deadline pump is a no-op");
  assert!(
    client.is_handshaking(),
    "still handshaking before the deadline"
  );
  assert_eq!(
    client.poll_timeout(),
    Some(deadline),
    "a Handshaking bridge surfaces its dial deadline as the only timer"
  );

  // At the deadline `pump_out` terminalizes the stalled handshake.
  let res = client.pump_out(deadline);
  assert!(res.is_err(), "a stalled handshake at its deadline fails");
  assert!(
    client.is_terminal(),
    "the bridge is terminal after the handshake deadline elapses"
  );
  assert!(
    client.stream_is_none(),
    "no Stream is minted on a handshake timeout"
  );
  assert!(
    matches!(
      client.phase_ref(),
      StreamPhase::Established(BridgePhase::Failed(BridgeFailure::Timeout))
    ),
    "the handshake timeout maps to Failed(Timeout), got {:?}",
    phase_label(client.phase_ref())
  );
  assert!(
    !client.is_handshaking(),
    "a timed-out bridge is no longer handshaking"
  );
  // A terminal bridge contributes no deadline (it is reaped this tick).
  assert_eq!(client.poll_timeout(), None);

  // `drain_then_reap` on a no-`Stream` bridge is a clean no-op — no FSM
  // lifecycle notice is owed for a handshake that never minted a `Stream`.
  let mut ep: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7300)));
  client.drain_then_reap(&mut ep, deadline);
  assert!(
    ep.poll_event().is_none(),
    "no endpoint events on the handshake-timeout path"
  );
}

/// A handshake flight delivered in two PARTIAL transport reads exercises the
/// `intake_handshaking` partial-header path: the first read is consumed in
/// full (`Intake::Done`) but leaves the server still `Handshaking` (the record
/// is incomplete), so the loop returns early (`matches!(Done) || !consumed`)
/// and waits for the rest. The second read completes the flight and the
/// handshake proceeds.
#[test]
fn handshake_flight_split_across_two_reads_buffers_partial() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

  // The client's first flight (ClientHello).
  let mut hello = Vec::new();
  client.poll_transport_transmit(&mut hello);
  assert!(hello.len() > 8, "the ClientHello is large enough to split");

  // Deliver it in two halves. The first half is a partial record: rustls
  // consumes it (Done) but cannot complete the handshake — the server stays
  // Handshaking with the partial buffered.
  let mid = hello.len() / 2;
  server
    .handle_transport_data(&hello[..mid], now)
    .expect("the partial first read is accepted");
  assert!(
    server.is_handshaking(),
    "the server is still handshaking after a partial flight"
  );
  assert!(
    server.stream_is_none(),
    "no Stream is minted while the handshake is incomplete"
  );

  // The second half completes the flight; the handshake can now advance.
  server
    .handle_transport_data(&hello[mid..], now)
    .expect("the second read completes the flight");

  // Finish the handshake the rest of the way to prove the split did not
  // corrupt it.
  complete_handshake(&mut client, &mut server, now);
  assert!(!client.is_handshaking() && !server.is_handshaking());
}

/// A retained pre-promotion ciphertext tail followed by a SECOND transport
/// read delivered via `handle_transport_data` (rather than `replay_pending`)
/// exercises the established-intake path that PREPENDS the retained tail to
/// the newly-supplied data: `pending_inbound` is non-empty, so the bytes are
/// combined (tail first, then the new read) and the whole >16 KiB frame
/// reassembles in wire order.
///
/// The first read carries the dialer's final handshake flight plus PART of the
/// large frame's ciphertext — enough to complete the handshake and trip
/// rustls backpressure so a tail is retained; the remainder is withheld and
/// delivered post-promotion as the second transport read.
#[test]
fn retained_tail_then_second_read_via_handle_transport_data_reassembles() {
  let now = Instant::now();
  let (mut client, mut server) = handshaking_pair(now + Duration::from_secs(10));

  // Drive the handshake until the CLIENT has finished but withhold its final
  // flight from the server (still Handshaking, no Stream).
  for _ in 0..64 {
    if !client.is_handshaking() {
      break;
    }
    let mut c_out = Vec::new();
    client.poll_transport_transmit(&mut c_out);
    if !c_out.is_empty() {
      server.handle_transport_data(&c_out, now).unwrap();
    }
    let mut s_out = Vec::new();
    server.poll_transport_transmit(&mut s_out);
    if !s_out.is_empty() {
      client.handle_transport_data(&s_out, now).unwrap();
    }
    if c_out.is_empty() && s_out.is_empty() {
      break;
    }
  }
  assert!(!client.is_handshaking(), "client finished its handshake");
  assert!(server.is_handshaking(), "server's final flight withheld");

  // Mint + pump the client with a large one-way user message so its `Finished`
  // flight + the >16 KiB app frame are produced together.
  let mut ep_c: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("cli"), addr(7560)));
  let payload = Bytes::from((0..48 * 1024).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
  let sid = ep_c
    .start_user_message(addr(7000), payload.clone(), now)
    .expect("issued while running");
  let c_stream = ep_c.dial_succeeded(sid, now).expect("dial mints");
  client.promote(c_stream);
  client.pump_out(now).expect("client pumps its request");

  let mut all = Vec::new();
  for _ in 0..64 {
    let before = all.len();
    client.poll_transport_transmit(&mut all);
    if all.len() == before {
      break;
    }
  }
  assert!(all.len() > 16 * 1024, "coalesced ciphertext exceeds 16 KiB");

  // FIRST read: the final flight + all but the last 2 KiB of the app
  // ciphertext — completes the handshake and trips rustls backpressure well
  // before the buffer is exhausted, so the bridge retains the unconsumed
  // ciphertext tail.
  let split = all.len() - 2 * 1024;
  server
    .handle_transport_data(&all[..split], now)
    .expect("the partial coalesced read is accepted");
  assert!(
    !server.is_handshaking(),
    "the handshake completed inside the first read"
  );

  // Promote, but DO NOT call replay_pending — instead deliver the remaining
  // ciphertext via handle_transport_data while the retained tail is still
  // present, so the established intake combines tail + new read.
  let mut ep_s: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(EndpointOptions::new(SmolStr::new("srv"), addr(7000)));
  let s_stream = ep_s
    .accept_stream(addr(7560), now)
    .expect("node is running");
  server.promote(s_stream);
  assert!(
    !server.pending_inbound_is_empty(),
    "a ciphertext tail is retained after the first backpressured read"
  );

  // SECOND read: the remaining app ciphertext. `pending_inbound` is non-empty,
  // so this is the combine-tail-then-data path.
  server
    .handle_transport_data(&all[split..], now)
    .expect("the second read reassembles the frame with the retained tail");

  // Finish the exchange and reap; the whole frame must have decoded.
  server.drain_payload_only(&mut ep_s, now);
  for _ in 0..64 {
    client.pump_out(now).ok();
    server.pump_out(now).ok();
    let moved = shuttle(&mut client, &mut server, now);
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
    "the >16 KiB frame reassembled from the retained tail + the second read"
  );
}

/// A `StreamBridge<TlsRecords>` built with an ENABLED `EncryptionOptions`
/// ends up with a DISABLED effective `EncryptionOptions`: `TlsRecords
/// ::is_secure() == true`, so `StreamBridge::new` zeroes the encryption
/// field. The on-wire reliable bytes therefore carry no `Encrypted` wrapper
/// — TLS already provides confidentiality, and double-encrypting on the
/// reliable path costs CPU and bandwidth without adding security.
#[cfg(feature = "encryption-aes-gcm")]
#[test]
fn tls_bridge_reliable_skips_encryption_when_is_secure() {
  use crate::{EncryptionOptions, Keyring, SecretKey, streams::test_support::TEST_RELIABLE_MAX};
  let deadline = Instant::now() + Duration::from_secs(30);
  let client = TlsRecords::client(
    Arc::new(test_client()),
    ServerName::try_from("localhost").unwrap(),
  )
  .unwrap();
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
  let bridge: StreamBridge<SmolStr, SocketAddr, TlsRecords> = StreamBridge::new(
    client,
    deadline,
    crate::CompressionOptions::new(),
    opts,
    TEST_RELIABLE_MAX,
  );
  assert!(
    !bridge.encryption_for_test().is_enabled(),
    "a TLS bridge zeroes the EncryptionOptions regardless of caller intent"
  );
}
