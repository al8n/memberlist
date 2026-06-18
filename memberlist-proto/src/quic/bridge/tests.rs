use super::*;
use core::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::typed::Message;
use bytes::Bytes;
use quinn_proto::{Dir, Side};
use smol_str::SmolStr;

use crate::{config::EndpointOptions, event::Event};

/// `drain_payload_only` must not commit a dispatched frame's side
/// effects until the recv half has observed peer FIN. Without this
/// gate, a split-delivery `[valid_frame][later_junk]` (chunk 1
/// dispatches and is drained on tick 1; chunk 2 fails the stream on
/// tick 2) commits the merge / emits `UserDataReceived` BEFORE chunk
/// 2 can invalidate the stream. The verification observable here: a
/// UserData inbound frame queues `EndpointEvent::UserDataReceived`,
/// which `drain_payload_only` would normally convert into
/// `Event::UserPacket` on the Endpoint. While the bridge is `Active`,
/// no `Event::UserPacket` may surface. Once `observe_recv_fin` flips
/// the bridge to `RecvClosed`, the drain runs and the event surfaces.
#[test]
fn drain_payload_only_defers_until_recv_fin_observed() {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();

  // Bridge wraps a fresh accept_stream — the construction-time
  // invariant for `Bridge::new` is a pre-Done stream with a Some
  // exchange deadline. Dispatching the frame BELOW transitions the
  // FSM out of `InboundAwaitingFirstMessage`, which is why the
  // bridge is constructed first.
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  let mut bridge = Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    None,
    false,
    false,
  );
  assert!(matches!(bridge.phase, BridgePhase::Active));

  // Dispatch the UserData frame through the bridge's inner FSM. The
  // FSM transitions to `Done` and queues `EndpointEvent::UserDataReceived`
  // — but the bridge phase stays `Active` because the recv half has
  // NOT yet observed peer FIN.
  let msg = Message::<SmolStr, SocketAddr>::UserData(Bytes::from_static(b"hello"));
  let bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&msg).expect("encode");
  bridge
    .stream
    .handle_data(&bytes, t0)
    .expect("dispatch user data frame");

  // Pre-drain: the Endpoint must have no observable side effects from
  // this stream.
  let mut conns = ConnTable::new();
  assert!(
    !ep
      .poll_event()
      .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
    "no UserPacket may be queued before any drain"
  );

  // Drain attempt #1 — bridge is Active. The deferred-commit gate
  // MUST block.
  bridge.drain_payload_only(&mut ep, &mut conns, t0);
  assert!(
    ep.poll_event().is_none(),
    "drain_payload_only on an Active bridge MUST be a no-op — \
       the dispatched frame's `UserDataReceived` event MUST stay \
       queued until peer FIN proves no trailing bytes can invalidate \
       this stream"
  );

  // Flip the bridge to RecvClosed (peer FIN observed).
  bridge.observe_recv_fin();
  assert!(matches!(bridge.phase, BridgePhase::RecvClosed));

  // Drain attempt #2 — gate releases. The queued event flows through
  // `Endpoint::handle_stream_event` which emits `Event::UserPacket`.
  bridge.drain_payload_only(&mut ep, &mut conns, t0);
  let ev = ep.poll_event();
  assert!(
    matches!(&ev, Some(Event::UserPacket(p)) if p.data_ref().as_ref() == b"hello"),
    "drain_payload_only on a RecvClosed bridge MUST commit the \
       queued UserData event — got {ev:?}"
  );
}

/// A transport-level failure BEFORE peer FIN must not commit the
/// queued endpoint events. `drain_payload_only` gates on
/// `BridgePhase::RecvClosed`, but `drain_then_reap` is the terminal
/// D1 path and drains the FSM queue unconditionally to enforce D1
/// before lifecycle delivery. When `Bridge::fail` is driven by a
/// transport signal (peer RESET, ConnectionLost) while the bridge is
/// still `Active` / `SendClosed` (recv FIN never observed), the queue
/// must be cleared at the failure transition — symmetric with
/// `Stream::enter_failed`'s queue-clearing on the FSM-failure path.
/// Without this, a peer that sends one complete `UserData` /
/// `PushPull` frame and then resets the connection before FIN could
/// commit `Event::UserPacket` / a merge through the terminal drain
/// path.
#[test]
fn pre_fin_transport_failure_discards_queued_payload_events() {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();

  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  let mut bridge = Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    None,
    false,
    false,
  );
  assert!(matches!(bridge.phase, BridgePhase::Active));

  // Dispatch a UserData frame — FSM → Done, queues
  // `EndpointEvent::UserDataReceived` AND `StreamEvent::Closed`.
  // The bridge is STILL Active (recv FIN not observed).
  let msg = Message::<SmolStr, SocketAddr>::UserData(Bytes::from_static(b"hello"));
  let bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&msg).expect("encode");
  bridge
    .stream
    .handle_data(&bytes, t0)
    .expect("dispatch user data frame");

  // Simulate a transport-level failure BEFORE peer FIN. ConnectionLost
  // (or peer RESET) routes through `Bridge::fail`, which MUST clear
  // the FSM queue here: phase was Active (no FIN observed).
  bridge.fail_connection_lost();
  assert!(matches!(
    bridge.phase,
    BridgePhase::Failed(BridgeFailure::ConnectionLost)
  ));

  // Drive the terminal D1 reap path. Without the pre-FIN queue clear
  // in `Bridge::fail`, `drain_then_reap` would drain
  // `UserDataReceived` into `Endpoint::handle_stream_event`, which
  // would emit `Event::UserPacket`. With the clear, the queue is
  // already empty — only the `StreamErrored` lifecycle notice is
  // delivered (visible via the FSM's `StreamEvent::Failed` after
  // `handle_stream_event(StreamErrored{...})`).
  let mut conns = ConnTable::new();
  bridge.drain_then_reap(&mut ep, &mut conns, t0);

  // Public observable: NO `Event::UserPacket` may surface. The
  // dispatched frame's side effects were never authorized by peer
  // FIN and the transport-level failure does not authorize them
  // either.
  let mut saw_user_packet = false;
  while let Some(ev) = ep.poll_event() {
    if matches!(ev, Event::UserPacket(..)) {
      saw_user_packet = true;
    }
  }
  assert!(
    !saw_user_packet,
    "a transport-level failure BEFORE recv FIN must NOT commit the \
       dispatched frame's side effects — `Event::UserPacket` would \
       mean the deferred-commit gate was bypassed by the terminal \
       drain path"
  );
}

#[test]
fn bridge_module_compiles() {
  fn _assert<I, A>()
  where
    I: crate::Id,
    A: crate::Data
      + crate::CheapClone
      + Eq
      + core::hash::Hash
      + core::fmt::Debug
      + core::fmt::Display
      + Send
      + Sync
      + 'static,
  {
    let _: fn(
      Stream<I, A>,
      ConnectionHandle,
      QuicSid,
      crate::CompressionOptions,
      crate::EncryptionOptions,
      usize,
      Option<bytes::Bytes>,
      bool,
      bool,
    ) -> Bridge<I, A> = Bridge::<I, A>::new;
  }
}

#[cfg(feature = "lz4")]
#[test]
fn quic_reliable_unit_accumulation_roundtrips() {
  use crate::{CompressAlgorithm, CompressionOptions, encode_reliable_unit, take_reliable_unit};
  let opts = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
  let unit = encode_reliable_unit(&opts, &framed);
  let mut accum = unit.clone();
  let (back, consumed) = take_reliable_unit(&accum, 16 * 1024 * 1024)
    .expect("decode ok")
    .expect("a complete unit is present");
  accum.drain(..consumed);
  assert_eq!(back, framed);
  assert!(accum.is_empty());
}

#[cfg(feature = "lz4")]
#[test]
fn quic_reliable_unit_split_across_two_chunks_buffers_then_completes() {
  // A reliable unit may arrive split across two `chunks.next` yields. The
  // accumulator must hold the first partial chunk (no frame yet) and
  // complete on the second.
  use crate::{CompressAlgorithm, CompressionOptions, encode_reliable_unit, take_reliable_unit};
  let opts = CompressionOptions::new()
    .with_algorithm(CompressAlgorithm::Lz4)
    .with_threshold(8);
  let framed = b"the quick brown fox jumps over the lazy dog".repeat(32);
  let unit = encode_reliable_unit(&opts, &framed);
  assert!(unit.len() > 4, "unit is large enough to split");
  let split = unit.len() / 2;

  let mut accum: Vec<u8> = Vec::new();
  accum.extend_from_slice(&unit[..split]);
  assert!(
    take_reliable_unit(&accum, 16 * 1024 * 1024)
      .expect("a partial unit is not an error")
      .is_none(),
    "the first partial chunk must buffer, not decode"
  );
  accum.extend_from_slice(&unit[split..]);
  let (back, consumed) = take_reliable_unit(&accum, 16 * 1024 * 1024)
    .expect("decode ok")
    .expect("the unit is complete after the second chunk");
  accum.drain(..consumed);
  assert_eq!(back, framed, "the split unit decompresses to the original");
  assert!(accum.is_empty());
}

#[test]
fn quic_reliable_unit_disabled_is_byte_identical() {
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
fn build_test_quic_bridge_with_encryption(
  encryption: crate::EncryptionOptions,
) -> Bridge<SmolStr, SocketAddr> {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    encryption,
    ep.max_stream_frame_size(),
    None,
    false,
    false,
  )
}

/// A QUIC `Bridge` built with an ENABLED `EncryptionOptions` ends up with
/// a DISABLED effective `EncryptionOptions`: the 9-arg `Bridge::new` zeroes
/// the encryption field unconditionally. The on-wire reliable bytes
/// therefore carry no `Encrypted` wrapper — quinn already provides
/// confidentiality, and double-encrypting on the reliable path costs CPU
/// and bandwidth without adding security.
#[cfg(feature = "aes-gcm")]
#[test]
fn quic_bridge_reliable_skips_encryption_unconditionally() {
  use crate::{EncryptionOptions, Keyring, SecretKey};
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0; 32])));
  let bridge = build_test_quic_bridge_with_encryption(opts);
  assert!(
    !bridge.encryption_for_test().is_enabled(),
    "QUIC bridge zeroes encryption — quinn already encrypts the stream"
  );
}

// ── Label mechanism helpers ────────────────────────────────────────────────

fn make_labeled_dialer(label: bytes::Bytes) -> Bridge<SmolStr, SocketAddr> {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    Some(label),
    false,
    true,
  )
}

fn make_labeled_acceptor(label: bytes::Bytes) -> Bridge<SmolStr, SocketAddr> {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    Some(label),
    false,
    false,
  )
}

fn make_labeled_acceptor_skip(label: bytes::Bytes) -> Bridge<SmolStr, SocketAddr> {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    Some(label),
    true,
    false,
  )
}

fn build_label_header(label: &[u8]) -> Vec<u8> {
  let mut buf = Vec::new();
  crate::label::encode_label_prefix(label, &mut buf);
  buf
}

// ── Label mechanism tests ──────────────────────────────────────────────────

/// Two bridges with the same label — the dialer validates the acceptor's
/// inbound label in one shot, and the acceptor validates the dialer's.
/// Both `inbound_label_validated` latches flip to `true` after receiving
/// the matching header from the other side.
#[test]
fn labeled_exchange_same_label_both_validate() {
  let label = bytes::Bytes::from_static(b"cluster-x");

  // Dialer: inbound_label_validated starts false (label is Some).
  let mut dialer = make_labeled_dialer(label.clone());
  assert!(
    !dialer.inbound_label_validated(),
    "dialer starts with inbound latch unset"
  );
  // Dialer is eager: outbound label is staged on first pump opportunity
  // (pending_out still empty at construction; the label gets pushed
  // when pump_out runs — but our test helper drives it directly).
  // Drive inbound classification with the acceptor's label header.
  let header = build_label_header(b"cluster-x");
  assert!(
    dialer
      .push_recv_and_classify(&header)
      .expect("should not fail"),
    "dialer: matching inbound label is accepted"
  );
  assert!(
    dialer.inbound_label_validated(),
    "dialer inbound latch set after matching header"
  );

  // Acceptor: inbound_label_validated starts false (label is Some).
  let mut acceptor = make_labeled_acceptor(label.clone());
  assert!(
    !acceptor.inbound_label_validated(),
    "acceptor starts with inbound latch unset"
  );
  assert!(
    !acceptor.outbound_label_written(),
    "acceptor outbound latch unset before inbound validation"
  );
  // Drive inbound classification with the dialer's label header.
  assert!(
    acceptor
      .push_recv_and_classify(&header)
      .expect("should not fail"),
    "acceptor: matching inbound label is accepted"
  );
  assert!(
    acceptor.inbound_label_validated(),
    "acceptor inbound latch set after matching header"
  );
  // Acceptor lazy release: outbound label must have been staged.
  assert!(
    acceptor.outbound_label_written(),
    "acceptor outbound latch set after inbound validates"
  );
  // The staged bytes must be the expected label frame.
  let expected_frame = build_label_header(b"cluster-x");
  assert_eq!(
    acceptor.pending_out_bytes(),
    expected_frame.as_slice(),
    "acceptor pending_out must contain the label frame after lazy release"
  );
}

/// A mismatched inbound label is rejected before any reliable unit reaches
/// the FSM — `classify_header` returns `Rejected(Mismatch)`.
#[test]
fn labeled_bridge_mismatched_inbound_label_rejected() {
  let label = bytes::Bytes::from_static(b"cluster-x");
  let mut acceptor = make_labeled_acceptor(label);

  // Send a label frame claiming a DIFFERENT cluster.
  let wrong_header = build_label_header(b"cluster-y");
  let result = acceptor.push_recv_and_classify(&wrong_header);
  assert!(
    matches!(result, Err(crate::label::LabelError::Mismatch)),
    "a mismatched inbound label must be Rejected(Mismatch) — got {result:?}"
  );
  // The FSM must not have received any data (recv_accum still has the
  // rejected header bytes, latch is still unset).
  assert!(
    !acceptor.inbound_label_validated(),
    "inbound latch must remain unset after a rejected label"
  );
}

/// `skip_inbound_label_check=true` on an acceptor allows an unlabeled peer
/// (no `[12]` header present). The inbound latch flips to `true` consuming 0
/// bytes from recv_accum.
#[test]
fn labeled_bridge_skip_accepts_unlabeled_inbound() {
  let label = bytes::Bytes::from_static(b"cluster-x");
  let mut acceptor = make_labeled_acceptor_skip(label);

  // Inbound peer sends a plain reliable unit byte-sequence (no label header).
  // The first byte is NOT 12, so classify_header returns Accepted(0) with
  // skip=true.
  let payload = b"some plain payload data";
  let validated = acceptor
    .push_recv_and_classify(payload)
    .expect("skip=true must not fail for an unlabeled inbound");
  assert!(validated, "skip must accept an unlabeled inbound");
  assert!(
    acceptor.inbound_label_validated(),
    "inbound latch must be set after skip-accepted unlabeled inbound"
  );
  // recv_accum retains the payload bytes (Accepted(0) consumes 0 bytes).
  assert_eq!(
    acceptor.pending_out_bytes().len(),
    build_label_header(b"cluster-x").len(),
    "acceptor must have staged its own label frame after lazy release"
  );
}

/// An unlabeled bridge (label=None) passes a 12-byte first reliable unit
/// through unchanged — no false reject, no classify_header call.
///
/// Faithful to `memberlist-core/src/network.rs`: an unlabeled coordinator
/// treats a 12-byte first unit as a normal reliable frame, not as a label
/// header to parse. inbound_label_validated starts true so the classifier
/// is never invoked.
#[test]
fn unlabeled_bridge_twelve_byte_first_unit_passes_through() {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  let bridge = Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    None,
    false,
    false,
  );
  // The latch starts true for an unlabeled bridge — the classifier is
  // never invoked, so a first unit whose varint-encoded length happens
  // to be 12 (== LABELED_TAG) is never falsely classified as a label frame.
  assert!(
    bridge.inbound_label_validated(),
    "unlabeled bridge: inbound latch must start true — byte-identical to today"
  );
  // Outbound latch is also true: no label frame is ever written.
  assert!(
    bridge.outbound_label_written(),
    "unlabeled bridge: outbound latch must start true — no label frame emitted"
  );
}

/// A label header split across two `push_recv_and_classify` calls validates
/// only once the full header is present. The first partial push returns
/// `Ok(false)` (Incomplete); the second push with the remaining bytes
/// returns `Ok(true)` (Accepted).
#[test]
fn labeled_bridge_split_label_header_validates_on_completion() {
  let label = bytes::Bytes::from_static(b"cluster-x");
  let header = build_label_header(b"cluster-x");
  assert!(header.len() > 3, "label header large enough to split");

  let split = header.len() / 2;
  let mut acceptor = make_labeled_acceptor(label);

  // First half — Incomplete.
  let result = acceptor
    .push_recv_and_classify(&header[..split])
    .expect("partial header must not fail");
  assert!(
    !result,
    "partial label header must return Incomplete (Ok(false))"
  );
  assert!(
    !acceptor.inbound_label_validated(),
    "inbound latch must remain unset after partial header"
  );

  // Second half — Accepted.
  let result = acceptor
    .push_recv_and_classify(&header[split..])
    .expect("completing the header must not fail");
  assert!(
    result,
    "completing the label header must return Accepted (Ok(true))"
  );
  assert!(
    acceptor.inbound_label_validated(),
    "inbound latch must be set after the split header completes"
  );
}

/// The acceptor MUST NOT write its outbound label before the inbound label
/// validates. Before `push_recv_and_classify` is called, `pending_out` must
/// be empty and `outbound_label_written` must be false.
#[test]
fn acceptor_holds_outbound_label_until_inbound_validates() {
  let label = bytes::Bytes::from_static(b"cluster-x");
  let acceptor = make_labeled_acceptor(label);

  // At construction: outbound label must NOT be staged.
  assert!(
    !acceptor.outbound_label_written(),
    "acceptor: outbound latch must be false at construction"
  );
  assert!(
    acceptor.pending_out_bytes().is_empty(),
    "acceptor: pending_out must be empty at construction — no label written yet"
  );

  // Note: the dialer (eager=true) gets its label staged the first time
  // pump_out runs (when it calls encode_label_prefix into pending_out).
  // That path is tested via make_labeled_dialer + pump_out; here we only
  // assert the construction-time invariant for the acceptor.
  let dialer = make_labeled_dialer(bytes::Bytes::from_static(b"cluster-x"));
  // Dialer: outbound_label_written starts FALSE too — the label is written
  // into pending_out the first time pump_out fires (eager, not at construction).
  // At construction-time pending_out is empty.
  assert!(
    !dialer.outbound_label_written(),
    "dialer: outbound latch also false at construction — written on first pump_out"
  );
  assert!(
    dialer.pending_out_bytes().is_empty(),
    "dialer: pending_out must be empty at construction"
  );
}

// ── Phase / accessor / drain-then-reap coverage ────────────────────────────

/// Build a plain unlabeled, encryption-disabled bridge over a freshly
/// accepted stream. `ch = ConnectionHandle(0)` and an empty `ConnTable` mean
/// the `pump_*`/`retire_halves` paths that look up `conns.get_mut(self.ch)`
/// short-circuit — these tests drive phase + drain logic in isolation.
fn make_plain_bridge() -> Bridge<SmolStr, SocketAddr> {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    None,
    false,
    false,
  )
}

/// `set_encryption` force-disables regardless of input (quinn already
/// encrypts the reliable stream); `id`/`ch`/`sid` accessors report the
/// construction values; `is_phase_failed` is false for a fresh `Active`
/// bridge.
#[test]
fn bridge_accessors_and_set_encryption_force_disable() {
  let mut bridge = make_plain_bridge();
  assert!(
    !bridge.is_phase_failed(),
    "fresh bridge is Active, not Failed"
  );
  assert_eq!(bridge.ch(), ConnectionHandle(0));
  assert_eq!(bridge.sid(), QuicSid::new(Side::Client, Dir::Bi, 0));
  // Even handed a (would-be) enabled policy, the stored options stay
  // disabled on the QUIC reliable path.
  bridge.set_encryption(crate::EncryptionOptions::new());
  // The field is reachable directly from the child test module.
  assert!(
    !bridge.encryption.is_enabled(),
    "QUIC bridge encryption stays disabled after set_encryption"
  );
}

/// `poll_timeout` returns the bridge's own deadline while non-terminal and
/// `None` once terminal (`BothClosed`/`Failed`) — the terminal short-circuit
/// that keeps a reaped bridge from contributing a stale wake to the
/// coordinator's unified `min`.
#[test]
fn bridge_poll_timeout_some_while_active_none_when_terminal() {
  let mut bridge = make_plain_bridge();
  assert!(
    bridge.poll_timeout().is_some(),
    "an Active bridge contributes its exchange deadline"
  );
  // Drive to BothClosed via the two FIN observers.
  bridge.observe_send_fin();
  assert!(matches!(bridge.phase, BridgePhase::SendClosed));
  bridge.observe_recv_fin();
  assert!(matches!(bridge.phase, BridgePhase::BothClosed));
  assert!(bridge.is_terminal(), "BothClosed is terminal");
  assert!(
    bridge.poll_timeout().is_none(),
    "a terminal bridge contributes no deadline"
  );
}

/// `observe_send_fin` / `observe_recv_fin` are sticky no-ops once terminal:
/// a `Failed` bridge stays `Failed` after either observer fires.
#[test]
fn fin_observers_are_noops_on_terminal_phase() {
  let mut bridge = make_plain_bridge();
  bridge.fail_connection_lost();
  assert!(matches!(
    bridge.phase,
    BridgePhase::Failed(BridgeFailure::ConnectionLost)
  ));
  bridge.observe_send_fin();
  bridge.observe_recv_fin();
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::ConnectionLost)
    ),
    "FIN observers must not overwrite a sticky Failed phase"
  );
}

/// `fail_stopped_already_retired` records the peer STOP_SENDING error code as
/// a `Transport` failure and clears `pending_out`. Sticky: a follow-up
/// `fail_connection_lost` does not overwrite the first cause.
#[test]
fn fail_stopped_already_retired_sets_transport_failure_and_is_sticky() {
  let mut bridge = make_plain_bridge();
  bridge.pending_out.extend_from_slice(b"stale outbound tail");
  bridge.fail_stopped_already_retired(quinn_proto::VarInt::from_u32(7));
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::Transport(_))
    ),
    "STOP_SENDING maps to a Transport failure"
  );
  assert!(
    bridge.pending_out.is_empty(),
    "the staged outbound tail is cleared on failure"
  );
  // First failure wins.
  bridge.fail_connection_lost();
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::Transport(_))
    ),
    "the first failure cause is sticky — ConnectionLost must not overwrite it"
  );
}

/// `drain_then_reap` selects the lifecycle notice from the bridge's terminal
/// phase. For every `BridgeFailure` variant the bridge emits a
/// `StreamErrored` notice (covering each reason-string arm); for a clean
/// `BothClosed` it emits `StreamClosed`. Driven with an empty `ConnTable`
/// (the directly-set `Failed` phase needs no half retirement at drain time)
/// so the notice-selection match is exercised in isolation. Observable: no
/// panic, and a clean reap surfaces no `UserPacket` (no payload was queued).
#[test]
fn drain_then_reap_notice_selection_covers_every_failure_reason() {
  let mut conns = ConnTable::new();
  let t0 = Instant::now();
  let reasons = [
    BridgeFailure::Timeout,
    BridgeFailure::Transport("boom".to_string()),
    BridgeFailure::Decode,
    BridgeFailure::ConnectionLost,
    BridgeFailure::AdmissionClosed,
    BridgeFailure::DialRetired,
    BridgeFailure::EncryptionPolicyChanged,
  ];
  for reason in reasons {
    let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
      SmolStr::new("self"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ));
    let mut bridge = make_plain_bridge();
    bridge.phase = BridgePhase::Failed(reason);
    assert!(bridge.is_terminal());
    // Drains the (empty) FSM event queue then emits the StreamErrored notice
    // for this reason — the arm under test. No panic == arm executed.
    bridge.drain_then_reap(&mut ep, &mut conns, t0);
    assert!(
      !ep
        .poll_event()
        .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
      "a failed reap with no queued payload must not emit a UserPacket"
    );
  }
}

/// The clean-`BothClosed` arm of `drain_then_reap`'s notice selection emits
/// `StreamClosed` (not `StreamErrored`). Driven in isolation on a bridge with
/// no queued payload events.
#[test]
fn drain_then_reap_clean_bothclosed_emits_stream_closed() {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  let mut conns = ConnTable::new();
  let t0 = Instant::now();
  let mut bridge = make_plain_bridge();
  bridge.observe_send_fin();
  bridge.observe_recv_fin();
  assert!(matches!(bridge.phase, BridgePhase::BothClosed));
  bridge.drain_then_reap(&mut ep, &mut conns, t0);
  // No payload was dispatched, so the only effect is the clean StreamClosed
  // lifecycle notice routed into the Endpoint — no UserPacket may surface.
  assert!(
    !ep
      .poll_event()
      .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
    "a clean reap with no queued payload must not emit a UserPacket"
  );
}

/// A `MergeDelegate` that rejects every inbound merge — drives
/// `Endpoint::handle_stream_event(PushPullRequestReceived{Join})` to return
/// `StreamCommand::Close`.
struct RejectAllMerges;
impl crate::delegate::MergeDelegate<SmolStr, SocketAddr> for RejectAllMerges {
  fn notify_merge(&self, _peers: &[crate::typed::NodeState<SmolStr, SocketAddr>]) -> bool {
    false
  }
}

/// Build an inbound bridge whose stream has already decoded a PushPull join
/// request (FSM in `InboundSendingResponse`, `PushPullRequestReceived{Join}`
/// queued), over an `Endpoint` that rejects every merge. Returns
/// `(ep, conns, bridge)` so the caller drives the drain paths.
fn inbound_join_bridge_with_rejecting_endpoint() -> (
  Endpoint<SmolStr, SocketAddr>,
  ConnTable,
  Bridge<SmolStr, SocketAddr>,
  Instant,
) {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(EndpointOptions::new(
    SmolStr::new("self"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  ));
  ep.set_merge_delegate(RejectAllMerges);
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let stream = ep.accept_stream(peer, t0).expect("node is running");
  let ch = ConnectionHandle(0);
  let sid = QuicSid::new(Side::Client, Dir::Bi, 0);
  let mut bridge = Bridge::new(
    stream,
    ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    None,
    false,
    false,
  );
  // Dispatch a join PushPull request so the FSM queues
  // `PushPullRequestReceived{Join}` — the event the rejecting delegate turns
  // into `StreamCommand::Close`.
  let dave = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7004);
  let dave_state =
    crate::typed::PushNodeState::new(1, SmolStr::new("dave"), dave, crate::typed::State::Alive);
  let pp =
    crate::typed::PushPull::new(true, core::iter::once(dave_state)).with_user_data(Bytes::new());
  let bytes =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::PushPull(pp)).expect("encode");
  bridge
    .stream
    .handle_data(&bytes, t0)
    .expect("dispatch the join push/pull request");
  (ep, ConnTable::new(), bridge, t0)
}

/// `drain_payload_only`'s `StreamCommand::Close` arm: a rejected inbound join
/// terminalizes the bridge (`Failed(AdmissionClosed)`) in the same drain. The
/// deferred-commit gate is released first via `observe_recv_fin`.
#[test]
fn drain_payload_only_close_arm_terminalizes_on_rejected_merge() {
  let (mut ep, mut conns, mut bridge, t0) = inbound_join_bridge_with_rejecting_endpoint();
  // Release the RecvClosed gate so `drain_payload_only` actually drains.
  bridge.observe_recv_fin();
  assert!(matches!(bridge.phase, BridgePhase::RecvClosed));
  bridge.drain_payload_only(&mut ep, &mut conns, t0);
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::AdmissionClosed)
    ),
    "the rejected-merge Close command must terminalize the bridge as \
       AdmissionClosed — got {:?}",
    bridge.phase
  );
}

/// `drain_then_reap`'s `StreamCommand::Close` arm (the terminal D1 path
/// drains unconditionally): the same rejected-join scenario terminalizes the
/// bridge as `Failed(AdmissionClosed)` and delivers the `StreamErrored`
/// notice (no `UserPacket` surfaces — the join state was rejected).
#[test]
fn drain_then_reap_close_arm_terminalizes_on_rejected_merge() {
  let (mut ep, mut conns, mut bridge, t0) = inbound_join_bridge_with_rejecting_endpoint();
  bridge.drain_then_reap(&mut ep, &mut conns, t0);
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::AdmissionClosed)
    ),
    "drain_then_reap's Close arm must terminalize as AdmissionClosed — got {:?}",
    bridge.phase
  );
  assert!(
    !ep
      .poll_event()
      .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
    "a rejected join must not surface application user data"
  );
}

/// `push_recv_and_classify` short-circuits to `Ok(true)` when the inbound
/// label latch is already set — the `if self.inbound_label_validated` early
/// return. An unlabeled bridge starts with the latch set, so any feed returns
/// `Ok(true)` without invoking `classify_header`.
#[test]
fn push_recv_and_classify_returns_true_when_latch_already_set() {
  let mut bridge = make_plain_bridge();
  assert!(
    bridge.inbound_label_validated(),
    "an unlabeled bridge starts with the inbound latch set"
  );
  assert_eq!(
    bridge.push_recv_and_classify(b"arbitrary reliable bytes"),
    Ok(true),
    "a feed on an already-validated bridge returns Ok(true) immediately"
  );
}

/// `pump_in` / `pump_out` short-circuit to `Ok(())` when the bridge's
/// connection handle is absent from the `ConnTable` (`conns.get_mut(self.ch)
/// == None`) — the missing-connection guard. No phase change, no panic.
#[test]
fn pump_in_out_are_ok_noops_when_connection_absent() {
  let mut bridge = make_plain_bridge();
  let mut conns = ConnTable::new();
  let t0 = Instant::now();
  assert!(
    bridge.pump_in(&mut conns, t0).is_ok(),
    "pump_in is a no-op Ok when the connection is gone"
  );
  assert!(
    bridge.pump_out(&mut conns, t0).is_ok(),
    "pump_out is a no-op Ok when the connection is gone"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Active),
    "neither pump changes phase when the connection is absent"
  );
}

// ---------------------------------------------------------------------------
// Real quinn-proto connection pair, for the bridge fault paths that only fire
// when `pump_in` / `pump_out` operate over a LIVE quinn stream (the
// `conns.get_mut(self.ch)` guard short-circuits over an empty `ConnTable`, so
// the `make_plain_bridge` tests above cannot reach them). The harness drives a
// real client↔server handshake by ferrying datagrams, parks the SERVER
// connection in a `ConnTable` (where a `Bridge` finds it via its `ch`), and
// keeps the CLIENT connection raw so a test can open/write/reset/finish a
// single bidi stream and observe the server bridge's reaction.
// ---------------------------------------------------------------------------

use quinn_proto::{ConnectionHandle as QpCh, Endpoint as QuinnEndpoint, Transmit as QpTransmit};

use crate::quic::crypto::tests::{test_client, test_endpoint_config, test_server};

const CLIENT_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6500);
const SERVER_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6501);

/// One real quinn client connection + one real quinn server connection, each
/// driven directly (no `QuicEndpoint` coordinator). The server connection is
/// parked in `server_conns` under `server_ch` so a `Bridge` can pump it.
struct RawQuicPair {
  client_ep: QuinnEndpoint,
  server_ep: QuinnEndpoint,
  client_conn: quinn_proto::Connection,
  client_ch: QpCh,
  server_conns: ConnTable,
  server_ch: QpCh,
  now: std::time::Instant,
}

impl RawQuicPair {
  /// Mint a client+server endpoint pair and drive the handshake to
  /// Established on both sides by ferrying datagrams.
  fn handshaked() -> Self {
    Self::handshaked_with_transport(quinn_proto::TransportConfig::default())
  }

  /// `handshaked` with a caller-supplied `TransportConfig` (e.g. a tiny
  /// `stream_receive_window` to force flow-control back-pressure on a write).
  fn handshaked_with_transport(transport: quinn_proto::TransportConfig) -> Self {
    let cfg = super::super::crypto::QuicOptions::new(
      test_endpoint_config(&[9u8; 32]),
      test_server(),
      test_client(),
      transport,
      "localhost",
      super::super::UnreliableTransport::Datagram,
    );
    let mut client_ep = QuinnEndpoint::new(cfg.endpoint_arc(), None, true, Some([0x42; 32]));
    let mut server_ep = QuinnEndpoint::new(
      cfg.endpoint_arc(),
      Some(cfg.server_arc()),
      true,
      Some([0x24; 32]),
    );
    let now = Instant::now().into_std();
    let (client_ch, client_conn) = client_ep
      .connect(now, cfg.client().clone(), SERVER_ADDR, "localhost")
      .expect("client dial");

    let mut server_conns = ConnTable::new();
    let mut client_conn = client_conn;
    let mut server_ch: Option<QpCh> = None;

    // Ferry datagrams both ways until both connections are Established.
    for _ in 0..200 {
      // Client → server.
      let mut buf = Vec::new();
      while let Some(t) = client_conn.poll_transmit(now, 1, &mut buf) {
        Self::deliver(
          &mut server_ep,
          &mut server_conns,
          &mut server_ch,
          CLIENT_ADDR,
          &t,
          &buf,
          now,
        );
        buf.clear();
      }
      // Server → client.
      if let Some(sch) = server_ch {
        let mut sbuf = Vec::new();
        if let Some(e) = server_conns.get_mut(sch) {
          while let Some(t) = e.conn_mut().poll_transmit(now, 1, &mut sbuf) {
            let data = bytes::BytesMut::from(&sbuf[..t.size]);
            let mut scratch = Vec::new();
            if let Some(ev) = client_ep.handle(now, SERVER_ADDR, None, None, data, &mut scratch) {
              Self::apply_client_event(&mut client_conn, client_ch, ev);
            }
            sbuf.clear();
          }
        }
      }
      // Drain endpoint events on both sides so CID issuance completes.
      Self::pump_endpoint_events(&mut client_ep, &mut client_conn, client_ch);
      if let Some(sch) = server_ch
        && let Some(e) = server_conns.get_mut(sch)
      {
        while let Some(ev) = e.conn_mut().poll_endpoint_events() {
          if ev.is_drained() {
            continue;
          }
          if let Some(cev) = server_ep.handle_event(sch, ev) {
            e.conn_mut().handle_event(cev);
          }
        }
      }
      client_conn.handle_timeout(now);
      if let Some(sch) = server_ch
        && let Some(e) = server_conns.get_mut(sch)
      {
        e.conn_mut().handle_timeout(now);
      }
      let client_up = !client_conn.is_handshaking() && !client_conn.is_closed();
      let server_up = server_ch
        .and_then(|sch| server_conns.get(sch))
        .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
        .unwrap_or(false);
      if client_up && server_up {
        break;
      }
    }

    let server_ch = server_ch.expect("server must have accepted the connection");
    assert!(
      !client_conn.is_handshaking(),
      "client connection must reach Established"
    );
    assert!(
      server_conns
        .get(server_ch)
        .map(|e| !e.conn_ref().is_handshaking())
        .unwrap_or(false),
      "server connection must reach Established"
    );
    Self {
      client_ep,
      server_ep,
      client_conn,
      client_ch,
      server_conns,
      server_ch,
      now,
    }
  }

  /// Route a client-originated datagram into the server endpoint, accepting a
  /// brand-new connection on first contact.
  fn deliver(
    server_ep: &mut QuinnEndpoint,
    server_conns: &mut ConnTable,
    server_ch: &mut Option<QpCh>,
    from: SocketAddr,
    t: &QpTransmit,
    buf: &[u8],
    now: std::time::Instant,
  ) {
    let data = bytes::BytesMut::from(&buf[..t.size]);
    let mut scratch = Vec::new();
    let Some(ev) = server_ep.handle(now, from, None, None, data, &mut scratch) else {
      return;
    };
    match ev {
      quinn_proto::DatagramEvent::ConnectionEvent(ch, cev) => {
        if let Some(e) = server_conns.get_mut(ch) {
          e.conn_mut().handle_event(cev);
        }
      }
      quinn_proto::DatagramEvent::NewConnection(incoming) => {
        let mut abuf = Vec::new();
        if let Ok((ch, conn)) = server_ep.accept(incoming, now, &mut abuf, None) {
          server_conns.insert_accepted(ch, conn, from);
          *server_ch = Some(ch);
        }
      }
      quinn_proto::DatagramEvent::Response(_) => {}
    }
  }

  fn apply_client_event(
    client_conn: &mut quinn_proto::Connection,
    client_ch: QpCh,
    ev: quinn_proto::DatagramEvent,
  ) {
    if let quinn_proto::DatagramEvent::ConnectionEvent(ch, cev) = ev {
      debug_assert_eq!(ch, client_ch);
      client_conn.handle_event(cev);
    }
  }

  fn pump_endpoint_events(
    client_ep: &mut QuinnEndpoint,
    client_conn: &mut quinn_proto::Connection,
    client_ch: QpCh,
  ) {
    while let Some(ev) = client_conn.poll_endpoint_events() {
      if ev.is_drained() {
        continue;
      }
      if let Some(cev) = client_ep.handle_event(client_ch, ev) {
        client_conn.handle_event(cev);
      }
    }
  }

  /// Open a bidi stream on the CLIENT and return its `StreamId`.
  fn client_open_bi(&mut self) -> QuicSid {
    self
      .client_conn
      .streams()
      .open(Dir::Bi)
      .expect("client opens a bidi stream")
  }

  /// Write `bytes` on the client's send half of `sid`.
  fn client_write(&mut self, sid: QuicSid, bytes: &[u8]) {
    let mut off = 0;
    while off < bytes.len() {
      match self.client_conn.send_stream(sid).write(&bytes[off..]) {
        Ok(n) => off += n,
        Err(quinn_proto::WriteError::Blocked) => {
          self.ferry();
          // After a ferry the peer may have credited us; retry.
        }
        Err(e) => panic!("client write failed: {e:?}"),
      }
    }
  }

  /// Reset the client's send half of `sid` (peer's recv half sees
  /// RESET_STREAM and `pump_in` reads `ReadError::Reset`).
  fn client_reset(&mut self, sid: QuicSid) {
    // Ignoring Err: `reset` returns `Err(ClosedStream)` only if the half is
    // already gone; the test asserts on the peer-side effect, not this call.
    let _ = self.client_conn.send_stream(sid).reset(VarInt::from_u32(0));
  }

  /// Finish the client's send half of `sid` (peer's recv half sees FIN).
  fn client_finish(&mut self, sid: QuicSid) {
    // Ignoring Err: `finish` returns `Err` only if already finished/reset; the
    // test asserts on the peer observing FIN, not this call's result.
    let _ = self.client_conn.send_stream(sid).finish();
  }

  /// Ferry every queued datagram both ways once and advance both connections
  /// one timer tick. Drives client-written stream bytes to the server and the
  /// server's responses (ACKs, RESET_STREAM, …) back to the client.
  fn ferry(&mut self) {
    let now = self.now;
    let mut buf = Vec::new();
    while let Some(t) = self.client_conn.poll_transmit(now, 1, &mut buf) {
      Self::deliver(
        &mut self.server_ep,
        &mut self.server_conns,
        &mut Some(self.server_ch),
        CLIENT_ADDR,
        &t,
        &buf,
        now,
      );
      buf.clear();
    }
    let sch = self.server_ch;
    let mut sbuf = Vec::new();
    if let Some(e) = self.server_conns.get_mut(sch) {
      while let Some(t) = e.conn_mut().poll_transmit(now, 1, &mut sbuf) {
        let data = bytes::BytesMut::from(&sbuf[..t.size]);
        let mut scratch = Vec::new();
        if let Some(ev) = self
          .client_ep
          .handle(now, SERVER_ADDR, None, None, data, &mut scratch)
        {
          Self::apply_client_event(&mut self.client_conn, self.client_ch, ev);
        }
        sbuf.clear();
      }
    }
    Self::pump_endpoint_events(&mut self.client_ep, &mut self.client_conn, self.client_ch);
    if let Some(e) = self.server_conns.get_mut(sch) {
      while let Some(ev) = e.conn_mut().poll_endpoint_events() {
        if ev.is_drained() {
          continue;
        }
        if let Some(cev) = self.server_ep.handle_event(sch, ev) {
          e.conn_mut().handle_event(cev);
        }
      }
    }
    self.client_conn.handle_timeout(now);
    if let Some(e) = self.server_conns.get_mut(sch) {
      e.conn_mut().handle_timeout(now);
    }
  }

  /// Accept the server's next inbound bidi stream id (ferrying until it
  /// appears).
  fn server_accept_bi(&mut self) -> QuicSid {
    for _ in 0..200 {
      let sch = self.server_ch;
      if let Some(e) = self.server_conns.get_mut(sch)
        && let Some(sid) = e.conn_mut().streams().accept(Dir::Bi)
      {
        return sid;
      }
      self.ferry();
    }
    panic!("server never accepted an inbound bidi stream");
  }

  /// Build a `Bridge` over the server's accepted bidi `sid` (inbound role,
  /// no label). The bridge's inner FSM is a fresh `accept_stream`.
  fn server_bridge(
    &mut self,
    ep: &mut Endpoint<SmolStr, SocketAddr>,
    sid: QuicSid,
  ) -> Bridge<SmolStr, SocketAddr> {
    let stream = ep
      .accept_stream(CLIENT_ADDR, Instant::from_std(self.now))
      .expect("node is running");
    Bridge::new(
      stream,
      self.server_ch,
      sid,
      crate::CompressionOptions::new(),
      crate::EncryptionOptions::new(),
      ep.max_stream_frame_size(),
      None,
      false,
      false,
    )
  }
}

fn make_server_endpoint() -> Endpoint<SmolStr, SocketAddr> {
  Endpoint::new_seeded(EndpointOptions::new(SmolStr::new("server"), SERVER_ADDR))
}

/// `pump_in` over a live quinn stream: an over-ceiling `unit_len` (a forged
/// varint above `reliable_max`) makes `take_reliable_unit_with_encryption`
/// return `Err` inside the unit loop, driving the `unit_err` → `decode_failed`
/// path that retires both halves and transitions the bridge to
/// `Failed(Decode)`. Reaches the `take_reliable_unit` `Err(_)` arm and the
/// post-borrow `decode_failed` retire block.
#[test]
fn pump_in_over_ceiling_unit_len_fails_decode() {
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  // Forge a `[unit_len: varint]` far above any plausible `reliable_max`, with
  // no payload — `take_reliable_unit` rejects the unit before reading bytes.
  let mut forged = Vec::new();
  crate::framing::encode_varint_u32(u32::MAX, &mut forged);
  pair.client_write(sid, &forged);
  pair.client_finish(sid);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_sid);

  // Pump the forged unit through the live server stream.
  let mut result = Ok(());
  for _ in 0..50 {
    result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if result.is_err() {
      break;
    }
    pair.ferry();
  }
  assert_eq!(
    result,
    Err(()),
    "an over-ceiling unit_len must fail pump_in via the decode-fail retire path"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
    "a forged over-ceiling unit_len terminalizes the bridge as Failed(Decode) — got {:?}",
    bridge.phase
  );
}

/// `pump_in` over a live quinn stream: a well-FRAMED reliable unit whose
/// payload is not a decodable memberlist `Message` makes `Stream::handle_data`
/// return `Err` inside the unit loop (the `decode_failed = true; break` arm),
/// then the post-borrow `decode_failed` block retires both halves and sets
/// `Failed(Decode)`.
#[test]
fn pump_in_undecodable_unit_payload_fails_decode() {
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  // A structurally valid `[unit_len][payload]` (so `take_reliable_unit`
  // succeeds) whose 64-byte payload is not a valid `Message` (so the FSM's
  // `handle_data` rejects it).
  let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[0xffu8; 64]);
  pair.client_write(sid, &unit);
  pair.client_finish(sid);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_sid);

  let mut result = Ok(());
  for _ in 0..50 {
    result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if result.is_err() {
      break;
    }
    pair.ferry();
  }
  assert_eq!(
    result,
    Err(()),
    "an undecodable unit payload must fail pump_in via handle_data → decode_failed"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
    "an undecodable unit payload terminalizes the bridge as Failed(Decode) — got {:?}",
    bridge.phase
  );
}

/// `pump_in` over a live quinn stream: the peer FINs in the middle of a
/// reliable unit (writes a partial `[unit_len][..]` then finishes). At EOF the
/// trailing partial unit in `recv_accum` is a truncated transmission — the
/// `fin_seen && !recv_accum.is_empty()` arm flips `decode_failed`, and the
/// retire block terminalizes the bridge `Failed(Decode)`.
#[test]
fn pump_in_truncated_unit_at_fin_fails_decode() {
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  // A full unit's prefix: declare a 64-byte payload but deliver only 8 bytes,
  // then FIN. The recv side decodes the varint length, waits for 64 bytes,
  // and at FIN finds a partial unit → truncated.
  let full = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[7u8; 64]);
  // A 64-byte payload yields a unit well over 10 bytes; send only the first 10
  // (a partial `[unit_len][..]`) so the recv side waits for the rest, then
  // FINs.
  assert!(full.len() > 10);
  let partial = &full[..10];
  pair.client_write(sid, partial);
  pair.client_finish(sid);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_sid);

  let mut result = Ok(());
  for _ in 0..50 {
    result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if result.is_err() {
      break;
    }
    pair.ferry();
  }
  assert_eq!(
    result,
    Err(()),
    "a FIN mid-unit must be treated as a truncated transmission (decode fail)"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
    "a truncated unit at FIN terminalizes the bridge as Failed(Decode) — got {:?}",
    bridge.phase
  );
}

/// `pump_in` over a live quinn stream: the peer FINs while the bridge's FSM
/// still expects inbound bytes (the request stream is opened and FINned with
/// NO data). The `fin_seen && handle_data(&[]).is_err()` premature-FIN arm
/// (the FSM's `PeerClosed` entry) flips `decode_failed` and the bridge
/// terminalizes `Failed(Decode)`.
#[test]
fn pump_in_premature_fin_with_no_data_fails_decode() {
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  // Open + immediately FIN with no bytes. The server FSM (an inbound stream
  // awaiting its first message) sees an empty-buffer EOF → PeerClosed.
  pair.client_finish(sid);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_sid);

  let mut result = Ok(());
  for _ in 0..50 {
    result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if result.is_err() {
      break;
    }
    pair.ferry();
  }
  assert_eq!(
    result,
    Err(()),
    "a premature FIN with no data must route through the FSM PeerClosed failure"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
    "a premature empty FIN terminalizes the bridge as Failed(Decode) — got {:?}",
    bridge.phase
  );
}

/// `pump_in` over a live quinn stream: the peer RESETs its send half, so the
/// bridge's ordered read returns `ReadError::Reset`, driving the `reset`
/// retire block (RESET + STOP + `Failed(Transport)`).
#[test]
fn pump_in_peer_reset_recv_fails_transport() {
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  // Write a partial unit then RESET — the recv side sees the bytes followed by
  // a RESET_STREAM rather than a clean FIN.
  let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[2u8; 24]);
  // Send only the first few bytes (a partial unit) before resetting, so the
  // recv side sees bytes followed by a RESET_STREAM rather than a clean FIN.
  assert!(unit.len() > 6);
  pair.client_write(sid, &unit[..6]);
  pair.ferry();
  pair.client_reset(sid);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_sid);

  let mut result = Ok(());
  for _ in 0..50 {
    result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if result.is_err() {
      break;
    }
    pair.ferry();
  }
  assert_eq!(
    result,
    Err(()),
    "a peer RESET_STREAM on the recv half must fail pump_in via the reset path"
  );
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::Transport(_))
    ),
    "a peer reset terminalizes the bridge as Failed(Transport) — got {:?}",
    bridge.phase
  );
}

/// `pump_in` over a live quinn stream: a prior UNORDERED read on the recv
/// half makes the bridge's ordered `RecvStream::read(true)` return
/// `ReadableError::IllegalOrderedRead`, driving the `illegal_ordered` retire
/// block (RESET + STOP + `Failed(Transport)`).
#[test]
fn pump_in_illegal_ordered_read_fails_transport() {
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &[1u8; 16]);
  pair.client_write(sid, &unit);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_sid);

  // Ferry so the client bytes are buffered on the server recv stream, then do
  // a poisoning UNORDERED read on the SAME recv half. The bridge's subsequent
  // ordered `read(true)` then trips `IllegalOrderedRead`.
  for _ in 0..50 {
    pair.ferry();
    let mut buffered = false;
    if let Some(e) = pair.server_conns.get_mut(pair.server_ch) {
      let mut rs = e.conn_mut().recv_stream(server_sid);
      if let Ok(mut chunks) = rs.read(false) {
        if matches!(chunks.next(usize::MAX), Ok(Some(_))) {
          buffered = true;
        }
        // Ignoring Err: the unordered-read finalize wakeup is unused here.
        let _ = chunks.finalize();
      }
    }
    if buffered {
      break;
    }
  }

  let result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
  assert_eq!(
    result,
    Err(()),
    "an ordered read after an unordered read must fail via IllegalOrderedRead"
  );
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::Transport(_))
    ),
    "an illegal ordered read terminalizes the bridge as Failed(Transport) — got {:?}",
    bridge.phase
  );
}

/// Build a LABELED inbound bridge (acceptor role, lazy outbound disclosure)
/// over the server's accepted bidi `sid`.
fn server_labeled_bridge(
  pair: &mut RawQuicPair,
  ep: &mut Endpoint<SmolStr, SocketAddr>,
  sid: QuicSid,
  label: bytes::Bytes,
) -> Bridge<SmolStr, SocketAddr> {
  let stream = ep
    .accept_stream(CLIENT_ADDR, Instant::from_std(pair.now))
    .expect("node is running");
  Bridge::new(
    stream,
    pair.server_ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    Some(label),
    false,
    false,
  )
}

/// Build a LABELED outbound DIALER bridge (eager outbound disclosure) over the
/// server's open bidi `sid`.
fn server_labeled_dialer_bridge(
  pair: &mut RawQuicPair,
  ep: &mut Endpoint<SmolStr, SocketAddr>,
  sid: QuicSid,
  label: bytes::Bytes,
) -> Bridge<SmolStr, SocketAddr> {
  let stream = ep
    .accept_stream(CLIENT_ADDR, Instant::from_std(pair.now))
    .expect("node is running");
  Bridge::new(
    stream,
    pair.server_ch,
    sid,
    crate::CompressionOptions::new(),
    crate::EncryptionOptions::new(),
    ep.max_stream_frame_size(),
    Some(label),
    false,
    true,
  )
}

/// `pump_out` over a live quinn stream for a LABELED dialer: the first
/// `pump_out` writes the eager outbound label frame into `pending_out` (the
/// `!outbound_label_written && eager_outbound_label` arm) and the
/// `pending_out` flush writes it onto the quinn send stream. After this the
/// `outbound_label_written` latch is set and `pending_out` has drained.
#[test]
fn pump_out_labeled_dialer_writes_eager_label_over_live_stream() {
  let label = bytes::Bytes::from_static(b"cluster-out");
  let mut pair = RawQuicPair::handshaked();
  // The server opens a bidi (live send half) for the dialer bridge to write on.
  let server_open_sid = {
    let e = pair
      .server_conns
      .get_mut(pair.server_ch)
      .expect("server connection present");
    e.conn_mut()
      .streams()
      .open(Dir::Bi)
      .expect("server opens a bidi")
  };
  let mut ep = make_server_endpoint();
  let mut bridge = server_labeled_dialer_bridge(&mut pair, &mut ep, server_open_sid, label.clone());
  assert!(
    !bridge.outbound_label_written(),
    "a labeled dialer starts with the outbound-label latch unset"
  );

  let result = bridge.pump_out(&mut pair.server_conns, Instant::from_std(pair.now));
  assert_eq!(
    result,
    Ok(()),
    "the eager-label write + flush must succeed on a healthy send half"
  );
  assert!(
    bridge.outbound_label_written(),
    "the first pump_out must write the eager outbound label frame and set the latch"
  );
  assert!(
    bridge.pending_out_bytes().is_empty(),
    "the label frame must flush onto the send stream (no back-pressure on a fresh stream)"
  );

  // Confirm the label bytes actually reached the peer: ferry, accept the bidi
  // on the client, and read the label header off the front.
  let mut client_recv = Vec::new();
  for _ in 0..50 {
    pair.ferry();
    if let Some(csid) = pair.client_conn.streams().accept(Dir::Bi) {
      if let Ok(mut chunks) = pair.client_conn.recv_stream(csid).read(true) {
        while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
          client_recv.extend_from_slice(&chunk.bytes);
        }
        // Ignoring Err: the read finalize wakeup is unused here.
        let _ = chunks.finalize();
      }
      break;
    }
  }
  let expected_header = build_label_header(&label);
  assert!(
    client_recv.starts_with(&expected_header),
    "the eager label frame must be the FIRST bytes on the wire — got {client_recv:?}"
  );
}

/// `pump_in` over a live quinn stream for a LABELED bridge: the peer writes
/// the matching label header followed by a reliable unit. The inline label
/// classifier (the `if !self.inbound_label_validated` block inside the chunk
/// loop) drains the header, latches `inbound_label_validated`, and the
/// acceptor's lazy outbound-label disclosure prepends its own label to
/// `pending_out`. The trailing reliable unit then dispatches into the FSM.
#[test]
fn pump_in_labeled_validates_inbound_label_over_live_stream() {
  let label = bytes::Bytes::from_static(b"cluster-quic");
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  // The dialer writes its label header first, then one reliable unit carrying
  // a valid Ping message (so the FSM accepts it cleanly after the header).
  let mut wire = Vec::new();
  crate::label::encode_label_prefix(&label, &mut wire);
  let ping = crate::typed::Message::<SmolStr, SocketAddr>::Ping(crate::typed::Ping::new(
    1,
    crate::typed::Node::new(SmolStr::new("client"), CLIENT_ADDR),
    crate::typed::Node::new(SmolStr::new("server"), SERVER_ADDR),
  ));
  let framed = crate::wire::encode_message::<SmolStr, SocketAddr>(&ping).expect("encode ping");
  let unit = crate::encode_reliable_unit(&crate::CompressionOptions::new(), &framed);
  wire.extend_from_slice(&unit);
  pair.client_write(sid, &wire);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = server_labeled_bridge(&mut pair, &mut ep, server_sid, label);
  assert!(
    !bridge.inbound_label_validated(),
    "a labeled bridge starts with the inbound latch unset"
  );

  // Pump until the label header validates (the inline classifier consumes it
  // and latches).
  for _ in 0..50 {
    // Ignoring Err: this loop drives the pump and inspects the bridge's
    // latch/phase below; a terminal Err is itself the asserted outcome.
    let _ = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if bridge.inbound_label_validated() {
      break;
    }
    pair.ferry();
  }
  assert!(
    bridge.inbound_label_validated(),
    "the inline label classifier must validate the matching inbound header \
       and latch over a live stream"
  );
  assert!(
    bridge.outbound_label_written(),
    "an acceptor's lazy outbound-label disclosure must arm once the peer's \
       label validates"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Active),
    "a matching label + valid reliable unit keeps the bridge non-failed — got {:?}",
    bridge.phase
  );
}

/// `pump_in` over a live quinn stream for a LABELED bridge whose peer sends a
/// MISMATCHED label header: the inline classifier returns
/// `LabelOutcome::Rejected`, flipping `decode_failed` and routing the bridge
/// through the decode-fail retire block (`Failed(Decode)`).
#[test]
fn pump_in_labeled_mismatched_inbound_label_rejected_over_live_stream() {
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  // The peer writes a DIFFERENT label than the bridge expects.
  let mut wire = Vec::new();
  crate::label::encode_label_prefix(b"wrong-cluster", &mut wire);
  pair.client_write(sid, &wire);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = server_labeled_bridge(
    &mut pair,
    &mut ep,
    server_sid,
    bytes::Bytes::from_static(b"expected-cluster"),
  );

  let mut result = Ok(());
  for _ in 0..50 {
    result = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if result.is_err() {
      break;
    }
    pair.ferry();
  }
  assert_eq!(
    result,
    Err(()),
    "a mismatched inbound label must fail pump_in via LabelOutcome::Rejected"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Decode)),
    "a mismatched inbound label terminalizes the bridge as Failed(Decode) — got {:?}",
    bridge.phase
  );
}

/// `pump_in` over a live quinn stream for a LABELED bridge whose peer delivers
/// the label header SPLIT across two writes: the first `pump_in` sees only a
/// partial header → `LabelOutcome::Incomplete` (the `break` that holds
/// `recv_accum` for the next chunk), and a later `pump_in` completes it and
/// latches `inbound_label_validated`.
#[test]
fn pump_in_labeled_split_label_header_incompletes_then_validates() {
  let label = bytes::Bytes::from_static(b"split-cluster");
  let mut pair = RawQuicPair::handshaked();
  let sid = pair.client_open_bi();
  let header = build_label_header(&label);
  assert!(header.len() >= 2, "a label header is at least [tag][len]");
  // Write ONLY the first byte (the tag); the classifier needs more → Incomplete.
  pair.client_write(sid, &header[..1]);

  let server_sid = pair.server_accept_bi();
  let mut ep = make_server_endpoint();
  let mut bridge = server_labeled_bridge(&mut pair, &mut ep, server_sid, label);

  // Drive until the partial header has arrived and been pumped (Incomplete
  // break holds it). The latch must still be UNSET.
  let mut pumped_partial = false;
  for _ in 0..50 {
    pair.ferry();
    // Ignoring Err: this loop drives the pump and inspects the bridge's
    // latch/phase below; a terminal Err is itself the asserted outcome.
    let _ = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    // The test module is a child of this module, so it reads the private
    // `recv_accum` directly: the Incomplete break holds the partial header
    // here rather than draining it.
    if !bridge.recv_accum.is_empty() {
      pumped_partial = true;
      break;
    }
  }
  assert!(
    pumped_partial,
    "the partial label header (1 byte) must reach recv_accum and be held by \
       the Incomplete break"
  );
  assert!(
    !bridge.inbound_label_validated(),
    "a partial label header must NOT latch — it Incompletes and waits for more"
  );

  // Now write the REST of the header; a later pump_in completes it and latches.
  pair.client_write(sid, &header[1..]);
  for _ in 0..50 {
    pair.ferry();
    // Ignoring Err: this loop drives the pump and inspects the bridge's
    // latch/phase below; a terminal Err is itself the asserted outcome.
    let _ = bridge.pump_in(&mut pair.server_conns, Instant::from_std(pair.now));
    if bridge.inbound_label_validated() {
      break;
    }
  }
  assert!(
    bridge.inbound_label_validated(),
    "the completing chunk must latch the inbound label after the Incomplete hold"
  );
}

/// `pump_out` deadline enforcement over a live quinn stream: a non-terminal
/// bridge with a back-pressured `pending_out` tail whose inner-stream deadline
/// has elapsed routes through the pre-write deadline check
/// (`stream.poll_timeout() <= now`), which drives `handle_timeout`, retires
/// both halves, and transitions the bridge to `Failed(Timeout)`.
#[test]
fn pump_out_deadline_with_pending_out_tail_fails_timeout() {
  let mut pair = RawQuicPair::handshaked();
  // The server opens a bidi (so it is the outbound dialer with a live send
  // half) and accepts a fresh outbound FSM stream whose deadline we can read.
  let server_open_sid = {
    let e = pair
      .server_conns
      .get_mut(pair.server_ch)
      .expect("server connection present");
    e.conn_mut()
      .streams()
      .open(Dir::Bi)
      .expect("server opens a bidi")
  };
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_open_sid);

  // Stage a back-pressured tail and pin the inner stream's deadline in the
  // past so the pre-write deadline check fires on the next pump_out.
  bridge.pending_out.extend_from_slice(b"back-pressured tail");
  let past = Instant::from_std(pair.now);
  // The inner FSM's deadline must be `Some` and `<= now`. A fresh inbound
  // accept_stream carries an exchange deadline at `now + stream_timeout`; we
  // pump at a `now` far in the future so `poll_timeout() <= now`.
  let future = past + core::time::Duration::from_secs(3600);
  assert!(
    bridge.stream.poll_timeout().is_some(),
    "a pre-Done stream must expose its exchange deadline"
  );

  let result = bridge.pump_out(&mut pair.server_conns, future);
  assert_eq!(
    result,
    Err(()),
    "a back-pressured tail past the exchange deadline must fail pump_out"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Failed(BridgeFailure::Timeout)),
    "the pre-write deadline check must terminalize the bridge as \
       Failed(Timeout) — got {:?}",
    bridge.phase
  );
  assert!(
    bridge.pending_out.is_empty(),
    "the back-pressured tail must be cleared so no post-deadline bytes are written"
  );
}

/// The back-pressure safety of `pump_out`: when the stream is flow-control
/// blocked, the staged `pending_out` flush RETAINS the remainder intact and
/// returns `Ok` (the bridge waits for credit) rather than losing it, growing
/// it, or failing the exchange — and `pump_out` returns at the flush WITHOUT
/// reaching the gather loop, so a new unit is never started while a remainder
/// is outstanding (the one-unit-in-flight invariant). The companion of the
/// deadline test: same staged tail, but pumped before the deadline against a
/// saturated send window.
#[test]
fn pump_out_blocked_pending_out_flush_retains_tail_and_waits() {
  let mut pair = RawQuicPair::handshaked();
  let server_open_sid = {
    let e = pair
      .server_conns
      .get_mut(pair.server_ch)
      .expect("server connection present");
    e.conn_mut()
      .streams()
      .open(Dir::Bi)
      .expect("server opens a bidi")
  };
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_open_sid);

  // Saturate the stream's flow-control window so the next write blocks.
  let big = vec![0u8; 64 * 1024];
  loop {
    let e = pair
      .server_conns
      .get_mut(pair.server_ch)
      .expect("server connection present");
    match e.conn_mut().send_stream(server_open_sid).write(&big) {
      Ok(_) => continue,
      Err(quinn_proto::WriteError::Blocked) => break,
      Err(other) => panic!("unexpected write error while saturating the window: {other:?}"),
    }
  }

  // Stage a back-pressured tail and pump BEFORE the exchange deadline.
  bridge.pending_out.extend_from_slice(b"back-pressured tail");
  let result = bridge.pump_out(&mut pair.server_conns, Instant::from_std(pair.now));

  assert_eq!(
    result,
    Ok(()),
    "a flow-control-blocked flush waits for credit; it does not fail"
  );
  assert_eq!(
    bridge.pending_out_bytes(),
    b"back-pressured tail",
    "the blocked flush retains the staged tail intact — no loss, no growth, no duplication"
  );
  assert!(
    !matches!(bridge.phase, BridgePhase::Failed(_)),
    "a flow-control block is normal back-pressure, not a failure: phase = {:?}",
    bridge.phase
  );
}

/// Full multi-pump round-trip: a reliable unit LARGER than the stream's
/// flow-control window is staged in `pending_out` (a prior pump's gathered-
/// then-Blocked remainder) and streamed to the peer across several
/// flow-control-limited `pump_out` flushes. Each flush writes up to one
/// window; the peer reads (granting credit); the next flush replays the
/// remainder head-first. The unit must arrive EXACTLY once — reassembling
/// intact with no duplication and no loss.
#[test]
fn pump_out_back_pressured_unit_streams_across_windows_and_reassembles() {
  use crate::{CompressionOptions, encode_reliable_unit, take_reliable_unit};

  // A tiny per-stream receive window so a multi-hundred-byte unit needs many
  // flow-control-limited pumps to drain.
  const WINDOW: u32 = 256;
  let mut transport = quinn_proto::TransportConfig::default();
  transport.stream_receive_window(VarInt::from_u32(WINDOW));
  let mut pair = RawQuicPair::handshaked_with_transport(transport);

  let server_open_sid = {
    let e = pair
      .server_conns
      .get_mut(pair.server_ch)
      .expect("server connection present");
    e.conn_mut()
      .streams()
      .open(Dir::Bi)
      .expect("server opens a bidi")
  };
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_open_sid);

  // A real reliable unit several windows long, staged as a back-pressured
  // remainder — exactly what pump_out's gather-then-Blocked arm leaves behind.
  let payload = b"the quick brown fox jumps over the lazy dog. ".repeat(48);
  let unit = encode_reliable_unit(&CompressionOptions::new(), &payload);
  assert!(
    unit.len() > 3 * WINDOW as usize,
    "the unit must span several windows to exercise back-pressure ({} bytes)",
    unit.len()
  );
  bridge.pending_out.extend_from_slice(&unit);

  // Flush up to a window, ferry it to the peer, the peer reads (granting
  // credit), repeat until pending_out drains.
  let now = Instant::from_std(pair.now);
  let mut client_recv = Vec::new();
  let mut csid = None;
  let mut progress_pumps = 0;
  for _ in 0..200 {
    if bridge.pending_out_bytes().is_empty() {
      break;
    }
    let before = bridge.pending_out_bytes().len();
    bridge
      .pump_out(&mut pair.server_conns, now)
      .expect("a flow-control-blocked flush returns Ok, never fails");
    if bridge.pending_out_bytes().len() < before {
      progress_pumps += 1;
    }
    pair.ferry();
    if csid.is_none() {
      csid = pair.client_conn.streams().accept(Dir::Bi);
    }
    if let Some(id) = csid
      && let Ok(mut chunks) = pair.client_conn.recv_stream(id).read(true)
    {
      while let Ok(Some(chunk)) = chunks.next(usize::MAX) {
        client_recv.extend_from_slice(&chunk.bytes);
      }
      // Ignoring Err: the read-finalize wakeup is unused here.
      let _ = chunks.finalize();
    }
    pair.ferry();
  }

  assert!(
    bridge.pending_out_bytes().is_empty(),
    "the unit fully drained across the windowed transfer"
  );
  assert!(
    progress_pumps >= 3,
    "the tiny window genuinely forced a multi-pump transfer — got {progress_pumps} flushes"
  );

  // The peer received EXACTLY the unit: no duplication (replay-head-first never
  // double-writes), no loss, and it reassembles to the original payload.
  assert_eq!(
    client_recv.len(),
    unit.len(),
    "exactly the unit's bytes reached the wire — no duplication, no loss"
  );
  let (back, consumed) = take_reliable_unit(&client_recv, 16 * 1024 * 1024)
    .expect("decode ok")
    .expect("a complete unit");
  assert_eq!(consumed, unit.len(), "the whole unit was consumed");
  assert_eq!(
    back, payload,
    "the unit reassembled to the original payload across the windowed transfer"
  );
}

/// `pump_out`'s post-`poll_transmit` FSM-failure retire (the
/// `if let Some(e) = self.stream.is_failed()` block AFTER the gather loop):
/// an outbound stream with an EMPTY `pending_out` (so the pre-write deadline
/// check is skipped) whose exchange deadline has elapsed self-fails INSIDE
/// `poll_transmit`; the post-loop check then retires both halves and sets
/// `Failed(Transport)`.
#[test]
fn pump_out_post_transmit_fsm_timeout_retires() {
  let mut pair = RawQuicPair::handshaked();
  let server_open_sid = {
    let e = pair
      .server_conns
      .get_mut(pair.server_ch)
      .expect("server connection present");
    e.conn_mut()
      .streams()
      .open(Dir::Bi)
      .expect("server opens a bidi")
  };
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_open_sid);
  // `pending_out` is empty (a fresh bridge), so the pre-write deadline check
  // (`!pending_out.is_empty()`) is skipped and the FSM must self-fail inside
  // `poll_transmit` at the future `now`.
  assert!(bridge.pending_out.is_empty());
  let future = Instant::from_std(pair.now) + core::time::Duration::from_secs(3600);

  let result = bridge.pump_out(&mut pair.server_conns, future);
  assert_eq!(
    result,
    Err(()),
    "an empty-pending_out outbound stream past its deadline must fail via the \
       post-poll_transmit FSM-failure retire"
  );
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::Transport(_))
    ),
    "the post-poll_transmit `stream.is_failed()` retire must terminalize the \
       bridge as Failed(Transport) — got {:?}",
    bridge.phase
  );
}

/// `pump_out`'s leading terminal-bridge guard, `Some(e)` arm: when the inner
/// FSM has already failed (here via an elapsed exchange deadline) but the
/// BRIDGE phase is still non-`Failed`, the first thing `pump_out` does is map
/// `self.stream.is_failed()` Some → `BridgeFailure::Transport(e.to_string())`,
/// retire both halves, and return `Err(())`.
#[test]
fn pump_out_leading_guard_fsm_failed_retires() {
  let mut bridge = make_plain_bridge();
  let mut conns = ConnTable::new();
  // Fail the inner FSM via its own exchange deadline WITHOUT routing through
  // the bridge's `fail*` helpers, so `stream.is_failed()` is Some while
  // `bridge.phase` is still `Active`.
  let inner_deadline = bridge
    .stream
    .poll_timeout()
    .expect("a fresh stream exposes its exchange deadline");
  bridge.stream.handle_timeout(inner_deadline);
  assert!(bridge.stream.is_failed().is_some());
  assert!(
    matches!(bridge.phase, BridgePhase::Active),
    "the bridge phase has not yet cascaded the FSM failure"
  );

  // The leading guard's `Some(e)` arm fires (the empty ConnTable makes the
  // retire a no-op, but the phase transition still runs).
  let result = bridge.pump_out(&mut conns, inner_deadline);
  assert_eq!(
    result,
    Err(()),
    "pump_out's leading guard must fail an FSM-failed bridge before any write"
  );
  assert!(
    matches!(
      bridge.phase,
      BridgePhase::Failed(BridgeFailure::Transport(_))
    ),
    "the leading guard's `Some(e)` arm maps the FSM error to \
       Failed(Transport) — got {:?}",
    bridge.phase
  );
}

/// Build an inbound bridge over a default-ACCEPTING endpoint that has already
/// decoded a PushPull join request (FSM in `InboundSendingResponse`,
/// `PushPullRequestReceived{Join}` queued). The bridge rides a live server
/// stream so its `pump_out` can actually write the response.
fn inbound_join_bridge_accepting(
  pair: &mut RawQuicPair,
  server_sid: QuicSid,
) -> (Endpoint<SmolStr, SocketAddr>, Bridge<SmolStr, SocketAddr>) {
  let mut ep = make_server_endpoint();
  let mut bridge = pair.server_bridge(&mut ep, server_sid);
  // Dispatch a join PushPull request directly into the FSM (no rejecting
  // delegate is set, so `handle_stream_event` will return
  // `SendPushPullResponse`).
  let dave = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7004);
  let dave_state =
    crate::typed::PushNodeState::new(1, SmolStr::new("dave"), dave, crate::typed::State::Alive);
  let pp =
    crate::typed::PushPull::new(true, core::iter::once(dave_state)).with_user_data(Bytes::new());
  let bytes =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::PushPull(pp)).expect("encode");
  bridge
    .stream
    .handle_data(&bytes, Instant::from_std(pair.now))
    .expect("dispatch the join push/pull request");
  (ep, bridge)
}

/// `drain_then_reap`'s `StreamCommand::SendPushPullResponse` arm: a TERMINAL
/// inbound bridge that still holds an undrained `PushPullRequestReceived`
/// (queued by the FSM, never drained by `drain_payload_only`) drains it
/// through the accepting endpoint, which returns `SendPushPullResponse`. The
/// arm encodes + `stream_load_response`s the reply, refreshes the bridge
/// deadline, and `pump_out`s the response onto the live quinn stream.
#[test]
fn drain_then_reap_sends_push_pull_response_over_live_stream() {
  let mut pair = RawQuicPair::handshaked();
  let client_sid = pair.client_open_bi();
  // Nudge the client stream into existence on the server so it accepts a bidi.
  pair.client_write(client_sid, b"\x00");
  let server_sid = pair.server_accept_bi();
  let (mut ep, mut bridge) = inbound_join_bridge_accepting(&mut pair, server_sid);

  // Drive the bridge terminal (BothClosed) WITHOUT draining payload events, so
  // the queued `PushPullRequestReceived` is still pending when the terminal D1
  // path runs.
  bridge.observe_send_fin();
  bridge.observe_recv_fin();
  assert!(matches!(bridge.phase, BridgePhase::BothClosed));
  assert!(bridge.is_terminal());

  let deadline_before = bridge.deadline;
  bridge.drain_then_reap(&mut ep, &mut pair.server_conns, Instant::from_std(pair.now));

  // The SendPushPullResponse arm refreshes the bridge deadline to exactly
  // `now + 5s` (the response write window, measured from response start, not
  // from accept). The accept deadline was `now + stream_timeout` (10s by
  // default), so the refresh moves it to a DIFFERENT, earlier value — the
  // observable that the arm ran (a no-op drain would leave the accept
  // deadline untouched).
  let expected = Instant::from_std(pair.now) + core::time::Duration::from_secs(5);
  assert_eq!(
    bridge.deadline, expected,
    "the SendPushPullResponse arm must refresh the bridge deadline to \
       `now + 5s`; deadline_before = {deadline_before:?}"
  );
  // The encoded response was loaded + flushed by the arm's `pump_out`: the
  // inner FSM advanced past `InboundSendingResponse` to `Done` and the send
  // half was finished.
  assert!(
    bridge.stream.is_done(),
    "the inner stream must be Done after the response is encoded, loaded, and \
       pumped out"
  );
  assert!(
    bridge.finish_called,
    "pump_out must have finished the send half after writing the response"
  );
}

/// `drain_then_reap`'s notice selection `(_, Some(e))` arm: a bridge whose
/// inner FSM has failed (here via an elapsed exchange deadline →
/// `Failed(Timeout)` at the FSM level) while the BRIDGE phase is still
/// non-`Failed` emits a `StreamErrored` notice carrying the FSM error string.
/// This is the transient-inconsistency window the phase-authoritative
/// terminality contract tolerates for one drain.
#[test]
fn drain_then_reap_fsm_failed_notice_arm() {
  let mut ep = make_server_endpoint();
  let mut conns = ConnTable::new();
  let mut bridge = make_plain_bridge();
  let t0 = Instant::now();
  // Fail the inner FSM via its own exchange deadline WITHOUT routing through
  // the bridge's `fail*` helpers, so `stream.is_failed()` is Some while
  // `bridge.phase` is still `Active`.
  let inner_deadline = bridge
    .stream
    .poll_timeout()
    .expect("a fresh stream exposes its exchange deadline");
  bridge.stream.handle_timeout(inner_deadline);
  assert!(
    bridge.stream.is_failed().is_some(),
    "the inner FSM must be failed after its exchange deadline elapses"
  );
  assert!(
    matches!(bridge.phase, BridgePhase::Active),
    "the bridge phase is still Active — the FSM failure has not yet cascaded"
  );
  assert!(
    !bridge.is_terminal(),
    "a non-Failed phase is not terminal even with a failed inner FSM"
  );

  // `drain_then_reap` selects the lifecycle notice from `(phase, fsm_failed)`;
  // the `(_, Some(e))` arm emits `StreamErrored(e.to_string())`. No panic ==
  // the arm executed; no UserPacket may surface (no payload was queued).
  bridge.drain_then_reap(&mut ep, &mut conns, t0);
  assert!(
    !ep
      .poll_event()
      .is_some_and(|ev| matches!(ev, Event::UserPacket(..))),
    "the FSM-failed notice arm must not surface application user data"
  );
}
