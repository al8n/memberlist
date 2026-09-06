use super::*;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

#[cfg(compression)]
use memberlist_proto::CompressionOptions;
#[cfg(encryption)]
use memberlist_proto::{EncryptionOptions, Keyring, SecretKey};
use memberlist_proto::{SeedableRng, SmallRng, typed::NodeState};
use smol_str::SmolStr;
use std::vec::Vec;

/// A fixed-seed gossip RNG for the engine constructors. These are single-node
/// state tests; a deterministic seed keeps them reproducible.
fn test_rng() -> SmallRng {
  SmallRng::seed_from_u64(42)
}

/// The receive ring every gossip fake here declares. Their queues are `Vec`-backed
/// and have no fixed bound, so they declare the largest ring the engine accepts —
/// one slot below its per-pump read cap — and every fake is usable both as the
/// construction-time capacity witness and as the pumped view.
const FAKE_RECV_CAPACITY: usize = GOSSIP_READ_CAP - 1;

struct NoGossip;

impl GossipIo for NoGossip {
  fn recv(&mut self, _buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    None
  }

  fn send(&mut self, _bytes: &[u8], _dest: SocketAddr) {}

  fn recv_capacity(&self) -> usize {
    FAKE_RECV_CAPACITY
  }
}

/// A [`GossipIo`] that records every outbound datagram so a test can inspect
/// the actual on-wire bytes (e.g. the transform wrapper tag). Only the checksum
/// wire-shape and CIDR datagram-suppression tests consume it, so it shares their
/// feature gates.
#[cfg(any(feature = "crc32", feature = "cidr"))]
struct CaptureGossip {
  sent: Vec<Vec<u8>>,
}

#[cfg(any(feature = "crc32", feature = "cidr"))]
impl CaptureGossip {
  fn new() -> Self {
    Self { sent: Vec::new() }
  }
}

#[cfg(any(feature = "crc32", feature = "cidr"))]
impl GossipIo for CaptureGossip {
  fn recv(&mut self, _buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    None
  }

  fn send(&mut self, bytes: &[u8], _dest: SocketAddr) {
    self.sent.push(bytes.to_vec());
  }

  fn recv_capacity(&self) -> usize {
    FAKE_RECV_CAPACITY
  }
}

/// Drive `engine` until it emits at least one outbound gossip datagram (or the
/// budget of pumps elapses), returning the captured datagrams. A peer is
/// injected first so gossip has a destination. Only the checksum wire-shape
/// tests consume it, so it shares their feature gate.
#[cfg(feature = "crc32")]
fn capture_gossip(transform: TransformOptions) -> Vec<Vec<u8>> {
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let mut engine = Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng(), &NoGossip)
    .expect("valid configuration");
  engine.start(now);
  // A peer to gossip TO, so `pump` emits at least one outbound gossip datagram.
  engine.inject_alive(SmolStr::new("peer"), node_addr(7947), now);

  let mut gossip = CaptureGossip::new();
  let mut stream = NoStream::with_pool(2);
  let mut t = now;
  for _ in 0..40 {
    engine.pump(t, &mut gossip, &mut stream);
    if !gossip.sent.is_empty() {
      break;
    }
    t += Duration::from_millis(50);
  }
  gossip.sent
}

struct NoStream {
  free: Vec<u32>,
  /// What `accepted_peer` reports for any slot — `Some` to simulate a settled
  /// inbound handshake so `check_listener` proceeds to `accept_connection`.
  accept_peer: Option<SocketAddr>,
}

impl NoStream {
  fn with_pool(size: u32) -> Self {
    Self {
      free: (0..size).collect(),
      accept_peer: None,
    }
  }
}

impl StreamIo for NoStream {
  type Conn = u32;

  fn take_free(&mut self) -> Option<u32> {
    self.free.pop()
  }

  fn give(&mut self, c: u32) {
    self.free.push(c);
  }

  fn free_count(&self) -> usize {
    self.free.len()
  }

  fn teardown_done(&self, _c: u32, _g: crate::SlotGen) -> bool {
    // Synchronous mock: a retired slot is immediately reusable.
    true
  }

  fn listen(
    &mut self,
    _c: u32,
    _port: u16,
    _g: crate::SlotGen,
  ) -> Result<(), crate::StreamIoError> {
    Ok(())
  }

  fn accepted_peer(&self, _c: u32) -> Option<SocketAddr> {
    self.accept_peer
  }

  fn connect(
    &mut self,
    _c: u32,
    _remote: SocketAddr,
    _local_port: u16,
    _g: crate::SlotGen,
  ) -> Result<(), crate::StreamIoError> {
    Err(crate::StreamIoError::Busy)
  }

  fn may_send(&self, _c: u32) -> bool {
    false
  }

  fn may_recv(&self, _c: u32) -> bool {
    false
  }

  fn is_open(&self, _c: u32) -> bool {
    false
  }

  fn is_established(&self, _c: u32) -> bool {
    false
  }

  fn recv(&mut self, _c: u32, _buf: &mut [u8]) -> Option<usize> {
    None
  }

  fn recv_finished(&self, _c: u32) -> bool {
    false
  }

  fn send(&mut self, _c: u32, _bytes: &[u8]) -> usize {
    0
  }

  fn send_queue(&self, _c: u32) -> usize {
    0
  }

  fn close(&mut self, _c: u32, _g: crate::SlotGen) {}

  fn abort(&mut self, _c: u32, _g: crate::SlotGen) {}
}

fn node_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port)
}

fn make_engine() -> Engine<SmolStr, u32> {
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("valid configuration must construct without error")
}

/// `set_compression_options` is accepted and the engine remains operational
/// (a subsequent `pump` does not panic or error).
#[cfg(compression)]
#[test]
fn set_compression_options_accepted_and_engine_still_pumps() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));

  engine
    .set_compression_options(CompressionOptions::default())
    .expect("compression accepted while running");
  engine.start(now);

  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(2);
  // `pump` must not panic after a compression-options update.
  let _deadline = engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1,
    "single-node engine has exactly one member"
  );
}

/// A caller `AliveDelegate` composes with the built-in routable filter: it can
/// reject an otherwise-admissible (routable) peer, while a peer it accepts is
/// admitted.
#[test]
fn custom_alive_delegate_restricts_admission() {
  struct RejectId(SmolStr);
  impl AliveDelegate<SmolStr, SocketAddr> for RejectId {
    fn notify_alive(&self, peer: &NodeState<SmolStr, SocketAddr>) -> bool {
      peer.id_ref() != &self.0
    }
  }

  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.set_alive_delegate(RejectId(SmolStr::new("blocked")));
  engine.start(now);

  // Rejected by the custom delegate even though its address is routable.
  engine.inject_alive(SmolStr::new("blocked"), node_addr(7001), now);
  assert!(
    !engine.is_alive(&SmolStr::new("blocked")),
    "a peer the custom delegate rejects must not be admitted"
  );

  // Passes both the routable filter and the custom delegate.
  engine.inject_alive(SmolStr::new("allowed"), node_addr(7002), now);
  assert!(
    engine.is_alive(&SmolStr::new("allowed")),
    "a peer that passes both the routable filter and the custom delegate is admitted"
  );
}

/// A CIDR policy set via `Options::with_cidr_policy` gates membership admission
/// by the peer's self-advertised address: a routable peer outside the allow-list
/// is rejected, while one inside is admitted. (The transport-boundary recv/accept
/// guards share the same `cidr_blocks` predicate and are exercised end-to-end by
/// the std drivers' integration tests; this pins the membership half on the
/// shared no_std core that smoltcp and embassy both drive.)
#[cfg(feature = "cidr")]
#[test]
fn cidr_policy_gates_membership_admission_by_advertised_address() {
  use memberlist_proto::CidrPolicy;

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");
  engine.start(now);

  // Routable but outside 10.0.0.0/8 — rejected by the policy.
  let outside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
  engine.inject_alive(SmolStr::new("outside"), outside, now);
  assert!(
    !engine.is_alive(&SmolStr::new("outside")),
    "a routable peer outside the CIDR allow-list must not be admitted"
  );

  // Inside 10.0.0.0/8 — admitted (non-vacuity: the policy gates by IP, not all).
  let inside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7002);
  engine.inject_alive(SmolStr::new("inside"), inside, now);
  assert!(
    engine.is_alive(&SmolStr::new("inside")),
    "a peer inside the CIDR allow-list is admitted"
  );
}

/// Installing a caller alive delegate after a CIDR policy was set does NOT drop
/// the policy: `set_alive_delegate` re-folds the stored policy, so admission
/// stays routable AND in-policy AND delegate. Without the re-fold an accept-all
/// delegate would re-admit an out-of-policy peer — this is the regression guard
/// for that composition.
#[cfg(feature = "cidr")]
#[test]
fn set_alive_delegate_preserves_the_cidr_policy() {
  use memberlist_proto::CidrPolicy;

  struct AcceptAll;
  impl AliveDelegate<SmolStr, SocketAddr> for AcceptAll {
    fn notify_alive(&self, _: &NodeState<SmolStr, SocketAddr>) -> bool {
      true
    }
  }

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");
  // An accept-all delegate installed AFTER the policy must not loosen it.
  engine.set_alive_delegate(AcceptAll);
  engine.start(now);

  let outside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
  engine.inject_alive(SmolStr::new("outside"), outside, now);
  assert!(
    !engine.is_alive(&SmolStr::new("outside")),
    "the CIDR policy must survive a later set_alive_delegate (accept-all must not re-admit an \
       out-of-policy peer)"
  );

  // Non-vacuity: an in-policy peer the accept-all delegate also accepts is admitted.
  let inside = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7002);
  engine.inject_alive(SmolStr::new("inside"), inside, now);
  assert!(
    engine.is_alive(&SmolStr::new("inside")),
    "an in-policy peer is still admitted with the accept-all delegate"
  );
}

/// A reliable user-message (`send_reliable`) to a CIDR-blocked peer terminalizes
/// as `Failed`, NOT a benign success. The outbound dial is rejected before
/// connect via `handle_dial_failed`: a clean EOF on a never-connected one-way
/// `UserMessage` would otherwise complete the exchange as `Succeeded`, falsely
/// reporting the send delivered when the bytes were dropped with the reclaimed
/// connection.
#[cfg(feature = "cidr")]
#[test]
fn cidr_blocked_send_reliable_fails_not_succeeds() {
  use memberlist_proto::{
    CidrPolicy,
    event::{Event, ExchangeKind, ExchangeStatus},
  };

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");

  // Seed a reliable slot for the dial plus a listener slot, so the Connect drives
  // a real dial this tick rather than deferring to PendingDial.
  engine.set_listener(1);
  engine.plane_mut().pool.push(0);
  engine.start(now);

  // A one-way reliable user-message to a routable-but-out-of-policy peer; the
  // CIDR screen (which precedes the routable screen) rejects the dial.
  let blocked = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
  engine
    .send_reliable(blocked, bytes::Bytes::from_static(b"blocked-bytes"), now)
    .expect("send_reliable queues the exchange");

  // Pump until the exchange terminalizes: Connect -> dial(blocked) -> reject.
  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(0);
  let mut outcome = None;
  for _ in 0..4 {
    engine.pump(now, &mut gossip, &mut stream);
    while let Some(ev) = engine.poll_event() {
      if let Event::ExchangeCompleted(ec) = ev {
        if ec.kind() == ExchangeKind::UserMessage {
          outcome = Some(ec.outcome());
        }
      }
    }
    if outcome.is_some() {
      break;
    }
  }
  assert_eq!(
    outcome,
    Some(ExchangeStatus::Failed),
    "a CIDR-blocked send_reliable must complete as Failed (a benign EOF would falsely succeed it)"
  );
}

/// A rejected inbound accept must abort the socket AND return its slot to the
/// reliable pool — the same abort-and-reclaim the CIDR reject path uses — or
/// each rejection (a peer hitting `max_inbound_streams`, or any accept while
/// leaving) shrinks the finite pool one slot at a time until the listener can
/// no longer be restored and inbound reliable connections stop working.
#[test]
fn rejected_inbound_accept_returns_its_slot_to_the_pool() {
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  // A zero inbound-stream ceiling refuses every passive-open admission while the
  // node stays running, so `check_listener` always takes the `None` arm.
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_max_inbound_streams(Some(0));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");

  // Two free reliable slots plus a listener on a third: capacity is three.
  engine.plane_mut().pool.push(10);
  engine.plane_mut().pool.push(11);
  engine.set_listener(12);
  engine.start(now);
  let capacity = engine.pool_free_count() + engine.listener_present() as usize;

  // A settled inbound handshake the endpoint refuses (the cap): the slot must
  // come back to the pool (possibly re-armed as the listener), never leak.
  let mut stream = NoStream::with_pool(0);
  stream.accept_peer = Some(node_addr(7950));
  engine.check_listener(now, &mut stream);

  assert_eq!(
    engine.pool_free_count() + engine.listener_present() as usize,
    capacity,
    "a rejected inbound accept leaked a reliable-pool slot"
  );
}

/// A directed unreliable `send` to a CIDR-blocked destination emits NO gossip
/// datagram — the outbound counterpart to the recv source filter. An in-policy
/// destination still emits, proving the drop is the policy and not a vacuous
/// no-send.
#[cfg(feature = "cidr")]
#[test]
fn cidr_blocked_unreliable_send_emits_no_datagram() {
  use memberlist_proto::CidrPolicy;

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");
  engine.start(now);
  let mut stream = NoStream::with_pool(0);

  // A routable-but-out-of-policy destination: the send is dropped before
  // enqueueing, so the gossip drain emits nothing.
  let blocked = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
  engine
    .send(blocked, bytes::Bytes::from_static(b"blocked"))
    .expect("best-effort send returns Ok");
  let mut gossip = CaptureGossip::new();
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    gossip.sent.is_empty(),
    "no datagram may be emitted to a CIDR-blocked destination, saw {}",
    gossip.sent.len()
  );

  // Non-vacuity: an in-policy destination DOES emit the directed datagram.
  let allowed = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7002);
  engine
    .send(allowed, bytes::Bytes::from_static(b"allowed"))
    .expect("send");
  let mut gossip2 = CaptureGossip::new();
  engine.pump(now, &mut gossip2, &mut stream);
  assert!(
    !gossip2.sent.is_empty(),
    "an in-policy directed send must emit a datagram (the block is by IP, not unconditional)"
  );
}

/// A caller `MergeDelegate` installs cleanly and the engine stays operational.
/// (Merge-rejection behaviour itself is covered by the machine's own tests; the
/// engine only forwards the predicate.)
#[test]
fn custom_merge_delegate_installs_and_engine_still_pumps() {
  struct RejectAllMerges;
  impl MergeDelegate<SmolStr, SocketAddr> for RejectAllMerges {
    fn notify_merge(
      &self,
      _peers: crate::MaybeOwned<'_, [NodeState<SmolStr, SocketAddr>]>,
    ) -> bool {
      false
    }
  }

  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.set_merge_delegate(RejectAllMerges);
  engine.start(now);

  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(2);
  let _deadline = engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1,
    "single-node engine has exactly one member after installing a merge delegate"
  );
}

/// After `leave()`, every runtime data- and policy-state setter rejects with
/// `NotRunning` rather than a false `Ok`, since a post-leave mutation could
/// never reach the wire.
#[test]
fn control_setters_reject_after_leave() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(now);
  engine.leave(now).expect("leave from a running node");

  let meta =
    memberlist_proto::typed::Meta::try_from(bytes::Bytes::from_static(b"x")).expect("meta");
  assert!(
    matches!(
      engine.update_node_metadata(meta),
      Err(memberlist_proto::Error::NotRunning)
    ),
    "update_node_metadata must reject after leave"
  );
  assert!(
    matches!(
      engine.set_local_state(bytes::Bytes::from_static(b"s")),
      Err(memberlist_proto::Error::NotRunning)
    ),
    "set_local_state must reject after leave"
  );
  assert!(
    matches!(
      engine.set_ack_payload(bytes::Bytes::from_static(b"a")),
      Err(memberlist_proto::Error::NotRunning)
    ),
    "set_ack_payload must reject after leave"
  );
  assert!(
    matches!(
      engine.queue_user_broadcast(bytes::Bytes::from_static(b"b")),
      Err(memberlist_proto::Error::NotRunning)
    ),
    "queue_user_broadcast must reject after leave"
  );
  #[cfg(compression)]
  assert!(
    matches!(
      engine.set_compression_options(CompressionOptions::default()),
      Err(memberlist_proto::Error::NotRunning)
    ),
    "set_compression_options must reject after leave"
  );
  #[cfg(encryption)]
  assert!(
    matches!(
      engine.set_encryption_options(EncryptionOptions::default()),
      Err(crate::error::ControlError::NotRunning)
    ),
    "set_encryption_options must reject after leave"
  );
}

/// After `leave()` the machine admits no inbound Alive, so installing an
/// admission delegate is inert — it succeeds (matching the core machine's
/// infallible setter) but is never consulted.
#[test]
fn admission_delegate_install_after_leave_is_inert() {
  struct AcceptAll;
  impl AliveDelegate<SmolStr, SocketAddr> for AcceptAll {
    fn notify_alive(&self, _: &NodeState<SmolStr, SocketAddr>) -> bool {
      true
    }
  }

  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(now);
  engine.leave(now).expect("leave from a running node");

  // Installing succeeds but the machine never consults it: an inbound Alive
  // during the drain is not admitted, accept-all delegate notwithstanding.
  engine.set_alive_delegate(AcceptAll);
  engine.inject_alive(SmolStr::new("late"), node_addr(7050), now);
  assert!(
    !engine.is_alive(&SmolStr::new("late")),
    "a left node admits no Alive even with an accept-all delegate installed"
  );
}

/// The `Checksumed` wrapper tag on the wire. With neither encryption nor a
/// label configured, the checksum wrapper is the OUTERMOST frame, so an
/// outbound gossip datagram begins with this tag exactly when checksum is
/// applied on the send path.
#[cfg(feature = "crc32")]
const CHECKSUMED_TAG: u8 = 15;

/// With checksum enabled, every outbound gossip datagram carries the
/// `Checksumed` wrapper tag — proving the engine's send path actually applies
/// `checksum_gossip` (a wire-shape assertion, not mere convergence: an
/// unwrapped datagram would still be accepted by a peer, so convergence alone
/// cannot detect a send path that skips the checksum wrap).
#[cfg(feature = "crc32")]
#[test]
fn enabled_checksum_stamps_the_checksumed_tag_on_outbound_gossip() {
  use memberlist_proto::{ChecksumAlgorithm, ChecksumOptions};

  let transform = TransformOptions::default()
    .with_checksum(ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32));
  let sent = capture_gossip(transform);

  assert!(
    !sent.is_empty(),
    "engine must emit at least one gossip datagram"
  );
  for dg in &sent {
    assert_eq!(
      dg.first().copied(),
      Some(CHECKSUMED_TAG),
      "every outbound gossip datagram must begin with the Checksumed tag when \
         checksum is enabled; got first byte {:?}",
      dg.first()
    );
  }
}

/// With checksum disabled (the default), no outbound gossip datagram carries
/// the `Checksumed` wrapper tag — confirming the wrap is opt-in and the
/// positive test above is discriminating rather than vacuous.
#[cfg(feature = "crc32")]
#[test]
fn disabled_checksum_leaves_no_checksumed_tag_on_outbound_gossip() {
  let sent = capture_gossip(TransformOptions::default());

  assert!(
    !sent.is_empty(),
    "engine must emit at least one gossip datagram"
  );
  for dg in &sent {
    assert_ne!(
      dg.first().copied(),
      Some(CHECKSUMED_TAG),
      "a default (checksum-disabled) node must not stamp the Checksumed tag"
    );
  }
}

/// `set_encryption_options` with no keyring (disabled) is always accepted.
#[cfg(encryption)]
#[test]
fn set_encryption_options_disabled_is_always_ok() {
  let mut engine = make_engine();
  let result = engine.set_encryption_options(EncryptionOptions::default());
  assert!(result.is_ok(), "disabling encryption must always succeed");
}

/// `set_encryption_options` with a valid AES-256 keyring succeeds when the
/// `aes-gcm` backend is compiled in, and the engine pumps normally
/// afterward.
#[cfg(feature = "aes-gcm")]
#[test]
fn set_encryption_options_accepts_valid_aes256_keyring_and_engine_still_pumps() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));

  let key = SecretKey::Aes256([0x42; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  engine
    .set_encryption_options(opts)
    .expect("valid AES-256 keyring must be accepted when aes-gcm is compiled in");

  engine.start(now);
  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(2);
  let _deadline = engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1,
    "engine remains functional after encryption update"
  );
}

/// `validate_runtime_config` is deterministic for an encryption keyring: the
/// usability probe draws no entropy, so the driver's pre-resolution screen and
/// the engine's identical re-check inside `try_new_at` cannot disagree. Repeated
/// calls return the same verdict, and whether that verdict is `Ok` depends only on
/// whether the AES-GCM backend is compiled in — never on entropy availability.
#[cfg(encryption)]
#[test]
fn validate_runtime_config_for_encryption_is_deterministic() {
  let key = SecretKey::Aes256([0x42; 32]);
  let transform = TransformOptions::default()
    .with_encryption(EncryptionOptions::new().with_keyring(Keyring::new(key)));
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let gossip_mtu =
    memberlist_proto::EndpointOptions::new(SmolStr::new("enc"), node_addr(7946)).gossip_mtu();

  // Identical verdict on every call: the probe draws no entropy, so nothing
  // transient can flip it between the driver screen and the engine re-check.
  let first = validate_runtime_config(&cfg, &transform, gossip_mtu).is_ok();
  for _ in 0..4 {
    assert_eq!(
      validate_runtime_config(&cfg, &transform, gossip_mtu).is_ok(),
      first,
      "the encryption preflight must return the same verdict on every call"
    );
  }

  // The verdict tracks ONLY backend availability. With AES-GCM compiled in the
  // keyring is usable; without it the probe rejects with the specific
  // `UnsupportedAlgorithm` — deterministically, never an entropy error.
  let result = validate_runtime_config(&cfg, &transform, gossip_mtu);
  #[cfg(feature = "aes-gcm")]
  result.expect("a valid AES-256 keyring validates when the aes-gcm backend is present");
  #[cfg(not(feature = "aes-gcm"))]
  assert!(
    matches!(
      result,
      Err(InitError::Encryption(
        memberlist_proto::EncryptionError::UnsupportedAlgorithm(_)
      ))
    ),
    "without the AES-GCM backend the probe must reject with UnsupportedAlgorithm, got {result:?}"
  );
}

/// A disabled (no-algorithm) checksum policy always constructs cleanly — there
/// is no backend to probe, so `try_new_at` succeeds whichever checksum backend is
/// compiled in.
#[cfg(checksum)]
#[test]
fn try_new_at_accepts_disabled_checksum() {
  use memberlist_proto::ChecksumOptions;

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("no-checksum"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let transform = TransformOptions::default().with_checksum(ChecksumOptions::new());

  assert!(
    Engine::<SmolStr, u32>::try_new_at(cfg, transform, ep_cfg, now, test_rng(), &NoGossip).is_ok(),
    "a disabled checksum policy must always construct"
  );
}

struct QueueGossip {
  /// Pending inbound datagrams: each entry is `(src, bytes)`.
  inbound: Vec<(SocketAddr, Vec<u8>)>,
  /// Outbound datagrams captured from `send`.
  outbound: Vec<(Vec<u8>, SocketAddr)>,
  /// What [`GossipIo::recv_capacity`] declares. The `inbound` queue itself is
  /// unbounded, so this is the ring size the fake CLAIMS — settable so a test can
  /// present the engine with a capacity it must reject.
  recv_capacity: usize,
}

impl QueueGossip {
  fn new() -> Self {
    Self::with_recv_capacity(FAKE_RECV_CAPACITY)
  }

  fn with_recv_capacity(recv_capacity: usize) -> Self {
    Self {
      inbound: Vec::new(),
      outbound: Vec::new(),
      recv_capacity,
    }
  }

  fn push(&mut self, src: SocketAddr, bytes: Vec<u8>) {
    self.inbound.push((src, bytes));
  }
}

impl GossipIo for QueueGossip {
  fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    if self.inbound.is_empty() {
      return None;
    }
    let (src, bytes) = self.inbound.remove(0);
    let n = bytes.len().min(buf.len());
    buf[..n].copy_from_slice(&bytes[..n]);
    Some((src, n))
  }

  fn send(&mut self, bytes: &[u8], dest: SocketAddr) {
    self.outbound.push((bytes.to_vec(), dest));
  }

  fn recv_capacity(&self) -> usize {
    self.recv_capacity
  }
}

/// An engine with a cluster label must:
///
/// - Reject gossip datagrams that carry no label (or a wrong label) — the
///   `decode_incoming` label check drops them before the machine sees them, so
///   no membership change occurs.
/// - Accept gossip datagrams that carry the matching label — the machine
///   processes the Alive and the member count rises.
/// - Stamp the cluster label onto every outbound gossip datagram — the on-wire
///   bytes decode successfully with the matching label and fail when no label
///   (or the wrong label) is expected.
#[test]
fn gossip_carries_and_checks_the_configured_label() {
  use memberlist_proto::{
    DecodeOptions, EncodeOptions, Node, encode_outgoing,
    typed::{Alive, Message},
  };

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("alpha"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let transform = TransformOptions::new()
    .with_label(Some(b"alpha".to_vec()))
    .expect("valid label");
  let mut engine =
    Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng(), &NoGossip).expect("valid config");
  engine.start(now);

  // ── Ingress: unlabeled datagram must be dropped. ─────────────────────────
  // Build a plaintext Alive for a fake peer. Incarnation > 0 passes SWIM's
  // freshness check for a node this engine has never seen.
  let peer_addr: SocketAddr = SocketAddr::new(
    core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 2)),
    7946,
  );
  let ghost_node = Node::new(SmolStr::new("ghost"), peer_addr);
  let alive_msg = Alive::new(1, ghost_node.clone());
  let unlabeled = encode_outgoing::<SmolStr, SocketAddr>(
    &Message::Alive(alive_msg),
    &EncodeOptions::default(), // no label
  )
  .expect("encode unlabeled Alive");

  let src: SocketAddr = SocketAddr::new(
    core::net::IpAddr::V4(core::net::Ipv4Addr::new(10, 0, 0, 3)),
    7946,
  );
  let mut gossip = QueueGossip::new();
  gossip.push(src, unlabeled.to_vec());
  let mut stream = NoStream::with_pool(2);
  let _ = engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.num_members(),
    1,
    "unlabeled inbound gossip must be rejected — ghost must not appear"
  );

  // ── Ingress: wrong-label datagram must also be dropped. ──────────────────
  let alive_msg2 = Alive::new(1, ghost_node.clone());
  let beta_labeled = encode_outgoing::<SmolStr, SocketAddr>(
    &Message::Alive(alive_msg2),
    &EncodeOptions::new(Some(bytes::Bytes::from_static(b"beta"))),
  )
  .expect("encode beta-labeled Alive");

  gossip.push(src, beta_labeled.to_vec());
  let _ = engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.num_members(),
    1,
    "wrong-label inbound gossip must be rejected — ghost must not appear"
  );

  // ── Ingress: correctly-labeled datagram must be accepted. ─────────────────
  let alive_msg3 = Alive::new(1, ghost_node);
  let alpha_labeled = encode_outgoing::<SmolStr, SocketAddr>(
    &Message::Alive(alive_msg3),
    &EncodeOptions::new(Some(bytes::Bytes::from_static(b"alpha"))),
  )
  .expect("encode alpha-labeled Alive");

  gossip.push(src, alpha_labeled.to_vec());
  let _ = engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.num_members(),
    2,
    "alpha-labeled inbound gossip must be accepted — ghost must appear"
  );

  // ── Egress: outbound gossip must carry the cluster label. ────────────────
  // Advance time enough that the machine emits at least one gossip transmit.
  // The gossip timer fires on the first tick; pump once more to drain it.
  let later = Instant::from_origin(Duration::from_secs(86_400 + 2));
  let _ = engine.pump(later, &mut gossip, &mut stream);

  // Collect the first outbound datagram (if any).  The machine may or may
  // not emit one on the first ticked pump; iterate until we see a send or
  // exhaust a few more ticks.
  let mut tries = 0u32;
  while gossip.outbound.is_empty() && tries < 10 {
    let t = Instant::from_origin(Duration::from_secs(86_400 + 2 + tries as u64));
    let _ = engine.pump(t, &mut gossip, &mut stream);
    tries += 1;
  }

  assert!(
    !gossip.outbound.is_empty(),
    "engine must emit at least one gossip transmit after the timer fires"
  );

  let (wire_bytes, _dest) = &gossip.outbound[0];
  // Decoding with the matching label must succeed.
  let ok = memberlist_proto::codec::decode_incoming(
    bytes::Bytes::copy_from_slice(wire_bytes),
    &DecodeOptions::new(Some(bytes::Bytes::from_static(b"alpha"))),
  );
  assert!(
    ok.is_ok(),
    "outbound gossip must be decodable with the cluster label; got {:?}",
    ok.err()
  );

  // Decoding with no expected label must fail (labeled frame on an unlabeled
  // receiver is rejected with DoubleLabel).
  let no_label = memberlist_proto::codec::decode_incoming(
    bytes::Bytes::copy_from_slice(wire_bytes),
    &DecodeOptions::new(None),
  );
  assert!(
    no_label.is_err(),
    "outbound gossip must NOT be accepted by a no-label decoder"
  );

  // Decoding with the wrong label must fail (LabelMismatch).
  let wrong_label = memberlist_proto::codec::decode_incoming(
    bytes::Bytes::copy_from_slice(wire_bytes),
    &DecodeOptions::new(Some(bytes::Bytes::from_static(b"beta"))),
  );
  assert!(
    wrong_label.is_err(),
    "outbound gossip must NOT be accepted by a wrong-label decoder"
  );
}

// ── Reliable-plane harness ───────────────────────────────────────────────────
//
// The reliable-plane lifecycle (dial → promote → flush → half-close → close →
// reap, plus inbound recv/EOF) is link-layer-independent engine code: the
// machine emits the `StreamAction`s, the engine pumps them over `StreamIo`. The
// mocks below stand in for a driver's socket pool so a single test can drive the
// engine through those paths and assert real state transitions (slot returns to
// the pool, EOF delivered once, exchange terminalizes Succeeded/Failed) rather
// than convergence side effects.

use core::cell::RefCell;
use std::{collections::BTreeMap, rc::Rc};

/// The simulated TCP state of one mock reliable socket, as the engine observes
/// it through `StreamIo`. A test programs these directly (the single-engine
/// `ProgRel` mock) or the fabric drives them (the two-engine `link`).
#[derive(Clone)]
struct SockState {
  /// `connect` was issued and the handshake is modelled complete: `may_send` is
  /// true and writes are accepted. Set by the fabric once the peer's listener
  /// has accepted, or directly by `ProgRel` to skip the handshake.
  established: bool,
  /// The socket is not yet `Closed`/`TimeWait` — i.e. `is_open` is true. A
  /// `close()` leaves it open (FIN in flight); an `abort()` or a completed close
  /// drops it to false so a pooled handle is reclaimable.
  open: bool,
  /// Bytes the peer wrote toward this socket, awaiting our `recv`.
  rx: Vec<u8>,
  /// The peer FIN'd and our rx is drained: `recv_finished` reports the one-shot
  /// EOF. Modelled as a flag the fabric/test sets after the peer's `close`.
  peer_fin: bool,
  /// Unacknowledged bytes in the tx ring (`send_queue`). The drain-before-close
  /// gate waits for this to reach zero before a graceful FIN.
  tx_unacked: usize,
  /// The remote address `accepted_peer` reports once a passive open settled, or
  /// `None` for a dialing/listening/idle slot.
  accepted: Option<SocketAddr>,
  /// Cap on bytes one `send` accepts, to model partial-write backpressure. A
  /// `send` of more than this many bytes leaves the remainder parked in the
  /// connection's `out` queue. `usize::MAX` accepts the whole buffer.
  send_cap: usize,
  /// The outbound ring is full, so the socket is momentarily NOT writable even
  /// though it is established: `may_send` (writability-gated) reads false while
  /// `is_established` stays true. Models the F1 backpressure divergence. Off by
  /// default, so existing tests keep `may_send == is_established`.
  ring_full: bool,
}

impl SockState {
  fn idle() -> Self {
    Self {
      established: false,
      open: false,
      rx: Vec::new(),
      peer_fin: false,
      tx_unacked: 0,
      accepted: None,
      send_cap: usize::MAX,
      ring_full: false,
    }
  }
}

/// A directly-programmable single-engine reliable mock. A test mutates each
/// slot's [`SockState`] between pumps to walk the engine through a reliable
/// path without a second engine — e.g. flip `established` to promote a dial, or
/// set `peer_fin` to deliver an inbound EOF. Sends are recorded so a test can
/// assert exactly what reached the wire and how partial-write backpressure
/// parked the remainder.
struct ProgRel {
  /// Free pool slots (handles), LIFO like a driver's pool.
  free: Vec<u32>,
  /// Per-handle simulated socket state.
  socks: BTreeMap<u32, SockState>,
  /// Every `(handle, bytes)` accepted by `send`, in order, for assertions.
  sent: Vec<(u32, Vec<u8>)>,
  /// Handles `close()` was called on (graceful FIN).
  closed: Vec<u32>,
  /// Handles `abort()` was called on (RST).
  aborted: Vec<u32>,
  /// Every `(handle, remote)` the engine issued a link-layer `connect` for, in
  /// order — the record of which dials actually reached the wire.
  connects: Vec<(u32, SocketAddr)>,
  /// When set, every `connect` is rejected with `Busy` — modelling a link layer
  /// that refuses the dial before any SYN, which the engine's `dial` reclaims
  /// and terminalizes as a failure.
  connect_fails: bool,
}

impl ProgRel {
  /// `pool` free handles `0..pool`, each with an idle socket. The engine's own
  /// `plane_mut().pool` is the authority the reliable handlers consult; this
  /// mock only realizes the sockets, so a test pushes the same handles into the
  /// engine pool (or via `set_listener`).
  fn new(handles: &[u32]) -> Self {
    let mut socks = BTreeMap::new();
    for &h in handles {
      socks.insert(h, SockState::idle());
    }
    Self {
      free: handles.to_vec(),
      socks,
      sent: Vec::new(),
      closed: Vec::new(),
      aborted: Vec::new(),
      connects: Vec::new(),
      connect_fails: false,
    }
  }

  fn sock(&self, c: u32) -> &SockState {
    self.socks.get(&c).expect("handle exists")
  }

  fn sock_mut(&mut self, c: u32) -> &mut SockState {
    self.socks.get_mut(&c).expect("handle exists")
  }
}

impl StreamIo for ProgRel {
  type Conn = u32;

  fn take_free(&mut self) -> Option<u32> {
    self.free.pop()
  }

  fn give(&mut self, c: u32) {
    self.free.push(c);
  }

  fn free_count(&self) -> usize {
    self.free.len()
  }

  fn teardown_done(&self, c: u32, _g: crate::SlotGen) -> bool {
    // Synchronous mock: a retired slot is reusable once its socket is Closed
    // (`abort` drops `open` at once; a graceful `close` leaves it open until the
    // test/fabric drops `open`, mirroring the old `!is_open()` reap gate).
    !self.sock(c).open
  }

  fn listen(&mut self, c: u32, _port: u16, _g: crate::SlotGen) -> Result<(), crate::StreamIoError> {
    // A listening socket is open and awaiting a passive open; reset any prior
    // per-slot residue so a reclaimed-then-relistened handle starts clean.
    *self.sock_mut(c) = SockState::idle();
    self.sock_mut(c).open = true;
    Ok(())
  }

  fn accepted_peer(&self, c: u32) -> Option<SocketAddr> {
    self.sock(c).accepted
  }

  fn connect(
    &mut self,
    c: u32,
    remote: SocketAddr,
    _local_port: u16,
    _g: crate::SlotGen,
  ) -> Result<(), crate::StreamIoError> {
    if self.connect_fails {
      return Err(crate::StreamIoError::Busy);
    }
    self.connects.push((c, remote));
    // A dial opens the socket; the test flips `established` to model the
    // handshake completing on a later tick. Record the remote as the eventual
    // accepted peer for symmetry, though the dialer side never reads it.
    let s = self.sock_mut(c);
    s.open = true;
    s.accepted = Some(remote);
    Ok(())
  }

  fn may_send(&self, c: u32) -> bool {
    let s = self.sock(c);
    // Writability-gated: established AND the outbound ring has room. `ring_full`
    // models an established connection whose tx ring is momentarily full, so
    // `may_send` reads false while `is_established` stays true — the F1 divergence.
    s.established && s.open && !s.ring_full
  }

  fn may_recv(&self, c: u32) -> bool {
    !self.sock(c).rx.is_empty()
  }

  fn is_open(&self, c: u32) -> bool {
    self.sock(c).open
  }

  fn is_established(&self, c: u32) -> bool {
    self.sock(c).established
  }

  fn recv(&mut self, c: u32, buf: &mut [u8]) -> Option<usize> {
    let s = self.sock_mut(c);
    if s.rx.is_empty() {
      return None;
    }
    let n = s.rx.len().min(buf.len());
    buf[..n].copy_from_slice(&s.rx[..n]);
    s.rx.drain(..n);
    Some(n)
  }

  fn recv_finished(&self, c: u32) -> bool {
    let s = self.sock(c);
    s.peer_fin && s.rx.is_empty()
  }

  fn send(&mut self, c: u32, bytes: &[u8]) -> usize {
    let cap = self.sock(c).send_cap;
    let n = bytes.len().min(cap);
    self.sent.push((c, bytes[..n].to_vec()));
    n
  }

  fn send_queue(&self, c: u32) -> usize {
    self.sock(c).tx_unacked
  }

  fn close(&mut self, c: u32, _g: crate::SlotGen) {
    self.closed.push(c);
    // A graceful close leaves the socket open (FIN in flight) until the test (or
    // reap) drops `open`; the engine parks it in `retiring` (Draining) for the reap.
  }

  fn abort(&mut self, c: u32, _g: crate::SlotGen) {
    self.aborted.push(c);
    let s = self.sock_mut(c);
    s.open = false;
    s.established = false;
  }
}

/// Build a running engine with a short reliable-exchange (`stream_timeout`)
/// deadline so a dial that never completes its handshake is reaped to a failed
/// terminal `Abort` within a couple of clock advances rather than the default
/// many seconds.
fn engine_with_stream_timeout(stream_timeout: Duration) -> (Engine<SmolStr, u32>, Instant) {
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(stream_timeout);
  engine_from(cfg, ep_cfg)
}

/// Build and start an engine from a fully-specified pair of options, for the
/// admission tests that need to set a cap or a timer interval the shorthand
/// builders above do not expose.
fn engine_from(
  cfg: Options,
  ep_cfg: memberlist_proto::EndpointOptions<SmolStr, SocketAddr>,
) -> (Engine<SmolStr, u32>, Instant) {
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");
  engine.start(now);
  (engine, now)
}

/// Drain `poll_event` until an `ExchangeCompleted` of `kind` appears, returning
/// its outcome — pumping with the two mocks between drains for at most `rounds`
/// ticks (the clock held at `now`; the caller advances when a deadline must
/// elapse).
fn pump_until_exchange(
  engine: &mut Engine<SmolStr, u32>,
  stream: &mut ProgRel,
  now: Instant,
  kind: memberlist_proto::event::ExchangeKind,
  rounds: u32,
) -> Option<memberlist_proto::event::ExchangeStatus> {
  let mut gossip = NoGossip;
  for _ in 0..rounds {
    engine.pump(now, &mut gossip, stream);
    while let Some(ev) = engine.poll_event() {
      if let Event::ExchangeCompleted(ec) = ev {
        if ec.kind() == kind {
          return Some(ec.outcome());
        }
      }
    }
  }
  None
}

/// A reliable user-message whose dial is rejected by the link layer (`connect`
/// returns `Busy`) terminalizes as `Failed`, and the freshly-assigned slot is
/// aborted and returned to the pool — never leaked. This drives the `dial`
/// connect-rejection branch end-to-end through `drain_stream_actions`.
#[test]
fn reliable_dial_connect_rejection_fails_and_reclaims_slot() {
  use memberlist_proto::event::{ExchangeKind, ExchangeStatus};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(5));
  // One dial slot in the engine pool; the listener gets its own so the dial does
  // not have to wait on the listener for a slot.
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);

  let mut stream = ProgRel::new(&[0, 1]);
  stream.connect_fails = true;

  let to = node_addr(7001);
  engine
    .send_reliable(to, bytes::Bytes::from_static(b"payload"), now)
    .expect("send_reliable queues the exchange");

  let outcome = pump_until_exchange(&mut engine, &mut stream, now, ExchangeKind::UserMessage, 6);
  assert_eq!(
    outcome,
    Some(ExchangeStatus::Failed),
    "a connect-rejected reliable send must complete as Failed"
  );
  assert!(
    stream.aborted.contains(&0),
    "the dial slot must be aborted on the connect rejection"
  );
  // The slot came back: the engine pool holds it again (listener-first rebalance
  // may instead re-arm it as the listener, so accept either).
  assert_eq!(
    engine.pool_free_count() + engine.listener_present() as usize,
    2,
    "the connect-rejected slot must return to the pool, never leak"
  );
  assert_eq!(
    engine.outbound_correlation_len(),
    0,
    "the outbound-StreamId correlation entry must be pruned once the exchange completes"
  );
}

/// A reliable user-message to a non-routable destination is screened inside
/// `dial` BEFORE `connect` is ever called: the slot is reclaimed and the
/// exchange terminalizes `Failed` (a never-connected one-way send must fail, not
/// read a benign EOF as success).
#[test]
fn reliable_non_routable_dial_fails_before_connect() {
  use memberlist_proto::event::{ExchangeKind, ExchangeStatus};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(5));
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);

  // Port 0 is non-routable: the dial must reject it up front.
  let dead = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9)), 0);
  engine
    .send_reliable(dead, bytes::Bytes::from_static(b"x"), now)
    .expect("send_reliable queues the exchange");

  let outcome = pump_until_exchange(&mut engine, &mut stream, now, ExchangeKind::UserMessage, 6);
  assert_eq!(
    outcome,
    Some(ExchangeStatus::Failed),
    "a non-routable reliable send must complete as Failed"
  );
  assert!(
    stream.sent.is_empty(),
    "a non-routable dial must never reach connect/send (screened up front)"
  );
}

/// When the pool is exhausted at `Connect` time, the engine records a
/// `PendingDial` connection (no slot) rather than losing the dial intent; once a
/// slot frees, `drain_pending_dials` assigns it and issues the connect. This
/// witnesses the `None` arm of `drain_stream_actions` plus the deferred-dial
/// servicing.
#[test]
fn pending_dial_when_pool_exhausted_then_dialed_once_slot_frees() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  // A listener is present but the dial pool is empty, so the `Connect` finds no
  // free slot and must defer. (With NO listener the listener-first rebalance
  // would claim the later-freed slot for the listener instead, which is correct
  // but not the path under test.)
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[5, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  let to = node_addr(7002);
  engine
    .send_reliable(to, bytes::Bytes::from_static(b"deferred"), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_dial_count(),
    1,
    "a Connect with an exhausted pool must park as PendingDial, not drop the dial"
  );

  // Free a reuse-ready slot: the next pump's rebalance assigns it to the deferred
  // dial (the listener is already present, so it claims nothing).
  engine.plane_mut().pool.push(5);
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_dial_count(),
    0,
    "the deferred dial must be assigned a freed slot and leave PendingDial"
  );
  // `connect` (ProgRel) opens the dialed socket, so slot 5 is now the dialing
  // connection's socket and the pool is empty again.
  assert!(
    StreamIo::is_open(&stream, 5),
    "the deferred dial must have issued connect on the assigned slot"
  );
  assert_eq!(
    engine.pool_free_count(),
    0,
    "the freed slot was consumed by the deferred dial"
  );
}

/// A parked dial whose exchange deadline elapses WHILE it is parked is never
/// dialed: the machine tick judges the expiry first and the exchange is already
/// terminal by the time the pump reaches its single dial site, so the freed slot
/// is never spent on a doomed SYN.
///
/// The engine compares no instant of its own here. The deadline belongs to the
/// machine, and step 6's `handle_timeout` fails and reaps the bridge in one pass,
/// so the parked connection is removed by the resulting `Abort` before the late
/// rebalance looks for work. The slot stays in the pool for the next viable dial
/// instead of being pinned through a connect-then-RST cycle.
#[test]
fn parked_dial_expired_while_parked_is_never_dialed() {
  use memberlist_proto::event::{ExchangeKind, ExchangeStatus};

  let stream_timeout = Duration::from_secs(5);
  let (mut engine, now) = engine_with_stream_timeout(stream_timeout);
  // A listener is present but the dial pool is empty, so the `Connect` parks.
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[5, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  let to = node_addr(7002);
  engine
    .send_reliable(to, bytes::Bytes::from_static(b"doomed"), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_dial_count(),
    1,
    "the dial must park, or there is no expiry-while-parked window to test"
  );
  while engine.poll_event().is_some() {}

  // A slot frees, but only after the exchange deadline has already elapsed.
  engine.plane_mut().pool.push(5);
  let late = now + stream_timeout + Duration::from_secs(1);
  engine.pump(late, &mut gossip, &mut stream);

  assert!(
    stream.connects.is_empty(),
    "an exchange the machine has already failed must never reach connect, got {:?}",
    stream.connects
  );
  assert!(
    stream.aborted.is_empty(),
    "no socket may be dialed and then RST for an expired exchange, got {:?}",
    stream.aborted
  );
  assert!(
    !StreamIo::is_open(&stream, 5),
    "the freed slot must never have been opened"
  );
  assert_eq!(
    engine.pending_dial_count(),
    0,
    "the expired exchange must be gone from the parked set"
  );
  assert_eq!(
    engine.pool_free_count(),
    1,
    "the freed slot stays available for the next viable dial"
  );

  let mut outcome = None;
  while let Some(ev) = engine.poll_event() {
    if let Event::ExchangeCompleted(ec) = ev {
      if ec.kind() == ExchangeKind::UserMessage {
        outcome = Some(ec.outcome());
      }
    }
  }
  assert_eq!(
    outcome,
    Some(ExchangeStatus::Failed),
    "the expired exchange terminalizes through the machine's own failure path"
  );
}

/// With no free slot there is nothing to assign, so the dial drain returns before
/// collecting and sorting the parked set: the parked dials survive untouched and
/// no `connect` is issued. This is the common shape on a saturated node, where the
/// scan would otherwise be paid twice over for nothing.
#[test]
fn parked_dials_are_untouched_while_the_pool_is_empty() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // Two sends with an empty pool: both park, oldest first by ExchangeId.
  engine
    .send_reliable(node_addr(7002), bytes::Bytes::from_static(b"a"), now)
    .expect("send_reliable queues the exchange");
  engine
    .send_reliable(node_addr(7003), bytes::Bytes::from_static(b"b"), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  for _ in 0..3 {
    engine.pump(now, &mut gossip, &mut stream);
    assert_eq!(
      engine.pending_dial_count(),
      2,
      "an empty pool must leave every parked dial exactly where it is"
    );
    assert!(
      stream.connects.is_empty(),
      "no slot is free, so no dial may reach connect, got {:?}",
      stream.connects
    );
  }
}

/// Options with the engine defaults plus a caller-chosen tweak, for the
/// admission tests.
fn admission_cfg() -> Options {
  Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
}

/// The join seed queue holds each address at most once, and a seed already being
/// exchanged with is not re-queued. Both are what stops the natural
/// retry-until-joined loop from re-encoding the whole local membership on every
/// call: one address means one push/pull exchange in flight, however many times
/// it is offered.
#[test]
fn join_dedups_duplicate_seeds_within_and_across_calls() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.plane_mut().pool.push(1);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  let a = node_addr(7002);
  engine.join(&[a, a, a]).expect("join is accepted");
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "one address must queue once however many times it is offered"
  );
  assert_eq!(
    engine.join_seeds_deduped(),
    2,
    "the two repeats must be counted as deduped, not queued"
  );

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    stream.connects.len(),
    1,
    "one seed address must produce exactly one dial, got {:?}",
    stream.connects
  );
  assert_eq!(
    engine.outbound_correlation_len(),
    1,
    "one seed address must produce exactly one outbound exchange"
  );

  // Offered again while the exchange it started is still live: still one exchange.
  engine.join(&[a]).expect("join is accepted");
  assert_eq!(
    engine.pending_seed_count(),
    0,
    "a seed with a live reliable connection must not be re-queued"
  );
  assert_eq!(engine.join_seeds_deduped(), 3, "the re-offer is counted");

  // Complete the handshake so the parked push/pull request reaches the wire, and
  // confirm it went out once, on the one connection.
  stream.sock_mut(1).established = true;
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    !stream.sent.is_empty(),
    "the admitted seed's push/pull request must reach the wire"
  );
  assert!(
    stream.sent.iter().all(|(h, _)| *h == 1),
    "every byte must belong to the single seed exchange, got {:?}",
    stream.sent.iter().map(|(h, _)| *h).collect::<Vec<_>>()
  );
  assert_eq!(
    stream.connects.len(),
    1,
    "no second dial may appear for the same seed"
  );
  assert_eq!(
    engine.outbound_correlation_len(),
    1,
    "still exactly one outbound exchange"
  );
}

/// Seeds offered past the queue ceiling are dropped and counted, and the call
/// still succeeds: a seed list is best-effort discovery intent, so its tail not
/// fitting must not fail the whole call.
#[test]
fn join_seeds_over_cap_are_dropped_and_counted() {
  let cfg = admission_cfg().with_max_pending_seeds(2);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, _now) = engine_from(cfg, ep_cfg);

  engine
    .join(&[node_addr(7002), node_addr(7003), node_addr(7004)])
    .expect("an over-cap seed list is still accepted");
  assert_eq!(
    engine.pending_seed_count(),
    2,
    "the queue must hold exactly the cap"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    1,
    "the surplus seed must be counted as dropped"
  );
  assert_eq!(
    engine.join_seeds_deduped(),
    0,
    "distinct addresses are not duplicates"
  );
}

/// A caller re-offering a seed list longer than the queue cap must reach every
/// entry in it, not only the prefix that fits.
///
/// The starving shape: the seed that takes the first turn fails its dial instantly,
/// so by the next offer it is neither queued nor mid-exchange and is admissible all
/// over again. Admitting the same first turn every time would refill the only queue
/// slot with it and drop the reachable seed behind it, on every offer, forever.
/// Resuming where the previous call ran out of room queues the seed that missed its
/// turn instead, so the reachable one is dialed on the very next offer.
///
/// First here means first in ADDRESS order, not first in the caller's list, so the
/// fixture puts `dead` below `live` in that order to be sure it is the fast-failing
/// seed that takes the turn the rotation then has to move past.
#[cfg(feature = "cidr")]
#[test]
fn re_offered_seeds_past_the_cap_are_admitted_round_robin() {
  use memberlist_proto::CidrPolicy;

  // The policy covers this node and `live`, but not `dead`: a blocked peer's dial is
  // rejected before `connect`, which terminalizes the exchange and frees its slot in
  // the same pump — the fast, total failure the rotation has to survive.
  let cfg = admission_cfg()
    .with_max_pending_seeds(1)
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.plane_mut().pool.push(1);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // Below `live` in address order, so the fast-failing seed is the one that takes
  // the first turn and the rotation is what has to move past it.
  let dead = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 0, 0, 1)), 7002);
  let live = node_addr(7003);
  let mut gossip = NoGossip;

  // Two offers of the SAME list, exactly as a driver's retry loop makes them.
  let mut offers = 0;
  let mut failed_dead_exchanges = 0;
  while offers < 2 && !stream.connects.iter().any(|(_, a)| *a == live) {
    engine.join(&[dead, live]).expect("join is accepted");
    engine.pump(now, &mut gossip, &mut stream);
    while let Some(ev) = engine.poll_event() {
      if let memberlist_proto::event::Event::ExchangeCompleted(ec) = ev {
        if *ec.peer() == dead && !ec.outcome().is_succeeded() {
          failed_dead_exchanges += 1;
        }
      }
    }
    offers += 1;
  }

  assert!(
    stream.connects.iter().any(|(_, a)| *a == live),
    "the reachable seed must be dialed within two offers of the list, got {:?}",
    stream.connects
  );
  assert_eq!(
    offers, 2,
    "the reachable seed must reach the wire on the second offer"
  );
  assert_eq!(
    failed_dead_exchanges, 1,
    "the fast-failing seed must be attempted once, not once per offer"
  );
  assert!(
    !stream.connects.iter().any(|(_, a)| *a == dead),
    "a blocked seed is rejected before connect, so it may never reach the wire"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    2,
    "each offer sheds exactly one seed — the one the single slot had no room for"
  );
}

/// The same seed SET offered in a DIFFERENT order each time must still reach every
/// entry in it.
///
/// A cursor into the previous list rotates only while the caller's order holds
/// still. Alternating `[dead, live]` with `[live, dead]` — what a re-resolution
/// that reorders its candidates produces — puts the resumed index back on the
/// failing seed every time, so the reachable one is never dialed at all. Ranking by
/// ADDRESS instead makes the seed a call had no room for rank first on the next
/// one, whatever position the caller then gives it. That bounds a two-address set
/// at two offers: the first admits one of the pair, and if that was the failing
/// seed the rotation now names the other.
#[cfg(feature = "cidr")]
#[test]
fn reordered_re_offers_cannot_starve_a_reachable_seed() {
  use memberlist_proto::CidrPolicy;

  // The policy covers this node and `live`, but not `dead`: a blocked peer's dial
  // is rejected before `connect`, which terminalizes the exchange and frees its
  // slot in the same pump, so `dead` is admissible all over again by the next
  // offer.
  let cfg = admission_cfg()
    .with_max_pending_seeds(1)
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.plane_mut().pool.push(1);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // Below `live` in address order, so the fast-failing seed is the one that takes
  // the first turn and the rotation is what has to move past it.
  let dead = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 0, 0, 1)), 7002);
  let live = node_addr(7003);
  let mut gossip = NoGossip;

  let mut offers = 0u64;
  while offers < 20 && !stream.connects.iter().any(|(_, a)| *a == live) {
    if offers.is_multiple_of(2) {
      engine.join(&[dead, live]).expect("join is accepted");
    } else {
      engine.join(&[live, dead]).expect("join is accepted");
    }
    engine.pump(now, &mut gossip, &mut stream);
    while engine.poll_event().is_some() {}
    offers += 1;
  }

  assert!(
    stream.connects.iter().any(|(_, a)| *a == live),
    "the reachable seed must be dialed however the caller reorders the list, got {:?}",
    stream.connects
  );
  assert!(
    offers <= 2,
    "a two-address set must reach its reachable entry within two offers, took {offers}"
  );
  assert!(
    !stream.connects.iter().any(|(_, a)| *a == dead),
    "a blocked seed is rejected before connect, so it may never reach the wire"
  );
}

/// Where a positional cursor lands when two disjoint lists are offered alternately,
/// modelling the admission the rank rotation replaced: one engine-wide index into
/// whichever list an offer presents, resumed on the entry the previous offer had no
/// room for.
///
/// The surrounding dynamics are the union test's own — one free queue slot, and
/// every seed but `target` failing inside the pump that admitted it, so it is
/// admissible all over again on the next offer. Returns the offer number that
/// admits `target`, or `None` if `budget` offers pass without it.
#[cfg(feature = "cidr")]
fn positional_cursor_reaches(
  lists: &[&[SocketAddr]],
  target: SocketAddr,
  budget: u64,
) -> Option<u64> {
  let mut cursor = 0usize;
  for n in 0..budget {
    let list = lists[(n as usize) % lists.len()];
    // The single free slot goes to the first entry at or after the cursor.
    let admitted = cursor % list.len();
    if list[admitted] == target {
      return Some(n + 1);
    }
    // The cursor resumes on the first entry this offer had no room for.
    cursor = (admitted + 1) % list.len();
  }
  None
}

/// Two seed lists a caller would otherwise run separate join loops for must reach
/// every reachable seed when they are offered TOGETHER as one list.
///
/// The rotation is engine-wide and a call advances it only past the entries that
/// call saw, so the round-robin bound is over the addresses one offer names. Named
/// together, `[dead1, live]` and `[dead2, dead3]` are four distinct addresses on one
/// cycle against a single free slot, so the bound is `⌈4 ÷ 1⌉` offers: `live` is
/// dialed within four wherever it sits in the address order, and each offer sheds
/// the three the slot had no room for. The fixture puts `live` last in that order,
/// so the sweep here is walked in full and the whole four-offer bound is exercised
/// rather than a lucky prefix of it.
///
/// The negative control is the design the address cycle replaced. A positional
/// cursor round-robins a single list perfectly well, but two disjoint lists offered
/// alternately pin it: each list resumes at the index the other's offer left behind,
/// so with one slot the cursor lands on a failing seed every single time and `live`
/// is never admitted at all. That is the shape both halves of this contract remove —
/// the address cycle so an offered set sweeps whatever order it arrives in, and the
/// driver's union so the two lists arrive as one set.
#[cfg(feature = "cidr")]
#[test]
fn the_union_of_two_lists_offered_together_reaches_every_reachable_seed() {
  use memberlist_proto::CidrPolicy;

  let cfg = admission_cfg()
    .with_max_pending_seeds(1)
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.plane_mut().pool.push(1);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // Every `dead` is outside the policy, so its dial fails inside the pump that
  // admitted it and frees the slot again before the next offer. All three sort
  // below `live`, so `live` takes the last of the four turns and the sweep is
  // walked end to end.
  let dead1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 0, 0, 1)), 7002);
  let dead2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 0, 0, 2)), 7004);
  let dead3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 0, 0, 3)), 7005);
  let live = node_addr(7003);
  // The two lists as one offer, exactly as a driver that merges its live join loops
  // hands them over.
  let union = [dead1, live, dead2, dead3];
  let mut gossip = NoGossip;

  let mut offers = 0u64;
  while offers < 20 && !stream.connects.iter().any(|(_, a)| *a == live) {
    engine.join(&union).expect("join is accepted");
    engine.pump(now, &mut gossip, &mut stream);
    while engine.poll_event().is_some() {}
    offers += 1;
  }

  assert!(
    stream.connects.iter().any(|(_, a)| *a == live),
    "the reachable seed must be dialed once both lists are offered together, got {:?}",
    stream.connects
  );
  assert!(
    offers <= 4,
    "four distinct addresses against one free slot bound the sweep at four offers, took {offers}"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    3 * offers,
    "every offer holds four candidates and the queue one slot, so each sheds exactly three"
  );
  assert_eq!(
    engine.join_seeds_deduped(),
    0,
    "no address is offered twice while it is queued or mid-exchange"
  );
  assert_eq!(
    stream.connects.len(),
    1,
    "only the routable seed reaches the wire, got {:?}",
    stream.connects
  );

  // The control is a working round-robin on ONE list — it reaches `live` on the
  // second offer — so what pins it below is the alternation, not the model.
  assert_eq!(
    positional_cursor_reaches(&[&[dead1, live]], live, 20),
    Some(2),
    "a positional cursor sweeps a single list, so the model is not rigged to fail"
  );
  assert_eq!(
    positional_cursor_reaches(&[&[dead1, live], &[dead2, dead3]], live, 20),
    None,
    "two disjoint lists offered alternately pin a positional cursor on a failing seed \
     forever — the starvation the address cycle and the driver's union remove"
  );
}

/// Run two offers of `pair` against a queue with exactly one free slot, returning
/// the address admitted by each, in order.
///
/// The policy admits this node but neither seed, so every dial is rejected before
/// `connect` and its exchange terminalizes inside the pump that made it — the slot
/// is free again by the next offer and both entries are admissible every time. What
/// picks between them is therefore the admission order alone.
#[cfg(feature = "cidr")]
fn admitted_over_two_offers(pair: [SocketAddr; 2]) -> [SocketAddr; 2] {
  use memberlist_proto::CidrPolicy;

  let cfg = admission_cfg()
    .with_max_pending_seeds(1)
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.plane_mut().pool.push(1);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");
  let mut gossip = NoGossip;

  let mut admitted = [pair[0]; 2];
  for slot in admitted.iter_mut() {
    engine.join(&pair).expect("join is accepted");
    *slot = *engine
      .pending_seeds
      .front()
      .expect("the free slot must take one of the pair");
    engine.pump(now, &mut gossip, &mut stream);
    while engine.poll_event().is_some() {}
  }
  admitted
}

/// Admission orders seeds by the ADDRESS, so two endpoints that differ in any field
/// at all take turns instead of one holding the slot forever.
///
/// Ranking through a digest of the address cannot promise that: a digest has fewer
/// bits than the addresses it summarizes, so two distinct endpoints can share one
/// value, and a tie the selection resolves by offered position then hands the slot
/// to the same entry on every offer — the other is never dialed at all, however long
/// the caller retries. An IPv6 pair differing only in `flowinfo` or `scope_id` makes
/// that collision structural rather than a matter of luck, because those fields
/// address a destination but are not part of its IP or port.
///
/// The order over `SocketAddr` agrees with equality on every one of those fields, so
/// distinct endpoints never tie, and the pair here alternates: the lower takes the
/// first turn, the rotation moves to the higher, and the higher takes the second.
#[cfg(feature = "cidr")]
#[test]
fn join_admission_is_a_total_order_over_addresses() {
  // One IPv6 host and port, two scope ids: the same bytes on the wire, two
  // different destinations.
  let host = core::net::Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 7);
  let scoped_lo = SocketAddr::V6(core::net::SocketAddrV6::new(host, 7002, 0, 1));
  let scoped_hi = SocketAddr::V6(core::net::SocketAddrV6::new(host, 7002, 0, 2));
  assert!(
    scoped_lo < scoped_hi,
    "the scope id must separate two otherwise identical endpoints"
  );
  assert_eq!(
    admitted_over_two_offers([scoped_lo, scoped_hi]),
    [scoped_lo, scoped_hi],
    "a pair differing only in scope id must take alternate turns, not one of them \
     every turn"
  );
  // Offered the other way round the outcome must not move: the order is over the
  // addresses, not over the caller's list.
  assert_eq!(
    admitted_over_two_offers([scoped_hi, scoped_lo]),
    [scoped_lo, scoped_hi],
    "the caller's order must not decide which of the pair goes first"
  );

  // The same contract one family down, where the port is the only distinguishing
  // field.
  let ported_lo = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 0, 0, 1)), 7002);
  let ported_hi = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 0, 0, 1)), 7003);
  assert!(
    ported_lo < ported_hi,
    "the port must separate two endpoints on one host"
  );
  assert_eq!(
    admitted_over_two_offers([ported_lo, ported_hi]),
    [ported_lo, ported_hi],
    "a pair differing only in port must take alternate turns"
  );
}

/// A repeat of an address WITHIN one offer is a duplicate, not shed load: it takes
/// no second slot, and it must be counted as deduped rather than as a seed the cap
/// turned away. Only the distinct address that found no room is a drop.
#[test]
fn a_duplicate_within_one_offer_is_deduped_not_dropped() {
  let cfg = admission_cfg().with_max_pending_seeds(1);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, _now) = engine_from(cfg, ep_cfg);

  // Both are on one host, so the ports order them: `a` takes the only slot, its
  // repeat is the duplicate, and `b` is what the cap sheds.
  let a = node_addr(7002);
  let b = node_addr(7003);

  engine.join(&[a, a, b]).expect("join is accepted");

  assert_eq!(
    engine.pending_seeds.front(),
    Some(&a),
    "the lower-ranked address takes the only slot"
  );
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the repeat must not take a second slot"
  );
  assert_eq!(
    engine.join_seeds_deduped(),
    1,
    "the repeat is a duplicate of an address this very call queued"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    1,
    "only the distinct address the cap turned away is a drop"
  );
}

/// A single offer far longer than the queue costs the engine a scratch of one entry
/// per free slot plus one, and leaves both counters reconcilable against the length
/// of the list the caller passed.
///
/// The bound is structural rather than timed, so there is no timing assertion here:
/// what pins it is that the selection never holds more than that, which shows in the
/// outcome — the queue keeps exactly the lowest-ranked entries, the rotation names
/// the single entry just past them, and every other offered entry is counted once.
/// Offering the list from the largest address down is the adversarial order for a
/// bounded selection: every entry after the window fills displaces one already held,
/// so the scratch is rewritten on every step and nothing is decided by arrival order.
///
/// The queue order is the second half of the contract. Ranking picks WHICH entries a
/// full queue keeps; the caller's own order still decides the order they are dialed
/// in, so the four survivors come back in the descending order they were offered in,
/// not in the ascending order that selected them.
#[test]
fn selection_window_is_bounded_and_counts_stay_exact() {
  let cfg = admission_cfg().with_max_pending_seeds(4);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, _now) = engine_from(cfg, ep_cfg);

  let addr = |i: u32| {
    SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, (i >> 8) as u8, i as u8)),
      7946,
    )
  };
  let seeds: Vec<SocketAddr> = (0..1000u32).rev().map(addr).collect();
  engine.join(&seeds).expect("join is accepted");

  assert_eq!(
    engine.pending_seed_count(),
    4,
    "the queue must hold exactly the cap, however long the offer"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    996,
    "every offered entry the cap turned away is counted exactly once"
  );
  assert_eq!(
    engine.join_seeds_deduped(),
    0,
    "a thousand distinct addresses hold no duplicate"
  );
  assert_eq!(
    engine.join_rotation,
    Some(addr(4)),
    "the rotation names the one entry just past the room the queue had"
  );
  assert_eq!(
    engine.pending_seeds.iter().copied().collect::<Vec<_>>(),
    std::vec![addr(3), addr(2), addr(1), addr(0)],
    "the four lowest-ranked addresses must queue in the order the caller offered them"
  );
}

/// The ranking window a long offer runs never holds more than `max_pending_seeds + 1`
/// candidates, even when every entry ranks ahead of everything already held.
///
/// That window is the engine's own scratch, reserved at exactly that bound. An entry
/// inserted BEFORE the worst one is popped holds one more than the bound for the
/// length of the call, and nothing a caller can read would show it: the queue, the
/// drop count and the rotation all come out identical either way. What it does do is
/// grow the scratch past its reservation — the allocation the bound exists to avoid
/// on a device whose whole heap may be a few kilobytes.
///
/// A descending offer is the adversarial order. With the rotation unset, rank rises
/// with the address, so every entry after the first ranks ahead of every candidate
/// held and takes the insert-into-a-full-window path rather than being shed outright.
#[test]
fn a_descending_offer_never_holds_more_than_the_window_bound() {
  const CAP: usize = 7;
  const OFFERED: u32 = 20;

  let cfg = admission_cfg().with_max_pending_seeds(CAP);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, _now) = engine_from(cfg, ep_cfg);

  let addr = |i: u32| {
    SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, (i >> 8) as u8, i as u8)),
      7946,
    )
  };
  let seeds: Vec<SocketAddr> = (0..OFFERED).rev().map(addr).collect();
  let reserved = engine.join_window.capacity();
  engine.join(&seeds).expect("join is accepted");

  assert_eq!(
    engine.pending_seed_count(),
    CAP,
    "the queue must hold exactly the cap, however long the offer"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    u64::from(OFFERED) - CAP as u64,
    "and every entry the cap turned away is counted exactly once"
  );

  assert_eq!(
    engine.join_window_high_water,
    CAP + 1,
    "the window fills to the room the queue had plus the entry that names the next \
     rotation, and never holds more than that at any point of the call"
  );
  assert_eq!(
    engine.join_window.capacity(),
    reserved,
    "so the scratch reserved at construction still holds the ranking, unchanged"
  );
}

/// At the SHIPPED default cap, an offer far longer than the queue ranks entirely
/// inside the scratch construction reserved: the window fills to
/// `max_pending_seeds + 1` and the buffer never grows.
///
/// This is the bound that matters on the tier this crate targets. A call-local
/// buffer would have to hold 33 candidates here, so every such `join` would reach
/// the heap — silently, since the queue, the drop count and the rotation all come out
/// the same either way. Owning one buffer of exactly that capacity moves the single
/// allocation to construction, where a driver can account for it.
///
/// A descending offer is the adversarial order (see the sibling test): every entry
/// after the first ranks ahead of everything held, so each takes the
/// insert-into-a-full-window path rather than being shed outright.
#[test]
fn a_default_cap_offer_ranks_inside_the_scratch_reserved_at_construction() {
  const OFFERED: u32 = 40;

  let cfg = admission_cfg();
  let cap = cfg.max_pending_seeds;
  assert_eq!(
    cap,
    crate::DEFAULT_MAX_PENDING_SEEDS,
    "this pins the SHIPPED default, so it must be the default the crate ships"
  );

  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946));
  let (mut engine, _now) = engine_from(cfg, ep_cfg);

  assert_eq!(
    engine.join_window.capacity(),
    cap + 1,
    "construction reserves the whole bound: the cap's worth of room plus the entry \
     that names the next rotation"
  );
  let reserved = engine.join_window.capacity();

  let addr = |i: u32| {
    SocketAddr::new(
      IpAddr::V4(Ipv4Addr::new(10, 0, (i >> 8) as u8, i as u8)),
      7946,
    )
  };
  let seeds: Vec<SocketAddr> = (0..OFFERED).rev().map(addr).collect();
  engine.join(&seeds).expect("join is accepted");

  assert_eq!(
    engine.join_window_high_water,
    cap + 1,
    "the window fills to the full bound and never holds more at any point of the call"
  );
  assert_eq!(
    engine.join_window.capacity(),
    reserved,
    "and having filled it, the ranking still did not grow the scratch — the join \
     allocated nothing"
  );

  assert_eq!(
    engine.pending_seed_count(),
    cap,
    "the queue holds exactly the cap"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    u64::from(OFFERED) - cap as u64,
    "and every entry it had no room for is counted once — including the one the \
     rotation now resumes from, which is why an over-long offer into an EMPTY queue \
     already moves this counter"
  );
}

/// An offer that finds NO free queue slot admits nothing, and costs the offered set
/// no turn: the rotation stays on the entry that already ranked first, so the next
/// offer with room admits exactly the address the full ones would have.
///
/// This is what makes the fairness bound a statement about capacity-bearing offers.
/// Were a zero-room offer to advance the rotation PAST the entry it could not serve,
/// a caller re-offering into a persistently full queue would walk the rotation over
/// its whole set while admitting nothing, and the entry that came up each time would
/// be skipped once room finally appeared — starvation driven by the retries meant to
/// prevent it.
///
/// The queue is kept full by a PARKED seed head rather than by withholding the pump:
/// with the pool empty the head the queue already gave up cannot be dialed, and while
/// a seed head is parked the pump admits no further seed — so the pump runs between
/// every offer here and the one queued address stays queued.
#[test]
fn offers_that_find_no_room_neither_count_nor_lose_progress() {
  let cfg = admission_cfg().with_max_pending_seeds(1);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(30));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");
  let mut gossip = NoGossip;

  // The pool is empty, so the first seed becomes the parked head: it holds the
  // queue's place in the dial order and cannot make progress until a slot appears.
  let parked = node_addr(7001);
  engine.join(&[parked]).expect("join is accepted");
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    engine.plane_mut().has_parked_seed(),
    "the first seed must park for want of a slot"
  );

  // The one queue slot now holds an address that cannot leave it: no room, and no
  // head rule to give it up while a seed head is already parked.
  let held = node_addr(7002);
  engine.join(&[held]).expect("join is accepted");
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the cap is one, and that one slot is taken"
  );

  // Three offers of the same two addresses, each meeting a full queue.
  let lower = node_addr(7003);
  let higher = node_addr(7004);
  assert!(lower < higher, "the ports must order the pair");
  for offer in 1..=3u64 {
    engine.join(&[lower, higher]).expect("join is accepted");
    engine.pump(now, &mut gossip, &mut stream);
    assert_eq!(
      engine.pending_seed_count(),
      1,
      "offer {offer} met a full queue, so it admitted nothing"
    );
    assert_eq!(
      engine.pending_seeds.front(),
      Some(&held),
      "and displaced nothing already queued"
    );
    assert_eq!(
      engine.join_seeds_dropped(),
      2 * offer,
      "both its entries are shed and counted, offer {offer}"
    );
    assert_eq!(
      engine.join_rotation,
      Some(lower),
      "and the rotation rests on the entry that ranks first, offer {offer}"
    );
  }
  assert_eq!(
    engine.join_seeds_deduped(),
    0,
    "none of those entries was already queued or already being exchanged with"
  );

  // Free the head: a slot appears, the parked head is dialed, and the address behind
  // it becomes the new head — so the queue empties and room returns.
  engine.plane_mut().pool.push(1);
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    !engine.plane_mut().has_parked_seed(),
    "the freed slot must dial the parked head"
  );
  assert_eq!(
    stream.connects,
    std::vec![(1, parked)],
    "and that dial is the head, got {:?}",
    stream.connects
  );
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_seed_count(),
    0,
    "with nothing parked, the queued address is taken as the new head"
  );

  // The next offer of the same pair admits the address the full offers had ranked
  // first — the three of them cost the set no turn.
  engine.join(&[lower, higher]).expect("join is accepted");
  assert_eq!(
    engine.pending_seeds.front(),
    Some(&lower),
    "the entry the zero-room offers left the rotation on must be the one admitted"
  );
  assert_eq!(
    engine.join_seeds_dropped(),
    7,
    "only the one entry this offer had no room for is a further drop"
  );
  assert_eq!(
    engine.join_rotation,
    Some(higher),
    "and the rotation now moves on to the entry this offer could not serve"
  );
}

/// Seeds are handed to the machine as the pool can back them, plus ONE head that
/// holds the queue's place in the dial order. Everything past the head stays a bare
/// address — no bridge, no full-state encoding, no deadline — so a long seed list
/// costs one encoding per slot plus that single head, not one per seed. While the
/// head is parked no further seed is admitted, so the exposure stays at one.
#[test]
fn join_admits_seeds_to_pool_capacity_plus_one_queue_head() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.plane_mut().pool.push(1);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 6, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  engine
    .join(&[node_addr(7002), node_addr(7003), node_addr(7004)])
    .expect("join is accepted");
  assert_eq!(engine.pending_seed_count(), 3, "all three seeds queue");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the single free slot backs one seed, and one more is admitted as the head"
  );
  assert_eq!(
    engine.pending_dial_count(),
    1,
    "the head parks; the seed admitted against the free slot took it"
  );
  assert!(
    engine.plane_mut().has_parked_seed(),
    "the parked dial is the seed head"
  );
  assert_eq!(
    engine.outbound_correlation_len(),
    2,
    "two exchanges — and so two full-state encodings — for three seeds"
  );
  assert_eq!(
    stream.connects.len(),
    1,
    "exactly one dial, got {:?}",
    stream.connects
  );

  // A second slot appears: it goes to the parked head, and the seed still queued
  // behind it is NOT admitted, because a seed is already holding a place.
  engine.plane_mut().pool.push(6);
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the last seed waits while a seed head is parked"
  );
  assert_eq!(
    stream.connects.len(),
    2,
    "the freed slot dials the head, got {:?}",
    stream.connects
  );
  assert_eq!(
    engine.outbound_correlation_len(),
    2,
    "and starts no further encoding"
  );

  // With the head dialed, nothing seed-originated is parked any more, so the next
  // pump takes the last seed as the new head.
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(engine.pending_seed_count(), 0, "the queue drains");
  assert_eq!(
    engine.outbound_correlation_len(),
    3,
    "one encoding per seed in total, spread across three pumps"
  );
}

/// A seed left queued for want of capacity is work the ENGINE deferred, so the
/// pump asks for an immediate re-pump the moment a slot is free to admit it
/// against — but not while the pool is empty, when there is nothing to admit it
/// against and some other deadline governs the wake.
#[test]
fn queued_seeds_with_a_free_slot_request_an_immediate_repump() {
  let stream_timeout = Duration::from_secs(5);
  let (mut engine, now) = engine_with_stream_timeout(stream_timeout);
  engine.plane_mut().pool.push(5);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[5, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // The one free slot is spoken for by a reliable send before the seeds are queued.
  // Two seeds, so one is left as a bare address behind the head — the queue entry
  // this test is about.
  engine
    .send_reliable(node_addr(7002), bytes::Bytes::from_static(b"first"), now)
    .expect("send_reliable queues the exchange");
  let head = node_addr(7003);
  let waiting = node_addr(7004);
  engine.join(&[head, waiting]).expect("join is accepted");

  let mut gossip = NoGossip;
  let wake = engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the send claimed the only slot, so the queue gives up only its head"
  );
  assert_eq!(
    stream.connects,
    std::vec![(5, node_addr(7002))],
    "only the send was dialed; the seed head parks behind it"
  );
  assert!(
    wake.is_some_and(|w| w > now),
    "with an empty pool there is nothing to admit against, so no immediate re-pump"
  );

  // Both exchange deadlines elapse: the send's slot is aborted and reaped back into
  // the pool within this same tick, and the parked head — which owns a deadline of
  // its own from the moment it was admitted — is reaped with it, leaving a free slot
  // and the still-queued seed.
  let later = now + stream_timeout + Duration::from_secs(1);
  let wake = engine.pump(later, &mut gossip, &mut stream);
  assert_eq!(
    engine.pool_free_count(),
    1,
    "the failed send's slot is reaped back in-tick"
  );
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the seed behind the head is still queued"
  );
  assert_eq!(
    wake,
    Some(later),
    "a queued seed with a free slot must ask for an immediate re-pump"
  );

  // That re-pump admits it against the reaped slot and dials it the same tick.
  engine.pump(later, &mut gossip, &mut stream);
  assert_eq!(engine.pending_seed_count(), 0, "the seed is admitted");
  assert!(
    stream.connects.contains(&(5, waiting)),
    "the reaped slot is spent on the queued seed, got {:?}",
    stream.connects
  );
}

/// `send_reliable` refuses once dials are already waiting the configured bound
/// beyond what the pool could take, and says so at the call site as typed
/// backpressure rather than parking yet another request.
#[test]
fn send_reliable_backpressure_bounds_parked_dials() {
  let cfg = admission_cfg().with_max_pending_dials(3);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  // A listener but no dial slots, so every send parks.
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  for i in 0..3u16 {
    engine
      .send_reliable(node_addr(7002 + i), bytes::Bytes::from_static(b"x"), now)
      .expect("sends up to the bound are admitted");
  }

  let over = engine.send_reliable(node_addr(7010), bytes::Bytes::from_static(b"x"), now);
  match over {
    Err(memberlist_proto::Error::UserDialBacklogFull(full)) => {
      assert_eq!(full.limit(), 3, "the carried limit is the configured bound");
      assert_eq!(full.peer(), node_addr(7010), "and the refused destination");
    }
    other => panic!("expected UserDialBacklogFull, got {other:?}"),
  }

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_dial_count(),
    3,
    "exactly the admitted sends parked"
  );
  assert_eq!(
    engine.pending_dial_rejections(),
    0,
    "the API pre-check spared the action drain any refusal"
  );

  assert!(
    matches!(
      engine.send_reliable(node_addr(7011), bytes::Bytes::from_static(b"x"), now),
      Err(memberlist_proto::Error::UserDialBacklogFull(_))
    ),
    "the bound still holds once the parked set itself carries the backlog"
  );
}

/// A left node answers `NotRunning`, not backpressure, however saturated its dial
/// backlog is. The two refusals mean opposite things — pace and retry versus never
/// again — so a lifecycle verdict must not arrive disguised as a retryable one.
///
/// The backlog is carried by the PARKED set here, which `leave` does not clear, so
/// the pre-check still measures a saturated node on the send that follows: the only
/// thing that can spare the caller the retryable answer is the lifecycle check
/// running first.
#[test]
fn send_reliable_reports_the_lifecycle_before_the_dial_backlog() {
  let cfg = admission_cfg().with_max_pending_dials(2);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  // A listener but no dial slots, so every send parks and the bound saturates.
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  for i in 0..2u16 {
    engine
      .send_reliable(node_addr(7002 + i), bytes::Bytes::from_static(b"x"), now)
      .expect("sends up to the bound are admitted");
  }
  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_dial_count(),
    2,
    "both sends park, so the backlog lives in the parked set"
  );
  assert!(
    matches!(
      engine.send_reliable(node_addr(7010), bytes::Bytes::from_static(b"x"), now),
      Err(memberlist_proto::Error::UserDialBacklogFull(_))
    ),
    "the backlog is saturated while the node is still running"
  );

  // Leave, and do NOT pump: the parked dials the pre-check measures are untouched.
  engine.leave(now).expect("leave from a running node");
  assert_eq!(
    engine.pending_dial_count(),
    2,
    "leave alone does not unwind the parked dials, so the backlog still reads full"
  );

  match engine.send_reliable(node_addr(7011), bytes::Bytes::from_static(b"x"), now) {
    Err(memberlist_proto::Error::NotRunning) => {}
    other => panic!("a left node must refuse a reliable send as NotRunning, got {other:?}"),
  }
}

/// The bound applies to EVERY dial source, not just the application's. A
/// protocol-paced dial requested while the caller has saturated the bound is
/// failed through the machine's never-connected path — counted, and terminal —
/// rather than left parked past the cap, and never reaches the wire. The trim runs
/// within the same pump the dial was requested in, so it is never dialed.
#[test]
fn machine_originated_connect_over_cap_is_failed_not_parked() {
  let cfg = admission_cfg().with_max_pending_dials(3);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60))
    .with_push_pull_interval(Duration::from_millis(100));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // A peer for the periodic push/pull to target.
  engine.inject_alive(SmolStr::new("peer"), node_addr(7947), now);
  for i in 0..3u16 {
    engine
      .send_reliable(node_addr(7002 + i), bytes::Bytes::from_static(b"x"), now)
      .expect("sends up to the bound are admitted");
  }

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(engine.pending_dial_count(), 3, "the bound is saturated");
  assert_eq!(
    engine.pending_dial_rejections(),
    0,
    "nothing has been refused yet"
  );
  while engine.poll_event().is_some() {}

  // Advance past the push/pull interval so the machine requests its own dial.
  let t = now + Duration::from_millis(150);
  engine.pump(t, &mut gossip, &mut stream);

  assert_eq!(
    engine.pending_dial_rejections(),
    1,
    "the machine-paced dial must be refused, not parked"
  );
  assert_eq!(
    engine.pending_dial_count(),
    3,
    "the parked set must not grow past the bound"
  );

  assert!(
    stream.connects.is_empty(),
    "a refused dial must never reach the wire, got {:?}",
    stream.connects
  );
}

/// A dial refused by the bound still carries its originating `StreamId` through
/// to its terminal completion, and the correlation entry is pruned afterwards. A
/// driver resolves a parked reliable-send waiter by that `StreamId`, so a refusal
/// recorded before the correlation would leave the waiter hanging.
///
/// This is also the one shape in which an application send admitted at the call
/// site is still refused after the action drain: the listener replenishing itself
/// from the pool takes the free slot the pre-check measured against. The trim sheds
/// the newest intent first, so it is the SECOND send — the one the pre-check
/// admitted last — that is refused.
#[test]
fn over_cap_rejection_keeps_the_stream_id_correlation() {
  let cfg = admission_cfg().with_max_pending_dials(1);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.plane_mut().pool.push(5);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[5, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");
  // The listener's passive open has settled, so this pump consumes it and
  // replenishes the listener from the pool — the free slot the sends measured
  // against is gone by the time the actions drain.
  stream.sock_mut(9).accepted = Some(node_addr(7900));
  stream.sock_mut(9).established = true;

  engine
    .send_reliable(node_addr(7002), bytes::Bytes::from_static(b"first"), now)
    .expect("the first send is admitted");
  let refused_sid = engine
    .send_reliable(node_addr(7003), bytes::Bytes::from_static(b"second"), now)
    .expect("the second send is admitted against the then-free slot");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.outbound_correlation_len(),
    2,
    "both sends were correlated as their Connects drained"
  );
  assert_eq!(
    engine.pending_dial_rejections(),
    1,
    "the slot the listener took leaves the second send over the bound"
  );
  assert_eq!(engine.pending_dial_count(), 1, "only the first send parked");

  let mut resolved = None;
  while let Some(ev) = engine.poll_event() {
    if matches!(ev, Event::ExchangeCompleted(_)) {
      if let Some(sid) = engine.last_completed_send() {
        resolved = Some(sid);
      }
    }
  }
  assert_eq!(
    resolved,
    Some(refused_sid),
    "the refused send's completion must resolve the StreamId its caller holds"
  );
  assert_eq!(
    engine.outbound_correlation_len(),
    1,
    "the refused exchange's correlation entry is pruned, leaving only the parked send"
  );
}

/// A queued seed cannot be starved by a caller that keeps sending. The queue's head
/// becomes a real exchange at the pump that first cannot back it with a slot, so it
/// sits in the machine's dial order AHEAD of every send issued afterwards and takes
/// the next freed slot before any of them. Seeds behind the head stay bare
/// addresses, and exactly one seed-originated exchange is outstanding while they do.
#[test]
fn a_queued_seed_takes_a_freed_slot_before_later_sends() {
  let cfg = admission_cfg();
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.plane_mut().pool.push(1);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 6, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // The one dial slot is spoken for before the join, so no seed can be backed by it.
  let occupier = node_addr(7002);
  engine
    .send_reliable(occupier, bytes::Bytes::from_static(b"occupier"), now)
    .expect("send_reliable queues the exchange");

  let seed = node_addr(7003);
  engine
    .join(&[seed, node_addr(7004), node_addr(7005)])
    .expect("join is accepted");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    stream.connects,
    std::vec![(1, occupier)],
    "the older send takes the only slot"
  );
  assert_eq!(
    engine.pending_dial_count(),
    1,
    "the seed head parks; nothing else is outstanding"
  );
  assert!(
    engine.plane_mut().has_parked_seed(),
    "the parked dial is the seed head"
  );
  assert_eq!(
    engine.pending_seed_count(),
    2,
    "the seeds behind the head stay bare addresses"
  );

  // A steady application send rate, one per pump, each newer than the parked head.
  let later_sends = [node_addr(7010), node_addr(7011), node_addr(7012)];
  for to in later_sends {
    engine
      .send_reliable(to, bytes::Bytes::from_static(b"later"), now)
      .expect("the sends stay within the default bound");
    engine.pump(now, &mut gossip, &mut stream);
  }
  assert_eq!(
    engine.pending_seed_count(),
    2,
    "no further seed is admitted while the head is still parked"
  );
  assert_eq!(
    engine.plane_mut().pending_dial_count(),
    4,
    "the head plus the three later sends are all waiting"
  );

  // A slot frees. It must go to the seed, not to any send issued after it.
  engine.plane_mut().pool.push(6);
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    stream.connects,
    std::vec![(1, occupier), (6, seed)],
    "the freed slot must dial the seed ahead of every later send, got {:?}",
    stream.connects
  );
  for to in later_sends {
    assert!(
      !stream.connects.iter().any(|(_, peer)| *peer == to),
      "no send issued after the seed's admission may be dialed before it ({to})"
    );
  }
}

/// `join` dedups against JOIN INTENT, not against any connection that happens to
/// share the address. An application send or a reliable ping to a seed is a
/// different exchange with a different purpose: suppressing the seed against one
/// discards the join outright while still returning `Ok`, leaving a caller waiting
/// on its own timeout with no attempt outstanding at all.
#[test]
fn join_dedups_against_join_intent_not_any_connection_to_the_address() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(60));
  engine.plane_mut().pool.push(1);
  engine.plane_mut().pool.push(2);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[1, 2, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // Two user messages, one per address, dialed on the pool's two slots.
  let dialing_peer = node_addr(7002);
  let half_closed_peer = node_addr(7003);
  for to in [dialing_peer, half_closed_peer] {
    engine
      .send_reliable(to, bytes::Bytes::from_static(b"user"), now)
      .expect("send_reliable queues the exchange");
  }
  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);

  // One stays mid-handshake; the other is driven to the half-closed stage a reply
  // is still awaited in. Neither is a join.
  let half_closed_eid = *engine
    .plane_mut()
    .connections
    .iter()
    .find(|(_, c)| c.peer == half_closed_peer)
    .expect("the second send's connection")
    .0;
  engine
    .plane_mut()
    .connections
    .get_mut(&half_closed_eid)
    .expect("the second send's connection")
    .state = ConnState::HalfClosed;
  assert_eq!(
    engine
      .plane_mut()
      .connections
      .values()
      .find(|c| c.peer == dialing_peer)
      .map(|c| c.state),
    Some(ConnState::Dialing),
    "the first send is mid-handshake"
  );

  engine
    .join(&[dialing_peer, half_closed_peer])
    .expect("join is accepted");
  assert_eq!(
    engine.pending_seed_count(),
    2,
    "neither a dialing nor a half-closed user message may suppress its seed"
  );
  assert_eq!(
    engine.join_seeds_deduped(),
    0,
    "and neither may be counted as a duplicate join"
  );

  // A JOIN exchange to the same address IS a reason to skip: one push/pull per seed
  // at a time is what keeps the retry loop from re-encoding the whole membership.
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    engine.plane_mut().has_parked_seed(),
    "the queue's head became a join exchange"
  );
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the second seed waits behind the head"
  );

  engine.join(&[dialing_peer]).expect("join is accepted");
  assert_eq!(
    engine.pending_seed_count(),
    1,
    "the seed with a live join exchange is not re-queued"
  );
  assert_eq!(
    engine.join_seeds_deduped(),
    1,
    "and the skip is counted as the duplicate join it is"
  );
}

/// The excess bound is evaluated over the parked set the drain LEAVES, not the one
/// it starts from. The machine surfaces every `Connect` before any teardown of the
/// same tick, so a tick that both reaps cap-saturating parked dials and emits a
/// fresh protocol-paced dial would, measured inline, refuse the new dial against
/// entries the same drain is about to remove — shedding liveness work while
/// capacity was in fact being freed.
#[test]
fn a_fresh_connect_survives_a_tick_that_reaps_the_saturating_dials() {
  let stream_timeout = Duration::from_millis(100);
  let cfg = admission_cfg().with_max_pending_dials(3);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(stream_timeout)
    .with_push_pull_interval(Duration::from_millis(100));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // A peer for the periodic push/pull to target.
  engine.inject_alive(SmolStr::new("peer"), node_addr(7947), now);
  for i in 0..3u16 {
    engine
      .send_reliable(node_addr(7002 + i), bytes::Bytes::from_static(b"x"), now)
      .expect("sends up to the bound are admitted");
  }

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(engine.pending_dial_count(), 3, "the bound is saturated");
  assert_eq!(engine.pending_dial_rejections(), 0, "nothing shed yet");
  while engine.poll_event().is_some() {}

  // One tick past BOTH the sends' exchange deadline and the push/pull interval: step
  // 6 expires all three parked sends and mints the periodic push/pull, and the drain
  // sees the fresh `Connect` before any of the three `Abort`s.
  let t = now + stream_timeout + Duration::from_millis(50);
  engine.pump(t, &mut gossip, &mut stream);

  assert_eq!(
    engine.pending_dial_rejections(),
    0,
    "the fresh dial must not be shed against dials the same drain removed"
  );
  assert_eq!(
    engine.pending_dial_count(),
    1,
    "the three expired sends are gone and the fresh dial is parked in their place"
  );
  assert!(
    !engine.plane_mut().has_parked_seed(),
    "the surviving parked dial is the machine's own, not a seed"
  );
}

/// Over the cap, the trim sheds the NEWEST intent first, so what survives is the
/// oldest application sends the bound still admits, plus the seed head holding the
/// join queue's place. The head is outside the bound: it is neither shed nor able to
/// push a send over. Every shed send still resolves the `StreamId` its caller holds.
#[test]
fn the_post_drain_trim_sheds_the_newest_non_seed_dials() {
  let cfg = admission_cfg().with_max_pending_dials(2);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.plane_mut().pool.push(5);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[5, 6, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");
  // The listener's passive open has settled, so this pump consumes it and
  // replenishes the listener from the pool: the free slot the sends were admitted
  // against is gone by the time the actions drain, putting the parked set over the
  // cap without any send having to be refused at the call site.
  stream.sock_mut(9).accepted = Some(node_addr(7900));
  stream.sock_mut(9).established = true;

  let oldest = node_addr(7002);
  engine
    .send_reliable(oldest, bytes::Bytes::from_static(b"oldest"), now)
    .expect("the first send is admitted");
  let kept_middle = engine
    .send_reliable(node_addr(7003), bytes::Bytes::from_static(b"middle"), now)
    .expect("the second send is admitted");
  let shed = engine
    .send_reliable(node_addr(7004), bytes::Bytes::from_static(b"newest"), now)
    .expect("the third send is admitted");

  // Queued last, so the seed's exchange is the NEWEST of the four — and still
  // outside the bound, neither shed by it nor counted against it.
  engine.join(&[node_addr(7005)]).expect("join is accepted");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.pending_dial_rejections(),
    1,
    "one send stood beyond the bound over non-seed dials and was shed"
  );
  assert_eq!(
    engine.pending_dial_count(),
    3,
    "the two oldest sends and the seed head remain parked"
  );
  assert!(
    engine.plane_mut().has_parked_seed(),
    "the seed head is outside the trim however new it is"
  );

  let mut resolved = Vec::new();
  while let Some(ev) = engine.poll_event() {
    if matches!(ev, Event::ExchangeCompleted(_)) {
      if let Some(sid) = engine.last_completed_send() {
        resolved.push(sid);
      }
    }
  }
  assert_eq!(
    resolved,
    std::vec![shed],
    "the shed send's completion must resolve the StreamId its caller holds"
  );
  assert!(
    !resolved.contains(&kept_middle),
    "the trim takes the newest intent first, so the middle send must not be failed"
  );

  // A slot frees: it goes to the OLDEST survivor, proving the trim kept the right
  // end of the queue. The seed's exchange is newer, so it waits one more turn.
  engine.plane_mut().pool.push(6);
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    stream.connects.iter().any(|(_, peer)| *peer == oldest),
    "the oldest surviving send is dialed, got {:?}",
    stream.connects
  );
}

/// The bound is over the caller's and the protocol's own parked dials, so a join
/// that arrives AFTER a burst has saturated it cannot make the trim fire. The queue
/// head is the engine's own admission and the youngest intent in the plane; counting
/// it would have the trim shed the newest of the sends admitted before the join
/// existed — the head evicting intent that preceded it, the exact inverse of the
/// oldest-survives order the trim keeps.
#[test]
fn a_late_join_head_never_evicts_an_older_admitted_send() {
  const CAP: usize = 2;
  let cfg = admission_cfg().with_max_pending_dials(CAP);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  // A listener but no dial slots, so every send parks and the bound saturates at
  // exactly CAP, with no free slot to widen it.
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  for i in 0..CAP as u16 {
    engine
      .send_reliable(node_addr(7002 + i), bytes::Bytes::from_static(b"x"), now)
      .expect("sends up to the bound are admitted");
  }

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_dial_count(),
    CAP,
    "every admitted send parked"
  );
  assert_eq!(
    engine.pending_dial_rejections(),
    0,
    "the burst sits exactly at the bound, so nothing is shed"
  );
  while engine.poll_event().is_some() {}

  // The join arrives after the burst. Its head is admitted past measured capacity
  // and parks as the NEWEST exchange in the plane.
  engine.join(&[node_addr(7005)]).expect("join is accepted");
  engine.pump(now, &mut gossip, &mut stream);

  assert!(
    engine.plane_mut().has_parked_seed(),
    "the join's head must park, or there is nothing over capacity to test"
  );
  assert_eq!(
    engine.pending_dial_rejections(),
    0,
    "a join must not shed a send that was admitted before it"
  );
  assert_eq!(
    engine.pending_dial_count(),
    CAP + 1,
    "the head parks ON TOP of the saturated burst; no send may be evicted for it"
  );
  while let Some(ev) = engine.poll_event() {
    assert!(
      !matches!(ev, Event::ExchangeCompleted(_)),
      "no admitted send may be terminalized by the arrival of a join"
    );
  }

  // Counting the head with the sends is exactly what would fire the trim: measured
  // over ALL parked entries the plane stands one past the bound, and the victim that
  // excess would take is the newest send — admitted, and older than the head that
  // displaced it. Measured over the non-seed entries alone it is not over at all.
  assert_eq!(
    engine
      .pending_dial_count()
      .saturating_sub(engine.pool_free_count())
      .saturating_sub(CAP),
    1,
    "the all-entries excess is one; only the non-seed excess is zero"
  );

  // The caller's own budget is spent all the same: the bound over non-seed entries
  // is saturated, so the next send is still refused at the call site.
  assert!(
    matches!(
      engine.send_reliable(node_addr(7010), bytes::Bytes::from_static(b"x"), now),
      Err(memberlist_proto::Error::UserDialBacklogFull(_))
    ),
    "the caller's bound is saturated whether or not a seed is parked"
  );
}

/// The ceiling is a POST-PUMP bound, so it holds even when the free slot the pool
/// had goes somewhere other than the backlog it would have been credited to. The
/// dial site serves the OLDEST parked exchange first, seed or not; a ceiling applied
/// ahead of it can only assume that slot will be spent on the non-seed backlog, and
/// when an older seed head takes it instead the pump ends one entry over the cap
/// with no free slot behind it.
#[test]
fn the_parked_dial_ceiling_holds_when_a_freed_slot_goes_to_an_older_seed() {
  const CAP: usize = 2;
  let cfg = admission_cfg().with_max_pending_dials(CAP);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(Duration::from_secs(60))
    .with_push_pull_interval(Duration::from_millis(100));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.set_listener(9);
  let mut stream = ProgRel::new(&[6, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  // A peer for the periodic push/pull to dial.
  let peer = node_addr(7947);
  engine.inject_alive(SmolStr::new("peer"), peer, now);

  // The seed head parks with no slot to take, so it is the OLDEST parked exchange in
  // the plane and the dial site reaches it ahead of every send below.
  let seed = node_addr(7005);
  engine.join(&[seed]).expect("join is accepted");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    engine.plane_mut().has_parked_seed(),
    "the seed head must park, or nothing in the plane is older than the sends"
  );
  assert_eq!(
    engine.pending_dial_count(),
    1,
    "only the head is outstanding"
  );
  while engine.poll_event().is_some() {}

  // CAP sends, minted after the head and dated at the pump's own instant so their
  // exchange deadlines outlive it.
  let t = now + Duration::from_millis(150);
  let sends = [node_addr(7002), node_addr(7003)];
  for to in sends {
    engine
      .send_reliable(to, bytes::Bytes::from_static(b"x"), t)
      .expect("sends up to the ceiling are admitted");
  }
  // ONE slot, free when the dial site runs.
  engine.plane_mut().pool.push(6);

  // `t` is past the push/pull interval, so step 6 mints one fresh protocol dial in
  // the same tick: CAP + 1 non-seed dials park behind a single free slot the older
  // seed head is first in line for.
  engine.pump(t, &mut gossip, &mut stream);

  let parked_non_seed = engine.plane_mut().pending_non_seed_dial_count();
  assert!(
    parked_non_seed <= CAP,
    "the ceiling is absolute: at most {CAP} caller- and protocol-originated dials may \
     stay parked after a pump, got {parked_non_seed}"
  );
  assert!(
    !engine.plane_mut().has_parked_seed(),
    "the free slot went to the older seed head, so no seed is left parked"
  );
  assert!(
    stream.connects.iter().any(|(_, p)| *p == seed),
    "the head's dial is the one that reached the wire, got {:?}",
    stream.connects
  );
  assert_eq!(
    engine.pending_dial_rejections(),
    1,
    "exactly the one entry the pool could not back is shed"
  );

  // The NEWEST non-seed intent is this tick's protocol dial, so that is the entry
  // taken: its exchange is gone from the plane while both older sends stay parked.
  let live: Vec<SocketAddr> = engine
    .plane_mut()
    .connections
    .values()
    .map(|c| c.peer)
    .collect();
  assert!(
    !live.contains(&peer),
    "the newest non-seed intent — this tick's protocol dial — is the one shed, \
     but {peer} is still in {live:?}"
  );
  for to in sends {
    assert!(
      live.contains(&to),
      "the older sends survive the trim, but {to} is gone from {live:?}"
    );
  }
}

/// The same ceiling holds when the free slot goes to the LISTENER. `ensure_listener`
/// claims its slot inside the rebalance, ahead of every parked dial, so a ceiling
/// applied earlier in the tick credits the backlog with a slot the listener is about
/// to take — and the pump ends over the cap with the listener correctly re-armed.
#[test]
fn the_parked_dial_ceiling_holds_when_the_listener_takes_the_only_free_slot() {
  const CAP: usize = 2;
  let stream_timeout = Duration::from_millis(100);
  let cfg = admission_cfg().with_max_pending_dials(CAP);
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(stream_timeout)
    .with_push_pull_interval(Duration::from_millis(100));
  let (mut engine, now) = engine_from(cfg, ep_cfg);
  engine.set_listener(9);
  engine.plane_mut().pool.push(5);
  let mut stream = ProgRel::new(&[5, 9]);
  stream
    .listen(9, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");

  let peer = node_addr(7947);
  engine.inject_alive(SmolStr::new("peer"), peer, now);

  // One dial takes the only pooled slot and never establishes: its exchange deadline
  // is what frees that slot again, mid-tick, at the pump below.
  let victim = node_addr(7001);
  engine
    .send_reliable(victim, bytes::Bytes::from_static(b"x"), now)
    .expect("the first send is admitted");

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    stream.connects.iter().any(|(_, p)| *p == victim),
    "the pooled slot was spent on the first send"
  );
  assert_eq!(
    engine.pool_free_count(),
    0,
    "and nothing is left in the pool behind it"
  );
  while engine.poll_event().is_some() {}

  // The listener's passive open settles, so the next pump hands its slot to the
  // accepted exchange and finds nothing in the pool to replenish from: the node
  // spends that tick with NO listener until a slot frees.
  stream.sock_mut(9).accepted = Some(node_addr(7900));
  stream.sock_mut(9).established = true;

  let t = now + Duration::from_millis(150);
  let sends = [node_addr(7002), node_addr(7003)];
  for to in sends {
    engine
      .send_reliable(to, bytes::Bytes::from_static(b"x"), t)
      .expect("sends up to the ceiling are admitted");
  }
  let dialed_before = stream.connects.len();

  // All in one tick: the victim's exchange elapses and its `Abort` frees the only
  // slot, step 6 mints a fresh protocol dial, and CAP + 1 non-seed dials stand parked
  // when the rebalance hands that slot to the missing listener.
  engine.pump(t, &mut gossip, &mut stream);

  assert!(
    engine.listener_present(),
    "the freed slot re-armed the listener, which claims it ahead of every dial"
  );
  let parked_non_seed = engine.plane_mut().pending_non_seed_dial_count();
  assert!(
    parked_non_seed <= CAP,
    "the ceiling is absolute: at most {CAP} caller- and protocol-originated dials may \
     stay parked after a pump, got {parked_non_seed}"
  );
  assert_eq!(
    engine.pending_dial_rejections(),
    1,
    "exactly the one entry the pool could not back is shed"
  );
  assert_eq!(
    stream.connects.len(),
    dialed_before,
    "the slot went to the listener, so no parked dial reached the wire, got {:?}",
    stream.connects
  );
}

/// Both admission ceilings are rejected at zero, by the shared preflight and by
/// construction alike: a zero seed queue could never join, and a zero parked-dial
/// bound would refuse every dial a momentarily-empty pool could not take at once.
#[test]
fn zero_admission_caps_are_rejected_as_the_knobs_they_are() {
  let now = Instant::from_origin(Duration::from_secs(86_400));

  for (cfg, name) in [
    (admission_cfg().with_max_pending_seeds(0), "seeds"),
    (admission_cfg().with_max_pending_dials(0), "dials"),
  ] {
    let preflight = validate_runtime_config(&cfg, &TransformOptions::default(), 1400);
    match (name, &preflight) {
      ("seeds", Err(InitError::ZeroMaxPendingSeeds)) => {}
      ("dials", Err(InitError::ZeroMaxPendingDials)) => {}
      _ => panic!("{name}: preflight must name the zeroed knob, got {preflight:?}"),
    }

    let rejected: Result<Engine<SmolStr, u32>, _> = Engine::try_new_at(
      cfg,
      TransformOptions::default(),
      memberlist_proto::EndpointOptions::new(SmolStr::new("z"), node_addr(7946)),
      now,
      test_rng(),
      &NoGossip,
    );
    assert!(
      rejected.is_err(),
      "{name}: construction must reject the zeroed knob too"
    );
  }
}

/// A reliable exchange whose dial connects but whose handshake never completes is
/// reaped at its `stream_timeout` deadline: the machine emits
/// `StreamAction::Abort`, `abort_exchange` RST-resets the socket and returns the
/// slot straight to the pool, and the exchange terminalizes `Failed`. This is the
/// deadline-driven failed-terminal path (distinct from the connect-rejection one).
#[test]
fn reliable_exchange_deadline_aborts_and_reclaims_slot() {
  use memberlist_proto::event::{ExchangeKind, ExchangeStatus};

  let stream_timeout = Duration::from_secs(2);
  let (mut engine, now) = engine_with_stream_timeout(stream_timeout);
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);

  let to = node_addr(7003);
  engine
    .send_reliable(to, bytes::Bytes::from_static(b"never-acked"), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  // First pump: the Connect dials slot 0 (connect succeeds), but the mock never
  // marks it Established, so the handshake stalls and the request bytes stay parked.
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    StreamIo::is_open(&stream, 0),
    "the dial issued connect on slot 0"
  );

  // Advance past the exchange deadline and pump: `handle_timeout` reaps the stalled
  // bridge and emits Abort.
  let later = now + stream_timeout + Duration::from_secs(1);
  let outcome = pump_until_exchange(
    &mut engine,
    &mut stream,
    later,
    ExchangeKind::UserMessage,
    4,
  );
  assert_eq!(
    outcome,
    Some(ExchangeStatus::Failed),
    "a stalled-handshake exchange must fail at its stream_timeout deadline"
  );
  assert!(
    stream.aborted.contains(&0),
    "the stalled exchange's socket must be aborted (RST) on the deadline"
  );
  assert_eq!(
    engine.pool_free_count() + engine.listener_present() as usize,
    2,
    "the aborted exchange's slot must return to the pool, never leak"
  );
}

/// The reliable egress pump preserves per-connection byte order under
/// partial-write backpressure: a `send` that accepts fewer bytes than offered
/// leaves the unsent tail at the front of `out`, and a later tick with the ring
/// drained delivers exactly the remainder — never reordered, never duplicated.
#[test]
fn reliable_partial_write_parks_remainder_then_flushes_in_order() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);
  // The dial slot accepts at most 4 bytes per `send` this round, forcing the
  // push/pull request bytes to flush across multiple ticks.
  stream.sock_mut(0).send_cap = 4;

  let to = node_addr(7004);
  engine
    .send_reliable(to, bytes::Bytes::from_static(b"reliable-user-payload"), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  // Tick 1: Connect → dial slot 0. The mock leaves it un-established, so nothing
  // flushes yet (the engine skips a `!may_send` socket).
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    stream.sent.is_empty(),
    "a still-handshaking socket must not be written to"
  );

  // Establish the socket: now the egress pump flushes, but only 4 bytes per send.
  stream.sock_mut(0).established = true;
  engine.pump(now, &mut gossip, &mut stream);
  let after_first: usize = stream.sent.iter().map(|(_, b)| b.len()).sum();
  assert!(
    after_first > 0 && after_first < 64,
    "the capped send must accept only a partial prefix this tick, got {after_first} bytes"
  );

  // Lift the cap and keep pumping until the queue drains.
  stream.sock_mut(0).send_cap = usize::MAX;
  for _ in 0..8 {
    engine.pump(now, &mut gossip, &mut stream);
  }

  // Reassemble what reached slot 0 and confirm it is a contiguous, in-order byte
  // stream with no gaps or duplication from the partial-write parking.
  let mut reassembled: Vec<u8> = Vec::new();
  for (c, b) in &stream.sent {
    if *c == 0 {
      reassembled.extend_from_slice(b);
    }
  }
  assert!(
    reassembled
      .windows(b"reliable-user-payload".len())
      .any(|w| w == b"reliable-user-payload"),
    "the partial-write remainder must reassemble to the original payload in order"
  );
}

/// The reap pass reclaims a retired handle as soon as its teardown is
/// acknowledged (`teardown_done`), and escalates a Draining handle whose close has
/// exceeded `close_timeout` to Aborting (force-abort) — reclaiming it only once
/// that abort's teardown is itself acknowledged, so a vanished-mid-FIN peer can
/// never permanently shrink the pool yet the engine never blind-frees an
/// unacknowledged teardown. Both `retiring` handles are driven directly.
#[test]
fn reap_retiring_reclaims_finished_and_escalates_timed_out() {
  use crate::{RetirePhase, Retiring, SlotGen};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  let mut stream = ProgRel::new(&[0, 1]);
  // Slot 0: a clean graceful close that has reached Closed (`teardown_done`). Slot
  // 1: a peer that vanished mid-FIN — still open, its Draining deadline elapsed.
  stream.sock_mut(0).open = false;
  stream.sock_mut(1).open = true;
  let close_timeout = Duration::from_secs(10);
  // Park both Draining, slot 1 with an already-elapsed deadline.
  engine.plane_mut().retiring.insert(
    0,
    Retiring {
      generation: SlotGen::START,
      deadline: now + close_timeout,
      phase: RetirePhase::Draining,
    },
  );
  engine.plane_mut().retiring.insert(
    1,
    Retiring {
      generation: SlotGen::START,
      deadline: now,
      phase: RetirePhase::Draining,
    },
  );

  assert_eq!(engine.retiring_count(), 2, "two handles retiring");
  assert_eq!(
    engine.closing_count(),
    2,
    "both are Draining (graceful close)"
  );

  // Pump at `now`: slot 0's teardown is acknowledged (reclaimed on the
  // `teardown_done` path); slot 1 is past its Draining deadline, so it is escalated
  // — force-aborted and switched to Aborting — but NOT freed this pass (the engine
  // never blind-frees an unacknowledged teardown).
  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);

  assert!(
    stream.aborted.contains(&1),
    "the vanished-mid-FIN handle past its Draining deadline must be force-aborted"
  );
  assert!(
    !stream.aborted.contains(&0),
    "the cleanly-closed handle must be reclaimed without an abort"
  );
  assert_eq!(
    engine.closing_count(),
    0,
    "slot 0 reaped; slot 1 escalated out of Draining into Aborting"
  );
  assert_eq!(
    engine.retiring_count(),
    1,
    "the escalated handle stays retiring (Aborting) until its teardown is acknowledged"
  );

  // The escalation's `abort` dropped slot 1's `open`, so its teardown is now
  // acknowledged: a second pass reaps it and the ledger empties.
  let later = now + Duration::from_millis(1);
  engine.pump(later, &mut gossip, &mut stream);
  assert_eq!(
    engine.retiring_count(),
    0,
    "the escalated handle is reclaimed once its abort teardown is acknowledged"
  );
}

/// A settled inbound passive open is admitted by `check_listener`: the connected
/// listener slot becomes the accepted exchange, the inbound counter rises, and a
/// fresh listener is replenished from the spare pool — so the next inbound SYN
/// still has a slot. (The reject-and-reclaim half is already covered by
/// `rejected_inbound_accept_returns_its_slot_to_the_pool`.)
#[test]
fn check_listener_admits_inbound_and_replenishes_the_listener() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  // One listener (slot 1) plus one spare (slot 0) to replenish from.
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);
  stream
    .listen(1, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");
  // The listener's passive open settled with a known, in-policy remote.
  stream.sock_mut(1).accepted = Some(node_addr(7950));
  stream.sock_mut(1).established = true;

  let before = engine.accepted_inbound_count();
  engine.check_listener(now, &mut stream);

  assert_eq!(
    engine.accepted_inbound_count(),
    before + 1,
    "a settled inbound passive open must be admitted (the inbound counter rises)"
  );
  assert!(
    engine.listener_present(),
    "the listener must be replenished from the spare so the next inbound has a slot"
  );
  assert_eq!(
    engine.pool_free_count(),
    0,
    "the spare became the new listener, so the pool is now empty"
  );
}

// ── Two-engine in-memory reliable link ───────────────────────────────────────
//
// A faithful loopback so a real push/pull (or reliable user-message) runs
// end-to-end through BOTH engines' reliable planes, exercising the terminal
// lifecycle the single-engine `ProgRel` cannot synthesize without a peer:
// `Connect` → handshake promote → request flush → deferred-FIN (`Shutdown`) →
// inbound reply pump → peer EOF → graceful `Close` (`teardown` /
// `flush_closing`) → reap. One end's `connect` registers a pending SYN on the
// shared fabric; the destination's listener completes the passive open when its
// `accepted_peer` is polled, after which bytes ferry both ways over the matched
// pipe. Partial-write backpressure is intentionally NOT modelled here (the link
// accepts whole `send`s) — that path is covered by `ProgRel`; this harness
// models the connection lifecycle and byte transport.

/// One side of an established pipe owns a `PipeEnd`; the two ends share a `Pipe`
/// via the fabric. `tx` carries bytes this end wrote toward the peer (the peer
/// reads them from the SAME buffer as ITS `rx`), so the two ends cross-reference
/// one `Pipe`'s two buffers.
#[derive(Default)]
struct Pipe {
  /// Bytes written by the dialer end, read by the acceptor end.
  d2a: VecDeque<u8>,
  /// Bytes written by the acceptor end, read by the dialer end.
  a2d: VecDeque<u8>,
  /// The dialer end emitted its FIN (`close`).
  d_fin: bool,
  /// The acceptor end emitted its FIN (`close`).
  a_fin: bool,
  /// Either end issued an `abort` (RST) — both ends see the pipe closed.
  reset: bool,
  /// The passive open settled: the acceptor's `accepted_peer` matched this pipe's
  /// SYN, so both ends are now send-capable.
  established: bool,
  /// Bytes the dialer end handed to `send` that the peer has not yet acked —
  /// reported by `send_queue` for the dialer. Bytes are DELIVERED to the peer's rx
  /// immediately (so the FSM completes), but this lingers until a test acks it, so
  /// a graceful `Close` over this end parks in `Closing` until it drains — the
  /// drain-before-close path. Normally zero (the default link acks instantly).
  d_unacked: usize,
  /// As `d_unacked`, for the acceptor end.
  a_unacked: usize,
}

/// A SYN parked on the fabric by a dialer's `connect`, awaiting the destination's
/// listener to complete the passive open.
struct PendingSyn {
  /// The destination advertised address the SYN is dialing.
  dest: SocketAddr,
  /// The dialer's source address (what the acceptor's `accepted_peer` returns).
  src: SocketAddr,
  /// The shared pipe id both ends bind to once the open settles.
  pipe: u64,
}

/// Shared fabric state: every established pipe plus the not-yet-accepted SYNs.
#[derive(Default)]
struct FabricInner {
  pipes: BTreeMap<u64, Pipe>,
  pending: Vec<PendingSyn>,
  next_pipe: u64,
}

impl FabricInner {
  fn fresh() -> Fabric {
    Rc::new(RefCell::new(FabricInner::default()))
  }
}

type Fabric = Rc<RefCell<FabricInner>>;

/// Which end of a pipe a slot is bound to, so `send`/`recv`/`close` route to the
/// correct buffer.
#[derive(Clone, Copy, PartialEq)]
enum End {
  Dialer,
  Acceptor,
}

/// What one of an engine's reliable slots is currently doing.
#[derive(Clone)]
enum SlotRole {
  /// Free / idle — not listening, dialing, or bound.
  Idle,
  /// Listening for a passive open on this engine's advertised port.
  Listening,
  /// Bound to a pipe as the named end.
  Bound(u64, End),
}

/// One engine's reliable I/O over the shared fabric: its own slot pool and
/// per-slot role, plus this engine's advertised address (the SYN destination its
/// listener answers). `role` is a `RefCell` so the `&self` `accepted_peer` can
/// re-bind a Listening slot to the Acceptor end the instant it completes a
/// passive open — the same handle the engine then keeps for the exchange.
struct LinkRel {
  fabric: Fabric,
  me: SocketAddr,
  free: Vec<u32>,
  role: RefCell<BTreeMap<u32, SlotRole>>,
  /// When set, a `send` on this end ALSO accrues unacked tx (`send_queue` > 0),
  /// modelling a peer that is slow to acknowledge. A graceful `Close` over such a
  /// connection parks in `Closing` until the tx is acked, exercising the
  /// drain-before-close path. Off by default (the link acks instantly).
  hold_tx: bool,
}

impl LinkRel {
  fn new(fabric: Fabric, me: SocketAddr, handles: &[u32]) -> Self {
    let mut role = BTreeMap::new();
    for &h in handles {
      role.insert(h, SlotRole::Idle);
    }
    Self {
      fabric,
      me,
      free: handles.to_vec(),
      role: RefCell::new(role),
      hold_tx: false,
    }
  }

  fn role_of(&self, c: u32) -> Option<SlotRole> {
    self.role.borrow().get(&c).cloned()
  }
}

/// Bytes waiting for the end `end` to read (the OTHER end's tx buffer).
fn pipe_inbound(p: &Pipe, end: End) -> &VecDeque<u8> {
  match end {
    End::Dialer => &p.a2d,
    End::Acceptor => &p.d2a,
  }
}

/// Whether THIS end emitted its FIN.
fn pipe_end_fin(p: &Pipe, end: End) -> bool {
  match end {
    End::Dialer => p.d_fin,
    End::Acceptor => p.a_fin,
  }
}

/// Whether the PEER end emitted its FIN.
fn pipe_peer_fin(p: &Pipe, end: End) -> bool {
  match end {
    End::Dialer => p.a_fin,
    End::Acceptor => p.d_fin,
  }
}

impl StreamIo for LinkRel {
  type Conn = u32;

  fn take_free(&mut self) -> Option<u32> {
    self.free.pop()
  }

  fn give(&mut self, c: u32) {
    self.role.borrow_mut().insert(c, SlotRole::Idle);
    self.free.push(c);
  }

  fn free_count(&self) -> usize {
    self.free.len()
  }

  fn teardown_done(&self, c: u32, _g: crate::SlotGen) -> bool {
    // Synchronous fabric mock: a retired occupancy is reusable once its pipe is no
    // longer open (a RST, or both FINs exchanged) — exactly the pre-ledger reap
    // gate. A graceful close whose peer FIN has not arrived stays `is_open`, so it
    // waits in `retiring` (Draining) until the peer FINs, then frees.
    !self.is_open(c)
  }

  fn listen(&mut self, c: u32, _port: u16, _g: crate::SlotGen) -> Result<(), crate::StreamIoError> {
    self.role.borrow_mut().insert(c, SlotRole::Listening);
    Ok(())
  }

  fn accepted_peer(&self, c: u32) -> Option<SocketAddr> {
    // Only a Listening slot completes a passive open; it matches the first parked
    // SYN addressed to this engine's advertised address.
    if !matches!(self.role_of(c), Some(SlotRole::Listening)) {
      return None;
    }
    let mut fab = self.fabric.borrow_mut();
    let pos = fab.pending.iter().position(|s| s.dest == self.me)?;
    let syn = fab.pending.remove(pos);
    fab.pipes.entry(syn.pipe).or_default().established = true;
    drop(fab);
    // The passive open settled: bind THIS handle as the acceptor end. The engine
    // keeps using `c` for the exchange and re-`listen`s a fresh slot for the next
    // inbound.
    self
      .role
      .borrow_mut()
      .insert(c, SlotRole::Bound(syn.pipe, End::Acceptor));
    Some(syn.src)
  }

  fn connect(
    &mut self,
    c: u32,
    remote: SocketAddr,
    _local_port: u16,
    _g: crate::SlotGen,
  ) -> Result<(), crate::StreamIoError> {
    let mut fab = self.fabric.borrow_mut();
    let pipe = fab.next_pipe;
    fab.next_pipe += 1;
    fab.pipes.insert(pipe, Pipe::default());
    fab.pending.push(PendingSyn {
      dest: remote,
      src: self.me,
      pipe,
    });
    drop(fab);
    self
      .role
      .borrow_mut()
      .insert(c, SlotRole::Bound(pipe, End::Dialer));
    Ok(())
  }

  fn may_send(&self, c: u32) -> bool {
    match self.role_of(c) {
      Some(SlotRole::Bound(pipe, end)) => {
        let fab = self.fabric.borrow();
        match fab.pipes.get(&pipe) {
          Some(p) => p.established && !p.reset && !pipe_end_fin(p, end),
          None => false,
        }
      }
      _ => false,
    }
  }

  fn may_recv(&self, c: u32) -> bool {
    match self.role_of(c) {
      Some(SlotRole::Bound(pipe, end)) => {
        let fab = self.fabric.borrow();
        fab
          .pipes
          .get(&pipe)
          .map(|p| !pipe_inbound(p, end).is_empty())
          .unwrap_or(false)
      }
      _ => false,
    }
  }

  fn is_open(&self, c: u32) -> bool {
    match self.role_of(c) {
      Some(SlotRole::Bound(pipe, _)) => {
        let fab = self.fabric.borrow();
        match fab.pipes.get(&pipe) {
          // Open until a RST, or until BOTH FINs have been exchanged (the clean
          // Closed/TimeWait state the engine treats as reclaimable).
          Some(p) => !p.reset && !(p.d_fin && p.a_fin),
          None => false,
        }
      }
      Some(SlotRole::Listening) => true,
      _ => false,
    }
  }

  fn is_established(&self, c: u32) -> bool {
    self.may_send(c)
  }

  fn recv(&mut self, c: u32, buf: &mut [u8]) -> Option<usize> {
    let (pipe, end) = match self.role_of(c) {
      Some(SlotRole::Bound(pipe, end)) => (pipe, end),
      _ => return None,
    };
    let mut fab = self.fabric.borrow_mut();
    let p = fab.pipes.get_mut(&pipe)?;
    let q = match end {
      End::Dialer => &mut p.a2d,
      End::Acceptor => &mut p.d2a,
    };
    if q.is_empty() {
      return None;
    }
    let n = q.len().min(buf.len());
    for (i, b) in q.drain(..n).enumerate() {
      buf[i] = b;
    }
    Some(n)
  }

  fn recv_finished(&self, c: u32) -> bool {
    match self.role_of(c) {
      Some(SlotRole::Bound(pipe, end)) => {
        let fab = self.fabric.borrow();
        match fab.pipes.get(&pipe) {
          // The peer FIN'd (a graceful close, NOT a reset) and our inbound buffer is
          // drained — the one-shot EOF condition.
          Some(p) => !p.reset && pipe_peer_fin(p, end) && pipe_inbound(p, end).is_empty(),
          None => false,
        }
      }
      _ => false,
    }
  }

  fn send(&mut self, c: u32, bytes: &[u8]) -> usize {
    let (pipe, end) = match self.role_of(c) {
      Some(SlotRole::Bound(pipe, end)) => (pipe, end),
      _ => return 0,
    };
    let hold = self.hold_tx;
    let mut fab = self.fabric.borrow_mut();
    let Some(p) = fab.pipes.get_mut(&pipe) else {
      return 0;
    };
    if p.reset || !p.established {
      return 0;
    }
    // Deliver to the peer's rx (the FSM sees the bytes), and — when holding —
    // accrue unacked tx so a later graceful close parks in `Closing`.
    match end {
      End::Dialer => {
        p.d2a.extend(bytes.iter().copied());
        if hold {
          p.d_unacked += bytes.len();
        }
      }
      End::Acceptor => {
        p.a2d.extend(bytes.iter().copied());
        if hold {
          p.a_unacked += bytes.len();
        }
      }
    }
    bytes.len()
  }

  fn send_queue(&self, c: u32) -> usize {
    match self.role_of(c) {
      Some(SlotRole::Bound(pipe, end)) => {
        let fab = self.fabric.borrow();
        match fab.pipes.get(&pipe) {
          Some(p) => match end {
            End::Dialer => p.d_unacked,
            End::Acceptor => p.a_unacked,
          },
          None => 0,
        }
      }
      _ => 0,
    }
  }

  fn close(&mut self, c: u32, _g: crate::SlotGen) {
    if let Some(SlotRole::Bound(pipe, end)) = self.role_of(c) {
      let mut fab = self.fabric.borrow_mut();
      if let Some(p) = fab.pipes.get_mut(&pipe) {
        match end {
          End::Dialer => p.d_fin = true,
          End::Acceptor => p.a_fin = true,
        }
      }
    }
  }

  fn abort(&mut self, c: u32, _g: crate::SlotGen) {
    if let Some(SlotRole::Bound(pipe, _)) = self.role_of(c) {
      let mut fab = self.fabric.borrow_mut();
      if let Some(p) = fab.pipes.get_mut(&pipe) {
        p.reset = true;
      }
    }
  }
}

/// A paired gossip relay: datagrams `send`-emitted toward a peer's address land
/// in that peer's inbound queue (and vice versa), so the two engines also
/// exchange SWIM gossip. Each engine holds one end keyed by its own address.
#[derive(Clone)]
struct GossipWire {
  /// `(dest, bytes)` queued by SENDS on this end, drained into the matching
  /// peer's `inbound` by the driver between pumps.
  outbound: Rc<RefCell<Vec<(SocketAddr, Vec<u8>)>>>,
  /// `(src, bytes)` waiting for THIS engine to `recv`.
  inbound: Rc<RefCell<VecDeque<(SocketAddr, Vec<u8>)>>>,
}

impl GossipIo for GossipWire {
  fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    let (src, bytes) = self.inbound.borrow_mut().pop_front()?;
    let n = bytes.len().min(buf.len());
    buf[..n].copy_from_slice(&bytes[..n]);
    Some((src, n))
  }

  fn send(&mut self, bytes: &[u8], dest: SocketAddr) {
    self.outbound.borrow_mut().push((dest, bytes.to_vec()));
  }

  fn recv_capacity(&self) -> usize {
    FAKE_RECV_CAPACITY
  }
}

/// A linked two-engine fixture sharing one reliable fabric and a cross-wired
/// gossip relay. `step` pumps both engines once and then ferries each side's
/// emitted gossip into the other side's inbound queue, modelling a network that
/// delivers a datagram to its addressed peer.
struct LinkPair {
  a: Engine<SmolStr, u32>,
  b: Engine<SmolStr, u32>,
  a_rel: LinkRel,
  b_rel: LinkRel,
  a_gossip: GossipWire,
  b_gossip: GossipWire,
  a_addr: SocketAddr,
  b_addr: SocketAddr,
}

impl LinkPair {
  /// Two running engines `a` (port 7946) and `b` (port 7947) on a shared fabric,
  /// each with `pool` dial slots plus a listener. A short `stream_timeout` keeps a
  /// wedged exchange from hanging the test.
  fn new(pool_handles_a: &[u32], pool_handles_b: &[u32]) -> Self {
    let now = Instant::from_origin(Duration::from_secs(86_400));
    let a_addr = node_addr(7946);
    let b_addr = node_addr(7947);

    let mk = |id: &str, port: u16, addr: SocketAddr| -> Engine<SmolStr, u32> {
      let cfg = Options::new()
        .with_port(port)
        .with_close_timeout(Duration::from_secs(10));
      let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new(id), addr)
        .with_stream_timeout(Duration::from_secs(5));
      let mut e: Engine<SmolStr, u32> = Engine::try_new_at(
        cfg,
        TransformOptions::default(),
        ep_cfg,
        now,
        test_rng(),
        &NoGossip,
      )
      .expect("construct");
      e.start(now);
      e
    };

    let mut a = mk("a", 7946, a_addr);
    let mut b = mk("b", 7947, b_addr);

    let fabric = FabricInner::fresh();
    // The listener handle is the last in each pool; the rest are dial slots.
    let (a_listener, a_dials) = pool_handles_a.split_last().expect("at least one handle");
    let (b_listener, b_dials) = pool_handles_b.split_last().expect("at least one handle");
    for &h in a_dials {
      a.plane_mut().pool.push(h);
    }
    for &h in b_dials {
      b.plane_mut().pool.push(h);
    }
    a.set_listener(*a_listener);
    b.set_listener(*b_listener);

    let mut a_rel = LinkRel::new(fabric.clone(), a_addr, pool_handles_a);
    let mut b_rel = LinkRel::new(fabric, b_addr, pool_handles_b);
    // The engine already owns each pool/listener; tell the mocks which handles are
    // the listeners so `accepted_peer` answers on them, and remove them from the
    // mock free-lists so a re-listen does not double-hand a listener.
    a_rel.free.retain(|h| h != a_listener);
    b_rel.free.retain(|h| h != b_listener);
    a_rel
      .listen(*a_listener, 7946, crate::SlotGen::START)
      .expect("listen");
    b_rel
      .listen(*b_listener, 7947, crate::SlotGen::START)
      .expect("listen");

    let a2b: Rc<RefCell<Vec<(SocketAddr, Vec<u8>)>>> = Rc::new(RefCell::new(Vec::new()));
    let b2a: Rc<RefCell<Vec<(SocketAddr, Vec<u8>)>>> = Rc::new(RefCell::new(Vec::new()));
    let a_in: Rc<RefCell<VecDeque<(SocketAddr, Vec<u8>)>>> = Rc::new(RefCell::new(VecDeque::new()));
    let b_in: Rc<RefCell<VecDeque<(SocketAddr, Vec<u8>)>>> = Rc::new(RefCell::new(VecDeque::new()));
    let a_gossip = GossipWire {
      outbound: a2b.clone(),
      inbound: a_in.clone(),
    };
    let b_gossip = GossipWire {
      outbound: b2a.clone(),
      inbound: b_in.clone(),
    };

    LinkPair {
      a,
      b,
      a_rel,
      b_rel,
      a_gossip,
      b_gossip,
      a_addr,
      b_addr,
    }
  }

  /// Make B (the acceptor) accrue unacked tx on every `send`, so when B's bridge
  /// gracefully closes with its reply still unacknowledged the connection parks in
  /// `Closing` rather than FIN-ing immediately — the drain-before-close path.
  fn hold_b_tx(&mut self) {
    self.b_rel.hold_tx = true;
  }

  /// Acknowledge up to `amount` of B's accrued unacked tx across all pipes,
  /// modelling the peer draining B's reply. Used to step the `Closing` drain
  /// through its Progress (partial) and Fin (fully-drained) branches.
  fn ack_b(&mut self, amount: usize) {
    let mut fab = self.b_rel.fabric.borrow_mut();
    let mut left = amount;
    for p in fab.pipes.values_mut() {
      let take = p.a_unacked.min(left);
      p.a_unacked -= take;
      left -= take;
      if left == 0 {
        break;
      }
    }
  }

  /// Pump both engines once at `now`, then ferry each side's emitted gossip into
  /// the peer's inbound queue (a datagram is delivered to its addressed peer).
  fn step(&mut self, now: Instant) {
    self.a.pump(now, &mut self.a_gossip, &mut self.a_rel);
    self.b.pump(now, &mut self.b_gossip, &mut self.b_rel);
    // a's outbound (addressed to b) → b's inbound; b's outbound → a's inbound.
    let a_out: Vec<_> = self.a_gossip.outbound.borrow_mut().drain(..).collect();
    for (dest, bytes) in a_out {
      if dest == self.b_addr {
        self
          .b_gossip
          .inbound
          .borrow_mut()
          .push_back((self.a_addr, bytes));
      }
    }
    let b_out: Vec<_> = self.b_gossip.outbound.borrow_mut().drain(..).collect();
    for (dest, bytes) in b_out {
      if dest == self.a_addr {
        self
          .a_gossip
          .inbound
          .borrow_mut()
          .push_back((self.b_addr, bytes));
      }
    }
  }
}

/// A full join push/pull over the in-memory reliable link: `a.join([b])` drives
/// `Connect` → dial → handshake promote → request flush → deferred-FIN
/// (`Shutdown`) → inbound reply pump → peer EOF → graceful `Close`, and BOTH
/// nodes learn each other. This is the engine's whole reliable lifecycle
/// end-to-end (the single biggest uncovered cluster), asserted by mutual
/// membership convergence AND the originating exchange terminalizing `Succeeded`.
#[test]
fn two_engine_join_push_pull_converges_and_succeeds() {
  use memberlist_proto::event::{ExchangeKind, ExchangeStatus};

  // Each pool: one dial slot plus one listener (the last handle).
  let mut link = LinkPair::new(&[10, 11], &[20, 21]);
  let now = Instant::from_origin(Duration::from_secs(86_400));

  link.a.join(&[link.b_addr]).expect("join queues the seed");

  // Drive both engines until A's push/pull exchange completes. Each push/pull
  // settles in a handful of zero-latency ticks; the bound only fails loudly.
  let mut a_outcome = None;
  for _ in 0..40 {
    link.step(now);
    while let Some(ev) = link.a.poll_event() {
      if let Event::ExchangeCompleted(ec) = ev {
        if ec.kind() == ExchangeKind::PushPull {
          a_outcome = Some(ec.outcome());
        }
      }
    }
    // Drain B's events too so its queue cannot stall the bridge.
    while link.b.poll_event().is_some() {}
    if a_outcome.is_some() && link.a.is_joined() && link.b.is_joined() {
      break;
    }
  }

  assert_eq!(
    a_outcome,
    Some(ExchangeStatus::Succeeded),
    "the initiating push/pull must complete Succeeded over the reliable link"
  );
  assert!(
    link.a.is_alive(&SmolStr::new("b")),
    "A must have learned B Alive through the push/pull state exchange"
  );
  assert!(
    link.b.is_alive(&SmolStr::new("a")),
    "B must have learned A Alive from the inbound push/pull request it processed"
  );

  // The reliable plane fully unwound: no lingering exchanges, and every slot is
  // back (as the pool or the re-armed listener) on both nodes.
  assert_eq!(
    link.a.outbound_correlation_len(),
    0,
    "A's outbound-StreamId correlation map must be pruned once the exchange completed"
  );
  assert_eq!(
    link.a.pool_free_count() + link.a.listener_present() as usize,
    2,
    "every reliable slot on A must return to the pool or the listener"
  );
  assert_eq!(
    link.b.pool_free_count() + link.b.listener_present() as usize,
    2,
    "every reliable slot on B must return to the pool or the listener"
  );
}

/// A reliable user-message delivered over the link: `a.send_reliable(b, payload)`
/// drives the same `Connect`→flush→half-close→close lifecycle, the payload
/// arrives at B as a RELIABLE `Event::UserPacket` (so B's inbound reliable pump
/// drained the bytes and delivered the peer EOF), and A's exchange terminalizes
/// `Succeeded`.
#[test]
fn two_engine_reliable_user_message_delivers_and_succeeds() {
  use memberlist_proto::event::{ExchangeKind, ExchangeStatus};

  let mut link = LinkPair::new(&[10, 11], &[20, 21]);
  let now = Instant::from_origin(Duration::from_secs(86_400));

  let payload = bytes::Bytes::from_static(b"a-reliable-hello-to-b");
  link
    .a
    .send_reliable(link.b_addr, payload.clone(), now)
    .expect("send_reliable queues the exchange");

  let mut a_outcome = None;
  let mut b_received: Option<bytes::Bytes> = None;
  for _ in 0..40 {
    link.step(now);
    while let Some(ev) = link.a.poll_event() {
      if let Event::ExchangeCompleted(ec) = ev {
        if ec.kind() == ExchangeKind::UserMessage {
          a_outcome = Some(ec.outcome());
        }
      }
    }
    while let Some(ev) = link.b.poll_event() {
      if let Event::UserPacket(up) = ev {
        let (_src, bytes, _rel) = up.into_parts();
        b_received = Some(bytes);
      }
    }
    if a_outcome.is_some() && b_received.is_some() {
      break;
    }
  }

  assert_eq!(
    a_outcome,
    Some(ExchangeStatus::Succeeded),
    "a reliable user-message must complete Succeeded once the peer reads it + EOFs"
  );
  assert_eq!(
    b_received.as_deref(),
    Some(payload.as_ref()),
    "B must receive the exact reliable user-message payload over its inbound pump"
  );
  assert_eq!(
    link.a.outbound_correlation_len(),
    0,
    "A's correlation map must be pruned after the user-message completes"
  );
}

/// After a join, `a.leave()` drains gracefully and emits `Event::LeftCluster`,
/// and A's reliable plane unwinds — every slot returns to the pool or the
/// listener, no exchange lingers. (`leave` also clears any queued seeds, covered
/// by the post-leave control-setter tests; this drives the graceful-drain pump.)
#[test]
fn two_engine_leave_after_join_drains_and_reclaims_slots() {
  let mut link = LinkPair::new(&[10, 11], &[20, 21]);
  let now = Instant::from_origin(Duration::from_secs(86_400));

  link.a.join(&[link.b_addr]).expect("join queues the seed");
  for _ in 0..40 {
    link.step(now);
    while link.a.poll_event().is_some() {}
    while link.b.poll_event().is_some() {}
    if link.a.is_joined() && link.b.is_joined() {
      break;
    }
  }
  assert!(link.a.is_joined(), "precondition: A joined B");

  // Leave and drive the graceful-leave drain to its LeftCluster terminal.
  let leave_at = now + Duration::from_secs(1);
  link.a.leave(leave_at).expect("leave from a running node");
  let mut left = false;
  let mut t = leave_at;
  for _ in 0..60 {
    link.step(t);
    while let Some(ev) = link.a.poll_event() {
      if matches!(ev, Event::LeftCluster) {
        left = true;
      }
    }
    while link.b.poll_event().is_some() {}
    if left {
      break;
    }
    t += Duration::from_millis(200);
  }

  assert!(left, "A must emit LeftCluster after a graceful leave");
  // Post-leave the control setters reject and no new exchange can be dialed; the
  // reliable plane must hold no exchange that would pin a slot.
  assert_eq!(
    link.a.pending_dial_count(),
    0,
    "a left node initiates no new dial, so nothing parks in PendingDial"
  );
  // Draining a couple more ticks lets any in-flight close finish reclaiming.
  for _ in 0..6 {
    link.step(t);
    while link.a.poll_event().is_some() {}
    while link.b.poll_event().is_some() {}
    t += Duration::from_millis(200);
  }
  assert_eq!(
    link.a.pool_free_count() + link.a.listener_present() as usize,
    2,
    "every reliable slot on the left node must return to the pool or the listener"
  );
  // The join exchange itself terminalized (Succeeded) before the leave; assert no
  // further reliable correlation lingers.
  assert_eq!(
    link.a.outbound_correlation_len(),
    0,
    "no outbound reliable exchange may linger after leave"
  );
}

/// Drive both engines until they mutually converge (or the budget elapses),
/// returning the joined `LinkPair`. A shared fixture for the membership-query
/// accessor tests, which need a real second member to query.
fn linked_and_converged() -> LinkPair {
  let mut link = LinkPair::new(&[10, 11], &[20, 21]);
  let now = Instant::from_origin(Duration::from_secs(86_400));
  link.a.join(&[link.b_addr]).expect("join queues the seed");
  for _ in 0..40 {
    link.step(now);
    while link.a.poll_event().is_some() {}
    while link.b.poll_event().is_some() {}
    if link.a.is_joined() && link.b.is_joined() {
      break;
    }
  }
  assert!(
    link.a.is_alive(&SmolStr::new("b")),
    "fixture precondition: A learned B Alive"
  );
  link
}

/// The membership-query accessors all agree on the converged two-member view and
/// stamp the live FSM liveness: `members`/`online_members`/`num_online_members`/
/// `members_by`/`num_members_by`/`members_map_by`/`by_id` are mutually
/// consistent, and the local-node reads (`local_id`/`advertise_address`/
/// `local_state`/`health_score`) report A's own identity.
#[test]
fn membership_query_accessors_agree_on_the_converged_view() {
  use memberlist_proto::typed::State;

  let link = linked_and_converged();
  let a = &link.a;

  assert_eq!(a.num_members(), 2, "A knows itself and B");
  assert_eq!(
    a.members().len(),
    2,
    "members() lists every known member (self + B)"
  );

  // online_members agrees with is_alive and with the count helper; each entry is
  // stamped Alive.
  let online = a.online_members();
  assert_eq!(
    online.len(),
    a.num_online_members(),
    "online list/count agree"
  );
  assert_eq!(online.len(), 2, "both nodes are Alive after convergence");
  assert!(
    online.iter().all(|ns| ns.state() == State::Alive),
    "every online member is stamped with the live Alive FSM state"
  );

  // by_id round-trips B and stamps it Alive; an unknown id is None.
  let b = a.by_id(&SmolStr::new("b")).expect("B is known");
  assert_eq!(
    b.state(),
    State::Alive,
    "by_id stamps the live FSM liveness"
  );
  assert!(
    a.by_id(&SmolStr::new("nobody")).is_none(),
    "by_id is None for an unknown id"
  );
  assert!(!a.is_dead(&SmolStr::new("b")), "B is Alive, not Dead");

  // members_by / num_members_by / members_map_by are consistent filters over the
  // same stamped view.
  let only_b = a.members_by(|ns| ns.id_ref() == &SmolStr::new("b"));
  assert_eq!(only_b.len(), 1, "members_by selects exactly B");
  assert_eq!(
    a.num_members_by(|ns| ns.id_ref() == &SmolStr::new("b")),
    1,
    "num_members_by counts exactly B"
  );
  let ids: std::vec::Vec<SmolStr> = a.members_map_by(|ns| Some(ns.id_ref().clone()));
  assert_eq!(ids.len(), 2, "members_map_by maps every member");
  assert!(
    ids.contains(&SmolStr::new("a")) && ids.contains(&SmolStr::new("b")),
    "members_map_by yields both ids"
  );

  // Local-node reads.
  assert_eq!(a.local_id(), SmolStr::new("a"), "local_id is A's own id");
  assert_eq!(
    a.advertise_address(),
    link.a_addr,
    "advertise_address is A's bound address"
  );
  assert_eq!(
    a.local_state().id_ref(),
    &SmolStr::new("a"),
    "local_state is A's own NodeState"
  );
  assert_eq!(
    a.local_state().state(),
    State::Alive,
    "the local node always reads Alive from its own perspective"
  );
  // health_score is a valid read (0 for a freshly-converged healthy node).
  assert_eq!(
    a.health_score(),
    0,
    "a healthy node has a zero health score"
  );
}

/// The reliable-plane diagnostic counters read consistently after convergence:
/// the exchanges have all completed, so `half_closed_count`, `pending_dial_count`,
/// and `closing_count` are all zero, and `accepted_inbound_count` rose on the
/// node that accepted the inbound push/pull (B).
#[test]
fn reliable_diagnostics_settle_to_zero_after_convergence() {
  let link = linked_and_converged();

  assert_eq!(
    link.a.half_closed_count(),
    0,
    "no half-closed exchange lingers after convergence"
  );
  assert_eq!(
    link.a.pending_dial_count(),
    0,
    "no deferred dial lingers after convergence"
  );
  assert!(
    link.b.accepted_inbound_count() >= 1,
    "B accepted at least one inbound reliable connection (the join push/pull)"
  );
}

/// `ping` enqueues a direct probe and the engine pumps it without panicking; the
/// returned `PingId` is the caller's correlation token. (Completion/timeout is a
/// transport-level event covered by the driver suites; here we exercise the
/// engine forwarder and the gossip-egress encode of the ping.)
#[test]
fn ping_enqueues_and_engine_pumps() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(now);

  let target = Node::new(SmolStr::new("peer"), node_addr(7100));
  engine
    .ping(target, now)
    .expect("ping enqueues while running");

  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(2);
  // The ping rides the gossip egress; the pump must drive it without panicking.
  let _deadline = engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(engine.num_members(), 1, "ping does not add a member");
}

/// `send_many` compounds a directed unreliable batch to one in-policy
/// destination: the batch is enqueued and the gossip drain emits at least one
/// datagram (the compound frame). Exercises the `send_many` forwarder and its
/// non-blocked path.
#[cfg(feature = "crc32")]
#[test]
fn send_many_enqueues_a_directed_batch() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(now);

  let to = node_addr(7101);
  let payloads = [
    bytes::Bytes::from_static(b"one"),
    bytes::Bytes::from_static(b"two"),
  ];
  engine
    .send_many(to, &payloads)
    .expect("send_many enqueues the batch");

  let mut gossip = CaptureGossip::new();
  let mut stream = NoStream::with_pool(2);
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    !gossip.sent.is_empty(),
    "the directed unreliable batch must emit at least one datagram"
  );
}

/// `queue_user_broadcast` accepts an in-budget payload while running and the
/// engine pumps it. After `leave` the same call rejects with `NotRunning` (the
/// gossip scheduler is stopped) — the running-vs-left split for the broadcast
/// queue.
#[test]
fn queue_user_broadcast_accepts_while_running_then_rejects_after_leave() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(now);

  engine
    .queue_user_broadcast(bytes::Bytes::from_static(b"broadcast"))
    .expect("an in-budget broadcast is accepted while running");

  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(2);
  let _deadline = engine.pump(now, &mut gossip, &mut stream);

  engine.leave(now).expect("leave from a running node");
  assert!(
    matches!(
      engine.queue_user_broadcast(bytes::Bytes::from_static(b"late")),
      Err(memberlist_proto::Error::NotRunning)
    ),
    "queue_user_broadcast must reject after leave"
  );
}

/// The data-state setters succeed while running: `update_node_metadata`,
/// `set_local_state`, and `set_ack_payload` each apply without error and the
/// engine keeps pumping. (Their post-leave rejection is covered by
/// `control_setters_reject_after_leave`; this is the accepted-while-running half.)
#[test]
fn data_state_setters_apply_while_running() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(now);

  let meta =
    memberlist_proto::typed::Meta::try_from(bytes::Bytes::from_static(b"meta-v2")).expect("meta");
  engine
    .update_node_metadata(meta)
    .expect("metadata update applies while running");
  engine
    .set_local_state(bytes::Bytes::from_static(b"app-state"))
    .expect("local state applies while running");
  engine
    .set_ack_payload(bytes::Bytes::from_static(b"ack-extra"))
    .expect("ack payload applies while running");

  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(2);
  let _deadline = engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1,
    "the data-state setters keep the engine operational"
  );
}

/// `port()` reports the configured bind port, and `new_at` (the panicking
/// convenience constructor) builds successfully on a valid config — the non-panic
/// path of the `try_new_at` wrapper.
#[test]
fn new_at_builds_and_port_reads_back() {
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("p"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let engine: Engine<SmolStr, u32> = Engine::new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  );
  assert_eq!(
    engine.port(),
    7946,
    "port() reports the configured bind port"
  );
  assert_eq!(engine.num_members(), 1, "a fresh engine has only itself");
}

/// `try_new_at` rejects each advertise-independent and advertise-dependent
/// misconfiguration with its specific typed `InitError`, never a panic — the
/// construction-time validation contract.
#[test]
fn try_new_at_rejects_each_misconfiguration() {
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let ok_cfg = || {
    Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10))
  };

  // Zero port.
  let zero_port = Options::new()
    .with_port(0)
    .with_close_timeout(Duration::from_secs(10));
  let ep = memberlist_proto::EndpointOptions::new(SmolStr::new("z"), node_addr(7946));
  assert!(
    matches!(
      Engine::<SmolStr, u32>::try_new_at(
        zero_port,
        TransformOptions::default(),
        ep,
        now,
        test_rng(),
        &NoGossip
      ),
      Err(InitError::ZeroPort)
    ),
    "a zero port must reject with ZeroPort"
  );

  // Zero close timeout.
  let zero_close = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::ZERO);
  let ep = memberlist_proto::EndpointOptions::new(SmolStr::new("z"), node_addr(7946));
  assert!(
    matches!(
      Engine::<SmolStr, u32>::try_new_at(
        zero_close,
        TransformOptions::default(),
        ep,
        now,
        test_rng(),
        &NoGossip
      ),
      Err(InitError::ZeroCloseTimeout)
    ),
    "a zero close timeout must reject with ZeroCloseTimeout"
  );

  // Over-ceiling gossip MTU.
  let ep = memberlist_proto::EndpointOptions::new(SmolStr::new("z"), node_addr(7946))
    .with_gossip_mtu(usize::MAX / 2);
  assert!(
    matches!(
      Engine::<SmolStr, u32>::try_new_at(
        ok_cfg(),
        TransformOptions::default(),
        ep,
        now,
        test_rng(),
        &NoGossip
      ),
      Err(InitError::GossipMtuTooLarge(_))
    ),
    "an over-ceiling gossip MTU must reject with GossipMtuTooLarge"
  );

  // Non-routable advertise address (port 0 on the advertised socket).
  let ep = memberlist_proto::EndpointOptions::new(
    SmolStr::new("z"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 0),
  );
  assert!(
    matches!(
      Engine::<SmolStr, u32>::try_new_at(
        ok_cfg(),
        TransformOptions::default(),
        ep,
        now,
        test_rng(),
        &NoGossip
      ),
      Err(InitError::NonRoutableAdvertiseAddr(_))
    ),
    "a non-routable advertise address must reject with NonRoutableAdvertiseAddr"
  );

  // Advertised port differs from the bound port.
  let ep = memberlist_proto::EndpointOptions::new(SmolStr::new("z"), node_addr(7000));
  assert!(
    matches!(
      Engine::<SmolStr, u32>::try_new_at(
        ok_cfg(),
        TransformOptions::default(),
        ep,
        now,
        test_rng(),
        &NoGossip
      ),
      Err(InitError::AdvertisePortMismatch)
    ),
    "an advertised-port mismatch must reject with AdvertisePortMismatch"
  );
}

/// A label that asks to skip the inbound label check is wired through to the
/// reliable-plane label options at construction (the `skip_inbound_label_check`
/// branch in `try_new_at`); the engine constructs and pumps normally.
#[test]
fn try_new_at_honors_skip_inbound_label_check() {
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("lbl"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let transform = TransformOptions::new()
    .with_label(Some(b"cluster".to_vec()))
    .expect("valid label")
    .with_skip_inbound_label_check(true);
  let mut engine = Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng(), &NoGossip)
    .expect("construct with skip-label");
  engine.start(now);
  let mut gossip = NoGossip;
  let mut stream = NoStream::with_pool(2);
  let _deadline = engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1,
    "constructs and pumps with skip-label"
  );
}

/// `send_many` to a CIDR-blocked destination is dropped before enqueueing (the
/// batch counterpart to `send`'s suppression): no datagram is emitted, while an
/// in-policy batch does emit.
#[cfg(feature = "cidr")]
#[test]
fn send_many_to_cidr_blocked_destination_emits_no_datagram() {
  use memberlist_proto::CidrPolicy;

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("c"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");
  engine.start(now);
  let mut stream = NoStream::with_pool(0);

  let blocked = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
  let batch = [
    bytes::Bytes::from_static(b"a"),
    bytes::Bytes::from_static(b"b"),
  ];
  engine
    .send_many(blocked, &batch)
    .expect("send_many returns Ok");
  let mut gossip = CaptureGossip::new();
  engine.pump(now, &mut gossip, &mut stream);
  assert!(
    gossip.sent.is_empty(),
    "no datagram may be emitted to a CIDR-blocked send_many destination"
  );
}

/// The drain-before-close path: B accepts a join push/pull but its reply tx is
/// held unacknowledged when its bridge gracefully closes, so `teardown` parks the
/// connection in `Closing` (KEEPING it mapped) instead of FIN-ing immediately;
/// `flush_closing` keeps it mapped while it drains and finally emits the terminal
/// FIN once the tx is fully acked, reclaiming the slot. A graceful close must never
/// truncate an unacknowledged reply.
#[test]
fn closing_drain_defers_fin_until_tx_acked_then_reclaims_slot() {
  let mut link = LinkPair::new(&[10, 11], &[20, 21]);
  let now = Instant::from_origin(Duration::from_secs(86_400));
  // B accrues unacked tx, so its push/pull reply lingers in the tx ring when its
  // bridge closes — forcing the `Closing` drain instead of an immediate FIN.
  link.hold_b_tx();

  link.a.join(&[link.b_addr]).expect("join queues the seed");

  // Drive until B has a connection parked in `Closing` (its reply sent but
  // unacked, its bridge graceful-closed). A still converges because the reply
  // bytes were delivered to its rx; only the ack is withheld.
  let mut saw_closing = false;
  for _ in 0..40 {
    link.step(now);
    while link.a.poll_event().is_some() {}
    while link.b.poll_event().is_some() {}
    if has_closing_connection(&mut link.b) {
      saw_closing = true;
      break;
    }
  }
  assert!(
    saw_closing,
    "B's graceful close with unacked tx must park the connection in Closing, not FIN at once"
  );
  // B held its slot in the Closing connection; it is NOT yet back in the pool.
  let reclaimed_before = link.b.pool_free_count() + link.b.listener_present() as usize;
  assert!(
    reclaimed_before < 2,
    "the draining connection still pins its slot before the tx is acked"
  );

  // Partially ack: the undelivered count shrinks but is still non-zero and the
  // (fixed, never re-armed) deadline has not elapsed, so `flush_closing` leaves the
  // connection mapped and keeps draining.
  link.ack_b(1);
  link.step(now);
  while link.b.poll_event().is_some() {}

  // Fully ack the remainder: `flush_closing` now sees zero undelivered, emits the
  // terminal FIN, detaches the slot into `closing`, and the reap pass reclaims it.
  link.ack_b(usize::MAX);
  let mut t = now;
  for _ in 0..20 {
    link.step(t);
    while link.b.poll_event().is_some() {}
    if link.b.pool_free_count() + link.b.listener_present() as usize == 2 {
      break;
    }
    t += Duration::from_millis(200);
  }
  assert_eq!(
    link.b.pool_free_count() + link.b.listener_present() as usize,
    2,
    "once the held tx is fully acked, the drained connection FINs and its slot is reclaimed"
  );
}

/// Whether the engine currently has a reliable connection in the `Closing` drain
/// state. `closing_count` counts the DETACHED `closing`-map handles, not the
/// still-mapped `Closing` connections, so this scans the live connections (via the
/// public `plane_mut`) for the drain state the test waits on.
fn has_closing_connection(e: &mut Engine<SmolStr, u32>) -> bool {
  e.plane_mut()
    .connections
    .values()
    .any(|c| c.state == ConnState::Closing)
}

/// After a completed reliable user-message, `last_completed_send` returns the
/// originating `StreamId` right after the `poll_event` that yielded its
/// `ExchangeCompleted`, and is reset to `None` on the next poll — the driver's
/// hook for resolving a send-awaiting waiter by id rather than arrival order.
#[test]
fn last_completed_send_reports_the_stream_id_then_resets() {
  use memberlist_proto::event::ExchangeKind;

  let mut link = LinkPair::new(&[10, 11], &[20, 21]);
  let now = Instant::from_origin(Duration::from_secs(86_400));

  let sid = link
    .a
    .send_reliable(link.b_addr, bytes::Bytes::from_static(b"hi"), now)
    .expect("send_reliable queues the exchange");

  let mut resolved: Option<StreamId> = None;
  for _ in 0..40 {
    link.step(now);
    while let Some(ev) = link.a.poll_event() {
      if let Event::ExchangeCompleted(ec) = ev {
        if ec.kind() == ExchangeKind::UserMessage {
          // Read right after the completing poll: the StreamId of the finished send.
          resolved = link.a.last_completed_send();
        }
      }
    }
    while link.b.poll_event().is_some() {}
    if resolved.is_some() {
      break;
    }
  }
  assert_eq!(
    resolved,
    Some(sid),
    "last_completed_send must report the originating StreamId of the completed send"
  );
  // A subsequent poll (no completion) resets it.
  let _ = link.a.poll_event();
  assert_eq!(
    link.a.last_completed_send(),
    None,
    "last_completed_send resets on the next poll"
  );
}

/// `inject_alive` drops a non-routable peer up front: a port-0 (or unspecified)
/// address is never built into a synthetic Alive, so no member is added — the
/// explicit-contract early return.
#[test]
fn inject_alive_drops_a_non_routable_peer() {
  let mut engine = make_engine();
  let now = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(now);

  // Port 0 is non-routable.
  let bad = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 0);
  engine.inject_alive(SmolStr::new("ghost"), bad, now);
  assert_eq!(
    engine.num_members(),
    1,
    "a non-routable injected peer must be dropped, never added"
  );
}

/// `check_listener` drops an inbound passive open from a CIDR-blocked peer at the
/// transport boundary: it aborts the connected listener socket, returns the slot
/// to the pool, and re-arms a fresh listener — without registering the exchange.
#[cfg(feature = "cidr")]
#[test]
fn check_listener_rejects_cidr_blocked_inbound_and_rearms() {
  use memberlist_proto::CidrPolicy;

  let now = Instant::from_origin(Duration::from_secs(86_400));
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("c"), node_addr(7946));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    now,
    test_rng(),
    &NoGossip,
  )
  .expect("construct");
  // A spare slot to re-arm the listener from after the reject.
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  engine.start(now);

  let before = engine.accepted_inbound_count();
  let mut stream = ProgRel::new(&[0, 1]);
  stream
    .listen(1, 7946, crate::SlotGen::START)
    .expect("listen");
  // The passive open settled, but the remote is out-of-policy (192.168/16).
  stream.sock_mut(1).accepted = Some(SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(192, 168, 1, 9)),
    7000,
  ));
  stream.sock_mut(1).established = true;

  engine.check_listener(now, &mut stream);

  assert_eq!(
    engine.accepted_inbound_count(),
    before,
    "a CIDR-blocked inbound must NOT be admitted (the counter does not rise)"
  );
  assert!(
    stream.aborted.contains(&1),
    "the blocked listener socket must be aborted"
  );
  assert!(
    engine.listener_present(),
    "a fresh listener must be re-armed from the spare after the reject"
  );
}

/// `reap_retiring` leaves a still-draining handle parked when its teardown is not
/// yet acknowledged AND its deadline has not elapsed — it is reclaimed only later.
/// This pins the keep-parked arm (the complement of the reclaim/escalate arms).
#[test]
fn reap_retiring_keeps_a_still_draining_handle_parked() {
  use crate::{RetirePhase, Retiring, SlotGen};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  let mut stream = ProgRel::new(&[0]);
  // Slot 0 is still flushing its FIN: open, and its deadline is in the future.
  stream.sock_mut(0).open = true;
  engine.plane_mut().retiring.insert(
    0,
    Retiring {
      generation: SlotGen::START,
      deadline: now + Duration::from_secs(60),
      phase: RetirePhase::Draining,
    },
  );

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.closing_count(),
    1,
    "a still-open, within-deadline draining handle must stay parked for a later tick"
  );
  assert!(
    !stream.aborted.contains(&0),
    "a within-deadline closing handle must NOT be force-aborted yet"
  );
}

/// The `Closing` drain's force-abort backstop: when B's reply tx is held
/// unacknowledged AND the peer never drains it past `close_timeout`, `flush_closing`
/// gives up on the remainder, RST-aborts the socket, and reclaims the slot — so a
/// vanished/stalled peer can never permanently wedge a pooled slot mid-drain.
#[test]
fn closing_drain_force_aborts_a_stalled_peer_at_the_deadline() {
  let mut link = LinkPair::new(&[10, 11], &[20, 21]);
  let now = Instant::from_origin(Duration::from_secs(86_400));
  // B holds its reply tx unacknowledged; we never ack it, so the drain stalls.
  link.hold_b_tx();

  link.a.join(&[link.b_addr]).expect("join queues the seed");
  let mut saw_closing = false;
  for _ in 0..40 {
    link.step(now);
    while link.a.poll_event().is_some() {}
    while link.b.poll_event().is_some() {}
    if has_closing_connection(&mut link.b) {
      saw_closing = true;
      break;
    }
  }
  assert!(
    saw_closing,
    "precondition: B parked a connection in Closing"
  );

  // Never ack. Advance well past `close_timeout` (10 s) so the no-progress idle
  // bound elapses and `flush_closing` force-aborts the drain.
  let mut t = now + Duration::from_secs(15);
  for _ in 0..10 {
    link.step(t);
    while link.b.poll_event().is_some() {}
    if !has_closing_connection(&mut link.b)
      && link.b.pool_free_count() + link.b.listener_present() as usize == 2
    {
      break;
    }
    t += Duration::from_secs(15);
  }
  assert!(
    !has_closing_connection(&mut link.b),
    "the stalled Closing connection must be force-aborted off the map at the deadline"
  );
  assert_eq!(
    link.b.pool_free_count() + link.b.listener_present() as usize,
    2,
    "the force-aborted slot must be reclaimed so the pool cannot wedge"
  );
}

/// An encryption-configured engine drops an inbound PLAINTEXT gossip datagram on
/// the ingress decrypt step: a node on an encrypted cluster must not admit an
/// unauthenticated frame, so the unencrypted Alive is rejected and no ghost member
/// appears. (The drop is at the keyring-aware `decrypt_gossip` unwrap.)
#[cfg(feature = "aes-gcm")]
#[test]
fn encrypted_node_drops_plaintext_inbound_gossip() {
  use memberlist_proto::{
    EncodeOptions, Node, encode_outgoing,
    typed::{Alive, Message},
  };

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("enc"), node_addr(7946));
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let key = SecretKey::Aes256([0x11; 32]);
  let transform = TransformOptions::default()
    .with_encryption(EncryptionOptions::new().with_keyring(Keyring::new(key)));
  let mut engine = Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng(), &NoGossip)
    .expect("construct encrypted");
  engine.start(now);

  // A perfectly valid PLAINTEXT Alive — but this node expects encrypted frames, so
  // the decrypt step rejects it.
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7946);
  let ghost = Node::new(SmolStr::new("ghost"), peer);
  let plaintext = encode_outgoing::<SmolStr, SocketAddr>(
    &Message::Alive(Alive::new(1, ghost)),
    &EncodeOptions::default(),
  )
  .expect("encode plaintext Alive");

  let src = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3)), 7946);
  let mut gossip = QueueGossip::new();
  gossip.push(src, plaintext.to_vec());
  let mut stream = NoStream::with_pool(2);
  let _ = engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.num_members(),
    1,
    "a plaintext datagram on an encrypted node must be dropped — no ghost admitted"
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Generation-tagged slot-teardown protocol (the reuse ledger).
//
// A generation-aware async-teardown mock whose `teardown_done` reports `true`
// ONLY once the test explicitly acknowledges the matching occupancy generation
// (`ack`) — modelling the embassy-net worker that resets a socket on a later wake
// and acknowledges the exact occupancy it served. It pins the ledger's
// generation-tagged reuse gate directly, without a second engine.

struct AckRel {
  /// Driver-side free-list (unused by the engine, which owns `ReliablePlane::pool`).
  free: Vec<u32>,
  /// The occupancy generation whose teardown the "worker" has acknowledged, per
  /// slot. Absent = not yet acknowledged.
  acked: BTreeMap<u32, crate::SlotGen>,
  /// Per-slot open flag (set by `listen`/`connect`; `ack` clears it, modelling the
  /// socket reaching a clean Closed state).
  open: BTreeMap<u32, bool>,
  /// `(slot, gen)` of every `abort`, for assertions.
  aborts: Vec<(u32, crate::SlotGen)>,
}

impl AckRel {
  fn new(handles: &[u32]) -> Self {
    Self {
      free: handles.to_vec(),
      acked: BTreeMap::new(),
      open: BTreeMap::new(),
      aborts: Vec::new(),
    }
  }

  /// Acknowledge that the worker finished tearing down occupancy `g` of slot `c`
  /// (the async reset completed), so `teardown_done(c, g)` now reports `true`.
  fn ack(&mut self, c: u32, g: crate::SlotGen) {
    self.acked.insert(c, g);
    self.open.insert(c, false);
  }
}

impl StreamIo for AckRel {
  type Conn = u32;

  fn take_free(&mut self) -> Option<u32> {
    self.free.pop()
  }

  fn give(&mut self, c: u32) {
    self.free.push(c);
  }

  fn free_count(&self) -> usize {
    self.free.len()
  }

  fn teardown_done(&self, c: u32, g: crate::SlotGen) -> bool {
    // Only the acknowledged occupancy is reusable; a mismatched/unknown gen is inert.
    self.acked.get(&c) == Some(&g)
  }

  fn listen(&mut self, c: u32, _port: u16, _g: crate::SlotGen) -> Result<(), crate::StreamIoError> {
    self.open.insert(c, true);
    Ok(())
  }

  fn accepted_peer(&self, _c: u32) -> Option<SocketAddr> {
    None
  }

  fn connect(
    &mut self,
    c: u32,
    _remote: SocketAddr,
    _local_port: u16,
    _g: crate::SlotGen,
  ) -> Result<(), crate::StreamIoError> {
    self.open.insert(c, true);
    Ok(())
  }

  fn may_send(&self, _c: u32) -> bool {
    false
  }

  fn may_recv(&self, _c: u32) -> bool {
    false
  }

  fn is_open(&self, c: u32) -> bool {
    self.open.get(&c).copied().unwrap_or(false)
  }

  fn is_established(&self, _c: u32) -> bool {
    false
  }

  fn recv(&mut self, _c: u32, _buf: &mut [u8]) -> Option<usize> {
    None
  }

  fn recv_finished(&self, _c: u32) -> bool {
    false
  }

  fn send(&mut self, _c: u32, _bytes: &[u8]) -> usize {
    0
  }

  fn send_queue(&self, _c: u32) -> usize {
    0
  }

  fn close(&mut self, _c: u32, _g: crate::SlotGen) {}

  fn abort(&mut self, c: u32, g: crate::SlotGen) {
    self.aborts.push((c, g));
  }
}

/// (a) A retired slot is NOT returned to the pool until the driver acknowledges
/// its teardown for the retired generation; the matching acknowledgement then
/// frees it on the next reap.
#[test]
fn retiring_slot_is_not_freed_until_teardown_done() {
  use crate::{RetirePhase, Retiring, SlotGen};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  // A listener is already installed so the freed slot is not immediately re-armed
  // as the listener (which would hide it from the pool count).
  engine.set_listener(9);
  let mut stream = AckRel::new(&[]);
  engine.plane_mut().slot_gen.insert(0, SlotGen::START);
  engine.plane_mut().retiring.insert(
    0,
    Retiring {
      generation: SlotGen::START,
      deadline: now + Duration::from_secs(30),
      phase: RetirePhase::Aborting,
    },
  );

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.retiring_count(),
    1,
    "an unacknowledged teardown keeps the slot retiring"
  );
  assert_eq!(
    engine.pool_free_count(),
    0,
    "a retiring slot is never in the pool"
  );

  // The worker acknowledges the occupancy: the next reap frees it.
  stream.ack(0, SlotGen::START);
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.retiring_count(),
    0,
    "an acknowledged teardown frees the slot"
  );
  assert_eq!(
    engine.pool_free_count(),
    1,
    "the freed slot returns to the pool"
  );
}

/// (b) A stale (mismatched-generation) acknowledgement never frees a later
/// occupancy — the reuse gate is generation-tagged, so an ack for a prior
/// occupancy of the same slot is inert.
#[test]
fn a_stale_gen_ack_never_frees_a_later_occupancy() {
  use crate::{RetirePhase, Retiring, SlotGen};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.set_listener(9);
  let mut stream = AckRel::new(&[]);
  // Slot 0 is on its SECOND occupancy (generation START.next()).
  let g1 = SlotGen::START.next();
  engine.plane_mut().slot_gen.insert(0, g1);
  engine.plane_mut().retiring.insert(
    0,
    Retiring {
      generation: g1,
      deadline: now + Duration::from_secs(30),
      phase: RetirePhase::Aborting,
    },
  );

  let mut gossip = NoGossip;
  // A stale acknowledgement for the PRIOR occupancy (START) must not free g1.
  stream.ack(0, SlotGen::START);
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.retiring_count(),
    1,
    "a mismatched-generation ack must never free a later occupancy"
  );

  // The matching acknowledgement frees it.
  stream.ack(0, g1);
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.retiring_count(),
    0,
    "the matching-generation ack frees the slot"
  );
}

/// (c) An `Aborting` occupancy whose teardown is never acknowledged past its
/// deadline re-issues the (idempotent) abort, re-arms the deadline, and counts a
/// `teardown_overruns` — surfacing the residual pin without ever blind-freeing it.
#[test]
fn an_unacknowledged_abort_past_its_deadline_counts_a_teardown_overrun() {
  use crate::{RetirePhase, Retiring, SlotGen};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.set_listener(9);
  let mut stream = AckRel::new(&[]);
  // `engine_with_stream_timeout` fixes `close_timeout` at 10s.
  let close_timeout = Duration::from_secs(10);
  engine.plane_mut().slot_gen.insert(0, SlotGen::START);
  // Already Aborting with an elapsed deadline; the worker never acknowledges.
  engine.plane_mut().retiring.insert(
    0,
    Retiring {
      generation: SlotGen::START,
      deadline: now,
      phase: RetirePhase::Aborting,
    },
  );

  let mut gossip = NoGossip;
  assert_eq!(engine.teardown_overruns(), 0);

  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.teardown_overruns(),
    1,
    "an Aborting deadline expiry counts an overrun"
  );
  assert_eq!(
    engine.retiring_count(),
    1,
    "the residual pin is surfaced, never blind-freed"
  );
  assert!(
    stream.aborts.iter().any(|&(c, _)| c == 0),
    "the overrun re-issues the (idempotent) abort"
  );

  // The deadline was re-armed to now + close_timeout, so a same-instant pump does
  // not double-count.
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.teardown_overruns(),
    1,
    "the re-armed deadline is not immediately re-expired"
  );

  // Past the re-armed deadline it counts again.
  let later = now + close_timeout + Duration::from_millis(1);
  engine.pump(later, &mut gossip, &mut stream);
  assert_eq!(
    engine.teardown_overruns(),
    2,
    "a second Aborting deadline expiry counts again"
  );
}

/// (d) A never-activated occupancy — a retired slot whose socket was never opened
/// (the CIDR / routable reject inside `dial`, aborting before any `listen` /
/// `connect`) — round-trips cleanly through the ledger: retire → acknowledge →
/// free. With a synchronous driver the never-opened socket is already reusable, so
/// the immediate reap frees it the SAME tick, exactly as the real dial-reject
/// paths (`reliable_non_routable_dial_fails_before_connect`,
/// `reliable_dial_connect_rejection_fails_and_reclaims_slot`) rely on.
#[test]
fn a_never_activated_occupancy_round_trips_through_the_ledger() {
  use crate::{RetirePhase, Retiring, SlotGen};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.set_listener(9);
  // Slot 0 was taken and its generation minted, but never listened/connected, so
  // its socket is `!open` — a synchronous driver reports its teardown done at once.
  let mut stream = ProgRel::new(&[0, 9]);
  assert!(
    !stream.sock(0).open,
    "the never-activated slot's socket is closed"
  );
  engine.plane_mut().slot_gen.insert(0, SlotGen::START);
  engine.plane_mut().retiring.insert(
    0,
    Retiring {
      generation: SlotGen::START,
      deadline: now + Duration::from_secs(30),
      phase: RetirePhase::Aborting,
    },
  );

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);
  assert_eq!(
    engine.retiring_count(),
    0,
    "a never-activated occupancy's teardown is acknowledged and freed the same tick"
  );
  assert_eq!(
    engine.pool_free_count(),
    1,
    "the reclaimed slot is back in the pool"
  );
}

// --- Engine teardown, close-deadline and inbound-fault policy ---

/// Drive an outbound reliable user-message to `Established` on slot 0 (listener on
/// slot 1), returning the engine, the mock, the clock, and the exchange's
/// `ExchangeId`. The tx ring is held non-empty (`tx_unacked = 8`) so the machine's
/// write-half Shutdown FIN stays withheld — keeping the connection `Established`
/// (not `HalfClosed`) so a caller can drive the teardown / drain / inbound-fault
/// paths from a known state.
fn established_outbound(
  payload: &'static [u8],
) -> (
  Engine<SmolStr, u32>,
  ProgRel,
  Instant,
  memberlist_proto::streams::ExchangeId,
) {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);

  let to = node_addr(7100);
  engine
    .send_reliable(to, bytes::Bytes::from_static(payload), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  // Tick 1: Connect → dial slot 0 (SynSent).
  engine.pump(now, &mut gossip, &mut stream);
  // Complete the handshake but hold the tx ring non-empty, so the deferred-FIN gate
  // (`send_queue == 0`) never fires and the connection stays Established.
  stream.sock_mut(0).established = true;
  stream.sock_mut(0).tx_unacked = 8;
  // Tick 2: promote Dialing → Established; the request bytes flush.
  engine.pump(now, &mut gossip, &mut stream);

  let eid = *engine
    .plane_mut()
    .connections
    .keys()
    .next()
    .expect("exactly one outbound connection");
  assert_eq!(
    engine.plane_mut().connections.get(&eid).map(|c| c.state),
    Some(ConnState::Established),
    "the exchange must reach Established with its FIN withheld (held tx)"
  );
  (engine, stream, now, eid)
}

/// F1: a graceful `Close` of an ESTABLISHED connection under FULL outbound-ring
/// backpressure (`may_send` false, `is_established` true) must DRAIN — enter
/// `Closing` and later emit its FIN once the ring frees — never RST. Before the
/// fix the `!may_send` gate sent it down the abrupt-abort branch.
#[test]
fn backpressured_established_close_drains_not_aborts() {
  let (mut engine, mut stream, now, eid) = established_outbound(b"reply-in-flight");

  // Model a FULL outbound ring: the socket is established but not writable, with
  // bytes still unacknowledged in the tx ring.
  stream.sock_mut(0).ring_full = true;
  assert!(
    !StreamIo::may_send(&stream, 0) && StreamIo::is_established(&stream, 0),
    "the socket must be established but NOT writable (the F1 divergence)"
  );

  // The graceful Close arrives now. With the fix it DRAINS (Closing), not RST.
  engine.teardown(eid, now, &mut stream);

  assert!(
    !stream.aborted.contains(&0),
    "a backpressured established close must NOT abort/RST"
  );
  {
    let conn = engine
      .plane_mut()
      .connections
      .get(&eid)
      .expect("the connection stays mapped to drain");
    assert_eq!(
      conn.state,
      ConnState::Closing,
      "a backpressured established close must enter Closing to drain, not abort"
    );
    assert!(
      conn.close_deadline.is_some(),
      "the Closing drain arms its hard-cap deadline"
    );
  }

  // The ring frees and the tx drains: `flush_closing` now emits the terminal FIN.
  stream.sock_mut(0).ring_full = false;
  stream.sock_mut(0).tx_unacked = 0;
  engine.flush_closing(now, &mut stream);
  assert!(
    stream.closed.contains(&0),
    "once drained, the graceful close emits a FIN (close), never an abort"
  );
  assert!(!stream.aborted.contains(&0), "the drained close never RSTs");
  assert!(
    engine.plane_mut().connections.get(&eid).is_none(),
    "the drained connection is removed after its terminal FIN"
  );
}

/// F1 control: a never-established (`Dialing`) connection whose graceful `Close`
/// arrives still ABORTS (the peer never established a wire, so there is nothing to
/// drain). This is the abrupt-abort branch the F1 divergence must NOT capture.
#[test]
fn unestablished_dialing_close_aborts_not_drains() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);

  let to = node_addr(7101);
  engine
    .send_reliable(to, bytes::Bytes::from_static(b"never-established"), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  // One pump: Connect dials slot 0 (the socket opens, SynSent) but the handshake
  // never completes — the connection stays `Dialing`.
  engine.pump(now, &mut gossip, &mut stream);
  let eid = *engine
    .plane_mut()
    .connections
    .keys()
    .next()
    .expect("one dialing connection");
  assert_eq!(
    engine.plane_mut().connections.get(&eid).map(|c| c.state),
    Some(ConnState::Dialing),
    "the connection must still be Dialing (handshake incomplete)"
  );

  engine.teardown(eid, now, &mut stream);
  assert!(
    stream.aborted.contains(&0),
    "a never-established close aborts (RST), it does not drain"
  );
  assert!(
    !stream.closed.contains(&0),
    "a never-established close never emits a graceful FIN"
  );
  assert!(
    engine.plane_mut().connections.get(&eid).is_none(),
    "the aborted connection is removed"
  );
}

/// F2: an ACK trickle (undelivered bytes shrinking by 1 each tick) must NOT extend
/// the `Closing` drain deadline. The deadline is set ONCE at `Closing` entry and
/// never re-armed, so a permanently-trickling peer is force-aborted at the ORIGINAL
/// deadline rather than deferring the close forever.
#[test]
fn ack_trickle_cannot_extend_closing_past_one_window() {
  let (mut engine, mut stream, now, eid) = established_outbound(b"held-reply");

  // Park a known undelivered count and take the graceful close into `Closing`.
  stream.sock_mut(0).tx_unacked = 5;
  engine.teardown(eid, now, &mut stream);
  let deadline = engine
    .plane_mut()
    .connections
    .get(&eid)
    .expect("the connection parks in Closing")
    .close_deadline
    .expect("the Closing drain arms a deadline");

  // Trickle: the peer acks 1 byte per tick. Each `flush_closing` at an advancing
  // clock (still before the deadline) makes progress but must NOT re-arm the
  // deadline — the fix removed the progress re-arm entirely.
  let mut t = now;
  for acked in 1..=4u32 {
    t += Duration::from_secs(1);
    stream.sock_mut(0).tx_unacked = 5 - acked as usize; // 4, 3, 2, 1
    engine.flush_closing(t, &mut stream);
    let d = engine
      .plane_mut()
      .connections
      .get(&eid)
      .expect("still draining within the deadline")
      .close_deadline
      .expect("deadline still present");
    assert_eq!(
      d, deadline,
      "an ACK trickle must NOT re-arm the close deadline (hard cap)"
    );
  }

  // One undelivered byte remains. Advance PAST the original deadline: the drain is
  // force-aborted at that (never-extended) instant, proving the trickle bought no
  // extra time.
  let past = deadline + Duration::from_secs(1);
  engine.flush_closing(past, &mut stream);
  assert!(
    stream.aborted.contains(&0),
    "the still-draining connection is force-aborted at its ORIGINAL deadline"
  );
  assert!(
    engine.plane_mut().connections.get(&eid).is_none(),
    "the force-aborted Closing connection is removed"
  );
}

/// F2: `close_deadline` is set exactly once at `Closing` entry and is never moved
/// by a later `flush_closing` pass, even when the drain makes progress.
#[test]
fn close_deadline_set_once() {
  let (mut engine, mut stream, now, eid) = established_outbound(b"reply");

  stream.sock_mut(0).tx_unacked = 3;
  engine.teardown(eid, now, &mut stream);
  let deadline = engine
    .plane_mut()
    .connections
    .get(&eid)
    .expect("Closing")
    .close_deadline
    .expect("deadline set once at Closing entry");
  assert_eq!(
    deadline,
    now + crate::DEFAULT_CLOSE_TIMEOUT,
    "the deadline is now + close_timeout"
  );

  // A progress tick (undelivered shrinks) at a later clock must leave the deadline
  // untouched.
  stream.sock_mut(0).tx_unacked = 1;
  engine.flush_closing(now + Duration::from_secs(3), &mut stream);
  assert_eq!(
    engine
      .plane_mut()
      .connections
      .get(&eid)
      .expect("still Closing")
      .close_deadline,
    Some(deadline),
    "progress must not re-arm the deadline"
  );
}

/// F6: a socket that dies mid-exchange (`!is_open`, no graceful FIN) is surfaced to
/// the machine as a transport error THIS pump — the bridge fails, its `Abort`
/// action is drained, and `abort_exchange` removes+retires the slot within the same
/// pump — instead of the exchange lingering until its ~stream-timeout deadline.
#[test]
fn mid_exchange_rst_terminalizes_within_one_pump() {
  use memberlist_proto::event::{Event, ExchangeKind, ExchangeStatus};

  let (mut engine, mut stream, now, eid) = established_outbound(b"mid-exchange");
  // Drain any setup events.
  while engine.poll_event().is_some() {}

  // A mid-exchange RST: the socket closes WITHOUT a graceful peer FIN.
  stream.sock_mut(0).open = false; // is_open → false
  // peer_fin stays false, so recv_finished is false — this is a fault, not an EOF.

  // A SINGLE pump at the (un-advanced) clock: the stream deadline (now + 30s) has
  // NOT elapsed, so the ONLY way the exchange fails this pump is the F6 fault path.
  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);

  let mut failed = false;
  while let Some(ev) = engine.poll_event() {
    if let Event::ExchangeCompleted(ec) = ev {
      if ec.kind() == ExchangeKind::UserMessage && ec.outcome() == ExchangeStatus::Failed {
        failed = true;
      }
    }
  }
  assert!(
    failed,
    "a mid-exchange RST must fail the exchange within one pump, not at the stream deadline"
  );
  assert!(
    engine.plane_mut().connections.get(&eid).is_none(),
    "the failed exchange's connection is removed the same pump (abort_exchange)"
  );
  assert!(
    stream.aborted.contains(&0),
    "abort_exchange RST-resets the slot"
  );
  assert_eq!(
    engine.pool_free_count() + engine.listener_present() as usize,
    2,
    "the reset slot is reclaimed, never leaked"
  );
}

/// F6: a `Dialing` connection whose socket dies before the handshake completes
/// (`!is_open`, never established) routes through `handle_dial_failed` — failing the
/// exchange within one pump rather than reading a benign EOF as a false success.
#[test]
fn async_dial_failure_calls_handle_dial_failed() {
  use memberlist_proto::event::{Event, ExchangeKind, ExchangeStatus};

  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);

  let to = node_addr(7102);
  engine
    .send_reliable(to, bytes::Bytes::from_static(b"dial-fails"), now)
    .expect("send_reliable queues the exchange");

  let mut gossip = NoGossip;
  // One pump: Connect dials slot 0 (opens, SynSent); the connection is `Dialing`.
  engine.pump(now, &mut gossip, &mut stream);
  let eid = *engine
    .plane_mut()
    .connections
    .keys()
    .next()
    .expect("one dialing connection");
  assert_eq!(
    engine.plane_mut().connections.get(&eid).map(|c| c.state),
    Some(ConnState::Dialing),
    "the connection must be Dialing (handshake not yet complete)"
  );
  while engine.poll_event().is_some() {}

  // The async dial fails: the socket dies while still Dialing (never established).
  stream.sock_mut(0).open = false;

  // One pump: the F6 fault path routes a Dialing `!is_open` through
  // `handle_dial_failed`, failing the exchange this pump.
  engine.pump(now, &mut gossip, &mut stream);
  let mut failed = false;
  while let Some(ev) = engine.poll_event() {
    if let Event::ExchangeCompleted(ec) = ev {
      if ec.kind() == ExchangeKind::UserMessage && ec.outcome() == ExchangeStatus::Failed {
        failed = true;
      }
    }
  }
  assert!(
    failed,
    "an async dial failure must fail the exchange within one pump (handle_dial_failed)"
  );
  assert!(
    engine.plane_mut().connections.get(&eid).is_none(),
    "the dial-failed exchange's connection is removed the same pump"
  );
}

/// F6: the benign completed tail — the peer's EOF was already delivered, `out` is
/// empty, and the tx ring is fully acked — must NOT be surfaced as a transport
/// error even though the socket has gone `!is_open`. That `!is_open` is the normal
/// LastAck→Closed end of a SUCCESSFUL exchange whose own `Close` is already due;
/// failing it would falsely turn a success into a failure.
#[test]
fn benign_completed_tail_does_not_false_fail() {
  use memberlist_proto::event::{Event, ExchangeStatus};

  let (mut engine, mut stream, now, eid) = established_outbound(b"benign-tail");

  // Put the connection into the benign completed-tail window: EOF already delivered,
  // our FIN sent (HalfClosed, so not Dialing), `out` empty, tx ring fully acked.
  {
    let conn = engine
      .plane_mut()
      .connections
      .get_mut(&eid)
      .expect("the exchange connection");
    conn.eof_delivered = true;
    conn.state = ConnState::HalfClosed;
    assert!(
      conn.out_is_empty(),
      "the benign tail has no parked out bytes"
    );
  }
  stream.sock_mut(0).tx_unacked = 0; // send_queue == 0
  stream.sock_mut(0).open = false; // !is_open
  stream.sock_mut(0).peer_fin = false; // recv_finished is false (past FIN-wait, Closed)
  while engine.poll_event().is_some() {}

  // The inbound pump observes `!is_open` on the still-mapped connection but must
  // recognise the benign tail and NOT surface a transport error.
  engine.pump_inbound_reliable(now, &mut stream);

  let mut any_failed = false;
  while let Some(ev) = engine.poll_event() {
    if let Event::ExchangeCompleted(ec) = ev {
      if ec.outcome() == ExchangeStatus::Failed {
        any_failed = true;
      }
    }
  }
  assert!(
    !any_failed,
    "the benign completed tail must NOT be surfaced as a transport failure"
  );
  let conn = engine
    .plane_mut()
    .connections
    .get(&eid)
    .expect("the benign tail is not torn down by the inbound pump");
  assert!(
    conn.error_delivered,
    "the !is_open observation still latches error_delivered (one-shot), even benign"
  );
}

/// F6: `error_delivered` is a one-shot latch — the first `!is_open` observation
/// surfaces the fault and sets the latch, so a second inbound pump over the (still
/// mapped) connection does not re-surface it.
#[test]
fn error_delivered_is_one_shot() {
  let (mut engine, mut stream, now, eid) = established_outbound(b"one-shot");
  while engine.poll_event().is_some() {}

  // Mid-exchange RST.
  stream.sock_mut(0).open = false;

  // Call the inbound pump directly (so `drain_stream_actions` does not remove the
  // connection): the first observation surfaces the fault and latches.
  engine.pump_inbound_reliable(now, &mut stream);
  assert!(
    engine
      .plane_mut()
      .connections
      .get(&eid)
      .expect("still mapped (no action drain)")
      .error_delivered,
    "the first !is_open observation latches error_delivered"
  );
  while engine.poll_event().is_some() {}

  // A second inbound pump over the still-mapped, still-`!is_open` connection is a
  // no-op for the fault path (latch set) — it neither clears the latch nor surfaces
  // another completion.
  engine.pump_inbound_reliable(now, &mut stream);
  assert!(
    engine
      .plane_mut()
      .connections
      .get(&eid)
      .expect("still mapped")
      .error_delivered,
    "the latch stays set across pumps (one-shot)"
  );
  assert!(
    engine.poll_event().is_none(),
    "a latched connection surfaces no further fault"
  );
}

/// Number of datagrams still waiting in the fake driver ring.
impl QueueGossip {
  fn ring_len(&self) -> usize {
    self.inbound.len()
  }
}

/// Encode a plaintext, unlabelled `Alive` for `(id, addr)` at `incarnation`, as a
/// peer's gossip datagram would arrive on the wire.
fn alive_datagram(id: &str, addr: SocketAddr, incarnation: u32) -> Vec<u8> {
  use memberlist_proto::{
    EncodeOptions, codec,
    typed::{Alive, Message},
  };

  let msg = Message::<SmolStr, SocketAddr>::Alive(Alive::new(
    incarnation,
    Node::new(SmolStr::new(id), addr),
  ));
  codec::encode_outgoing(&msg, &EncodeOptions::new(None))
    .expect("encode Alive")
    .to_vec()
}

/// An undecodable gossip datagram: it costs a pop and an unwrap attempt, and is
/// dropped before the machine sees anything.
fn junk_datagram() -> Vec<u8> {
  std::vec![0xABu8; 24]
}

/// Bring up a running engine whose sole other member is peer `B`, then pump the
/// clock forward until the SWIM failure-detection probe fires a direct `Ping` at
/// `B`. Returns the engine, its gossip fake, `B`'s address, the pump instant `F`
/// at which the probe was sent, and the probe's ack sequence number decoded from
/// that `Ping`. At the initial (healthy) awareness score the probe's authoritative
/// failure deadline is `F + probe_interval` (the 1s default).
///
/// `cfg` carries whatever engine options the caller needs (a CIDR policy, say);
/// the port must stay 7946 so `B` is reachable from the local advertise address.
fn arm_detection_probe_on_b_with(
  cfg: Options,
) -> (Engine<SmolStr, u32>, QueueGossip, SocketAddr, Instant, u32) {
  use memberlist_proto::{DecodeOptions, codec, typed::Message};

  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("local"), node_addr(7946));
  let start = Instant::from_origin(Duration::from_secs(86_400));
  let mut engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    ep_cfg,
    start,
    test_rng(),
    &NoGossip,
  )
  .expect("valid configuration");
  engine.start(start);
  // A distinct, routable address so B is a separate member and the probe's direct
  // Ping is addressed to it.
  let b_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7947);
  engine.inject_alive(SmolStr::new("b"), b_addr, start);

  let mut gossip = QueueGossip::new();
  let mut stream = NoStream::with_pool(4);
  // The probe interval is 1s; step coarsely until the direct Ping at B appears.
  let mut t = start;
  for _ in 0..200 {
    t += Duration::from_millis(50);
    engine.pump(t, &mut gossip, &mut stream);
    // Scan (and clear) this pump's egress for a Ping addressed to B; decode it to
    // recover the ack sequence number the machine expects back.
    let mut seq = None;
    for (bytes, dest) in core::mem::take(&mut gossip.outbound) {
      if dest != b_addr {
        continue;
      }
      let Ok(plain) = codec::decode_incoming(bytes::Bytes::from(bytes), &DecodeOptions::new(None))
      else {
        continue;
      };
      let Ok(msgs) = codec::parse_messages::<SmolStr, SocketAddr>(plain) else {
        continue;
      };
      for msg in msgs {
        if let Message::Ping(p) = msg {
          seq = Some(p.sequence_number());
        }
      }
    }
    if let Some(seq) = seq {
      return (engine, gossip, b_addr, t, seq);
    }
  }
  panic!("the failure-detection probe never fired a direct Ping at B");
}

/// [`arm_detection_probe_on_b_with`] under the plain default options.
fn arm_detection_probe_on_b() -> (Engine<SmolStr, u32>, QueueGossip, SocketAddr, Instant, u32) {
  arm_detection_probe_on_b_with(
    Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10)),
  )
}

/// Let the armed probe on `B` expire so `B` transitions to `Suspect`, and return
/// `(t1, S)`: the pump instant at which it was suspected, and the instant its
/// suspicion deadline falls due.
///
/// With the default `probe_interval` (1s) and `suspicion_mult` (4) over a
/// two-member cluster the node-count scale is 1.0 and `k` collapses to 0
/// (`n < suspicion_mult`), so the suspicion timer is fixed at
/// `probe_interval * suspicion_mult` = 4s from `t1`.
fn suspect_b(
  engine: &mut Engine<SmolStr, u32>,
  gossip: &mut QueueGossip,
  stream: &mut NoStream,
  f: Instant,
) -> (Instant, Instant) {
  use memberlist_proto::typed::State;

  let t1 = f + Duration::from_millis(1100);
  engine.pump(t1, gossip, stream);
  assert_eq!(
    engine.num_members_by(|ns| ns.id_ref() == &SmolStr::new("b") && ns.state() == State::Suspect),
    1,
    "the expired detection probe must suspect B, or the test proves nothing"
  );
  // Discard the events the arming produced; the assertions below watch only the
  // events the refutation pump emits.
  while engine.poll_event().is_some() {}
  (t1, t1 + Duration::from_secs(4))
}

/// Fill the fake's declared ring with undecodable datagrams and then `B`'s
/// `Alive(inc + 1)` refutation, so the refutation is the LAST datagram the pump's
/// read bound admits.
fn junk_then_b_refutation(
  engine: &Engine<SmolStr, u32>,
  gossip: &mut QueueGossip,
  b_addr: SocketAddr,
) {
  let junk_src = node_addr(7050);
  for _ in 0..FAKE_RECV_CAPACITY {
    gossip.push(junk_src, junk_datagram());
  }
  let inc = engine
    .endpoint
    .endpoint_ref()
    .node_incarnation(&SmolStr::new("b"))
    .expect("B is a member");
  gossip.push(b_addr, alive_datagram("b", b_addr, inc + 1));
}

/// Assert `B` came out of the pump `Alive` with no membership flap.
fn assert_b_refuted(engine: &mut Engine<SmolStr, u32>) {
  use memberlist_proto::typed::State;

  assert_eq!(
    engine.num_members_by(|ns| ns.id_ref() == &SmolStr::new("b") && ns.state() == State::Alive),
    1,
    "B's own refutation, read in this pump, must leave it Alive"
  );
  while let Some(ev) = engine.poll_event() {
    assert!(
      !matches!(ev, Event::NodeLeft(_) | Event::NodeJoined(_)),
      "B must not flap: no death and no re-join event"
    );
  }
}

/// A refutation read in the same pump that crosses its subject's suspicion
/// deadline wins, because phase 2 applies every datagram it pops before step 6
/// can fire a timer.
///
/// `process_alive` has no instant cutoff — only the incarnation gate — so an
/// `Alive(inc + 1)` for a `Suspect` B clears the suspicion outright. What decides
/// the outcome is purely the order of the two within the pump: read-then-tick
/// keeps B alive, tick-then-read kills it and immediately resurrects it. The
/// refutation is placed LAST in the ring, behind a full ring of junk, so it is
/// also the final datagram the read bound admits.
#[test]
fn same_pump_alive_refutation_precedes_the_tick() {
  let (mut engine, mut gossip, b_addr, f, _seq) = arm_detection_probe_on_b();
  let mut stream = NoStream::with_pool(4);
  let (_t1, s) = suspect_b(&mut engine, &mut gossip, &mut stream, f);

  junk_then_b_refutation(&engine, &mut gossip, b_addr);

  let t = s + Duration::from_secs(1);
  assert!(t > s, "this pump runs past B's suspicion deadline");
  engine.pump(t, &mut gossip, &mut stream);

  assert_b_refuted(&mut engine);
  assert_eq!(
    gossip.ring_len(),
    0,
    "a full cap's worth of datagrams is read and applied within the pump"
  );
}

/// Reliable-plane input reaches the machine through a coordinator tick as well,
/// so arbitrary bytes on an unrelated connection must not fire a suspicion the
/// gossip ring already refutes. Two independent properties hold that: phase 3
/// follows phase 2, and this engine's coordinator does not advance membership
/// time on a feed at all, so the sweep runs only at step 6. The bytes here are
/// junk: input the record layer will reject is enough, because the class is
/// about the feed, not the parse.
#[test]
fn same_pump_reliable_input_cannot_sweep_past_this_pumps_gossip() {
  let (mut engine, mut gossip, b_addr, f, _seq) = arm_detection_probe_on_b();
  let mut probe_stream = NoStream::with_pool(4);
  let (t1, s) = suspect_b(&mut engine, &mut gossip, &mut probe_stream, f);

  // Admit an inbound reliable connection on a pump before the deadline, so the
  // pump under test only has to feed its bytes.
  let mut stream = armed_inbound_listener(&mut engine);
  let accepted_before = engine.accepted_inbound_count();
  engine.pump(t1 + Duration::from_millis(100), &mut gossip, &mut stream);
  assert_eq!(
    engine.accepted_inbound_count(),
    accepted_before + 1,
    "the inbound reliable connection must be admitted, or the test proves nothing"
  );
  while engine.poll_event().is_some() {}

  stream.sock_mut(1).rx = std::vec![0xABu8; 64];
  junk_then_b_refutation(&engine, &mut gossip, b_addr);

  let t = s + Duration::from_secs(1);
  engine.pump(t, &mut gossip, &mut stream);

  assert_b_refuted(&mut engine);
  assert_eq!(gossip.ring_len(), 0, "the whole ring was read this pump");
}

/// The reliable plane reaches the machine on its FAULT paths too: a socket that
/// died without a graceful peer FIN routes to `handle_transport_error`, which —
/// like `handle_transport_data` — runs a coordinator tick. A peer that merely
/// RSTs its connection must not sweep past this pump's gossip either.
#[test]
fn same_pump_reliable_fault_cannot_sweep_past_this_pumps_gossip() {
  let (mut engine, mut gossip, b_addr, f, _seq) = arm_detection_probe_on_b();
  let mut probe_stream = NoStream::with_pool(4);
  let (t1, s) = suspect_b(&mut engine, &mut gossip, &mut probe_stream, f);

  let mut stream = armed_inbound_listener(&mut engine);
  let accepted_before = engine.accepted_inbound_count();
  engine.pump(t1 + Duration::from_millis(100), &mut gossip, &mut stream);
  assert_eq!(
    engine.accepted_inbound_count(),
    accepted_before + 1,
    "the inbound reliable connection must be admitted, or the test proves nothing"
  );
  while engine.poll_event().is_some() {}

  // The peer RSTs: the socket closes with no graceful FIN and nothing to read,
  // which phase 3 surfaces to the machine as a transport fault.
  stream.sock_mut(1).open = false;
  junk_then_b_refutation(&engine, &mut gossip, b_addr);

  let t = s + Duration::from_secs(1);
  engine.pump(t, &mut gossip, &mut stream);

  assert_b_refuted(&mut engine);
  assert_eq!(gossip.ring_len(), 0, "the whole ring was read this pump");
}

/// The read bound is the PUMPED view's own declared capacity, never the capacity
/// construction screened — and it holds in every build profile.
///
/// `try_new_at` sees one view; `pump` accepts a fresh view on every call, and only
/// the driver can see the two diverge. Here the engine is built over a conforming
/// 63-slot view and then pumped with a truthful 65-slot one whose last slot holds
/// B's refutation, at an instant past B's suspicion deadline. A bound fixed at
/// `GOSSIP_READ_CAP` would leave that datagram in the ring across step 6's sweep,
/// so B would die and be resurrected by a later pump — a flap the immediate wake
/// cannot undo. Deriving the bound from the view in hand applies it first instead,
/// with no assertion for a release build to strip out.
#[test]
fn pumping_a_larger_truthful_view_than_constructed_with_still_applies_everything_before_the_sweep()
{
  let (mut engine, mut small, b_addr, f, _seq) = arm_detection_probe_on_b();
  let mut stream = NoStream::with_pool(4);
  let (_t1, s) = suspect_b(&mut engine, &mut small, &mut stream, f);

  // A second, distinct view: 65 truthful slots — past the engine's cap, and larger
  // than the 63-slot view construction screened. It arrives full: 64 junk
  // datagrams, then B's own refutation in the last slot.
  let over_cap = GOSSIP_READ_CAP + 1;
  let mut big = QueueGossip::with_recv_capacity(over_cap);
  let junk_src = node_addr(7050);
  for _ in 0..(over_cap - 1) {
    big.push(junk_src, junk_datagram());
  }
  let inc = engine
    .endpoint
    .endpoint_ref()
    .node_incarnation(&SmolStr::new("b"))
    .expect("B is a member");
  big.push(b_addr, alive_datagram("b", b_addr, inc + 1));

  let t = s + Duration::from_secs(1);
  assert!(t > s, "this pump runs past B's suspicion deadline");
  let wake = engine.pump(t, &mut big, &mut stream);

  assert_b_refuted(&mut engine);
  assert_eq!(
    big.ring_len(),
    0,
    "the whole 65-slot ring is read within the pump that observed it"
  );
  assert_eq!(
    engine.gossip_over_cap_pumps(),
    1,
    "the divergence from the screened view is surfaced, not silently trusted"
  );
  assert!(
    wake > Some(t),
    "a truthful ring, over-cap or not, is emptied and folds no already-due wake"
  );
}

/// A view that delivers more datagrams than it declares is read to its declared
/// capacity plus the one probe pop, and no further. The remainder stays in the
/// driver's ring — not dropped, not copied into the engine — and is read on the
/// next pump, which the returned already-due deadline asks for immediately.
///
/// Only an under-declaring (or mid-pump refilling) view can reach this: for a
/// truthful one the declared capacity is the most the ring can hold, so the ring
/// is always emptied within the pump.
#[test]
fn a_view_delivering_more_than_it_declares_is_read_to_its_bound_and_re_pumped() {
  let mut engine = make_engine();
  let t = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(t);
  // The fake's queue is unbounded, so it can be handed more datagrams than the
  // capacity it declares — the under-declaration the probe pop exists to catch.
  let declared = FAKE_RECV_CAPACITY;
  let bound = declared + 1;
  let mut gossip = QueueGossip::with_recv_capacity(declared);
  let mut stream = NoStream::with_pool(2);

  // Settle the machine's own schedulers first, so the only already-due term the
  // pump under test can return is the gossip one.
  let quiescent = engine.pump(t, &mut gossip, &mut stream);
  assert!(
    quiescent > Some(t),
    "no machine timer may be due at this instant, or the wake assertion proves nothing"
  );

  // `bound + k` distinct, valid Alives, each for its own id and address.
  let extra = 5usize;
  let src = node_addr(7050);
  for i in 0..(bound + extra) {
    let addr = node_addr(8000 + i as u16);
    gossip.push(src, alive_datagram(&std::format!("n{i}"), addr, 1));
  }

  let first = engine.pump(t, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1 + bound,
    "the declared capacity plus the probe pop is applied in the first pump"
  );
  assert_eq!(
    gossip.ring_len(),
    extra,
    "the remainder waits in the driver's ring, neither dropped nor engine-held"
  );
  assert_eq!(
    first,
    Some(t),
    "the probe pop found a datagram past the declaration: fold an already-due wake so the caller \
     polls again at once"
  );
  assert_eq!(
    engine.gossip_over_cap_pumps(),
    0,
    "the view declared a conforming capacity — it under-declared its occupancy, which is not an \
     over-cap pump"
  );

  let second = engine.pump(t, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1 + bound + extra,
    "the remainder is applied on the next pump"
  );
  assert_eq!(gossip.ring_len(), 0, "the ring is now empty");
  assert!(
    second > Some(t),
    "a pump whose probe pop found nothing folds no already-due wake"
  );
  assert_eq!(
    engine.gossip_over_cap_pumps(),
    0,
    "still no over-cap pump: the declaration never reached the cap"
  );
}

/// A conforming ring that is completely full is emptied within the pump and folds
/// no already-due wake: the bound is the declared capacity plus one, so the probe
/// pop past a full ring of capacity `C` finds nothing. This is the corner a bound
/// fixed at the cap could not tell apart from a ring with more behind it, and it
/// cost one spurious re-pump on every full read.
#[test]
fn a_full_conforming_ring_does_not_set_the_wake_term() {
  let capacity = 8usize;
  let mut engine = make_engine();
  let t = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(t);
  let mut gossip = QueueGossip::with_recv_capacity(capacity);
  let mut stream = NoStream::with_pool(2);

  // Settle the machine's own schedulers first, so the only already-due term the
  // pump under test could return is the gossip one.
  let quiescent = engine.pump(t, &mut gossip, &mut stream);
  assert!(
    quiescent > Some(t),
    "no machine timer may be due at this instant, or the wake assertion proves nothing"
  );

  let src = node_addr(7050);
  for i in 0..capacity {
    let addr = node_addr(8000 + i as u16);
    gossip.push(src, alive_datagram(&std::format!("n{i}"), addr, 1));
  }

  let wake = engine.pump(t, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    1 + capacity,
    "every datagram in the full ring is applied in the pump that popped it"
  );
  assert_eq!(gossip.ring_len(), 0, "the full ring is emptied");
  assert!(
    wake > Some(t),
    "a full conforming ring asks for no immediate re-pump"
  );
  assert_eq!(
    engine.gossip_over_cap_pumps(),
    0,
    "a conforming view is not an over-cap pump"
  );
}

/// A [`GossipIo`] whose ring the link layer refills as fast as the engine reads
/// it: every `recv` succeeds, so only the engine's own bound can end the loop.
struct RefillGossip {
  /// The ring size the link layer keeps topped up, as declared to the engine.
  recv_capacity: usize,
  /// `recv` calls served, so a test can pin the per-pump read bound exactly.
  recv_calls: usize,
}

impl RefillGossip {
  fn new(recv_capacity: usize) -> Self {
    Self {
      recv_capacity,
      recv_calls: 0,
    }
  }
}

impl GossipIo for RefillGossip {
  fn recv(&mut self, buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    self.recv_calls += 1;
    let bytes = junk_datagram();
    let n = bytes.len().min(buf.len());
    buf[..n].copy_from_slice(&bytes[..n]);
    Some((node_addr(7050), n))
  }

  fn send(&mut self, _bytes: &[u8], _dest: SocketAddr) {}

  fn recv_capacity(&self) -> usize {
    self.recv_capacity
  }
}

/// A ring that refills while the loop is reading it costs a bounded number of pops
/// and sets the wake term.
///
/// The declared capacity is what a ring can hold between two pumps, so a datagram
/// found past it means the link layer topped the ring up mid-pump (or the view
/// under-declares itself). The engine takes exactly one pop past the declaration —
/// enough to detect that and no more, so an unbounded arrival rate cannot
/// monopolize a pump — and asks for an immediate re-pump instead.
#[test]
fn a_ring_that_refills_mid_pump_sets_the_wake_term() {
  let capacity = 4usize;
  let mut engine = make_engine();
  let t = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(t);
  let mut stream = NoStream::with_pool(2);

  // Settle the machine's schedulers over an empty view — the refilling one can
  // never report an idle ring — so the only already-due term the pump under test
  // could return is the gossip one.
  let quiescent = engine.pump(t, &mut NoGossip, &mut stream);
  assert!(
    quiescent > Some(t),
    "no machine timer may be due at this instant, or the wake assertion proves nothing"
  );

  let mut gossip = RefillGossip::new(capacity);
  let wake = engine.pump(t, &mut gossip, &mut stream);

  assert_eq!(
    gossip.recv_calls,
    capacity + 1,
    "an endlessly refilling ring is read to the declared capacity plus the probe pop, no further"
  );
  assert_eq!(
    wake,
    Some(t),
    "a datagram past the declaration folds an already-due wake for the remainder"
  );
  assert_eq!(
    engine.gossip_over_cap_pumps(),
    0,
    "the declared capacity is below the cap, so a refill is not an over-cap pump"
  );
}

/// The sweep is never withheld while gossip is outstanding: a pump that stops at
/// its gossip read bound still fires every timer due at its instant. A design that
/// ticked only once nothing observed was pending would let a sustained flood
/// silence the node's own failure detection.
#[test]
fn a_due_timer_fires_in_the_pump_that_stops_at_the_gossip_read_bound() {
  use memberlist_proto::typed::State;

  let (mut engine, mut gossip, _b_addr, f, _seq) = arm_detection_probe_on_b();
  let mut stream = NoStream::with_pool(4);

  // More than the fake's declared capacity, so the pump under test cannot empty
  // the ring and has gossip outstanding when the probe deadline falls due.
  let junk_src = node_addr(7050);
  for _ in 0..(FAKE_RECV_CAPACITY + 6) {
    gossip.push(junk_src, junk_datagram());
  }

  // Past the probe's authoritative failure deadline (`F + probe_interval`).
  let t = f + Duration::from_millis(1100);
  let wake = engine.pump(t, &mut gossip, &mut stream);

  assert_eq!(
    engine.num_members_by(|ns| ns.id_ref() == &SmolStr::new("b") && ns.state() == State::Suspect),
    1,
    "the expired probe must suspect B in the very pump that stopped at the read bound"
  );
  assert_eq!(
    wake,
    Some(t),
    "the probe pop found more behind the declaration: fold an already-due wake for the datagrams \
     still in the ring"
  );
  assert_eq!(
    engine.gossip_over_cap_pumps(),
    0,
    "the fake declares a conforming capacity, so no over-cap pump is counted"
  );
  assert_eq!(
    gossip.ring_len(),
    5,
    "the pump read exactly the declared capacity plus the probe pop and left the rest in the ring"
  );
}

/// After `leave` the engine still POPS gossip — the ring is the driver's and would
/// otherwise back up — but it decodes none of it, changes no membership, and never
/// asks to be re-pumped. The pop stays bounded by the pumped view's read bound, so
/// a flood aimed at a node that has left costs at most that many memcpys per pump.
#[test]
fn post_leave_gossip_is_popped_not_decoded_and_never_wakes() {
  let mut engine = make_engine();
  let t = Instant::from_origin(Duration::from_secs(86_400));
  engine.start(t);
  let mut gossip = QueueGossip::new();
  let mut stream = NoStream::with_pool(2);
  engine.leave(t).expect("a running engine leaves cleanly");

  let members_before = engine.num_members();
  let extra = 8usize;
  let src = node_addr(7050);
  for i in 0..(FAKE_RECV_CAPACITY + 1 + extra) {
    let addr = node_addr(8000 + i as u16);
    gossip.push(src, alive_datagram(&std::format!("n{i}"), addr, 1));
  }

  let wake = engine.pump(t, &mut gossip, &mut stream);
  assert_eq!(
    engine.num_members(),
    members_before,
    "a left node admits no member from gossip"
  );
  assert_eq!(
    gossip.ring_len(),
    extra,
    "the post-leave pop is bounded by the view's read bound, not run to exhaustion"
  );
  assert_eq!(
    engine.gossip_over_cap_pumps(),
    0,
    "the fake declares a conforming capacity, so no over-cap pump is counted"
  );
  assert_ne!(
    wake,
    Some(t),
    "a left node never asks to be re-pumped for gossip it will not decode"
  );

  engine.pump(t, &mut gossip, &mut stream);
  assert_eq!(gossip.ring_len(), 0, "the rest is popped on the next pump");
  assert_eq!(
    engine.num_members(),
    members_before,
    "still no membership change"
  );
}

/// The rebalance dials a `PendingDial` parked on a previous pump, and a dial the
/// transport rejects synchronously terminalizes through the machine — whose tick
/// sweeps membership at this pump's instant. Running that before the gossip phase,
/// as the pump once did, let an unrelated outbound dial fire a suspicion this
/// pump's ring already refutes. The dial site therefore sits at the END of the
/// tick (7d''), after both evidence feeds and after the machine tick, so a
/// rejection can never precede the evidence that refutes it.
#[cfg(feature = "cidr")]
#[test]
fn dial_rejection_cannot_sweep_before_this_pumps_gossip() {
  use memberlist_proto::CidrPolicy;

  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10))
    // B (10.0.0.2) and the local node (10.0.0.1) are both in policy; the dial
    // target below is not.
    .with_cidr_policy(CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr"));
  let (mut engine, mut gossip, b_addr, f, _seq) = arm_detection_probe_on_b_with(cfg);
  let mut stream = NoStream::with_pool(4);
  let (t1, s) = suspect_b(&mut engine, &mut gossip, &mut stream, f);

  // A listener is installed and the pool is empty, so the dial below has no slot
  // to claim even at the late rebalance and is still parked when the next pump runs.
  engine.set_listener(1);
  let blocked = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 7001);
  engine
    .send_reliable(blocked, bytes::Bytes::from_static(b"blocked-bytes"), t1)
    .expect("send_reliable queues the exchange");

  let t2 = t1 + Duration::from_millis(100);
  engine.pump(t2, &mut gossip, &mut stream);
  assert_eq!(
    engine.pending_dial_count(),
    1,
    "the dial must still be parked, or the rebalance under test has nothing to do"
  );
  while engine.poll_event().is_some() {}

  // Free the slot the parked dial will claim in the pump under test. The listener
  // is already present, so `ensure_listener` leaves it for the dial.
  engine.plane_mut().pool.push(0);
  junk_then_b_refutation(&engine, &mut gossip, b_addr);

  let t = s + Duration::from_secs(1);
  engine.pump(t, &mut gossip, &mut stream);

  assert_eq!(
    engine.pending_dial_count(),
    0,
    "the rebalance must have dialed the parked exchange in this pump"
  );
  assert_b_refuted(&mut engine);
  assert_eq!(gossip.ring_len(), 0, "the whole ring was read this pump");
}

/// Nothing the engine observed is still pending when the machine ticks, at every
/// ring size a driver can present — and no size costs a spurious wake.
///
/// Each view here is TRUTHFUL: it declares the ring size it is then handed, which
/// is the most a real ring of that size could hold. The pump's bound is that
/// declaration plus one, so the ring is always emptied and the probe pop always
/// comes back empty, whatever the size. Even a full ring at the cap — the corner
/// that used to cost one spurious re-pump — folds no already-due wake now; it is
/// only counted as the over-cap pump it is, since construction would have rejected
/// that view.
#[test]
fn nothing_observed_is_pending_when_the_machine_ticks() {
  for ring in [1usize, 8, 16, GOSSIP_READ_CAP - 1, GOSSIP_READ_CAP] {
    let mut engine = make_engine();
    let t = Instant::from_origin(Duration::from_secs(86_400));
    engine.start(t);
    let mut gossip = QueueGossip::with_recv_capacity(ring);
    let mut stream = NoStream::with_pool(2);

    let quiescent = engine.pump(t, &mut gossip, &mut stream);
    assert!(
      quiescent > Some(t),
      "ring {ring}: no machine timer may be due at this instant"
    );

    let src = node_addr(7050);
    for i in 0..ring {
      let addr = node_addr(8000 + i as u16);
      gossip.push(src, alive_datagram(&std::format!("n{i}"), addr, 1));
    }

    let wake = engine.pump(t, &mut gossip, &mut stream);
    assert_eq!(gossip.ring_len(), 0, "ring {ring}: the ring is emptied");
    assert_eq!(
      engine.num_members(),
      1 + ring,
      "ring {ring}: every datagram read was applied in that same pump"
    );
    assert!(
      wake > Some(t),
      "ring {ring}: a truthful ring is emptied, so no already-due wake is folded"
    );
    // The settle pump above and the pump under test both ran over this view, and
    // the counter counts PUMPS over an over-cap view, not distinct views.
    let pumps_over_this_view = 2;
    assert_eq!(
      engine.gossip_over_cap_pumps(),
      if ring >= GOSSIP_READ_CAP {
        pumps_over_this_view
      } else {
        0
      },
      "ring {ring}: every pump over an over-cap view is counted, and a conforming one never is"
    );
  }
}

/// An engine cannot be constructed over a gossip view whose declared receive ring
/// reaches the per-pump read cap: `try_new_at` rejects it with the typed
/// `GossipRecvCapacityTooLarge`, carrying the capacity the view declared. A ring
/// one slot below the cap constructs.
///
/// What the screen enforces is the per-pump WORK CEILING, not the correctness
/// bound: phase 2 reads to whatever capacity the pumped view declares, so a
/// 65-slot ring is emptied ahead of that pump's sweep either way. Capping the ring
/// a driver may present is what keeps the unwrap/decode/apply a single pump can be
/// made to do bounded. The screen sits on the trait, so it binds every `GossipIo`,
/// including one implemented outside this workspace.
#[test]
fn a_gossip_io_whose_ring_reaches_the_read_cap_cannot_construct_an_engine() {
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let cfg = || {
    Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10))
  };
  let ep_cfg = || memberlist_proto::EndpointOptions::new(SmolStr::new("cap"), node_addr(7946));

  // One slot over the cap, and the cap itself: the bound is strict.
  for declared in [GOSSIP_READ_CAP + 1, GOSSIP_READ_CAP] {
    let gossip = QueueGossip::with_recv_capacity(declared);
    let rejected: Result<Engine<SmolStr, u32>, _> = Engine::try_new_at(
      cfg(),
      TransformOptions::default(),
      ep_cfg(),
      now,
      test_rng(),
      &gossip,
    );
    match rejected {
      Err(InitError::GossipRecvCapacityTooLarge(n)) => assert_eq!(
        n, declared,
        "the rejection carries the capacity the view declared"
      ),
      Err(other) => panic!("a {declared}-slot ring must be rejected for its capacity, got {other}"),
      Ok(_) => panic!("a {declared}-slot ring must not construct an engine"),
    }
  }

  // The largest conforming ring still constructs a working single-node engine.
  let gossip = QueueGossip::with_recv_capacity(GOSSIP_READ_CAP - 1);
  let engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg(),
    TransformOptions::default(),
    ep_cfg(),
    now,
    test_rng(),
    &gossip,
  )
  .expect("a ring one slot below the cap must construct");
  assert_eq!(engine.num_members(), 1, "the constructed engine is usable");
}

/// The ring screen follows the CONFIGURED ceiling, not the constant: an engine
/// built with a cap of 8 rejects a 9-slot ring (and the 8-slot ring at the cap),
/// and accepts a 7-slot one — all of which the default cap of 64 would have
/// admitted. The rejection still carries the capacity the view declared.
#[test]
fn the_ring_screen_follows_the_configured_cap() {
  const CAP: usize = 8;
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let cfg = || {
    Options::new()
      .with_port(7946)
      .with_close_timeout(Duration::from_secs(10))
      .with_gossip_read_cap(CAP)
  };
  let ep_cfg = || memberlist_proto::EndpointOptions::new(SmolStr::new("cap"), node_addr(7946));

  for declared in [CAP + 1, CAP] {
    let gossip = QueueGossip::with_recv_capacity(declared);
    let rejected: Result<Engine<SmolStr, u32>, _> = Engine::try_new_at(
      cfg(),
      TransformOptions::default(),
      ep_cfg(),
      now,
      test_rng(),
      &gossip,
    );
    match rejected {
      Err(InitError::GossipRecvCapacityTooLarge(n)) => assert_eq!(
        n, declared,
        "the rejection carries the capacity the view declared"
      ),
      Err(other) => panic!("a {declared}-slot ring must be rejected under a cap of {CAP}: {other}"),
      Ok(_) => panic!(
        "a {declared}-slot ring must not construct under a cap of {CAP} (the default 64 would admit it)"
      ),
    }
  }

  let gossip = QueueGossip::with_recv_capacity(CAP - 1);
  let engine: Engine<SmolStr, u32> = Engine::try_new_at(
    cfg(),
    TransformOptions::default(),
    ep_cfg(),
    now,
    test_rng(),
    &gossip,
  )
  .expect("a ring one slot below the configured cap must construct");
  assert_eq!(engine.num_members(), 1, "the constructed engine is usable");
}

/// A zero gossip read cap is rejected as the knob it is, by the shared preflight
/// and by construction alike. Zero admits no ring at all (the screen is strictly
/// below the cap), so reporting it as a ring-capacity failure would name the wrong
/// field: no `udp_rx_packets` / socket size could ever satisfy it.
#[test]
fn a_zero_gossip_read_cap_is_rejected_as_the_knob_it_is() {
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let cfg = Options::new().with_port(7946).with_gossip_read_cap(0);

  assert!(
    matches!(
      validate_runtime_config(&cfg, &TransformOptions::default(), 1400),
      Err(InitError::ZeroGossipReadCap)
    ),
    "the shared preflight rejects a zero cap before a driver resolves or binds anything"
  );

  let gossip = QueueGossip::with_recv_capacity(0);
  let rejected: Result<Engine<SmolStr, u32>, _> = Engine::try_new_at(
    cfg,
    TransformOptions::default(),
    memberlist_proto::EndpointOptions::new(SmolStr::new("cap"), node_addr(7946)),
    now,
    test_rng(),
    &gossip,
  );
  assert!(
    matches!(rejected, Err(InitError::ZeroGossipReadCap)),
    "construction rejects a zero cap even for an empty ring, naming the knob"
  );
}

/// Arm `engine` with a reliable listener on slot 1 and one spare (slot 0) to
/// replenish from, backed by a fresh `ProgRel`. The mock admits the passive open
/// on the first `check_listener` that runs after the caller marks slot 1
/// `accepted` + `established`, which is what gives the test a live inbound
/// exchange to drive reliable input through.
fn armed_inbound_listener(engine: &mut Engine<SmolStr, u32>) -> ProgRel {
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  let mut stream = ProgRel::new(&[0, 1]);
  stream
    .listen(1, 7946, crate::SlotGen::START)
    .expect("mock listen succeeds");
  // A settled passive open from an in-policy remote, ready for the next accept.
  stream.sock_mut(1).accepted = Some(node_addr(7950));
  stream.sock_mut(1).established = true;
  stream
}

/// A proto dialer's coalesced `[label || push/pull request]` bytes, advertising
/// `b` at `b_inc` as `Alive` — the refutation an engine merges when the request
/// decodes. `pad` bytes of application snapshot inflate the request; at
/// `16 * 1024` it spans several of the phase-3 read loop's `READ_BUF` chunks.
///
/// The dialer's record layer and transforms are built from the SAME
/// [`TransformOptions::default()`] the engine fixtures construct their
/// coordinator with, so the bytes decode identically under every feature
/// combination the crate is gated on.
fn push_pull_refuting_b(
  dialer_addr: SocketAddr,
  engine_addr: SocketAddr,
  b_addr: SocketAddr,
  b_inc: u32,
  now: Instant,
  pad: usize,
) -> Vec<u8> {
  let transform = TransformOptions::default();
  let ep: Endpoint<SmolStr, SocketAddr, SmallRng> = Endpoint::new_at(
    EndpointOptions::new(SmolStr::new("dialer"), dialer_addr),
    now,
    test_rng(),
  );
  let mut dialer: StreamEndpoint<SmolStr, SocketAddr, RawRecords, SmallRng> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(transform.label().map(|b| b.to_vec()), ()),
    Box::new(|_: &SocketAddr| -> Option<std::string::String> { None }),
    Box::new(|addr: &SocketAddr| *addr),
  );
  #[cfg(compression)]
  dialer.set_compression_options(transform.compression);
  #[cfg(encryption)]
  dialer.set_encryption_options(transform.encryption);

  dialer.handle_alive(
    b_addr,
    Alive::new(b_inc, Node::new(SmolStr::new("b"), b_addr)),
    now,
  );
  if pad > 0 {
    dialer
      .set_local_state_snapshot(bytes::Bytes::from(std::vec![0u8; pad]))
      .expect("the padding snapshot is well within the reliable frame budget");
  }
  dialer.start_push_pull(engine_addr, PushPullKind::Join, now);
  while dialer.poll_action().is_some() {}
  let mut blob = Vec::new();
  while let Some((_id, _peer, chunk)) = dialer.poll_transport_transmit() {
    blob.extend_from_slice(&chunk);
  }
  blob
}

/// Assert `B` came out of the pump `Alive` and never flapped. Unlike
/// [`assert_b_refuted`] this tolerates the membership events a real peer's
/// push/pull necessarily produces for the DIALER's own node — only `B`'s events
/// are under test.
fn assert_b_refuted_over_reliable(engine: &mut Engine<SmolStr, u32>) {
  use memberlist_proto::typed::State;

  let b = SmolStr::new("b");
  assert_eq!(
    engine.num_members_by(|ns| ns.id_ref() == &b && ns.state() == State::Alive),
    1,
    "B's own refutation, read in this pump, must leave it Alive"
  );
  while let Some(ev) = engine.poll_event() {
    if let Event::NodeLeft(ns) | Event::NodeJoined(ns) = ev {
      assert!(
        ns.id_ref() != &b,
        "B must not flap: no death and no re-join event"
      );
    }
  }
}

/// Admit one inbound reliable connection on a pump before `B`'s suspicion
/// deadline, so the pump under test has only to feed its bytes. Returns the
/// mock stream with the connection live on slot 1.
fn accept_one_inbound(
  engine: &mut Engine<SmolStr, u32>,
  gossip: &mut QueueGossip,
  at: Instant,
) -> ProgRel {
  let mut stream = armed_inbound_listener(engine);
  let accepted_before = engine.accepted_inbound_count();
  engine.pump(at, gossip, &mut stream);
  assert_eq!(
    engine.accepted_inbound_count(),
    accepted_before + 1,
    "the inbound reliable connection must be admitted, or the test proves nothing"
  );
  while engine.poll_event().is_some() {}
  stream
}

/// The reliable unit a peer sends can exceed the phase-3 read buffer, so ONE
/// pump feeds it to the machine as several `handle_transport_data` calls. No
/// feed may sweep membership between those chunks: the `Alive(inc + 1)` the last
/// chunk completes refutes a suspicion this pump is already past, so a sweep on
/// an earlier chunk would fire `NodeLeft(B)` and the refutation would then read
/// as a `NodeJoined(B)` rejoin — a flap on a peer that never died.
///
/// Deterministic by construction: one connection, and the read loop's chunking
/// is a plain `min(rx.len, READ_BUF)` drain, so the split is not hash- or
/// schedule-dependent.
#[test]
fn chunked_push_pull_refutation_in_one_pump_does_not_flap() {
  let (mut engine, mut gossip, b_addr, f, _seq) = arm_detection_probe_on_b();
  let mut probe_stream = NoStream::with_pool(4);
  let (t1, s) = suspect_b(&mut engine, &mut gossip, &mut probe_stream, f);

  let mut stream = accept_one_inbound(&mut engine, &mut gossip, t1 + Duration::from_millis(100));

  let b_inc = engine
    .endpoint
    .endpoint_ref()
    .node_incarnation(&SmolStr::new("b"))
    .expect("B is a member");
  let blob = push_pull_refuting_b(
    node_addr(7950),
    node_addr(7946),
    b_addr,
    b_inc + 1,
    t1,
    16 * 1024,
  );
  assert!(
    blob.len() > 2 * 4096,
    "the request must span more than two read-buffer chunks, got {} bytes",
    blob.len()
  );
  stream.sock_mut(1).rx = blob;
  stream.sock_mut(1).peer_fin = true;

  let t = s + Duration::from_secs(1);
  assert!(t > s, "this pump runs past B's suspicion deadline");
  engine.pump(t, &mut gossip, &mut stream);

  assert_b_refuted_over_reliable(&mut engine);
}

/// The inter-connection form of the same class: one pump feeds junk on one
/// connection and `B`'s refutation on another. With the pump's single sweep at
/// step 6 the outcome does not depend on which connection phase 3 reaches
/// first, so this passes for either order.
///
/// It is a guard, not a detector: phase 3 iterates a `HashMap`, so on an engine
/// whose feeds do sweep it only fails when the hash order happens to put the
/// junk connection first. The deterministic guard for the class is the chunked
/// single-connection test above.
#[test]
fn two_connections_in_one_pump_junk_then_refutation_does_not_flap() {
  let (mut engine, mut gossip, b_addr, f, _seq) = arm_detection_probe_on_b();
  let mut probe_stream = NoStream::with_pool(4);
  let (t1, s) = suspect_b(&mut engine, &mut gossip, &mut probe_stream, f);

  let mut stream = accept_one_inbound(&mut engine, &mut gossip, t1 + Duration::from_millis(100));

  // The accept replenished the listener from the spare slot; arm THAT one too so
  // a second inbound connection is live for the pump under test.
  stream.sock_mut(0).accepted = Some(node_addr(7951));
  stream.sock_mut(0).established = true;
  let accepted_before = engine.accepted_inbound_count();
  engine.pump(t1 + Duration::from_millis(200), &mut gossip, &mut stream);
  assert_eq!(
    engine.accepted_inbound_count(),
    accepted_before + 1,
    "the second inbound connection must be admitted, or the test proves nothing"
  );
  while engine.poll_event().is_some() {}

  let b_inc = engine
    .endpoint
    .endpoint_ref()
    .node_incarnation(&SmolStr::new("b"))
    .expect("B is a member");
  let blob = push_pull_refuting_b(node_addr(7951), node_addr(7946), b_addr, b_inc + 1, t1, 0);
  assert!(
    blob.len() < 4096,
    "the small request must fit one read-buffer chunk, got {} bytes",
    blob.len()
  );
  stream.sock_mut(1).rx = std::vec![0xABu8; 64];
  stream.sock_mut(0).rx = blob;
  stream.sock_mut(0).peer_fin = true;

  let t = s + Duration::from_secs(1);
  engine.pump(t, &mut gossip, &mut stream);

  assert_b_refuted_over_reliable(&mut engine);
}
