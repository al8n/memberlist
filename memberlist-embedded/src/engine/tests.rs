use super::*;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use memberlist_proto::{
  CompressionOptions, EncryptionOptions, Keyring, SecretKey, SeedableRng, SmallRng,
  typed::NodeState,
};
use smol_str::SmolStr;
use std::vec::Vec;

/// A fixed-seed gossip RNG for the engine constructors. These are single-node
/// state tests; a deterministic seed keeps them reproducible.
fn test_rng() -> SmallRng {
  SmallRng::seed_from_u64(42)
}

struct NoGossip;

impl GossipIo for NoGossip {
  fn recv(&mut self, _buf: &mut [u8]) -> Option<(SocketAddr, usize)> {
    None
  }

  fn send(&mut self, _bytes: &[u8], _dest: SocketAddr) {}
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
  let mut engine =
    Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng()).expect("valid configuration");
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

  fn listen(&mut self, _c: u32, _port: u16) -> Result<(), crate::StreamIoError> {
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

  fn close(&mut self, _c: u32) {}

  fn abort(&mut self, _c: u32) {}
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
  Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
    .expect("valid configuration must construct without error")
}

/// `set_compression_options` is accepted and the engine remains operational
/// (a subsequent `pump` does not panic or error).
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
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
  assert!(
    matches!(
      engine.set_compression_options(CompressionOptions::default()),
      Err(memberlist_proto::Error::NotRunning)
    ),
    "set_compression_options must reject after leave"
  );
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
/// is no backend to probe, so `try_new_at` succeeds regardless of feature set.
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
    Engine::<SmolStr, u32>::try_new_at(cfg, transform, ep_cfg, now, test_rng()).is_ok(),
    "a disabled checksum policy must always construct"
  );
}

struct QueueGossip {
  /// Pending inbound datagrams: each entry is `(src, bytes)`.
  inbound: Vec<(SocketAddr, Vec<u8>)>,
  /// Outbound datagrams captured from `send`.
  outbound: Vec<(Vec<u8>, SocketAddr)>,
}

impl QueueGossip {
  fn new() -> Self {
    Self {
      inbound: Vec::new(),
      outbound: Vec::new(),
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
    Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng()).expect("valid config");
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

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

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

  fn listen(&mut self, c: u32, _port: u16) -> Result<(), crate::StreamIoError> {
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
  ) -> Result<(), crate::StreamIoError> {
    if self.connect_fails {
      return Err(crate::StreamIoError::Busy);
    }
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
    s.established && s.open
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

  fn close(&mut self, c: u32) {
    self.closed.push(c);
    // A graceful close leaves the socket open (FIN in flight) until the test (or
    // reap) drops `open`; the engine parks it in `closing` for the reap pass.
  }

  fn abort(&mut self, c: u32) {
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
  let now = Instant::from_origin(Duration::from_secs(86_400));
  let cfg = Options::new()
    .with_port(7946)
    .with_close_timeout(Duration::from_secs(10));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("test"), node_addr(7946))
    .with_stream_timeout(stream_timeout);
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
  stream.listen(9, 7946).expect("mock listen succeeds");

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

/// The reap pass reclaims a gracefully-closing handle as soon as its socket
/// reaches a reusable (`!is_open`) state, and force-aborts one whose close has
/// exceeded `close_timeout` — so a vanished-mid-FIN peer can never permanently
/// shrink the pool. Both `closing`-map handles are driven directly.
#[test]
fn reap_closing_reclaims_finished_and_force_aborts_timed_out() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  let mut stream = ProgRel::new(&[0, 1]);
  // Slot 0: a clean close that has reached Closed (`!is_open`). Slot 1: a peer that
  // vanished mid-FIN — still open, past its deadline.
  stream.sock_mut(0).open = false;
  stream.sock_mut(1).open = true;
  let close_timeout = Duration::from_secs(10);
  // Park both in `closing`, slot 1 with an already-elapsed deadline.
  engine.plane_mut().closing.insert(0, now + close_timeout);
  engine.plane_mut().closing.insert(1, now);

  assert_eq!(engine.closing_count(), 2, "two handles parked mid-close");

  // Pump at `now`: slot 0 is reusable (reclaimed on the `!is_open` path); slot 1 is
  // past its deadline (`now >= deadline`) so it is force-aborted then reclaimed.
  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.closing_count(),
    0,
    "both closing handles must be reaped (one clean, one force-aborted)"
  );
  assert!(
    stream.aborted.contains(&1),
    "the vanished-mid-FIN handle past its deadline must be force-aborted"
  );
  assert!(
    !stream.aborted.contains(&0),
    "the cleanly-closed handle must be reclaimed without an abort"
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
  stream.listen(1, 7946).expect("mock listen succeeds");
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

  fn listen(&mut self, c: u32, _port: u16) -> Result<(), crate::StreamIoError> {
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

  fn close(&mut self, c: u32) {
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

  fn abort(&mut self, c: u32) {
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
      let mut e: Engine<SmolStr, u32> =
        Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
    a_rel.listen(*a_listener, 7946).expect("listen");
    b_rel.listen(*b_listener, 7947).expect("listen");

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
  let engine: Engine<SmolStr, u32> =
    Engine::new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng());
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
        test_rng()
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
        test_rng()
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
        test_rng()
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
        test_rng()
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
        test_rng()
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
  let mut engine =
    Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng()).expect("construct with skip-label");
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
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
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
/// `flush_closing` then drains it — re-arming on partial progress and finally
/// emitting the terminal FIN once the tx is fully acked, reclaiming the slot. A
/// graceful close must never truncate an unacknowledged reply.
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

  // Partially ack: the undelivered count shrinks, so `flush_closing` takes the
  // Progress branch (re-arm) and keeps the connection mapped.
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
  let mut engine: Engine<SmolStr, u32> =
    Engine::try_new_at(cfg, TransformOptions::default(), ep_cfg, now, test_rng())
      .expect("construct");
  // A spare slot to re-arm the listener from after the reject.
  engine.plane_mut().pool.push(0);
  engine.set_listener(1);
  engine.start(now);

  let before = engine.accepted_inbound_count();
  let mut stream = ProgRel::new(&[0, 1]);
  stream.listen(1, 7946).expect("listen");
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

/// `reap_closing` leaves a still-closing handle parked when its socket has not yet
/// reached a reusable state AND its deadline has not elapsed — it is reclaimed only
/// later. This pins the keep-parked arm (the complement of the reclaim/abort arms).
#[test]
fn reap_closing_keeps_a_still_closing_handle_parked() {
  let (mut engine, now) = engine_with_stream_timeout(Duration::from_secs(30));
  let mut stream = ProgRel::new(&[0]);
  // Slot 0 is still flushing its FIN: open, and its deadline is in the future.
  stream.sock_mut(0).open = true;
  engine
    .plane_mut()
    .closing
    .insert(0, now + Duration::from_secs(60));

  let mut gossip = NoGossip;
  engine.pump(now, &mut gossip, &mut stream);

  assert_eq!(
    engine.closing_count(),
    1,
    "a still-open, within-deadline closing handle must stay parked for a later tick"
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
  let mut engine =
    Engine::try_new_at(cfg, transform, ep_cfg, now, test_rng()).expect("construct encrypted");
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
