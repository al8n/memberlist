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
