use super::*;
use crate::{
  AdvertiseAddrResolver, Delegate, MaybeResolved, Resolver, Transport, TransportRuntime,
  error::MemberlistError,
};
use std::net::SocketAddr;

struct MockTransport;

impl Transport for MockTransport {
  type Error = std::io::Error;
  type Id = smol_str::SmolStr;
  type Address = String;
  type Options = ();

  async fn new<RES, AR>(
    _options: Self::Options,
    _resolver: &RES,
    _advertise_resolver: &AR,
  ) -> Result<Self, Self::Error>
  where
    RES: Resolver<Address = Self::Address>,
    AR: AdvertiseAddrResolver,
  {
    unimplemented!("mock — only exercises Options<T> shape")
  }

  fn local_id(&self) -> &Self::Id {
    unimplemented!()
  }

  fn local_address(&self) -> &MaybeResolved<Self::Address, SocketAddr> {
    unimplemented!()
  }

  fn advertise_address(&self) -> &SocketAddr {
    unimplemented!()
  }

  async fn run<D, G>(self, _runtime: TransportRuntime<Self, D>, _gossip_rng: G)
  where
    D: Delegate<Id = Self::Id, Address = SocketAddr>,
    G: rand::Rng + Send + Unpin + 'static,
  {
    unimplemented!()
  }
}

#[test]
fn options_construction_and_accessors() {
  let opts: Options<MockTransport> = Options::new(());
  let _t: &() = opts.transport();
  let _d: &RuntimeOptions = opts.runtime();
  let _m: &MemberlistOptions = opts.memberlist();
}

#[test]
fn options_builder_chain() {
  let opts: Options<MockTransport> = Options::new(())
    .with_runtime(RuntimeOptions::new())
    .with_memberlist(MemberlistOptions::new());
  let _ = opts.transport(); // Unused: binding only to suppress unused-value lint
}

#[test]
fn validate_driver_options_rejects_deterministic_break_knobs() {
  use core::time::Duration;
  // Baseline (all defaults) is accepted.
  assert!(validate_driver_options(&RuntimeOptions::new()).is_ok());

  // The four deterministic-break knobs are each rejected at zero.
  let breaks = [
    RuntimeOptions::new().with_idle_wake_interval(Duration::ZERO),
    RuntimeOptions::new().with_cmd_fairness_budget(0),
    RuntimeOptions::new().with_peek_budget(Duration::ZERO),
    RuntimeOptions::new().with_observation_channel(crate::Channel::Bounded(0)),
  ];
  for opts in breaks {
    assert!(
      matches!(
        validate_driver_options(&opts),
        Err(MemberlistError::InvalidOption(_))
      ),
      "a deterministic-break knob at zero must be rejected: {opts:?}"
    );
  }

  // Degrade-but-function knobs at zero, and a positive bounded channel,
  // are accepted.
  assert!(validate_driver_options(&RuntimeOptions::new().with_iter_drain_cap(0)).is_ok());
  assert!(validate_driver_options(&RuntimeOptions::new().with_event_queue_cap(0)).is_ok());
  assert!(
    validate_driver_options(
      &RuntimeOptions::new().with_observation_channel(crate::Channel::Bounded(16))
    )
    .is_ok()
  );
}

#[test]
fn observation_channel_round_trips() {
  // Bounded by default (safe against remote-driven OOM).
  assert_eq!(
    RuntimeOptions::new().observation_channel(),
    crate::Channel::Bounded(1024)
  );
  // Explicit opt-in to never-drop, and an explicit smaller bound, round-trip.
  assert_eq!(
    RuntimeOptions::new()
      .with_observation_channel(crate::Channel::Unbounded)
      .observation_channel(),
    crate::Channel::Unbounded
  );
  assert_eq!(
    RuntimeOptions::new()
      .with_observation_channel(crate::Channel::Bounded(8))
      .observation_channel(),
    crate::Channel::Bounded(8)
  );
}

// A scoped/flow-labelled IPv6 advertise address (nonzero `scope_id` or
// `flowinfo`) is not representable on the compact memberlist wire layout, so
// every local-node-bearing control packet would fail to encode at runtime.
// `validate_gossip_mtu_for_identity` builds those packets with the ACTUAL
// advertise address for the local node, so it must reject such an address at
// construction with `InvalidAdvertiseAddr` rather than letting the node
// construct `Ok` and then silently fail every send.
//
// The validator is exercised directly (it takes `&local_id`, `&SocketAddr`,
// `&MemberlistOptions`) rather than end-to-end through `Memberlist::new`:
// binding a real link-local scoped IPv6 socket is system-dependent and would
// fail at the OS bind before reaching this check, making an end-to-end test
// flaky. Calling the validator directly with a constructed scoped address is
// deterministic and isolates exactly the encodability gate under test.
#[test]
fn identity_floor_rejects_scoped_ipv6_advertise_addr() {
  use std::net::{Ipv6Addr, SocketAddrV6};

  let id = smol_str::SmolStr::new("node-a");
  let opts = MemberlistOptions::new();

  // Nonzero scope_id (a link-local `fe80::1%scope` advertise address).
  let scoped_scope = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
    7946,
    0,
    3,
  ));
  match validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &scoped_scope, &opts) {
    Err(MemberlistError::InvalidAdvertiseAddr(e)) => {
      assert_eq!(
        e.addr(),
        scoped_scope,
        "carries the rejected advertise addr"
      );
      assert!(
        !e.reason().is_empty(),
        "carries the underlying wire-codec reason"
      );
    }
    other => panic!(
      "a scoped (nonzero scope_id) IPv6 advertise address must be rejected with InvalidAdvertiseAddr, got {other:?}"
    ),
  }

  // Nonzero flowinfo is equally non-encodable.
  let scoped_flow = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
    7946,
    1,
    0,
  ));
  assert!(
    matches!(
      validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &scoped_flow, &opts),
      Err(MemberlistError::InvalidAdvertiseAddr(_))
    ),
    "a nonzero-flowinfo IPv6 advertise address must also be rejected"
  );

  // A plain IPv4 advertise address is wire-encodable ⇒ Ok.
  let v4: SocketAddr = "127.0.0.1:7946".parse().unwrap();
  assert!(
    validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &v4, &opts).is_ok(),
    "a normal IPv4 advertise address must construct Ok"
  );

  // An UNSCOPED IPv6 advertise address (flowinfo = scope_id = 0) is the form
  // the compact wire encoder accepts ⇒ Ok.
  let v6_unscoped = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 7946, 0, 0));
  assert!(
    validate_gossip_mtu_for_identity::<smol_str::SmolStr>(&id, &v6_unscoped, &opts).is_ok(),
    "an unscoped IPv6 advertise address must construct Ok"
  );
}

// The resolved advertise address is the local node's published contact: an
// unspecified / multicast / IPv4-broadcast IP or a zero port encodes fine but
// is undialable, so `validate_advertise_addr` must reject it (the node would
// otherwise join as an unreachable member). Loopback / private / global
// unicast must stay valid. Exercised directly — it is the same function
// `Memberlist::new` calls on `transport.advertise_address()`, with no socket
// bind, so a zero port (which the post-readback address never carries) and a
// multicast/broadcast IP (which the OS would refuse to bind) can both be
// tested deterministically.
#[test]
fn validate_advertise_addr_rejects_unroutable_contacts() {
  use MemberlistError::InvalidAdvertiseAddr;

  // Unspecified (the wildcard-bind footgun): 0.0.0.0 and [::].
  for addr in ["0.0.0.0:7946", "[::]:7946"] {
    let a: SocketAddr = addr.parse().unwrap();
    assert!(
      matches!(validate_advertise_addr(&a), Err(InvalidAdvertiseAddr(e)) if e.addr() == a),
      "unspecified advertise {addr} must be rejected"
    );
  }

  // Multicast: an IPv4 group and an IPv6 link-local-all-nodes group.
  for addr in ["224.0.0.1:7946", "[ff02::1]:7946"] {
    let a: SocketAddr = addr.parse().unwrap();
    assert!(
      matches!(validate_advertise_addr(&a), Err(InvalidAdvertiseAddr(_))),
      "multicast advertise {addr} must be rejected"
    );
  }

  // IPv4 broadcast.
  let bcast: SocketAddr = "255.255.255.255:7946".parse().unwrap();
  assert!(
    matches!(
      validate_advertise_addr(&bcast),
      Err(InvalidAdvertiseAddr(_))
    ),
    "broadcast advertise must be rejected"
  );

  // Zero port (a resolved advertise should never carry it, but reject if it
  // ever surfaces) on an otherwise-valid loopback IP.
  let port0: SocketAddr = "127.0.0.1:0".parse().unwrap();
  assert!(
    matches!(
      validate_advertise_addr(&port0),
      Err(InvalidAdvertiseAddr(_))
    ),
    "a zero port must be rejected even on a routable IP"
  );

  // ACCEPTED: loopback (v4 + v6), private, and global unicast.
  for addr in [
    "127.0.0.1:7946",   // IPv4 loopback
    "10.0.0.5:7946",    // private (10/8)
    "192.168.1.7:7946", // private (192.168/16)
    "203.0.113.10:443", // global unicast (TEST-NET-3)
    "[::1]:7946",       // IPv6 loopback (unscoped)
  ] {
    let a: SocketAddr = addr.parse().unwrap();
    assert!(
      validate_advertise_addr(&a).is_ok(),
      "a usable unicast advertise {addr} must be accepted"
    );
  }
}

#[test]
fn validate_max_stream_frame_size_bounds() {
  // Unset is accepted (the machine default applies at `T::run`).
  assert!(validate_max_stream_frame_size(&MemberlistOptions::new()).is_ok());
  // Positive caps within the u32 wire envelope are accepted, INCLUDING small
  // caps — a no-snapshot node legitimately runs with small reliable frames
  // (e.g. the byte-backstop test uses 256 KiB).
  for ok in [256usize, 64 * 1024, u32::MAX as usize] {
    assert!(
      validate_max_stream_frame_size(&MemberlistOptions::new().with_max_stream_frame_size(ok))
        .is_ok(),
      "cap {ok} within the u32 wire envelope must be accepted"
    );
  }
  // Zero rejects every reliable frame — rejected fail-fast.
  assert!(matches!(
    validate_max_stream_frame_size(&MemberlistOptions::new().with_max_stream_frame_size(0)),
    Err(MemberlistError::InvalidOption(_))
  ));
  // Above the u32 wire envelope is rejected (unencodable body / unreachable
  // receive gate). Only representable where usize exceeds u32 (64-bit).
  #[cfg(target_pointer_width = "64")]
  {
    for bad in [(u32::MAX as usize) + 1, usize::MAX] {
      assert!(
        matches!(
          validate_max_stream_frame_size(&MemberlistOptions::new().with_max_stream_frame_size(bad)),
          Err(MemberlistError::InvalidOption(_))
        ),
        "cap {bad} above the u32 wire envelope must be rejected"
      );
    }
  }
}

#[test]
fn validate_initial_local_state_honors_configured_cap() {
  // A ~4 KiB snapshot frames far under the 64 MiB machine default, so with no
  // override it validates.
  let snapshot = Bytes::from(vec![0u8; 4096]);
  assert!(
    validate_initial_local_state::<smol_str::SmolStr, SocketAddr>(
      &MemberlistOptions::new().with_initial_local_state(snapshot.clone())
    )
    .is_ok()
  );
  // The SAME snapshot is rejected against a 256-byte configured ceiling —
  // proving the check honors the configured `max_stream_frame_size`, not the
  // machine default (under which 4 KiB would pass).
  assert!(matches!(
    validate_initial_local_state::<smol_str::SmolStr, SocketAddr>(
      &MemberlistOptions::new()
        .with_max_stream_frame_size(256)
        .with_initial_local_state(snapshot)
    ),
    Err(MemberlistError::Proto(_))
  ));
}

#[test]
fn validate_stream_frame_for_identity_rejects_tiny_caps() {
  let id = smol_str::SmolStr::new("sf-node");
  let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap();
  // A cap of 1 cannot carry the local node's minimal push/pull frame — the
  // node would construct Ok yet never complete a reliable membership exchange.
  assert!(matches!(
    validate_stream_frame_for_identity(
      &id,
      &addr,
      &MemberlistOptions::new().with_max_stream_frame_size(1)
    ),
    Err(MemberlistError::InvalidOption(_))
  ));
  // A small-but-realistic cap (256 KiB — the no-snapshot byte-backstop config)
  // and the machine default both genuinely fit the node, so both are accepted:
  // the floor is dynamic, not a fixed magic-number that would reject 256 KiB.
  assert!(
    validate_stream_frame_for_identity(
      &id,
      &addr,
      &MemberlistOptions::new().with_max_stream_frame_size(256 * 1024)
    )
    .is_ok()
  );
  assert!(validate_stream_frame_for_identity(&id, &addr, &MemberlistOptions::new()).is_ok());

  // The floor reserves room for a `meta_max_size`-sized meta (worst case), so
  // a cap that fits the node's CURRENT (empty) meta but NOT its meta_max_size
  // ceiling is rejected — closing the bypass where a later `update_node_metadata`
  // grows the meta past the cap. A 60 KiB ceiling needs a ~60 KiB frame even
  // with an empty initial meta, so a 50 KiB cap is rejected while a 128 KiB cap
  // (which fits the worst-case meta) is accepted.
  assert!(matches!(
    validate_stream_frame_for_identity(
      &id,
      &addr,
      &MemberlistOptions::new()
        .with_meta_max_size(60 * 1024)
        .with_max_stream_frame_size(50 * 1024)
    ),
    Err(MemberlistError::InvalidOption(_))
  ));
  assert!(
    validate_stream_frame_for_identity(
      &id,
      &addr,
      &MemberlistOptions::new()
        .with_meta_max_size(60 * 1024)
        .with_max_stream_frame_size(128 * 1024)
    )
    .is_ok()
  );
}

#[test]
fn validate_initial_meta_rejects_oversized() {
  // Unset, and an initial meta within meta_max_size, are accepted.
  assert!(validate_initial_meta(&MemberlistOptions::new()).is_ok());
  assert!(
    validate_initial_meta(
      &MemberlistOptions::new()
        .with_meta_max_size(64)
        .with_initial_meta(Meta::try_from(Bytes::from(vec![0u8; 64])).unwrap())
    )
    .is_ok()
  );
  // An initial meta larger than meta_max_size is rejected fail-fast — the
  // machine only debug-asserts this, so the release check is what keeps the
  // reliable-frame floor (sized for meta_max_size) from under-counting the
  // actual local PushPull.
  assert!(matches!(
    validate_initial_meta(
      &MemberlistOptions::new()
        .with_meta_max_size(64)
        .with_initial_meta(Meta::try_from(Bytes::from(vec![0u8; 128])).unwrap())
    ),
    Err(MemberlistError::InvalidOption(_))
  ));
}

#[test]
fn memberlist_options_default_is_all_unset() {
  let opts = MemberlistOptions::new();
  assert_eq!(opts.gossip_mtu(), None);
  assert_eq!(opts.meta_max_size(), None);
  assert_eq!(opts.max_stream_frame_size(), None);
  assert!(opts.initial_meta().is_none());
  assert!(opts.initial_local_state().is_none());
  assert_eq!(opts.label(), None);
  assert!(!opts.skip_inbound_label_check());
  // `new()` and `default()` agree on every scalar.
  let d = MemberlistOptions::default();
  assert_eq!(d.gossip_mtu(), None);
  assert_eq!(
    d.skip_inbound_label_check(),
    opts.skip_inbound_label_check()
  );
}

// Every scalar / payload `with_*` builder round-trips through its accessor.
#[test]
fn memberlist_options_builders_round_trip() {
  let meta = Meta::try_from(Bytes::from_static(b"meta")).unwrap();
  let state = Bytes::from_static(b"local-state");
  let opts = MemberlistOptions::new()
    .with_gossip_mtu(1500)
    .with_meta_max_size(256)
    .with_max_stream_frame_size(4096)
    .with_initial_meta(meta.clone())
    .with_initial_local_state(state.clone())
    .with_skip_inbound_label_check(true);

  assert_eq!(opts.gossip_mtu(), Some(1500));
  assert_eq!(opts.meta_max_size(), Some(256));
  assert_eq!(opts.max_stream_frame_size(), Some(4096));
  assert_eq!(opts.initial_meta(), Some(&meta));
  assert_eq!(opts.initial_local_state(), Some(&state));
  assert!(opts.skip_inbound_label_check());
  // Clone + Debug are derived and usable.
  assert_eq!(opts.clone().gossip_mtu(), Some(1500));
  assert!(!format!("{opts:?}").is_empty());
}

// The compression / encryption policy builders replace the stored policy;
// the accessors hand back a borrow of it.
#[test]
fn memberlist_options_compression_and_encryption_round_trip() {
  let opts = MemberlistOptions::new()
    .with_compression(CompressionOptions::new())
    .with_encryption(EncryptionOptions::new());
  // No keyring by default ⇒ encryption disabled (a usable identity policy).
  assert!(opts.encryption().keyring().is_none());
  // Accessor returns a borrow without panicking.
  let _ = opts.compression();
}

// `with_label` validates immediately, normalizes empty to `None`, and the
// accepted bytes round-trip; an over-long or non-UTF-8 label is rejected.
#[test]
fn memberlist_options_label_validation_and_round_trip() {
  let ok = MemberlistOptions::new()
    .with_label(Some(b"cluster-z".to_vec()))
    .expect("a short ASCII label is valid");
  assert_eq!(ok.label(), Some(b"cluster-z".as_slice()));

  // Empty normalizes to no label.
  let empty = MemberlistOptions::new()
    .with_label(Some(Vec::new()))
    .expect("empty label normalizes to None");
  assert_eq!(empty.label(), None);

  // Explicit `None` is no label.
  let none = MemberlistOptions::new()
    .with_label(None)
    .expect("None label is valid");
  assert_eq!(none.label(), None);

  // Over the 253-byte max is rejected.
  assert!(matches!(
    MemberlistOptions::new().with_label(Some(vec![b'x'; 254])),
    Err(MemberlistError::InvalidLabel(_))
  ));
  // Non-UTF-8 is rejected.
  assert!(matches!(
    MemberlistOptions::new().with_label(Some(vec![0xff, 0xfe])),
    Err(MemberlistError::InvalidLabel(_))
  ));
}

#[test]
fn options_admission_delegates_and_into_parts() {
  use memberlist_proto::typed::NodeState;

  // A trivial admit-all predicate, exercising the `with_alive_delegate` /
  // `with_merge_delegate` install paths.
  struct AdmitAll;
  impl AliveDelegate<smol_str::SmolStr, SocketAddr> for AdmitAll {
    fn notify_alive(&self, _peer: &NodeState<smol_str::SmolStr, SocketAddr>) -> bool {
      true
    }
  }
  impl MergeDelegate<smol_str::SmolStr, SocketAddr> for AdmitAll {
    fn notify_merge(&self, _peers: &[NodeState<smol_str::SmolStr, SocketAddr>]) -> bool {
      true
    }
  }

  let custom_ml = MemberlistOptions::new().with_gossip_mtu(1234);
  let custom_runtime = RuntimeOptions::new().with_iter_drain_cap(7);
  let opts: Options<MockTransport> = Options::new(())
    .with_memberlist(custom_ml)
    .with_runtime(custom_runtime)
    .with_alive_delegate(AdmitAll)
    .with_merge_delegate(AdmitAll);

  // Accessors reflect the installed sub-options.
  assert_eq!(opts.memberlist().gossip_mtu(), Some(1234));
  assert_eq!(opts.runtime().iter_drain_cap(), 7);

  // `into_parts` hands back the transport opts, both sub-options, and the
  // two installed predicates.
  let (_transport, ml, runtime, alive, merge) = opts.into_parts();
  assert_eq!(ml.gossip_mtu(), Some(1234));
  assert_eq!(runtime.iter_drain_cap(), 7);
  assert!(alive.is_some(), "alive predicate was installed");
  assert!(merge.is_some(), "merge predicate was installed");
}

#[test]
fn options_into_parts_defaults_have_no_delegates() {
  let opts: Options<MockTransport> = Options::new(());
  let (_transport, _ml, _runtime, alive, merge) = opts.into_parts();
  assert!(alive.is_none(), "no alive predicate by default");
  assert!(merge.is_none(), "no merge predicate by default");
}

// The identity-free `validate_gossip_mtu` enforces both ends of the
// single-datagram budget: the UDP ceiling (above which a near-MTU wire
// datagram cannot fit one packet) and the mandatory-control-packet floor.
#[test]
fn validate_gossip_mtu_enforces_floor_and_ceiling() {
  // Unset leaves the machine default ⇒ Ok.
  assert!(validate_gossip_mtu(&MemberlistOptions::new()).is_ok());

  // Values comfortably between the floor and ceiling are accepted, INCLUDING
  // the exact floor and the exact ceiling (the bounds are inclusive).
  for ok in [GOSSIP_MTU_MIN, 1400, 32 * 1024, GOSSIP_MTU_MAX] {
    assert!(
      validate_gossip_mtu(&MemberlistOptions::new().with_gossip_mtu(ok)).is_ok(),
      "mtu {ok} within [{GOSSIP_MTU_MIN}, {GOSSIP_MTU_MAX}] must be accepted"
    );
  }

  // Just above the UDP ceiling is rejected as InvalidGossipMtu, carrying the
  // offending value and the ceiling.
  match validate_gossip_mtu(&MemberlistOptions::new().with_gossip_mtu(GOSSIP_MTU_MAX + 1)) {
    Err(MemberlistError::InvalidGossipMtu(e)) => {
      assert_eq!(
        e.configured(),
        GOSSIP_MTU_MAX + 1,
        "carries the offending mtu"
      );
      assert_eq!(e.ceiling(), GOSSIP_MTU_MAX, "carries the UDP ceiling");
    }
    other => panic!("mtu above the UDP ceiling must be InvalidGossipMtu, got {other:?}"),
  }

  // Just below the control-packet floor is rejected as GossipMtuTooSmall.
  match validate_gossip_mtu(&MemberlistOptions::new().with_gossip_mtu(GOSSIP_MTU_MIN - 1)) {
    Err(MemberlistError::GossipMtuTooSmall(e)) => {
      assert_eq!(
        e.configured(),
        GOSSIP_MTU_MIN - 1,
        "carries the offending mtu"
      );
      assert_eq!(e.minimum(), GOSSIP_MTU_MIN, "carries the required floor");
    }
    other => panic!("mtu below the floor must be GossipMtuTooSmall, got {other:?}"),
  }
  // A tiny but nonzero value (1 byte) is below the floor ⇒ GossipMtuTooSmall.
  assert!(matches!(
    validate_gossip_mtu(&MemberlistOptions::new().with_gossip_mtu(1)),
    Err(MemberlistError::GossipMtuTooSmall(_))
  ));
}

// `validate_encryption_options` is a usability probe: a disabled (no keyring)
// policy is always usable, and a keyring whose every key names an AEAD whose
// backend is compiled into THIS build trial-encrypts cleanly. The
// unsupported-algorithm REJECTION arm is unreachable in this feature set
// (both `aes-gcm` and `chacha20-poly1305` are on, so
// every `SecretKey` variant has a present backend) — it would require a build
// missing one AEAD feature, i.e. fault injection at the feature level.
#[test]
fn validate_encryption_options_accepts_usable_policies() {
  use memberlist_proto::{Keyring, SecretKey};

  // No keyring ⇒ encryption disabled ⇒ always usable.
  assert!(validate_encryption_options(&EncryptionOptions::new()).is_ok());

  // A single supported AES-256-GCM primary trial-encrypts cleanly.
  let aes = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x11; 32])));
  assert!(validate_encryption_options(&aes).is_ok());

  // A ChaCha20-Poly1305 primary likewise.
  let chacha =
    EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::ChaCha20Poly1305([0x22; 32])));
  assert!(validate_encryption_options(&chacha).is_ok());

  // A mixed-cipher ring (a common key-rotation state) probes EVERY key — the
  // primary AND all secondaries — so a supported-primary + supported-secondary
  // ring is accepted only because each variant's backend is present.
  let mixed = EncryptionOptions::new().with_keyring(Keyring::with_secondaries(
    SecretKey::Aes256([0x33; 32]),
    [
      SecretKey::Aes128([0x44; 16]),
      SecretKey::ChaCha20Poly1305([0x55; 32]),
    ],
  ));
  assert!(
    validate_encryption_options(&mixed).is_ok(),
    "a mixed ring whose every key's backend is compiled in must validate"
  );
}

#[cfg(feature = "serde")]
#[test]
fn memberlist_options_serde_round_trip_and_partial() {
  // An empty config deserializes to the full default.
  assert_eq!(
    serde_json::from_str::<MemberlistOptions>("{}")
      .unwrap()
      .gossip_mtu(),
    MemberlistOptions::new().gossip_mtu()
  );
  // A round-trip preserves the serde-able scalar overrides. The opaque
  // `initial_meta` / `initial_local_state` / `label` fields are serde-skipped
  // (no string/JSON form), so they are not part of the wire shape.
  let opts = MemberlistOptions::new()
    .with_gossip_mtu(1500)
    .with_meta_max_size(256)
    .with_max_stream_frame_size(4096)
    .with_skip_inbound_label_check(true);
  let json = serde_json::to_string(&opts).unwrap();
  let back: MemberlistOptions = serde_json::from_str(&json).unwrap();
  assert_eq!(back.gossip_mtu(), Some(1500));
  assert_eq!(back.meta_max_size(), Some(256));
  assert_eq!(back.max_stream_frame_size(), Some(4096));
  assert!(back.skip_inbound_label_check());
  // A partial config overrides one field and defaults the rest.
  let partial: MemberlistOptions = serde_json::from_str(r#"{"gossip_mtu": 1234}"#).unwrap();
  assert_eq!(partial.gossip_mtu(), Some(1234));
  assert_eq!(partial.meta_max_size(), None);
  assert!(!partial.skip_inbound_label_check());
}

#[cfg(feature = "serde")]
#[test]
fn memberlist_options_serde_rejects_unknown_field() {
  // A misspelled knob (`gosip_mtu` for `gossip_mtu`) must be rejected rather
  // than silently dropped — a typo'd field would otherwise leave that knob at
  // its default with no warning.
  assert!(serde_json::from_str::<MemberlistOptions>(r#"{"gosip_mtu": 1234}"#).is_err());
}

#[cfg(feature = "clap")]
#[test]
fn memberlist_options_clap_parses_and_wires_env() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    memberlist: MemberlistOptions,
  }

  // The `Option<usize>` and `bool` flags parse; the opaque fields have none.
  let cli = Cli::try_parse_from([
    "app",
    "--memberlist-gossip-mtu",
    "1500",
    "--memberlist-skip-inbound-label-check",
  ])
  .unwrap();
  assert_eq!(cli.memberlist.gossip_mtu(), Some(1500));
  assert!(cli.memberlist.skip_inbound_label_check());
  // Unspecified leaves the gossip-mtu override unset.
  assert_eq!(
    Cli::try_parse_from(["app"])
      .unwrap()
      .memberlist
      .gossip_mtu(),
    None
  );
  // The env var is wired — assert via command introspection, never `set_var`.
  let cmd = Cli::command();
  let arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "memberlist-gossip-mtu")
    .expect("memberlist-gossip-mtu arg is registered");
  assert_eq!(
    arg.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_GOSSIP_MTU")
  );
}

#[cfg(feature = "serde")]
#[test]
fn memberlist_options_label_serde_round_trips() {
  // A configured label round-trips as a string and is actually set.
  let opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid label");
  let json = serde_json::to_string(&opts).unwrap();
  let back: MemberlistOptions = serde_json::from_str(&json).unwrap();
  assert_eq!(back.label(), Some(b"cluster-x".as_slice()));

  // A config carrying a label sets it (the accessor is `Some`, not `None`).
  let from_cfg: MemberlistOptions = serde_json::from_str(r#"{"label": "prod"}"#).unwrap();
  assert_eq!(from_cfg.label(), Some(b"prod".as_slice()));

  // An absent or empty label normalizes to no label.
  assert_eq!(
    serde_json::from_str::<MemberlistOptions>("{}")
      .unwrap()
      .label(),
    None
  );
  assert_eq!(
    serde_json::from_str::<MemberlistOptions>(r#"{"label": ""}"#)
      .unwrap()
      .label(),
    None
  );
}

#[cfg(feature = "serde")]
#[test]
fn memberlist_options_label_serde_rejects_too_long() {
  // A >253-byte label is rejected on deserialize rather than silently dropped.
  let long = "x".repeat(254);
  let json = format!(r#"{{"label": "{long}"}}"#);
  assert!(
    serde_json::from_str::<MemberlistOptions>(&json).is_err(),
    "a label exceeding 253 bytes must be rejected by serde"
  );
}

#[cfg(feature = "clap")]
#[test]
fn memberlist_options_label_clap_parses_and_rejects_too_long() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    memberlist: MemberlistOptions,
  }

  // A valid label parses and is set.
  let cli = Cli::try_parse_from(["app", "--memberlist-label", "cluster-x"]).unwrap();
  assert_eq!(cli.memberlist.label(), Some(b"cluster-x".as_slice()));

  // A >253-byte label is rejected by the value parser.
  let long = "x".repeat(254);
  assert!(
    Cli::try_parse_from(["app", "--memberlist-label", &long]).is_err(),
    "a label exceeding 253 bytes must be rejected by clap"
  );

  // The env var is wired.
  let cmd = Cli::command();
  let arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "memberlist-label")
    .expect("memberlist-label arg is registered");
  assert_eq!(
    arg.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_LABEL")
  );
}

// A partial `try_update_from` carrying one unrelated flag must NOT reset the
// defaulted own field (`skip_inbound_label_check`) nor — through the flattened
// children — a child's seeded knob. clap's `default_value` makes an unset arg
// look "present" in update mode; the value-source gate in the manual
// `update_from_arg_matches` (and the delegation to each child's own
// value-source-correct update) is what keeps seeded values alive across the
// update.
#[cfg(feature = "clap")]
#[test]
fn memberlist_options_partial_update_preserves_unset_fields() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    o: MemberlistOptions,
  }

  // Seed a non-default value on the defaulted own field and on an unset
  // `Option` field, plus a child knob (compression algorithm), then run a
  // partial update supplying ONE unrelated own flag.
  let seeded = MemberlistOptions::new()
    .with_skip_inbound_label_check(true)
    .with_meta_max_size(4242);
  #[cfg(compression)]
  let seeded = seeded.with_compression(
    CompressionOptions::new().with_algorithm(memberlist_proto::CompressAlgorithm::Lz4),
  );
  let mut cli = Cli { o: seeded };

  cli
    .try_update_from(["app", "--memberlist-gossip-mtu", "1500"])
    .expect("partial update parses");

  // The supplied flag is applied.
  assert_eq!(cli.o.gossip_mtu(), Some(1500));
  // The defaulted own field SURVIVES the partial update (the bug under test).
  assert!(
    cli.o.skip_inbound_label_check(),
    "skip_inbound_label_check must survive an unrelated partial update"
  );
  // The seeded `Option` field survives too.
  assert_eq!(cli.o.meta_max_size(), Some(4242));
  // The seeded child knob survives the partial parent update.
  #[cfg(compression)]
  assert_eq!(
    cli.o.compression().algorithm(),
    Some(memberlist_proto::CompressAlgorithm::Lz4),
    "a flattened child's seeded knob must survive a partial parent update"
  );
}

// An explicit override on update IS applied — both an own defaulted field and a
// flattened child field.
#[cfg(feature = "clap")]
#[test]
fn memberlist_options_update_applies_explicit_override() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    o: MemberlistOptions,
  }

  // Seed the defaulted flag at its default (false), then supply it on update:
  // the SetTrue flag flips it to true, proving the explicit override is applied
  // through the value-source gate.
  let mut cli = Cli {
    o: MemberlistOptions::new(),
  };
  cli
    .try_update_from([
      "app",
      "--memberlist-skip-inbound-label-check",
      "--memberlist-gossip-mtu",
      "1600",
    ])
    .expect("explicit override parses");
  assert!(cli.o.skip_inbound_label_check());
  assert_eq!(cli.o.gossip_mtu(), Some(1600));

  // A child flag supplied on update is applied via the delegated child update.
  #[cfg(compression)]
  {
    let mut cli = Cli {
      o: MemberlistOptions::new(),
    };
    cli
      .try_update_from(["app", "--compression-algorithm", "lz4"])
      .expect("child override parses");
    assert_eq!(
      cli.o.compression().algorithm(),
      Some(memberlist_proto::CompressAlgorithm::Lz4)
    );
  }
}
