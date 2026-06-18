use smol_str::SmolStr;

use super::*;

#[test]
fn memberlist_options_overrides_round_trip() {
  let meta = Meta::try_from(Bytes::from_static(b"meta")).expect("valid meta");
  let state = Bytes::from_static(b"local-state");
  let opts = MemberlistOptions::new()
    .with_gossip_mtu(1400)
    .with_meta_max_size(512)
    .with_max_stream_frame_size(65_536)
    .with_initial_meta(meta.clone())
    .with_initial_local_state(state.clone());

  assert_eq!(opts.gossip_mtu(), Some(1400));
  assert_eq!(opts.meta_max_size(), Some(512));
  assert_eq!(opts.max_stream_frame_size(), Some(65_536));
  assert_eq!(opts.initial_meta(), Some(&meta));
  assert_eq!(opts.initial_local_state(), Some(&state));
}

#[test]
fn memberlist_options_default_leaves_every_override_unset() {
  let opts = MemberlistOptions::default();
  assert_eq!(opts.gossip_mtu(), None);
  assert_eq!(opts.meta_max_size(), None);
  assert_eq!(opts.max_stream_frame_size(), None);
  assert_eq!(opts.initial_meta(), None);
  assert_eq!(opts.initial_local_state(), None);
  assert_eq!(opts.label(), None);
  assert!(!opts.skip_inbound_label_check());
  // Exercise the compression/checksum/encryption accessors (machine defaults).
  let _compression = opts.compression();
  let _checksum = opts.checksum();
  let _encryption = opts.encryption();
}

#[test]
fn memberlist_options_compression_encryption_builders() {
  let opts = MemberlistOptions::new()
    .with_compression(CompressionOptions::default())
    .with_encryption(EncryptionOptions::default());
  let _compression = opts.compression();
  let _encryption = opts.encryption();
}

/// `with_checksum` sets the gossip (unreliable) checksum field; the accessor
/// reflects the override. Checksumming is gossip-plane only — the reliable
/// stream path carries no checksum — so this configures just the one field.
#[test]
fn memberlist_options_with_checksum_sets_field() {
  use memberlist_proto::ChecksumAlgorithm;
  let configured = ChecksumOptions::default().with_algorithm(ChecksumAlgorithm::Crc32);
  let opts = MemberlistOptions::new().with_checksum(configured);
  assert_eq!(opts.checksum().algorithm(), Some(ChecksumAlgorithm::Crc32));

  // The default leaves checksumming disabled (no algorithm selected).
  assert_eq!(MemberlistOptions::new().checksum().algorithm(), None);
}

#[test]
fn channel_default_is_bounded_1024() {
  assert_eq!(Channel::default(), Channel::Bounded(1024));
  assert_ne!(Channel::Bounded(1024), Channel::Unbounded);
}

#[test]
fn driver_options_overrides_round_trip() {
  let opts = DriverOptions::new()
    .with_observation_channel(Channel::Unbounded)
    .with_event_stream_capacity(2048)
    .with_recv_batch(32)
    .with_transmit_batch(16)
    .with_join_deadline(Duration::from_secs(5))
    .with_close_timeout(Duration::from_secs(3));

  assert_eq!(opts.observation_channel(), Channel::Unbounded);
  assert_eq!(opts.event_stream_capacity(), 2048);
  assert_eq!(opts.recv_batch(), 32);
  assert_eq!(opts.transmit_batch(), 16);
  assert_eq!(opts.join_deadline(), Duration::from_secs(5));
  assert_eq!(opts.close_timeout(), Duration::from_secs(3));
}

#[test]
fn driver_options_default_tuning() {
  let opts = DriverOptions::default();
  assert_eq!(opts.observation_channel(), Channel::Bounded(1024));
  assert_eq!(opts.event_stream_capacity(), 1024);
  assert_eq!(opts.recv_batch(), 64);
  assert_eq!(opts.transmit_batch(), 64);
  assert_eq!(opts.join_deadline(), Duration::from_secs(10));
  assert_eq!(opts.close_timeout(), Duration::from_secs(10));
}

/// `MemberlistOptions::new` matches `default`, and `DriverOptions::new` matches
/// `default` (the `new` delegations).
#[test]
fn new_delegates_to_default() {
  assert_eq!(MemberlistOptions::new().gossip_mtu(), None);
  assert_eq!(
    DriverOptions::new().observation_channel(),
    DriverOptions::default().observation_channel()
  );
}

/// `Channel` is `Copy`/`Debug`; `Bounded` and `Unbounded` are distinct and
/// each round-trips its discriminant.
#[test]
fn channel_variants_and_debug() {
  let bounded = Channel::Bounded(7);
  let copy = bounded;
  assert_eq!(copy, Channel::Bounded(7));
  assert_ne!(Channel::Bounded(7), Channel::Bounded(8));
  assert!(!format!("{:?}", Channel::Unbounded).is_empty());
}

/// `Options::new` defaults to no SWIM/driver overrides and no admission
/// delegates; the `with_memberlist` / `with_driver` setters and `memberlist()`
/// / `driver()` accessors round-trip.
#[test]
fn options_builder_round_trips() {
  let ml = MemberlistOptions::new().with_gossip_mtu(900);
  let drv = DriverOptions::new().with_recv_batch(8);
  let opts: Options<SmolStr> = Options::new().with_memberlist(ml).with_driver(drv);
  assert_eq!(opts.memberlist().gossip_mtu(), Some(900));
  assert_eq!(opts.driver().recv_batch(), 8);

  // The Default impl leaves both at their own defaults.
  let def: Options<SmolStr> = Options::default();
  assert_eq!(def.memberlist().gossip_mtu(), None);
  assert_eq!(def.driver().recv_batch(), 64);
}

/// The admission-delegate setters install the predicates, and `into_parts`
/// hands every component back to the backend constructor — including the boxed
/// admission delegates, whose predicates still fire through the box.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
#[test]
fn options_into_parts_carries_admission_delegates() {
  use memberlist_proto::typed::{NodeState, State};

  struct DenyAlive;
  impl AliveDelegate<SmolStr, SocketAddr> for DenyAlive {
    fn notify_alive(&self, _peer: &NodeState<SmolStr, SocketAddr>) -> bool {
      false
    }
  }
  struct AllowMerge;
  impl MergeDelegate<SmolStr, SocketAddr> for AllowMerge {
    fn notify_merge(&self, _peers: &[NodeState<SmolStr, SocketAddr>]) -> bool {
      true
    }
  }

  let opts: Options<SmolStr> = Options::new()
    .with_memberlist(MemberlistOptions::new().with_meta_max_size(64))
    .with_driver(DriverOptions::new().with_transmit_batch(4))
    .with_alive_delegate(DenyAlive)
    .with_merge_delegate(AllowMerge);

  let (ml, drv, alive, merge, _cidr) = opts.into_parts();
  assert_eq!(ml.meta_max_size(), Some(64));
  assert_eq!(drv.transmit_batch(), 4);
  let alive = alive.expect("alive delegate installed");
  let merge = merge.expect("merge delegate installed");

  let node = NodeState::new(
    SmolStr::new("p"),
    SocketAddr::from(([127, 0, 0, 1], 1)),
    State::Alive,
  );
  assert!(
    !alive.notify_alive(&node),
    "the installed alive predicate denies admission"
  );
  assert!(
    merge.notify_merge(std::slice::from_ref(&node)),
    "the installed merge predicate allows the merge"
  );
}

/// With no admission delegates installed, `into_parts` yields `None` for both.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
#[test]
fn options_into_parts_without_delegates_is_none() {
  let opts: Options<SmolStr> = Options::new();
  let (_ml, _drv, alive, merge, _cidr) = opts.into_parts();
  assert!(alive.is_none(), "no alive delegate by default");
  assert!(merge.is_none(), "no merge delegate by default");
}
