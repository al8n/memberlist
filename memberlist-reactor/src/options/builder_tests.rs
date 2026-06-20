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
fn validate_runtime_options_rejects_zero_capacity_observation_channel() {
  use crate::{error::Error, memberlist::validate_runtime_options};

  // Defaults are accepted.
  assert!(validate_runtime_options(&RuntimeOptions::new()).is_ok());
  // A Bounded(0) observation channel is a rendezvous the driver's try_send can
  // never deposit into — rejected as InvalidOption.
  assert!(matches!(
    validate_runtime_options(&RuntimeOptions::new().with_observation_channel(Channel::Bounded(0))),
    Err(Error::InvalidOption(_))
  ));
  // A nonzero bound and an unbounded channel are both accepted.
  assert!(
    validate_runtime_options(&RuntimeOptions::new().with_observation_channel(Channel::Bounded(16)))
      .is_ok()
  );
  assert!(
    validate_runtime_options(&RuntimeOptions::new().with_observation_channel(Channel::Unbounded))
      .is_ok()
  );
  // A zero recv_batch / event_stream_capacity is NOT rejected: the driver clamps
  // the batch at its use site, and a zero-capacity EventStream degrades the
  // subscriber stream without breaking the node.
  assert!(validate_runtime_options(&RuntimeOptions::new().with_recv_batch(0)).is_ok());
  assert!(validate_runtime_options(&RuntimeOptions::new().with_event_stream_capacity(0)).is_ok());
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
fn runtime_options_overrides_round_trip() {
  let opts = RuntimeOptions::new()
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
fn runtime_options_default_tuning() {
  let opts = RuntimeOptions::default();
  assert_eq!(opts.observation_channel(), Channel::Bounded(1024));
  assert_eq!(opts.event_stream_capacity(), 1024);
  assert_eq!(opts.recv_batch(), 64);
  assert_eq!(opts.transmit_batch(), 64);
  assert_eq!(opts.join_deadline(), Duration::from_secs(10));
  assert_eq!(opts.close_timeout(), Duration::from_secs(10));
}

/// `MemberlistOptions::new` matches `default`, and `RuntimeOptions::new` matches
/// `default` (the `new` delegations).
#[test]
fn new_delegates_to_default() {
  assert_eq!(MemberlistOptions::new().gossip_mtu(), None);
  assert_eq!(
    RuntimeOptions::new().observation_channel(),
    RuntimeOptions::default().observation_channel()
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

/// `Options::new` defaults to no SWIM/runtime overrides and no admission
/// delegates; the `with_memberlist` / `with_runtime` setters and `memberlist()`
/// / `runtime()` accessors round-trip.
#[test]
fn options_builder_round_trips() {
  let ml = MemberlistOptions::new().with_gossip_mtu(900);
  let drv = RuntimeOptions::new().with_recv_batch(8);
  let opts: Options<SmolStr> = Options::new().with_memberlist(ml).with_runtime(drv);
  assert_eq!(opts.memberlist().gossip_mtu(), Some(900));
  assert_eq!(opts.runtime().recv_batch(), 8);

  // The Default impl leaves both at their own defaults.
  let def: Options<SmolStr> = Options::default();
  assert_eq!(def.memberlist().gossip_mtu(), None);
  assert_eq!(def.runtime().recv_batch(), 64);
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
    .with_runtime(RuntimeOptions::new().with_transmit_batch(4))
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

/// `Channel` parses from a string: `"unbounded"` and a bare integer round-trip
/// through `FromStr`/`Display`; anything else is a `ParseChannelError`.
#[test]
fn channel_from_str_and_display_round_trip() {
  assert_eq!("unbounded".parse::<Channel>().unwrap(), Channel::Unbounded);
  assert_eq!("2048".parse::<Channel>().unwrap(), Channel::Bounded(2048));
  assert_eq!(Channel::Unbounded.to_string(), "unbounded");
  assert_eq!(Channel::Bounded(2048).to_string(), "2048");
  // The Display form re-parses to the same value.
  for ch in [Channel::Unbounded, Channel::Bounded(7)] {
    assert_eq!(ch.to_string().parse::<Channel>().unwrap(), ch);
  }
  assert!("nope".parse::<Channel>().is_err());
}

#[cfg(feature = "serde")]
#[test]
fn channel_serde_is_tagged_snake_case() {
  // The data variant is a one-key snake_case map; the unit variant a bare string.
  assert_eq!(
    serde_json::to_string(&Channel::Bounded(1024)).unwrap(),
    r#"{"bounded":1024}"#
  );
  assert_eq!(
    serde_json::to_string(&Channel::Unbounded).unwrap(),
    r#""unbounded""#
  );
  for ch in [Channel::Bounded(512), Channel::Unbounded] {
    let json = serde_json::to_string(&ch).unwrap();
    assert_eq!(serde_json::from_str::<Channel>(&json).unwrap(), ch);
  }
}

#[cfg(feature = "serde")]
#[test]
fn memberlist_options_serde_round_trip_and_partial() {
  // An empty config deserializes to the full default (every override unset).
  let from_empty = serde_json::from_str::<MemberlistOptions>("{}").unwrap();
  assert_eq!(from_empty.gossip_mtu(), None);
  assert_eq!(from_empty.meta_max_size(), None);
  assert!(!from_empty.skip_inbound_label_check());

  // A configured subset round-trips, and an omitted field stays default.
  let opts = MemberlistOptions::new()
    .with_gossip_mtu(1400)
    .with_skip_inbound_label_check(true);
  let json = serde_json::to_string(&opts).unwrap();
  let back = serde_json::from_str::<MemberlistOptions>(&json).unwrap();
  assert_eq!(back.gossip_mtu(), Some(1400));
  assert!(back.skip_inbound_label_check());
  assert_eq!(back.meta_max_size(), None);

  // A partial config fills the rest from the default.
  let partial = serde_json::from_str::<MemberlistOptions>(r#"{"meta_max_size":512}"#).unwrap();
  assert_eq!(partial.meta_max_size(), Some(512));
  assert_eq!(partial.gossip_mtu(), None);
}

#[cfg(feature = "serde")]
#[test]
fn memberlist_options_serde_rejects_unknown_field() {
  // A misspelled knob (`gosip_mtu` for `gossip_mtu`) must be rejected rather
  // than silently dropped — a typo'd field would otherwise leave that knob at
  // its default with no warning.
  assert!(serde_json::from_str::<MemberlistOptions>(r#"{"gosip_mtu":1400}"#).is_err());
}

#[cfg(feature = "serde")]
#[test]
fn runtime_options_serde_rejects_unknown_field() {
  // A misspelled `recv_batch` must be rejected, not silently dropped.
  assert!(serde_json::from_str::<RuntimeOptions>(r#"{"rcv_batch":16}"#).is_err());
}

#[cfg(feature = "serde")]
#[test]
fn runtime_options_serde_round_trip_and_partial() {
  // `{}` is the full default.
  let from_empty = serde_json::from_str::<RuntimeOptions>("{}").unwrap();
  assert_eq!(from_empty.observation_channel(), Channel::default());
  assert_eq!(
    from_empty.event_stream_capacity(),
    DEFAULT_EVENT_STREAM_CAPACITY
  );
  assert_eq!(from_empty.join_deadline(), DEFAULT_JOIN_DEADLINE);

  let opts = RuntimeOptions::new()
    .with_observation_channel(Channel::Unbounded)
    .with_recv_batch(8)
    .with_join_deadline(Duration::from_millis(2500));
  let json = serde_json::to_string(&opts).unwrap();
  // The `Duration` field serializes via humantime, not as a struct.
  assert!(
    json.contains("2s 500ms") || json.contains("2500ms"),
    "json = {json}"
  );
  let back = serde_json::from_str::<RuntimeOptions>(&json).unwrap();
  assert_eq!(back.observation_channel(), Channel::Unbounded);
  assert_eq!(back.recv_batch(), 8);
  assert_eq!(back.join_deadline(), Duration::from_millis(2500));

  // A partial config overrides one field and defaults the rest.
  let partial = serde_json::from_str::<RuntimeOptions>(r#"{"transmit_batch":16}"#).unwrap();
  assert_eq!(partial.transmit_batch(), 16);
  assert_eq!(partial.recv_batch(), DEFAULT_RECV_BATCH);
  assert_eq!(partial.close_timeout(), DEFAULT_CLOSE_TIMEOUT);
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

  // The scalar flags parse; the bool is a flag.
  let cli = Cli::try_parse_from([
    "app",
    "--memberlist-gossip-mtu",
    "1400",
    "--memberlist-skip-inbound-label-check",
  ])
  .unwrap();
  assert_eq!(cli.memberlist.gossip_mtu(), Some(1400));
  assert!(cli.memberlist.skip_inbound_label_check());
  // Unspecified stays default.
  let bare = Cli::try_parse_from(["app"]).unwrap();
  assert_eq!(bare.memberlist.gossip_mtu(), None);
  assert!(!bare.memberlist.skip_inbound_label_check());

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

#[cfg(feature = "clap")]
#[test]
fn runtime_options_clap_parses_and_wires_env() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    runtime: RuntimeOptions,
  }

  // The `Channel` flag parses through its `FromStr`; the `Duration` flag through
  // humantime; unspecified fields fall back to their defaults.
  let cli = Cli::try_parse_from([
    "app",
    "--runtime-observation-channel",
    "unbounded",
    "--runtime-recv-batch",
    "8",
    "--runtime-join-deadline",
    "5s",
  ])
  .unwrap();
  assert_eq!(cli.runtime.observation_channel(), Channel::Unbounded);
  assert_eq!(cli.runtime.recv_batch(), 8);
  assert_eq!(cli.runtime.join_deadline(), Duration::from_secs(5));
  // Unspecified fields keep their `default_value` (== `new()`'s default).
  let bare = Cli::try_parse_from(["app"]).unwrap();
  assert_eq!(bare.runtime.observation_channel(), Channel::default());
  assert_eq!(
    bare.runtime.event_stream_capacity(),
    DEFAULT_EVENT_STREAM_CAPACITY
  );
  assert_eq!(bare.runtime.close_timeout(), DEFAULT_CLOSE_TIMEOUT);

  // The env var is wired for both a scalar and the `Duration` flag.
  let cmd = Cli::command();
  let recv = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "runtime-recv-batch")
    .expect("runtime-recv-batch arg is registered");
  assert_eq!(
    recv.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_RUNTIME_RECV_BATCH")
  );
  let deadline = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "runtime-join-deadline")
    .expect("runtime-join-deadline arg is registered");
  assert_eq!(
    deadline.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_RUNTIME_JOIN_DEADLINE")
  );
}

#[cfg(feature = "clap")]
#[test]
fn runtime_options_clap_update_preserves_unoverridden_fields() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    runtime: RuntimeOptions,
  }

  let base = || {
    RuntimeOptions::new()
      .with_recv_batch(8)
      .with_close_timeout(Duration::from_secs(45))
  };

  // A partial update (only the join deadline) must leave every other field
  // intact — clap's `default_value`/`default_value_t` must not reset the unset
  // defaulted scalars back to their defaults.
  let mut cli = Cli { runtime: base() };
  cli
    .try_update_from(["app", "--runtime-join-deadline", "3s"])
    .expect("update");
  assert_eq!(
    cli.runtime.join_deadline(),
    Duration::from_secs(3),
    "the supplied override is applied"
  );
  assert_eq!(
    cli.runtime.recv_batch(),
    8,
    "non-default recv_batch survives"
  );
  assert_eq!(
    cli.runtime.close_timeout(),
    Duration::from_secs(45),
    "non-default close_timeout survives"
  );

  // An update carrying no override at all leaves every field untouched.
  let mut cli2 = Cli { runtime: base() };
  cli2.try_update_from(["app"]).expect("update");
  assert_eq!(
    cli2.runtime.recv_batch(),
    8,
    "recv_batch survives a no-op update"
  );
  assert_eq!(
    cli2.runtime.close_timeout(),
    Duration::from_secs(45),
    "close_timeout survives a no-op update"
  );

  // An explicitly-supplied override IS still applied.
  let mut cli3 = Cli { runtime: base() };
  cli3
    .try_update_from(["app", "--runtime-recv-batch", "32"])
    .expect("update");
  assert_eq!(
    cli3.runtime.recv_batch(),
    32,
    "an explicit override is applied"
  );
}

#[cfg(feature = "clap")]
#[test]
fn memberlist_options_clap_update_preserves_unoverridden_fields() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    memberlist: MemberlistOptions,
  }

  let base = || {
    let opts = MemberlistOptions::new()
      .with_gossip_mtu(1400)
      .with_skip_inbound_label_check(true)
      .with_label(Some(b"team-a".to_vec()))
      .expect("valid label");
    #[cfg(compression)]
    let opts = opts.with_compression(
      crate::CompressionOptions::new().with_algorithm(crate::CompressAlgorithm::Lz4),
    );
    opts
  };

  // A partial update (only the gossip MTU) must leave every other field intact,
  // including the flattened child options — an unrelated parent flag must not
  // reset them via clap's defaulted-arg-looks-present behavior.
  let mut cli = Cli { memberlist: base() };
  cli
    .try_update_from(["app", "--memberlist-gossip-mtu", "1500"])
    .expect("update");
  assert_eq!(
    cli.memberlist.gossip_mtu(),
    Some(1500),
    "the supplied override is applied"
  );
  assert!(
    cli.memberlist.skip_inbound_label_check(),
    "non-default skip_inbound_label_check survives"
  );
  assert_eq!(
    cli.memberlist.label(),
    Some(b"team-a".as_slice()),
    "non-default label survives"
  );
  #[cfg(compression)]
  assert_eq!(
    cli.memberlist.compression().algorithm(),
    Some(crate::CompressAlgorithm::Lz4),
    "the flattened compression child survives a partial parent update"
  );

  // An explicitly-supplied override IS still applied.
  let mut cli2 = Cli { memberlist: base() };
  cli2
    .try_update_from(["app", "--memberlist-meta-max-size", "9000"])
    .expect("update");
  assert_eq!(
    cli2.memberlist.meta_max_size(),
    Some(9000),
    "an explicit override is applied"
  );
  assert_eq!(
    cli2.memberlist.gossip_mtu(),
    Some(1400),
    "the unrelated non-default gossip_mtu survives"
  );
}
