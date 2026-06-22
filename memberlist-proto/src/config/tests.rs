use super::EndpointOptions;

#[test]
fn initial_incarnation_default_is_one() {
  let opts = EndpointOptions::<(), ()>::new((), ());
  assert_eq!(opts.initial_incarnation(), 1);
}

#[test]
fn with_initial_incarnation_accepts_lower_half() {
  // A small seed (the realistic case) and the exact upper boundary of the
  // accepted lower half both round-trip.
  let small = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(7);
  assert_eq!(small.initial_incarnation(), 7);
  let boundary = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX / 2);
  assert_eq!(boundary.initial_incarnation(), u32::MAX / 2);
}

#[test]
#[should_panic(expected = "lower half")]
fn with_initial_incarnation_rejects_upper_half() {
  // Just past the midpoint is rejected — the smallest value outside the
  // reserved headroom.
  let _ = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX / 2 + 1);
}

#[test]
#[should_panic(expected = "lower half")]
fn with_initial_incarnation_rejects_near_max() {
  // `u32::MAX - 1` is only two bumps from wrapping; the previous MAX-only
  // guard wrongly accepted it.
  let _ = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX - 1);
}

#[test]
#[should_panic(expected = "lower half")]
fn with_initial_incarnation_rejects_u32_max() {
  let _ = EndpointOptions::<(), ()>::new((), ()).with_initial_incarnation(u32::MAX);
}

#[test]
fn every_builder_round_trips_through_its_accessor() {
  use core::time::Duration;
  use std::net::SocketAddr;

  use bytes::Bytes;
  use smol_str::SmolStr;

  use crate::typed::{DelegateVersion, Meta, ProtocolVersion};

  let local = SmolStr::new("local");
  let advertise = SocketAddr::from(([127, 0, 0, 1], 7946));
  let meta = Meta::try_from(Bytes::from_static(b"meta")).expect("valid meta");

  let opts = EndpointOptions::<SmolStr, SocketAddr>::new(local.clone(), advertise)
    .with_initial_meta(meta.clone())
    .with_initial_local_state(Bytes::from_static(b"state"))
    .with_suspicion_mult(5)
    .with_suspicion_max_timeout_mult(7)
    .with_probe_interval(Duration::from_millis(100))
    .with_gossip_to_the_dead_time(Duration::from_secs(30))
    .with_dead_node_reclaim_time(Duration::from_secs(60))
    .with_awareness_max_multiplier(9)
    .with_indirect_checks(4)
    .with_probe_timeout(Duration::from_millis(250))
    .with_stream_timeout(Duration::from_secs(15))
    .with_max_stream_frame_size(70_000)
    .with_gossip_mtu(1300)
    .with_gossip_interval(Duration::from_millis(200))
    .with_gossip_nodes(3)
    .with_meta_max_size(400)
    .with_accept_handshake_deadline(Duration::from_secs(11))
    .with_push_pull_interval(Duration::from_secs(45))
    .with_retransmit_mult(6)
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1);

  assert_eq!(opts.local_id_ref(), &local);
  assert_eq!(opts.advertise_addr_ref(), &advertise);
  assert_eq!(opts.initial_meta_ref(), &meta);
  assert_eq!(opts.initial_local_state(), b"state");
  assert_eq!(opts.initial_local_state_bytes().as_ref(), b"state");
  assert_eq!(opts.suspicion_mult(), 5);
  assert_eq!(opts.suspicion_max_timeout_mult(), 7);
  assert_eq!(opts.probe_interval(), Duration::from_millis(100));
  assert_eq!(opts.gossip_to_the_dead_time(), Duration::from_secs(30));
  assert_eq!(opts.dead_node_reclaim_time(), Duration::from_secs(60));
  assert_eq!(opts.awareness_max_multiplier(), 9);
  assert_eq!(opts.indirect_checks(), 4);
  assert_eq!(opts.probe_timeout(), Duration::from_millis(250));
  assert_eq!(opts.stream_timeout(), Duration::from_secs(15));
  assert_eq!(opts.max_stream_frame_size(), 70_000);
  assert_eq!(opts.gossip_mtu(), 1300);
  assert_eq!(opts.gossip_interval(), Duration::from_millis(200));
  assert_eq!(opts.gossip_nodes(), 3);
  assert_eq!(opts.meta_max_size(), 400);
  assert_eq!(opts.accept_handshake_deadline(), Duration::from_secs(11));
  assert_eq!(opts.push_pull_interval(), Duration::from_secs(45));
  assert_eq!(opts.retransmit_mult(), 6);
  assert_eq!(opts.protocol_version(), ProtocolVersion::V1);
  assert_eq!(opts.delegate_version(), DelegateVersion::V1);
}

#[test]
fn max_indirect_forwards_and_ack_payload_round_trip() {
  // These two builders sit between the ones the omnibus round-trip exercises and
  // are otherwise unreached: both flip a value away from its default and the
  // accessor must observe it.
  let opts = EndpointOptions::<(), ()>::new((), ())
    .with_max_indirect_forwards(17)
    .with_ack_payload_to_members_only(true);
  assert_eq!(opts.max_indirect_forwards(), 17);
  assert!(opts.ack_payload_to_members_only());

  // The other side of the bool, from a default that is the opposite value.
  let off = EndpointOptions::<(), ()>::new((), ()).with_ack_payload_to_members_only(false);
  assert!(!off.ack_payload_to_members_only());
}

#[test]
fn map_advertise_transforms_addr_and_preserves_every_other_field() {
  use core::time::Duration;
  use std::net::SocketAddr;

  use bytes::Bytes;
  use smol_str::SmolStr;

  use crate::typed::{DelegateVersion, Meta, ProtocolVersion};

  let local = SmolStr::new("local");
  // The unresolved advertise address is a string; `map_advertise` resolves it to
  // a concrete `SocketAddr`, exactly the driver-layer use the method documents.
  let advertise = "127.0.0.1:7946".to_string();
  let meta = Meta::try_from(Bytes::from_static(b"meta")).expect("valid meta");

  // Set every non-default knob so the field-by-field carry-over is observable: a
  // dropped field would revert to a default that differs from these.
  let opts = EndpointOptions::<SmolStr, String>::new(local.clone(), advertise.clone())
    .with_initial_meta(meta.clone())
    .with_initial_local_state(Bytes::from_static(b"state"))
    .with_suspicion_mult(5)
    .with_suspicion_max_timeout_mult(7)
    .with_probe_interval(Duration::from_millis(100))
    .with_gossip_to_the_dead_time(Duration::from_secs(30))
    .with_dead_node_reclaim_time(Duration::from_secs(60))
    .with_awareness_max_multiplier(9)
    .with_indirect_checks(4)
    .with_max_indirect_forwards(33)
    .with_max_members(Some(500))
    .with_ack_payload_to_members_only(true)
    .with_max_inbound_streams(Some(64))
    .with_probe_timeout(Duration::from_millis(250))
    .with_stream_timeout(Duration::from_secs(15))
    .with_max_stream_frame_size(70_000)
    .with_gossip_mtu(1300)
    .with_gossip_interval(Duration::from_millis(200))
    .with_gossip_nodes(3)
    .with_meta_max_size(400)
    .with_accept_handshake_deadline(Duration::from_secs(11))
    .with_push_pull_interval(Duration::from_secs(45))
    .with_retransmit_mult(6)
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1)
    .with_initial_incarnation(12);

  let expected: SocketAddr = advertise.parse().unwrap();
  let mapped: EndpointOptions<SmolStr, SocketAddr> =
    opts.map_advertise(|s| s.parse::<SocketAddr>().unwrap());

  // The mapped field carries the function's output.
  assert_eq!(mapped.advertise_addr_ref(), &expected);

  // Every other field survives the rebuild unchanged.
  assert_eq!(mapped.local_id_ref(), &local);
  assert_eq!(mapped.initial_meta_ref(), &meta);
  assert_eq!(mapped.initial_local_state(), b"state");
  assert_eq!(mapped.suspicion_mult(), 5);
  assert_eq!(mapped.suspicion_max_timeout_mult(), 7);
  assert_eq!(mapped.probe_interval(), Duration::from_millis(100));
  assert_eq!(mapped.gossip_to_the_dead_time(), Duration::from_secs(30));
  assert_eq!(mapped.dead_node_reclaim_time(), Duration::from_secs(60));
  assert_eq!(mapped.awareness_max_multiplier(), 9);
  assert_eq!(mapped.indirect_checks(), 4);
  assert_eq!(mapped.max_indirect_forwards(), 33);
  assert_eq!(mapped.max_members(), Some(500));
  assert!(mapped.ack_payload_to_members_only());
  assert_eq!(mapped.max_inbound_streams(), Some(64));
  assert_eq!(mapped.probe_timeout(), Duration::from_millis(250));
  assert_eq!(mapped.stream_timeout(), Duration::from_secs(15));
  assert_eq!(mapped.max_stream_frame_size(), 70_000);
  assert_eq!(mapped.gossip_mtu(), 1300);
  assert_eq!(mapped.gossip_interval(), Duration::from_millis(200));
  assert_eq!(mapped.gossip_nodes(), 3);
  assert_eq!(mapped.meta_max_size(), 400);
  assert_eq!(mapped.accept_handshake_deadline(), Duration::from_secs(11));
  assert_eq!(mapped.push_pull_interval(), Duration::from_secs(45));
  assert_eq!(mapped.retransmit_mult(), 6);
  assert_eq!(mapped.protocol_version(), ProtocolVersion::V1);
  assert_eq!(mapped.delegate_version(), DelegateVersion::V1);
  assert_eq!(mapped.initial_incarnation(), 12);
}

#[cfg(feature = "serde")]
#[test]
fn endpoint_options_serde_roundtrip_and_partial() {
  use core::time::Duration;
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap();
  let opts = EndpointOptions::<SmolStr, SocketAddr>::new(SmolStr::new("node-1"), addr)
    .with_suspicion_mult(9)
    .with_probe_interval(Duration::from_secs(2))
    .with_max_members(Some(500));

  let json = serde_json::to_string(&opts).unwrap();
  // Durations render as humantime strings, not {"secs":..,"nanos":..}.
  assert!(json.contains("\"probe_interval\":\"2s\""), "json = {json}");

  let back: EndpointOptions<SmolStr, SocketAddr> = serde_json::from_str(&json).unwrap();
  assert_eq!(back.local_id_ref().as_str(), "node-1");
  assert_eq!(*back.advertise_addr_ref(), addr);
  assert_eq!(back.suspicion_mult(), 9);
  assert_eq!(back.probe_interval(), Duration::from_secs(2));
  assert_eq!(back.max_members(), Some(500));

  // A partial config carries the required id/addr + one knob; the rest default.
  let partial: EndpointOptions<SmolStr, SocketAddr> =
    serde_json::from_str(r#"{"local_id":"n2","advertise_addr":"10.0.0.1:1","gossip_nodes":7}"#)
      .unwrap();
  assert_eq!(partial.gossip_nodes(), 7);
  assert_eq!(partial.suspicion_mult(), 4);
  assert_eq!(partial.probe_interval(), Duration::from_secs(1));

  // local_id / advertise_addr are required (no serde default).
  assert!(
    serde_json::from_str::<EndpointOptions<SmolStr, SocketAddr>>(r#"{"gossip_nodes":7}"#).is_err()
  );
}

#[cfg(feature = "serde")]
#[test]
fn endpoint_options_serde_rejects_unknown_field() {
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  // A misspelled knob (`gosip_mtu` for `gossip_mtu`) alongside the required
  // id/addr must be rejected, not silently dropped — a typo'd field would
  // otherwise leave the affected knob at its default with no warning.
  assert!(
    serde_json::from_str::<EndpointOptions<SmolStr, SocketAddr>>(
      r#"{"local_id":"n","advertise_addr":"127.0.0.1:1","gosip_mtu":1400}"#
    )
    .is_err()
  );
}

#[cfg(feature = "clap")]
#[test]
fn endpoint_options_clap_parses_and_wires_env() {
  use crate::typed::ProtocolVersion;
  use clap::{CommandFactory, Parser};
  use core::time::Duration;
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    opts: EndpointOptions<SmolStr, SocketAddr>,
  }

  let cli = Cli::try_parse_from([
    "app",
    "--local-id",
    "node-1",
    "--advertise-addr",
    "127.0.0.1:7946",
    "--suspicion-mult",
    "9",
    "--probe-interval",
    "2s",
    "--protocol-version",
    "1",
    "--max-members",
    "500",
  ])
  .unwrap();
  assert_eq!(cli.opts.local_id_ref().as_str(), "node-1");
  assert_eq!(
    *cli.opts.advertise_addr_ref(),
    "127.0.0.1:7946".parse::<SocketAddr>().unwrap()
  );
  assert_eq!(cli.opts.suspicion_mult(), 9);
  assert_eq!(cli.opts.probe_interval(), Duration::from_secs(2));
  assert_eq!(cli.opts.protocol_version(), ProtocolVersion::V1);
  assert_eq!(cli.opts.max_members(), Some(500));

  // Unspecified knobs default; only the required id/addr are given.
  let def =
    Cli::try_parse_from(["app", "--local-id", "n", "--advertise-addr", "127.0.0.1:1"]).unwrap();
  assert_eq!(def.opts.suspicion_mult(), 4);
  assert_eq!(def.opts.gossip_nodes(), 3);
  assert_eq!(def.opts.max_members(), None);

  // Env wired (introspect the command; never set_var).
  let cmd = Cli::command();
  let arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "endpoint-suspicion-mult")
    .expect("endpoint-suspicion-mult arg is registered");
  assert_eq!(
    arg.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_SUSPICION_MULT")
  );
}

#[cfg(feature = "serde")]
#[test]
fn endpoint_options_serde_rejects_out_of_range_incarnation() {
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  let over = format!(
    r#"{{"local_id":"n","advertise_addr":"127.0.0.1:1","initial_incarnation":{}}}"#,
    u32::MAX / 2 + 1
  );
  assert!(serde_json::from_str::<EndpointOptions<SmolStr, SocketAddr>>(&over).is_err());

  let boundary = format!(
    r#"{{"local_id":"n","advertise_addr":"127.0.0.1:1","initial_incarnation":{}}}"#,
    u32::MAX / 2
  );
  assert!(serde_json::from_str::<EndpointOptions<SmolStr, SocketAddr>>(&boundary).is_ok());
}

#[cfg(feature = "clap")]
#[test]
fn endpoint_options_clap_rejects_out_of_range_incarnation() {
  use clap::Parser;
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    opts: EndpointOptions<SmolStr, SocketAddr>,
  }

  let over = (u32::MAX / 2 + 1).to_string();
  assert!(
    Cli::try_parse_from([
      "app",
      "--local-id",
      "n",
      "--advertise-addr",
      "127.0.0.1:1",
      "--initial-incarnation",
      &over,
    ])
    .is_err()
  );
}

#[cfg(feature = "clap")]
#[test]
fn endpoint_options_clap_update_preserves_unoverridden_fields() {
  use crate::typed::Meta;
  use bytes::Bytes;
  use clap::Parser;
  use core::time::Duration;
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    opts: EndpointOptions<SmolStr, SocketAddr>,
  }

  let base = || {
    EndpointOptions::<SmolStr, SocketAddr>::new(
      SmolStr::new("n"),
      "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
    )
    .with_initial_meta(Meta::try_from(Bytes::from_static(b"meta")).unwrap())
    .with_initial_local_state(Bytes::from_static(b"snap"))
    .with_gossip_mtu(2000)
    .with_max_stream_frame_size(7_000_000)
    .with_probe_interval(Duration::from_secs(5))
  };

  // A partial update (only suspicion_mult) must leave every other field intact —
  // both the clap-defaulted scalars and the mirror-skipped runtime-only fields —
  // rather than reset them to clap defaults.
  let mut cli = Cli { opts: base() };
  cli
    .try_update_from(["app", "--suspicion-mult", "9"])
    .expect("update");
  assert_eq!(
    cli.opts.suspicion_mult(),
    9,
    "the supplied override is applied"
  );
  assert_eq!(
    cli.opts.gossip_mtu(),
    2000,
    "non-default gossip_mtu survives"
  );
  assert_eq!(
    cli.opts.max_stream_frame_size(),
    7_000_000,
    "non-default max_stream_frame_size survives"
  );
  assert_eq!(
    cli.opts.probe_interval(),
    Duration::from_secs(5),
    "non-default probe_interval survives"
  );
  assert_eq!(
    cli.opts.initial_meta_ref().len(),
    4,
    "initial_meta survives"
  );
  assert_eq!(
    cli.opts.initial_local_state_bytes(),
    Bytes::from_static(b"snap"),
    "initial_local_state survives"
  );

  // An explicitly-supplied override IS still applied.
  let mut cli2 = Cli { opts: base() };
  cli2
    .try_update_from(["app", "--gossip-mtu", "3000"])
    .expect("update");
  assert_eq!(
    cli2.opts.gossip_mtu(),
    3000,
    "an explicit override is applied"
  );
}
