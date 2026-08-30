use core::time::Duration;

use super::EndpointTuning;
use crate::config::EndpointOptions;

#[test]
fn endpoint_tuning_default_is_all_unset() {
  let t = EndpointTuning::new();
  assert_eq!(t, EndpointTuning::default());
  assert_eq!(t.probe_interval(), None);
  assert_eq!(t.probe_timeout(), None);
  assert_eq!(t.gossip_interval(), None);
  assert_eq!(t.gossip_to_the_dead_time(), None);
  assert_eq!(t.dead_node_reclaim_time(), None);
  assert_eq!(t.push_pull_interval(), None);
  assert_eq!(t.stream_timeout(), None);
  assert_eq!(t.accept_handshake_deadline(), None);
  assert_eq!(t.suspicion_mult(), None);
  assert_eq!(t.suspicion_max_timeout_mult(), None);
  assert_eq!(t.awareness_max_multiplier(), None);
  assert_eq!(t.indirect_checks(), None);
  assert_eq!(t.retransmit_mult(), None);
  assert_eq!(t.gossip_nodes(), None);
  assert_eq!(t.max_members(), None);
  assert_eq!(t.max_indirect_forwards(), None);
  assert_eq!(t.max_inbound_streams(), None);
  assert_eq!(t.ack_payload_to_members_only(), None);
}

#[test]
fn endpoint_tuning_builders_round_trip_through_accessors() {
  let t = EndpointTuning::new()
    .with_probe_interval(Duration::from_secs(5))
    .with_probe_timeout(Duration::from_millis(250))
    .with_gossip_interval(Duration::from_millis(100))
    .with_gossip_to_the_dead_time(Duration::from_secs(45))
    .with_dead_node_reclaim_time(Duration::from_secs(60))
    .with_push_pull_interval(Duration::from_secs(15))
    .with_stream_timeout(Duration::from_secs(20))
    .with_accept_handshake_deadline(Duration::from_secs(7))
    .with_suspicion_mult(7)
    .with_suspicion_max_timeout_mult(9)
    .with_awareness_max_multiplier(5)
    .with_indirect_checks(4)
    .with_retransmit_mult(6)
    .with_gossip_nodes(5)
    .with_max_members(32)
    .with_max_indirect_forwards(128)
    .with_max_inbound_streams(64)
    .with_ack_payload_to_members_only(true);

  assert_eq!(t.probe_interval(), Some(Duration::from_secs(5)));
  assert_eq!(t.probe_timeout(), Some(Duration::from_millis(250)));
  assert_eq!(t.gossip_interval(), Some(Duration::from_millis(100)));
  assert_eq!(t.gossip_to_the_dead_time(), Some(Duration::from_secs(45)));
  assert_eq!(t.dead_node_reclaim_time(), Some(Duration::from_secs(60)));
  assert_eq!(t.push_pull_interval(), Some(Duration::from_secs(15)));
  assert_eq!(t.stream_timeout(), Some(Duration::from_secs(20)));
  assert_eq!(t.accept_handshake_deadline(), Some(Duration::from_secs(7)));
  assert_eq!(t.suspicion_mult(), Some(7));
  assert_eq!(t.suspicion_max_timeout_mult(), Some(9));
  assert_eq!(t.awareness_max_multiplier(), Some(5));
  assert_eq!(t.indirect_checks(), Some(4));
  assert_eq!(t.retransmit_mult(), Some(6));
  assert_eq!(t.gossip_nodes(), Some(5));
  assert_eq!(t.max_members(), Some(32));
  assert_eq!(t.max_indirect_forwards(), Some(128));
  assert_eq!(t.max_inbound_streams(), Some(64));
  assert_eq!(t.ack_payload_to_members_only(), Some(true));
}

// The core parity regression: a fully-populated tuning (every value chosen to
// differ from the machine default) applied over a fresh `EndpointOptions` must
// move ALL 18 corresponding `EndpointOptions` accessors to the tuned value.
// This locks the `apply_to` copy list — a knob dropped from `apply_to` would
// leave its accessor at the default and fail here.
#[test]
fn endpoint_tuning_apply_to_overrides_every_knob() {
  let t = EndpointTuning::new()
    .with_probe_interval(Duration::from_secs(5))
    .with_probe_timeout(Duration::from_millis(250))
    .with_gossip_interval(Duration::from_millis(100))
    .with_gossip_to_the_dead_time(Duration::from_secs(45))
    .with_dead_node_reclaim_time(Duration::from_secs(60))
    .with_push_pull_interval(Duration::from_secs(15))
    .with_stream_timeout(Duration::from_secs(20))
    .with_accept_handshake_deadline(Duration::from_secs(7))
    .with_suspicion_mult(7)
    .with_suspicion_max_timeout_mult(9)
    .with_awareness_max_multiplier(5)
    .with_indirect_checks(4)
    .with_retransmit_mult(6)
    .with_gossip_nodes(5)
    .with_max_members(32)
    .with_max_indirect_forwards(128)
    .with_max_inbound_streams(64)
    .with_ack_payload_to_members_only(true);

  // Confirm every chosen value actually differs from the `EndpointOptions::new`
  // default, so a passing assertion below cannot be a coincidence.
  let base = EndpointOptions::<(), ()>::new((), ());
  assert_ne!(base.probe_interval(), Duration::from_secs(5));
  assert_ne!(base.probe_timeout(), Duration::from_millis(250));
  assert_ne!(base.gossip_interval(), Duration::from_millis(100));
  assert_ne!(base.gossip_to_the_dead_time(), Duration::from_secs(45));
  assert_ne!(base.dead_node_reclaim_time(), Duration::from_secs(60));
  assert_ne!(base.push_pull_interval(), Duration::from_secs(15));
  assert_ne!(base.stream_timeout(), Duration::from_secs(20));
  assert_ne!(base.accept_handshake_deadline(), Duration::from_secs(7));
  assert_ne!(base.suspicion_mult(), 7);
  assert_ne!(base.suspicion_max_timeout_mult(), 9);
  assert_ne!(base.awareness_max_multiplier(), 5);
  assert_ne!(base.indirect_checks(), 4);
  assert_ne!(base.retransmit_mult(), 6);
  assert_ne!(base.gossip_nodes(), 5);
  assert_ne!(base.max_members(), Some(32));
  assert_ne!(base.max_indirect_forwards(), 128);
  assert_ne!(base.max_inbound_streams(), Some(64));
  assert!(!base.ack_payload_to_members_only());

  let cfg = t.apply_to(EndpointOptions::<(), ()>::new((), ()));
  assert_eq!(cfg.probe_interval(), Duration::from_secs(5));
  assert_eq!(cfg.probe_timeout(), Duration::from_millis(250));
  assert_eq!(cfg.gossip_interval(), Duration::from_millis(100));
  assert_eq!(cfg.gossip_to_the_dead_time(), Duration::from_secs(45));
  assert_eq!(cfg.dead_node_reclaim_time(), Duration::from_secs(60));
  assert_eq!(cfg.push_pull_interval(), Duration::from_secs(15));
  assert_eq!(cfg.stream_timeout(), Duration::from_secs(20));
  assert_eq!(cfg.accept_handshake_deadline(), Duration::from_secs(7));
  assert_eq!(cfg.suspicion_mult(), 7);
  assert_eq!(cfg.suspicion_max_timeout_mult(), 9);
  assert_eq!(cfg.awareness_max_multiplier(), 5);
  assert_eq!(cfg.indirect_checks(), 4);
  assert_eq!(cfg.retransmit_mult(), 6);
  assert_eq!(cfg.gossip_nodes(), 5);
  assert_eq!(cfg.max_members(), Some(32));
  assert_eq!(cfg.max_indirect_forwards(), 128);
  assert_eq!(cfg.max_inbound_streams(), Some(64));
  assert!(cfg.ack_payload_to_members_only());
}

// The byte-identical-when-unset guarantee: a default (all-unset) tuning applied
// over a fresh `EndpointOptions` leaves every one of the 18 knobs at the exact
// `EndpointOptions::new` default.
#[test]
fn endpoint_tuning_apply_to_unset_leaves_machine_defaults() {
  let base = EndpointOptions::<(), ()>::new((), ());
  let cfg = EndpointTuning::new().apply_to(EndpointOptions::<(), ()>::new((), ()));

  assert_eq!(cfg.probe_interval(), base.probe_interval());
  assert_eq!(cfg.probe_timeout(), base.probe_timeout());
  assert_eq!(cfg.gossip_interval(), base.gossip_interval());
  assert_eq!(
    cfg.gossip_to_the_dead_time(),
    base.gossip_to_the_dead_time()
  );
  assert_eq!(cfg.dead_node_reclaim_time(), base.dead_node_reclaim_time());
  assert_eq!(cfg.push_pull_interval(), base.push_pull_interval());
  assert_eq!(cfg.stream_timeout(), base.stream_timeout());
  assert_eq!(
    cfg.accept_handshake_deadline(),
    base.accept_handshake_deadline()
  );
  assert_eq!(cfg.suspicion_mult(), base.suspicion_mult());
  assert_eq!(
    cfg.suspicion_max_timeout_mult(),
    base.suspicion_max_timeout_mult()
  );
  assert_eq!(
    cfg.awareness_max_multiplier(),
    base.awareness_max_multiplier()
  );
  assert_eq!(cfg.indirect_checks(), base.indirect_checks());
  assert_eq!(cfg.retransmit_mult(), base.retransmit_mult());
  assert_eq!(cfg.gossip_nodes(), base.gossip_nodes());
  assert_eq!(cfg.max_members(), base.max_members());
  assert_eq!(cfg.max_indirect_forwards(), base.max_indirect_forwards());
  assert_eq!(cfg.max_inbound_streams(), base.max_inbound_streams());
  assert_eq!(
    cfg.ack_payload_to_members_only(),
    base.ack_payload_to_members_only()
  );
}

#[cfg(feature = "serde")]
#[test]
fn endpoint_tuning_serde_round_trip_and_partial() {
  // `{}` deserializes to the full default (every knob unset).
  let from_empty = serde_json::from_str::<EndpointTuning>("{}").unwrap();
  assert_eq!(from_empty, EndpointTuning::new());

  let t = EndpointTuning::new()
    .with_probe_interval(Duration::from_secs(2))
    .with_suspicion_mult(9)
    .with_max_members(500)
    .with_ack_payload_to_members_only(true);
  let json = serde_json::to_string(&t).unwrap();
  // Durations render as humantime strings, not {"secs":..,"nanos":..}.
  assert!(json.contains("\"probe_interval\":\"2s\""), "json = {json}");
  // An unset Duration serializes as null (no skip_serializing_if).
  assert!(json.contains("\"probe_timeout\":null"), "json = {json}");

  let back = serde_json::from_str::<EndpointTuning>(&json).unwrap();
  assert_eq!(back, t);
  assert_eq!(back.probe_interval(), Some(Duration::from_secs(2)));
  assert_eq!(back.suspicion_mult(), Some(9));
  assert_eq!(back.max_members(), Some(500));
  assert_eq!(back.ack_payload_to_members_only(), Some(true));
  assert_eq!(back.probe_timeout(), None);

  // A partial config carries one knob; the rest stay unset.
  let partial: EndpointTuning = serde_json::from_str(r#"{"gossip_nodes":7}"#).unwrap();
  assert_eq!(partial.gossip_nodes(), Some(7));
  assert_eq!(partial.probe_interval(), None);
  assert_eq!(partial.suspicion_mult(), None);
}

#[cfg(feature = "serde")]
#[test]
fn endpoint_tuning_serde_rejects_unknown_field() {
  // A misspelled knob (`probe_intervl`) must be rejected, not silently dropped.
  assert!(serde_json::from_str::<EndpointTuning>(r#"{"probe_intervl":"2s"}"#).is_err());
}

#[cfg(feature = "clap")]
#[test]
fn endpoint_tuning_clap_parses_and_wires_env() {
  use clap::{CommandFactory, Parser};

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    tuning: EndpointTuning,
  }

  let cli = Cli::try_parse_from([
    "app",
    "--memberlist-probe-interval",
    "5s",
    "--memberlist-suspicion-mult",
    "7",
    "--memberlist-max-members",
    "32",
    "--memberlist-max-inbound-streams",
    "64",
    "--memberlist-ack-payload-to-members-only",
    "true",
  ])
  .unwrap();
  assert_eq!(cli.tuning.probe_interval(), Some(Duration::from_secs(5)));
  assert_eq!(cli.tuning.suspicion_mult(), Some(7));
  assert_eq!(cli.tuning.max_members(), Some(32));
  assert_eq!(cli.tuning.max_inbound_streams(), Some(64));
  assert_eq!(cli.tuning.ack_payload_to_members_only(), Some(true));

  // Unspecified knobs stay unset.
  let bare = Cli::try_parse_from(["app"]).unwrap();
  assert_eq!(bare.tuning.probe_interval(), None);
  assert_eq!(bare.tuning.suspicion_mult(), None);
  assert_eq!(bare.tuning.max_members(), None);
  assert_eq!(bare.tuning.ack_payload_to_members_only(), None);

  // Env wired (introspect the command; never set_var).
  let cmd = Cli::command();
  let arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "memberlist-probe-interval")
    .expect("memberlist-probe-interval arg is registered");
  assert_eq!(
    arg.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_PROBE_INTERVAL")
  );
}

#[cfg(feature = "clap")]
#[test]
fn endpoint_tuning_clap_update_preserves_unoverridden_fields() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    tuning: EndpointTuning,
  }

  let base = || {
    EndpointTuning::new()
      .with_probe_interval(Duration::from_secs(5))
      .with_max_members(32)
      .with_suspicion_mult(7)
  };

  // A partial update (only gossip_nodes) leaves the builder-seeded fields intact
  // — clap's defaulted-arg-looks-present behavior must not reset the unset ones.
  let mut cli = Cli { tuning: base() };
  cli
    .try_update_from(["app", "--memberlist-gossip-nodes", "9"])
    .expect("update");
  assert_eq!(
    cli.tuning.gossip_nodes(),
    Some(9),
    "the supplied override is applied"
  );
  assert_eq!(
    cli.tuning.probe_interval(),
    Some(Duration::from_secs(5)),
    "seeded probe_interval survives"
  );
  assert_eq!(
    cli.tuning.max_members(),
    Some(32),
    "seeded max_members survives"
  );
  assert_eq!(
    cli.tuning.suspicion_mult(),
    Some(7),
    "seeded suspicion_mult survives"
  );

  // An explicitly-supplied override IS still applied.
  let mut cli2 = Cli { tuning: base() };
  cli2
    .try_update_from(["app", "--memberlist-suspicion-mult", "11"])
    .expect("update");
  assert_eq!(
    cli2.tuning.suspicion_mult(),
    Some(11),
    "an explicit override is applied"
  );
  assert_eq!(
    cli2.tuning.probe_interval(),
    Some(Duration::from_secs(5)),
    "the unrelated seeded probe_interval survives"
  );
}
