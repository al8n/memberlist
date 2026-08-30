use super::*;
use crate::error::MemberlistError;

#[test]
fn runtime_options_defaults_match_constants() {
  let opts = RuntimeOptions::new();
  assert_eq!(opts.join_deadline(), DEFAULT_JOIN_DEADLINE);
  assert_eq!(opts.leave_timeout(), DEFAULT_LEAVE_TIMEOUT);
  assert_eq!(opts.idle_wake_interval(), DEFAULT_IDLE_WAKE_INTERVAL);
  assert_eq!(opts.iter_drain_cap(), DEFAULT_ITER_DRAIN_CAP);
  assert_eq!(opts.cmd_fairness_budget(), DEFAULT_CMD_FAIRNESS_BUDGET);
  assert_eq!(opts.event_queue_cap(), DEFAULT_EVENT_QUEUE_CAP);
  assert_eq!(opts.peek_budget(), DEFAULT_PEEK_BUDGET);
  assert_eq!(opts.observation_channel(), DEFAULT_OBSERVATION_CHANNEL);
  // `Default` delegates to `new`.
  assert_eq!(RuntimeOptions::default(), opts);
  // Debug is derived and non-empty.
  assert!(!format!("{opts:?}").is_empty());
}

// Every `with_*` builder round-trips through its accessor.
#[test]
fn runtime_options_builders_round_trip() {
  let opts = RuntimeOptions::new()
    .with_join_deadline(Duration::from_secs(33))
    .with_leave_timeout(Duration::from_secs(7))
    .with_idle_wake_interval(Duration::from_secs(11))
    .with_iter_drain_cap(99)
    .with_cmd_fairness_budget(8)
    .with_event_queue_cap(2048)
    .with_peek_budget(Duration::from_millis(5))
    .with_observation_channel(Channel::Unbounded);

  assert_eq!(opts.join_deadline(), Duration::from_secs(33));
  assert_eq!(opts.leave_timeout(), Duration::from_secs(7));
  assert_eq!(opts.idle_wake_interval(), Duration::from_secs(11));
  assert_eq!(opts.iter_drain_cap(), 99);
  assert_eq!(opts.cmd_fairness_budget(), 8);
  assert_eq!(opts.event_queue_cap(), 2048);
  assert_eq!(opts.peek_budget(), Duration::from_millis(5));
  assert_eq!(opts.observation_channel(), Channel::Unbounded);

  // Copy + PartialEq are derived: a builder-modified value differs from the
  // default, an identical rebuild compares equal.
  assert_ne!(opts, RuntimeOptions::new());
  let same = RuntimeOptions::new()
    .with_join_deadline(Duration::from_secs(33))
    .with_leave_timeout(Duration::from_secs(7))
    .with_idle_wake_interval(Duration::from_secs(11))
    .with_iter_drain_cap(99)
    .with_cmd_fairness_budget(8)
    .with_event_queue_cap(2048)
    .with_peek_budget(Duration::from_millis(5))
    .with_observation_channel(Channel::Unbounded);
  assert_eq!(opts, same);
}

#[test]
fn channel_variants_compare_and_default() {
  assert_eq!(Channel::Bounded(8), Channel::Bounded(8));
  assert_ne!(Channel::Bounded(8), Channel::Bounded(9));
  assert_ne!(Channel::Bounded(8), Channel::Unbounded);
  assert_eq!(DEFAULT_OBSERVATION_CHANNEL, Channel::Bounded(1024));
  assert!(!format!("{:?}", Channel::Unbounded).is_empty());
}

/// `"bounded:<n>"`, `"bounded=<n>"`, and a bare integer all parse to
/// `Bounded(n)`; `"unbounded"` is case-insensitive; anything else is a
/// `ParseChannelError`. The bare-integer form is a back-compat alias kept
/// alongside the canonical `bounded:<n>` grammar.
#[test]
fn channel_parses_bounded_prefixed_and_bare() {
  use core::str::FromStr;

  assert_eq!(
    Channel::from_str("bounded:1024"),
    Ok(Channel::Bounded(1024))
  );
  assert_eq!(
    Channel::from_str("bounded=1024"),
    Ok(Channel::Bounded(1024))
  );
  assert_eq!(Channel::from_str("1024"), Ok(Channel::Bounded(1024)));
  assert_eq!(Channel::from_str("unbounded"), Ok(Channel::Unbounded));
  assert_eq!(Channel::from_str("UNBOUNDED"), Ok(Channel::Unbounded));
  assert_eq!(Channel::from_str("bounded=8"), Ok(Channel::Bounded(8)));
  // Garbage, a missing capacity, and a non-numeric capacity are rejected —
  // the bare fallback only applies when neither `bounded:`/`bounded=` prefix
  // is present.
  assert!(Channel::from_str("nope").is_err());
  assert!(Channel::from_str("bounded:").is_err());
  assert!(Channel::from_str("bounded:x").is_err());
}

/// `Display` always emits the canonical `bounded:<n>` / `unbounded` forms,
/// matching the shared serde representation — never the legacy bare-integer
/// form.
#[test]
fn channel_display_is_canonical() {
  assert_eq!(Channel::Unbounded.to_string(), "unbounded");
  assert_eq!(Channel::Bounded(16).to_string(), "bounded:16");
}

/// The canonical `Display` output always re-parses to the same value through
/// `FromStr`.
#[test]
fn channel_display_roundtrips_through_fromstr() {
  use core::str::FromStr;

  for c in [
    Channel::Unbounded,
    Channel::Bounded(0),
    Channel::Bounded(1024),
  ] {
    assert_eq!(Channel::from_str(&c.to_string()), Ok(c));
  }
}

/// The string grammar is cross-driver: `memberlist-reactor`'s `Channel` has no
/// crate-level dependency relationship with this one, so parity is pinned by
/// literal string, not by importing the other crate's type. Every string
/// reactor's canonical `Display` can emit (`"bounded:<n>"`, `"unbounded"`) and
/// reactor's own legacy bare-integer form all parse here to the expected
/// variant — the same strings are independently verified against reactor's
/// `FromStr` in its own test suite.
#[test]
fn channel_grammar_is_cross_driver() {
  use core::str::FromStr;

  for s in ["bounded:1024", "bounded=1024", "1024"] {
    assert_eq!(Channel::from_str(s), Ok(Channel::Bounded(1024)));
  }
  for s in ["unbounded", "UNBOUNDED"] {
    assert_eq!(Channel::from_str(s), Ok(Channel::Unbounded));
  }
}

#[cfg(feature = "serde")]
#[test]
fn runtime_options_serde_round_trip_and_partial() {
  // An empty config deserializes to the full default.
  assert_eq!(
    serde_json::from_str::<RuntimeOptions>("{}").unwrap(),
    RuntimeOptions::new()
  );
  // A full round-trip preserves every knob, including the humantime durations
  // and the tagged observation channel.
  let opts = RuntimeOptions::new()
    .with_join_deadline(Duration::from_secs(33))
    .with_peek_budget(Duration::from_millis(5))
    .with_iter_drain_cap(99)
    .with_observation_channel(Channel::Unbounded);
  let json = serde_json::to_string(&opts).unwrap();
  assert_eq!(serde_json::from_str::<RuntimeOptions>(&json).unwrap(), opts);
  // A partial config overrides one field and defaults the rest.
  let partial: RuntimeOptions = serde_json::from_str(r#"{"iter_drain_cap": 7}"#).unwrap();
  assert_eq!(partial.iter_drain_cap(), 7);
  assert_eq!(partial.join_deadline(), DEFAULT_JOIN_DEADLINE);
  assert_eq!(partial.observation_channel(), DEFAULT_OBSERVATION_CHANNEL);
  // The bounded channel serializes as its snake_case tagged form.
  let bounded = RuntimeOptions::new().with_observation_channel(Channel::Bounded(8));
  let bjson = serde_json::to_string(&bounded).unwrap();
  assert!(bjson.contains("bounded"), "json = {bjson}");
  assert_eq!(
    serde_json::from_str::<RuntimeOptions>(&bjson).unwrap(),
    bounded
  );
}

#[cfg(feature = "serde")]
#[test]
fn runtime_options_serde_rejects_unknown_field() {
  // A misspelled `iter_drain_cap` must be rejected, not silently dropped.
  assert!(serde_json::from_str::<RuntimeOptions>(r#"{"iter_drain_capp": 7}"#).is_err());
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

  // Flags parse: a humantime duration, a usize, and the channel via FromStr.
  let cli = Cli::try_parse_from([
    "app",
    "--runtime-join-deadline",
    "30s",
    "--runtime-iter-drain-cap",
    "12",
    "--runtime-observation-channel",
    "bounded:64",
  ])
  .unwrap();
  assert_eq!(cli.runtime.join_deadline(), Duration::from_secs(30));
  assert_eq!(cli.runtime.iter_drain_cap(), 12);
  assert_eq!(cli.runtime.observation_channel(), Channel::Bounded(64));
  // Unspecified flags stay at the defaults.
  let dflt = Cli::try_parse_from(["app"]).unwrap();
  assert_eq!(dflt.runtime, RuntimeOptions::new());
  // The env var is wired — assert via command introspection, never `set_var`.
  let cmd = Cli::command();
  let arg = cmd
    .get_arguments()
    .find(|a| a.get_id().as_str() == "runtime-peek-budget")
    .expect("runtime-peek-budget arg is registered");
  assert_eq!(
    arg.get_env().and_then(|e| e.to_str()),
    Some("MEMBERLIST_RUNTIME_PEEK_BUDGET")
  );
}

// A partial `try_update_from` carrying one unrelated flag must NOT reset the
// other defaulted knobs. clap's `default_value` / `default_value_t` makes an
// unset arg look "present" in update mode, so the value-source gate in the
// manual `update_from_arg_matches` is what keeps a seeded non-default value
// alive across an update.
#[cfg(feature = "clap")]
#[test]
fn runtime_options_partial_update_preserves_unset_fields() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    o: RuntimeOptions,
  }

  // Seed NON-default values on several defaulted fields, then run a partial
  // update that supplies ONE unrelated flag.
  let mut cli = Cli {
    o: RuntimeOptions::new()
      .with_join_deadline(Duration::from_secs(99))
      .with_leave_timeout(Duration::from_secs(42))
      .with_event_queue_cap(7)
      .with_peek_budget(Duration::from_millis(250))
      .with_observation_channel(Channel::Unbounded),
  };

  cli
    .try_update_from(["app", "--runtime-iter-drain-cap", "13"])
    .expect("partial update parses");

  // The supplied flag is applied.
  assert_eq!(cli.o.iter_drain_cap(), 13);
  // Every seeded non-default field SURVIVES the partial update.
  assert_eq!(cli.o.join_deadline(), Duration::from_secs(99));
  assert_eq!(cli.o.leave_timeout(), Duration::from_secs(42));
  assert_eq!(cli.o.event_queue_cap(), 7);
  assert_eq!(cli.o.peek_budget(), Duration::from_millis(250));
  assert_eq!(cli.o.observation_channel(), Channel::Unbounded);
}

// An explicit override on update IS applied (the value-source gate lets a
// command-line value through).
#[cfg(feature = "clap")]
#[test]
fn runtime_options_update_applies_explicit_override() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    o: RuntimeOptions,
  }

  let mut cli = Cli {
    o: RuntimeOptions::new().with_join_deadline(Duration::from_secs(99)),
  };
  cli
    .try_update_from(["app", "--runtime-join-deadline", "3s"])
    .expect("explicit override parses");
  assert_eq!(cli.o.join_deadline(), Duration::from_secs(3));
}

#[test]
fn stream_transport_options_defaults_match_constants() {
  let opts = StreamTransportOptions::new();
  assert_eq!(opts.dial_timeout(), DEFAULT_DIAL_TIMEOUT);
  assert_eq!(opts.close_timeout(), DEFAULT_CLOSE_TIMEOUT);
  assert_eq!(opts.bridge_inbound_cap(), DEFAULT_BRIDGE_INBOUND_CAP);
  assert_eq!(opts.bridge_recv_buf_len(), DEFAULT_BRIDGE_RECV_BUF_LEN);
  assert_eq!(StreamTransportOptions::default(), opts);
  assert!(!format!("{opts:?}").is_empty());
}

#[test]
fn stream_transport_options_builders_round_trip() {
  let opts = StreamTransportOptions::new()
    .with_dial_timeout(Duration::from_secs(3))
    .with_close_timeout(Duration::from_secs(20))
    .with_bridge_inbound_cap(512)
    .with_bridge_recv_buf_len(8 * 1024);

  assert_eq!(opts.dial_timeout(), Duration::from_secs(3));
  assert_eq!(opts.close_timeout(), Duration::from_secs(20));
  assert_eq!(opts.bridge_inbound_cap(), 512);
  assert_eq!(opts.bridge_recv_buf_len(), 8 * 1024);
  assert_ne!(opts, StreamTransportOptions::new());
}

// `validate` rejects exactly the two deterministic-break knobs at zero and
// accepts the loud / degrade-but-function ones. Compiled only with a stream
// backend feature, matching the cfg on `validate` itself.
#[cfg(any(
  feature = "tcp",
  feature = "tls-rustls-ring",
  feature = "tls-rustls-aws-lc-rs"
))]
#[test]
fn stream_transport_options_validate() {
  // Baseline (all defaults) is accepted.
  assert!(StreamTransportOptions::new().validate().is_ok());

  // A zero read buffer turns every reliable read into a false EOF.
  assert!(matches!(
    StreamTransportOptions::new()
      .with_bridge_recv_buf_len(0)
      .validate(),
    Err(MemberlistError::InvalidOption(_))
  ));
  // A zero close timeout truncates every graceful close.
  assert!(matches!(
    StreamTransportOptions::new()
      .with_close_timeout(Duration::ZERO)
      .validate(),
    Err(MemberlistError::InvalidOption(_))
  ));
  // A zero inbound cap deadlocks the local inbound channel (no rendezvous path).
  assert!(matches!(
    StreamTransportOptions::new()
      .with_bridge_inbound_cap(0)
      .validate(),
    Err(MemberlistError::InvalidOption(_))
  ));
  // A zero dial timeout (a loud immediate failure) is NOT rejected.
  assert!(
    StreamTransportOptions::new()
      .with_dial_timeout(Duration::ZERO)
      .validate()
      .is_ok()
  );
}

// `capped_timer` caps an extreme duration at `MAX_TIMER_DURATION` and passes
// a small one through unchanged; the cap is representable by
// `Instant::now() + d` while the uncapped `Duration::MAX` is not, which is
// exactly why `compio::time::sleep` needs the cap.
#[test]
fn capped_timer_caps_extreme_and_passes_through_small() {
  assert_eq!(capped_timer(Duration::MAX), MAX_TIMER_DURATION);
  assert_eq!(capped_timer(Duration::from_secs(5)), Duration::from_secs(5));

  // The cap is representable, so `sleep(capped)` cannot overflow-panic.
  assert!(
    std::time::Instant::now()
      .checked_add(MAX_TIMER_DURATION)
      .is_some()
  );
  // Mutation-anchor: without the cap, the raw extreme duration overflows the
  // monotonic clock's `checked_add` — this is the panic `capped_timer` exists
  // to prevent.
  assert!(
    std::time::Instant::now()
      .checked_add(Duration::MAX)
      .is_none()
  );
}
