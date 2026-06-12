use super::*;

#[test]
fn driver_options_defaults_match_constants() {
  let opts = DriverOptions::new();
  assert_eq!(opts.join_deadline(), DEFAULT_JOIN_DEADLINE);
  assert_eq!(opts.leave_timeout(), DEFAULT_LEAVE_TIMEOUT);
  assert_eq!(opts.idle_wake_interval(), DEFAULT_IDLE_WAKE_INTERVAL);
  assert_eq!(opts.iter_drain_cap(), DEFAULT_ITER_DRAIN_CAP);
  assert_eq!(opts.cmd_fairness_budget(), DEFAULT_CMD_FAIRNESS_BUDGET);
  assert_eq!(opts.event_queue_cap(), DEFAULT_EVENT_QUEUE_CAP);
  assert_eq!(opts.peek_budget(), DEFAULT_PEEK_BUDGET);
  assert_eq!(opts.observation_channel(), DEFAULT_OBSERVATION_CHANNEL);
  // `Default` delegates to `new`.
  assert_eq!(DriverOptions::default(), opts);
  // Debug is derived and non-empty.
  assert!(!format!("{opts:?}").is_empty());
}

// Every `with_*` builder round-trips through its accessor.
#[test]
fn driver_options_builders_round_trip() {
  let opts = DriverOptions::new()
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
  assert_ne!(opts, DriverOptions::new());
  let same = DriverOptions::new()
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
    Err(crate::error::MemberlistError::InvalidOption(_))
  ));
  // A zero close timeout truncates every graceful close.
  assert!(matches!(
    StreamTransportOptions::new()
      .with_close_timeout(Duration::ZERO)
      .validate(),
    Err(crate::error::MemberlistError::InvalidOption(_))
  ));
  // A zero inbound cap (valid rendezvous) and a zero dial timeout (loud) are
  // NOT rejected.
  assert!(
    StreamTransportOptions::new()
      .with_bridge_inbound_cap(0)
      .validate()
      .is_ok()
  );
  assert!(
    StreamTransportOptions::new()
      .with_dial_timeout(Duration::ZERO)
      .validate()
      .is_ok()
  );
}
