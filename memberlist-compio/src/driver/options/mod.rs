//! Per-`Memberlist` runtime tuning knobs.
//!
//! [`RuntimeOptions`] carries the generic-free runtime knobs. Per-backend
//! knobs live on each backend's `*TransportOptions` struct, folded into
//! [`Options<T>`](crate::Options) by `Options::new(T::Options)`.
//!
//! Operators with pathological workloads can dial individual knobs
//! without forking the crate. Each `DEFAULT_*` constant is `pub` so
//! callers can derive new values from the default (e.g.
//! `DEFAULT_JOIN_DEADLINE * 3`).

use crate::error::{InvalidOption, MemberlistError};
use core::time::Duration;

/// Default per-call deadline for [`Memberlist::join`](crate::Memberlist::join).
pub const DEFAULT_JOIN_DEADLINE: Duration = Duration::from_secs(10);

/// Default per-call deadline for [`Memberlist::leave`](crate::Memberlist::leave).
///
/// A graceful leave returns once the machine's `Event::LeftCluster`
/// fires — i.e. once the direct `Dead`-self notices to live peers have
/// been flushed to the wire. This bounds how long the call blocks on
/// that flush before surfacing
/// [`MemberlistError::LeaveTimeout`](crate::MemberlistError::LeaveTimeout).
pub const DEFAULT_LEAVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default fallback sleep when the coordinator's `poll_timeout` returns
/// `None`.
pub const DEFAULT_IDLE_WAKE_INTERVAL: Duration = Duration::from_secs(60);

/// Default per-iteration drain cap for inbound surfaces.
pub const DEFAULT_ITER_DRAIN_CAP: usize = 256;

/// Default per-iteration cmd-channel fairness budget.
pub const DEFAULT_CMD_FAIRNESS_BUDGET: usize = 4;

/// Default events-channel capacity.
pub const DEFAULT_EVENT_QUEUE_CAP: usize = 1024;

/// Default past-due peek budget.
pub const DEFAULT_PEEK_BUDGET: Duration = Duration::from_millis(1);

/// Default outbound dial budget for the stream-transport driver.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Default bridge-inbound channel capacity for the stream-transport driver.
pub const DEFAULT_BRIDGE_INBOUND_CAP: usize = 1024;

/// Default per-bridge TCP read buffer size for the stream-transport driver.
pub const DEFAULT_BRIDGE_RECV_BUF_LEN: usize = 16 * 1024;

/// Default bound on a per-bridge graceful-drain write for the stream-transport
/// driver: 10 seconds.
///
/// After a graceful `StreamAction::Close` the bridge drains the queued response
/// `Bytes` via `write_all`, but the handle is already gone so there is no
/// remaining cancel path. A peer that sent a valid request+FIN and then STOPPED
/// reading collapses its receive window to zero, and that `write_all` would
/// block forever — leaking the detached bridge task and its socket. This bounds
/// each post-Close drain write: a write making progress (the peer is reading)
/// never trips it; only a write stalled for the full timeout (the peer is not
/// reading) is abandoned and the bridge torn down (RST). Mirrors the smoltcp
/// driver's `Config::close_timeout` default.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// How the per-driver delegate **observation channel** is bounded.
///
/// The driver hands every machine `Event` to a separate
/// observation task that runs the user [`Delegate`](crate::Delegate) hooks,
/// decoupled from the protocol loop so a slow hook never stalls SWIM. This
/// knob chooses how that hand-off channel is sized.
///
/// - [`Unbounded`](Self::Unbounded): the hand-off never drops, so
///   the delegate observes every event in order — but a delegate that
///   persistently runs slower than inbound traffic (e.g. under an adversarial
///   `UserPacket` flood) grows the channel without limit, an unbounded-memory
///   risk. Choose this when delivery completeness matters more than a hard
///   memory bound and the delegate is trusted to keep up.
/// - [`Bounded`](Self::Bounded): caps the channel at `n` queued events. When
///   full, the driver drops the newest event rather than blocking — blocking
///   would stall SWIM — and increments the `observation_dropped` counter so the
///   drop is observable rather than silent. Choose this to bound memory under
///   overload at the cost of possibly dropping events the delegate never sees.
///   A drop here means BOTH the delegate AND any `EventStream` subscriber miss
///   the event; drops at the later `EventStream` fan-out (a slow subscriber)
///   are counted separately in `events_dropped`. A subscriber that needs to
///   detect every gap must monitor BOTH counters.
///
/// `Bounded(0)` is rejected at construction: a zero-capacity channel is a
/// rendezvous that the driver's non-blocking send can never deposit into, so
/// the delegate would observe nothing.
///
/// As a config value (serde / CLI) it is open-vocabulary: `Unbounded` is the
/// bare string `"unbounded"`, `Bounded(n)` is `{"bounded": n}` under serde and
/// `bounded:n` on the CLI (via [`FromStr`](core::str::FromStr) /
/// [`Display`](core::fmt::Display)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Channel {
  /// Never drop; the channel grows without bound (see the type-level docs
  /// for the slow-delegate memory risk).
  Unbounded,
  /// Cap the channel at this many queued events; drop-newest and count in
  /// `observation_dropped` when full (EventStream fan-out drops are counted
  /// separately in `events_dropped` — see the type-level docs).
  Bounded(usize),
}

impl core::fmt::Display for Channel {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Unbounded => f.write_str("unbounded"),
      Self::Bounded(n) => write!(f, "bounded:{n}"),
    }
  }
}

/// Parse a [`Channel`] from its string form — `"unbounded"` or `"bounded:<n>"`
/// — for a config value or a CLI flag. The inverse of [`Channel`]'s `Display`;
/// unconditional (not gated on `clap`) so any caller can use it.
impl core::str::FromStr for Channel {
  type Err = ParseChannelError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    if s.eq_ignore_ascii_case("unbounded") {
      return Ok(Self::Unbounded);
    }
    let cap = s
      .strip_prefix("bounded:")
      .or_else(|| s.strip_prefix("bounded="))
      .ok_or(ParseChannelError(()))?;
    let n = cap.parse::<usize>().map_err(|_| ParseChannelError(()))?;
    Ok(Self::Bounded(n))
  }
}

/// The error from [`Channel::from_str`]: the input was neither `"unbounded"`
/// nor a `"bounded:<n>"` with a valid capacity.
///
/// Opaque — the private unit field seals construction to this module, so the
/// error can gain detail later without a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid observation channel (expected `unbounded` or `bounded:<n>`)")]
pub struct ParseChannelError(());

/// Default delegate observation channel: [`Channel::Bounded`] at 1024 events.
///
/// Bounded by default so a slow or wedged delegate cannot let remote traffic
/// grow the observation queue without limit — an OOM a peer could drive. 1024
/// matches [`DEFAULT_EVENT_QUEUE_CAP`] and sits well above normal per-tick
/// event volume, so a well-behaved delegate never sees a drop; a delegate that
/// needs guaranteed never-drop delivery (and controls its own latency) can opt
/// into [`Channel::Unbounded`].
pub const DEFAULT_OBSERVATION_CHANNEL: Channel = Channel::Bounded(1024);

/// Per-`Memberlist` runtime tuning knobs. Generic-free; per-backend knobs
/// live on each backend's `*TransportOptions` struct, folded into
/// `Options<T>` by `Options::new(T::Options)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
pub struct RuntimeOptions {
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  join_deadline: Duration,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  leave_timeout: Duration,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  idle_wake_interval: Duration,
  iter_drain_cap: usize,
  cmd_fairness_budget: usize,
  event_queue_cap: usize,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  peek_budget: Duration,
  observation_channel: Channel,
}

// `clap::Args` is NOT derived on `RuntimeOptions`. The derived
// `update_from_arg_matches` treats every `default_value` / `default_value_t`
// arg as present even when the operator did not pass it, so a partial
// `try_update_from` carrying one unrelated flag would reset every other
// defaulted knob to its default. A private mirror carries the `#[arg(...)]`
// attributes and derives `Args`; `RuntimeOptions` delegates its `Args` /
// `FromArgMatches` to the mirror and, on update, applies ONLY the args whose
// value came from the command line or an env var.
#[cfg(feature = "clap")]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  #[derive(Args)]
  struct RuntimeOptionsCli {
    #[arg(
      id = "runtime-join-deadline",
      long = "runtime-join-deadline",
      env = "MEMBERLIST_RUNTIME_JOIN_DEADLINE",
      value_parser = humantime::parse_duration,
      // The humantime spelling of DEFAULT_JOIN_DEADLINE (Duration has no
      // Display, so clap's default must be a string the value_parser accepts).
      default_value = "10s",
    )]
    join_deadline: Duration,
    #[arg(
      id = "runtime-leave-timeout",
      long = "runtime-leave-timeout",
      env = "MEMBERLIST_RUNTIME_LEAVE_TIMEOUT",
      value_parser = humantime::parse_duration,
      // The humantime spelling of DEFAULT_LEAVE_TIMEOUT.
      default_value = "5s",
    )]
    leave_timeout: Duration,
    #[arg(
      id = "runtime-idle-wake-interval",
      long = "runtime-idle-wake-interval",
      env = "MEMBERLIST_RUNTIME_IDLE_WAKE_INTERVAL",
      value_parser = humantime::parse_duration,
      // The humantime spelling of DEFAULT_IDLE_WAKE_INTERVAL.
      default_value = "60s",
    )]
    idle_wake_interval: Duration,
    #[arg(
      id = "runtime-iter-drain-cap",
      long = "runtime-iter-drain-cap",
      env = "MEMBERLIST_RUNTIME_ITER_DRAIN_CAP",
      default_value_t = DEFAULT_ITER_DRAIN_CAP,
    )]
    iter_drain_cap: usize,
    #[arg(
      id = "runtime-cmd-fairness-budget",
      long = "runtime-cmd-fairness-budget",
      env = "MEMBERLIST_RUNTIME_CMD_FAIRNESS_BUDGET",
      default_value_t = DEFAULT_CMD_FAIRNESS_BUDGET,
    )]
    cmd_fairness_budget: usize,
    #[arg(
      id = "runtime-event-queue-cap",
      long = "runtime-event-queue-cap",
      env = "MEMBERLIST_RUNTIME_EVENT_QUEUE_CAP",
      default_value_t = DEFAULT_EVENT_QUEUE_CAP,
    )]
    event_queue_cap: usize,
    #[arg(
      id = "runtime-peek-budget",
      long = "runtime-peek-budget",
      env = "MEMBERLIST_RUNTIME_PEEK_BUDGET",
      value_parser = humantime::parse_duration,
      // The humantime spelling of DEFAULT_PEEK_BUDGET.
      default_value = "1ms",
    )]
    peek_budget: Duration,
    #[arg(
      id = "runtime-observation-channel",
      long = "runtime-observation-channel",
      env = "MEMBERLIST_RUNTIME_OBSERVATION_CHANNEL",
      default_value_t = DEFAULT_OBSERVATION_CHANNEL,
    )]
    observation_channel: Channel,
  }

  impl From<RuntimeOptionsCli> for RuntimeOptions {
    fn from(c: RuntimeOptionsCli) -> Self {
      Self {
        join_deadline: c.join_deadline,
        leave_timeout: c.leave_timeout,
        idle_wake_interval: c.idle_wake_interval,
        iter_drain_cap: c.iter_drain_cap,
        cmd_fairness_budget: c.cmd_fairness_budget,
        event_queue_cap: c.event_queue_cap,
        peek_budget: c.peek_budget,
        observation_channel: c.observation_channel,
      }
    }
  }

  impl Args for RuntimeOptions {
    fn augment_args(cmd: Command) -> Command {
      RuntimeOptionsCli::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      RuntimeOptionsCli::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for RuntimeOptions {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      RuntimeOptionsCli::from_arg_matches(m).map(Into::into)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // Apply ONLY operator-supplied overrides — args whose value came from the
      // command line or an env var, not a clap default. A bare derived update
      // treats every `default_value` arg as present and would reset unset
      // fields.
      macro_rules! take {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            if let Some(v) = m.get_one::<$ty>($id) {
              self.$field = v.clone();
            }
          }
        };
      }
      take!("runtime-join-deadline", join_deadline, Duration);
      take!("runtime-leave-timeout", leave_timeout, Duration);
      take!("runtime-idle-wake-interval", idle_wake_interval, Duration);
      take!("runtime-iter-drain-cap", iter_drain_cap, usize);
      take!("runtime-cmd-fairness-budget", cmd_fairness_budget, usize);
      take!("runtime-event-queue-cap", event_queue_cap, usize);
      take!("runtime-peek-budget", peek_budget, Duration);
      take!("runtime-observation-channel", observation_channel, Channel);
      Ok(())
    }
  }
};

impl RuntimeOptions {
  /// Construct from the canonical base defaults.
  #[inline]
  pub const fn new() -> Self {
    Self {
      join_deadline: DEFAULT_JOIN_DEADLINE,
      leave_timeout: DEFAULT_LEAVE_TIMEOUT,
      idle_wake_interval: DEFAULT_IDLE_WAKE_INTERVAL,
      iter_drain_cap: DEFAULT_ITER_DRAIN_CAP,
      cmd_fairness_budget: DEFAULT_CMD_FAIRNESS_BUDGET,
      event_queue_cap: DEFAULT_EVENT_QUEUE_CAP,
      peek_budget: DEFAULT_PEEK_BUDGET,
      observation_channel: DEFAULT_OBSERVATION_CHANNEL,
    }
  }

  /// Builder: per-call deadline for `Memberlist::join_with`.
  #[must_use]
  #[inline]
  pub const fn with_join_deadline(mut self, d: Duration) -> Self {
    self.join_deadline = d;
    self
  }

  /// Builder: per-call deadline for `Memberlist::leave`. Bounds how
  /// long a graceful leave blocks waiting for the machine's
  /// `Event::LeftCluster` (the dead-self flush completion) before
  /// surfacing `MemberlistError::LeaveTimeout`.
  #[must_use]
  #[inline]
  pub const fn with_leave_timeout(mut self, d: Duration) -> Self {
    self.leave_timeout = d;
    self
  }

  /// Builder: fallback driver-loop sleep when the coordinator has no
  /// pending deadline.
  #[must_use]
  #[inline]
  pub const fn with_idle_wake_interval(mut self, d: Duration) -> Self {
    self.idle_wake_interval = d;
    self
  }

  /// Builder: per-iteration drain cap.
  #[must_use]
  #[inline]
  pub const fn with_iter_drain_cap(mut self, n: usize) -> Self {
    self.iter_drain_cap = n;
    self
  }

  /// Builder: per-iteration cmd-channel fairness budget.
  #[must_use]
  #[inline]
  pub const fn with_cmd_fairness_budget(mut self, n: usize) -> Self {
    self.cmd_fairness_budget = n;
    self
  }

  /// Builder: events-channel capacity.
  #[must_use]
  #[inline]
  pub const fn with_event_queue_cap(mut self, n: usize) -> Self {
    self.event_queue_cap = n;
    self
  }

  /// Builder: past-due peek budget.
  #[must_use]
  #[inline]
  pub const fn with_peek_budget(mut self, d: Duration) -> Self {
    self.peek_budget = d;
    self
  }

  /// Builder: how the delegate observation channel is bounded
  /// ([`Channel::Unbounded`] vs [`Channel::Bounded`]).
  #[must_use]
  #[inline]
  pub const fn with_observation_channel(mut self, c: Channel) -> Self {
    self.observation_channel = c;
    self
  }

  /// Per-call deadline for `Memberlist::join_with`.
  #[inline]
  pub const fn join_deadline(&self) -> Duration {
    self.join_deadline
  }

  /// Per-call deadline for `Memberlist::leave`.
  #[inline]
  pub const fn leave_timeout(&self) -> Duration {
    self.leave_timeout
  }

  /// Fallback driver-loop sleep.
  #[inline]
  pub const fn idle_wake_interval(&self) -> Duration {
    self.idle_wake_interval
  }

  /// Per-iteration drain cap.
  #[inline]
  pub const fn iter_drain_cap(&self) -> usize {
    self.iter_drain_cap
  }

  /// Per-iteration cmd-channel fairness budget.
  #[inline]
  pub const fn cmd_fairness_budget(&self) -> usize {
    self.cmd_fairness_budget
  }

  /// Events-channel capacity.
  #[inline]
  pub const fn event_queue_cap(&self) -> usize {
    self.event_queue_cap
  }

  /// Past-due peek budget.
  #[inline]
  pub const fn peek_budget(&self) -> Duration {
    self.peek_budget
  }

  /// How the delegate observation channel is bounded.
  #[inline]
  pub const fn observation_channel(&self) -> Channel {
    self.observation_channel
  }
}

impl Default for RuntimeOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// Stream-transport-specific tuning knobs.
///
/// Apply to the stream-backed (TCP / TLS) [`Memberlist`](crate::Memberlist)
/// drivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamTransportOptions {
  dial_timeout: Duration,
  close_timeout: Duration,
  bridge_inbound_cap: usize,
  bridge_recv_buf_len: usize,
}

impl StreamTransportOptions {
  /// Construct with the canonical stream-transport defaults.
  #[inline]
  pub const fn new() -> Self {
    Self {
      dial_timeout: DEFAULT_DIAL_TIMEOUT,
      close_timeout: DEFAULT_CLOSE_TIMEOUT,
      bridge_inbound_cap: DEFAULT_BRIDGE_INBOUND_CAP,
      bridge_recv_buf_len: DEFAULT_BRIDGE_RECV_BUF_LEN,
    }
  }

  /// Builder: outbound `TcpStream::connect` budget.
  #[must_use]
  #[inline]
  pub const fn with_dial_timeout(mut self, d: Duration) -> Self {
    self.dial_timeout = d;
    self
  }

  /// Builder: bound on a per-bridge graceful-drain write.
  ///
  /// After a graceful `StreamAction::Close` the bridge has no remaining cancel
  /// path, so a peer that stopped reading would otherwise wedge the drain
  /// `write_all` forever. This caps each such write: a write that makes
  /// progress never trips it; a write stalled for the full duration is
  /// abandoned and the bridge torn down (RST). See `DEFAULT_CLOSE_TIMEOUT`.
  #[must_use]
  #[inline]
  pub const fn with_close_timeout(mut self, d: Duration) -> Self {
    self.close_timeout = d;
    self
  }

  /// Builder: bridge-inbound channel capacity.
  #[must_use]
  #[inline]
  pub const fn with_bridge_inbound_cap(mut self, n: usize) -> Self {
    self.bridge_inbound_cap = n;
    self
  }

  /// Builder: per-bridge TCP read buffer size.
  #[must_use]
  #[inline]
  pub const fn with_bridge_recv_buf_len(mut self, n: usize) -> Self {
    self.bridge_recv_buf_len = n;
    self
  }

  /// Outbound `TcpStream::connect` budget.
  #[inline]
  pub const fn dial_timeout(&self) -> Duration {
    self.dial_timeout
  }

  /// Bound on a per-bridge graceful-drain write.
  #[inline]
  pub const fn close_timeout(&self) -> Duration {
    self.close_timeout
  }

  /// Bridge-inbound channel capacity.
  #[inline]
  pub const fn bridge_inbound_cap(&self) -> usize {
    self.bridge_inbound_cap
  }

  /// Per-bridge TCP read buffer size.
  #[inline]
  pub const fn bridge_recv_buf_len(&self) -> usize {
    self.bridge_recv_buf_len
  }

  /// Validate the stream-transport knobs that would DETERMINISTICALLY break
  /// (not merely degrade) a stream backend.
  ///
  /// Two knobs are rejected:
  /// - `bridge_recv_buf_len == 0`: the per-bridge byte-mover reads into a
  ///   `vec![0u8; bridge_recv_buf_len]`, and a zero-length read returns `Ok(0)`
  ///   — which the bridge treats as peer EOF. So every TCP/TLS bridge would
  ///   report EOF instead of reading reliable frames and all join / push-pull
  ///   stream traffic would deterministically break, with no error surfaced.
  /// - `close_timeout == 0`: the post-`StreamAction::Close` graceful drain
  ///   bounds each queued-response `write` with this timeout, and a zero timeout
  ///   fires immediately — so every graceful close abandons (RSTs) its queued
  ///   push/pull response bytes instead of draining them, truncating reliable
  ///   exchanges. Mirrors the smoltcp driver's `ZeroCloseTimeout` rejection.
  ///
  /// Both are rejected fail-fast at `Transport::new` (mirroring the
  /// reject-not-clamp `gossip_mtu` doctrine) rather than constructing `Ok` over
  /// a silently-broken cluster. `bridge_inbound_cap == 0` (a valid flume
  /// rendezvous channel — synchronous handoff, degraded throughput) and
  /// `dial_timeout == 0` (a loud immediate dial-timeout failure) are NOT
  /// rejected.
  ///
  /// QUIC has no bridges, so this is a stream-only knob; it lives in
  /// `T::Options` and so is not reachable at `Memberlist::new` — both stream
  /// backends call this at the top of their `Transport::new`. Compiled only for
  /// the stream transports (its only callers); a QUIC-only build omits it.
  #[cfg(any(
    feature = "tcp",
    feature = "tls-rustls-ring",
    feature = "tls-rustls-aws-lc-rs"
  ))]
  pub(crate) fn validate(&self) -> Result<(), MemberlistError> {
    if self.bridge_recv_buf_len == 0 {
      return Err(MemberlistError::InvalidOption(InvalidOption::new(
        "bridge_recv_buf_len",
        "the per-bridge reliable-stream read buffer must be nonzero: a zero-length read \
           returns Ok(0), which the bridge treats as peer EOF, so every TCP/TLS join / \
           push-pull stream exchange would break"
          .to_string(),
      )));
    }
    if self.close_timeout.is_zero() {
      return Err(MemberlistError::InvalidOption(InvalidOption::new(
        "close_timeout",
        "the reliable graceful-close drain timeout must be nonzero: a zero timeout fires \
           immediately, so a graceful close abandons (RSTs) queued push/pull response bytes \
           instead of draining them, truncating reliable exchanges"
          .to_string(),
      )));
    }
    Ok(())
  }
}

impl Default for StreamTransportOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests;
