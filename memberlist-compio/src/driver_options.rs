//! Per-`Memberlist` driver tuning knobs.
//!
//! [`DriverOptions<E>`] is generic over a transport-specific knob
//! block. Two instantiations are shipped:
//! - [`StreamDriverOptions`] = `DriverOptions<StreamTransportOptions>`
//!   for the [`TcpMemberlist`](crate::TcpMemberlist) /
//!   [`TlsMemberlist`](crate::TlsMemberlist) drivers.
//! - [`QuicDriverOptions`] = `DriverOptions<QuicTransportOptions>`
//!   for the [`QuicMemberlist`](crate::QuicMemberlist) driver.
//!
//! Operators with pathological workloads can dial individual knobs
//! without forking the crate. Each `DEFAULT_*` constant is `pub` so
//! callers can derive new values from the default (e.g.
//! `DEFAULT_JOIN_DEADLINE * 3`); the type aliases' `Default` impls
//! return the canonical baseline.

use core::time::Duration;

// ---------- base defaults ----------

/// Default per-call deadline for [`Memberlist::join_with`](crate::Memberlist::join_with).
pub const DEFAULT_JOIN_DEADLINE: Duration = Duration::from_secs(10);

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

// ---------- stream-transport defaults ----------

/// Default outbound dial budget for the stream-transport driver.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Default bridge-inbound channel capacity for the stream-transport driver.
pub const DEFAULT_BRIDGE_INBOUND_CAP: usize = 1024;

/// Default per-bridge TCP read buffer size for the stream-transport driver.
pub const DEFAULT_BRIDGE_RECV_BUF_LEN: usize = 16 * 1024;

// ---------- DriverOptions<E> ----------

/// Per-`Memberlist` driver tuning knobs, generic over the
/// transport-specific knob block `E`. See [`StreamDriverOptions`] and
/// [`QuicDriverOptions`] for the ready-to-use instantiations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DriverOptions<E> {
  join_deadline: Duration,
  idle_wake_interval: Duration,
  iter_drain_cap: usize,
  cmd_fairness_budget: usize,
  event_queue_cap: usize,
  peek_budget: Duration,
  transport_options: E,
}

impl<E> DriverOptions<E> {
  /// Construct from the canonical base defaults plus a caller-supplied
  /// transport options value.
  #[inline]
  pub const fn new(transport_options: E) -> Self {
    Self {
      join_deadline: DEFAULT_JOIN_DEADLINE,
      idle_wake_interval: DEFAULT_IDLE_WAKE_INTERVAL,
      iter_drain_cap: DEFAULT_ITER_DRAIN_CAP,
      cmd_fairness_budget: DEFAULT_CMD_FAIRNESS_BUDGET,
      event_queue_cap: DEFAULT_EVENT_QUEUE_CAP,
      peek_budget: DEFAULT_PEEK_BUDGET,
      transport_options,
    }
  }

  /// Builder: per-call deadline for `Memberlist::join_with`.
  #[must_use]
  #[inline]
  pub const fn with_join_deadline(mut self, d: Duration) -> Self {
    self.join_deadline = d;
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

  /// Builder: per-iteration drain cap for inbound surfaces.
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

  /// Builder: replace the transport-specific knob block.
  #[must_use]
  #[inline]
  pub fn with_transport_options(mut self, opts: E) -> Self {
    self.transport_options = opts;
    self
  }

  /// Per-call deadline for `Memberlist::join_with`.
  #[inline]
  pub const fn join_deadline(&self) -> Duration {
    self.join_deadline
  }

  /// Fallback driver-loop sleep when the coordinator has no pending
  /// deadline.
  #[inline]
  pub const fn idle_wake_interval(&self) -> Duration {
    self.idle_wake_interval
  }

  /// Per-iteration drain cap for inbound surfaces.
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

  /// Transport-specific knob block (by reference).
  #[inline]
  pub const fn transport_options(&self) -> &E {
    &self.transport_options
  }
}

impl<E: Default> Default for DriverOptions<E> {
  #[inline]
  fn default() -> Self {
    Self::new(E::default())
  }
}

// ---------- StreamTransportOptions ----------

/// Stream-transport-specific tuning knobs.
///
/// Apply to the [`TcpMemberlist`](crate::TcpMemberlist) /
/// [`TlsMemberlist`](crate::TlsMemberlist) drivers; see
/// [`StreamDriverOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamTransportOptions {
  dial_timeout: Duration,
  bridge_inbound_cap: usize,
  bridge_recv_buf_len: usize,
}

impl StreamTransportOptions {
  /// Construct with the canonical stream-transport defaults.
  #[inline]
  pub const fn new() -> Self {
    Self {
      dial_timeout: DEFAULT_DIAL_TIMEOUT,
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
}

impl Default for StreamTransportOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// [`DriverOptions`] for the stream-transport (TCP / TLS) drivers.
pub type StreamDriverOptions = DriverOptions<StreamTransportOptions>;

// ---------- QuicTransportOptions ----------

/// QUIC-specific tuning knobs.
///
/// Unit-shaped today — knobs are added on concrete need. Apply to the
/// QUIC memberlist driver via [`QuicDriverOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct QuicTransportOptions {}

impl QuicTransportOptions {
  /// Construct.
  #[inline]
  pub const fn new() -> Self {
    Self {}
  }
}

/// [`DriverOptions`] for the QUIC driver.
pub type QuicDriverOptions = DriverOptions<QuicTransportOptions>;
