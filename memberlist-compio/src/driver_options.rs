//! Per-`Memberlist` driver tuning knobs.
//!
//! All defaults match the hand-picked constants the driver uses for
//! LAN-clustered SWIM with moderate concurrency. Operators with
//! pathological workloads (very high in-flight reliable-stream counts,
//! tight latency budgets, etc.) can dial individual knobs without
//! forking the crate.
//!
//! Each knob's `DEFAULT_*` constant is `pub` so callers can derive new
//! values from the default (e.g. `DEFAULT_JOIN_DEADLINE * 3`) and
//! `DriverOptions::default()` returns the canonical baseline.

use core::time::Duration;

/// Default per-call deadline for [`Memberlist::join_with`](crate::Memberlist::join_with).
/// 10s comfortably covers a healthy seed's push/pull plus pathological
/// latency margin; the caller can race the returned future against
/// their own timer for a tighter bound.
pub const DEFAULT_JOIN_DEADLINE: Duration = Duration::from_secs(10);

/// Default fallback sleep when the coordinator's `poll_timeout` returns
/// `None`. Long enough to avoid wake-storms on an idle endpoint, short
/// enough to bound how stale a snapshot may get if every other arm is
/// quiet — defence-in-depth, every state-affecting event already drives
/// an immediate republish via the `dirty` flag.
pub const DEFAULT_IDLE_WAKE_INTERVAL: Duration = Duration::from_secs(60);

/// Default outbound dial budget. Bounds the time the driver waits on
/// `TcpStream::connect` before reporting failure. 5s is comfortably
/// above typical LAN connect latency (~1ms) while keeping unreachable
/// peers from stranding a dial task.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Default bridge-inbound channel capacity. Each per-bridge byte-mover
/// task pushes `Bytes` / `Eof` / `Error` messages into this channel; the
/// driver iter-top drain consumes them. Backlog beyond this bound
/// blocks the producing bridge task (back-pressuring the read half)
/// until the driver catches up.
pub const DEFAULT_BRIDGE_INBOUND_CAP: usize = 1024;

/// Default per-iteration drain cap for the bridge_inbound and
/// bridge_ready channels. Keeps a busy reliable-stream workload from
/// starving the recv / accept / cmd arms; the past-due path and the
/// select timer arm bypass this cap (drain to empty) to keep deadline
/// accounting correct under burst load.
pub const DEFAULT_ITER_DRAIN_CAP: usize = 256;

/// Default per-iteration cmd-channel fairness budget. The cmd arm sits
/// at MEDIUM priority in `select_biased!`; the iter-top drain pulls up
/// to this many commands via `try_recv` so a user-side flood always
/// makes bounded progress, capped so the cmd flood itself does not
/// starve the network arms.
pub const DEFAULT_CMD_FAIRNESS_BUDGET: usize = 4;

/// Default per-bridge TCP read buffer size. Sized to match typical
/// jumbo-frame payloads with headroom for small push/pull responses.
/// One allocation per read syscall in each bridge task.
pub const DEFAULT_BRIDGE_RECV_BUF_LEN: usize = 16 * 1024;

/// Default events-channel capacity. Bounded so a slow / stuck
/// subscriber cannot accumulate events without limit. `try_send`
/// drops the newest event on full and increments
/// [`Memberlist::events_dropped`](crate::Memberlist::events_dropped) so
/// subscribers can detect the gap.
pub const DEFAULT_EVENT_QUEUE_CAP: usize = 1024;

/// Default past-due peek budget. Maximum time the past-due preemption
/// path waits for a kernel-buffered gossip datagram before falling
/// through to `handle_timeout`. 1ms is comfortably above io_uring's
/// completion latency for a buffered recv (~100µs) while keeping the
/// per-iteration cost negligible.
pub const DEFAULT_PEEK_BUDGET: Duration = Duration::from_millis(1);

/// Per-`Memberlist` driver tuning knobs.
///
/// Constructed via [`Self::new`] (defaults) and tuned via the
/// `with_*` builder methods. Pass to
/// [`TcpMemberlist::new_with_options`](crate::TcpMemberlist::new_with_options)
/// or
/// [`TlsMemberlist::new_with_options`](crate::TlsMemberlist::new_with_options)
/// to install. Cheap to clone (Copy-of-scalars under the hood) — callers
/// often construct one shared instance and pass it to every memberlist
/// they spawn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DriverOptions {
  join_deadline: Duration,
  idle_wake_interval: Duration,
  dial_timeout: Duration,
  bridge_inbound_cap: usize,
  iter_drain_cap: usize,
  cmd_fairness_budget: usize,
  bridge_recv_buf_len: usize,
  event_queue_cap: usize,
  peek_budget: Duration,
}

impl DriverOptions {
  /// Construct with the canonical defaults — equivalent to
  /// `DriverOptions::default()`.
  #[inline]
  pub const fn new() -> Self {
    Self {
      join_deadline: DEFAULT_JOIN_DEADLINE,
      idle_wake_interval: DEFAULT_IDLE_WAKE_INTERVAL,
      dial_timeout: DEFAULT_DIAL_TIMEOUT,
      bridge_inbound_cap: DEFAULT_BRIDGE_INBOUND_CAP,
      iter_drain_cap: DEFAULT_ITER_DRAIN_CAP,
      cmd_fairness_budget: DEFAULT_CMD_FAIRNESS_BUDGET,
      bridge_recv_buf_len: DEFAULT_BRIDGE_RECV_BUF_LEN,
      event_queue_cap: DEFAULT_EVENT_QUEUE_CAP,
      peek_budget: DEFAULT_PEEK_BUDGET,
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

  /// Builder: per-iteration drain cap for bridge_inbound /
  /// bridge_ready.
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

  /// Builder: per-bridge TCP read buffer size.
  #[must_use]
  #[inline]
  pub const fn with_bridge_recv_buf_len(mut self, n: usize) -> Self {
    self.bridge_recv_buf_len = n;
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

  /// Per-iteration drain cap for bridge_inbound / bridge_ready.
  #[inline]
  pub const fn iter_drain_cap(&self) -> usize {
    self.iter_drain_cap
  }

  /// Per-iteration cmd-channel fairness budget.
  #[inline]
  pub const fn cmd_fairness_budget(&self) -> usize {
    self.cmd_fairness_budget
  }

  /// Per-bridge TCP read buffer size.
  #[inline]
  pub const fn bridge_recv_buf_len(&self) -> usize {
    self.bridge_recv_buf_len
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
}

impl Default for DriverOptions {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}
