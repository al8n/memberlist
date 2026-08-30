//! `EndpointTuning` — the SWIM-behavioral subset of [`EndpointOptions`] the
//! standard drivers carry through to the machine.
//!
//! A driver's user-facing options type builds an [`EndpointOptions`] internally
//! and would otherwise only forward a handful of size knobs, leaving the probe /
//! suspicion / gossip cadence and the membership admission ceilings pinned at
//! their machine defaults. `EndpointTuning` is the shared, presence-tracking
//! override set both standard drivers embed and hand back to the machine: every
//! field is an `Option`, an unset field leaves the corresponding
//! `EndpointOptions` knob untouched (byte-identical to not carrying tuning at
//! all), and [`apply_to`](EndpointTuning::apply_to) — the single copy-through —
//! lives here so the two drivers cannot drift.
//!
//! # Coverage
//!
//! The type exposes the SWIM tuning knobs that are total inputs: any value is a
//! legal (if extreme) configuration, and a zero duration is a defined disable
//! semantic rather than a fault. Four `EndpointOptions` knobs are deliberately
//! NOT surfaced, because routing operator input to them through a driver would
//! be a footgun rather than a convenience — an embedder that genuinely needs one
//! builds its `EndpointOptions` directly:
//!
//! - `protocol_version` / `delegate_version` — wire-negotiation identity on a
//!   V1-only, no-legacy-compat protocol. A stray version byte does not tune
//!   behavior; it splits the cluster into non-interoperating halves.
//! - `initial_incarnation` — crash-restart identity, an orchestrator concern,
//!   and `with_initial_incarnation` panics above `u32::MAX / 2`. Exposing it
//!   through a driver would convert an operator's out-of-range input into a
//!   panic inside a driver's construction path.
//! - `user_broadcast_tiers` — only meaningful with
//!   `Endpoint::queue_user_broadcast_ranked`, which neither standard driver
//!   surfaces, so as a driver knob it would configure nothing.

use core::time::Duration;

use super::EndpointOptions;

/// The SWIM-behavioral override set the standard drivers carry through to the
/// machine's [`EndpointOptions`].
///
/// Every field is an `Option`: `None` (the default for all of them) leaves the
/// corresponding `EndpointOptions` knob at its machine default, so a default
/// `EndpointTuning` applied over a fresh `EndpointOptions` changes nothing.
/// Set a field with its `with_*` builder to override that one knob.
///
/// A zero `Duration` is not "unset" — it is the machine's documented disable
/// semantic for the intervals that define one (`gossip_interval` /
/// `push_pull_interval` disable that background round; `dead_node_reclaim_time`
/// of zero disables reclaim). "Unset" is `None`; "explicitly zero" is
/// `Some(Duration::ZERO)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
pub struct EndpointTuning {
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  probe_interval: Option<Duration>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  probe_timeout: Option<Duration>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  gossip_interval: Option<Duration>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  gossip_to_the_dead_time: Option<Duration>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  dead_node_reclaim_time: Option<Duration>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  push_pull_interval: Option<Duration>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  stream_timeout: Option<Duration>,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  accept_handshake_deadline: Option<Duration>,
  suspicion_mult: Option<u32>,
  suspicion_max_timeout_mult: Option<u32>,
  awareness_max_multiplier: Option<u32>,
  indirect_checks: Option<u32>,
  retransmit_mult: Option<u32>,
  gossip_nodes: Option<usize>,
  /// The membership admission ceiling. The machine default is `None`
  /// (unlimited open-join), so "unset" (leave the machine default) and
  /// "explicitly unlimited" coincide: a driver cannot express "set the ceiling
  /// to unlimited" as distinct from "do not override", but the two are the same
  /// configuration, so nothing is lost. Set `Some(n)` to cap membership at `n`.
  max_members: Option<usize>,
  max_indirect_forwards: Option<usize>,
  /// The concurrent inbound-stream ceiling. As with [`Self::max_members`], the
  /// machine default is `None` (unlimited), so "unset" and "explicitly
  /// unlimited" coincide — lossless, since they are the same configuration.
  /// Set `Some(n)` to cap concurrent inbound reliable exchanges at `n`.
  max_inbound_streams: Option<usize>,
  ack_payload_to_members_only: Option<bool>,
}

impl Default for EndpointTuning {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl EndpointTuning {
  /// A tuning set with every knob unset. Applied over an [`EndpointOptions`] it
  /// changes nothing — every knob keeps its machine default.
  #[must_use]
  #[inline]
  pub const fn new() -> Self {
    Self {
      probe_interval: None,
      probe_timeout: None,
      gossip_interval: None,
      gossip_to_the_dead_time: None,
      dead_node_reclaim_time: None,
      push_pull_interval: None,
      stream_timeout: None,
      accept_handshake_deadline: None,
      suspicion_mult: None,
      suspicion_max_timeout_mult: None,
      awareness_max_multiplier: None,
      indirect_checks: None,
      retransmit_mult: None,
      gossip_nodes: None,
      max_members: None,
      max_indirect_forwards: None,
      max_inbound_streams: None,
      ack_payload_to_members_only: None,
    }
  }

  /// Builder: override the probe interval (the SWIM protocol period).
  #[must_use]
  #[inline]
  pub const fn with_probe_interval(mut self, v: Duration) -> Self {
    self.probe_interval = Some(v);
    self
  }

  /// Builder: override the direct-ping timeout before indirect fallback.
  #[must_use]
  #[inline]
  pub const fn with_probe_timeout(mut self, v: Duration) -> Self {
    self.probe_timeout = Some(v);
    self
  }

  /// Builder: override the gossip interval. `Duration::ZERO` disables gossip.
  #[must_use]
  #[inline]
  pub const fn with_gossip_interval(mut self, v: Duration) -> Self {
    self.gossip_interval = Some(v);
    self
  }

  /// Builder: override the window during which broadcasts are still gossiped to
  /// recently-dead nodes.
  #[must_use]
  #[inline]
  pub const fn with_gossip_to_the_dead_time(mut self, v: Duration) -> Self {
    self.gossip_to_the_dead_time = Some(v);
    self
  }

  /// Builder: override the dead-node reclaim time. `Duration::ZERO` disables
  /// reclaim.
  #[must_use]
  #[inline]
  pub const fn with_dead_node_reclaim_time(mut self, v: Duration) -> Self {
    self.dead_node_reclaim_time = Some(v);
    self
  }

  /// Builder: override the push/pull anti-entropy interval. `Duration::ZERO`
  /// disables push/pull.
  #[must_use]
  #[inline]
  pub const fn with_push_pull_interval(mut self, v: Duration) -> Self {
    self.push_pull_interval = Some(v);
    self
  }

  /// Builder: override the per-stream exchange timeout.
  #[must_use]
  #[inline]
  pub const fn with_stream_timeout(mut self, v: Duration) -> Self {
    self.stream_timeout = Some(v);
    self
  }

  /// Builder: override the server-side accept handshake deadline.
  #[must_use]
  #[inline]
  pub const fn with_accept_handshake_deadline(mut self, v: Duration) -> Self {
    self.accept_handshake_deadline = Some(v);
    self
  }

  /// Builder: override the suspicion multiplier.
  #[must_use]
  #[inline]
  pub const fn with_suspicion_mult(mut self, v: u32) -> Self {
    self.suspicion_mult = Some(v);
    self
  }

  /// Builder: override the suspicion max-timeout multiplier.
  #[must_use]
  #[inline]
  pub const fn with_suspicion_max_timeout_mult(mut self, v: u32) -> Self {
    self.suspicion_max_timeout_mult = Some(v);
    self
  }

  /// Builder: override the awareness max multiplier. The machine rejects a value
  /// of `0` at construction, so a driver that reaches the machine through an
  /// infallible path validates this knob before construction.
  #[must_use]
  #[inline]
  pub const fn with_awareness_max_multiplier(mut self, v: u32) -> Self {
    self.awareness_max_multiplier = Some(v);
    self
  }

  /// Builder: override the number of indirect peers used as failed-direct-ping
  /// fallback.
  #[must_use]
  #[inline]
  pub const fn with_indirect_checks(mut self, v: u32) -> Self {
    self.indirect_checks = Some(v);
    self
  }

  /// Builder: override the broadcast retransmit multiplier.
  #[must_use]
  #[inline]
  pub const fn with_retransmit_mult(mut self, v: u32) -> Self {
    self.retransmit_mult = Some(v);
    self
  }

  /// Builder: override the number of random peers gossiped to per round.
  #[must_use]
  #[inline]
  pub const fn with_gossip_nodes(mut self, v: usize) -> Self {
    self.gossip_nodes = Some(v);
    self
  }

  /// Builder: set the membership admission ceiling to `v`. Leaving it unset
  /// keeps the machine default (unlimited); see [`Self::max_members`].
  #[must_use]
  #[inline]
  pub const fn with_max_members(mut self, v: usize) -> Self {
    self.max_members = Some(v);
    self
  }

  /// Builder: override the concurrent indirect-forward relay cap.
  #[must_use]
  #[inline]
  pub const fn with_max_indirect_forwards(mut self, v: usize) -> Self {
    self.max_indirect_forwards = Some(v);
    self
  }

  /// Builder: set the concurrent inbound-stream ceiling to `v`. Leaving it unset
  /// keeps the machine default (unlimited); see [`Self::max_inbound_streams`].
  #[must_use]
  #[inline]
  pub const fn with_max_inbound_streams(mut self, v: usize) -> Self {
    self.max_inbound_streams = Some(v);
    self
  }

  /// Builder: override whether the ack payload is restricted to known members.
  #[must_use]
  #[inline]
  pub const fn with_ack_payload_to_members_only(mut self, v: bool) -> Self {
    self.ack_payload_to_members_only = Some(v);
    self
  }

  /// The probe-interval override, if set.
  #[must_use]
  #[inline]
  pub const fn probe_interval(&self) -> Option<Duration> {
    self.probe_interval
  }

  /// The probe-timeout override, if set.
  #[must_use]
  #[inline]
  pub const fn probe_timeout(&self) -> Option<Duration> {
    self.probe_timeout
  }

  /// The gossip-interval override, if set.
  #[must_use]
  #[inline]
  pub const fn gossip_interval(&self) -> Option<Duration> {
    self.gossip_interval
  }

  /// The gossip-to-the-dead-time override, if set.
  #[must_use]
  #[inline]
  pub const fn gossip_to_the_dead_time(&self) -> Option<Duration> {
    self.gossip_to_the_dead_time
  }

  /// The dead-node-reclaim-time override, if set.
  #[must_use]
  #[inline]
  pub const fn dead_node_reclaim_time(&self) -> Option<Duration> {
    self.dead_node_reclaim_time
  }

  /// The push/pull-interval override, if set.
  #[must_use]
  #[inline]
  pub const fn push_pull_interval(&self) -> Option<Duration> {
    self.push_pull_interval
  }

  /// The stream-timeout override, if set.
  #[must_use]
  #[inline]
  pub const fn stream_timeout(&self) -> Option<Duration> {
    self.stream_timeout
  }

  /// The accept-handshake-deadline override, if set.
  #[must_use]
  #[inline]
  pub const fn accept_handshake_deadline(&self) -> Option<Duration> {
    self.accept_handshake_deadline
  }

  /// The suspicion-multiplier override, if set.
  #[must_use]
  #[inline]
  pub const fn suspicion_mult(&self) -> Option<u32> {
    self.suspicion_mult
  }

  /// The suspicion-max-timeout-multiplier override, if set.
  #[must_use]
  #[inline]
  pub const fn suspicion_max_timeout_mult(&self) -> Option<u32> {
    self.suspicion_max_timeout_mult
  }

  /// The awareness-max-multiplier override, if set.
  #[must_use]
  #[inline]
  pub const fn awareness_max_multiplier(&self) -> Option<u32> {
    self.awareness_max_multiplier
  }

  /// The indirect-checks override, if set.
  #[must_use]
  #[inline]
  pub const fn indirect_checks(&self) -> Option<u32> {
    self.indirect_checks
  }

  /// The retransmit-multiplier override, if set.
  #[must_use]
  #[inline]
  pub const fn retransmit_mult(&self) -> Option<u32> {
    self.retransmit_mult
  }

  /// The gossip-nodes override, if set.
  #[must_use]
  #[inline]
  pub const fn gossip_nodes(&self) -> Option<usize> {
    self.gossip_nodes
  }

  /// The membership admission ceiling override, if set. See
  /// [`Self::max_members`].
  #[must_use]
  #[inline]
  pub const fn max_members(&self) -> Option<usize> {
    self.max_members
  }

  /// The concurrent indirect-forward relay-cap override, if set.
  #[must_use]
  #[inline]
  pub const fn max_indirect_forwards(&self) -> Option<usize> {
    self.max_indirect_forwards
  }

  /// The concurrent inbound-stream ceiling override, if set. See
  /// [`Self::max_inbound_streams`].
  #[must_use]
  #[inline]
  pub const fn max_inbound_streams(&self) -> Option<usize> {
    self.max_inbound_streams
  }

  /// The ack-payload-to-members-only override, if set.
  #[must_use]
  #[inline]
  pub const fn ack_payload_to_members_only(&self) -> Option<bool> {
    self.ack_payload_to_members_only
  }

  /// Layer every set override onto `cfg`, returning the result. Each unset knob
  /// leaves the corresponding [`EndpointOptions`] value untouched, so a default
  /// `EndpointTuning` returns `cfg` unchanged. This is the single copy-through
  /// both standard drivers route their SWIM tuning through.
  #[must_use]
  pub fn apply_to<I, A>(&self, mut cfg: EndpointOptions<I, A>) -> EndpointOptions<I, A> {
    if let Some(v) = self.probe_interval {
      cfg = cfg.with_probe_interval(v);
    }
    if let Some(v) = self.probe_timeout {
      cfg = cfg.with_probe_timeout(v);
    }
    if let Some(v) = self.gossip_interval {
      cfg = cfg.with_gossip_interval(v);
    }
    if let Some(v) = self.gossip_to_the_dead_time {
      cfg = cfg.with_gossip_to_the_dead_time(v);
    }
    if let Some(v) = self.dead_node_reclaim_time {
      cfg = cfg.with_dead_node_reclaim_time(v);
    }
    if let Some(v) = self.push_pull_interval {
      cfg = cfg.with_push_pull_interval(v);
    }
    if let Some(v) = self.stream_timeout {
      cfg = cfg.with_stream_timeout(v);
    }
    if let Some(v) = self.accept_handshake_deadline {
      cfg = cfg.with_accept_handshake_deadline(v);
    }
    if let Some(v) = self.suspicion_mult {
      cfg = cfg.with_suspicion_mult(v);
    }
    if let Some(v) = self.suspicion_max_timeout_mult {
      cfg = cfg.with_suspicion_max_timeout_mult(v);
    }
    if let Some(v) = self.awareness_max_multiplier {
      cfg = cfg.with_awareness_max_multiplier(v);
    }
    if let Some(v) = self.indirect_checks {
      cfg = cfg.with_indirect_checks(v);
    }
    if let Some(v) = self.retransmit_mult {
      cfg = cfg.with_retransmit_mult(v);
    }
    if let Some(v) = self.gossip_nodes {
      cfg = cfg.with_gossip_nodes(v);
    }
    if let Some(v) = self.max_members {
      cfg = cfg.with_max_members(Some(v));
    }
    if let Some(v) = self.max_indirect_forwards {
      cfg = cfg.with_max_indirect_forwards(v);
    }
    if let Some(v) = self.max_inbound_streams {
      cfg = cfg.with_max_inbound_streams(Some(v));
    }
    if let Some(v) = self.ack_payload_to_members_only {
      cfg = cfg.with_ack_payload_to_members_only(v);
    }
    cfg
  }
}

// `clap::Args` is delegated to a private mirror rather than derived on the
// public struct: a derived `update_from_arg_matches` treats every arg as present
// and would reset an unset field, so a `try_update_from` carrying one unrelated
// flag would wipe every other knob. The manual `update_from_arg_matches` applies
// a field only when its value came from the command line or an env var. Every
// field here is an `Option` with no clap default, so an unset flag is a no-op on
// update. The ids/longs carry the `memberlist-` prefix so they compose with the
// drivers' `--memberlist-*` family without colliding with proto's own
// `endpoint-*` flags (which live on a different, un-composed command).
#[cfg(feature = "clap")]
#[cfg_attr(docsrs, doc(cfg(feature = "clap")))]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  #[derive(Args)]
  struct EndpointTuningCli {
    #[arg(
      id = "memberlist-probe-interval",
      long = "memberlist-probe-interval",
      env = "MEMBERLIST_PROBE_INTERVAL",
      value_parser = humantime::parse_duration
    )]
    probe_interval: Option<Duration>,
    #[arg(
      id = "memberlist-probe-timeout",
      long = "memberlist-probe-timeout",
      env = "MEMBERLIST_PROBE_TIMEOUT",
      value_parser = humantime::parse_duration
    )]
    probe_timeout: Option<Duration>,
    #[arg(
      id = "memberlist-gossip-interval",
      long = "memberlist-gossip-interval",
      env = "MEMBERLIST_GOSSIP_INTERVAL",
      value_parser = humantime::parse_duration
    )]
    gossip_interval: Option<Duration>,
    #[arg(
      id = "memberlist-gossip-to-the-dead-time",
      long = "memberlist-gossip-to-the-dead-time",
      env = "MEMBERLIST_GOSSIP_TO_THE_DEAD_TIME",
      value_parser = humantime::parse_duration
    )]
    gossip_to_the_dead_time: Option<Duration>,
    #[arg(
      id = "memberlist-dead-node-reclaim-time",
      long = "memberlist-dead-node-reclaim-time",
      env = "MEMBERLIST_DEAD_NODE_RECLAIM_TIME",
      value_parser = humantime::parse_duration
    )]
    dead_node_reclaim_time: Option<Duration>,
    #[arg(
      id = "memberlist-push-pull-interval",
      long = "memberlist-push-pull-interval",
      env = "MEMBERLIST_PUSH_PULL_INTERVAL",
      value_parser = humantime::parse_duration
    )]
    push_pull_interval: Option<Duration>,
    #[arg(
      id = "memberlist-stream-timeout",
      long = "memberlist-stream-timeout",
      env = "MEMBERLIST_STREAM_TIMEOUT",
      value_parser = humantime::parse_duration
    )]
    stream_timeout: Option<Duration>,
    #[arg(
      id = "memberlist-accept-handshake-deadline",
      long = "memberlist-accept-handshake-deadline",
      env = "MEMBERLIST_ACCEPT_HANDSHAKE_DEADLINE",
      value_parser = humantime::parse_duration
    )]
    accept_handshake_deadline: Option<Duration>,
    #[arg(
      id = "memberlist-suspicion-mult",
      long = "memberlist-suspicion-mult",
      env = "MEMBERLIST_SUSPICION_MULT"
    )]
    suspicion_mult: Option<u32>,
    #[arg(
      id = "memberlist-suspicion-max-timeout-mult",
      long = "memberlist-suspicion-max-timeout-mult",
      env = "MEMBERLIST_SUSPICION_MAX_TIMEOUT_MULT"
    )]
    suspicion_max_timeout_mult: Option<u32>,
    #[arg(
      id = "memberlist-awareness-max-multiplier",
      long = "memberlist-awareness-max-multiplier",
      env = "MEMBERLIST_AWARENESS_MAX_MULTIPLIER"
    )]
    awareness_max_multiplier: Option<u32>,
    #[arg(
      id = "memberlist-indirect-checks",
      long = "memberlist-indirect-checks",
      env = "MEMBERLIST_INDIRECT_CHECKS"
    )]
    indirect_checks: Option<u32>,
    #[arg(
      id = "memberlist-retransmit-mult",
      long = "memberlist-retransmit-mult",
      env = "MEMBERLIST_RETRANSMIT_MULT"
    )]
    retransmit_mult: Option<u32>,
    #[arg(
      id = "memberlist-gossip-nodes",
      long = "memberlist-gossip-nodes",
      env = "MEMBERLIST_GOSSIP_NODES"
    )]
    gossip_nodes: Option<usize>,
    #[arg(
      id = "memberlist-max-members",
      long = "memberlist-max-members",
      env = "MEMBERLIST_MAX_MEMBERS"
    )]
    max_members: Option<usize>,
    #[arg(
      id = "memberlist-max-indirect-forwards",
      long = "memberlist-max-indirect-forwards",
      env = "MEMBERLIST_MAX_INDIRECT_FORWARDS"
    )]
    max_indirect_forwards: Option<usize>,
    #[arg(
      id = "memberlist-max-inbound-streams",
      long = "memberlist-max-inbound-streams",
      env = "MEMBERLIST_MAX_INBOUND_STREAMS"
    )]
    max_inbound_streams: Option<usize>,
    #[arg(
      id = "memberlist-ack-payload-to-members-only",
      long = "memberlist-ack-payload-to-members-only",
      env = "MEMBERLIST_ACK_PAYLOAD_TO_MEMBERS_ONLY"
    )]
    ack_payload_to_members_only: Option<bool>,
  }

  impl From<EndpointTuningCli> for EndpointTuning {
    fn from(c: EndpointTuningCli) -> Self {
      Self {
        probe_interval: c.probe_interval,
        probe_timeout: c.probe_timeout,
        gossip_interval: c.gossip_interval,
        gossip_to_the_dead_time: c.gossip_to_the_dead_time,
        dead_node_reclaim_time: c.dead_node_reclaim_time,
        push_pull_interval: c.push_pull_interval,
        stream_timeout: c.stream_timeout,
        accept_handshake_deadline: c.accept_handshake_deadline,
        suspicion_mult: c.suspicion_mult,
        suspicion_max_timeout_mult: c.suspicion_max_timeout_mult,
        awareness_max_multiplier: c.awareness_max_multiplier,
        indirect_checks: c.indirect_checks,
        retransmit_mult: c.retransmit_mult,
        gossip_nodes: c.gossip_nodes,
        max_members: c.max_members,
        max_indirect_forwards: c.max_indirect_forwards,
        max_inbound_streams: c.max_inbound_streams,
        ack_payload_to_members_only: c.ack_payload_to_members_only,
      }
    }
  }

  impl Args for EndpointTuning {
    fn augment_args(cmd: Command) -> Command {
      EndpointTuningCli::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      EndpointTuningCli::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for EndpointTuning {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      EndpointTuningCli::from_arg_matches(m).map(Into::into)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // Apply ONLY operator-supplied overrides — args whose value came from the
      // command line or an env var, not an absent flag. Every field is an
      // `Option` with no clap default, so an unset flag leaves the current value
      // (including a builder-set one) untouched.
      macro_rules! take_opt {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            self.$field = m.get_one::<$ty>($id).copied();
          }
        };
      }
      take_opt!("memberlist-probe-interval", probe_interval, Duration);
      take_opt!("memberlist-probe-timeout", probe_timeout, Duration);
      take_opt!("memberlist-gossip-interval", gossip_interval, Duration);
      take_opt!(
        "memberlist-gossip-to-the-dead-time",
        gossip_to_the_dead_time,
        Duration
      );
      take_opt!(
        "memberlist-dead-node-reclaim-time",
        dead_node_reclaim_time,
        Duration
      );
      take_opt!(
        "memberlist-push-pull-interval",
        push_pull_interval,
        Duration
      );
      take_opt!("memberlist-stream-timeout", stream_timeout, Duration);
      take_opt!(
        "memberlist-accept-handshake-deadline",
        accept_handshake_deadline,
        Duration
      );
      take_opt!("memberlist-suspicion-mult", suspicion_mult, u32);
      take_opt!(
        "memberlist-suspicion-max-timeout-mult",
        suspicion_max_timeout_mult,
        u32
      );
      take_opt!(
        "memberlist-awareness-max-multiplier",
        awareness_max_multiplier,
        u32
      );
      take_opt!("memberlist-indirect-checks", indirect_checks, u32);
      take_opt!("memberlist-retransmit-mult", retransmit_mult, u32);
      take_opt!("memberlist-gossip-nodes", gossip_nodes, usize);
      take_opt!("memberlist-max-members", max_members, usize);
      take_opt!(
        "memberlist-max-indirect-forwards",
        max_indirect_forwards,
        usize
      );
      take_opt!("memberlist-max-inbound-streams", max_inbound_streams, usize);
      take_opt!(
        "memberlist-ack-payload-to-members-only",
        ack_payload_to_members_only,
        bool
      );
      Ok(())
    }
  }
};

#[cfg(test)]
mod tests;
