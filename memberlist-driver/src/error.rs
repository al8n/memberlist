//! Common error payload structs shared by the memberlist async driver crates.

/// Payload for the gossip-MTU-too-small error: the configured `gossip_mtu` is
/// below the floor needed to carry the mandatory single-datagram control packets
/// (probe Ping / Ack / minimal self-Alive) the SWIM protocol always emits.
/// Carries the configured value and the required minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GossipMtuTooSmall {
  configured: usize,
  minimum: usize,
}

impl GossipMtuTooSmall {
  /// Build a new payload from the configured `gossip_mtu` and the minimum.
  #[inline]
  pub const fn new(configured: usize, minimum: usize) -> Self {
    Self {
      configured,
      minimum,
    }
  }

  /// The configured `gossip_mtu` that was rejected.
  #[must_use]
  #[inline]
  pub const fn configured(&self) -> usize {
    self.configured
  }

  /// The required minimum `gossip_mtu` — the floor that fits the mandatory
  /// single-datagram control packets the protocol always emits.
  #[must_use]
  #[inline]
  pub const fn minimum(&self) -> usize {
    self.minimum
  }
}

impl core::fmt::Display for GossipMtuTooSmall {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "gossip_mtu {} is below the minimum of {} bytes required to carry the mandatory \
       single-datagram control packets (probe Ping / Ack / a minimal self-Alive); a \
       smaller gossip_mtu would make normal probes exceed the plaintext gossip ceiling, \
       so peers would reject them and falsely suspect this node",
      self.configured, self.minimum,
    )
  }
}

/// Payload for the invalid-option error: an operator-set tuning knob was given a
/// value that would DETERMINISTICALLY break the node — an
/// accept-then-silently-fail configuration the constructor rejects rather than
/// honoring. Carries the knob name and a human-readable reason describing why the
/// value is invalid.
#[derive(Debug, Clone)]
pub struct InvalidOption {
  option: &'static str,
  reason: String,
}

impl InvalidOption {
  /// Build a new payload from the rejected knob name and the reason.
  #[inline]
  pub fn new(option: &'static str, reason: String) -> Self {
    Self { option, reason }
  }

  /// The name of the tuning knob whose value was rejected.
  #[must_use]
  #[inline]
  pub const fn option(&self) -> &'static str {
    self.option
  }

  /// The reason the value would deterministically break the node.
  #[must_use]
  #[inline]
  pub fn reason(&self) -> &str {
    &self.reason
  }
}

impl core::fmt::Display for InvalidOption {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "invalid {} option: {}", self.option, self.reason)
  }
}
