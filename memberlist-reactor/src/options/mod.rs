//! Configuration: the [`Options`] umbrella plus its [`MemberlistOptions`]
//! (SWIM-level machine knobs) and [`RuntimeOptions`] (runtime tuning).

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
#[cfg(checksum)]
use memberlist_proto::ChecksumOptions;
#[cfg(compression)]
use memberlist_proto::CompressionOptions;
#[cfg(encryption)]
use memberlist_proto::EncryptionOptions;
use memberlist_proto::{AliveDelegate, MergeDelegate, label::validate_label, typed::Meta};

use crate::cidr::CidrFilter;

/// SWIM-protocol-level overrides applied to the machine-layer
/// [`EndpointOptions`](memberlist_proto::EndpointOptions) at construction.
///
/// Every field is an override layered over the `EndpointOptions` default: a
/// `None` leaves that knob at its machine default.
///
/// - `gossip_mtu` — the outbound gossip-datagram cap (and the recv-buffer sizing
///   that tracks it). For the QUIC backend it must also fit the connection's
///   `max_datagram_size`.
/// - `meta_max_size` — the local node's `Meta` byte ceiling (a broadcast-size
///   cap, not a peer-rejection filter).
/// - `max_stream_frame_size` — the reliable-stream frame ceiling.
/// - `initial_meta` — the local node's initial metadata payload.
/// - `initial_local_state` — the local node's initial push/pull state snapshot.
/// - `compression` — the initial gossip+stream compression policy (disabled by
///   default). The machine exposes a runtime setter for the post-start case.
/// - `checksum` — the initial gossip (unreliable) checksum policy (disabled by
///   default). Applied to the gossip datagram path only — NOT the reliable
///   stream path, whose transport already provides integrity. The machine
///   exposes a runtime setter for the post-start case.
/// - `encryption` — the initial gossip+stream encryption policy (disabled /
///   no keyring by default). Callers that supply a keyring should verify it
///   before construction; the driver returns `Error::Encryption` if the keyring
///   cannot be activated.
/// - `label` — cluster label applied to both the gossip and reliable planes
///   (no label by default). Validated at the setter: must be ≤253 bytes and
///   valid UTF-8. Feeds the reliable-plane `LabelOptions` (TCP/TLS) and the
///   gossip codec `EncodeOptions` from a single source. QUIC clusters use the
///   SNI hostname for isolation and do not consult this field.
/// - `skip_inbound_label_check` — when `true`, an inbound stream that presents
///   no label header is accepted rather than rejected. Defaults to `false`.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
pub struct MemberlistOptions {
  gossip_mtu: Option<usize>,
  meta_max_size: Option<usize>,
  max_stream_frame_size: Option<usize>,
  // A binary metadata payload: not a sensible config-file or CLI value, so it is
  // left to the validating `with_initial_meta` builder and skipped by both
  // layers (`Option<Meta>` defaults to `None` on deserialize).
  #[cfg_attr(feature = "serde", serde(skip))]
  initial_meta: Option<Meta>,
  // A binary push/pull state snapshot, skipped for the same reason as
  // `initial_meta`.
  #[cfg_attr(feature = "serde", serde(skip))]
  initial_local_state: Option<Bytes>,
  #[cfg(compression)]
  compression: CompressionOptions,
  #[cfg(checksum)]
  checksum: ChecksumOptions,
  #[cfg(encryption)]
  encryption: EncryptionOptions,
  // The cluster label is the only isolation between two clusters that otherwise
  // share a transport, so it must be config/CLI-settable — but only as a
  // VALIDATED value. A label is UTF-8 (`validate_label`), so it rides as a
  // string: serde de/serializes it through `label_serde` (deserialize validates
  // ≤253 bytes + UTF-8, empty → `None`; serialize renders the `Bytes` back as a
  // `&str`), and clap parses it through `parse_label` (the same `validate_label`
  // the `with_label` builder runs). The field stays `Option<Bytes>` so clap wraps
  // the parsed value and an unset label is `None`.
  #[cfg_attr(
    feature = "serde",
    serde(default, with = "label_serde", skip_serializing_if = "Option::is_none")
  )]
  label: Option<Bytes>,
  skip_inbound_label_check: bool,
}

// `MemberlistOptions` does not derive `clap::Args` directly: clap's
// `default_value` makes a defaulted arg appear "present" during an update, so a
// derived `update_from_arg_matches` would reset unset fields. A `try_update_from`
// carrying one unrelated flag would then wipe every other field — including the
// flattened child options. Instead a private mirror derives `Args`, and the
// public struct applies ONLY operator-supplied overrides (`value_source` =
// command line or env), delegating each flattened child to its own
// `update_from_arg_matches` so the same gate protects the child's fields.
#[cfg(feature = "clap")]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  #[derive(Args)]
  struct MemberlistOptionsCli {
    #[arg(
      id = "memberlist-gossip-mtu",
      long = "memberlist-gossip-mtu",
      env = "MEMBERLIST_GOSSIP_MTU"
    )]
    gossip_mtu: Option<usize>,
    #[arg(
      id = "memberlist-meta-max-size",
      long = "memberlist-meta-max-size",
      env = "MEMBERLIST_META_MAX_SIZE"
    )]
    meta_max_size: Option<usize>,
    #[arg(
      id = "memberlist-max-stream-frame-size",
      long = "memberlist-max-stream-frame-size",
      env = "MEMBERLIST_MAX_STREAM_FRAME_SIZE"
    )]
    max_stream_frame_size: Option<usize>,
    #[cfg(compression)]
    #[command(flatten)]
    compression: CompressionOptions,
    #[cfg(checksum)]
    #[command(flatten)]
    checksum: ChecksumOptions,
    #[cfg(encryption)]
    #[command(flatten)]
    encryption: EncryptionOptions,
    #[arg(
      id = "memberlist-label",
      long = "memberlist-label",
      env = "MEMBERLIST_LABEL",
      value_parser = parse_label,
    )]
    label: Option<Bytes>,
    #[arg(
      id = "memberlist-skip-inbound-label-check",
      long = "memberlist-skip-inbound-label-check",
      env = "MEMBERLIST_SKIP_INBOUND_LABEL_CHECK"
    )]
    skip_inbound_label_check: bool,
  }

  impl From<MemberlistOptionsCli> for MemberlistOptions {
    fn from(c: MemberlistOptionsCli) -> Self {
      Self {
        gossip_mtu: c.gossip_mtu,
        meta_max_size: c.meta_max_size,
        max_stream_frame_size: c.max_stream_frame_size,
        initial_meta: None,
        initial_local_state: None,
        #[cfg(compression)]
        compression: c.compression,
        #[cfg(checksum)]
        checksum: c.checksum,
        #[cfg(encryption)]
        encryption: c.encryption,
        label: c.label,
        skip_inbound_label_check: c.skip_inbound_label_check,
      }
    }
  }

  impl Args for MemberlistOptions {
    fn augment_args(cmd: Command) -> Command {
      MemberlistOptionsCli::augment_args(cmd)
    }

    fn augment_args_for_update(cmd: Command) -> Command {
      MemberlistOptionsCli::augment_args_for_update(cmd)
    }
  }

  impl FromArgMatches for MemberlistOptions {
    fn from_arg_matches(m: &ArgMatches) -> Result<Self, Error> {
      MemberlistOptionsCli::from_arg_matches(m).map(Into::into)
    }

    fn update_from_arg_matches(&mut self, m: &ArgMatches) -> Result<(), Error> {
      // Apply ONLY operator-supplied overrides — args whose value came from the
      // command line or an env var, not a clap default. The `Option` and label
      // fields have no clap default, so the gate is a no-op when they are unset
      // and re-applies them when supplied; the `skip_inbound_label_check` bool is
      // gated the same way.
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
      macro_rules! take_opt {
        ($id:literal, $field:ident, $ty:ty) => {
          if matches!(
            m.value_source($id),
            Some(ValueSource::CommandLine) | Some(ValueSource::EnvVariable)
          ) {
            self.$field = m.get_one::<$ty>($id).cloned();
          }
        };
      }
      take_opt!("memberlist-gossip-mtu", gossip_mtu, usize);
      take_opt!("memberlist-meta-max-size", meta_max_size, usize);
      take_opt!(
        "memberlist-max-stream-frame-size",
        max_stream_frame_size,
        usize
      );
      take_opt!("memberlist-label", label, Bytes);
      take!(
        "memberlist-skip-inbound-label-check",
        skip_inbound_label_check,
        bool
      );
      // Each flattened child applies its own operator-supplied overrides through
      // its own `value_source` gate, so an unrelated parent flag never resets a
      // child's fields.
      #[cfg(compression)]
      self.compression.update_from_arg_matches(m)?;
      #[cfg(checksum)]
      self.checksum.update_from_arg_matches(m)?;
      #[cfg(encryption)]
      self.encryption.update_from_arg_matches(m)?;
      Ok(())
    }
  }
};

/// Parse a cluster label from a CLI flag / env var, running the SAME
/// [`validate_label`] the [`with_label`](MemberlistOptions::with_label) builder
/// enforces (≤253 bytes, valid UTF-8). An empty string normalizes to "no label"
/// (`None` once clap wraps it), matching the builder's empty-slice handling.
#[cfg(feature = "clap")]
fn parse_label(s: &str) -> Result<Bytes, String> {
  if s.is_empty() {
    return Ok(Bytes::new());
  }
  validate_label(s.as_bytes()).map_err(|e| e.to_string())?;
  Ok(Bytes::copy_from_slice(s.as_bytes()))
}

/// Serde shim for the `label: Option<Bytes>` field. A label is UTF-8, so it
/// rides as an `Option<String>`: deserialize validates it through the SAME
/// [`validate_label`] the builder runs (rejecting a >253-byte or non-UTF-8 label
/// with a serde error) and normalizes an empty / absent value to `None`;
/// serialize renders the stored `Bytes` back as a `&str`.
#[cfg(feature = "serde")]
mod label_serde {
  use super::{Bytes, validate_label};
  use serde::{Deserialize, Deserializer, Serializer};

  pub(super) fn serialize<S>(label: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    match label {
      // The stored bytes are validated UTF-8, so this never fails.
      Some(bytes) => serializer.serialize_some(
        core::str::from_utf8(bytes)
          .map_err(|_| serde::ser::Error::custom("stored cluster label is not valid UTF-8"))?,
      ),
      None => serializer.serialize_none(),
    }
  }

  pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
  where
    D: Deserializer<'de>,
  {
    let Some(label) = Option::<String>::deserialize(deserializer)? else {
      return Ok(None);
    };
    if label.is_empty() {
      return Ok(None);
    }
    validate_label(label.as_bytes()).map_err(serde::de::Error::custom)?;
    Ok(Some(Bytes::from(label.into_bytes())))
  }
}

impl MemberlistOptions {
  /// Options with every knob left at its machine default.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Sets the outbound gossip-datagram cap.
  #[must_use]
  pub fn with_gossip_mtu(mut self, mtu: usize) -> Self {
    self.gossip_mtu = Some(mtu);
    self
  }

  /// Sets the local node's `Meta` byte ceiling.
  #[must_use]
  pub fn with_meta_max_size(mut self, size: usize) -> Self {
    self.meta_max_size = Some(size);
    self
  }

  /// Sets the reliable-stream frame ceiling.
  #[must_use]
  pub fn with_max_stream_frame_size(mut self, size: usize) -> Self {
    self.max_stream_frame_size = Some(size);
    self
  }

  /// Sets the local node's initial metadata payload.
  #[must_use]
  pub fn with_initial_meta(mut self, meta: Meta) -> Self {
    self.initial_meta = Some(meta);
    self
  }

  /// Sets the local node's initial push/pull state snapshot.
  #[must_use]
  pub fn with_initial_local_state(mut self, state: Bytes) -> Self {
    self.initial_local_state = Some(state);
    self
  }

  /// Sets the initial gossip+stream compression policy.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  #[must_use]
  pub fn with_compression(mut self, compression: CompressionOptions) -> Self {
    self.compression = compression;
    self
  }

  /// Sets the initial gossip (unreliable) checksum policy.
  ///
  /// Checksumming is applied to the gossip datagram path only — NOT the
  /// reliable stream path, whose transport already provides integrity.
  #[cfg(checksum)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3"
    )))
  )]
  #[must_use]
  pub fn with_checksum(mut self, checksum: ChecksumOptions) -> Self {
    self.checksum = checksum;
    self
  }

  /// Sets the initial gossip+stream encryption policy.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  #[must_use]
  pub fn with_encryption(mut self, encryption: EncryptionOptions) -> Self {
    self.encryption = encryption;
    self
  }

  /// Sets the cluster label for the gossip and reliable planes.
  ///
  /// The label is validated immediately: it must be ≤253 bytes and valid
  /// UTF-8. An empty slice normalizes to `None` (no label). Returns
  /// `Err(Error::InvalidLabel(_))` when either constraint is violated.
  ///
  /// The validated label is the single source for both the reliable-plane
  /// `LabelOptions` (plain TCP or TLS) and the gossip codec `EncodeOptions`,
  /// so the two planes cannot diverge.
  pub fn with_label(mut self, label: Option<Vec<u8>>) -> Result<Self, crate::error::Error> {
    self.label = match label {
      None => None,
      Some(v) if v.is_empty() => None,
      Some(v) => {
        validate_label(&v).map_err(crate::error::Error::InvalidLabel)?;
        Some(Bytes::from(v))
      }
    };
    Ok(self)
  }

  /// Suppresses the inbound reliable-plane label check.
  ///
  /// When set, an inbound TCP/TLS stream that presents no label header is
  /// accepted rather than rejected. Defaults to `false`. Faithful to
  /// memberlist-core `Options::skip_inbound_label_check`.
  #[must_use]
  pub fn with_skip_inbound_label_check(mut self, skip: bool) -> Self {
    self.skip_inbound_label_check = skip;
    self
  }

  /// The gossip-MTU override, if set.
  #[must_use]
  pub const fn gossip_mtu(&self) -> Option<usize> {
    self.gossip_mtu
  }

  /// The `Meta` byte-ceiling override, if set.
  #[must_use]
  pub const fn meta_max_size(&self) -> Option<usize> {
    self.meta_max_size
  }

  /// The reliable-stream frame-size override, if set.
  #[must_use]
  pub const fn max_stream_frame_size(&self) -> Option<usize> {
    self.max_stream_frame_size
  }

  /// The configured initial `Meta`, if set.
  #[must_use]
  pub fn initial_meta(&self) -> Option<&Meta> {
    self.initial_meta.as_ref()
  }

  /// The configured initial local-state snapshot, if set.
  #[must_use]
  pub fn initial_local_state(&self) -> Option<&Bytes> {
    self.initial_local_state.as_ref()
  }

  /// The initial gossip+stream compression policy.
  #[cfg(compression)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "lz4",
      feature = "snappy",
      feature = "zstd",
      feature = "brotli"
    )))
  )]
  #[must_use]
  pub fn compression(&self) -> &CompressionOptions {
    &self.compression
  }

  /// The initial gossip (unreliable) checksum policy.
  ///
  /// Applied to the gossip datagram path only — NOT the reliable stream path.
  #[cfg(checksum)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "crc32",
      feature = "xxhash32",
      feature = "xxhash64",
      feature = "xxhash3",
      feature = "murmur3"
    )))
  )]
  #[must_use]
  pub fn checksum(&self) -> &ChecksumOptions {
    &self.checksum
  }

  /// The initial gossip+stream encryption policy.
  #[cfg(encryption)]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305")))
  )]
  #[must_use]
  pub fn encryption(&self) -> &EncryptionOptions {
    &self.encryption
  }

  /// The cluster label, if set.
  #[must_use]
  pub fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// Whether the inbound reliable-plane label check is suppressed.
  #[must_use]
  pub const fn skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }
}

#[cfg(test)]
mod label_tests;

/// The default bounded observation-channel capacity.
pub const DEFAULT_OBSERVATION_CHANNEL_CAPACITY: usize = 1024;

/// Capacity policy for the bounded observation channel carrying machine
/// [`Event`](memberlist_proto::Event)s to the observation task.
///
/// As a config value or CLI flag the policy parses from a string
/// ([`Channel::from_str`]): `"unbounded"` selects [`Channel::Unbounded`], and a
/// bare unsigned integer selects [`Channel::Bounded`] of that capacity (`"1024"`
/// → `Bounded(1024)`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Channel {
  /// A bounded channel of the given capacity; when full, the driver yields and
  /// retries rather than blocking the pump, dropping only as a last resort.
  Bounded(usize),
  /// An unbounded channel — never drops, at the cost of unbounded memory if the
  /// observation task falls behind.
  Unbounded,
}

impl Default for Channel {
  fn default() -> Self {
    Self::Bounded(DEFAULT_OBSERVATION_CHANNEL_CAPACITY)
  }
}

impl core::fmt::Display for Channel {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Bounded(capacity) => write!(f, "{capacity}"),
      Self::Unbounded => f.write_str("unbounded"),
    }
  }
}

/// Parse a [`Channel`] from a string — a config value or CLI flag.
/// `"unbounded"` is [`Channel::Unbounded`]; a bare unsigned integer is
/// [`Channel::Bounded`] of that capacity. Any other input is a
/// [`ParseChannelError`].
impl core::str::FromStr for Channel {
  type Err = ParseChannelError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    if s == "unbounded" {
      return Ok(Self::Unbounded);
    }
    s.parse::<usize>()
      .map(Self::Bounded)
      .map_err(|_| ParseChannelError(()))
  }
}

/// The error from [`Channel::from_str`]: the input was neither `"unbounded"`
/// nor a bare unsigned integer.
///
/// Opaque — the private unit field seals construction to this module, so the
/// error can gain detail later without a breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid observation channel (expected \"unbounded\" or an unsigned integer capacity)")]
pub struct ParseChannelError(());

/// The default membership `EventStream` capacity.
pub const DEFAULT_EVENT_STREAM_CAPACITY: usize = 1024;

/// The default per-poll gossip-recv fairness bound.
pub const DEFAULT_RECV_BATCH: usize = 64;

/// The default per-poll transmit-drain fairness bound.
pub const DEFAULT_TRANSMIT_BATCH: usize = 64;

/// The default join deadline.
pub const DEFAULT_JOIN_DEADLINE: Duration = Duration::from_secs(10);

/// The default per-bridge graceful-drain write bound.
pub const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Runtime tuning.
///
/// The reactor driver wakes via shared state + a stored waker rather than a
/// command channel, so it has no command-fairness or completion-peek knobs. What
/// remains: the observation-channel policy, the membership `EventStream`
/// capacity, the per-poll work bounds that keep the pump fair under a gossip
/// flood, and the default join deadline.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
pub struct RuntimeOptions {
  observation_channel: Channel,
  event_stream_capacity: usize,
  recv_batch: usize,
  transmit_batch: usize,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  join_deadline: Duration,
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  close_timeout: Duration,
}

// `RuntimeOptions` does not derive `clap::Args` directly: clap's `default_value`
// makes every defaulted arg appear "present" during an update, so a derived
// `update_from_arg_matches` would reset unset fields to their defaults. A
// `try_update_from` carrying one unrelated flag would then wipe every other
// field. Instead a private mirror derives `Args`, and the public struct applies
// ONLY operator-supplied overrides (`value_source` = command line or env).
#[cfg(feature = "clap")]
const _: () = {
  use clap::{ArgMatches, Args, Command, Error, FromArgMatches, parser::ValueSource};

  #[derive(Args)]
  struct RuntimeOptionsCli {
    #[arg(
      id = "runtime-observation-channel",
      long = "runtime-observation-channel",
      env = "MEMBERLIST_RUNTIME_OBSERVATION_CHANNEL",
      default_value = "1024"
    )]
    observation_channel: Channel,
    #[arg(
      id = "runtime-event-stream-capacity",
      long = "runtime-event-stream-capacity",
      env = "MEMBERLIST_RUNTIME_EVENT_STREAM_CAPACITY",
      default_value_t = DEFAULT_EVENT_STREAM_CAPACITY
    )]
    event_stream_capacity: usize,
    #[arg(
      id = "runtime-recv-batch",
      long = "runtime-recv-batch",
      env = "MEMBERLIST_RUNTIME_RECV_BATCH",
      default_value_t = DEFAULT_RECV_BATCH
    )]
    recv_batch: usize,
    #[arg(
      id = "runtime-transmit-batch",
      long = "runtime-transmit-batch",
      env = "MEMBERLIST_RUNTIME_TRANSMIT_BATCH",
      default_value_t = DEFAULT_TRANSMIT_BATCH
    )]
    transmit_batch: usize,
    #[arg(
      id = "runtime-join-deadline",
      long = "runtime-join-deadline",
      env = "MEMBERLIST_RUNTIME_JOIN_DEADLINE",
      value_parser = humantime::parse_duration,
      default_value = "10s"
    )]
    join_deadline: Duration,
    #[arg(
      id = "runtime-close-timeout",
      long = "runtime-close-timeout",
      env = "MEMBERLIST_RUNTIME_CLOSE_TIMEOUT",
      value_parser = humantime::parse_duration,
      default_value = "10s"
    )]
    close_timeout: Duration,
  }

  impl From<RuntimeOptionsCli> for RuntimeOptions {
    fn from(c: RuntimeOptionsCli) -> Self {
      Self {
        observation_channel: c.observation_channel,
        event_stream_capacity: c.event_stream_capacity,
        recv_batch: c.recv_batch,
        transmit_batch: c.transmit_batch,
        join_deadline: c.join_deadline,
        close_timeout: c.close_timeout,
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
      // treats every `default_value` arg as present and would reset unset fields.
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
      take!("runtime-observation-channel", observation_channel, Channel);
      take!(
        "runtime-event-stream-capacity",
        event_stream_capacity,
        usize
      );
      take!("runtime-recv-batch", recv_batch, usize);
      take!("runtime-transmit-batch", transmit_batch, usize);
      take!("runtime-join-deadline", join_deadline, Duration);
      take!("runtime-close-timeout", close_timeout, Duration);
      Ok(())
    }
  }
};

impl Default for RuntimeOptions {
  fn default() -> Self {
    Self {
      observation_channel: Channel::default(),
      event_stream_capacity: DEFAULT_EVENT_STREAM_CAPACITY,
      recv_batch: DEFAULT_RECV_BATCH,
      transmit_batch: DEFAULT_TRANSMIT_BATCH,
      join_deadline: DEFAULT_JOIN_DEADLINE,
      // Bound a per-bridge graceful-drain write. After a graceful
      // `StreamAction::Close` the bridge has no remaining cancel path, so a
      // peer that stopped reading would otherwise wedge the drain `write_all`
      // forever — leaking the detached bridge task and its socket. A write
      // that makes progress never trips this; only a write stalled for the
      // full duration is abandoned and the bridge torn down (RST). 10s mirrors
      // the smoltcp driver's `Config::close_timeout`.
      close_timeout: DEFAULT_CLOSE_TIMEOUT,
    }
  }
}

impl RuntimeOptions {
  /// Runtime options with default tuning.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Sets the observation-channel capacity policy.
  #[must_use]
  pub fn with_observation_channel(mut self, channel: Channel) -> Self {
    self.observation_channel = channel;
    self
  }

  /// Sets the membership `EventStream` capacity.
  #[must_use]
  pub fn with_event_stream_capacity(mut self, capacity: usize) -> Self {
    self.event_stream_capacity = capacity;
    self
  }

  /// Sets the max gossip packets received per poll iteration (fairness bound).
  #[must_use]
  pub fn with_recv_batch(mut self, batch: usize) -> Self {
    self.recv_batch = batch;
    self
  }

  /// Sets the max transmits drained per poll iteration (fairness bound).
  #[must_use]
  pub fn with_transmit_batch(mut self, batch: usize) -> Self {
    self.transmit_batch = batch;
    self
  }

  /// Sets the default join deadline.
  #[must_use]
  pub fn with_join_deadline(mut self, deadline: Duration) -> Self {
    self.join_deadline = deadline;
    self
  }

  /// Sets the bound on a per-bridge graceful-drain write.
  ///
  /// After a graceful `StreamAction::Close` the reliable bridge has no
  /// remaining cancel path, so a peer that stopped reading would otherwise
  /// wedge the drain `write_all` forever. This caps each such write: a write
  /// that makes progress never trips it; a write stalled for the full duration
  /// is abandoned and the bridge torn down (RST). Mirrors the smoltcp driver's
  /// `Config::close_timeout` (default 10s).
  #[must_use]
  pub fn with_close_timeout(mut self, timeout: Duration) -> Self {
    self.close_timeout = timeout;
    self
  }

  /// The observation-channel capacity policy.
  #[must_use]
  pub const fn observation_channel(&self) -> Channel {
    self.observation_channel
  }

  /// The membership `EventStream` capacity.
  #[must_use]
  pub const fn event_stream_capacity(&self) -> usize {
    self.event_stream_capacity
  }

  /// The per-poll gossip-recv fairness bound.
  #[must_use]
  pub const fn recv_batch(&self) -> usize {
    self.recv_batch
  }

  /// The per-poll transmit-drain fairness bound.
  #[must_use]
  pub const fn transmit_batch(&self) -> usize {
    self.transmit_batch
  }

  /// The default join deadline.
  #[must_use]
  pub const fn join_deadline(&self) -> Duration {
    self.join_deadline
  }

  /// The bound on a per-bridge graceful-drain write.
  #[must_use]
  pub const fn close_timeout(&self) -> Duration {
    self.close_timeout
  }
}

/// The full configuration for a `Memberlist`: SWIM-level [`MemberlistOptions`],
/// runtime [`RuntimeOptions`], and the optional synchronous admission delegates.
///
/// The transport backend and the address resolver are supplied to the
/// constructor, not here.
pub struct Options<I> {
  memberlist: MemberlistOptions,
  runtime: RuntimeOptions,
  alive_delegate: Option<Box<dyn AliveDelegate<I, SocketAddr>>>,
  merge_delegate: Option<Box<dyn MergeDelegate<I, SocketAddr>>>,
  /// CIDR peer-admission policy. `()` when the `cidr` feature is off; otherwise
  /// an optional [`CidrPolicy`](memberlist_proto::CidrPolicy), default `None`.
  cidr_policy: CidrFilter,
}

impl<I> Default for Options<I> {
  fn default() -> Self {
    Self {
      memberlist: MemberlistOptions::default(),
      runtime: RuntimeOptions::default(),
      alive_delegate: None,
      merge_delegate: None,
      cidr_policy: Default::default(),
    }
  }
}

impl<I> Options<I> {
  /// Default options with no admission delegates.
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Sets the SWIM-level options.
  #[must_use]
  pub fn with_memberlist(mut self, opts: MemberlistOptions) -> Self {
    self.memberlist = opts;
    self
  }

  /// Sets the runtime options.
  #[must_use]
  pub fn with_runtime(mut self, opts: RuntimeOptions) -> Self {
    self.runtime = opts;
    self
  }

  /// Installs a synchronous alive-admission predicate.
  #[must_use]
  pub fn with_alive_delegate(mut self, delegate: impl AliveDelegate<I, SocketAddr>) -> Self {
    self.alive_delegate = Some(Box::new(delegate));
    self
  }

  /// Installs a synchronous merge-admission predicate.
  #[must_use]
  pub fn with_merge_delegate(mut self, delegate: impl MergeDelegate<I, SocketAddr>) -> Self {
    self.merge_delegate = Some(Box::new(delegate));
    self
  }

  /// Installs a CIDR peer-admission policy. One policy filters inbound gossip by
  /// datagram source and inbound reliable connections by peer address at the
  /// transport boundary, AND inbound alives by the peer's self-advertised address
  /// at membership admission — set once, enforced on both the unreliable and
  /// reliable planes. Composes with [`with_alive_delegate`](Self::with_alive_delegate):
  /// a peer must pass both the policy and the user delegate.
  #[cfg(feature = "cidr")]
  #[cfg_attr(docsrs, doc(cfg(feature = "cidr")))]
  #[must_use]
  pub fn with_cidr_policy(mut self, policy: memberlist_proto::CidrPolicy) -> Self {
    self.cidr_policy = Some(policy);
    self
  }

  /// The SWIM-level options.
  #[must_use]
  pub const fn memberlist(&self) -> &MemberlistOptions {
    &self.memberlist
  }

  /// The runtime options.
  #[must_use]
  pub const fn runtime(&self) -> &RuntimeOptions {
    &self.runtime
  }

  /// Decomposes the options into their parts for the backend constructor: the
  /// SWIM options, the runtime options, the optional admission delegates, and the
  /// CIDR policy carrier (`()` when the `cidr` feature is off).
  #[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
  #[allow(clippy::type_complexity)]
  pub(crate) fn into_parts(
    self,
  ) -> (
    MemberlistOptions,
    RuntimeOptions,
    Option<Box<dyn AliveDelegate<I, SocketAddr>>>,
    Option<Box<dyn MergeDelegate<I, SocketAddr>>>,
    CidrFilter,
  ) {
    (
      self.memberlist,
      self.runtime,
      self.alive_delegate,
      self.merge_delegate,
      self.cidr_policy,
    )
  }
}

#[cfg(test)]
mod builder_tests;
