//! Configuration: the [`Options`] umbrella plus its [`MemberlistOptions`]
//! (SWIM-level machine knobs) and [`DriverOptions`] (reactor-driver tuning).

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use memberlist_proto::{
  AliveDelegate, CompressionOptions, EncryptionOptions, MergeDelegate, label::validate_label,
  typed::Meta,
};

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
pub struct MemberlistOptions {
  gossip_mtu: Option<usize>,
  meta_max_size: Option<usize>,
  max_stream_frame_size: Option<usize>,
  initial_meta: Option<Meta>,
  initial_local_state: Option<Bytes>,
  compression: CompressionOptions,
  encryption: EncryptionOptions,
  label: Option<Bytes>,
  skip_inbound_label_check: bool,
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
  #[must_use]
  pub fn with_compression(mut self, compression: CompressionOptions) -> Self {
    self.compression = compression;
    self
  }

  /// Sets the initial gossip+stream encryption policy.
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
  #[must_use]
  pub fn compression(&self) -> &CompressionOptions {
    &self.compression
  }

  /// The initial gossip+stream encryption policy.
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
mod label_tests {
  use super::*;
  use crate::error::Error;

  #[test]
  fn with_label_validates_too_long() {
    let long = vec![b'x'; 254]; // one over the 253-byte max
    let result = MemberlistOptions::new().with_label(Some(long));
    assert!(
      matches!(result, Err(Error::InvalidLabel(_))),
      "a label exceeding 253 bytes must be rejected"
    );
  }

  #[test]
  fn with_label_validates_non_utf8() {
    let bad = vec![0xff, 0xfe];
    let result = MemberlistOptions::new().with_label(Some(bad));
    assert!(
      matches!(result, Err(Error::InvalidLabel(_))),
      "a non-UTF-8 label must be rejected"
    );
  }

  #[test]
  fn with_label_accepts_valid() {
    let opts = MemberlistOptions::new()
      .with_label(Some(b"cluster-x".to_vec()))
      .expect("valid ASCII label must be accepted");
    assert_eq!(opts.label(), Some(b"cluster-x".as_slice()));
    assert!(!opts.skip_inbound_label_check());
  }

  #[test]
  fn with_skip_inbound_label_check_round_trips() {
    let opts = MemberlistOptions::new()
      .with_label(Some(b"cluster-x".to_vec()))
      .expect("valid label")
      .with_skip_inbound_label_check(true);
    assert_eq!(opts.label(), Some(b"cluster-x".as_slice()));
    assert!(opts.skip_inbound_label_check());
  }

  #[test]
  fn empty_label_normalizes_to_none() {
    let opts = MemberlistOptions::new()
      .with_label(Some(Vec::new()))
      .expect("empty label is valid");
    assert_eq!(opts.label(), None);
  }
}

/// Capacity policy for the bounded observation channel carrying machine
/// [`Event`](memberlist_proto::Event)s to the observation task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Self::Bounded(1024)
  }
}

/// Reactor-driver tuning.
///
/// The reactor driver wakes via shared state + a stored waker rather than a
/// command channel, so it has no command-fairness or completion-peek knobs. What
/// remains: the observation-channel policy, the membership `EventStream`
/// capacity, the per-poll work bounds that keep the pump fair under a gossip
/// flood, and the default join deadline.
#[derive(Debug, Clone)]
pub struct DriverOptions {
  observation_channel: Channel,
  event_stream_capacity: usize,
  recv_batch: usize,
  transmit_batch: usize,
  join_deadline: Duration,
  close_timeout: Duration,
}

impl Default for DriverOptions {
  fn default() -> Self {
    Self {
      observation_channel: Channel::default(),
      event_stream_capacity: 1024,
      recv_batch: 64,
      transmit_batch: 64,
      join_deadline: Duration::from_secs(10),
      // Bound a per-bridge graceful-drain write. After a graceful
      // `StreamAction::Close` the bridge has no remaining cancel path, so a
      // peer that stopped reading would otherwise wedge the drain `write_all`
      // forever — leaking the detached bridge task and its socket. A write
      // that makes progress never trips this; only a write stalled for the
      // full duration is abandoned and the bridge torn down (RST). 10s mirrors
      // the smoltcp driver's `Config::close_timeout`.
      close_timeout: Duration::from_secs(10),
    }
  }
}

impl DriverOptions {
  /// Driver options with default tuning.
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
/// reactor [`DriverOptions`], and the optional synchronous admission delegates.
///
/// The transport backend and the address resolver are supplied to the
/// constructor, not here.
pub struct Options<I> {
  memberlist: MemberlistOptions,
  driver: DriverOptions,
  alive_delegate: Option<Box<dyn AliveDelegate<I, SocketAddr>>>,
  merge_delegate: Option<Box<dyn MergeDelegate<I, SocketAddr>>>,
}

impl<I> Default for Options<I> {
  fn default() -> Self {
    Self {
      memberlist: MemberlistOptions::default(),
      driver: DriverOptions::default(),
      alive_delegate: None,
      merge_delegate: None,
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

  /// Sets the reactor-driver options.
  #[must_use]
  pub fn with_driver(mut self, opts: DriverOptions) -> Self {
    self.driver = opts;
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

  /// The SWIM-level options.
  #[must_use]
  pub const fn memberlist(&self) -> &MemberlistOptions {
    &self.memberlist
  }

  /// The reactor-driver options.
  #[must_use]
  pub const fn driver(&self) -> &DriverOptions {
    &self.driver
  }

  /// Decomposes the options into their parts for the backend constructor: the
  /// SWIM options, the driver options, and the optional admission delegates.
  #[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
  #[allow(clippy::type_complexity)]
  pub(crate) fn into_parts(
    self,
  ) -> (
    MemberlistOptions,
    DriverOptions,
    Option<Box<dyn AliveDelegate<I, SocketAddr>>>,
    Option<Box<dyn MergeDelegate<I, SocketAddr>>>,
  ) {
    (
      self.memberlist,
      self.driver,
      self.alive_delegate,
      self.merge_delegate,
    )
  }
}

#[cfg(test)]
mod builder_tests {
  use smol_str::SmolStr;

  use super::*;

  #[test]
  fn memberlist_options_overrides_round_trip() {
    let meta = Meta::try_from(Bytes::from_static(b"meta")).expect("valid meta");
    let state = Bytes::from_static(b"local-state");
    let opts = MemberlistOptions::new()
      .with_gossip_mtu(1400)
      .with_meta_max_size(512)
      .with_max_stream_frame_size(65_536)
      .with_initial_meta(meta.clone())
      .with_initial_local_state(state.clone());

    assert_eq!(opts.gossip_mtu(), Some(1400));
    assert_eq!(opts.meta_max_size(), Some(512));
    assert_eq!(opts.max_stream_frame_size(), Some(65_536));
    assert_eq!(opts.initial_meta(), Some(&meta));
    assert_eq!(opts.initial_local_state(), Some(&state));
  }

  #[test]
  fn memberlist_options_default_leaves_every_override_unset() {
    let opts = MemberlistOptions::default();
    assert_eq!(opts.gossip_mtu(), None);
    assert_eq!(opts.meta_max_size(), None);
    assert_eq!(opts.max_stream_frame_size(), None);
    assert_eq!(opts.initial_meta(), None);
    assert_eq!(opts.initial_local_state(), None);
    assert_eq!(opts.label(), None);
    assert!(!opts.skip_inbound_label_check());
    // Exercise the compression/encryption accessors (they return machine defaults).
    let _compression = opts.compression();
    let _encryption = opts.encryption();
  }

  #[test]
  fn memberlist_options_compression_encryption_builders() {
    let opts = MemberlistOptions::new()
      .with_compression(CompressionOptions::default())
      .with_encryption(EncryptionOptions::default());
    let _compression = opts.compression();
    let _encryption = opts.encryption();
  }

  #[test]
  fn channel_default_is_bounded_1024() {
    assert_eq!(Channel::default(), Channel::Bounded(1024));
    assert_ne!(Channel::Bounded(1024), Channel::Unbounded);
  }

  #[test]
  fn driver_options_overrides_round_trip() {
    let opts = DriverOptions::new()
      .with_observation_channel(Channel::Unbounded)
      .with_event_stream_capacity(2048)
      .with_recv_batch(32)
      .with_transmit_batch(16)
      .with_join_deadline(Duration::from_secs(5))
      .with_close_timeout(Duration::from_secs(3));

    assert_eq!(opts.observation_channel(), Channel::Unbounded);
    assert_eq!(opts.event_stream_capacity(), 2048);
    assert_eq!(opts.recv_batch(), 32);
    assert_eq!(opts.transmit_batch(), 16);
    assert_eq!(opts.join_deadline(), Duration::from_secs(5));
    assert_eq!(opts.close_timeout(), Duration::from_secs(3));
  }

  #[test]
  fn driver_options_default_tuning() {
    let opts = DriverOptions::default();
    assert_eq!(opts.observation_channel(), Channel::Bounded(1024));
    assert_eq!(opts.event_stream_capacity(), 1024);
    assert_eq!(opts.recv_batch(), 64);
    assert_eq!(opts.transmit_batch(), 64);
    assert_eq!(opts.join_deadline(), Duration::from_secs(10));
    assert_eq!(opts.close_timeout(), Duration::from_secs(10));
  }

  /// `MemberlistOptions::new` matches `default`, and `DriverOptions::new` matches
  /// `default` (the `new` delegations).
  #[test]
  fn new_delegates_to_default() {
    assert_eq!(MemberlistOptions::new().gossip_mtu(), None);
    assert_eq!(
      DriverOptions::new().observation_channel(),
      DriverOptions::default().observation_channel()
    );
  }

  /// `Channel` is `Copy`/`Debug`; `Bounded` and `Unbounded` are distinct and
  /// each round-trips its discriminant.
  #[test]
  fn channel_variants_and_debug() {
    let bounded = Channel::Bounded(7);
    let copy = bounded;
    assert_eq!(copy, Channel::Bounded(7));
    assert_ne!(Channel::Bounded(7), Channel::Bounded(8));
    assert!(!format!("{:?}", Channel::Unbounded).is_empty());
  }

  /// `Options::new` defaults to no SWIM/driver overrides and no admission
  /// delegates; the `with_memberlist` / `with_driver` setters and `memberlist()`
  /// / `driver()` accessors round-trip.
  #[test]
  fn options_builder_round_trips() {
    let ml = MemberlistOptions::new().with_gossip_mtu(900);
    let drv = DriverOptions::new().with_recv_batch(8);
    let opts: Options<SmolStr> = Options::new().with_memberlist(ml).with_driver(drv);
    assert_eq!(opts.memberlist().gossip_mtu(), Some(900));
    assert_eq!(opts.driver().recv_batch(), 8);

    // The Default impl leaves both at their own defaults.
    let def: Options<SmolStr> = Options::default();
    assert_eq!(def.memberlist().gossip_mtu(), None);
    assert_eq!(def.driver().recv_batch(), 64);
  }

  /// The admission-delegate setters install the predicates, and `into_parts`
  /// hands every component back to the backend constructor — including the boxed
  /// admission delegates, whose predicates still fire through the box.
  #[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
  #[test]
  fn options_into_parts_carries_admission_delegates() {
    use memberlist_proto::typed::{NodeState, State};

    struct DenyAlive;
    impl AliveDelegate<SmolStr, SocketAddr> for DenyAlive {
      fn notify_alive(&self, _peer: &NodeState<SmolStr, SocketAddr>) -> bool {
        false
      }
    }
    struct AllowMerge;
    impl MergeDelegate<SmolStr, SocketAddr> for AllowMerge {
      fn notify_merge(&self, _peers: &[NodeState<SmolStr, SocketAddr>]) -> bool {
        true
      }
    }

    let opts: Options<SmolStr> = Options::new()
      .with_memberlist(MemberlistOptions::new().with_meta_max_size(64))
      .with_driver(DriverOptions::new().with_transmit_batch(4))
      .with_alive_delegate(DenyAlive)
      .with_merge_delegate(AllowMerge);

    let (ml, drv, alive, merge) = opts.into_parts();
    assert_eq!(ml.meta_max_size(), Some(64));
    assert_eq!(drv.transmit_batch(), 4);
    let alive = alive.expect("alive delegate installed");
    let merge = merge.expect("merge delegate installed");

    let node = NodeState::new(
      SmolStr::new("p"),
      SocketAddr::from(([127, 0, 0, 1], 1)),
      State::Alive,
    );
    assert!(
      !alive.notify_alive(&node),
      "the installed alive predicate denies admission"
    );
    assert!(
      merge.notify_merge(std::slice::from_ref(&node)),
      "the installed merge predicate allows the merge"
    );
  }

  /// With no admission delegates installed, `into_parts` yields `None` for both.
  #[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
  #[test]
  fn options_into_parts_without_delegates_is_none() {
    let opts: Options<SmolStr> = Options::new();
    let (_ml, _drv, alive, merge) = opts.into_parts();
    assert!(alive.is_none(), "no alive delegate by default");
    assert!(merge.is_none(), "no merge delegate by default");
  }
}
