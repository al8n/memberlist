//! The uniform [`Memberlist`] handle and the QUIC, TCP, and TLS backend
//! constructors.

use std::{
  net::SocketAddr,
  sync::{Arc, atomic::AtomicU64},
};

use agnostic::{
  Runtime,
  net::{Net, UdpSocket},
};
#[cfg(any(feature = "tcp", feature = "tls"))]
use memberlist_proto::LabelOptions;
#[cfg(feature = "tcp")]
use memberlist_proto::RawRecords;
#[cfg(any(feature = "tcp", feature = "tls"))]
use memberlist_proto::streams::{StreamEndpoint, StreamTransport};
use memberlist_proto::{
  AliveDelegate, CompressionOptions, EncryptionOptions, Endpoint, EndpointConfig, Instant,
  MergeDelegate, Node, event::Event, typed::NodeState,
};
#[cfg(feature = "tls")]
use memberlist_proto::{Labeled, TlsOptions, TlsRecords};
#[cfg(feature = "quic")]
use memberlist_proto::{QuicConfig, QuicEndpoint};

#[cfg(feature = "quic")]
use crate::quic_driver::QuicDriver;
#[cfg(any(feature = "tcp", feature = "tls"))]
use crate::stream_driver::{ACCEPT_CAP, StreamDriver, accept_task};
use crate::{
  NodeId,
  command::{
    Command, JoinCmd, LeaveCmd, PingCmd, SendReliableCmd, SendUserCmd, SetCompressionOptionsCmd,
    SetEncryptionOptionsCmd, ShutdownCmd,
  },
  delegate::Delegate,
  error::Error,
  events::EventStream,
  observation::observation_task,
  options::{Channel, MemberlistOptions, Options},
  resolver::{AddressResolver, MaybeResolved},
  shared::Shared,
  snapshot::{MemberlistSnapshot, snapshot_of},
  transform::validate_encryption,
};
#[cfg(any(feature = "tcp", feature = "tls"))]
use agnostic::net::TcpListener;

/// Wraps a boxed `AliveDelegate` so it satisfies the `impl AliveDelegate` bound
/// of `Endpoint::set_alive_delegate` (the machine takes the predicate by value,
/// and a `Box<dyn _>` does not implement the trait directly).
struct BoxedAlive<I, A>(Box<dyn AliveDelegate<I, A>>);

impl<I: 'static, A: 'static> AliveDelegate<I, A> for BoxedAlive<I, A> {
  fn notify_alive(&self, peer: &NodeState<I, A>) -> bool {
    self.0.notify_alive(peer)
  }
}

/// `MergeDelegate` counterpart to [`BoxedAlive`].
struct BoxedMerge<I, A>(Box<dyn MergeDelegate<I, A>>);

impl<I: 'static, A: 'static> MergeDelegate<I, A> for BoxedMerge<I, A> {
  fn notify_merge(&self, peers: &[NodeState<I, A>]) -> bool {
    self.0.notify_merge(peers)
  }
}

/// Layers the [`MemberlistOptions`] overrides onto a machine [`EndpointConfig`].
fn apply_memberlist_options<I, A>(
  mut cfg: EndpointConfig<I, A>,
  opts: &MemberlistOptions,
) -> EndpointConfig<I, A> {
  if let Some(mtu) = opts.gossip_mtu() {
    cfg = cfg.with_gossip_mtu(mtu);
  }
  if let Some(size) = opts.meta_max_size() {
    cfg = cfg.with_meta_max_size(size);
  }
  if let Some(size) = opts.max_stream_frame_size() {
    cfg = cfg.with_max_stream_frame_size(size);
  }
  if let Some(meta) = opts.initial_meta() {
    cfg = cfg.with_initial_meta(meta.clone());
  }
  if let Some(state) = opts.initial_local_state() {
    cfg = cfg.with_initial_local_state(state.clone());
  }
  cfg
}

/// A handle to a running memberlist node.
///
/// Cheap to clone; every clone shares the one backend driver, which runs until
/// the last handle is dropped (or [`shutdown`](Memberlist::shutdown) is called).
/// Membership reads are lock-free via the published [`MemberlistSnapshot`].
pub struct Memberlist<I> {
  shared: Arc<Shared<I>>,
  events_rx: flume::Receiver<Event<I, SocketAddr>>,
}

impl<I> Clone for Memberlist<I> {
  fn clone(&self) -> Self {
    self.shared.handle_cloned();
    Self {
      shared: self.shared.clone(),
      events_rx: self.events_rx.clone(),
    }
  }
}

impl<I> Drop for Memberlist<I> {
  fn drop(&mut self) {
    if self.shared.handle_dropped() {
      self.shared.begin_shutdown();
      self.shared.wake_driver();
    }
  }
}

impl<I: NodeId> Memberlist<I> {
  /// Builds a QUIC-backed node and spawns its driver on the runtime `R`.
  ///
  /// The advertise address is resolved once via `resolver`, then the socket is
  /// bound and the [`QuicEndpoint`] driven; the resolver is not retained.
  #[cfg(feature = "quic")]
  pub async fn quic<R, Res, D>(
    resolver: &Res,
    local_id: I,
    advertise: MaybeResolved<Res::Address>,
    options: Options<I>,
    delegate: D,
    quic_config: QuicConfig,
  ) -> Result<Self, Error>
  where
    R: Runtime,
    Res: AddressResolver,
    D: Delegate<Id = I, Address = SocketAddr>,
  {
    let advertise_socket = resolve_one(resolver, advertise).await?;
    let socket = <R::Net as Net>::UdpSocket::bind(advertise_socket)
      .await
      .map_err(Error::Io)?;
    let bound = socket.local_addr().map_err(Error::Io)?;
    validate_advertise(bound)?;

    let (ml_opts, drv_opts, alive, merge) = options.into_parts();
    validate_encryption(ml_opts.encryption())?;

    let cfg = apply_memberlist_options(EndpointConfig::new(local_id, bound), &ml_opts);
    let mut ep: Endpoint<I, SocketAddr> = Endpoint::new(cfg);
    if let Some(ad) = alive {
      ep.set_alive_delegate(BoxedAlive(ad));
    }
    if let Some(md) = merge {
      ep.set_merge_delegate(BoxedMerge(md));
    }

    // Retain the cluster label for the gossip codec.
    let label = ml_opts.label().map(bytes::Bytes::copy_from_slice);

    let mut endpoint = QuicEndpoint::new(ep, quic_config);
    // Apply the configured compression and encryption policies. QUIC's reliable
    // path has its own connection-level security layer; compression and encryption
    // are applied to the gossip (UDP datagram) path only.
    endpoint.set_compression_options(*ml_opts.compression());
    endpoint.set_encryption_options(ml_opts.encryption().clone());
    endpoint.start_scheduling(Instant::now());

    let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));

    // Observation byte backstop: bound the queued payload bytes a slow delegate
    // can pin (the obs-channel count cap alone does not).
    let obs_payload_bytes = Arc::new(AtomicU64::new(0));
    let obs_payload_budget = match drv_opts.observation_channel() {
      Channel::Bounded(_) => Some((endpoint.max_stream_frame_size() as u64).saturating_mul(4)),
      Channel::Unbounded => None,
    };

    // Driver -> observation task hand-off, plus the subscriber event stream.
    let (obs_tx, obs_rx) = match drv_opts.observation_channel() {
      Channel::Bounded(n) => flume::bounded(n),
      Channel::Unbounded => flume::unbounded(),
    };
    let (events_tx, events_rx) = flume::bounded(drv_opts.event_stream_capacity());
    R::spawn_detach(observation_task::<I, D>(
      obs_rx,
      delegate,
      events_tx,
      shared.clone(),
      obs_payload_bytes.clone(),
    ));

    let driver = QuicDriver::<I, R>::new(
      endpoint,
      socket,
      shared.clone(),
      drv_opts.recv_batch(),
      drv_opts.transmit_batch(),
      obs_tx,
      obs_payload_bytes,
      obs_payload_budget,
      label,
    );
    R::spawn_detach(driver);

    Ok(Self { shared, events_rx })
  }

  /// Builds a TCP-backed node and spawns its driver on the runtime `R`.
  ///
  /// The advertise address is resolved once via `resolver`, then a UDP gossip
  /// socket and a TCP listener are bound on it and the [`StreamEndpoint`] driven;
  /// the resolver is not retained.
  #[cfg(feature = "tcp")]
  pub async fn tcp<R, Res, D>(
    resolver: &Res,
    local_id: I,
    advertise: MaybeResolved<Res::Address>,
    options: Options<I>,
    delegate: D,
  ) -> Result<Self, Error>
  where
    R: Runtime,
    Res: AddressResolver,
    D: Delegate<Id = I, Address = SocketAddr>,
  {
    Self::build_stream_backend::<R, Res, D, RawRecords>(
      resolver,
      local_id,
      advertise,
      options,
      delegate,
      |ep, ml_opts| {
        // The single configured label feeds both the reliable-plane label
        // exchange (so peers verify they belong to the same cluster on every
        // TCP stream) and the gossip codec (wired below). The label is
        // validated at the MemberlistOptions setter so `try_new_in` never
        // fails here; the infallible `new_in` unwrap is safe.
        let label_bytes = ml_opts.label().map(|b| b.to_vec());
        let mut label_opts = LabelOptions::new_in(label_bytes, ());
        if ml_opts.skip_inbound_label_check() {
          label_opts = label_opts.skip_inbound_label_check();
        }
        StreamEndpoint::with_compression(
          ep,
          label_opts,
          Box::new(|_: &SocketAddr| -> Option<String> { None }),
          Box::new(|addr: &SocketAddr| *addr),
          *ml_opts.compression(),
        )
        .with_encryption(ml_opts.encryption().clone())
      },
    )
    .await
  }

  /// Builds a TLS-backed node and spawns its driver on the runtime `R`.
  ///
  /// Like [`tcp`](Self::tcp), but the reliable exchanges run over TLS. The caller
  /// supplies the rustls server/client configs via `tls_options`, and
  /// `sni_provider` maps each peer to the server name its certificate is verified
  /// against — a TLS dial requires one, and returning `None` skips that peer.
  #[cfg(feature = "tls")]
  pub async fn tls<R, Res, D, F>(
    resolver: &Res,
    local_id: I,
    advertise: MaybeResolved<Res::Address>,
    options: Options<I>,
    delegate: D,
    tls_options: TlsOptions,
    sni_provider: F,
  ) -> Result<Self, Error>
  where
    R: Runtime,
    Res: AddressResolver,
    D: Delegate<Id = I, Address = SocketAddr>,
    F: Fn(&SocketAddr) -> Option<String> + Send + Sync + 'static,
  {
    Self::build_stream_backend::<R, Res, D, Labeled<TlsRecords>>(
      resolver,
      local_id,
      advertise,
      options,
      delegate,
      move |ep, ml_opts| {
        // The reliable transport is the cluster-label decorator over the TLS
        // record layer: the configured cluster label rides as the first
        // plaintext inside the TLS session, so two clusters that share a TLS
        // trust anchor and SNI are still isolated on the reliable plane. The
        // same label feeds the gossip codec, so the two planes cannot diverge.
        let label_bytes = ml_opts.label().map(|b| b.to_vec());
        let mut label_opts = LabelOptions::new_in(label_bytes, tls_options);
        if ml_opts.skip_inbound_label_check() {
          label_opts = label_opts.skip_inbound_label_check();
        }
        StreamEndpoint::with_compression(
          ep,
          label_opts,
          Box::new(sni_provider),
          Box::new(|addr: &SocketAddr| *addr),
          *ml_opts.compression(),
        )
        .with_encryption(ml_opts.encryption().clone())
      },
    )
    .await
  }

  /// Shared bootstrap for the stream backends (TCP, TLS): resolves and binds the
  /// UDP gossip socket and the TCP listener, builds the membership `Endpoint`,
  /// hands it to `make_endpoint` to wrap in the record-layer-specific
  /// `StreamEndpoint`, then spawns the observation and accept tasks and the driver.
  ///
  /// `make_endpoint` receives both the bare `Endpoint` and a reference to the
  /// decoded `MemberlistOptions` so it can configure the record-layer-specific
  /// label, compression, and encryption on the `StreamEndpoint` it returns.
  #[cfg(any(feature = "tcp", feature = "tls"))]
  async fn build_stream_backend<R, Res, D, T>(
    resolver: &Res,
    local_id: I,
    advertise: MaybeResolved<Res::Address>,
    options: Options<I>,
    delegate: D,
    make_endpoint: impl FnOnce(
      Endpoint<I, SocketAddr>,
      &MemberlistOptions,
    ) -> StreamEndpoint<I, SocketAddr, T>,
  ) -> Result<Self, Error>
  where
    R: Runtime,
    Res: AddressResolver,
    D: Delegate<Id = I, Address = SocketAddr>,
    T: StreamTransport + Send + Unpin + 'static,
    T::Options: Send + Unpin,
  {
    let advertise_socket = resolve_one(resolver, advertise).await?;
    let socket = <R::Net as Net>::UdpSocket::bind(advertise_socket)
      .await
      .map_err(Error::Io)?;
    let bound = socket.local_addr().map_err(Error::Io)?;
    validate_advertise(bound)?;
    let listener = <R::Net as Net>::TcpListener::bind(bound)
      .await
      .map_err(Error::Io)?;

    let (ml_opts, drv_opts, alive, merge) = options.into_parts();
    // Reject a zero graceful-close drain timeout before any bridge spawns. The
    // stream driver bounds each post-Close drain write with `close_timeout`, so
    // zero fires immediately and a graceful close RSTs its queued push/pull
    // response bytes instead of draining them (matches the smoltcp driver).
    if drv_opts.close_timeout().is_zero() {
      return Err(Error::ZeroCloseTimeout);
    }
    // Validate the encryption configuration before the endpoint is built, so an
    // unusable keyring surfaces as a typed construction error rather than silently
    // discarding every encrypted gossip datagram at runtime.
    validate_encryption(ml_opts.encryption())?;

    let cfg = apply_memberlist_options(EndpointConfig::new(local_id, bound), &ml_opts);
    let mut ep: Endpoint<I, SocketAddr> = Endpoint::new(cfg);
    if let Some(ad) = alive {
      ep.set_alive_delegate(BoxedAlive(ad));
    }
    if let Some(md) = merge {
      ep.set_merge_delegate(BoxedMerge(md));
    }

    // Retain the cluster label for the gossip codec.
    let label = ml_opts.label().map(bytes::Bytes::copy_from_slice);

    let mut endpoint = make_endpoint(ep, &ml_opts);
    endpoint.start_scheduling(Instant::now());

    let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));

    // Observation byte backstop: bound the queued payload bytes a slow delegate
    // can pin (the obs-channel count cap alone does not).
    let obs_payload_bytes = Arc::new(AtomicU64::new(0));
    let obs_payload_budget = match drv_opts.observation_channel() {
      Channel::Bounded(_) => Some((endpoint.max_stream_frame_size() as u64).saturating_mul(4)),
      Channel::Unbounded => None,
    };

    // Driver -> observation task hand-off, plus the subscriber event stream.
    let (obs_tx, obs_rx) = match drv_opts.observation_channel() {
      Channel::Bounded(n) => flume::bounded(n),
      Channel::Unbounded => flume::unbounded(),
    };
    let (events_tx, events_rx) = flume::bounded(drv_opts.event_stream_capacity());
    R::spawn_detach(observation_task::<I, D>(
      obs_rx,
      delegate,
      events_tx,
      shared.clone(),
      obs_payload_bytes.clone(),
    ));

    // Inbound connections arrive on a dedicated accept task (accept is async); it
    // is cancelled when the driver drops accept_shutdown_tx, releasing the port.
    let (accepted_tx, accepted_rx) = flume::bounded(ACCEPT_CAP);
    let (accept_shutdown_tx, accept_shutdown_rx) = flume::bounded(1);
    R::spawn_detach(accept_task(
      listener,
      accepted_tx,
      accept_shutdown_rx,
      shared.clone(),
    ));

    let driver = StreamDriver::<I, R, T>::new(
      endpoint,
      socket,
      shared.clone(),
      drv_opts.recv_batch(),
      drv_opts.transmit_batch(),
      obs_tx,
      obs_payload_bytes,
      obs_payload_budget,
      accepted_rx,
      accept_shutdown_tx,
      drv_opts.close_timeout(),
      label,
    );
    R::spawn_detach(driver);

    Ok(Self { shared, events_rx })
  }

  /// The latest membership snapshot, read lock-free.
  #[must_use]
  pub fn snapshot(&self) -> Arc<MemberlistSnapshot<I, SocketAddr>> {
    self.shared.load_snapshot()
  }

  /// This node's own identity and advertised address.
  #[must_use]
  pub fn local(&self) -> Node<I, SocketAddr> {
    self.shared.load_snapshot().local()
  }

  /// The number of known members.
  #[must_use]
  pub fn num_members(&self) -> usize {
    self.shared.load_snapshot().num_members()
  }

  /// Subscribes to the membership / control event stream. Application data is
  /// delivered to the [`Delegate`], not here.
  #[must_use]
  pub fn events(&self) -> EventStream<I, SocketAddr> {
    EventStream::new(self.events_rx.clone())
  }

  /// The cumulative count of events dropped at the event-stream fan-out (a slow
  /// subscriber); these are recoverable from the snapshot.
  #[must_use]
  pub fn events_dropped(&self) -> u64 {
    self.shared.events_dropped()
  }

  /// The cumulative count of events dropped at the observation channel (a slow
  /// delegate); these may include unrecoverable application data.
  #[must_use]
  pub fn observation_dropped(&self) -> u64 {
    self.shared.observation_dropped()
  }

  /// The local node's id.
  #[must_use]
  #[inline]
  pub fn local_id(&self) -> I {
    self
      .shared
      .load_snapshot()
      .local_ref()
      .id_ref()
      .cheap_clone()
  }

  /// The local node's advertised address.
  #[must_use]
  #[inline]
  pub fn advertise_address(&self) -> SocketAddr {
    *self.shared.load_snapshot().local_ref().address_ref()
  }

  /// The local node's full state from the latest published snapshot.
  #[must_use]
  #[inline]
  pub fn local_state(&self) -> Arc<NodeState<I, SocketAddr>> {
    self.shared.load_snapshot().local_ref().clone()
  }

  /// Look up a member by id in the latest published snapshot.
  #[must_use]
  #[inline]
  pub fn by_id(&self, id: &I) -> Option<Arc<NodeState<I, SocketAddr>>>
  where
    I: PartialEq,
  {
    self.shared.load_snapshot().by_id(id).cloned()
  }

  /// All members currently in the alive state, from the latest published
  /// snapshot.
  #[must_use]
  #[inline]
  pub fn online_members(&self) -> Vec<Arc<NodeState<I, SocketAddr>>> {
    self
      .shared
      .load_snapshot()
      .online_members()
      .cloned()
      .collect()
  }

  /// Number of alive members. Equivalent to
  /// [`MemberlistSnapshot::alive_count`] on the snapshot.
  #[must_use]
  #[inline]
  pub fn num_online_members(&self) -> usize {
    self.shared.load_snapshot().alive_count()
  }

  /// All known members (alive + suspect + dead/left) from the latest
  /// published snapshot. Mirrors the legacy `Memberlist::members` name.
  #[must_use]
  #[inline]
  pub fn members(&self) -> Vec<Arc<NodeState<I, SocketAddr>>> {
    self.shared.load_snapshot().members().to_vec()
  }

  /// Members matching `pred`, from the latest published snapshot.
  #[must_use]
  #[inline]
  pub fn members_by(
    &self,
    pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool,
  ) -> Vec<Arc<NodeState<I, SocketAddr>>> {
    self
      .shared
      .load_snapshot()
      .members_by(pred)
      .cloned()
      .collect()
  }

  /// Count of members matching `pred`.
  #[must_use]
  #[inline]
  pub fn num_members_by(&self, pred: impl FnMut(&NodeState<I, SocketAddr>) -> bool) -> usize {
    self.shared.load_snapshot().num_members_by(pred)
  }

  /// Map-filter members, collecting all `Some` results into a `Vec`.
  #[must_use]
  #[inline]
  pub fn members_map_by<O>(&self, f: impl FnMut(&NodeState<I, SocketAddr>) -> Option<O>) -> Vec<O> {
    self.shared.load_snapshot().members_map_by(f)
  }

  /// The local node's Lifeguard health score (`0` = healthy; higher = worse).
  #[must_use]
  #[inline]
  pub fn health_score(&self) -> usize {
    self.shared.load_snapshot().health_score()
  }

  /// Joins the cluster by contacting `seeds` (resolved via `resolver`), waiting
  /// for the push/pull exchanges to complete and returning the number contacted.
  /// Errors with `JoinFailed` if seeds were dispatched but none was reached.
  pub async fn join<Res>(
    &self,
    resolver: &Res,
    seeds: &[MaybeResolved<Res::Address>],
  ) -> Result<usize, Error>
  where
    Res: AddressResolver,
  {
    self.join_inner(resolver, seeds, true).await
  }

  /// Like [`join`](Self::join) but fire-and-forget: dispatches the seeds and
  /// returns the dispatched count immediately, without awaiting the exchanges.
  pub async fn join_detached<Res>(
    &self,
    resolver: &Res,
    seeds: &[MaybeResolved<Res::Address>],
  ) -> Result<usize, Error>
  where
    Res: AddressResolver,
  {
    self.join_inner(resolver, seeds, false).await
  }

  async fn join_inner<Res>(
    &self,
    resolver: &Res,
    seeds: &[MaybeResolved<Res::Address>],
    wait: bool,
  ) -> Result<usize, Error>
  where
    Res: AddressResolver,
  {
    if self.shared.is_shutdown() {
      return Err(Error::Shutdown);
    }
    let mut addrs = Vec::with_capacity(seeds.len());
    for seed in seeds {
      match seed {
        MaybeResolved::Resolved(s) => addrs.push(*s),
        MaybeResolved::Unresolved(a) => addrs.extend(
          resolver
            .resolve(a)
            .await
            .map_err(|e| Error::Resolve(e.to_string()))?,
        ),
      }
    }
    // A non-empty seed list resolving to zero addresses is a bootstrap failure,
    // not a successful zero-contact join.
    if !seeds.is_empty() && addrs.is_empty() {
      return Err(Error::JoinFailed(seeds.len()));
    }
    let (tx, rx) = flume::bounded(1);
    if !self.shared.push_command(Command::Join(JoinCmd {
      addrs,
      wait,
      reply: tx,
    })) {
      return Err(Error::Shutdown);
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }

  /// Gracefully leaves the cluster (the node stops participating).
  pub async fn leave(&self) -> Result<(), Error> {
    if self.shared.is_shutdown() {
      return Err(Error::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    if !self
      .shared
      .push_command(Command::Leave(LeaveCmd { reply: tx }))
    {
      return Err(Error::Shutdown);
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }

  /// Stops the driver and releases its socket.
  pub async fn shutdown(&self) -> Result<(), Error> {
    let (tx, rx) = flume::bounded(1);
    if !self
      .shared
      .push_command(Command::Shutdown(ShutdownCmd { reply: tx }))
    {
      // The driver already exited; shutdown is idempotent.
      return Ok(());
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }

  /// Probes `node` and returns the measured round-trip time.
  ///
  /// Returns `Err(NotRunning)` if the node is not running, `Err(PingTimeout)`
  /// if no ack arrived within the probe deadline, or `Err(Shutdown)` if the
  /// driver shut down while waiting.
  pub async fn ping(&self, node: Node<I, SocketAddr>) -> Result<std::time::Duration, Error> {
    if self.shared.is_shutdown() {
      return Err(Error::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    if !self
      .shared
      .push_command(Command::Ping(PingCmd { node, reply: tx }))
    {
      return Err(Error::Shutdown);
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }

  /// Sends a single unreliable directed user message to `to` via gossip.
  pub async fn send(&self, to: SocketAddr, payload: bytes::Bytes) -> Result<(), Error> {
    self.send_many(to, core::iter::once(payload)).await
  }

  /// Sends multiple unreliable directed user messages to `to` via gossip.
  pub async fn send_many(
    &self,
    to: SocketAddr,
    payloads: impl IntoIterator<Item = bytes::Bytes>,
  ) -> Result<(), Error> {
    if self.shared.is_shutdown() {
      return Err(Error::Shutdown);
    }
    let payloads: Vec<bytes::Bytes> = payloads.into_iter().collect();
    let (tx, rx) = flume::bounded(1);
    if !self.shared.push_command(Command::SendUser(SendUserCmd {
      to,
      payloads,
      reply: tx,
    })) {
      return Err(Error::Shutdown);
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }

  /// Sends a single reliable directed user message to `to` via the stream
  /// plane (TCP or QUIC), waiting for the exchange to complete.
  pub async fn send_reliable(&self, to: SocketAddr, payload: bytes::Bytes) -> Result<(), Error> {
    self.send_many_reliable(to, core::iter::once(payload)).await
  }

  /// Sends multiple reliable directed user messages to `to` via the stream
  /// plane, waiting for all exchanges to complete.
  pub async fn send_many_reliable(
    &self,
    to: SocketAddr,
    payloads: impl IntoIterator<Item = bytes::Bytes>,
  ) -> Result<(), Error> {
    if self.shared.is_shutdown() {
      return Err(Error::Shutdown);
    }
    let payloads: Vec<bytes::Bytes> = payloads.into_iter().collect();
    let (tx, rx) = flume::bounded(1);
    if !self
      .shared
      .push_command(Command::SendReliable(SendReliableCmd {
        to,
        payloads,
        reply: tx,
      }))
    {
      return Err(Error::Shutdown);
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }

  /// Reconfigures the gossip compression policy in place.
  ///
  /// The change takes effect on the next outbound datagram. Rejected with
  /// `Err(NotRunning)` once the node has left the cluster.
  pub async fn set_compression_options(&self, opts: CompressionOptions) -> Result<(), Error> {
    if self.shared.is_shutdown() {
      return Err(Error::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    if !self
      .shared
      .push_command(Command::SetCompressionOptions(SetCompressionOptionsCmd {
        opts,
        reply: tx,
      }))
    {
      return Err(Error::Shutdown);
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }

  /// Reconfigures the gossip encryption policy in place.
  ///
  /// The keyring is validated before being applied: every key in the ring is
  /// trial-encrypted to confirm the AEAD backend is compiled in. Rejected with
  /// `Err(NotRunning)` once the node has left the cluster, or with
  /// `Err(Encryption(_))` when the keyring contains an unsupported algorithm.
  pub async fn set_encryption_options(&self, opts: EncryptionOptions) -> Result<(), Error> {
    if self.shared.is_shutdown() {
      return Err(Error::Shutdown);
    }
    let (tx, rx) = flume::bounded(1);
    if !self
      .shared
      .push_command(Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
        opts,
        reply: tx,
      }))
    {
      return Err(Error::Shutdown);
    }
    rx.recv_async().await.map_err(|_| Error::Shutdown)?
  }
}

/// Resolves a single address (the advertise / a seed) to one `SocketAddr`.
async fn resolve_one<Res: AddressResolver>(
  resolver: &Res,
  addr: MaybeResolved<Res::Address>,
) -> Result<SocketAddr, Error> {
  match addr {
    MaybeResolved::Resolved(s) => Ok(s),
    MaybeResolved::Unresolved(a) => resolver
      .resolve(&a)
      .await
      .map_err(|e| Error::Resolve(e.to_string()))?
      .into_iter()
      .next()
      .ok_or_else(|| Error::Resolve("no addresses resolved".into())),
  }
}

/// Rejects an advertise address peers could not dial: unspecified, multicast,
/// or broadcast. The port is already concrete (read back post-bind).
fn validate_advertise(addr: SocketAddr) -> Result<(), Error> {
  let ip = addr.ip();
  let bad = ip.is_unspecified()
    || ip.is_multicast()
    || matches!(ip, std::net::IpAddr::V4(v4) if v4.is_broadcast());
  if bad {
    return Err(Error::InvalidAdvertise(addr));
  }
  Ok(())
}
