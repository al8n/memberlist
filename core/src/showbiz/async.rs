use std::{future::Future, net::ToSocketAddrs, sync::atomic::Ordering, time::Duration};

use crate::{
  dns::DnsError,
  transport::TransportError,
  types::{Address, Dead, Domain},
  util::read_resolv_conf,
  Label,
};

use super::*;

use agnostic::Runtime;
use arc_swap::ArcSwapOption;
use async_lock::{Mutex, RwLock};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use itertools::{Either, Itertools};

#[cfg(all(feature = "async", feature = "test"))]
pub(crate) mod tests;

#[cfg(feature = "async")]
pub(crate) struct AckHandler {
  pub(crate) ack_fn:
    Box<dyn FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  pub(crate) nack_fn: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>>,
  pub(crate) timer: Timer,
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizCore<D: Delegate, T: Transport> {
  id: NodeId,
  hot: HotData,
  awareness: Awareness,
  broadcast: TransmitLimitedQueue<ShowbizBroadcast, DefaultNodeCalculator>,
  leave_broadcast_tx: Sender<()>,
  leave_broadcast_rx: Receiver<()>,
  status_change_lock: Mutex<()>,
  keyring: Option<SecretKeyring>,
  delegate: Option<D>,
  handoff_tx: Sender<()>,
  handoff_rx: Receiver<()>,
  queue: Mutex<MessageQueue>,
  nodes: Arc<RwLock<Memberlist<T::Runtime>>>,
  ack_handlers: Arc<Mutex<HashMap<u32, AckHandler>>>,
  dns: Option<Dns<T>>,
  #[cfg(feature = "metrics")]
  metrics_labels: Arc<Vec<metrics::Label>>,
  transport: T,
  /// We do not call send directly, just directly drop it.
  shutdown_tx: ArcSwapOption<Sender<()>>,
  advertise: SocketAddr,
  opts: Arc<Options<T>>,
}

impl<D: Delegate, T: Transport> Drop for ShowbizCore<D, T> {
  fn drop(&mut self) {
    self.shutdown_tx.store(None);
    if let Err(e) = self.transport.block_shutdown() {
      tracing::error!(target = "showbiz", err=%e, "failed to shutdown");
    }
  }
}

pub struct Showbiz<D: Delegate, T: Transport> {
  pub(crate) inner: Arc<ShowbizCore<D, T>>,
}

impl<D, T> Clone for Showbiz<D, T>
where
  T: Transport,
  D: Delegate,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<D, T> Showbiz<D, T>
where
  D: Delegate,
  T: Transport,
{
  /// Returns the local node's name.
  #[inline]
  pub fn name(&self) -> &Name {
    &self.inner.opts.name
  }

  /// Returns the number of alive nodes currently known. Between
  /// the time of calling this and calling Members, the number of alive nodes
  /// may have changed, so this shouldn't be used to determine how many
  /// members will be returned by Members.
  #[inline]
  pub async fn alive_members(&self) -> usize {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .filter(|n| !n.state.dead_or_left())
      .count()
  }

  /// Returns if the showbiz enabled encryption
  #[inline]
  pub fn encryption_enabled(&self) -> bool {
    if let Some(keyring) = &self.inner.keyring {
      !keyring.is_empty() && !self.inner.opts.encryption_algo.is_none()
    } else {
      false
    }
  }

  #[inline]
  pub async fn local_node(&self) -> Arc<Node> {
    let nodes = self.inner.nodes.read().await;
    // TODO: return an error
    nodes
      .node_map
      .get(&self.inner.id)
      .map(|&idx| nodes.nodes[idx].state.node.clone())
      .unwrap()
  }

  /// Returns the keyring used for the local node
  #[inline]
  pub fn keyring(&self) -> Option<&SecretKeyring> {
    self.inner.keyring.as_ref()
  }
}

pub struct JoinError<D: Delegate, T: Transport> {
  joined: Vec<NodeId>,
  errors: HashMap<Address, Error<D, T>>,
}

impl<D: Delegate, T: Transport> From<JoinError<D, T>>
  for (Vec<NodeId>, HashMap<Address, Error<D, T>>)
{
  fn from(e: JoinError<D, T>) -> Self {
    (e.joined, e.errors)
  }
}

impl<D: Delegate, T: Transport> core::fmt::Debug for JoinError<D, T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !self.joined.is_empty() {
      writeln!(f, "Successes: {}", self.joined.len())?;
    }
    writeln!(f, "Failures:")?;
    for (addr, err) in self.errors.iter() {
      writeln!(f, "\t{}: {}", addr, err)?;
    }
    Ok(())
  }
}

impl<D: Delegate, T: Transport> core::fmt::Display for JoinError<D, T> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !self.joined.is_empty() {
      writeln!(f, "Successes: {}", self.joined.len())?;
    }
    writeln!(f, "Failures:")?;
    for (addr, err) in self.errors.iter() {
      writeln!(f, "\t{}: {}", addr, err)?;
    }
    Ok(())
  }
}

impl<D: Delegate, T: Transport> JoinError<D, T> {
  /// Return the number of successful joined nodes
  pub fn num_joined(&self) -> usize {
    self.joined.len()
  }

  /// Return the joined nodes
  pub const fn joined(&self) -> &Vec<NodeId> {
    &self.joined
  }

  #[allow(clippy::mutable_key_type)]
  pub const fn errors(&self) -> &HashMap<Address, Error<D, T>> {
    &self.errors
  }
}

impl<D, T> Showbiz<D, T>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  #[inline]
  pub async fn new(opts: Options<T>) -> Result<Self, Error<D, T>> {
    Self::new_in(None, None, opts).await
  }

  #[inline]
  pub async fn with_delegate(delegate: D, opts: Options<T>) -> Result<Self, Error<D, T>> {
    Self::new_in(Some(delegate), None, opts).await
  }

  #[inline]
  pub async fn with_keyring(keyring: SecretKeyring, opts: Options<T>) -> Result<Self, Error<D, T>> {
    Self::new_in(None, Some(keyring), opts).await
  }

  #[inline]
  pub async fn with_delegate_and_keyring(
    delegate: D,
    keyring: SecretKeyring,
    opts: Options<T>,
  ) -> Result<Self, Error<D, T>> {
    Self::new_in(Some(delegate), Some(keyring), opts).await
  }

  async fn new_in(
    delegate: Option<D>,
    mut keyring: Option<SecretKeyring>,
    mut opts: Options<T>,
  ) -> Result<Self, Error<D, T>> {
    let (handoff_tx, handoff_rx) = async_channel::bounded(1);
    let (leave_broadcast_tx, leave_broadcast_rx) = async_channel::bounded(1);

    if let Some(pk) = opts.secret_key() {
      let has_keyring = keyring.is_some();
      let keyring = keyring.get_or_insert(SecretKeyring::new(pk));
      if has_keyring {
        keyring.insert(pk);
        keyring
          .use_key(&pk)
          .map_err(|e| Error::Transport(TransportError::Security(e)))?;
      }
    }

    let (config, options) = read_resolv_conf(opts.dns_config_path.as_path())
      .map_err(|e| TransportError::Dns(DnsError::from(e)))?;
    let dns = if config.name_servers().is_empty() {
      tracing::warn!(
        target = "showbiz",
        "no Dns servers found in {}",
        opts.dns_config_path.display()
      );

      None
    } else {
      Some(Dns::new(
        config,
        options,
        crate::dns::AsyncConnectionProvider::new(),
      ))
    };

    opts.transport.get_or_insert_with(|| {
      <T::Options as crate::transport::TransportOptions>::from_addr(opts.bind_addr, opts.bind_port)
    });

    async fn retry<D: Delegate, T: Transport>(
      limit: usize,
      label: Option<Label>,
      opts: T::Options,
      #[cfg(feature = "metrics")] metrics_labels: Arc<Vec<metrics::Label>>,
    ) -> Result<T, Error<D, T>> {
      let mut i = 0;
      loop {
        #[cfg(feature = "metrics")]
        let transport = {
          if !metrics_labels.is_empty() {
            T::with_metrics_labels(label.clone(), opts.clone(), metrics_labels.clone()).await
          } else {
            T::new(label.clone(), opts.clone()).await
          }
        };
        #[cfg(not(feature = "metrics"))]
        let transport = T::new(opts.transport.as_ref().unwrap().clone()).await;

        match transport {
          Ok(t) => return Ok(t),
          Err(e) => {
            tracing::debug!(target="showbiz", err=%e, "fail to create transport");
            if i == limit - 1 {
              return Err(e.into());
            }
            i += 1;
          }
        }
      }
    }

    let limit = match opts.bind_port {
      Some(0) | None => 10,
      Some(_) => 1,
    };
    let transport = retry(
      limit,
      (!opts.label.is_empty()).then_some(opts.label.clone()),
      opts.transport.as_ref().unwrap().clone(),
      #[cfg(feature = "metrics")]
      opts.metrics_labels.clone(),
    )
    .await?;

    if let Some(0) | None = opts.bind_port {
      let port = transport.auto_bind_port();
      opts.bind_port = Some(port);
      tracing::warn!(target = "showbiz", "using dynamic bind port {port}");
    }

    // Get the final advertise address from the transport, which may need
    // to see which address we bound to. We'll refresh this each time we
    // send out an alive message.
    let advertise = transport.final_advertise_addr(opts.advertise_addr, opts.bind_port.unwrap())?;

    let id = NodeId {
      name: opts.name.clone(),
      addr: advertise,
    };
    let awareness = Awareness::new(
      opts.awareness_max_multiplier as isize,
      #[cfg(feature = "metrics")]
      Arc::new(vec![]),
      #[cfg(feature = "metrics")]
      id.clone(),
    );
    let hot = HotData::new();
    let broadcast = TransmitLimitedQueue::new(
      DefaultNodeCalculator::new(hot.num_nodes),
      opts.retransmit_mult,
    );
    let encryption_enabled = if let Some(keyring) = &keyring {
      !keyring.is_empty() && !opts.encryption_algo.is_none()
    } else {
      false
    };

    // TODO: replace this with is_global when IpAddr::is_global is stable
    // https://github.com/rust-lang/rust/issues/27709
    if crate::util::IsGlobalIp::is_global_ip(&advertise.ip()) && !encryption_enabled {
      tracing::warn!(
        target = "showbiz",
        "binding to public address without encryption!"
      );
    }

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let this = Showbiz {
      inner: Arc::new(ShowbizCore {
        id: id.clone(),
        hot: HotData::new(),
        awareness,
        broadcast,
        leave_broadcast_tx,
        leave_broadcast_rx,
        status_change_lock: Mutex::new(()),
        keyring,
        delegate,
        queue: Mutex::new(MessageQueue::new()),
        nodes: Arc::new(RwLock::new(Memberlist::new(id))),
        ack_handlers: Arc::new(Mutex::new(HashMap::new())),
        dns,
        #[cfg(feature = "metrics")]
        metrics_labels: Arc::new(vec![]),
        transport,
        advertise,
        shutdown_tx: ArcSwapOption::from_pointee(shutdown_tx),
        opts: Arc::new(opts),
        handoff_tx,
        handoff_rx,
      }),
    };

    this.stream_listener(shutdown_rx.clone());
    this.packet_handler(shutdown_rx.clone());
    this.packet_listener(shutdown_rx.clone());
    #[cfg(feature = "metrics")]
    this.check_broadcast_queue_depth(shutdown_rx.clone());

    let meta = if let Some(d) = &this.inner.delegate {
      d.node_meta(META_MAX_SIZE)
    } else {
      Bytes::new()
    };

    if meta.len() > META_MAX_SIZE {
      panic!("Node meta data provided is longer than the limit");
    }

    let alive = Alive {
      incarnation: this.next_incarnation(),
      vsn: this.inner.opts.build_vsn_array(),
      meta,
      node: this.inner.id.clone(),
    };
    this.alive_node(alive, None, true).await;
    this.schedule(shutdown_rx).await;
    this
      .inner
      .hot
      .status
      .store(Status::Running, Ordering::Relaxed);
    tracing::debug!(target = "showbiz", local = %this.inner.id, advertise_addr = %advertise, "node is living");
    Ok(this)
  }

  /// Returns a list of all known nodes.
  #[inline]
  pub async fn members(&self) -> Vec<Arc<Node>> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .map(|n| n.state.node.clone())
      .collect()
  }

  /// Leave will broadcast a leave message but will not shutdown the background
  /// listeners, meaning the node will continue participating in gossip and state
  /// updates.
  ///
  /// This will block until the leave message is successfully broadcasted to
  /// a member of the cluster, if any exist or until a specified timeout
  /// is reached.
  ///
  /// This method is safe to call multiple times, but must not be called
  /// after the cluster is already shut down.
  pub async fn leave(&self, timeout: Duration) -> Result<(), Error<D, T>> {
    if !self.is_running() {
      return Err(Error::NotRunning);
    }

    let _mu = self.inner.status_change_lock.lock().await;

    if !self.is_left() {
      self.inner.hot.status.store(Status::Left, Ordering::Relaxed);

      let mut memberlist = self.inner.nodes.write().await;
      if let Some(&idx) = memberlist.node_map.get(&self.inner.id) {
        // This dead message is special, because Node and From are the
        // same. This helps other nodes figure out that a node left
        // intentionally. When Node equals From, other nodes know for
        // sure this node is gone.

        let state = &memberlist.nodes[idx];
        let d = Dead {
          incarnation: state.state.incarnation.load(Ordering::Relaxed),
          node: state.state.node.id.clone(),
          from: state.state.node.id.clone(),
        };

        self.dead_node(&mut memberlist, d).await?;

        // Block until the broadcast goes out
        if memberlist.any_alive() {
          if timeout > Duration::ZERO {
            futures_util::select_biased! {
              rst = self.inner.leave_broadcast_rx.recv().fuse() => {
                if let Err(e) = rst {
                  tracing::error!(
                    target = "showbiz",
                    "failed to receive leave broadcast: {}",
                    e
                  );
                }
              },
              _ = <T::Runtime as Runtime>::sleep(timeout).fuse() => {
                return Err(Error::LeaveTimeout);
              }
            }
          } else if let Err(e) = self.inner.leave_broadcast_rx.recv().await {
            tracing::error!(
              target = "showbiz",
              "failed to receive leave broadcast: {}",
              e
            );
          }
        }
      } else {
        tracing::warn!(target = "showbiz", "leave but we're not a member");
      }
    }
    Ok(())
  }

  /// Used to take an existing Memberlist and attempt to join a cluster
  /// by contacting all the given hosts and performing a state sync. Initially,
  /// the Memberlist only contains our own state, so doing this will cause
  /// remote nodes to become aware of the existence of this node, effectively
  /// joining the cluster.
  ///
  /// This returns the number of hosts successfully contacted and an error if
  /// none could be reached. If an error is returned, the node did not successfully
  /// join the cluster.
  pub async fn join(
    &self,
    existing: impl Iterator<Item = (Address, Name)>,
  ) -> Result<Vec<NodeId>, JoinError<D, T>> {
    if !self.is_running() {
      return Err(JoinError {
        joined: Vec::new(),
        errors: existing
          .into_iter()
          .map(|(addr, _)| (addr, Error::NotRunning))
          .collect(),
      });
    }
    let estimated_total = existing.size_hint().0;

    let (left, right): (FuturesUnordered<_>, FuturesUnordered<_>) = existing
      .into_iter()
      .partition_map(|(addr, name)| match addr {
        Address::Domain { domain, port } => Either::Right(async move {
          let addrs = match self.resolve_addr(&domain, port).await {
            Ok(addrs) => addrs,
            Err(e) => {
              tracing::debug!(
                target = "showbiz",
                err = %e,
                "failed to resolve address {}",
                domain.as_str(),
              );
              return Err((Address::Domain { domain, port }, e));
            }
          };
          let mut errors = Vec::new();
          let mut joined = Vec::with_capacity(addrs.len());
          for addr in addrs {
            let id = NodeId::new(name.clone(), addr);
            tracing::info!(target = "showbiz", local = %self.inner.id, peer = %id, "start join...");
            if let Err(e) = self.push_pull_node(&id, true).await {
              tracing::debug!(
                target = "showbiz",
                local = %self.inner.id,
                err = %e,
                "failed to join {}",
                id,
              );
              errors.push((Address::Socket(addr), e));
            } else {
              joined.push(id);
            }
          }
          Ok((joined, errors))
        }),
        address => {
          let (addr, is_ip) = match address {
            Address::Ip(addr) => (
              SocketAddr::new(addr, self.inner.opts.bind_port.unwrap()),
              true,
            ),
            Address::Socket(addr) => (addr, false),
            Address::Domain { .. } => unreachable!(),
          };
          Either::Left(async move {
            let id = NodeId::new(name, addr);
            tracing::info!(target = "showbiz", local = %self.inner.id, peer = %id, "start join...");
            if let Err(e) = self.push_pull_node(&id, true).await {
              tracing::debug!(
                target = "showbiz",
                local = %self.inner.id,
                err = %e,
                "failed to join {}",
                id,
              );
              let addr = if is_ip {
                Address::Ip(addr.ip())
              } else {
                Address::Socket(addr)
              };
              Err((addr, e))
            } else {
              Ok(id)
            }
          })
        }
      });

    let (left, right) =
      futures_util::future::join(left.collect::<Vec<_>>(), right.collect::<Vec<_>>()).await;

    let num_success = std::cell::RefCell::new(Vec::with_capacity(estimated_total));
    #[allow(clippy::mutable_key_type)]
    let errors = left
      .into_iter()
      .filter_map(|rst| match rst {
        Ok(id) => {
          num_success.borrow_mut().push(id);
          None
        }
        Err((addr, e)) => Some((addr, e)),
      })
      .chain(
        right
          .into_iter()
          .filter_map(|rst| match rst {
            Ok((success, errors)) => {
              num_success.borrow_mut().extend(success);
              if errors.is_empty() {
                None
              } else {
                Some(errors)
              }
            }
            Err((addr, e)) => Some(vec![(addr, e)]),
          })
          .flatten(),
      )
      .collect::<HashMap<_, _>>();

    if errors.is_empty() {
      return Ok(num_success.into_inner());
    }

    Err(JoinError {
      joined: num_success.into_inner(),
      errors,
    })
  }

  /// Gives this instance's idea of how well it is meeting the soft
  /// real-time requirements of the protocol. Lower numbers are better, and zero
  /// means "totally healthy".
  #[inline]
  pub async fn health_score(&self) -> usize {
    self.inner.awareness.get_health_score().await as usize
  }

  /// Used to trigger re-advertising the local node. This is
  /// primarily used with a Delegate to support dynamic updates to the local
  /// meta data.  This will block until the update message is successfully
  /// broadcasted to a member of the cluster, if any exist or until a specified
  /// timeout is reached.
  pub async fn update_node(&self, timeout: Duration) -> Result<(), Error<D, T>> {
    if !self.is_running() {
      return Err(Error::NotRunning);
    }

    // Get the node meta data
    let meta = if let Some(delegate) = &self.inner.delegate {
      let meta = delegate.node_meta(META_MAX_SIZE);
      if meta.len() > META_MAX_SIZE {
        panic!("node meta data provided is longer than the limit");
      }
      meta
    } else {
      Bytes::new()
    };

    // Get the existing node
    // unwrap safe here this is self
    let node_id = {
      let members = self.inner.nodes.read().await;

      let idx = *members.node_map.get(&self.inner.id).unwrap();

      members.nodes[idx].state.id().clone()
    };

    // Format a new alive message
    let alive = Alive {
      incarnation: self.next_incarnation(),
      node: node_id,
      meta,
      vsn: self.inner.opts.build_vsn_array(),
    };
    let (notify_tx, notify_rx) = async_channel::bounded(1);
    self.alive_node(alive, Some(notify_tx), true).await;

    // Wait for the broadcast or a timeout
    if self.any_alive().await {
      if timeout > Duration::ZERO {
        futures_util::select_biased! {
          _ = notify_rx.recv().fuse() => {},
          _ = <T::Runtime as Runtime>::sleep(timeout).fuse() => return Err(Error::UpdateTimeout),
        }
      } else {
        futures_util::select! {
          _ = notify_rx.recv().fuse() => {},
        }
      }
    }

    Ok(())
  }

  /// Uses the unreliable packet-oriented interface of the transport
  /// to target a user message at the given node (this does not use the gossip
  /// mechanism). The maximum size of the message depends on the configured
  /// `packet_buffer_size` for this memberlist instance.
  #[inline]
  pub async fn send(&self, to: &NodeId, msg: Message) -> Result<(), Error<D, T>> {
    if !self.is_running() {
      return Err(Error::NotRunning);
    }
    self.raw_send_msg_packet(to, msg.0).await
  }

  /// Uses the reliable stream-oriented interface of the transport to
  /// target a user message at the given node (this does not use the gossip
  /// mechanism). Delivery is guaranteed if no error is returned, and there is no
  /// limit on the size of the message.
  #[inline]
  pub async fn send_reliable(&self, to: &NodeId, msg: Message) -> Result<(), Error<D, T>> {
    if !self.is_running() {
      return Err(Error::NotRunning);
    }
    self.send_user_msg(to, msg).await
  }
}

// private impelementation
impl<D, T> Showbiz<D, T>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  /// a helper to initiate a TCP-based Dns lookup for the given host.
  /// The built-in Go resolver will do a UDP lookup first, and will only use TCP if
  /// the response has the truncate bit set, which isn't common on Dns servers like
  /// Consul's. By doing the TCP lookup directly, we get the best chance for the
  /// largest list of hosts to join. Since joins are relatively rare events, it's ok
  /// to do this rather expensive operation.
  pub(crate) async fn tcp_lookup_ip(
    &self,
    dns: &Dns<T>,
    host: &str,
    default_port: u16,
  ) -> Result<Vec<SocketAddr>, Error<D, T>> {
    // Don't attempt any TCP lookups against non-fully qualified domain
    // names, since those will likely come from the resolv.conf file.
    if !host.contains('.') {
      return Ok(Vec::new());
    }

    // Make sure the domain name is terminated with a dot (we know there's
    // at least one character at this point).
    let dn = host.chars().last().unwrap();
    let ips = if dn != '.' {
      let mut dn = host.to_string();
      dn.push('.');
      dns.lookup_ip(dn).await
    } else {
      dns.lookup_ip(host).await
    }
    .map_err(Error::dns_resolve)?;

    Ok(
      ips
        .into_iter()
        .map(|ip| SocketAddr::new(ip, default_port))
        .collect(),
    )
  }

  /// Used to resolve the address into an address,
  /// port, and error. If no port is given, use the default
  pub(crate) async fn resolve_addr(
    &self,
    addr: &Domain,
    port: Option<u16>,
  ) -> Result<Vec<SocketAddr>, Error<D, T>> {
    // This captures the supplied port, or the default one.
    let port = port.unwrap_or(
      self
        .inner
        .opts
        .bind_port
        .unwrap_or(self.inner.advertise.port()),
    );

    // First try TCP so we have the best chance for the largest list of
    // hosts to join. If this fails it's not fatal since this isn't a standard
    // way to query Dns, and we have a fallback below.
    if let Some(dns) = self.inner.dns.as_ref() {
      match self.tcp_lookup_ip(dns, addr.as_str(), port).await {
        Ok(ips) => {
          if !ips.is_empty() {
            return Ok(ips);
          }
        }
        Err(e) => {
          tracing::debug!(
            target = "showbiz",
            "TCP-first lookup failed for '{}', falling back to UDP: {}",
            addr,
            e
          );
        }
      }
    }

    // If TCP didn't yield anything then use the normal Go resolver which
    // will try UDP, then might possibly try TCP again if the UDP response
    // indicates it was truncated.
    addr
      .as_str()
      .to_socket_addrs()
      .map_err(|e| Error::Transport(TransportError::Dns(DnsError::IO(e))))
      .map(|addrs| addrs.into_iter().collect())
  }

  #[inline]
  pub(crate) fn get_advertise(&self) -> SocketAddr {
    self.inner.advertise
  }

  /// Check for any other alive node.
  #[inline]
  pub(crate) async fn any_alive(&self) -> bool {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .any(|n| !n.state.dead_or_left() && n.state.node.name() != self.inner.opts.name.as_ref())
  }

  #[cfg(feature = "metrics")]
  fn check_broadcast_queue_depth(&self, shutdown_rx: Receiver<()>) {
    let queue_check_interval = self.inner.opts.queue_check_interval;
    let this = self.clone();

    static QUEUE_BROADCAST: std::sync::Once = std::sync::Once::new();

    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          },
          _ = <T::Runtime as Runtime>::sleep(queue_check_interval).fuse() => {
            let numq = this.inner.broadcast.num_queued().await;
            QUEUE_BROADCAST.call_once(|| {
              metrics::register_gauge!("showbiz.queue.broadcasts");
            });

            metrics::gauge!("showbiz.queue.broadcasts", numq as f64);
          }
        }
      }
    });
  }

  pub(crate) async fn verify_protocol(&self, _remote: &[PushNodeState]) -> Result<(), Error<D, T>> {
    // TODO: now we do not need to handle this situation, because there is no update
    // on protocol.
    Ok(())
  }

  // #[cfg(test)]
  // pub(crate) async fn change_node<F>(&self, _addr: SocketAddr, _f: F)
  // where
  //   F: Fn(&LocalNodeState),
  // {
  //   // let mut nodes = self.inner.nodes.write().await;
  //   // if let Some(n) = nodes.node_map.get_mut(&addr) {
  //   //   f(n)
  //   // }
  // }
}
