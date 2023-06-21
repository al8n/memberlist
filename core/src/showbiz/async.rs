use std::{future::Future, net::ToSocketAddrs, sync::atomic::Ordering, time::Duration};

use crate::{
  dns::DnsError,
  transport::TransportError,
  types::{Address, Dead, Domain},
  util::read_resolv_conf,
};

use super::*;

use agnostic::Runtime;
use arc_swap::{strategy::DefaultStrategy, ArcSwapOption, Guard};
use async_lock::{Mutex, RwLock};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, Stream, StreamExt};
use itertools::{Either, Itertools};

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct Runner<T: Transport> {
  transport: T,
  /// We do not call send directly, just directly drop it.
  #[allow(dead_code)]
  shutdown_tx: Sender<()>,
  advertise: SocketAddr,
}

#[cfg(feature = "async")]
pub(crate) struct AckHandler {
  pub(crate) ack_fn:
    Box<dyn FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  pub(crate) nack_fn: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>>,
  pub(crate) timer: Timer,
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizCore<D: Delegate, T: Transport, R: Runtime> {
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
  nodes: Arc<RwLock<Memberlist<R>>>,
  ack_handlers: Arc<Mutex<HashMap<u32, AckHandler>>>,
  dns: Option<Dns<T, R>>,
  #[cfg(feature = "metrics")]
  metrics_labels: Arc<Vec<metrics::Label>>,
  runner: ArcSwapOption<Runner<T>>,
  opts: Arc<Options<T>>,
  _marker: std::marker::PhantomData<R>,
}

impl<D: Delegate, T: Transport, R: Runtime> Drop for ShowbizCore<D, T, R> {
  fn drop(&mut self) {
    if let Some(runner) = self.runner.swap(None) {
      if let Err(e) = runner.transport.block_shutdown() {
        tracing::error!(target = "showbiz", err=%e, "failed to block shutdown showbiz");
      }
    }
  }
}

pub struct Showbiz<D: Delegate, T: Transport, R: Runtime> {
  pub(crate) inner: Arc<ShowbizCore<D, T, R>>,
}

impl<D, T, R> Clone for Showbiz<D, T, R>
where
  T: Transport,
  D: Delegate,
  R: Runtime,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<D, T, R> Showbiz<D, T, R>
where
  D: Delegate,
  T: Transport<Runtime = R>,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  #[inline]
  pub async fn new(opts: Options<T>, runtime: R) -> Result<Self, Error<D, T>> {
    Self::new_in(None, None, opts, runtime).await
  }

  #[inline]
  pub async fn with_delegate(
    delegate: D,
    opts: Options<T>,
    runtime: R,
  ) -> Result<Self, Error<D, T>> {
    Self::new_in(Some(delegate), None, opts, runtime).await
  }

  #[inline]
  pub async fn with_keyring(
    keyring: SecretKeyring,
    opts: Options<T>,
    runtime: R,
  ) -> Result<Self, Error<D, T>> {
    Self::new_in(None, Some(keyring), opts, runtime).await
  }

  #[inline]
  pub async fn with_delegate_and_keyring(
    delegate: D,
    keyring: SecretKeyring,
    opts: Options<T>,
    runtime: R,
  ) -> Result<Self, Error<D, T>> {
    Self::new_in(Some(delegate), Some(keyring), opts, runtime).await
  }

  async fn new_in(
    delegate: Option<D>,
    mut keyring: Option<SecretKeyring>,
    mut opts: Options<T>,
    runtime: R,
  ) -> Result<Self, Error<D, T>> {
    let (handoff_tx, handoff_rx) = async_channel::bounded(1);
    let (leave_broadcast_tx, leave_broadcast_rx) = async_channel::bounded(1);

    if let Some(pk) = opts.secret_key() {
      let has_keyring = keyring.is_some();
      let keyring = keyring.get_or_insert(SecretKeyring::new(vec![], pk));
      if has_keyring {
        let mut mu = keyring.lock().await;
        mu.insert(pk);
        mu.use_key(&pk)?;
      }
    }

    opts.transport.get_or_insert_with(|| <T::Options as crate::transport::TransportOptions>::from_addr(opts.bind_addr));

    let id = NodeId {
      name: opts.name.clone(),
      addr: opts.bind_addr,
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
        crate::dns::AsyncConnectionProvider::new(runtime),
      ))
    };

    Ok(Showbiz {
      inner: Arc::new(ShowbizCore {
        id,
        hot: HotData::new(),
        awareness,
        broadcast,
        leave_broadcast_tx,
        leave_broadcast_rx,
        status_change_lock: Mutex::new(()),
        keyring,
        delegate,
        queue: Mutex::new(MessageQueue::new()),
        nodes: Arc::new(RwLock::new(Memberlist::new(opts.name.clone()))),
        ack_handlers: Arc::new(Mutex::new(HashMap::new())),
        dns,
        #[cfg(feature = "metrics")]
        metrics_labels: Arc::new(vec![]),
        runner: ArcSwapOption::empty(),
        opts: Arc::new(opts),
        _marker: std::marker::PhantomData,
        handoff_tx,
        handoff_rx,
      }),
    })
  }

  #[inline]
  pub async fn bootstrap(&self) -> Result<(), Error<D, T>> {
    // if we already in running status, just return
    if self.is_running() {
      return Ok(());
    }

    let _mu = self.inner.status_change_lock.lock().await;
    // mark self as running
    self
      .inner
      .hot
      .status
      .store(Status::Running, Ordering::Relaxed);

    #[cfg(feature = "metrics")]
    let transport = {
      if !self.inner.metrics_labels.is_empty() {
        T::with_metrics_labels(
          self.inner.opts.transport.as_ref().unwrap().clone(),
          self.inner.metrics_labels.clone(),
        )
        .await?
      } else {
        T::new(self.inner.opts.transport.as_ref().unwrap().clone()).await?
      }
    };
    #[cfg(not(feature = "metrics"))]
    let transport = T::new(self.inner.opts.transport.as_ref().unwrap().clone()).await?;

    // Get the final advertise address from the transport, which may need
    // to see which address we bound to. We'll refresh this each time we
    // send out an alive message.
    let advertise = transport.final_advertise_addr(self.inner.opts.advertise_addr)?;
    let encryption_enabled = self.encryption_enabled().await;

    // TODO: replace this with is_global when IpAddr::is_global is stable
    // https://github.com/rust-lang/rust/issues/27709
    if crate::util::IsGlobalIp::is_global_ip(&advertise.ip()) && !encryption_enabled {
      tracing::warn!(
        target = "showbiz",
        "binding to public address without encryption!"
      );
    }

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let runtime = Runner {
      transport,
      shutdown_tx,
      advertise,
    };
    self.inner.runner.store(Some(Arc::new(runtime)));

    self.stream_listener(shutdown_rx.clone());
    self.packet_handler(shutdown_rx.clone());
    self.packet_listener(shutdown_rx.clone());

    let meta = if let Some(d) = &self.inner.delegate {
      d.node_meta(META_MAX_SIZE)
    } else {
      Bytes::new()
    };

    if meta.len() > META_MAX_SIZE {
      self.inner.runner.store(None);
      panic!("Node meta data provided is longer than the limit");
    }

    let alive = Alive {
      incarnation: self.next_incarnation(),
      vsn: self.inner.opts.build_vsn_array(),
      meta,
      node: self.inner.id.clone(),
    };

    self.alive_node(alive, None, true).await;
    self.schedule(shutdown_rx).await;
    Ok(())
  }

  /// Returns a list of all known live nodes.
  #[inline]
  pub async fn members(&self) -> Vec<Arc<Node>> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .map(|n| n.node.clone())
      .collect()
  }

  /// Returns the number of alive nodes currently known. Between
  /// the time of calling this and calling Members, the number of alive nodes
  /// may have changed, so this shouldn't be used to determine how many
  /// members will be returned by Members.
  #[inline]
  pub async fn num_members(&self) -> usize {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .filter(|n| !n.dead_or_left())
      .count()
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
      if let Some(state) = memberlist.node_map.get(&memberlist.local) {
        // This dead message is special, because Node and From are the
        // same. This helps other nodes figure out that a node left
        // intentionally. When Node equals From, other nodes know for
        // sure this node is gone.

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
              _ = R::sleep(timeout).fuse() => {
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
  #[allow(clippy::mutable_key_type)]
  pub async fn join(
    &self,
    existing: HashMap<Name, Address>,
  ) -> Result<(usize, HashMap<Address, Vec<Error<D, T>>>), Error<D, T>> {
    if !self.is_running() {
      return Err(Error::NotRunning);
    }

    let (left, right): (FuturesUnordered<_>, FuturesUnordered<_>) = existing
      .into_iter()
      .partition_map(|(name, addr)| match addr {
        Address::Socket(addr) => Either::Left(async move {
          if let Err(e) = self.push_pull_node(&name, addr, true).await {
            tracing::debug!(
              target = "showbiz",
              err = %e,
              "failed to join {}({})",
              name,
              addr
            );
            Err((Address::Socket(addr), e))
          } else {
            Ok(())
          }
        }),
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
              return (0, Address::Domain { domain, port }, vec![e]);
            }
          };
          let mut errors = Vec::new();
          let mut num_success = 0;
          for addr in addrs {
            if let Err(e) = self.push_pull_node(&name, addr, true).await {
              tracing::debug!(
                target = "showbiz",
                err = %e,
                "failed to join {}({})",
                name,
                addr
              );
              errors.push(e);
            } else {
              num_success += 1;
            }
          }
          (num_success, Address::Domain { domain, port }, errors)
        }),
      });

    let mut num_success = 0;
    let (left, right) =
      futures_util::future::join(left.collect::<Vec<_>>(), right.collect::<Vec<_>>()).await;

    let mut errors = HashMap::new();
    for rst in left {
      if let Err((addr, e)) = rst {
        errors.insert(addr, vec![e]);
      } else {
        num_success += 1;
      }
    }

    for (success, addr, errs) in right {
      errors.insert(addr, errs);
      num_success += success;
    }

    Ok((num_success, errors))
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
    let node_id = self
      .inner
      .nodes
      .read()
      .await
      .node_map
      .get(&self.inner.id.name)
      .unwrap()
      .state
      .id()
      .clone();

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
          _ = R::sleep(timeout).fuse() => return Err(Error::UpdateTimeout),
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

  /// Stop any background maintenance of network activity
  /// for this memberlist, causing it to appear "dead". A leave message
  /// will not be broadcasted prior, so the cluster being left will have
  /// to detect this node's shutdown using probing. If you wish to more
  /// gracefully exit the cluster, call Leave prior to shutting down.
  ///
  /// This method is safe to call multiple times.
  #[inline]
  pub async fn shutdown(&self) -> Result<(), Error<D, T>> {
    // if we already in shutdown state, just return
    if self.is_shutdown() {
      return Ok(());
    }

    let _mu = self.inner.status_change_lock.lock().await;
    // mark self as shutdown
    self
      .inner
      .hot
      .status
      .store(Status::Shutdown, Ordering::Relaxed);
    if let Some(runner) = self.inner.runner.swap(None) {
      runner.transport.shutdown().await?;
    }

    Ok(())
  }
}

// private impelementation
impl<D, T, R> Showbiz<D, T, R>
where
  D: Delegate,
  T: Transport,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  #[inline]
  pub(crate) fn runner(&self) -> Guard<Option<Arc<Runner<T>>>, DefaultStrategy> {
    self.inner.runner.load()
  }

  /// a helper to initiate a TCP-based Dns lookup for the given host.
  /// The built-in Go resolver will do a UDP lookup first, and will only use TCP if
  /// the response has the truncate bit set, which isn't common on Dns servers like
  /// Consul's. By doing the TCP lookup directly, we get the best chance for the
  /// largest list of hosts to join. Since joins are relatively rare events, it's ok
  /// to do this rather expensive operation.
  pub(crate) async fn tcp_lookup_ip(
    &self,
    dns: &Dns<T, R>,
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
    mut port: Option<u16>,
  ) -> Result<Vec<SocketAddr>, Error<D, T>> {
    // This captures the supplied port, or the default one.
    if port.is_none() {
      port = Some(self.inner.opts.bind_addr.port());
    }

    // First try TCP so we have the best chance for the largest list of
    // hosts to join. If this fails it's not fatal since this isn't a standard
    // way to query Dns, and we have a fallback below.
    if let Some(dns) = self.inner.dns.as_ref() {
      match self.tcp_lookup_ip(dns, addr.as_str(), port.unwrap()).await {
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
    // Unwrap is safe here, because advertise is always set before get_advertise is called.
    self.inner.runner.load().as_ref().unwrap().advertise
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
      .any(|n| !n.dead_or_left() && n.node.name() != self.inner.opts.name.as_ref())
  }

  pub(crate) async fn encryption_enabled(&self) -> bool {
    if let Some(keyring) = &self.inner.keyring {
      !keyring.lock().await.is_empty() && !self.inner.opts.encryption_algo.is_none()
    } else {
      false
    }
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
