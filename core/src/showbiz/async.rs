use std::{future::Future, sync::atomic::Ordering, time::Duration};

use crate::{
  delegate::VoidDelegate,
  types::{Dead, PushServerState},
};

use super::*;

use agnostic::Runtime;
use async_lock::{Mutex, RwLock};
use futures::{future::BoxFuture, FutureExt, Stream};
use nodecraft::{resolver::AddressResolver, CheapClone};

// #[cfg(feature = "test")]
// pub(crate) mod tests;

pub(crate) struct AckHandler {
  pub(crate) ack_fn:
    Box<dyn FnOnce(Bytes, Instant) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
  pub(crate) nack_fn: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>>,
  pub(crate) timer: Timer,
}

#[viewit::viewit(getters(skip), setters(skip))]
pub(crate) struct ShowbizCore<T: Transport> {
  id: T::Id,
  hot: HotData,
  awareness: Awareness,
  broadcast: TransmitLimitedQueue<
    ShowbizBroadcast<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress, T::Wire>,
  >,
  leave_broadcast_tx: Sender<()>,
  leave_lock: Mutex<()>,
  leave_broadcast_rx: Receiver<()>,
  shutdown_lock: Mutex<()>,
  handoff_tx: Sender<()>,
  handoff_rx: Receiver<()>,
  queue: Mutex<MessageQueue<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  nodes:
    Arc<RwLock<Memberlist<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress, T::Runtime>>>,
  ack_handlers: Arc<Mutex<HashMap<u32, AckHandler>>>,
  transport: Arc<T>,
  /// We do not call send directly, just directly drop it.
  shutdown_tx: Sender<()>,
  advertise: <T::Resolver as AddressResolver>::ResolvedAddress,
  opts: Arc<Options>,
}

impl<T: Transport> Drop for ShowbizCore<T> {
  fn drop(&mut self) {
    self.shutdown_tx.close();
    if let Err(e) = self.transport.block_shutdown() {
      tracing::error!(target:  "showbiz", err=%e, "failed to shutdown");
    }
  }
}

pub struct Showbiz<
  T,
  D = VoidDelegate<
    <T as Transport>::Id,
    <<T as Transport>::Resolver as AddressResolver>::ResolvedAddress,
  >,
> where
  T: Transport,
{
  pub(crate) inner: Arc<ShowbizCore<T>>,
  pub(crate) delegate: Option<Arc<D>>,
}

impl<D, T> Clone for Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      delegate: self.delegate.clone(),
    }
  }
}

impl<D, T> Showbiz<T, D>
where
  D: Delegate,
  T: Transport,
{
  /// Returns the local node ID.
  #[inline]
  pub fn local_id(&self) -> &T::Id {
    &self.inner.id
  }

  /// Returns a [`Node`] with the local id and the advertise address of local node.
  #[inline]
  pub fn advertise_node(&self) -> Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress> {
    Node::new(self.inner.id.clone(), self.inner.advertise.clone())
  }

  /// Returns the delegate, if any.
  #[inline]
  pub fn delegate(&self) -> Option<&D> {
    self.delegate.as_deref()
  }

  // /// Returns the keyring used for the local node
  // #[inline]
  // pub fn keyring(&self) -> Option<&SecretKeyring> {
  //   self.inner.opts.secret_keyring.as_ref()
  // }

  #[inline]
  pub async fn local_server(
    &self,
  ) -> Arc<Server<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
    let nodes = self.inner.nodes.read().await;
    // TODO: return an error
    nodes
      .node_map
      .get(&self.inner.id)
      .map(|&idx| nodes.nodes[idx].state.server.clone())
      .unwrap()
  }

  /// Returns a list of all known nodes.
  #[inline]
  pub async fn members(
    &self,
  ) -> Vec<Arc<Server<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .map(|n| n.state.server.clone())
      .collect()
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
}

pub struct JoinError<T: Transport, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  joined: Vec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  errors: HashMap<Node<T::Id, <T::Resolver as AddressResolver>::Address>, Error<T, D>>,
}

impl<D, T: Transport> From<JoinError<T, D>>
  for (
    Vec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    HashMap<Node<T::Id, <T::Resolver as AddressResolver>::Address>, Error<T, D>>,
  )
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn from(e: JoinError<T, D>) -> Self {
    (e.joined, e.errors)
  }
}

impl<D, T: Transport> core::fmt::Debug for JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !self.joined.is_empty() {
      writeln!(f, "Successes: {:?}", self.joined)?;
    }

    if !self.errors.is_empty() {
      writeln!(f, "Failures:")?;
      for (addr, err) in self.errors.iter() {
        writeln!(f, "\t{}: {}", addr, err)?;
      }
    }

    Ok(())
  }
}

impl<D, T: Transport> core::fmt::Display for JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if !self.joined.is_empty() {
      writeln!(f, "Successes: {:?}", self.joined)?;
    }

    if !self.errors.is_empty() {
      writeln!(f, "Failures:")?;
      for (addr, err) in self.errors.iter() {
        writeln!(f, "\t{addr}: {err}")?;
      }
    }

    Ok(())
  }
}

impl<D: Delegate, T: Transport> std::error::Error for JoinError<T, D> where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>
{
}

impl<D: Delegate, T: Transport> JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  /// Return the number of successful joined nodes
  pub fn num_joined(&self) -> usize {
    self.joined.len()
  }

  /// Return the joined nodes
  pub const fn joined(
    &self,
  ) -> &Vec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
    &self.joined
  }
}

impl<D: Delegate, T: Transport> JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
{
  pub const fn errors(
    &self,
  ) -> &HashMap<Node<T::Id, <T::Resolver as AddressResolver>::Address>, Error<T, D>> {
    &self.errors
  }
}

impl<T> Showbiz<T>
where
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  #[inline]
  pub async fn new(
    transport: T,
    opts: Options,
  ) -> Result<Self, Error<T, VoidDelegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>
  {
    Self::create(transport, None, opts).await
  }
}

impl<D, T> Showbiz<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  #[inline]
  pub async fn with_delegate(
    transport: T,
    delegate: D,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    Self::create(transport, Some(delegate), opts).await
  }

  pub(crate) async fn create(
    transport: T,
    delegate: Option<D>,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    let (shutdown_rx, advertise, this) = Self::new_in(transport, delegate, opts).await?;
    let meta = if let Some(d) = &this.delegate {
      d.node_meta(META_MAX_SIZE)
    } else {
      Bytes::new()
    };

    if meta.len() > META_MAX_SIZE {
      panic!("Server meta data provided is longer than the limit");
    }

    let alive = Alive {
      incarnation: this.next_incarnation(),
      meta,
      node: Node::new(this.inner.id.clone(), this.inner.advertise.clone()),
      protocol_version: this.inner.opts.protocol_version,
      delegate_version: this.inner.opts.delegate_version,
    };
    this.alive_node(alive, None, true).await;
    this.schedule(shutdown_rx).await;
    tracing::debug!(target:  "showbiz", local = %this.inner.id, advertise_addr = %advertise, "node is living");
    Ok(this)
  }

  pub(crate) async fn new_in(
    transport: T,
    delegate: Option<D>,
    opts: Options,
  ) -> Result<
    (
      Receiver<()>,
      <T::Resolver as AddressResolver>::ResolvedAddress,
      Self,
    ),
    Error<T, D>,
  > {
    let (handoff_tx, handoff_rx) = async_channel::bounded(1);
    let (leave_broadcast_tx, leave_broadcast_rx) = async_channel::bounded(1);

    // if let Some(pk) = opts.secret_key() {
    //   let has_keyring = opts.secret_keyring.is_some();
    //   let keyring = opts.secret_keyring.get_or_insert(SecretKeyring::new(pk));
    //   if has_keyring {
    //     keyring.insert(pk);
    //     keyring
    //       .use_key(&pk)
    //       .map_err(|e| Error::Transport(TransportError::Security(e)))?;
    //   }
    // }

    // TODO: move this to concrete transport implementation
    // opts.transport.get_or_insert_with(|| {
    //   <T::Options as crate::transport::TransportOptions>::from_addr(opts.bind_addr, opts.bind_port)
    // });

    // async fn retry<D: Delegate, T: Transport>(
    //   limit: usize,
    //   label: Option<Label>,
    //   opts: T::Options,
    //   #[cfg(feature = "metrics")] metric_labels: Arc<Vec<metrics::Label>>,
    // ) -> Result<T, Error<T, D>> {
    //   let mut i = 0;
    //   loop {
    //     #[cfg(feature = "metrics")]
    //     let transport = {
    //       if !metric_labels.is_empty() {
    //         T::with_metric_labels(label.clone(), opts.clone(), metric_labels.clone()).await
    //       } else {
    //         T::new(label.clone(), opts.clone()).await
    //       }
    //     };
    //     #[cfg(not(feature = "metrics"))]
    //     let transport = T::new(opts.transport.as_ref().unwrap().clone()).await;

    //     match transport {
    //       Ok(t) => return Ok(t),
    //       Err(e) => {
    //         tracing::debug!(target="showbiz", err=%e, "fail to create transport");
    //         if i == limit - 1 {
    //           return Err(e.into());
    //         }
    //         i += 1;
    //       }
    //     }
    //   }
    // }

    // let limit = match opts.bind_port {
    //   Some(0) | None => 10,
    //   Some(_) => 1,
    // };
    // let transport = retry(
    //   limit,
    //   (!opts.label.is_empty()).then_some(opts.label.clone()),
    //   opts.transport.as_ref().unwrap().clone(),
    //   #[cfg(feature = "metrics")]
    //   opts.metric_labels.clone(),
    // )
    // .await?;

    // if let Some(0) | None = opts.bind_port {
    //   let port = transport.auto_bind_port();
    //   opts.bind_port = Some(port);
    //   tracing::warn!(target:  "showbiz", "using dynamic bind port {port}");
    // }

    // Get the final advertise address from the transport, which may need
    // to see which address we bound to. We'll refresh this each time we
    // send out an alive message.
    let advertise = transport.advertise_address();
    let id = transport.local_id();
    let node = Node::new(id.clone(), advertise.clone());
    let awareness = Awareness::new(
      opts.awareness_max_multiplier as isize,
      #[cfg(feature = "metrics")]
      Arc::new(vec![]),
    );
    let hot = HotData::new();
    let num_nodes = hot.num_nodes.clone();
    let broadcast = TransmitLimitedQueue::new(opts.retransmit_mult, move || {
      num_nodes.load(Ordering::Acquire) as usize
    });
    // let encryption_enabled = if let Some(keyring) = &opts.secret_keyring {
    //   !keyring.is_empty() && !opts.encryption_algo.is_none()
    // } else {
    //   false
    // };

    // // TODO: replace this with is_global when IpAddr::is_global is stable
    // // https://github.com/rust-lang/rust/issues/27709
    // if crate::util::IsGlobalIp::is_global_ip(&advertise.ip()) && encrypted_enable {
    //   tracing::warn!(
    //     target: "showbiz",
    //     "binding to public address without encryption!"
    //   );
    // }

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let this = Showbiz {
      inner: Arc::new(ShowbizCore {
        id: id.cheap_clone(),
        hot,
        awareness,
        broadcast,
        leave_broadcast_tx,
        leave_lock: Mutex::new(()),
        leave_broadcast_rx,
        shutdown_lock: Mutex::new(()),
        handoff_tx,
        handoff_rx,
        queue: Mutex::new(MessageQueue::new()),
        nodes: Arc::new(RwLock::new(Memberlist::new(node))),
        ack_handlers: Arc::new(Mutex::new(HashMap::new())),
        shutdown_tx,
        advertise: advertise.cheap_clone(),
        transport: Arc::new(transport),
        opts: Arc::new(opts),
      }),
      delegate: delegate.map(Arc::new),
    };

    this.stream_listener(shutdown_rx.clone());
    this.packet_handler(shutdown_rx.clone());
    this.packet_listener(shutdown_rx.clone());
    #[cfg(feature = "metrics")]
    this.check_broadcast_queue_depth(shutdown_rx.clone());

    Ok((shutdown_rx, this.inner.advertise.cheap_clone(), this))
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
  pub async fn leave(&self, timeout: Duration) -> Result<(), Error<T, D>> {
    let _mu = self.inner.leave_lock.lock().await;

    if self.has_shutdown() {
      panic!("leave after shutdown");
    }

    if !self.has_left() {
      self.inner.hot.leave.store(true, Ordering::Relaxed);

      let mut memberlist = self.inner.nodes.write().await;
      if let Some(&idx) = memberlist.node_map.get(&self.inner.id) {
        // This dead message is special, because Server and From are the
        // same. This helps other nodes figure out that a node left
        // intentionally. When Server equals From, other nodes know for
        // sure this node is gone.

        let state = &memberlist.nodes[idx];
        let d = Dead {
          incarnation: state.state.incarnation.load(Ordering::Relaxed),
          node: state.id().cheap_clone(),
          from: state.id().cheap_clone(),
        };

        self.dead_node(&mut memberlist, d).await?;

        // Block until the broadcast goes out
        if memberlist.any_alive() {
          if timeout > Duration::ZERO {
            futures::select_biased! {
              rst = self.inner.leave_broadcast_rx.recv().fuse() => {
                if let Err(e) = rst {
                  tracing::error!(
                    target: "showbiz",
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
              target: "showbiz",
              "failed to receive leave broadcast: {}",
              e
            );
          }
        }
      } else {
        tracing::warn!(target:  "showbiz", "leave but we're not a member");
      }
    }
    Ok(())
  }

  /// Join directly by contacting the given node id
  pub async fn join_node(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::Address>,
  ) -> Result<(), Error<T, D>> {
    if self.has_left() || self.has_shutdown() {
      return Err(Error::NotRunning);
    }

    let (id, addr) = node.into_components();
    let addr = self
      .inner
      .transport
      .resolve(&addr)
      .await
      .map_err(Error::Transport)?;
    self.push_pull_node(Node::new(id, addr), true).await
  }

  // /// Join directly by contacting the given node ids
  // pub async fn join_nodes(
  //   &self,
  //   ids: &[ServerId],
  // ) -> Result<(), Error<T, D>> {
  //   if !self.is_running() {
  //     return Err(Error::NotRunning);
  //   }

  //   self.push_pull_node(ids, true).await
  // }

  /// Used to take an existing Showbiz and attempt to join a cluster
  /// by contacting all the given hosts and performing a state sync. Initially,
  /// the Showbiz only contains our own state, so doing this will cause
  /// remote nodes to become aware of the existence of this node, effectively
  /// joining the cluster.
  ///
  /// This returns the number of hosts successfully contacted and an error if
  /// none could be reached. If an error is returned, the node did not successfully
  /// join the cluster.
  pub async fn join_many(
    &self,
    existing: impl Iterator<Item = Node<T::Id, <T::Resolver as AddressResolver>::Address>>,
  ) -> Result<Vec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>, JoinError<T, D>>
  {
    if self.has_left() || self.has_shutdown() {
      return Err(JoinError {
        joined: Vec::new(),
        errors: existing
          .into_iter()
          .map(|n| (n, Error::NotRunning))
          .collect(),
      });
    }
    let estimated_total = existing.size_hint().0;

    let futs = existing
      .into_iter()
      .map(|node| {
        async move {
          let resolved_addr = self.inner.transport.resolve(node.address()).await.map_err(|e| {
            tracing::debug!(
              target: "showbiz",
              err = %e,
              "failed to resolve address {}",
              node.address(),
            );
            (node.cheap_clone(), Error::<T, D>::transport(e))
          })?;
          let id = Node::new(node.id().cheap_clone(), resolved_addr);
          tracing::info!(target:  "showbiz", local = %self.inner.transport.local_id(), peer = %id, "start join...");
          if let Err(e) = self.push_pull_node(id.cheap_clone(), true).await {
            tracing::debug!(
              target: "showbiz",
              local = %self.inner.id,
              err = %e,
              "failed to join {}",
              id,
            );
            Err((node, e))
          } else {
            Ok(id)
          }
        }
      });

    let res = futures::future::join_all(futs).await;

    let num_success = std::cell::RefCell::new(Vec::with_capacity(estimated_total));
    let errors = res
      .into_iter()
      .filter_map(|rst| match rst {
        Ok(node) => {
          num_success.borrow_mut().push(node);
          None
        }
        Err((node, e)) => Some((node, e)),
      })
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
    self.inner.awareness.get_health_score() as usize
  }

  /// Used to trigger re-advertising the local node. This is
  /// primarily used with a Delegate to support dynamic updates to the local
  /// meta data.  This will block until the update message is successfully
  /// broadcasted to a member of the cluster, if any exist or until a specified
  /// timeout is reached.
  pub async fn update_node(&self, timeout: Duration) -> Result<(), Error<T, D>> {
    if self.has_left() || self.has_shutdown() {
      return Err(Error::NotRunning);
    }

    // Get the node meta data
    let meta = if let Some(delegate) = &self.delegate {
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
    let node = {
      let members = self.inner.nodes.read().await;

      let idx = *members.node_map.get(&self.inner.id).unwrap();

      let state = &members.nodes[idx].state;
      Node::new(state.id().cheap_clone(), state.address().cheap_clone())
    };

    // Format a new alive message
    let alive = Alive {
      incarnation: self.next_incarnation(),
      node,
      meta,
      protocol_version: self.inner.opts.protocol_version,
      delegate_version: self.inner.opts.delegate_version,
    };
    let (notify_tx, notify_rx) = async_channel::bounded(1);
    self.alive_node(alive, Some(notify_tx), true).await;

    // Wait for the broadcast or a timeout
    if self.any_alive().await {
      if timeout > Duration::ZERO {
        futures::select_biased! {
          _ = notify_rx.recv().fuse() => {},
          _ = <T::Runtime as Runtime>::sleep(timeout).fuse() => return Err(Error::UpdateTimeout),
        }
      } else {
        futures::select! {
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
  pub async fn send(
    &self,
    to: &<T::Resolver as AddressResolver>::ResolvedAddress,
    msg: Bytes,
  ) -> Result<(), Error<T, D>> {
    if self.has_left() || self.has_shutdown() {
      return Err(Error::NotRunning);
    }
    self.send_packet(to, Message::UserData(msg)).await
  }

  /// Uses the reliable stream-oriented interface of the transport to
  /// target a user message at the given node (this does not use the gossip
  /// mechanism). Delivery is guaranteed if no error is returned, and there is no
  /// limit on the size of the message.
  #[inline]
  pub async fn send_reliable(
    &self,
    to: &<T::Resolver as AddressResolver>::ResolvedAddress,
    msg: Bytes,
  ) -> Result<(), Error<T, D>> {
    if self.has_left() || self.has_shutdown() {
      return Err(Error::NotRunning);
    }
    self.send_user_msg(to, msg).await
  }

  /// Stop any background maintenance of network activity
  /// for this showbiz, causing it to appear "dead". A leave message
  /// will not be broadcasted prior, so the cluster being left will have
  /// to detect this node's shutdown using probing. If you wish to more
  /// gracefully exit the cluster, call Leave prior to shutting down.
  ///
  /// This method is safe to call multiple times.
  pub async fn shutdown(&self) -> Result<(), Error<T, D>> {
    let _mu = self.inner.shutdown_lock.lock().await;

    if self.has_shutdown() {
      return Ok(());
    }

    // Shut down the transport first, which should block until it's
    // completely torn down. If we kill the memberlist-side handlers
    // those I/O handlers might get stuck.
    if let Err(e) = self.inner.transport.shutdown().await {
      tracing::error!(target:  "showbiz", err=%e, "failed to shutdown transport");
    }

    // Now tear down everything else.
    self.inner.hot.shutdown.store(true, Ordering::Relaxed);
    self.inner.shutdown_tx.close();
    Ok(())
  }
}

// private impelementation
impl<D, T> Showbiz<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  #[inline]
  pub(crate) fn get_advertise(&self) -> &<T::Resolver as AddressResolver>::ResolvedAddress {
    &self.inner.advertise
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
      .any(|n| !n.state.dead_or_left() && n.state.server.id().ne(&self.inner.id))
  }

  #[cfg(feature = "metrics")]
  fn check_broadcast_queue_depth(&self, shutdown_rx: Receiver<()>) {
    let queue_check_interval = self.inner.opts.queue_check_interval;
    let this = self.clone();

    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          },
          _ = <T::Runtime as Runtime>::sleep(queue_check_interval).fuse() => {
            let numq = this.inner.broadcast.num_queued();
            metrics::histogram!("showbiz.queue.broadcasts").record(numq as f64);
          }
        }
      }
    });
  }

  pub(crate) async fn verify_protocol(
    &self,
    _remote: &[PushServerState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>],
  ) -> Result<(), Error<T, D>> {
    // TODO: now we do not need to handle this situation, because there is no update
    // on protocol.
    Ok(())
  }

  // #[cfg(test)]
  // pub(crate) async fn change_node<F>(&self, _addr: SocketAddr, _f: F)
  // where
  //   F: Fn(&LocalServerState),
  // {
  //   // let mut nodes = self.inner.nodes.write().await;
  //   // if let Some(n) = nodes.node_map.get_mut(&addr) {
  //   //   f(n)
  //   // }
  // }
}

#[test]
fn test() {}
