use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use agnostic_lite::{RuntimeLite, time::Instant};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use smallvec_wrapper::OneOrMore;

use super::{
  Options,
  base::Memberlist,
  delegate::{Delegate, VoidDelegate},
  error::Error,
  network::META_MAX_SIZE,
  proto::{Alive, Dead, MaybeResolvedAddress, Message, Meta, NodeState, Ping, SmallVec},
  state::AckMessage,
  transport::{AddressResolver, CheapClone, Node, Transport},
};

impl<T, D> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// Returns the local node ID.
  #[inline]
  pub fn local_id(&self) -> &T::Id {
    &self.inner.id
  }

  /// Returns the local node address
  #[inline]
  pub fn local_address(&self) -> &<T::Resolver as AddressResolver>::Address {
    self.inner.transport.local_address()
  }

  /// Returns a [`Node`] with the local id and the advertise address of local node.
  #[inline]
  pub fn advertise_node(&self) -> Node<T::Id, T::ResolvedAddress> {
    Node::new(self.inner.id.clone(), self.inner.advertise.clone())
  }

  /// Returns the advertise address of local node.
  #[inline]
  pub fn advertise_address(&self) -> &T::ResolvedAddress {
    &self.inner.advertise
  }

  /// Returns the keyring (only used for encryption) of the node
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[inline]
  pub fn keyring(&self) -> Option<&super::keyring::Keyring> {
    self.inner.keyring.as_ref()
  }

  /// Returns `true` if the node enables encryption.
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  #[inline]
  pub fn encryption_enabled(&self) -> bool {
    self.inner.keyring.is_some()
      && self.inner.opts.encryption_algo.is_some()
      && self.inner.opts.gossip_verify_outgoing
  }

  /// Returns the delegate, if any.
  #[inline]
  pub fn delegate(&self) -> Option<&D> {
    self.delegate.as_deref()
  }

  /// Returns the local node instance state.
  #[inline]
  pub async fn local_state(&self) -> Option<Arc<NodeState<T::Id, T::ResolvedAddress>>> {
    let nodes = self.inner.nodes.read().await;
    nodes
      .node_map
      .get(&self.inner.id)
      .map(|&idx| nodes.nodes[idx].state.server.clone())
  }

  /// Returns the node state of the given id. (if any).
  pub async fn by_id(&self, id: &T::Id) -> Option<Arc<NodeState<T::Id, T::ResolvedAddress>>> {
    let members = self.inner.nodes.read().await;

    members
      .node_map
      .get(id)
      .map(|&idx| members.nodes[idx].state.server.clone())
  }

  /// Returns a list of all known nodes.
  #[inline]
  pub async fn members(&self) -> SmallVec<Arc<NodeState<T::Id, T::ResolvedAddress>>> {
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

  /// Returns number of members
  #[inline]
  pub async fn num_members(&self) -> usize {
    self.inner.nodes.read().await.nodes.len()
  }

  /// Returns a list of all known nodes that are online.
  pub async fn online_members(&self) -> SmallVec<Arc<NodeState<T::Id, T::ResolvedAddress>>> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .filter(|n| !n.dead_or_left())
      .map(|n| n.state.server.clone())
      .collect()
  }

  /// Returns the number of online members.
  pub async fn num_online_members(&self) -> usize {
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

  /// Returns a list of all known nodes that match the given predicate.
  pub async fn members_by(
    &self,
    mut f: impl FnMut(&NodeState<T::Id, T::ResolvedAddress>) -> bool,
  ) -> SmallVec<Arc<NodeState<T::Id, T::ResolvedAddress>>> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .filter(|n| f(&n.state))
      .map(|n| n.state.server.clone())
      .collect()
  }

  /// Returns the number of members match the given predicate.
  pub async fn num_members_by(
    &self,
    mut f: impl FnMut(&NodeState<T::Id, T::ResolvedAddress>) -> bool,
  ) -> usize {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .filter(|n| f(&n.state))
      .count()
  }

  /// Returns a list of map result on all known members that match the given predicate.
  pub async fn members_map_by<O>(
    &self,
    mut f: impl FnMut(&NodeState<T::Id, T::ResolvedAddress>) -> Option<O>,
  ) -> SmallVec<O> {
    self
      .inner
      .nodes
      .read()
      .await
      .nodes
      .iter()
      .filter_map(|n| f(&n.state))
      .collect()
  }
}

impl<T> Memberlist<T>
where
  T: Transport,
{
  /// Create a new memberlist with the given transport and options.
  #[inline]
  pub async fn new(
    transport_options: T::Options,
    opts: Options,
  ) -> Result<Self, Error<T, VoidDelegate<T::Id, T::ResolvedAddress>>> {
    Self::create(None, transport_options, opts).await
  }
}

impl<T, D> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// Create a new memberlist with the given transport, delegate and options.
  #[inline]
  pub async fn with_delegate(
    delegate: D,
    transport_options: T::Options,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    Self::create(Some(delegate), transport_options, opts).await
  }

  pub(crate) async fn create(
    delegate: Option<D>,
    transport_options: T::Options,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    let transport = T::new(transport_options).await.map_err(Error::Transport)?;
    let (shutdown_rx, advertise, this) = Self::new_in(transport, delegate, opts).await?;
    let meta = if let Some(d) = &this.delegate {
      d.node_meta(META_MAX_SIZE).await
    } else {
      Meta::empty()
    };

    if meta.len() > META_MAX_SIZE {
      panic!("NodeState meta data provided is longer than the limit");
    }

    let alive = Alive::new(
      this.next_incarnation(),
      Node::new(this.inner.id.clone(), this.inner.advertise.clone()),
    )
    .with_meta(meta)
    .with_protocol_version(this.inner.opts.protocol_version)
    .with_delegate_version(this.inner.opts.delegate_version);
    this.alive_node(alive, None, true).await;
    this.schedule(shutdown_rx).await;
    tracing::debug!(local = %this.inner.id, advertise_addr = %advertise, "memberlist: node is living");
    Ok(this)
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
  ///
  /// Returns `true` if the node has successfully left the cluster by this call.
  pub async fn leave(&self, timeout: Duration) -> Result<bool, Error<T, D>> {
    if self.has_shutdown() {
      return Ok(false);
    }

    if self.has_left() {
      return Ok(false);
    }

    if !self.has_left() {
      self.inner.hot.leave.store(true, Ordering::Release);

      let mut memberlist = self.inner.nodes.write().await;
      if let Some(&idx) = memberlist.node_map.get(&self.inner.id) {
        // This dead message is special, because NodeState and From are the
        // same. This helps other nodes figure out that a node left
        // intentionally. When NodeState equals From, other nodes know for
        // sure this node is gone.

        let state = &memberlist.nodes[idx];
        let d = Dead::new(
          state.state.incarnation.load(Ordering::Acquire),
          state.id().cheap_clone(),
          state.id().cheap_clone(),
        );

        self.dead_node(&mut memberlist, d).await?;
        let any_alive = memberlist.any_alive();
        drop(memberlist);

        // Block until the broadcast goes out
        if any_alive {
          if timeout > Duration::ZERO {
            futures::select! {
              _ = self.inner.leave_broadcast_rx.recv().fuse() => {},
              _ = <T::Runtime as RuntimeLite>::sleep(timeout).fuse() => {
                return Err(Error::LeaveTimeout);
              }
            }
          } else if let Err(e) = self.inner.leave_broadcast_rx.recv().await {
            tracing::error!("memberlist: failed to receive leave broadcast: {}", e);
          }
        }
      } else {
        tracing::warn!("memberlist: leave but we're not a member");
      }
    }

    Ok(true)
  }

  /// Join directly by contacting the given node id,
  /// Returns the node if successfully joined, or an error if the node could not be reached.
  pub async fn join(
    &self,
    node: Node<T::Id, MaybeResolvedAddress<T::Address, T::ResolvedAddress>>,
  ) -> Result<Node<T::Id, T::ResolvedAddress>, Error<T, D>> {
    if self.has_left() || self.has_shutdown() {
      return Err(Error::NotRunning);
    }

    let (id, addr) = node.into_components();
    let addr = match addr {
      MaybeResolvedAddress::Resolved(addr) => addr,
      MaybeResolvedAddress::Unresolved(addr) => self
        .inner
        .transport
        .resolve(&addr)
        .await
        .map_err(Error::Transport)?,
    };
    let n = Node::new(id, addr);
    self.push_pull_node(n.cheap_clone(), true).await.map(|_| n)
  }

  /// Used to take an existing Memberlist and attempt to join a cluster
  /// by contacting all the given hosts and performing a state sync. Initially,
  /// the Memberlist only contains our own state, so doing this will cause
  /// remote nodes to become aware of the existence of this node, effectively
  /// joining the cluster.
  ///
  /// On success, returns a list of all nodes that were successfully joined with resolved addresses.
  /// On error, returns a list of nodes are successfully joined with resolved addresses and the error.
  pub async fn join_many(
    &self,
    existing: impl Iterator<Item = Node<T::Id, MaybeResolvedAddress<T::Address, T::ResolvedAddress>>>,
  ) -> Result<
    SmallVec<Node<T::Id, T::ResolvedAddress>>,
    (SmallVec<Node<T::Id, T::ResolvedAddress>>, Error<T, D>),
  > {
    if self.has_left() || self.has_shutdown() {
      return Err((Default::default(), Error::NotRunning));
    }

    let estimated_total = existing.size_hint().0;

    let futs = existing
      .into_iter()
      .map(|node| {
        async move {
          let (id, addr) = node.into_components();
          let resolved_addr = match addr {
            MaybeResolvedAddress::Resolved(addr) => addr,
            MaybeResolvedAddress::Unresolved(addr) => {
              match self.inner.transport.resolve(&addr).await {
                Ok(addr) => addr,
                Err(e) => {
                  tracing::debug!(
                    err = %e,
                    "memberlist: failed to resolve address {}",
                    addr,
                  );
                  return Err((Node::new(id, MaybeResolvedAddress::<T::Address, T::ResolvedAddress>::unresolved(addr)), Error::<T, D>::transport(e)))
                }
              }
            }
          };
          let node = Node::new(id, resolved_addr);
          tracing::info!(local = %self.inner.transport.local_id(), peer = %node, "memberlist: start join...");
          if let Err(e) = self.push_pull_node(node.cheap_clone(), true).await {
            tracing::debug!(
              local = %self.inner.id,
              err = %e,
              "memberlist: failed to join {}",
              node,
            );
            let (id, addr) = node.into_components();
            Err((Node::new(id, MaybeResolvedAddress::Resolved(addr)), e))
          } else {
            Ok(node)
          }
        }
      }).collect::<futures::stream::FuturesUnordered<_>>();

    let successes = std::cell::RefCell::new(SmallVec::with_capacity(estimated_total));
    let errors = futs
      .filter_map(|rst| async {
        match rst {
          Ok(node) => {
            successes.borrow_mut().push(node);
            None
          }
          Err((_, e)) => Some(e),
        }
      })
      .collect::<OneOrMore<_>>()
      .await;

    match Error::try_from_one_or_more(errors) {
      Ok(()) => Ok(successes.into_inner()),
      Err(e) => Err((successes.into_inner(), e)),
    }
  }

  /// Gives this instance's idea of how well it is meeting the soft
  /// real-time requirements of the protocol. Lower numbers are better, and zero
  /// means "totally healthy".
  #[inline]
  pub fn health_score(&self) -> usize {
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
      let meta = delegate.node_meta(META_MAX_SIZE).await;
      if meta.len() > META_MAX_SIZE {
        panic!("node meta data provided is longer than the limit");
      }
      meta
    } else {
      Meta::empty()
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
    let alive = Alive::new(self.next_incarnation(), node)
      .with_meta(meta)
      .with_protocol_version(self.inner.opts.protocol_version)
      .with_delegate_version(self.inner.opts.delegate_version);
    let (notify_tx, notify_rx) = async_channel::bounded(1);
    self.alive_node(alive, Some(notify_tx), true).await;

    // Wait for the broadcast or a timeout
    if self.any_alive().await {
      if timeout > Duration::ZERO {
        let _ = <T::Runtime as RuntimeLite>::timeout(timeout, notify_rx.recv())
          .await
          .map_err(|_| Error::UpdateTimeout)?;
      } else {
        let _ = notify_rx.recv().await;
      }
    }

    Ok(())
  }

  /// Uses the unreliable packet-oriented interface of the transport
  /// to target a user message at the given node (this does not use the gossip
  /// mechanism). The maximum size of the message depends on the configured
  /// `packet_buffer_size` for this memberlist instance.
  ///
  /// See also [`send_reliable`](Memberlist::send_reliable).
  #[inline]
  pub async fn send(&self, to: &T::ResolvedAddress, msg: Bytes) -> Result<(), Error<T, D>> {
    self.send_many(to, std::iter::once(msg)).await
  }

  /// Uses the unreliable packet-oriented interface of the transport
  /// to target a user message at the given node (this does not use the gossip
  /// mechanism). The maximum size of the message depends on the configured
  /// `packet_buffer_size` for this memberlist instance.
  #[inline]
  pub async fn send_many(
    &self,
    to: &T::ResolvedAddress,
    msgs: impl Iterator<Item = Bytes>,
  ) -> Result<(), Error<T, D>> {
    if self.has_left() || self.has_shutdown() {
      return Err(Error::NotRunning);
    }

    let stream = self
      .transport_send_packets(to, msgs.map(Message::UserData).collect::<OneOrMore<_>>())
      .await;
    futures::pin_mut!(stream);
    match stream.next().await {
      None => Ok(()),
      Some(Ok(_)) => Ok(()),
      Some(Err(e)) => Err(e),
    }
  }

  /// Uses the reliable stream-oriented interface of the transport to
  /// target a user message at the given node (this does not use the gossip
  /// mechanism). Delivery is guaranteed if no error is returned, and there is no
  /// limit on the size of the message.
  ///
  /// See also [`send_many_reliable`](Memberlist::send_many_reliable).
  #[inline]
  pub async fn send_reliable(
    &self,
    to: &T::ResolvedAddress,
    msg: Bytes,
  ) -> Result<(), Error<T, D>> {
    self.send_many_reliable(to, std::iter::once(msg)).await
  }

  /// Uses the reliable stream-oriented interface of the transport to
  /// target a user message at the given node (this does not use the gossip
  /// mechanism). Delivery is guaranteed if no error is returned, and there is no
  /// limit on the size of the message.
  #[inline]
  pub async fn send_many_reliable(
    &self,
    to: &T::ResolvedAddress,
    msgs: impl Iterator<Item = Bytes>,
  ) -> Result<(), Error<T, D>> {
    if self.has_left() || self.has_shutdown() {
      return Err(Error::NotRunning);
    }
    self
      .send_user_msg(to, msgs.map(Message::UserData).collect())
      .await
  }

  /// Initiates a ping to the node with the specified node.
  pub async fn ping(&self, node: Node<T::Id, T::ResolvedAddress>) -> Result<Duration, Error<T, D>> {
    // Prepare a ping message and setup an ack handler.
    let self_addr = self.get_advertise();
    let ping = Ping::new(
      self.next_sequence_number(),
      Node::new(self.inner.transport.local_id().clone(), self_addr.clone()),
      node.clone(),
    );

    let (ack_tx, ack_rx) = async_channel::bounded(self.inner.opts.indirect_checks + 1);
    self.inner.ack_manager.set_probe_channels(
      ping.sequence_number(),
      ack_tx,
      None,
      <T::Runtime as RuntimeLite>::now(),
      self.inner.opts.probe_interval,
    );

    // Send a ping to the node.
    // Wait to send or timeout.
    match <T::Runtime as RuntimeLite>::timeout(self.inner.opts.probe_timeout, async {
      let stream = self.send_packets(node.address(), ping.into()).await;
      futures::pin_mut!(stream);
      let errs = stream.collect::<OneOrMore<_>>().await;
      let num_errs = errs.len();

      match num_errs {
        0 => Ok(()),
        _ => match errs.into_either() {
          either::Either::Left([e]) => Err(e),
          either::Either::Right(e) => Err(Error::Multiple(e.into_vec().into())),
        },
      }
    })
    .await
    {
      Ok(Ok(())) => {}
      Ok(Err(e)) => return Err(e),
      Err(_) => {
        // If we timed out, return Error.
        tracing::debug!(
          "memberlist: failed ping {} by packet (timeout reached)",
          node
        );
        return Err(Error::Lost(node));
      }
    }

    // Mark the sent time here, which should be after any pre-processing and
    // system calls to do the actual send. This probably under-reports a bit,
    // but it's the best we can do.
    let sent = <T::Runtime as RuntimeLite>::now();

    // Wait for response or timeout.
    futures::select! {
      v = ack_rx.recv().fuse() => {
        // If we got a response, update the RTT.
        if let Ok(AckMessage { complete, .. }) = v {
          if complete {
            return Ok(sent.elapsed());
          }
        }
      }
      _ = <T::Runtime as RuntimeLite>::sleep(self.inner.opts.probe_timeout).fuse() => {}
    }

    // If we timed out, return Error.
    tracing::debug!(
      "memberlist: failed ping {} by packet (timeout reached)",
      node
    );
    Err(Error::Lost(node))
  }

  /// Stop any background maintenance of network activity
  /// for this memberlist, causing it to appear "dead". A leave message
  /// will not be broadcasted prior, so the cluster being left will have
  /// to detect this node's shutdown using probing. If you wish to more
  /// gracefully exit the cluster, call Leave prior to shutting down.
  ///
  /// This method is safe to call multiple times.
  pub async fn shutdown(&self) -> Result<(), Error<T, D>> {
    self.inner.shutdown().await.map_err(Error::Transport)
  }
}
