use std::{net::ToSocketAddrs, sync::atomic::Ordering, time::Duration};

use crate::util::{ensure_port, split_host_port};

use super::*;

use futures_channel::oneshot::channel;
use futures_timer::Delay;
use futures_util::FutureExt;
use showbiz_types::{RemoteNode, SmolStr};

impl<T, D> ShowbizBuilder<T, D>
where
  T: Transport,
  D: Delegate,
{
  pub async fn finalize<B: Broadcast>(self) -> Result<Showbiz<B, T, D>, Error<B, T, D>> {
    let Self {
      opts,
      transport,
      delegate,
      keyring,
    } = self;

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let (handoff_tx, handoff_rx) = async_channel::bounded(1);

    let vsn = opts.build_vsn_array();
    let name = opts.name.clone();
    let advertise = transport
      .final_advertise_addr(opts.advertise_addr)
      .map_err(Error::transport)?;
    let meta = if let Some(d) = &delegate {
      d.node_meta(META_MAX_SIZE)
    } else {
      Bytes::new()
    };
    let encryption_enabled = if let Some(keyring) = &keyring {
      !keyring.lock().await.is_empty()
    } else {
      false
    };
    if advertise.ip().is_global() && !encryption_enabled {
      tracing::warn!(
        target = "showbiz",
        "binding to public address without encryption!"
      );
    }

    // TODO: alive node

    let awareness = Awareness::new(opts.awareness_max_multiplier as isize, Arc::new(vec![]));
    let hot = HotData::new();
    let broadcast = TransmitLimitedQueue::new(
      DefaultNodeCalculator::new(hot.num_nodes.clone()),
      opts.retransmit_mult,
    );

    // let num_nodes = hot.num_nodes;
    Ok(Showbiz {
      inner: Arc::new(ShowbizCore {
        awareness,
        broadcast,
        hot: HotData::new(),
        advertise: RwLock::new(advertise),
        leave_lock: Mutex::new(()),
        opts: Arc::new(opts),
        transport,
        delegate,
        keyring,
        shutdown_rx,
        shutdown_tx,
        handoff_tx,
        handoff_rx,
        queue: Mutex::new(MessageQueue::new()),
        nodes: RwLock::new(Memberlist::new(LocalNodeState {
          node: Arc::new(Node {
            full_address: Address {
              addr: advertise,
              name,
            },
            meta,
            state: NodeState::Dead,
            pmin: vsn[0],
            pmax: vsn[1],
            pcur: vsn[2],
            dmin: vsn[3],
            dmax: vsn[4],
            dcur: vsn[5],
          }),
          incarnation: 0,
          state_change: Instant::now(),
        })),
      }),
    })
  }
}

impl<B, T, D> Showbiz<B, T, D>
where
  B: Broadcast,
  T: Transport,
  D: Delegate,
{
  /// Used to take an existing Memberlist and attempt to join a cluster
  /// by contacting all the given hosts and performing a state sync. Initially,
  /// the Memberlist only contains our own state, so doing this will cause
  /// remote nodes to become aware of the existence of this node, effectively
  /// joining the cluster.
  ///
  /// This returns the number of hosts successfully contacted and an error if
  /// none could be reached. If an error is returned, the node did not successfully
  /// join the cluster.
  pub async fn join(&self, existing: Vec<String>) -> Result<usize, Vec<Error<B, T, D>>> {
    let mut num_success = 0;
    let mut errors = Vec::new();
    for exist in existing {
      let exist: SmolStr = exist.into();
      let addrs = match self.resolve_addr(exist.clone()).await {
        Ok(addrs) => addrs,
        Err(e) => {
          tracing::debug!(
            target = "showbiz",
            err = %e,
            "failed to resolve address {}",
            exist
          );
          errors.push(e);
          continue;
        }
      };

      for addr in addrs {
        let sa = addr.addr();
        if let Err(e) = self.push_pull_node(addr, true).await {
          tracing::debug!(
            target = "showbiz",
            err = %e,
            "failed to join {}",
            sa
          );
          errors.push(e);
        } else {
          num_success += 1;
        }
      }
    }

    if num_success == 0 {
      return Err(errors);
    }

    Ok(num_success)
  }

  /// Used to return the local Node
  #[inline]
  pub async fn local_node(&self) -> Arc<Node> {
    self.inner.nodes.read().await.local.node.clone()
  }

  /// Used to trigger re-advertising the local node. This is
  /// primarily used with a Delegate to support dynamic updates to the local
  /// meta data.  This will block until the update message is successfully
  /// broadcasted to a member of the cluster, if any exist or until a specified
  /// timeout is reached.
  pub async fn update_node(&self, timeout: Duration) -> Result<(), Error<B, T, D>> {
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
    let node_addr = self
      .inner
      .nodes
      .read()
      .await
      .node_map
      .get(&self.inner.opts.name)
      .unwrap()
      .address();

    // Format a new alive message
    let alive = Alive {
      incarnation: self.next_incarnation(),
      node: self.inner.opts.name.clone(),
      addr: node_addr.into(),
      meta,
      vsn: self.inner.opts.build_vsn_array(),
    };
    let (notify_tx, notify_rx) = channel();
    self.alive_node(alive, notify_tx, true).await?;

    // Wait for the broadcast or a timeout
    if self.any_alive().await {
      if timeout > Duration::ZERO {
        futures_util::select_biased! {
          _ = notify_rx.fuse() => {},
          _ = Delay::new(timeout).fuse() => return Err(Error::UpdateTimeout),
        }
      } else {
        futures_util::select! {
          _ = notify_rx.fuse() => {},
        }
      }
    }

    Ok(())
  }

  /// Uses the unreliable packet-oriented interface of the transport
  /// to target a user message at the given node (this does not use the gossip
  /// mechanism). The maximum size of the message depends on the configured
  /// `packet_buffer_size` for this memberlist instance.
  pub async fn send_best_effort(&self, to: &Node, mut msg: Vec<u8>) -> Result<(), Error<B, T, D>> {
    // Encode as a user message
    let addr = Address {
      addr: to.address(),
      name: to.full_address().name().clone(),
    };

    // TODO: implement
    Ok(())
  }

  /// Uses the reliable stream-oriented interface of the transport to
  /// target a user message at the given node (this does not use the gossip
  /// mechanism). Delivery is guaranteed if no error is returned, and there is no
  /// limit on the size of the message.
  #[inline]
  pub async fn send_reliable(&self, to: &Node, msg: Message) -> Result<(), Error<B, T, D>> {
    self.send_user_msg(to.full_address(), msg).await
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

  pub async fn shutdown<P>(self, parker: P) -> Result<(), Error<B, T, D>>
  where
    P: std::future::Future<Output = ()> + Copy,
  {
    // Shut down the transport first, which should block until it's
    // completely torn down. If we kill the memberlist-side handlers
    // those I/O handlers might get stuck.
    let Self { inner: core } = self;

    while Arc::strong_count(&core) > 1 {
      parker.await;
    }

    let ShowbizCore {
      hot,
      awareness,
      advertise,
      broadcast,
      shutdown_rx,
      shutdown_tx,
      leave_lock,
      opts,
      transport,
      keyring,
      delegate,
      handoff_tx,
      handoff_rx,
      queue,
      nodes,
    } = Arc::into_inner(core).unwrap();

    // Shut down the transport first, which should block until it's
    // completely torn down. If we kill the memberlist-side handlers
    // those I/O handlers might get stuck.
    transport.shutdown().await.map_err(Error::transport)?;

    // Now tear down everything else.
    hot.shutdown.store(1, Ordering::SeqCst);
    drop(shutdown_tx);

    Ok(())
  }
}

// private impelementation
impl<B, T, D> Showbiz<B, T, D>
where
  B: Broadcast,
  T: Transport,
  D: Delegate,
{
  /// a helper to initiate a TCP-based DNS lookup for the given host.
  /// The built-in Go resolver will do a UDP lookup first, and will only use TCP if
  /// the response has the truncate bit set, which isn't common on DNS servers like
  /// Consul's. By doing the TCP lookup directly, we get the best chance for the
  /// largest list of hosts to join. Since joins are relatively rare events, it's ok
  /// to do this rather expensive operation.
  pub(crate) async fn tcp_lookup_ip(
    &self,
    host: &str,
    default_port: u16,
    node_name: Option<&Name>,
  ) -> Result<Vec<Address>, Error<B, T, D>> {
    // Don't attempt any TCP lookups against non-fully qualified domain
    // names, since those will likely come from the resolv.conf file.
    if !host.contains('.') {
      return Ok(Vec::new());
    }

    todo!()
  }

  /// Used to resolve the address into an address,
  /// port, and error. If no port is given, use the default
  pub(crate) async fn resolve_addr(
    &self,
    mut raw: SmolStr,
  ) -> Result<Vec<Address>, Error<B, T, D>> {
    let (host, node_name) = if let Some(pos) = raw.find('/') {
      if pos == 0 {
        return Err(Error::EmptyNodeName);
      }
      (raw.split(pos), Some(Name::from(raw)))
    } else {
      (raw, None)
    };

    // This captures the supplied port, or the default one.
    let host = ensure_port(&host, self.inner.opts.bind_addr.port());

    // If it looks like an IP address we are done.
    if let Ok(addr) = host.as_str().parse::<SocketAddr>() {
      return Ok(vec![Address {
        addr,
        name: node_name.unwrap_or_default(),
      }]);
    }

    let (host, port) = split_host_port(&host)?;

    // First try TCP so we have the best chance for the largest list of
    // hosts to join. If this fails it's not fatal since this isn't a standard
    // way to query DNS, and we have a fallback below.
    match self.tcp_lookup_ip(host.as_str(), port, node_name).await {
      Ok(ips) => {
        if !ips.is_empty() {
          return Ok(ips);
        }
      }
      Err(e) => {
        tracing::debug!(
          target = "showbiz",
          "TCP-first lookup failed for '{}', falling back to UDP: {}",
          host,
          e
        );
      }
    }

    // If TCP didn't yield anything then use the normal Go resolver which
    // will try UDP, then might possibly try TCP again if the UDP response
    // indicates it was truncated.
    host.to_socket_addrs().map_err(Into::into).map(|addrs| {
      addrs
        .into_iter()
        .map(|addr| Address {
          addr,
          name: node_name.clone().unwrap_or_default(),
        })
        .collect()
    })
  }

  #[inline]
  pub(crate) async fn get_advertise(&self) -> SocketAddr {
    *self.inner.advertise.read().await
  }

  #[inline]
  pub(crate) async fn set_advertise(&self, addr: SocketAddr) {
    *self.inner.advertise.write().await = addr;
  }

  #[inline]
  pub(crate) async fn refresh_advertise(&self) -> Result<SocketAddr, Error<B, T, D>> {
    let addr = self
      .inner
      .transport
      .final_advertise_addr(self.inner.opts.advertise_addr)
      .map_err(Error::transport)?;
    self.set_advertise(addr).await;
    Ok(addr)
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
      !keyring.lock().await.is_empty()
    } else {
      false
    }
  }

  pub(crate) async fn verify_protocol(
    &self,
    remote: &[PushNodeState],
  ) -> Result<(), Error<B, T, D>> {
    // TODO: implement

    Ok(())
  }

  #[cfg(test)]
  pub(crate) async fn change_node<F>(&self, addr: SocketAddr, mut f: F)
  where
    F: Fn(&LocalNodeState),
  {
    let mut nodes = self.inner.nodes.write().await;
    if let Some(n) = nodes.node_map.get_mut(&addr) {
      f(n)
    }
  }
}
