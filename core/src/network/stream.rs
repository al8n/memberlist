use std::{collections::HashMap, mem, sync::Arc};

use smol_str::SmolStr;

use crate::{delegate::DelegateError, types::NodeState};

use super::*;

// --------------------------------------------Crate Level Methods-------------------------------------------------
impl<D, T> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  /// A long running thread that pulls incoming streams from the
  /// transport and hands them off for processing.
  pub(crate) fn stream_listener(
    &self,
    shutdown_rx: async_channel::Receiver<()>,
  ) -> <T::Runtime as Runtime>::JoinHandle<()> {
    let this = self.clone();
    let transport_rx = this.inner.transport.stream();
    <T::Runtime as Runtime>::spawn(async move {
      tracing::debug!("memberlist stream listener start");
      let mut handle_id = 0u64;
      let handles = Arc::new(parking_lot::Mutex::new(HashMap::new()));
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            tracing::debug!("memberlist stream listener shutting down");
            let handles = mem::take(&mut *handles.lock());
            futures::future::join_all(handles.into_values()).await;
            return;
          }
          conn = transport_rx.recv().fuse() => {
            match conn {
              Ok((remote_addr, conn)) => {
                let this = this.clone();
                handle_id = handle_id.overflowing_add(1).0;
                let handles1 = handles.clone();
                let (finish_tx, finish_rx) = futures::channel::oneshot::channel();
                <T::Runtime as Runtime>::spawn_detach(async move {
                  this.handle_conn(handle_id, handles1, remote_addr, conn).await;
                  let _ = finish_tx.send(());
                });
                handles.lock().insert(handle_id, finish_rx);
              },
              Err(e) => {
                if !this.inner.shutdown_tx.is_closed() {
                  tracing::error!(local = %this.inner.id, "memberlist stream listener failed to accept connection: {}", e);
                }
                let handles = mem::take(&mut *handles.lock());
                futures::future::join_all(handles.into_values()).await;
                // If we got an error, which means on the other side the transport has been closed,
                // so we need to return and shutdown the stream listener
                return;
              },
            }
          }
        }
      }
    })
  }

  /// Used to merge the remote state with our local state
  pub(crate) async fn merge_remote_state(
    &self,
    node_state: PushPull<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self.verify_protocol(node_state.states.as_slice()).await?;

    // Invoke the merge delegate if any
    if node_state.join {
      if let Some(merge) = self.delegate.as_ref() {
        let peers = node_state
          .states
          .iter()
          .map(|n| {
            Arc::new(NodeState {
              id: n.id().clone(),
              addr: n.address().clone(),
              meta: n.meta.clone(),
              state: n.state,
              protocol_version: n.protocol_version,
              delegate_version: n.delegate_version,
            })
          })
          .collect::<SmallVec<_>>();
        merge.notify_merge(peers).await.map_err(|e| {
          Error::delegate(<<D as Delegate<_, _>>::Error as DelegateError>::merge(e))
        })?;
      }
    }

    // Merge the membership state
    self.merge_state(node_state.states.as_slice()).await;

    // Invoke the delegate for user state
    if let Some(d) = &self.delegate {
      if !node_state.user_data.is_empty() {
        d.merge_remote_state(node_state.user_data, node_state.join)
          .await;
      }
    }
    Ok(())
  }

  pub(crate) async fn send_user_msg(
    &self,
    addr: &<T::Resolver as AddressResolver>::ResolvedAddress,
    msg: Bytes,
  ) -> Result<(), Error<T, D>> {
    let mut conn = self
      .inner
      .transport
      .dial_timeout(addr, self.inner.opts.timeout)
      .await
      .map_err(Error::transport)?;
    self.send_message(&mut conn, Message::UserData(msg)).await?;
    self
      .inner
      .transport
      .cache_stream(addr, conn)
      .await
      .map_err(Error::transport)
  }
}

// ----------------------------------------Module Level Methods------------------------------------
impl<D, T> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(super) async fn send_local_state(
    &self,
    conn: &mut T::Stream,
    join: bool,
  ) -> Result<(), Error<T, D>> {
    // Setup a deadline
    conn.set_timeout(Some(self.inner.opts.timeout));

    // Prepare the local node state
    #[cfg(feature = "metrics")]
    let mut node_state_counts = State::empty_metrics();
    let local_nodes = {
      self
        .inner
        .nodes
        .read()
        .await
        .nodes
        .iter()
        .map(|m| {
          let n = &m.state;
          let this = PushNodeState::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress> {
            id: n.id().clone(),
            addr: n.address().clone(),
            meta: n.meta().clone(),
            incarnation: n.incarnation.load(Ordering::Relaxed),
            state: n.state,
            protocol_version: n.protocol_version,
            delegate_version: n.delegate_version,
          };

          #[cfg(feature = "metrics")]
          {
            node_state_counts[this.state as u8 as usize].1 += 1;
          }
          this
        })
        .collect::<TinyVec<_>>()
    };

    // Get the delegate state
    let user_data = if let Some(delegate) = &self.delegate {
      delegate.local_state(join).await
    } else {
      Bytes::new()
    };

    // Send our node state
    let msg: Message<_, _> = PushPull::new(local_nodes, user_data, join).into();
    #[cfg(feature = "metrics")]
    {
      std::thread_local! {
        #[cfg(not(target_family = "wasm"))]
        static NODE_INSTANCES_GAUGE: std::cell::OnceCell<std::cell::RefCell<memberlist_utils::MetricLabels>> = const { std::cell::OnceCell::new() };

        // TODO: remove this when cargo wasix toolchain update to rust 1.70
        #[cfg(target_family = "wasm")]
        static NODE_INSTANCES_GAUGE: once_cell::sync::OnceCell<std::cell::RefCell<memberlist_utils::MetricLabels>> = once_cell::sync::OnceCell::new();
      }

      NODE_INSTANCES_GAUGE.with(|g| {
        let mut labels = g
          .get_or_init(|| {
            let mut labels = (*self.inner.opts.metric_labels).clone();
            labels.reserve_exact(1);
            std::cell::RefCell::new(labels)
          })
          .borrow_mut();

        for (idx, (node_state, cnt)) in node_state_counts.into_iter().enumerate() {
          let label = metrics::Label::new("node_state", node_state);
          if idx == 0 {
            labels.push(label);
          } else {
            *labels.last_mut().unwrap() = label;
          }
          let iter = labels.iter();
          metrics::gauge!("memberlist.node.instances", iter).set(cnt as f64);
        }
        labels.pop();
      });
    }

    #[cfg(feature = "metrics")]
    {
      use crate::transport::Wire;
      metrics::gauge!(
        "memberlist.size.local",
        self.inner.opts.metric_labels.iter()
      )
      .set(<T::Wire as Wire>::encoded_len(&msg) as f64);
    }

    self.send_message(conn, msg).await
  }
}

// -----------------------------------------Private Level Methods-----------------------------------
impl<D, T> Memberlist<T, D>
where
  D: Delegate<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  /// Handles a single incoming stream connection from the transport.
  async fn handle_conn(
    self,
    task_id: u64,
    handles: Arc<parking_lot::Mutex<HashMap<u64, futures::channel::oneshot::Receiver<()>>>>,
    addr: <T::Resolver as AddressResolver>::ResolvedAddress,
    mut conn: T::Stream,
  ) {
    scopeguard::defer! {
      handles.lock().remove(&task_id);
    }

    tracing::debug!(target =  "memberlist.stream", local = %self.inner.id, peer = %addr, "handle stream connection");

    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "memberlist.promised.accept",
        self.inner.opts.metric_labels.iter()
      )
      .increment(1);
    }

    if self.inner.opts.timeout != Duration::ZERO {
      conn.set_timeout(Some(self.inner.opts.timeout));
    }

    let msg = match self.read_message(&addr, &mut conn).await {
      Ok((_read, msg)) => {
        #[cfg(feature = "metrics")]
        {
          metrics::histogram!(
            "memberlist.size.remote",
            self.inner.opts.metric_labels.iter()
          )
          .record(_read as f64);
        }
        msg
      }
      Err(e) => {
        tracing::error!(target =  "memberlist.stream", err=%e, local = %self.inner.id, remote_node = %addr, "failed to receive");

        let err_resp = ErrorResponse::new(SmolStr::new(e.to_string()));
        if let Err(e) = self.send_message(&mut conn, err_resp.into()).await {
          tracing::error!(target =  "memberlist.stream", err=%e, local = %self.inner.id, remote_node = %addr, "failed to send error response");
          return;
        }

        return;
      }
    };

    match msg {
      Message::Ping(ping) => {
        if ping.target.id().ne(self.local_id()) {
          tracing::error!(target =  "memberlist.stream", local=%self.inner.id, remote = %addr, "got ping for unexpected node {}", ping.target);
          return;
        }

        let ack = Ack::new(ping.seq_no);
        if let Err(e) = self.send_message(&mut conn, ack.into()).await {
          tracing::error!(target =  "memberlist.stream", err=%e, remote_node = %addr, "failed to send ack response");
        }
        if let Err(e) = self.inner.transport.cache_stream(&addr, conn).await {
          tracing::warn!(target =  "memberlist.stream", err=%e, remote_node = %addr, "failed to cache stream");
        }
      }
      Message::PushPull(pp) => {
        // Increment counter of pending push/pulls
        let num_concurrent = self.inner.hot.push_pull_req.fetch_add(1, Ordering::SeqCst);
        scopeguard::defer! {
          self.inner.hot.push_pull_req.fetch_sub(1, Ordering::SeqCst);
        }

        // Check if we have too many open push/pull requests
        if num_concurrent >= MAX_PUSH_PULL_REQUESTS {
          tracing::error!(
            target: "memberlist.stream",
            "too many pending push/pull requests"
          );
          return;
        }

        if let Err(e) = self.send_local_state(&mut conn, pp.join).await {
          tracing::error!(target =  "memberlist.stream", err=%e, remote_node = %addr, "failed to push local state");
          return;
        }

        if let Err(e) = self.merge_remote_state(pp).await {
          tracing::error!(target =  "memberlist.stream", err=%e, remote_node = %addr, "failed to push/pull merge");
        }

        if let Err(e) = self.inner.transport.cache_stream(&addr, conn).await {
          tracing::warn!(target =  "memberlist.stream", err=%e, remote_node = %addr, "failed to cache stream");
        }
      }
      Message::UserData(data) => {
        if let Some(d) = &self.delegate {
          tracing::trace!(target =  "memberlist.stream", remote_node = %addr, data=?data.as_ref(), "notify user message");
          d.notify_message(data).await
        }
      }
      msg => {
        tracing::error!(target =  "memberlist.stream", remote_node = %addr, "received invalid msg type {}", msg.kind());
      }
    }
  }
}
