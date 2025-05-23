use std::sync::Arc;

use agnostic_lite::AsyncSpawner;
use either::Either;
use nodecraft::CheapClone;
use smol_str::SmolStr;

use crate::{
  delegate::DelegateError,
  proto::{MessageRef, ProtoWriter},
  transport::Connection,
};

use super::*;

// --------------------------------------------Crate Level Methods-------------------------------------------------
impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// A long running thread that pulls incoming streams from the
  /// transport and hands them off for processing.
  pub(crate) fn stream_listener(
    &self,
    shutdown_rx: async_channel::Receiver<()>,
  ) -> <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()> {
    let this = self.clone();
    let transport_rx = this.inner.transport.stream();
    <T::Runtime as RuntimeLite>::spawn(async move {
      tracing::debug!("memberlist: stream listener start");
      loop {
        futures::select! {
          _ = shutdown_rx.recv().fuse() => {
            tracing::debug!("memberlist: stream listener exits");
            return;
          }
          conn = transport_rx.recv().fuse() => {
            match conn {
              Ok((remote_addr, conn)) => {
                let this = this.clone();
                <T::Runtime as RuntimeLite>::spawn_detach(async move {
                  this.handle_conn(remote_addr.cheap_clone(), conn).await;
                });
              },
              Err(e) => {
                if !this.inner.shutdown_tx.is_closed() {
                  tracing::error!(local = %this.inner.id, "memberlist stream listener failed to accept connection: {}", e);
                }
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
    pp: <PushPull<T::Id, T::ResolvedAddress> as Data>::Ref<'_>,
  ) -> Result<(), Error<T, D>> {
    let states = pp
      .states()
      .iter::<PushNodeState<T::Id, T::ResolvedAddress>>()
      .map(|res| res.and_then(Data::from_ref))
      .collect::<Result<SmallVec<_>, DecodeError>>()?
      .into_inner();

    let states = match states {
      Either::Left(states) => Arc::from_iter(states),
      Either::Right(e) => Arc::from(e),
    };

    self.verify_protocol(states.as_ref()).await?;

    let join = pp.join();
    let user_data = pp.user_data();

    // Invoke the merge delegate if any
    if join {
      if let Some(merge) = self.delegate.as_ref() {
        let peers = states
          .iter()
          .map(|n| {
            NodeState::new(n.id().cheap_clone(), n.address().cheap_clone(), n.state())
              .with_meta(n.meta().cheap_clone())
              .with_protocol_version(n.protocol_version())
              .with_delegate_version(n.delegate_version())
          })
          .collect::<Arc<[_]>>();
        merge
          .notify_merge(peers)
          .await
          .map_err(|e| Error::delegate(DelegateError::merge(e)))?;
      }
    }

    // Merge the membership state
    self.merge_state(states.as_ref()).await;

    // Invoke the delegate for user state
    if let Some(d) = &self.delegate {
      if !user_data.is_empty() {
        d.merge_remote_state(user_data, join).await;
      }
    }
    Ok(())
  }

  pub(crate) async fn send_user_msg(
    &self,
    addr: &T::ResolvedAddress,
    msgs: OneOrMore<Message<T::Id, T::ResolvedAddress>>,
  ) -> Result<(), Error<T, D>> {
    let conn = self
      .inner
      .transport
      .open(
        addr,
        <T::Runtime as RuntimeLite>::now() + self.inner.opts.timeout,
      )
      .await
      .map_err(Error::transport)?;
    let (_reader, mut writer) = conn.split();
    self.send_message(&mut writer, msgs).await?;
    writer.close().await.map_err(|e| Error::transport(e.into()))
  }
}

// ----------------------------------------Module Level Methods------------------------------------
impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  pub(crate) async fn send_local_state(
    &self,
    conn: &mut <T::Connection as Connection>::Writer,
    join: bool,
  ) -> Result<(), Error<T, D>> {
    let deadline = <T::Runtime as RuntimeLite>::now() + self.inner.opts.timeout;

    // Prepare the local node state
    #[cfg(feature = "metrics")]
    let mut node_state_counts = State::metrics_array();
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
          let this = PushNodeState::new(
            n.incarnation.load(Ordering::Acquire),
            n.id().cheap_clone(),
            n.address().cheap_clone(),
            n.state,
          )
          .with_meta(n.meta().cheap_clone())
          .with_protocol_version(n.protocol_version())
          .with_delegate_version(n.delegate_version());

          #[cfg(feature = "metrics")]
          {
            let state: u8 = this.state().into();
            node_state_counts[state as usize].1 += 1;
          }
          this
        })
        .collect::<TinyVec<_>>()
    };

    // Get the delegate state
    // Send our node state
    let msg: Message<_, _> = if let Some(delegate) = &self.delegate {
      PushPull::new(join, local_nodes.into_iter())
        .with_user_data(delegate.local_state(join).await)
        .into()
    } else {
      PushPull::new(join, local_nodes.into_iter()).into()
    };

    #[cfg(feature = "metrics")]
    {
      std::thread_local! {
        static NODE_INSTANCES_GAUGE: std::cell::OnceCell<std::cell::RefCell<crate::proto::MetricLabels>> = const { std::cell::OnceCell::new() };
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
      metrics::gauge!(
        "memberlist.size.local",
        self.inner.opts.metric_labels.iter()
      )
      .set(msg.encoded_len() as f64);
    }

    match <T::Runtime as RuntimeLite>::timeout_at(deadline, self.send_message(conn, [msg])).await {
      Ok(Ok(_)) => Ok(()),
      Ok(Err(e)) => Err(e),
      Err(e) => Err(Error::transport(std::io::Error::from(e).into())),
    }
  }
}

// -----------------------------------------Private Level Methods-----------------------------------
impl<D, T> Memberlist<T, D>
where
  D: Delegate<Id = T::Id, Address = T::ResolvedAddress>,
  T: Transport,
{
  /// Handles a single incoming stream connection from the transport.
  async fn handle_conn(&self, addr: T::ResolvedAddress, conn: T::Connection) {
    tracing::debug!(local = %self.inner.id, peer = %addr, "memberlist.stream: handle stream connection");

    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "memberlist.promised.accept",
        self.inner.opts.metric_labels.iter()
      )
      .increment(1);
    }

    let deadline = <T::Runtime as RuntimeLite>::now() + self.inner.opts.timeout;

    let (mut reader, mut writter) = conn.split();

    let res =
      <T::Runtime as RuntimeLite>::timeout_at(deadline, self.read_message(&addr, &mut reader))
        .await;

    let payload = match res {
      Err(e) => {
        tracing::error!(err=%e, local = %self.inner.id, remote_node = %addr, "memberlist.stream: failed to receive and send error response");
        return;
      }
      Ok(Err(e)) => {
        tracing::error!(err=%e, local = %self.inner.id, remote_node = %addr, "memberlist.stream: failed to receive");

        let err_resp = ErrorResponse::new(SmolStr::new(e.to_string()));
        let res = <T::Runtime as RuntimeLite>::timeout_at(
          deadline,
          self.send_message(&mut writter, [err_resp.into()]),
        )
        .await;

        match res {
          Ok(Ok(_)) => return,
          Ok(Err(e)) => {
            tracing::error!(err=%e, local = %self.inner.id, remote_node = %addr, "memberlist.stream: failed to send error response");
            return;
          }
          Err(e) => {
            tracing::error!(err=%e, local = %self.inner.id, remote_node = %addr, "memberlist.stream: failed to send error response");
            return;
          }
        }
      }
      Ok(Ok(payload)) => payload,
    };

    tracing::trace!(local = %self.inner.id, remote_node = %addr, payload=?payload.as_ref(), "memberlist.stream: received message");

    let msg = match <MessageRef<'_, _, _> as DataRef<Message<T::Id, T::ResolvedAddress>>>::decode(
      &payload,
    ) {
      Ok((_, msg)) => msg,
      Err(e) => {
        tracing::error!(local=%self.inner.id, remote = %addr, err=%e, "memberlist.stream: failed to decode message");
        return;
      }
    };

    match msg {
      MessageRef::Ping(ping) => {
        let tid = match T::Id::from_ref(*ping.target().id()) {
          Ok(tid) => tid,
          Err(e) => {
            tracing::error!(local=%self.inner.id, remote = %addr, err=%e, "memberlist.stream: failed to decode target id");
            return;
          }
        };

        if tid.ne(self.local_id()) {
          tracing::error!(local=%self.inner.id, remote = %addr, "memberlist.stream: got ping for unexpected node {:?}", ping.target());
          return;
        }

        let ack = Ack::new(ping.sequence_number());
        let res = <T::Runtime as RuntimeLite>::timeout_at(
          deadline,
          self.send_message(&mut writter, [ack.into()]),
        )
        .await;
        match res {
          Ok(Ok(_)) => {}
          Ok(Err(e)) => {
            tracing::error!(err=%e, local = %self.inner.id, remote_node = %addr, "memberlist.stream: failed to send ack response");
          }
          Err(e) => {
            tracing::error!(err=%e, local = %self.inner.id, remote_node = %addr, "memberlist.stream: failed to send ack response");
          }
        }
      }
      MessageRef::PushPull(pp) => {
        // Increment counter of pending push/pulls
        let num_concurrent = self.inner.hot.push_pull_req.fetch_add(1, Ordering::SeqCst);
        scopeguard::defer! {
          self.inner.hot.push_pull_req.fetch_sub(1, Ordering::SeqCst);
        }

        // Check if we have too many open push/pull requests
        if num_concurrent >= MAX_PUSH_PULL_REQUESTS {
          tracing::error!("memberlist.stream: too many pending push/pull requests");
          return;
        }

        if let Err(e) = self.send_local_state(&mut writter, pp.join()).await {
          tracing::error!(err=%e, remote_node = %addr, "memberlist.stream: failed to push local state");
          return;
        }

        if let Err(e) = self.merge_remote_state(pp).await {
          tracing::error!(err=%e, remote_node = %addr, "memberlist.stream: failed to push/pull merge");
        }
      }
      MessageRef::UserData(data) => {
        if let Some(d) = &self.delegate {
          tracing::trace!(remote_node = %addr, data=?data, "memberlist.stream: notify user message");
          d.notify_message(data.into()).await
        }
      }
      msg => {
        tracing::error!(remote_node = %addr, type=%msg.ty(), "memberlist.stream: received invalid msg");
      }
    }
  }
}
