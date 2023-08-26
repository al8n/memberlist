use std::sync::Arc;

// use agnostic::lock::RwLock;
use either::Either;
use futures_util::{Future, Stream};
use rkyv::{
  de::deserializers::{SharedDeserializeMap, SharedDeserializeMapError},
  Deserialize,
};

use crate::{
  transport::ReliableConnection,
  types::{Node, NodeId},
};

use super::*;

// --------------------------------------------Crate Level Methods-------------------------------------------------
impl<D, T> Showbiz<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  /// A long running thread that pulls incoming streams from the
  /// transport and hands them off for processing.
  pub(crate) fn stream_listener(&self, shutdown_rx: async_channel::Receiver<()>) {
    let this = self.clone();
    let transport_rx = this.inner.transport.stream();
    <T::Runtime as Runtime>::spawn_detach(async move {
      tracing::debug!(target = "showbiz.stream", "stream_listener start");
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            tracing::debug!(target = "showbiz.stream", "stream_listener shutting down");
            return;
          }
          conn = transport_rx.recv().fuse() => {
            match conn {
              Ok(conn) => {
                let this = this.clone();
                <T::Runtime as Runtime>::spawn_detach(this.handle_conn(conn))
              },
              Err(e) => {
                tracing::error!(target = "showbiz.stream", local = %this.inner.id, "failed to accept connection: {}", e);
                // If we got an error, which means on the other side the transport has been closed,
                // so we need to return and shutdown the stream listener
                return;
              },
            }
          }
        }
      }
    });
  }

  /// Used to merge the remote state with our local state
  pub(crate) async fn merge_remote_state<'a>(
    &'a self,
    node_state: &'a ArchivedPushPullMessage,
  ) -> Result<(), Error<T, D>> {
    self.verify_protocol(node_state.body.as_slice()).await?;

    // Invoke the merge delegate if any
    if node_state.header.join {
      if let Some(merge) = self.delegate.as_ref() {
        let peers = node_state
          .body
          .iter()
          .map(|n| {
            let mut deserializer = SharedDeserializeMap::new();
            let id = n.node.deserialize(&mut deserializer)?;
            let meta = n.meta.deserialize(&mut deserializer)?;
            Result::<_, SharedDeserializeMapError>::Ok(Arc::new(Node {
              id,
              meta,
              state: n.state.into(),
              protocol_version: n.protocol_version.into(),
              delegate_version: n.delegate_version.into(),
            }))
          })
          .collect::<Result<Vec<_>, _>>()
          .map_err(|e| Error::Transport(TransportError::Decode(DecodeError::Decode(e))))?;
        merge.notify_merge(peers).await.map_err(Error::delegate)?;
      }
    }

    // Merge the membership state
    self.merge_state(node_state.body.as_slice()).await;

    // Invoke the delegate for user state
    if let Some(d) = &self.delegate {
      if let Some(user_data) = node_state.user_data.as_ref() {
        d.merge_remote_state(user_data.as_slice(), node_state.header.join)
          .await
          .map_err(Error::delegate)?;
      }
    }
    Ok(())
  }

  pub(crate) async fn send_user_msg(
    &self,
    addr: &NodeId,
    msg: crate::types::Message,
  ) -> Result<(), Error<T, D>> {
    let mut conn = self
      .inner
      .transport
      .dial_timeout(addr.addr(), self.inner.opts.tcp_timeout)
      .await
      .map_err(Error::transport)?;

    self
      .raw_send_msg_stream(&mut conn, self.inner.opts.label.clone(), msg, addr.addr())
      .await
  }
}

// ----------------------------------------Module Level Methods------------------------------------
impl<D, T> Showbiz<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(super) async fn raw_send_msg_stream(
    &self,
    conn: &mut ReliableConnection<T>,
    label: Label,
    buf: Message,
    addr: SocketAddr,
  ) -> Result<(), Error<T, D>> {
    let compression_enabled = !self.inner.opts.compression_algo.is_none();
    let encryption_enabled = self.encryption_enabled() && self.inner.opts.gossip_verify_outgoing;
    // encrypt and compress are CPU-bound computation, so let rayon to handle it
    let buf = if compression_enabled || encryption_enabled {
      let primary_key = if encryption_enabled {
        Some(
          self
            .inner
            .opts
            .secret_keyring
            .as_ref()
            .unwrap()
            .primary_key(),
        )
      } else {
        None
      };

      let encryption_algo = self.inner.opts.encryption_algo;
      let compression_algo = self.inner.opts.compression_algo;

      // offload the encryption and compression to a thread pool
      if buf.len() > self.inner.opts.offload_size {
        #[cfg(feature = "metrics")]
        let metric_labels = self.inner.opts.metric_labels.clone();
        let keyring = self.inner.opts.secret_keyring.clone();
        let (tx, rx) = futures_channel::oneshot::channel();
        rayon::spawn(move || {
          let data = if compression_enabled {
            match Compress::encode_slice(compression_algo, 0, 0, buf.underlying_bytes()) {
              Ok(buf) => Either::Left(buf),
              Err(e) => {
                tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to compress payload");
                if tx.send(Err(e.into())).is_err() {
                  tracing::error!(
                    target = "showbiz.stream",
                    err = "failed to send compressed payload back to the onload thread"
                  );
                }
                return;
              }
            }
          } else {
            Either::Right(buf)
          };

          // Check if encryption is enabled
          if let Some(primary_key) = primary_key {
            let data = match &data {
              Either::Left(data) => data.as_slice(),
              Either::Right(data) => data.underlying_bytes(),
            };
            match Self::encrypt_local_state(
              primary_key,
              keyring.as_ref().unwrap(),
              data,
              &label,
              encryption_algo,
            ) {
              // Write out the entire send buffer
              Ok(crypt) => {
                #[cfg(feature = "metrics")]
                {
                  incr_tcp_sent_counter(crypt.len() as u64, metric_labels.iter());
                }

                if tx.send(Ok(Either::Left(crypt))).is_err() {
                  tracing::error!(
                    target = "showbiz.stream",
                    err = "failed to send encrypt payload back to the onload thread"
                  );
                }
              }
              Err(e) => {
                tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to encrypt local state");
                if tx.send(Err(e)).is_err() {
                  tracing::error!(
                    target = "showbiz.stream",
                    err = "failed to send encrypt error back to the onload thread"
                  );
                }
              }
            }
          } else if tx.send(Ok(data.map_left(Into::into))).is_err() {
            tracing::error!(
              target = "showbiz.stream",
              err = "failed to send compressed payload back to the onload thread"
            );
          }
        });

        match rx.await {
          Ok(Ok(buf)) => {
            // Write out the entire send buffer
            #[cfg(feature = "metrics")]
            {
              incr_tcp_sent_counter(buf.len() as u64, self.inner.opts.metric_labels.iter());
            }

            let data = match &buf {
              Either::Left(data) => data,
              Either::Right(data) => data.underlying_bytes(),
            };
            return conn.write_all(data).await.map_err(Error::transport);
          }
          Ok(Err(e)) => return Err(e),
          Err(_) => return Err(Error::OffloadPanic),
        }
      }

      let data = if compression_enabled {
        match Compress::encode_slice(compression_algo, 0, 0, buf.underlying_bytes()) {
          Ok(buf) => buf.into(),
          Err(e) => {
            tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to compress payload");
            return Err(e.into());
          }
        }
      } else {
        buf.freeze()
      };

      // Check if encryption is enabled
      if let Some(primary_key) = primary_key {
        match Self::encrypt_local_state(
          primary_key,
          self.inner.opts.secret_keyring.as_ref().unwrap(),
          &data,
          &label,
          encryption_algo,
        ) {
          // Write out the entire send buffer
          Ok(crypt) => crypt,
          Err(e) => {
            tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to encrypt local state");
            return Err(e);
          }
        }
      } else {
        data
      }
    } else {
      buf.freeze()
    };

    // Write out the entire send buffer
    #[cfg(feature = "metrics")]
    {
      incr_tcp_sent_counter(buf.len() as u64, self.inner.opts.metric_labels.iter());
    }
    conn.write_all(&buf).await.map_err(Error::transport)
  }

  /// Used to read the remote state from a connection
  pub(crate) async fn read_remote_state<'a>(
    &'a self,
    data: &'a [u8],
  ) -> Result<&'a ArchivedPushPullMessage, Error<T, D>> {
    PushPullMessage::decode_archived(data)
      .map(|(_, msg)| msg)
      .map_err(|e| TransportError::Decode(e).into())
  }

  pub(super) async fn send_local_state(
    &self,
    conn: &mut ReliableConnection<T>,
    addr: SocketAddr,
    join: bool,
    stream_label: Label,
  ) -> Result<(), Error<T, D>> {
    // Setup a deadline
    conn.set_timeout(Some(self.inner.opts.tcp_timeout));

    // Prepare the local node state
    #[cfg(feature = "metrics")]
    let mut node_state_counts = NodeState::empty_metrics();
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
          let this = PushNodeState {
            node: n.id().clone(),
            meta: n.node.meta().clone(),
            incarnation: n.incarnation.load(Ordering::Relaxed),
            state: n.state,
            protocol_version: n.node.protocol_version,
            delegate_version: n.node.delegate_version,
          };

          #[cfg(feature = "metrics")]
          {
            node_state_counts[this.state as u8 as usize].1 += 1;
          }
          this
        })
        .collect::<Vec<_>>()
    };

    // Get the delegate state
    let user_data = if let Some(delegate) = &self.delegate {
      delegate.local_state(join).await.map_err(Error::delegate)?
    } else {
      Bytes::new()
    };

    // Send our node state
    let header = PushPullHeader {
      nodes: local_nodes.len() as u32,
      user_state_len: user_data.len() as u32,
      join,
    };

    let msg = PushPullMessage::new(header, local_nodes, user_data);
    let buf = msg.encode(0, 0);
    #[cfg(feature = "metrics")]
    {
      std::thread_local! {
        #[cfg(not(target_family = "wasm"))]
        static NODE_INSTANCES_GAUGE: std::cell::OnceCell<std::cell::RefCell<Vec<metrics::Label>>> = std::cell::OnceCell::new();

        // TODO: remove this when cargo wasix toolchain update to rust 1.70
        #[cfg(target_family = "wasm")]
        static NODE_INSTANCES_GAUGE: once_cell::sync::OnceCell<std::cell::RefCell<Vec<metrics::Label>>> = once_cell::sync::OnceCell::new();
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
          set_node_instances_gauge(cnt as f64, iter);
        }
        labels.pop();
      });
    }

    #[cfg(feature = "metrics")]
    {
      set_local_size_gauge(buf.len() as f64, self.inner.opts.metric_labels.iter());
    }
    self
      .raw_send_msg_stream(conn, stream_label, buf, addr)
      .await
  }

  /// Used to read messages from a stream connection, decrypting and
  /// decompressing the stream if necessary.
  ///
  /// The provided streamLabel if present will be authenticated during decryption
  /// of each message.
  pub(super) async fn read_stream<'a>(
    conn: &mut ReliableConnection<T>,
    label: Label,
    encryption_enabled: bool,
    keyring: Option<SecretKeyring>,
    opts: &Options<T>,
    #[cfg(feature = "metrics")] metric_labels: &[metrics::Label],
  ) -> Result<(EncodeHeader, Bytes), Error<T, D>> {
    let mut h = conn.read_message_header().await.map_err(Error::transport)?;

    // Check if the message is encrypted
    let unencrypted = if h.meta.ty == MessageType::Encrypt {
      if !encryption_enabled {
        return Err(TransportError::Security(SecurityError::NotConfigured).into());
      }

      let encrypted_msg = conn
        .read_encrypt_message(
          h,
          #[cfg(feature = "metrics")]
          metric_labels,
        )
        .await?;

      let plain = if h.len as usize >= opts.offload_size {
        let (tx, rx) = futures_channel::oneshot::channel();
        rayon::spawn(move || {
          match Self::decrypt_remote_state(&label, encrypted_msg, keyring.as_ref().unwrap()) {
            Ok(plain) => {
              if tx.send(Ok(plain)).is_err() {
                tracing::error!(
                  target = "showbiz.stream",
                  err = "fail to send decrypted remote state, receiver end closed"
                );
              }
            }
            Err(e) => {
              if tx.send(Err(e)).is_err() {
                tracing::error!(
                  target = "showbiz.stream",
                  err = "fail to send decrypted remote state, receiver end closed"
                );
              }
            }
          };
        });
        match rx.await {
          Ok(plain) => plain?,
          Err(_) => return Err(Error::OffloadPanic),
        }
      } else {
        match Self::decrypt_remote_state(&label, encrypted_msg, keyring.as_ref().unwrap()) {
          Ok(plain) => plain,
          Err(e) => return Err(e),
        }
      };
      tracing::error!("debug: decrypted {:?}", plain.as_ref());
      // Reset message type and buf conn
      h.meta.ty = match MessageType::try_from(plain[0]) {
        Ok(mt) => mt,
        Err(e) => return Err(TransportError::Decode(DecodeError::from(e)).into()),
      };
      Some(plain)
    } else if encryption_enabled && opts.gossip_verify_incoming {
      return Err(TransportError::Security(SecurityError::PlainRemoteState).into());
    } else {
      None
    };

    if h.meta.ty == MessageType::Compress {
      if let Some(unencrypted) = unencrypted {
        let (_, compress) = Compress::decode_from_bytes(unencrypted)?;
        let uncompressed_data = if compress.algo.is_none() {
          unreachable!()
        } else if compress.buf.len() > opts.offload_size {
          let (tx, rx) = futures_channel::oneshot::channel();
          rayon::spawn(move || {
            match compress.decompress() {
              Ok(buf) => {
                if tx.send(Ok(buf)).is_err() {
                  tracing::error!(
                    target = "showbiz.stream",
                    err = "fail to send decompressed buffer, receiver end closed"
                  );
                }
              }
              Err(e) => {
                if tx.send(Err(Error::transport(e.into()))).is_err() {
                  tracing::error!(
                    target = "showbiz.stream",
                    err = "fail to send decompressed buffer, receiver end closed"
                  );
                }
              }
            };
          });
          match rx.await {
            Ok(buf) => buf?,
            Err(_) => return Err(Error::OffloadPanic),
          }
        } else {
          match compress.decompress() {
            Ok(buf) => buf,
            Err(e) => return Err(Error::transport(e.into())),
          }
        };

        // do not consume mt byte, let the callee to handle mt advance logic.
        return Ok(uncompressed_data);
      } else {
        let compress = conn
          .read_compressed_message(&h)
          .await
          .map_err(Error::transport)?;
        tracing::error!("compressed: {:?}", compress.buf.as_ref());
        return compress
          .decompress()
          .map_err(|e| Error::transport(TransportError::Decode(e)));
      }
    }

    if let Some(unencrypted) = unencrypted {
      return Ok((h, unencrypted));
    }

    let mut buf = vec![0; ENCODE_HEADER_SIZE + h.len as usize];
    buf[..ENCODE_HEADER_SIZE].copy_from_slice(&h.to_array());
    conn
      .read_exact(&mut buf[ENCODE_HEADER_SIZE..])
      .await
      .map(|_| (h, buf.into()))
      .map_err(Error::transport)
  }
}

// -----------------------------------------Private Level Methods-----------------------------------
impl<D, T> Showbiz<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  /// Handles a single incoming stream connection from the transport.
  async fn handle_conn(self, mut conn: ReliableConnection<T>) {
    let addr = conn.remote_node();
    tracing::debug!(target = "showbiz.stream", local = %self.inner.id, peer = %addr, "handle stream connection");

    #[cfg(feature = "metrics")]
    {
      incr_tcp_accept_counter(self.inner.opts.metric_labels.iter());
    }

    if self.inner.opts.tcp_timeout != Duration::ZERO {
      conn.set_timeout(Some(self.inner.opts.tcp_timeout));
    }

    let mut stream_label = match conn.remove_label_header().await {
      Ok(label) => label,
      Err(e) => {
        tracing::error!(target = "showbiz.stream", err = %e, remote_node = ?addr, "failed to remove label header");
        return;
      }
    };

    if self.inner.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!(target = "showbiz.stream", remote_node = ?addr, "unexpected double stream label header");
        return;
      }
      // Set this from config so that the auth data assertions work below
      stream_label = self.inner.opts.label.clone();
    }
    if self.inner.opts.label.ne(&stream_label) {
      tracing::error!(target = "showbiz.stream", remote_node = ?addr, "discarding stream with unacceptable label: {:?}", self.inner.opts.label.as_ref());
      return;
    }

    let encryption_enabled = self.encryption_enabled();
    let (h, data) = match Self::read_stream(
      &mut conn,
      stream_label.clone(),
      encryption_enabled,
      self.inner.opts.secret_keyring.clone(),
      &self.inner.opts,
      #[cfg(feature = "metrics")]
      &self.inner.opts.metric_labels,
    )
    .await
    {
      Ok(data) => data,
      Err(e) => match e {
        Error::Transport(TransportError::Connection(err)) => {
          if err.error.kind() != std::io::ErrorKind::UnexpectedEof {
            tracing::error!(target = "showbiz.stream", err=%err, local = %self.inner.id, remote_node = %addr, "failed to receive");
          }

          let err_resp = ErrorResponse::new(err.to_string());
          if let Err(e) = self
            .raw_send_msg_stream(&mut conn, stream_label, err_resp.encode(0, 0), addr)
            .await
          {
            tracing::error!(target = "showbiz.stream", err=%e, local = %self.inner.id, remote_node = %addr, "failed to send error response");
            return;
          }
          return;
        }
        e => {
          tracing::error!(target = "showbiz.stream", err=%e, local = %self.inner.id, remote_node = %addr, "failed to receive");
          return;
        }
      },
    };

    match h.meta.ty {
      MessageType::Ping => {
        let (_, ping) = {
          match Ping::decode_archived(&data) {
            Ok(ping) => ping,
            Err(e) => {
              tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to decode ping");
              return;
            }
          }
        };

        if ping.target != self.inner.id {
          tracing::error!(target = "showbiz.stream", local= %self.inner.id, remote = %addr, "got ping for unexpected node {}", ping.target);
          return;
        }

        let ack = AckResponse::empty(ping.seq_no);
        if let Err(e) = self
          .raw_send_msg_stream(&mut conn, stream_label, ack.encode(0, 0), addr)
          .await
        {
          tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to send ack response");
        }
      }
      MessageType::User => self.read_user_msg(data, addr).await,
      MessageType::PushPull => {
        // Increment counter of pending push/pulls
        let num_concurrent = self.inner.hot.push_pull_req.fetch_add(1, Ordering::SeqCst);
        scopeguard::defer! {
          self.inner.hot.push_pull_req.fetch_sub(1, Ordering::SeqCst);
        }

        // Check if we have too many open push/pull requests
        if num_concurrent >= MAX_PUSH_PULL_REQUESTS {
          tracing::error!(
            target = "showbiz.stream",
            "too many pending push/pull requests"
          );
          return;
        }
        let node_state = match self.read_remote_state(&data).await {
          Ok(ns) => ns,
          Err(e) => {
            tracing::error!(target = "showbiz.stream", err=%e, remote_node = ?addr, "failed to read remote state");
            return;
          }
        };
        if let Err(e) = self
          .send_local_state(&mut conn, addr, node_state.header.join, stream_label)
          .await
        {
          tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to push local state");
          return;
        }

        if let Err(e) = self.merge_remote_state(node_state).await {
          tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to push/pull merge");
        }
      }
      _ => {
        tracing::error!(target = "showbiz.stream", remote_node = %addr, "received invalid msg type {}", h.meta.ty);
      }
    }
  }

  async fn read_user_msg(&self, mut data: Bytes, addr: SocketAddr) {
    let user_msg_len = data.get_u32() as usize;
    let remaining = data.remaining();
    let user_msg = match user_msg_len.cmp(&remaining) {
      std::cmp::Ordering::Less => {
        tracing::error!(target = "showbiz.stream", remote_node = %addr, "failed to read full user message ({} / {})", remaining, user_msg_len);
        return;
      }
      std::cmp::Ordering::Equal => data,
      std::cmp::Ordering::Greater => data.slice(..user_msg_len),
    };

    if let Some(d) = &self.delegate {
      if let Err(e) = d.notify_user_msg(user_msg).await {
        tracing::error!(target = "showbiz.stream", err=%e, remote_node = %addr, "failed to notify user message");
      }
    }
  }
}
