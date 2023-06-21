// use agnostic::lock::RwLock;
use futures_util::{Future, Stream};

use crate::{
  transport::{ConnectionError, ConnectionErrorKind, ConnectionKind, ReliableConnection},
  types::{Node, NodeId},
};

use super::*;

// --------------------------------------------Crate Level Methods-------------------------------------------------
impl<D, T, R> Showbiz<D, T, R>
where
  D: Delegate,
  T: Transport,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  /// A long running thread that pulls incoming streams from the
  /// transport and hands them off for processing.
  pub(crate) fn stream_listener(&self, shutdown_rx: async_channel::Receiver<()>) {
    let this = self.clone();
    let transport_rx = this.runner().as_ref().unwrap().transport.stream();
    R::spawn_detach(async move {
      tracing::debug!(target = "showbiz", "stream_listener start");
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            tracing::debug!(target = "showbiz", "stream_listener shutting down");
            return;
          }
          conn = transport_rx.recv().fuse() => {
            match conn {
              Ok(conn) => {
                let this = this.clone();
                R::spawn_detach(this.handle_conn(conn))
              },
              Err(e) => {
                tracing::error!(target = "showbiz", "failed to accept connection: {}", e);
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
  pub(crate) async fn merge_remote_state(
    &self,
    node_state: RemoteNodeState,
  ) -> Result<(), Error<D, T>> {
    self.verify_protocol(&node_state.push_states).await?;

    // Invoke the merge delegate if any
    if node_state.join {
      if let Some(merge) = self.inner.delegate.as_ref() {
        let peers = node_state
          .push_states
          .iter()
          .map(|n| Node {
            id: NodeId::from_addr(n.node.addr()).set_name(n.node.name.clone()),
            meta: n.meta.clone(),
            state: n.state,
            vsn: n.vsn,
          })
          .collect::<Vec<_>>();
        merge.notify_merge(peers).await.map_err(Error::delegate)?;
      }
    }

    // Merge the membership state
    self.merge_state(node_state.push_states).await?;

    // Invoke the delegate for user state
    if let Some(d) = &self.inner.delegate {
      if !node_state.user_state.is_empty() {
        d.merge_remote_state(node_state.user_state, node_state.join)
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
  ) -> Result<(), Error<D, T>> {
    if addr.name().is_empty() && self.inner.opts.require_node_names {
      return Err(Error::MissingNodeName);
    }
    let mut conn = self
      .runner()
      .as_ref()
      .unwrap()
      .transport
      .dial_timeout(addr.addr(), self.inner.opts.tcp_timeout)
      .await
      .map_err(Error::transport)?;

    self
      .raw_send_msg_stream(
        &mut conn,
        &self.inner.opts.label,
        msg.freeze(),
        addr.addr(),
        self.encryption_enabled().await,
      )
      .await
  }
}

// ----------------------------------------Module Level Methods------------------------------------
impl<D, T, R> Showbiz<D, T, R>
where
  D: Delegate,
  T: Transport,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  pub(super) async fn raw_send_msg_stream(
    &self,
    conn: &mut ReliableConnection<T>,
    label: &Label,
    mut buf: Bytes,
    addr: SocketAddr,
    encryption_enabled: bool,
  ) -> Result<(), Error<D, T>> {
    // Check if compression is enabled
    if !self.inner.opts.compression_algo.is_none() {
      buf = match compress_payload(self.inner.opts.compression_algo, &buf) {
        Ok(buf) => buf.into(),
        Err(e) => {
          tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to compress payload");
          return Err(e.into());
        }
      }
    }

    // Check if encryption is enabled
    if encryption_enabled && self.inner.opts.gossip_verify_outgoing {
      match Self::encrypt_local_state(
        self.inner.keyring.as_ref().unwrap(),
        &buf,
        label,
        self.inner.opts.encryption_algo,
      )
      .await
      {
        // Write out the entire send buffer
        Ok(crypt) => {
          #[cfg(feature = "metrics")]
          {
            incr_tcp_sent_counter(crypt.len() as u64, self.inner.metrics_labels.iter());
          }

          conn.write_all(&crypt).await.map_err(Error::transport)
        }
        Err(e) => {
          tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to encrypt local state");
          Err(e)
        }
      }
    } else {
      // Write out the entire send buffer
      #[cfg(feature = "metrics")]
      {
        incr_tcp_sent_counter(buf.len() as u64, self.inner.metrics_labels.iter());
      }

      conn.write_all(&buf).await.map_err(Error::transport)
    }
  }

  /// Used to read the remote state from a connection
  pub(super) async fn read_remote_state(
    &self,
    conn: &mut ReliableConnection<T>,
    data: Option<Bytes>,
  ) -> Result<RemoteNodeState, Error<D, T>> {
    // Read the push/pull header
    let (header, mut data) = match data {
      Some(mut data) => {
        // consume total msg len
        let _ = decode_u32_from_buf(&mut data)
          .map_err(|e| TransportError::Decode(DecodeError::from(e)))?;
        let len = match PushPullHeader::decode_len(&mut data) {
          Ok(len) => len,
          Err(e) => return Err(TransportError::Decode(e).into()),
        };
        let h = match PushPullHeader::decode_from(data.split_to(len as usize)) {
          Ok(header) => header,
          Err(e) => return Err(TransportError::Decode(e).into()),
        };

        (h, data)
      }
      None => {
        let total_len = conn.read_u32_varint().await.map_err(Error::transport)?;
        let mut buf = vec![0; total_len];
        conn.read_exact(&mut buf).await.map_err(Error::transport)?;
        let mut buf: Bytes = buf.into();
        let len = PushPullHeader::decode_len(&mut buf).map_err(TransportError::Decode)? as usize;

        let h = match PushPullHeader::decode_from(buf.split_to(len)) {
          Ok(header) => header,
          Err(e) => return Err(TransportError::Decode(e).into()),
        };
        (h, buf)
      }
    };

    // Allocate space for the transfer
    let mut remote_nodes = Vec::<PushNodeState>::with_capacity(header.nodes as usize);

    // Try to decode all the states
    for _ in 0..header.nodes {
      let len = PushNodeState::decode_len(&mut data).map_err(TransportError::Decode)?;
      if len > data.remaining() {
        return Err(
          TransportError::Decode(DecodeError::FailReadRemoteState(data.remaining(), len)).into(),
        );
      }

      match PushNodeState::decode_from(data.split_to(len)) {
        Ok(state) => remote_nodes.push(state),
        Err(e) => return Err(TransportError::Decode(e).into()),
      }
    }

    // Read the remote user state into a buffer
    if header.user_state_len > 0 {
      if header.user_state_len as usize > data.remaining() {
        return Err(
          TransportError::Decode(DecodeError::FailReadUserState(
            data.remaining(),
            header.user_state_len as usize,
          ))
          .into(),
        );
      }
      let user_state = data.split_to(header.user_state_len as usize);
      return Ok(RemoteNodeState {
        join: header.join,
        push_states: remote_nodes,
        user_state,
      });
    }

    Ok(RemoteNodeState {
      join: header.join,
      push_states: remote_nodes,
      user_state: Bytes::new(),
    })
  }

  pub(super) async fn send_local_state(
    &self,
    conn: &mut ReliableConnection<T>,
    addr: SocketAddr,
    encryption_enabled: bool,
    join: bool,
    stream_label: &Label,
  ) -> Result<(), Error<D, T>> {
    // Setup a deadline
    conn.set_timeout(Some(self.inner.opts.tcp_timeout));

    // Prepare the local node state
    let mut states_encoded_size = 0;
    let local_nodes = {
      self
        .inner
        .nodes
        .read()
        .await
        .nodes
        .iter()
        .map(|n| {
          let this = PushNodeState {
            node: n.id().clone(),
            meta: n.node.meta().clone(),
            incarnation: n.incarnation.load(Ordering::Relaxed),
            state: n.state,
            vsn: n.node.vsn(),
          };
          states_encoded_size += this.encoded_len();
          this
        })
        .collect::<Vec<_>>()
    };

    // Get the delegate state
    let user_data = if let Some(delegate) = &self.inner.delegate {
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
    let header_size = header.encoded_len();

    let basic_len = header_size
      + states_encoded_size
      + if user_data.is_empty() {
        0
      } else {
        user_data.len()
      };
    let total_len = basic_len + encoded_u32_len(basic_len as u32);
    let mut buf = BytesMut::with_capacity(total_len);
    // begin state push
    header.encode_to(&mut buf);

    #[cfg(feature = "metrics")]
    let mut node_state_counts = NodeState::empty_metrics();

    for n in local_nodes {
      n.encode_to(&mut buf);

      #[cfg(feature = "metrics")]
      {
        node_state_counts[n.state as u8 as usize].1 += 1;
      }
    }

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
            let mut labels = (*self.inner.metrics_labels).clone();
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

    // Write the user state as well
    if !user_data.is_empty() {
      buf.put_slice(&user_data);
    }

    let buf = buf.freeze();
    #[cfg(feature = "metrics")]
    {
      set_local_size_gauge(basic_len as f64, self.inner.metrics_labels.iter());
    }

    self
      .raw_send_msg_stream(conn, stream_label, buf, addr, encryption_enabled)
      .await
  }

  /// Used to read messages from a stream connection, decrypting and
  /// decompressing the stream if necessary.
  ///
  /// The provided streamLabel if present will be authenticated during decryption
  /// of each message.
  pub(super) async fn read_stream(
    conn: &mut ReliableConnection<T>,
    label: &Label,
    encryption_enabled: bool,
    keyring: Option<&SecretKeyring>,
    opts: &Options<T>,
    #[cfg(feature = "metrics")] metrics_labels: &[metrics::Label],
  ) -> Result<(Option<Bytes>, MessageType), Error<D, T>> {
    // Read the message type
    let mut buf = [0u8; 1];
    let mut mt = match conn.read(&mut buf).await {
      Ok(n) => {
        if n == 0 {
          return Err(
            TransportError::Connection(ConnectionError {
              kind: ConnectionKind::Reliable,
              error_kind: ConnectionErrorKind::Read,
              error: std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "eof"),
            })
            .into(),
          );
        }
        match MessageType::try_from(buf[0]) {
          Ok(mt) => mt,
          Err(e) => {
            return Err(Error::transport(TransportError::Decode(DecodeError::from(
              e,
            ))));
          }
        }
      }
      Err(e) => return Err(Error::transport(e)),
    };

    // Check if the message is encrypted
    let unencrypted = if mt == MessageType::Encrypt {
      if !encryption_enabled {
        return Err(SecurityError::NotConfigured.into());
      }

      let Some(keyring) = keyring else {
        return Err(SecurityError::NotConfigured.into());
      };

      let mut plain = match Self::decrypt_remote_state(
        conn,
        label,
        keyring,
        #[cfg(feature = "metrics")]
        metrics_labels,
      )
      .await
      {
        Ok(plain) => plain,
        Err(_e) => return Err(NetworkError::Decrypt.into()),
      };

      // Reset message type and buf conn
      mt = match MessageType::try_from(plain[0]) {
        Ok(mt) => mt,
        Err(e) => return Err(TransportError::Decode(DecodeError::from(e)).into()),
      };

      plain.advance(1);
      Some(plain)
    } else if encryption_enabled && opts.gossip_verify_incoming {
      return Err(SecurityError::NotConfigured.into());
    } else {
      None
    };

    if mt == MessageType::Compress {
      let compressed = if let Some(mut unencrypted) = unencrypted {
        let size = Compress::decode_len(&mut unencrypted)
          .map_err(|e| Error::transport(TransportError::Decode(e)))?;
        unencrypted.split_to(size as usize)
      } else {
        let compressed_size = conn.read_u32_varint().await.map_err(Error::transport)?;
        let mut buf = vec![0; compressed_size];
        if let Err(e) = conn.read_exact(&mut buf).await {
          return Err(Error::transport(e));
        }
        buf.into()
      };

      let compress = match Compress::decode_from::<T::Checksumer>(compressed) {
        Ok(compress) => compress,
        Err(e) => {
          return Err(Error::transport(TransportError::Decode(e)));
        }
      };
      let mut uncompressed_data = if compress.algo.is_none() {
        compress.buf
      } else {
        match decompress_buffer(compress.algo, &compress.buf) {
          Ok(buf) => buf.into(),
          Err(e) => {
            return Err(e.into());
          }
        }
      };

      if !uncompressed_data.has_remaining() {
        return Err(Error::transport(TransportError::Decode(
          DecodeError::Truncated(MessageType::Compress.as_err_str()),
        )));
      }

      // Reset the message type
      mt = match MessageType::try_from(uncompressed_data.get_u8()) {
        Ok(mt) => mt,
        Err(e) => {
          return Err(Error::transport(TransportError::Decode(DecodeError::from(
            e,
          ))));
        }
      };

      // Create a new bufConn
      return Ok((Some(uncompressed_data), mt));
    }

    Ok((None, mt))
  }
}

// -----------------------------------------Private Level Methods-----------------------------------
impl<D, T, R> Showbiz<D, T, R>
where
  D: Delegate,
  T: Transport,
  R: Runtime,
  <R::Interval as Stream>::Item: Send,
  <R::Sleep as Future>::Output: Send,
{
  /// Handles a single incoming stream connection from the transport.
  async fn handle_conn(self, mut conn: ReliableConnection<T>) {
    let addr = conn.remote_node();
    eprintln!("{}", addr);
    tracing::debug!(target = "showbiz", remote_node = %addr, "stream connection");

    #[cfg(feature = "metrics")]
    {
      incr_tcp_accept_counter(self.inner.metrics_labels.iter());
    }

    if self.inner.opts.tcp_timeout != Duration::ZERO {
      conn.set_timeout(Some(self.inner.opts.tcp_timeout));
    }

    let mut stream_label = match conn.remove_label_header().await {
      Ok(label) => label,
      Err(e) => {
        tracing::error!(target = "showbiz", err = %e, remote_node = ?addr, "failed to remove label header");
        return;
      }
    };

    if self.inner.opts.skip_inbound_label_check {
      if !stream_label.is_empty() {
        tracing::error!(target = "showbiz", remote_node = ?addr, "unexpected double stream label header");
        return;
      }
      // Set this from config so that the auth data assertions work below
      stream_label = self.inner.opts.label.clone();
    }

    if self.inner.opts.label.ne(&stream_label) {
      tracing::error!(target = "showbiz", remote_node = ?addr, "discarding stream with unacceptable label: {:?}", self.inner.opts.label.as_ref());
      return;
    }

    let encryption_enabled = if let Some(keyring) = &self.inner.keyring {
      keyring.lock().await.is_empty()
    } else {
      false
    };

    let (data, mt) = match Self::read_stream(
      &mut conn,
      &stream_label,
      encryption_enabled,
      self.inner.keyring.as_ref(),
      &self.inner.opts,
      #[cfg(feature = "metrics")]
      &self.inner.metrics_labels,
    )
    .await
    {
      Ok((mt, lr)) => (mt, lr),
      Err(e) => match e {
        Error::Transport(TransportError::Connection(err)) => {
          if err.error.kind() != std::io::ErrorKind::UnexpectedEof {
            tracing::error!(target = "showbiz", err=%err, remote_node = ?addr, "failed to receive");
          }

          let err_resp = ErrorResponse::new(err);
          let mut out = BytesMut::with_capacity(MessageType::SIZE + err_resp.encoded_len());
          out.put_u8(MessageType::ErrorResponse as u8);
          err_resp.encode_to(&mut out);

          if let Err(e) = self
            .raw_send_msg_stream(
              &mut conn,
              &stream_label,
              out.freeze(),
              addr,
              encryption_enabled,
            )
            .await
          {
            tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to send error response");
            return;
          }
          return;
        }
        e => {
          tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to receive");
          return;
        }
      },
    };

    match mt {
      MessageType::Ping => {
        let ping = if let Some(mut data) = data {
          match Ping::decode_len(&mut data) {
            Ok(len) => {
              if len > data.len() {
                tracing::error!(target = "showbiz", remote_node = %addr, "failed to decode ping");
                return;
              }

              match Ping::decode_from(data.split_to(len)) {
                Ok(ping) => ping,
                Err(e) => {
                  tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to decode ping");
                  return;
                }
              }
            }
            Err(e) => {
              tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to decode ping");
              return;
            }
          }
        } else {
          match conn.read_u32_varint().await {
            Ok(len) => {
              let mut buf = vec![0; len];
              match conn.read_exact(&mut buf).await {
                Ok(_) => match Ping::decode_from(buf.into()) {
                  Ok(ping) => ping,
                  Err(e) => {
                    tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to decode ping");
                    return;
                  }
                },
                Err(e) => {
                  tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to decode ping");
                  return;
                }
              }
            }
            Err(e) => {
              tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to decode ping");
              return;
            }
          }
        };

        if ping.target.name != self.inner.opts.name {
          tracing::error!(target = "showbiz", local= %self.inner.id, remote = %addr, "got ping for unexpected node {}", ping.target);
          return;
        }

        let ack = AckResponse::empty(ping.seq_no);
        let mut out = BytesMut::with_capacity(MessageType::SIZE + ack.encoded_len());
        out.put_u8(MessageType::AckResponse as u8);
        ack.encode_to::<T::Checksumer>(&mut out);

        if let Err(e) = self
          .raw_send_msg_stream(
            &mut conn,
            &stream_label,
            out.freeze(),
            addr,
            encryption_enabled,
          )
          .await
        {
          tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to send ack response");
        }
      }
      MessageType::User => self.read_user_msg(conn, data, addr).await,
      MessageType::PushPull => {
        // Increment counter of pending push/pulls
        let num_concurrent = self.inner.hot.push_pull_req.fetch_add(1, Ordering::SeqCst);
        scopeguard::defer! {
          self.inner.hot.push_pull_req.fetch_sub(1, Ordering::SeqCst);
        }

        // Check if we have too many open push/pull requests
        if num_concurrent >= MAX_PUSH_PULL_REQUESTS {
          tracing::error!(target = "showbiz", "too many pending push/pull requests");
          return;
        }

        let node_state = match self.read_remote_state(&mut conn, data).await {
          Ok(ns) => ns,
          Err(e) => {
            tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to read remote state");
            return;
          }
        };

        if let Err(e) = self
          .send_local_state(
            &mut conn,
            addr,
            encryption_enabled,
            node_state.join,
            &stream_label,
          )
          .await
        {
          tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to push local state");
          return;
        }

        if let Err(e) = self.merge_remote_state(node_state).await {
          tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to push/pull merge");
        }
      }
      mt => {
        tracing::error!(target = "showbiz", remote_node = %addr, "received invalid msg type {}", mt);
      }
    }
  }

  async fn read_user_msg(
    &self,
    mut conn: ReliableConnection<T>,
    data: Option<Bytes>,
    addr: SocketAddr,
  ) {
    match data {
      Some(mut data) => {
        let user_msg_len = data.get_u32() as usize;
        let remaining = data.remaining();
        let user_msg = match user_msg_len.cmp(&remaining) {
          std::cmp::Ordering::Less => {
            tracing::error!(target = "showbiz", remote_node = %addr, "failed to read full user message ({} / {})", remaining, user_msg_len);
            return;
          }
          std::cmp::Ordering::Equal => data,
          std::cmp::Ordering::Greater => data.slice(..user_msg_len),
        };

        if let Some(d) = &self.inner.delegate {
          if let Err(e) = d.notify_user_msg(user_msg).await {
            tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to notify user message");
          }
        }
      }
      None => {
        let mut user_msg_len = [0u8; 4];
        if let Err(e) = conn.read_exact(&mut user_msg_len).await {
          tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to receive user message");
          return;
        }
        let user_msg_len = u32::from_be_bytes(user_msg_len) as usize;
        if user_msg_len > 0 {
          let mut user_msg = vec![0; user_msg_len];
          if let Err(e) = conn.read_exact(&mut user_msg).await {
            tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to receive user message");
            return;
          }
          if let Some(d) = &self.inner.delegate {
            if let Err(e) = d.notify_user_msg(user_msg.into()).await {
              tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to notify user message");
            }
          }
        }
      }
    }
  }
}
