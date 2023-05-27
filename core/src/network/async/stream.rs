use crate::types::{Node, NodeId};
use futures_util::io::BufReader;
use prost::Message;

use super::*;

impl<T, D> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  /// A long running thread that pulls incoming streams from the
  /// transport and hands them off for processing.
  pub(crate) fn steram_listen<R, S>(&self, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    let this = self.clone();
    let transport_rx = this.inner.transport.stream().clone();
    let shutdown_rx = this.inner.shutdown_rx.clone();
    (spawner)(Box::pin(async move {
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          conn = transport_rx.recv().fuse() => {
            let this = this.clone();
            (spawner)(async move {
              match conn {
                Ok(conn) => this.handle_conn(conn).await,
                Err(e) => tracing::error!(target = "showbiz", "failed to accept connection: {}", e),
              }
            }.boxed());
          }
        }
      }
    }));
  }

  /// Handles a single incoming stream connection from the transport.
  async fn handle_conn(self, mut conn: T::Connection) {
    let addr = <T::Connection as Connection>::remote_node(&conn).clone();
    tracing::debug!(target = "showbiz", remote_node = %addr, "stream connection");

    // TODO: metrics

    if self.inner.opts.tcp_timeout != Duration::ZERO {
      <T::Connection as Connection>::set_timeout(&mut conn, Some(self.inner.opts.tcp_timeout));
    }

    let mut lr = match Self::remove_label_header_from_stream(conn).await {
      Ok(lr) => lr,
      Err(e) => {
        tracing::error!(target = "showbiz", err = %e, remote_node = ?addr, "failed to remove label header");
        return;
      }
    };

    if self.inner.opts.skip_inbound_label_check {
      if !lr.label().is_empty() {
        tracing::error!(target = "showbiz", remote_node = ?addr, "unexpected double stream label header");
        return;
      }
      // Set this from config so that the auth data assertions work below
      lr.set_label(self.inner.opts.label.clone());
    }

    if self.inner.opts.label.ne(lr.label()) {
      tracing::error!(target = "showbiz", remote_node = ?addr, "discarding stream with unacceptable label: {:?}", self.inner.opts.label.as_ref());
      return;
    }

    let encryption_enabled = if let Some(keyring) = &self.inner.keyring {
      keyring.lock().await.is_empty()
    } else {
      false
    };

    let (data, mt) = match Self::read_stream(
      &mut lr,
      encryption_enabled,
      self.inner.keyring.as_ref(),
      &self.inner.opts,
    )
    .await
    {
      Ok((mt, lr)) => (mt, lr),
      Err(e) => match e {
        InnerError::IO(e) => {
          if e.kind() != std::io::ErrorKind::UnexpectedEof {
            tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to receive");
          }

          let out = match ErrorResponse::from(e).encode_with_prefix() {
            Ok(out) => out,
            Err(e) => {
              tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to encode error response");
              return;
            }
          };

          if let Err(e) = self
            .raw_send_msg_stream(lr, out, &addr, encryption_enabled)
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
        let mut buf = if let Some(data) = data {
          data
        } else {
          let mut buf = Vec::new();
          if let Err(e) = lr.read_to_end(&mut buf).await {
            tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to read ping");
            return;
          }
          Bytes::from(buf)
        };

        let ping = match Ping::decode(&mut buf) {
          Ok(ping) => ping,
          Err(e) => {
            tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to decode ping");
            return;
          }
        };

        if !ping.node.is_empty() && ping.node != self.inner.opts.name {
          tracing::warn!(target = "showbiz", remote_node = %addr, "got ping for unexpected node");
          return;
        }

        let out = match AckResponse::new(ping.seq_no, Bytes::new()).encode_with_prefix() {
          Ok(out) => out,
          Err(e) => {
            tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to encode ack response");
            return;
          }
        };

        if let Err(e) = self
          .raw_send_msg_stream(lr, out, &addr, encryption_enabled)
          .await
        {
          tracing::error!(target = "showbiz", err=%e, remote_node = %addr, "failed to send ack response");
          return;
        }
      }
      MessageType::User => self.read_user_msg(lr, data, &addr).await,
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

        let node_state = match self.read_remote_state(&mut lr, data, &addr).await {
          Ok(ns) => ns,
          Err(e) => {
            tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to read remote state");
            return;
          }
        };

        if let Err(e) = self
          .send_local_state(lr, &addr, encryption_enabled, node_state.join)
          .await
        {
          tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to push local state");
          return;
        }

        if let Err(e) = self.merge_remote_state(node_state).await {
          tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to push/pull merge");
          return;
        }
      }
      mt => {
        tracing::error!(target = "showbiz", remote_node = ?addr, "received invalid msg type {}", mt);
      }
    }
  }

  /// Used to read the remote state from a connection
  async fn read_remote_state(
    &self,
    lr: &mut LabeledConnection<T::Connection>,
    mut data: Option<Bytes>,
    addr: &NodeId,
  ) -> Result<RemoteNodeState, InnerError> {
    // Read the push/pull header
    let header = match &mut data {
      Some(data) => {
        let _size = data.get_u32() as usize;
        match PushPullHeader::decode(data) {
          Ok(header) => header,
          Err(e) => return Err(InnerError::Decode(e)),
        }
      }
      None => {
        let mut size_buf = [0u8; 4];
        lr.read_exact(&mut size_buf).await?;
        let size = u32::from_be_bytes(size_buf) as usize;
        let mut buf = vec![0; size];
        lr.read_exact(&mut buf).await?;
        match PushPullHeader::decode(buf.as_slice()) {
          Ok(header) => header,
          Err(e) => return Err(InnerError::Decode(e)),
        }
      }
    };

    // Allocate space for the transfer
    let mut remote_nodes = Vec::<PushNodeState>::with_capacity(header.nodes as usize);

    // Try to decode all the states
    for _ in 0..header.nodes {
      // read push node state size
      let size = match &mut data {
        Some(data) => data.get_u32() as usize,
        None => {
          let mut size_buf = [0u8; 4];
          lr.read_exact(&mut size_buf).await?;
          u32::from_be_bytes(size_buf) as usize
        }
      };
      let state = match &mut data {
        Some(data) => {
          if size > data.remaining() {
            return Err(InnerError::FailReadRemoteState(data.remaining(), size));
          }
          let mut buf = data.split_to(size);
          match PushNodeState::decode(&mut buf) {
            Ok(state) => state,
            Err(e) => return Err(InnerError::Decode(e)),
          }
        }
        None => {
          let mut buf = vec![0; size];
          if let Err(e) = lr.read_exact(&mut buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
              tracing::error!(target = "showbiz", remote_node = %addr, "failed to read full push node state");
            }
            return Err(InnerError::IO(e));
          }
          match PushNodeState::decode(buf.as_slice()) {
            Ok(state) => state,
            Err(e) => return Err(InnerError::Decode(e)),
          }
        }
      };

      remote_nodes.push(state);
    }

    // Read the remote user state into a buffer
    if header.user_state_len > 0 {
      let user_state = match &mut data {
        Some(data) => {
          if header.user_state_len as usize > data.remaining() {
            return Err(InnerError::FailReadUserState(
              data.remaining(),
              header.user_state_len as usize,
            ));
          }
          data.split_to(header.user_state_len as usize)
        }
        None => {
          let mut user_state = vec![0; header.user_state_len as usize];
          if let Err(e) = lr.read_exact(&mut user_state).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
              tracing::error!(target = "showbiz", remote_node = ?addr, "failed to read full user state");
            }
            return Err(InnerError::IO(e));
          }
          user_state.into()
        }
      };
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

  async fn send_local_state(
    &self,
    mut lr: LabeledConnection<T::Connection>,
    addr: &NodeId,
    encryption_enabled: bool,
    join: bool,
  ) -> Result<(), Error<T, D>> {
    // Setup a deadline
    lr.conn
      .get_mut()
      .set_timeout(Some(self.inner.opts.tcp_timeout));

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
            incarnation: n.incarnation,
            state: n.state,
            vsn: n.node.vsn(),
          };

          // we use length-prefix encoding, so we need to add the size of the length of encoded data
          states_encoded_size += this.encoded_len() + core::mem::size_of::<u32>();
          this
        })
        .collect::<Vec<_>>()
    };

    // TODO: metrics

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
    let header_size = header.encoded_len() + core::mem::size_of::<u32>();

    let mut buf = BytesMut::with_capacity(header_size + states_encoded_size + user_data.len());

    // begin state push
    buf.put_u32(header_size as u32);
    header.encode(&mut buf)?;

    for n in local_nodes {
      buf.put_u32(n.encoded_len() as u32);
      n.encode(&mut buf)?;
    }

    // Write the user state as well
    buf.put_slice(&user_data);

    // TODO: metrics

    self
      .raw_send_msg_stream(lr, buf.freeze(), addr, encryption_enabled)
      .await
  }

  /// Used to merge the remote state with our local state
  async fn merge_remote_state(&self, node_state: RemoteNodeState) -> Result<(), Error<T, D>> {
    self.verify_protocol(&node_state.push_states).await?;

    // Invoke the merge delegate if any
    if node_state.join {
      if let Some(merge) = self.inner.delegate.as_ref() {
        let peers = node_state
          .push_states
          .iter()
          .map(|n| Node {
            id: NodeId::from_addr(n.node.addr.clone()).set_name(n.node.name.clone()),
            meta: n.meta.clone(),
            state: n.state,
            pmin: n.pmin(),
            pmax: n.pmax(),
            pcur: n.pcur(),
            dmin: n.dmin(),
            dmax: n.dmax(),
            dcur: n.dcur(),
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

  async fn read_user_msg(
    &self,
    mut lr: LabeledConnection<T::Connection>,
    data: Option<Bytes>,
    addr: &NodeId,
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
        if let Err(e) = lr.read_exact(&mut user_msg_len).await {
          tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to receive user message");
          return;
        }
        let user_msg_len = u32::from_be_bytes(user_msg_len) as usize;
        if user_msg_len > 0 {
          let mut user_msg = vec![0; user_msg_len];
          if let Err(e) = lr.read_exact(&mut user_msg).await {
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

  pub(crate) async fn send_user_msg(
    &self,
    addr: &NodeId,
    msg: crate::types::Message,
  ) -> Result<(), Error<T, D>> {
    if addr.name().is_empty() && self.inner.opts.require_node_names {
      return Err(Error::MissingNodeName);
    }
    let conn = self
      .inner
      .transport
      .dial_address_timeout(addr, self.inner.opts.tcp_timeout)
      .await
      .map_err(Error::transport)?;

    let mut lr = LabeledConnection::new(BufReader::new(conn));
    lr.set_label(self.inner.opts.label.clone());
    self
      .raw_send_msg_stream(lr, msg.freeze(), addr, self.encryption_enabled().await)
      .await
  }

  async fn raw_send_msg_stream(
    &self,
    mut lr: LabeledConnection<T::Connection>,
    mut buf: Bytes,
    addr: &NodeId,
    encryption_enabled: bool,
  ) -> Result<(), Error<T, D>> {
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
        lr.label(),
        self.inner.opts.encryption_algo,
      )
      .await
      {
        // Write out the entire send buffer
        Ok(crypt) => lr.conn.write_all(&crypt).await.map_err(From::from),
        Err(e) => {
          tracing::error!(target = "showbiz", err=%e, remote_node = ?addr, "failed to encrypt local state");
          return Err(e);
        }
      }
    } else {
      // Write out the entire send buffer
      //TODO: handle metrics
      lr.conn
        .write_all(&buf)
        .await
        .map_err(|e| Error::transport(T::Error::from(e)))
    }
  }

  /// Used to read messages from a stream connection, decrypting and
  /// decompressing the stream if necessary.
  ///
  /// The provided streamLabel if present will be authenticated during decryption
  /// of each message.
  async fn read_stream<R: AsyncRead + Unpin>(
    lr: &mut LabeledConnection<R>,
    encryption_enabled: bool,
    keyring: Option<&SecretKeyring>,
    opts: &Options,
  ) -> Result<(Option<Bytes>, MessageType), InnerError> {
    // Read the message type
    let mut buf = [0u8; 1];
    let mut mt = match lr.read(&mut buf).await {
      Ok(n) => {
        if n == 0 {
          return Err(InnerError::IO(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "eof",
          )));
        }
        match MessageType::try_from(buf[0]) {
          Ok(mt) => mt,
          Err(e) => {
            return Err(InnerError::from(e));
          }
        }
      }
      Err(e) => {
        return Err(InnerError::IO(e));
      }
    };

    // Check if the message is encrypted
    let unencrypted = if mt == MessageType::Encrypt {
      if !encryption_enabled {
        return Err(InnerError::Other(
          "remote state is encrypted and encryption is not configured",
        ));
      }

      let Some(keyring) = keyring else {
        return Err(InnerError::Other(
          "remote state is encrypted and encryption is not configured",
        ));
      };

      let mut plain = match Self::decrypt_remote_state(lr, keyring).await {
        Ok(plain) => plain,
        Err(_e) => return Err(InnerError::Other("failed to decrypt remote state")),
      };

      // Reset message type and buf conn
      mt = match MessageType::try_from(plain[0]) {
        Ok(mt) => mt,
        Err(e) => return Err(InnerError::from(e)),
      };

      plain.advance(1);
      Some(plain)
    } else if encryption_enabled && opts.gossip_verify_incoming {
      return Err(InnerError::Other(
        "encryption is configured but remote state is not encrypted",
      ));
    } else {
      None
    };

    if mt == MessageType::Compress {
      let compressed = if let Some(mut unencrypted) = unencrypted {
        unencrypted.advance(core::mem::size_of::<u32>());
        unencrypted
      } else {
        let mut compressed_size = [0u8; core::mem::size_of::<u32>()];
        if let Err(e) = lr.read_exact(&mut compressed_size).await {
          return Err(e.into());
        }
        let compressed_size = u32::from_be_bytes(compressed_size) as usize;
        let mut buf = vec![0; compressed_size];
        if let Err(e) = lr.read_exact(&mut buf).await {
          return Err(e.into());
        }
        buf.into()
      };

      let compress = match Compress::decode(compressed) {
        Ok(compress) => compress,
        Err(e) => {
          return Err(e.into());
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

      // Reset the message type
      mt = match MessageType::try_from(uncompressed_data[0]) {
        Ok(mt) => mt,
        Err(e) => {
          return Err(InnerError::from(e));
        }
      };

      // Create a new bufConn
      uncompressed_data.advance(1);
      return Ok((Some(uncompressed_data), mt));
    }

    Ok((None, mt))
  }
}
