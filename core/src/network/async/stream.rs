use prost::Message;

use crate::util::decode;

use super::*;

impl<T, D, ED, CD, MD, PD, AD> Showbiz<T, D, ED, CD, MD, PD, AD>
where
  T: Transport,
  D: Delegate,
  ED: EventDelegate,
  CD: ConflictDelegate,
  MD: MergeDelegate,
  PD: PingDelegate,
  AD: AliveDelegate,
{
  /// A long running thread that pulls incoming streams from the
  /// transport and hands them off for processing.
  pub(crate) fn steram_listen<R, S>(&self, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    let this = self.clone();
    (spawner)(Box::pin(async move {
      loop {
        futures_util::select! {
          _ = this.inner.shutdown_rx.recv().fuse() => {
            return;
          }
          conn = this.inner.transport.stream().recv().fuse() => {
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
    let addr = match <T::Connection as Connection>::remote_address(&conn) {
      Ok(addr) => {
        tracing::debug!(target = "showbiz", remote_addr = %addr, "stream connection");
        Some(addr)
      }
      Err(e) => {
        tracing::error!(target = "showbiz", err = %e, "fail to get connection remote address");
        tracing::debug!(
          target = "showbiz",
          remote_addr = "unknown",
          "stream connection"
        );
        None
      }
    };

    // TODO: metrics

    if self.inner.opts.tcp_timeout != Duration::ZERO {
      <T::Connection as Connection>::set_timeout(&mut conn, Some(self.inner.opts.tcp_timeout));
    }

    let mut lr = match remove_label_header_from_stream(conn).await {
      Ok(lr) => lr,
      Err(e) => {
        tracing::error!(target = "showbiz", err = %e, remote_addr = ?addr, "failed to remove label header");
        return;
      }
    };

    if self.inner.opts.skip_inbound_label_check {
      if !lr.label().is_empty() {
        tracing::error!(target = "showbiz", remote_addr = ?addr, "unexpected double stream label header");
        return;
      }
      // Set this from config so that the auth data assertions work below
      lr.set_label(self.inner.opts.label.clone());
    }

    if self.inner.opts.label.ne(lr.label()) {
      tracing::error!(target = "showbiz", remote_addr = ?addr, "discarding stream with unacceptable label: {:?}", self.inner.opts.label.as_ref());
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
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to receive");
          }

          let out = match ErrorResponse::from(e).encode_with_prefix() {
            Ok(out) => out,
            Err(e) => {
              tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to encode error response");
              return;
            }
          };

          if let Err(e) = self
            .raw_send_msg_stream(lr, out, addr.as_ref(), encryption_enabled)
            .await
          {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to send error response");
            return;
          }
          return;
        }
        e => {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to receive");
          return;
        }
      },
    };

    match mt {
      MessageType::Ping => {
        let buf = if let Some(data) = data {
          data
        } else {
          let mut buf = Vec::new();
          if let Err(e) = lr.read_to_end(&mut buf).await {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to read ping");
            return;
          }
          Bytes::from(buf)
        };

        let ping = match rmp_serde::decode::from_slice::<'_, Ping>(&buf) {
          Ok(ping) => ping,
          Err(e) => {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to decode ping");
            return;
          }
        };

        if !ping.node.is_empty() && ping.node != self.inner.opts.name {
          tracing::warn!(target = "showbiz", remote_addr = ?addr, "got ping for unexpected node");
          return;
        }

        let out = match AckResponse::new(ping.seq_no, Bytes::new()).encode_with_prefix() {
          Ok(out) => out,
          Err(e) => {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to encode ack response");
            return;
          }
        };

        if let Err(e) = self
          .raw_send_msg_stream(lr, out, addr.as_ref(), encryption_enabled)
          .await
        {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to send ack response");
          return;
        }
      }
      MessageType::User => self.read_user_msg(lr, data, addr).await,
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

        let node_state = match self.read_remote_state(&mut lr, data, addr).await {
          Ok(ns) => ns,
          Err(e) => {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to read remote state");
            return;
          }
        };

        if let Err(e) = self.send_local_state(lr, node_state.join).await {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to push local state");
          return;
        }

        if let Err(e) = self.merge_remote_state(node_state).await {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to push/pull merge");
          return;
        }
      }
      mt => {
        tracing::error!(target = "showbiz", remote_addr = ?addr, "received invalid msg type {}", mt);
      }
    }
  }

  /// Used to read the remote state from a connection
  async fn read_remote_state(
    &self,
    lr: &mut LabeledConnection<T::Connection>,
    mut data: Option<Bytes>,
    addr: Option<SocketAddr>,
  ) -> Result<RemoteNodeState, InnerError> {
    // Read the push/pull header
    let push_pull_header = match &mut data {
      Some(data) => {
        let size = data.get_u32() as usize;
        match decode::<PushPullHeader>(data) {
          Ok(header) => header,
          Err(e) => return InnerError::Decode(e),
        }
      }
      None => {
        let mut size_buf = [0u8; 4];
        lr.read_exact(&mut size_buf).await?;
        let size = u32::from_be_bytes(size_buf) as usize;
        let mut buf = vec![0; size];
        lr.read_exact(&mut buf).await?;
        match decode::<PushPullHeader>(&buf) {
          Ok(header) => header,
          Err(e) => return InnerError::Decode(e),
        }
      }
    };

    // Allocate space for the transfer
    let mut remote_nodes = Vec::<PushNodeState>::with_capacity(push_pull_header.nodes as usize);

    // Try to decode all the states
    for _ in 0..push_pull_header.nodes {
      // remote_nodes.push(PushNodeState::)
    }
    // Read the remote user state into a buffer

    // TODO:
    // Ok(())
    todo!()
  }

  async fn send_local_state(
    &self,
    lr: LabeledConnection<T::Connection>,
    join: bool,
  ) -> Result<(), InnerError> {
    todo!()
  }

  /// Used to merge the remote state with our local state
  async fn merge_remote_state(&self, node_state: NodeState) -> Result<(), InnerError> {
    todo!()
  }

  async fn read_user_msg(
    &self,
    mut lr: LabeledConnection<T::Connection>,
    data: Option<Bytes>,
    addr: Option<SocketAddr>,
  ) {
    match data {
      Some(mut data) => {
        let user_msg_len = data.get_u32() as usize;
        let remaining = data.remaining();
        let user_msg = match user_msg_len.cmp(&remaining) {
          std::cmp::Ordering::Less => {
            tracing::error!(target = "showbiz", remote_addr = ?addr, "failed to read full user message ({} / {})", remaining, user_msg_len);
            return;
          }
          std::cmp::Ordering::Equal => data,
          std::cmp::Ordering::Greater => data.slice(..user_msg_len),
        };

        if let Some(d) = &self.inner.delegates.delegate {
          d.notify_msg(user_msg).await;
        }
      }
      None => {
        let mut user_msg_len = [0u8; 4];
        if let Err(e) = lr.read_exact(&mut user_msg_len).await {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to receive user message");
          return;
        }
        let user_msg_len = u32::from_be_bytes(user_msg_len) as usize;
        if user_msg_len > 0 {
          let mut user_msg = vec![0; user_msg_len];
          if let Err(e) = lr.read_exact(&mut user_msg).await {
            tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to receive user message");
            return;
          }
          if let Some(d) = &self.inner.delegates.delegate {
            d.notify_msg(user_msg.into()).await;
          }
        }
      }
    }
  }

  async fn raw_send_msg_stream(
    &self,
    mut lr: LabeledConnection<T::Connection>,
    mut buf: Bytes,
    addr: Option<&SocketAddr>,
    encryption_enabled: bool,
  ) -> Result<(), InnerError> {
    // Check if compression is enabled
    if !self.inner.opts.compression_algo.is_none() {
      buf = match compress_payload(self.inner.opts.compression_algo, &buf) {
        Ok(buf) => buf.into(),
        Err(e) => {
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to compress payload");
          return Err(e.into());
        }
      }
    }

    // Check if encryption is enabled
    if encryption_enabled && self.inner.opts.gossip_verify_outgoing {
      match encrypt_local_state(
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
          tracing::error!(target = "showbiz", err=%e, remote_addr = ?addr, "failed to encrypt local state");
          return Err(e.into());
        }
      }
    } else {
      // Write out the entire send buffer
      //TODO: handle metrics
      lr.conn.write_all(&buf).await.map_err(From::from)
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

      let mut plain = match decrypt_remote_state(lr, keyring).await {
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

      let compress = match rmp_serde::from_slice::<'_, Compress>(&compressed) {
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
