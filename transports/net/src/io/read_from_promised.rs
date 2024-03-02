use super::*;

#[cfg(feature = "compression")]
const MAX_INLINED_BYTES: usize = 64;

impl<I, A, S, W, R> NetTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  pub(crate) async fn read_from_promised_without_compression_and_encryption(
    &self,
    conn: Deadline<AsyncPeekable<impl AsyncRead + Send + Unpin>>,
  ) -> Result<(usize, Message<I, A::ResolvedAddress>), NetTransportError<A, W>> {
    match conn.deadline {
      Some(ddl) => R::timeout_at(ddl, W::decode_message_from_reader(conn.op))
        .await
        .map_err(|e| ConnectionError::promised_read(e.into()))?
        .map_err(|e| ConnectionError::promised_read(e).into()),
      None => W::decode_message_from_reader(conn.op)
        .await
        .map_err(|e| ConnectionError::promised_read(e).into()),
    }
  }

  #[cfg(feature = "compression")]
  pub(crate) async fn read_from_promised_with_compression_without_encryption(
    &self,
    mut conn: Deadline<AsyncPeekable<impl AsyncRead + Send + Unpin>>,
  ) -> Result<(usize, Message<I, A::ResolvedAddress>), NetTransportError<A, W>> {
    let mut tag = [0u8; 1];
    conn
      .peek_exact::<R>(&mut tag)
      .await
      .map_err(ConnectionError::promised_read)?;
    let tag = tag[0];
    if !COMPRESS_TAG.contains(&tag) {
      return self
        .read_from_promised_without_compression_and_encryption(conn)
        .await;
    }

    let mut readed = 0;
    let mut compress_header = [0u8; COMPRESS_HEADER];
    conn
      .read_exact::<R>(&mut compress_header)
      .await
      .map_err(ConnectionError::promised_read)?;
    readed += COMPRESS_HEADER;
    let compressor = Compressor::try_from(compress_header[0])?;
    let data_len = NetworkEndian::read_u32(&compress_header[1..]) as usize;
    if data_len <= self.opts.offload_size {
      if data_len <= MAX_INLINED_BYTES {
        let mut data = [0u8; MAX_INLINED_BYTES];
        conn
          .read_exact::<R>(&mut data[..data_len])
          .await
          .map_err(ConnectionError::promised_read)?;
        readed += data_len;
        let msg = Self::decompress(compressor, &data[..data_len])?;
        return Ok((readed, msg));
      }

      let mut data = vec![0u8; data_len];
      conn
        .read_exact::<R>(&mut data)
        .await
        .map_err(ConnectionError::promised_read)?;
      readed += data_len;
      let msg = Self::decompress(compressor, &data)?;
      return Ok((readed, msg));
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let mut data = vec![0u8; data_len];
    conn
      .read_exact::<R>(&mut data)
      .await
      .map_err(ConnectionError::promised_read)?;
    readed += data_len;
    rayon::spawn(move || {
      if tx.send(Self::decompress(compressor, &data)).is_err() {
        tracing::error!(
          target = "memberlist.net.promised",
          "failed to send computation task result back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(msg)) => Ok((readed, msg)),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(all(feature = "encryption", not(feature = "compression")))]
  pub(crate) async fn read_from_promised_with_encryption_without_compression(
    &self,
    mut conn: Deadline<AsyncPeekable<impl AsyncRead + Send + Unpin>>,
    stream_label: Label,
    from: &A::ResolvedAddress,
  ) -> Result<(usize, Message<I, A::ResolvedAddress>), NetTransportError<A, W>> {
    // read and check if the message is encrypted
    let mut tag = [0u8; 1];
    conn
      .peek_exact::<R>(&mut tag)
      .await
      .map_err(ConnectionError::promised_read)?;
    let tag = tag[0];
    if !ENCRYPT_TAG.contains(&tag) {
      return self
        .read_from_promised_without_compression_and_encryption(conn)
        .await;
    }

    // Read encrypted message overhead
    let mut encrypt_message_overhead = [0u8; ENCRYPT_HEADER];
    conn
      .read_exact::<R>(&mut encrypt_message_overhead)
      .await
      .map_err(ConnectionError::promised_read)?;
    let encryption_algo = EncryptionAlgo::try_from(encrypt_message_overhead[0])?;
    let encrypted_message_len = NetworkEndian::read_u32(&encrypt_message_overhead[1..]) as usize;

    if encrypted_message_len < encryption_algo.encrypted_length(0) {
      tracing::error!(target = "memberlist.net.promised", remote = %from, "received encrypted message with small payload");
      return Err(SecurityError::SmallPayload.into());
    }

    // Decrypt message
    let enp = match self.encryptor.as_ref() {
      Some(enp) => enp,
      None => {
        tracing::error!(target = "memberlist.net.promised", remote = %from, "received encrypted message, but encryption is disabled");
        return Err(SecurityError::Disabled.into());
      }
    };

    let mut readed = ENCRYPT_HEADER;

    // Read encrypted message
    let mut buf = BytesMut::with_capacity(encrypted_message_len);
    buf.resize(encrypted_message_len, 0);
    conn
      .read_exact::<R>(&mut buf)
      .await
      .map_err(ConnectionError::promised_read)?;
    readed += encrypted_message_len;

    // check if we should offload
    let keys = enp.keys().await;
    if encrypted_message_len <= self.opts.offload_size {
      let buf = Self::decrypt(enp, encryption_algo, keys, stream_label.as_bytes(), buf)?;
      let (_, msg) = W::decode_message(&buf).map_err(NetTransportError::Wire)?;
      return Ok((readed, msg));
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let enp = enp.clone();
    rayon::spawn(move || {
      if tx
        .send(
          Self::decrypt(&enp, encryption_algo, keys, stream_label.as_bytes(), buf)
            .and_then(|b| W::decode_message(&b).map_err(NetTransportError::Wire)),
        )
        .is_err()
      {
        tracing::error!(
          target = "memberlist.net.promised",
          "failed to send computation task result back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok((_, msg))) => Ok((readed, msg)),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  pub(crate) async fn read_from_promised_with_compression_and_encryption(
    &self,
    mut conn: Deadline<AsyncPeekable<impl AsyncRead + Send + Unpin>>,
    stream_label: Label,
    from: &A::ResolvedAddress,
  ) -> Result<(usize, Message<I, A::ResolvedAddress>), NetTransportError<A, W>> {
    // read and check if the message is encrypted
    let mut tag = [0u8; 1];
    conn
      .peek_exact::<R>(&mut tag)
      .await
      .map_err(ConnectionError::promised_read)?;
    let tag = tag[0];
    if !ENCRYPT_TAG.contains(&tag) {
      return self
        .read_from_promised_with_compression_without_encryption(conn)
        .await;
    }

    // Decrypt message
    let enp = match self.encryptor.as_ref() {
      Some(enp) => enp,
      None => {
        tracing::error!(target = "memberlist.net.promised", remote = %from, "received encrypted message, but encryption is disabled");
        return Err(SecurityError::Disabled.into());
      }
    };

    // Read encrypted message overhead
    let mut encrypt_message_overhead = [0u8; ENCRYPT_HEADER];
    conn
      .read_exact::<R>(&mut encrypt_message_overhead)
      .await
      .map_err(ConnectionError::promised_read)?;
    let encryption_algo = EncryptionAlgo::try_from(encrypt_message_overhead[0])?;
    let encrypted_message_len = NetworkEndian::read_u32(&encrypt_message_overhead[1..]) as usize;

    if encrypted_message_len < encryption_algo.encrypted_length(0) {
      tracing::error!(target = "memberlist.net.promised", remote = %from, "received encrypted message with small payload");
      return Err(SecurityError::SmallPayload.into());
    }

    let mut readed = ENCRYPT_HEADER;

    // Read encrypted message
    let mut buf = BytesMut::with_capacity(encrypted_message_len);
    buf.resize(encrypted_message_len, 0);
    conn
      .read_exact::<R>(&mut buf)
      .await
      .map_err(ConnectionError::promised_read)?;
    readed += encrypted_message_len;

    // check if we should offload
    let keys = enp.keys().await;
    if encrypted_message_len / 2 <= self.opts.offload_size {
      return Self::decrypt_and_decompress(
        enp,
        encryption_algo,
        keys,
        stream_label.as_bytes(),
        buf,
      )
      .map(|msg| (readed, msg));
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let enp = enp.clone();
    rayon::spawn(move || {
      if tx
        .send(Self::decrypt_and_decompress(
          &enp,
          encryption_algo,
          keys,
          stream_label.as_bytes(),
          buf,
        ))
        .is_err()
      {
        tracing::error!(
          target = "memberlist.net.promised",
          "failed to send computation task result back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(msg)) => Ok((readed, msg)),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(feature = "compression")]
  fn decompress(
    compressor: Compressor,
    data: &[u8],
  ) -> Result<Message<I, A::ResolvedAddress>, NetTransportError<A, W>> {
    let uncompressed = compressor.decompress(data)?;

    W::decode_message(&uncompressed)
      .map(|(_, msg)| msg)
      .map_err(NetTransportError::Wire)
  }

  #[cfg(feature = "encryption")]
  fn decrypt(
    encryptor: &SecretKeyring,
    algo: EncryptionAlgo,
    keys: impl Iterator<Item = SecretKey>,
    auth_data: &[u8],
    mut data: BytesMut,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let nonce = encryptor.read_nonce(&mut data);
    for key in keys {
      match encryptor.decrypt(key, algo, nonce, auth_data, &mut data) {
        Ok(_) => return Ok(data),
        Err(e) => {
          tracing::error!(
            target = "memberlist.net.promised",
            "failed to decrypt message: {}",
            e
          );
          continue;
        }
      }
    }
    Err(NetTransportError::Security(SecurityError::NoInstalledKeys))
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  fn decrypt_and_decompress(
    encryptor: &SecretKeyring,
    algo: EncryptionAlgo,
    keys: impl Iterator<Item = SecretKey>,
    auth_data: &[u8],
    data: BytesMut,
  ) -> Result<Message<I, A::ResolvedAddress>, NetTransportError<A, W>> {
    use bytes::Buf;

    let mut buf = Self::decrypt(encryptor, algo, keys, auth_data, data)?;
    let tag = buf[0];
    if !COMPRESS_TAG.contains(&tag) {
      let (_, msg) = W::decode_message(&buf).map_err(NetTransportError::Wire)?;
      return Ok(msg);
    }
    let compressed_message_size = NetworkEndian::read_u32(&buf[1..COMPRESS_HEADER]) as usize;
    buf.advance(COMPRESS_HEADER);
    Self::decompress(Compressor::try_from(tag)?, &buf[..compressed_message_size])
  }
}
