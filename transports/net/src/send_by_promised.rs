use super::*;

impl<I, A, S, W> NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire,
{
  fn enable_promised_encryption(&self) -> bool {
    self.encryptor.is_some() && self.opts.gossip_verify_outgoing && !S::is_secure()
  }

  pub(super) async fn send_by_promised(
    &self,
    conn: &mut S::Stream,
    target: &A::ResolvedAddress,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    return self
      .send_by_promised_without_compression_and_encryption(conn, target, msg)
      .await;

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    return self
      .send_by_promised_with_compression_without_encryption(conn, target, msg)
      .await;

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    return self
      .send_by_promised_with_encryption_without_compression(conn, target, msg, &self.opts.label)
      .await;

    let compression_enabled = self.opts.compressor.is_some();
    let encryption_enabled = self.enable_promised_encryption();

    if !compression_enabled && !encryption_enabled {
      return self
        .send_by_promised_without_compression_and_encryption(conn, target, msg)
        .await;
    }

    if compression_enabled && !encryption_enabled {
      return self
        .send_by_promised_with_compression_without_encryption(conn, target, msg)
        .await;
    }

    if !compression_enabled && encryption_enabled {
      return self
        .send_by_promised_with_encryption_without_compression(conn, target, msg, &self.opts.label)
        .await;
    }

    let encoded_size = W::encoded_len(&msg);
    let encryptor = self.encryptor.as_ref().unwrap();
    let compressor = self.opts.compressor.unwrap();

    let buf = if encoded_size < self.opts.offload_size {
      let pk = encryptor.kr.primary_key().await;
      Self::compress_and_encrypt(
        &compressor,
        encryptor,
        pk,
        &self.opts.label,
        msg,
        encoded_size,
      )?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let encryptor = encryptor.clone();
      let pk = encryptor.kr.primary_key().await;
      let stream_label = self.opts.label.cheap_clone();

      rayon::spawn(move || {
        if tx
          .send(Self::compress_and_encrypt(
            &compressor,
            &encryptor,
            pk,
            &stream_label,
            msg,
            encoded_size,
          ))
          .is_err()
        {
          tracing::error!(target: "showbiz.net.promised", "failed to send computation task result back to main thread");
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    let total_len = buf.len();
    conn
      .write_all(&buf)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;

    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_len)
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  fn compress_and_encrypt(
    compressor: &Compressor,
    encryptor: &Encryptor,
    pk: SecretKey,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let encrypt_header = encryptor.encrypt_overhead();
    let total_len = encrypt_header + COMPRESS_HEADER + encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    // write encryption algo
    buf.put_u8(encryptor.algo as u8);
    // write length placeholder
    buf.put_slice(&[0; MAX_MESSAGE_LEN_SIZE]);

    let nonce = encryptor.write_header(&mut buf);
    buf.resize(total_len, 0);

    W::encode_message(msg, &mut buf[encrypt_header..]).map_err(NetTransportError::Wire)?;

    let compressed = compressor
      .compress_into_bytes(&buf[encrypt_header..])
      .map_err(NetTransportError::Compress)?;
    let compressed_size = compressed.len();
    buf.truncate(encrypt_header);
    let compress_offset = buf.len();
    buf.put_u8(*compressor as u8);
    let mut compress_size_buf = [0; MAX_MESSAGE_LEN_SIZE];
    NetworkEndian::write_u32(&mut compress_size_buf, compressed_size as u32);
    buf.put_slice(&compress_size_buf);
    buf.put_slice(&compressed);
    let total_compressed_size = buf.len() - compress_offset;

    // update actual data size
    NetworkEndian::write_u32(&mut buf[1..ENCRYPT_HEADER], total_compressed_size as u32);

    let mut dst = buf.split_off(encrypt_header);
    encryptor
      .encrypt(pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        buf
      })
      .map_err(NetTransportError::Security)
  }

  #[cfg(feature = "encryption")]
  fn encrypt_message(
    encryptor: &Encryptor,
    pk: SecretKey,
    label: &Label,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let encrypt_header = encryptor.encrypt_overhead();
    let total_len = encrypt_header + encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    // write encryption algo
    buf.put_u8(encryptor.algo as u8);
    // write length placeholder
    buf.put_slice(&[0; MAX_MESSAGE_LEN_SIZE]);

    let nonce = encryptor.write_header(&mut buf);
    buf.resize(total_len, 0);

    let written =
      W::encode_message(msg, &mut buf[encrypt_header..]).map_err(NetTransportError::Wire)?;
    // write actual data size
    NetworkEndian::write_u32(&mut buf[1..ENCRYPT_HEADER], written as u32);
    let mut dst = buf.split_off(encrypt_header);
    encryptor
      .encrypt(pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        buf
      })
      .map_err(NetTransportError::Security)
  }

  #[cfg(feature = "compression")]
  fn compress_message(
    compressor: Compressor,
    msg: Message<I, A::ResolvedAddress>,
    encoded_size: usize,
  ) -> Result<Vec<u8>, NetTransportError<A, W>> {
    let mut buf = Vec::with_capacity(COMPRESS_HEADER + encoded_size);
    buf.push(compressor as u8);
    buf.extend(&[0; MAX_MESSAGE_LEN_SIZE]);

    let data = W::encode_message_to_vec(msg).map_err(NetTransportError::Wire)?;
    compressor
      .compress_to_vec(&data, &mut buf)
      .map_err(NetTransportError::Compress)?;

    let data_len = buf.len() - COMPRESS_HEADER;
    NetworkEndian::write_u32(&mut buf[1..COMPRESS_HEADER], data_len as u32);

    Ok(buf)
  }

  #[cfg(feature = "encryption")]
  async fn send_by_promised_with_encryption_without_compression(
    &self,
    mut conn: impl AsyncWrite + Unpin,
    target: &A::ResolvedAddress,
    msg: Message<I, A::ResolvedAddress>,
    stream_label: &Label,
  ) -> Result<usize, NetTransportError<A, W>> {
    let enable_encryption = self.enable_promised_encryption();
    if !enable_encryption {
      return self
        .send_by_promised_without_compression_and_encryption(conn, target, msg)
        .await;
    }

    let enp = self.encryptor.as_ref().unwrap();
    let encoded_size = W::encoded_len(&msg);
    let buf = if encoded_size < self.opts.offload_size {
      let pk = enp.kr.primary_key().await;
      Self::encrypt_message(enp, pk, &self.opts.label, msg, encoded_size)?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let pk = enp.kr.primary_key().await;
      let stream_label = stream_label.cheap_clone();
      let enp = enp.clone();
      rayon::spawn(move || {
        if tx
          .send(Self::encrypt_message(
            &enp,
            pk,
            &stream_label,
            msg,
            encoded_size,
          ))
          .is_err()
        {
          tracing::error!(target: "showbiz.net.promised", "failed to send computation task result back to main thread");
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    let total_len = buf.len();
    conn
      .write_all(&buf)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;

    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_len)
  }

  #[cfg(feature = "compression")]
  async fn send_by_promised_with_compression_without_encryption(
    &self,
    mut conn: impl AsyncWrite + Unpin,
    target: &A::ResolvedAddress,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let compressor = match self.opts.compressor {
      Some(c) => c,
      None => {
        return self
          .send_by_promised_without_compression_and_encryption(conn, target, msg)
          .await
      }
    };

    let encoded_size = W::encoded_len(&msg);

    let buf = if encoded_size < self.opts.offload_size {
      Self::compress_message(compressor, msg, encoded_size)?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      rayon::spawn(move || {
        if tx
          .send(Self::compress_message(compressor, msg, encoded_size))
          .is_err()
        {
          tracing::error!(target: "showbiz.net.promised", "failed to send computation task result back to main thread");
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(NetTransportError::ComputationTaskFailed),
      }
    };

    let total_len = buf.len();
    conn
      .write_all(&buf)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;
    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_len)
  }

  async fn send_by_promised_without_compression_and_encryption(
    &self,
    mut conn: impl AsyncWrite + Unpin,
    _target: &A::ResolvedAddress,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let data = W::encode_message_to_bytes(msg).map_err(NetTransportError::Wire)?;
    let total_data = data.len();
    conn
      .write_all(&data)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))?;

    conn
      .flush()
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::promised_write(e)))
      .map(|_| total_data)
  }
}
