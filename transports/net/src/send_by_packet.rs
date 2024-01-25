use super::*;

impl<I, A, S, W> NetTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire,
{
  fn enable_packet_encryption(&self) -> bool {
    self.encryptor.is_some() && self.opts.gossip_verify_outgoing
  }

  async fn send_batch_without_compression_and_encryption(
    &self,
    addr: &A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_len());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let mut offset = buf.len();

    // reserve to store checksum
    buf.put_u8(self.opts.checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;

    buf.resize(batch.estimate_encoded_len(), 0);

    // Encode compound message header
    buf[offset] = Message::<I, A::ResolvedAddress>::COMPOUND_TAG;
    offset += 1;
    buf[offset] = batch.num_packets as u8;
    offset += 1;

    // Encode messages to buffer
    for packet in batch.packets {
      let expected_packet_encoded_size = W::encoded_len(&packet);
      NetworkEndian::write_u16(
        &mut buf[offset..offset + PACKET_OVERHEAD],
        expected_packet_encoded_size as u16,
      );
      offset += PACKET_OVERHEAD;
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(NetTransportError::Wire)?;
      debug_assert_eq!(
        expected_packet_encoded_size, actual_packet_encoded_size,
        "expected packet encoded size {} (calculated by Wire::encoded_len()) is not match the actual encoded size {} returned by Wire::encode_message()",
        expected_packet_encoded_size, actual_packet_encoded_size
      );
      offset += actual_packet_encoded_size;
    }

    self.sockets[0]
      .send_to(&buf, addr)
      .await
      .map_err(|e| NetTransportError::Connection(ConnectionError::packet_write(e)))
  }

  fn encode_batch(
    buf: &mut [u8],
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let mut offset = 0;

    let num_packets = batch.packets.len();
    // Encode messages to buffer
    if num_packets <= 1 {
      let packet = batch.packets.into_iter().next().unwrap();
      let expected_packet_encoded_size = W::encoded_len(&packet);
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(NetTransportError::Wire)?;
      debug_assert_eq!(
        expected_packet_encoded_size, actual_packet_encoded_size,
        "expected packet encoded size {} (calculated by Wire::encoded_len()) is not match the actual encoded size {} returned by Wire::encode_message()",
        expected_packet_encoded_size, actual_packet_encoded_size
      );
      offset += actual_packet_encoded_size;
      return Ok(offset);
    }

    // Encode compound message header
    buf[offset] = Message::<I, A::ResolvedAddress>::COMPOUND_TAG;
    offset += 1;
    buf[offset] = num_packets as u8;
    offset += 1;

    for packet in batch.packets {
      let expected_packet_encoded_size = W::encoded_len(&packet);
      NetworkEndian::write_u16(
        &mut buf[offset..offset + PACKET_OVERHEAD],
        expected_packet_encoded_size as u16,
      );
      offset += PACKET_OVERHEAD;
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(NetTransportError::Wire)?;
      debug_assert_eq!(
        expected_packet_encoded_size, actual_packet_encoded_size,
        "expected packet encoded size {} (calculated by Wire::encoded_len()) is not match the actual encoded size {} returned by Wire::encode_message()",
        expected_packet_encoded_size, actual_packet_encoded_size
      );
      offset += actual_packet_encoded_size;
    }

    Ok(offset)
  }

  fn encode_and_compress_batch(
    checksumer: Checksumer,
    compressor: Compressor,
    mut buf: BytesMut,
    batch: Batch<I, A::ResolvedAddress>,
    max_payload_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let checksum_offset = buf.len();
    let mut offset = buf.len();
    // reserve to store checksum
    buf.put_u8(checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;

    buf.resize(batch.estimate_encoded_len(), 0);

    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    offset += encoded_size;
    let mut data_offset = checksum_offset + 1 + CHECKSUM_SIZE;

    let compressed = compressor.compress_into_bytes(&buf[data_offset..offset])?;
    // Write compressor tag
    buf[data_offset] = compressor as u8;
    data_offset += 1;
    NetworkEndian::write_u32(&mut buf[data_offset..], compressed.len() as u32);
    data_offset += MAX_MESSAGE_LEN_SIZE;

    buf[data_offset..data_offset + compressed.len()].copy_from_slice(&compressed);
    buf.truncate(data_offset + compressed.len());

    // check if the packet exceeds the max packet size can be sent by the packet layer
    if buf.len() >= max_payload_size {
      return Err(NetTransportError::PacketTooLarge(buf.len()));
    }

    let cks = checksumer.checksum(&compressed);
    NetworkEndian::write_u32(
      &mut buf[checksum_offset + 1..checksum_offset + 1 + CHECKSUM_SIZE],
      cks,
    );
    Ok(buf)
  }

  #[cfg(feature = "compression")]
  async fn send_batch_with_compression_without_encryption(
    &self,
    addr: &A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let Some(compressor) = self.opts.compressor else {
      return self
        .send_batch_without_compression_and_encryption(addr, batch)
        .await;
    };

    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_len());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    if buf.len() <= self.opts.offload_size {
      let buf = Self::encode_and_compress_batch(
        self.opts.checksumer,
        compressor,
        buf,
        batch,
        self.max_payload_size(),
      )?;

      return self.sockets[0]
        .send_to(&buf, addr)
        .await
        .map_err(|e| ConnectionError::packet_write(e).into());
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let checksumer = self.opts.checksumer;
    let max_payload_size = self.max_payload_size();
    rayon::spawn(move || {
      if tx
        .send(Self::encode_and_compress_batch(
          checksumer,
          compressor,
          buf,
          batch,
          max_payload_size,
        ))
        .is_err()
      {
        tracing::error!(target: "showbiz.net.packet", "failed to send computation task result back to main thread");
      }
    });

    match rx.await {
      Ok(Ok(buf)) => self.sockets[0]
        .send_to(&buf, addr)
        .await
        .map_err(|e| ConnectionError::packet_write(e).into()),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  fn encode_and_encrypt_batch(
    checksumer: Checksumer,
    pk: SecretKey,
    encryptor: &Encryptor,
    label: &Label,
    mut buf: BytesMut,
    batch: Batch<I, A::ResolvedAddress>,
    max_payload_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let mut offset = buf.len();
    // write encryption tag
    buf.put_u8(encryptor.algo as u8);
    offset += 1;
    // write length placeholder
    let encrypt_length_offset = offset;
    buf.put_u32(0);
    offset += MAX_MESSAGE_LEN_SIZE;
    // write encrypt header
    let nonce = encryptor.write_header(&mut buf);

    let checksum_offset = buf.len();

    // reserve to store checksum
    buf.put_u8(checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;

    let estimate_encoded_len = batch.estimate_encoded_len();
    if estimate_encoded_len >= max_payload_size {
      return Err(NetTransportError::PacketTooLarge(estimate_encoded_len));
    }

    buf.resize(estimate_encoded_len, 0);

    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    offset += encoded_size;
    let data_offset = checksum_offset;

    // update checksum
    let cks = checksumer.checksum(&buf[data_offset + CHECKSUM_HEADER..]);
    NetworkEndian::write_u32(
      &mut buf[checksum_offset + 1..checksum_offset + CHECKSUM_HEADER],
      cks,
    );

    // encrypt
    let mut dst = buf.split_off(data_offset);
    encryptor
      .encrypt(pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        let buf_len = buf.len();
        debug_assert_eq!(
          buf_len, offset,
          "wrong encrypt msg length, expected {}, actual {}",
          offset, buf_len
        );
        // update encrypt msg length
        NetworkEndian::write_u32(
          &mut buf[encrypt_length_offset..encrypt_length_offset + MAX_MESSAGE_LEN_SIZE],
          (buf_len - (encrypt_length_offset + MAX_MESSAGE_LEN_SIZE)) as u32,
        );
        buf
      })
      .map_err(NetTransportError::Security)
  }

  #[cfg(feature = "encryption")]
  async fn send_batch_with_encryption_without_compression(
    &self,
    addr: &A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    if self.encryptor.is_none() || !self.opts.gossip_verify_outgoing {
      return self
        .send_batch_without_compression_and_encryption(addr, batch)
        .await;
    }

    let encryptor = self.encryptor.as_ref().unwrap();
    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_len());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let pk = encryptor.kr.primary_key().await;
    if buf.len() <= self.opts.offload_size {
      let buf = Self::encode_and_encrypt_batch(
        self.opts.checksumer,
        pk,
        encryptor,
        &self.opts.label,
        buf,
        batch,
        self.max_payload_size(),
      )?;

      return self.sockets[0]
        .send_to(&buf, addr)
        .await
        .map_err(|e| ConnectionError::packet_write(e).into());
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let checksumer = self.opts.checksumer;
    let max_payload_size = self.max_payload_size();
    let encryptor = encryptor.clone();
    let label = self.opts.label.cheap_clone();

    rayon::spawn(move || {
      if tx
        .send(Self::encode_and_encrypt_batch(
          checksumer,
          pk,
          &encryptor,
          &label,
          buf,
          batch,
          max_payload_size,
        ))
        .is_err()
      {
        tracing::error!(target: "showbiz.net.packet", "failed to send computation task result back to main thread");
      }
    });

    match rx.await {
      Ok(Ok(buf)) => self.sockets[0]
        .send_to(&buf, addr)
        .await
        .map_err(|e| ConnectionError::packet_write(e).into()),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  #[allow(clippy::too_many_arguments)]
  fn encode_and_compress_and_encrypt_batch(
    checksumer: Checksumer,
    compressor: Compressor,
    pk: SecretKey,
    encryptor: &Encryptor,
    label: &Label,
    mut buf: BytesMut,
    batch: Batch<I, A::ResolvedAddress>,
    max_payload_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    let mut offset = buf.len();
    // write encryption tag
    buf.put_u8(encryptor.algo as u8);
    offset += 1;
    // write length placeholder
    let encrypt_length_offset = offset;
    buf.put_u32(0);
    offset += MAX_MESSAGE_LEN_SIZE;
    // write encrypt header
    let nonce = encryptor.write_header(&mut buf);

    let checksum_offset = buf.len();

    // reserve to store checksum
    buf.put_u8(checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;

    buf.resize(batch.estimate_encoded_len(), 0);

    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    offset += encoded_size;
    let mut data_offset = checksum_offset + 1 + CHECKSUM_SIZE;

    let compressed = compressor.compress_into_bytes(&buf[data_offset..offset])?;
    // Write compressor tag
    buf[data_offset] = compressor as u8;
    data_offset += 1;
    NetworkEndian::write_u32(&mut buf[data_offset..], compressed.len() as u32);
    data_offset += MAX_MESSAGE_LEN_SIZE;

    buf[data_offset..data_offset + compressed.len()].copy_from_slice(&compressed);
    buf.truncate(data_offset + compressed.len());

    // check if the packet exceeds the max packet size can be sent by the packet layer
    if buf.len() >= max_payload_size {
      return Err(NetTransportError::PacketTooLarge(buf.len()));
    }

    let cks = checksumer.checksum(&compressed);
    NetworkEndian::write_u32(
      &mut buf[checksum_offset + 1..checksum_offset + 1 + CHECKSUM_SIZE],
      cks,
    );

    // encrypt
    let mut dst = buf.split_off(data_offset);
    encryptor
      .encrypt(pk, nonce, label.as_bytes(), &mut dst)
      .map(|_| {
        buf.unsplit(dst);
        let buf_len = buf.len();
        // update encrypt msg length
        NetworkEndian::write_u32(
          &mut buf[encrypt_length_offset..encrypt_length_offset + MAX_MESSAGE_LEN_SIZE],
          (buf_len - (encrypt_length_offset + MAX_MESSAGE_LEN_SIZE)) as u32,
        );
        buf
      })
      .map_err(NetTransportError::Security)
  }

  pub(super) async fn send_batch(
    &self,
    addr: &A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    return self
      .send_batch_without_compression_and_encryption(addr, batch)
      .await;

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    return self
      .send_batch_with_compression_without_encryption(addr, batch)
      .await;

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    return self
      .send_batch_with_encryption_without_compression(addr, batch)
      .await;

    let encryption_enabled = self.enable_packet_encryption();
    if self.opts.compressor.is_none() && !encryption_enabled {
      return self
        .send_batch_without_compression_and_encryption(addr, batch)
        .await;
    }

    if self.opts.compressor.is_some() && !encryption_enabled {
      return self
        .send_batch_with_compression_without_encryption(addr, batch)
        .await;
    }

    if self.opts.compressor.is_none() && encryption_enabled {
      return self
        .send_batch_with_encryption_without_compression(addr, batch)
        .await;
    }

    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_len());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let encryptor = self.encryptor.as_ref().unwrap();
    let pk = encryptor.kr.primary_key().await;
    if batch.estimate_encoded_len() / 2 <= self.opts.offload_size {
      let buf = Self::encode_and_compress_and_encrypt_batch(
        self.opts.checksumer,
        self.opts.compressor.unwrap(),
        pk,
        encryptor,
        &self.opts.label,
        buf,
        batch,
        self.max_payload_size(),
      )?;

      return self.sockets[0]
        .send_to(&buf, addr)
        .await
        .map_err(|e| ConnectionError::packet_write(e).into());
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let checksumer = self.opts.checksumer;
    let compressor = self.opts.compressor.unwrap();
    let max_payload_size = self.max_payload_size();
    let encryptor = encryptor.clone();
    let label = self.opts.label.cheap_clone();

    rayon::spawn(move || {
      if tx
        .send(Self::encode_and_compress_and_encrypt_batch(
          checksumer,
          compressor,
          pk,
          &encryptor,
          &label,
          buf,
          batch,
          max_payload_size,
        ))
        .is_err()
      {
        tracing::error!(target: "showbiz.net.packet", "failed to send computation task result back to main thread");
      }
    });

    match rx.await {
      Ok(Ok(buf)) => self.sockets[0]
        .send_to(&buf, addr)
        .await
        .map_err(|e| ConnectionError::packet_write(e).into()),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }
}
