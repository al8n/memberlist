use bytes::Bytes;
use memberlist_core::types::LabelBufMutExt;

use super::*;

impl<I, A, S, W, R> NetTransport<I, A, S, W, R>
where
  I: Id + Send + Sync + 'static,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  A::Address: Send + Sync + 'static,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  pub(crate) fn fix_packet_overhead(&self) -> usize {
    let mut overhead = self.opts.label.encoded_overhead();
    overhead += 1 + CHECKSUM_SIZE;

    #[cfg(feature = "compression")]
    if self.opts.compressor.is_some() {
      overhead += 1 + core::mem::size_of::<u32>();
    }

    #[cfg(feature = "encryption")]
    if self.enable_packet_encryption() {
      overhead += self.opts.encryption_algo.unwrap().encrypt_overhead();
    }

    overhead
  }

  #[cfg(feature = "encryption")]
  fn enable_packet_encryption(&self) -> bool {
    self.encryptor.is_some()
      && self.opts.gossip_verify_outgoing
      && self.opts.encryption_algo.is_some()
  }

  fn encode_batch(
    buf: &mut [u8],
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    let mut offset = 0;

    let num_packets = batch.len();
    // Encode messages to buffer
    if num_packets <= 1 {
      let packet = batch.into_iter().next().unwrap();

      let expected_packet_encoded_size = W::encoded_len(&packet);
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(NetTransportError::Wire)?;
      debug_assert_eq!(
        expected_packet_encoded_size, actual_packet_encoded_size,
        "expected packet encoded size {} is not match the actual encoded size {}",
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

    for packet in batch {
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
        "expected packet encoded size {} is not match the actual encoded size {}",
        expected_packet_encoded_size, actual_packet_encoded_size
      );
      offset += actual_packet_encoded_size;
    }

    Ok(offset)
  }

  #[cfg(feature = "compression")]
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
    offset += CHECKSUM_HEADER;

    buf.resize(batch.estimate_encoded_size(), 0);

    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    offset += encoded_size;
    let mut data_offset = checksum_offset + CHECKSUM_HEADER;
    let compressed = compressor.compress_into_bytes(&buf[data_offset..offset])?;
    // Write compressor tag
    buf[data_offset] = compressor as u8;
    data_offset += 1;
    NetworkEndian::write_u32(&mut buf[data_offset..], compressed.len() as u32);
    data_offset += MAX_MESSAGE_LEN_SIZE;

    buf.truncate(data_offset);
    buf.put_slice(&compressed);

    // check if the packet exceeds the max packet size can be sent by the packet layer
    if buf.len() >= max_payload_size {
      return Err(NetTransportError::PacketTooLarge(buf.len()));
    }
    let cks = checksumer.checksum(&buf[checksum_offset + CHECKSUM_HEADER..]);
    NetworkEndian::write_u32(
      &mut buf[checksum_offset + 1..checksum_offset + CHECKSUM_HEADER],
      cks,
    );
    Ok(buf)
  }

  #[cfg(feature = "encryption")]
  #[allow(clippy::too_many_arguments)]
  fn encode_and_encrypt_batch(
    checksumer: Checksumer,
    pk: SecretKey,
    encryption_algo: EncryptionAlgo,
    label: &Label,
    mut buf: BytesMut,
    batch: Batch<I, A::ResolvedAddress>,
    max_payload_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    // write encryption tag
    buf.put_u8(encryption_algo as u8);
    // write length placeholder
    let encrypt_length_offset = buf.len();
    buf.put_u32(0);
    let nonce_offset = buf.len();
    // write encrypt header
    let nonce = security::write_header(&mut buf);
    let mut offset = buf.len();
    let checksum_offset = offset;
    // everything after nonce should be encrypted.
    let data_offset = nonce_offset + nonce.len();

    // reserve to store checksum
    buf.put_u8(checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;
    let estimate_encoded_size = batch.estimate_encoded_size();
    if estimate_encoded_size >= max_payload_size {
      return Err(NetTransportError::PacketTooLarge(estimate_encoded_size));
    }

    buf.resize(estimate_encoded_size, 0);

    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    buf.truncate(offset + encoded_size);

    // update checksum
    let cks = checksumer.checksum(&buf[checksum_offset + CHECKSUM_HEADER..]);
    NetworkEndian::write_u32(
      &mut buf[checksum_offset + 1..checksum_offset + CHECKSUM_HEADER],
      cks,
    );

    // encrypt
    let mut dst = buf.split_off(data_offset);
    security::encrypt(encryption_algo, pk, nonce, label.as_bytes(), &mut dst)
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

  #[cfg(all(feature = "compression", feature = "encryption"))]
  #[allow(clippy::too_many_arguments)]
  fn encode_and_compress_and_encrypt_batch(
    checksumer: Checksumer,
    compressor: Compressor,
    pk: SecretKey,
    encryption_algo: EncryptionAlgo,
    label: &Label,
    mut buf: BytesMut,
    batch: Batch<I, A::ResolvedAddress>,
    max_payload_size: usize,
  ) -> Result<BytesMut, NetTransportError<A, W>> {
    // write encryption tag
    buf.put_u8(encryption_algo as u8);
    // write length placeholder
    let encrypt_length_offset = buf.len();
    buf.put_u32(0);
    let nonce_offset = buf.len();
    // write encrypt header
    let nonce = security::write_header(&mut buf);
    let mut offset = buf.len();
    let checksum_offset = offset;
    // everything after nonce should be encrypted.
    let data_offset = nonce_offset + nonce.len();

    // reserve to store checksum
    buf.put_u8(checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_SIZE + 1;

    buf.resize(batch.estimate_encoded_size(), 0);
    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    offset += encoded_size;
    let mut compress_offset = checksum_offset + CHECKSUM_HEADER;

    let compressed = compressor.compress_into_bytes(&buf[compress_offset..offset])?;
    // Write compressor tag
    buf[compress_offset] = compressor as u8;
    compress_offset += 1;
    NetworkEndian::write_u32(&mut buf[compress_offset..], compressed.len() as u32);
    compress_offset += MAX_MESSAGE_LEN_SIZE;

    buf.truncate(compress_offset);
    buf.put_slice(&compressed);

    // check if the packet exceeds the max packet size can be sent by the packet layer
    if buf.len() >= max_payload_size {
      return Err(NetTransportError::PacketTooLarge(buf.len()));
    }

    let cks = checksumer.checksum(&buf[checksum_offset + CHECKSUM_HEADER..]);
    NetworkEndian::write_u32(
      &mut buf[checksum_offset + 1..checksum_offset + CHECKSUM_HEADER],
      cks,
    );

    // encrypt
    let mut dst = buf.split_off(data_offset);
    security::encrypt(encryption_algo, pk, nonce, label.as_bytes(), &mut dst)
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

  async fn send_batch_without_compression_and_encryption(
    &self,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_size());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let mut offset = buf.len();

    // reserve to store checksum
    let checksum_offset = offset;
    buf.put_u8(self.opts.checksumer as u8);
    buf.put_slice(&[0; CHECKSUM_SIZE]);
    offset += CHECKSUM_HEADER;

    buf.resize(batch.estimate_encoded_size(), 0);
    Self::encode_batch(&mut buf[offset..], batch)?;
    let data_offset = checksum_offset + CHECKSUM_HEADER;
    // update checksum
    let cks = self.opts.checksumer.checksum(&buf[data_offset..]);
    NetworkEndian::write_u32(&mut buf[checksum_offset + 1..data_offset], cks);
    Ok(buf.freeze())
  }

  #[cfg(feature = "compression")]
  async fn send_batch_with_compression_without_encryption(
    &self,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    let Some(compressor) = self.opts.compressor else {
      return self
        .send_batch_without_compression_and_encryption(batch)
        .await;
    };

    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_size());
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

      return Ok(buf.freeze());
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
        tracing::error!(
          "memberlist_net.packet: failed to send computation task result back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(buf)) => Ok(buf.freeze()),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(feature = "encryption")]
  async fn send_batch_with_encryption_without_compression(
    &self,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    if !self.enable_packet_encryption() {
      return self
        .send_batch_without_compression_and_encryption(batch)
        .await;
    }

    let encryptor = self.encryptor.as_ref().unwrap();
    let encryption_algo = self.opts.encryption_algo.unwrap();
    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_size());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let pk = encryptor.primary_key().await;
    if buf.len() <= self.opts.offload_size {
      let buf = Self::encode_and_encrypt_batch(
        self.opts.checksumer,
        pk,
        encryption_algo,
        &self.opts.label,
        buf,
        batch,
        self.max_payload_size(),
      )?;

      return Ok(buf.freeze());
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let checksumer = self.opts.checksumer;
    let max_payload_size = self.max_payload_size();
    let label = self.opts.label.cheap_clone();

    rayon::spawn(move || {
      if tx
        .send(Self::encode_and_encrypt_batch(
          checksumer,
          pk,
          encryption_algo,
          &label,
          buf,
          batch,
          max_payload_size,
        ))
        .is_err()
      {
        tracing::error!(
          "memberlist_net.packet: failed to send computation task result back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(buf)) => Ok(buf.freeze()),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  async fn send_batch_with_compression_and_encryption(
    &self,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<Bytes, NetTransportError<A, W>> {
    let encryption_enabled = self.enable_packet_encryption();
    if self.opts.compressor.is_none() && !encryption_enabled {
      return self
        .send_batch_without_compression_and_encryption(batch)
        .await;
    }

    if self.opts.compressor.is_some() && !encryption_enabled {
      return self
        .send_batch_with_compression_without_encryption(batch)
        .await;
    }

    if self.opts.compressor.is_none() && encryption_enabled {
      return self
        .send_batch_with_encryption_without_compression(batch)
        .await;
    }

    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_size());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let encryptor = self.encryptor.as_ref().unwrap();
    let pk = encryptor.primary_key().await;
    if batch.estimate_encoded_size() / 2 <= self.opts.offload_size {
      let buf = Self::encode_and_compress_and_encrypt_batch(
        self.opts.checksumer,
        self.opts.compressor.unwrap(),
        pk,
        self.opts.encryption_algo.unwrap(),
        &self.opts.label,
        buf,
        batch,
        self.max_payload_size(),
      )?;

      return Ok(buf.freeze());
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let checksumer = self.opts.checksumer;
    let compressor = self.opts.compressor.unwrap();
    let max_payload_size = self.max_payload_size();
    let encryption_algo = self.opts.encryption_algo.unwrap();
    let label = self.opts.label.cheap_clone();

    rayon::spawn(move || {
      if tx
        .send(Self::encode_and_compress_and_encrypt_batch(
          checksumer,
          compressor,
          pk,
          encryption_algo,
          &label,
          buf,
          batch,
          max_payload_size,
        ))
        .is_err()
      {
        tracing::error!(
          "memberlist_net.packet: failed to send computation task result back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(buf)) => Ok(buf.freeze()),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  pub(crate) async fn send_batch(
    &self,
    addr: &A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, NetTransportError<A, W>> {
    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    {
      let buf = self
        .send_batch_without_compression_and_encryption(batch)
        .await?;
      return self.send_batch_in(addr, &buf).await;
    };

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    {
      let buf = self
        .send_batch_with_compression_without_encryption(batch)
        .await?;
      return self.send_batch_in(addr, &buf).await;
    }

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    {
      let buf = self
        .send_batch_with_encryption_without_compression(batch)
        .await?;
      return self.send_batch_in(addr, &buf).await;
    }

    #[cfg(all(feature = "compression", feature = "encryption"))]
    {
      let buf = self
        .send_batch_with_compression_and_encryption(batch)
        .await?;
      self.send_batch_in(addr, &buf).await
    }
  }

  async fn send_batch_in(
    &self,
    addr: &A::ResolvedAddress,
    buf: &[u8],
  ) -> Result<usize, NetTransportError<A, W>> {
    self
      .next_socket(addr)
      .send_to(buf, addr)
      .await
      .map(|num| {
        tracing::trace!(remote=%addr, total_bytes = %num, sent=?buf, "memberlist_net.packet");
        num
      })
      .map_err(|e| ConnectionError::packet_write(e).into())
  }
}
