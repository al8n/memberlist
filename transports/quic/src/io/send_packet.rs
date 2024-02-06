use bytes::BytesMut;
use memberlist_utils::LabelBufMutExt;

use super::*;

impl<I, A, S, W> QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  fn encode_batch(
    buf: &mut [u8],
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    let mut offset = 0;

    let num_packets = batch.packets.len();
    // Encode messages to buffer
    if num_packets <= 1 {
      let packet = batch.packets.into_iter().next().unwrap();
      let tag = packet.tag();
      let expected_packet_encoded_size = W::encoded_len(&packet) + HEADER_SIZE;
      buf[offset] = tag;
      offset += 1;
      NetworkEndian::write_u32(
        &mut buf[offset..offset + MAX_MESSAGE_LEN_SIZE],
        expected_packet_encoded_size as u32,
      );
      offset += MAX_MESSAGE_LEN_SIZE;
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(QuicTransportError::Wire)?;
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
    let total_msg_len_offset = offset;
    // just add a place holder for total message length
    NetworkEndian::write_u32(&mut buf[offset..], 0);
    offset += MAX_MESSAGE_LEN_SIZE;
    buf[offset] = num_packets as u8;
    offset += 1;

    let packets_offset = offset;
    for packet in batch.packets {
      let expected_packet_encoded_size = W::encoded_len(&packet);
      NetworkEndian::write_u32(
        &mut buf[offset..offset + PACKET_OVERHEAD],
        expected_packet_encoded_size as u32,
      );
      offset += PACKET_OVERHEAD;
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(QuicTransportError::Wire)?;
      debug_assert_eq!(
        expected_packet_encoded_size, actual_packet_encoded_size,
        "expected packet encoded size {} (calculated by Wire::encoded_len()) is not match the actual encoded size {} returned by Wire::encode_message()",
        expected_packet_encoded_size, actual_packet_encoded_size
      );
      offset += actual_packet_encoded_size;
    }

    let actual_encoded_size = offset - packets_offset;
    NetworkEndian::write_u32(
      &mut buf[total_msg_len_offset..total_msg_len_offset + MAX_MESSAGE_LEN_SIZE],
      actual_encoded_size as u32,
    );

    Ok(offset)
  }

  async fn send_batch_without_compression(
    &self,
    addr: A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_len());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let offset = buf.len();

    buf.resize(batch.estimate_encoded_len(), 0);

    Self::encode_batch(&mut buf[offset..], batch)?;

    self
      .next_connector()
      .open_uni(addr)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?
      .write_all(buf.freeze())
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))
  }

  #[cfg(feature = "compression")]
  fn encode_and_compress_batch(
    compressor: Compressor,
    mut buf: BytesMut,
    batch: Batch<I, A::ResolvedAddress>,
    max_payload_size: usize,
  ) -> Result<BytesMut, QuicTransportError<A, S, W>> {
    let mut offset = buf.len();
    let mut data_offset = offset;

    buf.resize(batch.estimate_encoded_len(), 0);

    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    offset += encoded_size;

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
      return Err(QuicTransportError::PacketTooLarge(buf.len()));
    }

    Ok(buf)
  }

  #[cfg(feature = "compression")]
  async fn send_batch_with_compression(
    &self,
    addr: A::ResolvedAddress,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    let Some(compressor) = self.opts.compressor else {
      return self.send_batch_without_compression(addr, batch).await;
    };

    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_len());
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    if buf.len() <= self.opts.offload_size {
      let buf = Self::encode_and_compress_batch(compressor, buf, batch, self.max_payload_size())?;

      return self
        .next_connector()
        .open_uni(addr)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?
        .write_all(buf.freeze())
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()));
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let max_payload_size = self.max_payload_size();
    rayon::spawn(move || {
      if tx
        .send(Self::encode_and_compress_batch(
          compressor,
          buf,
          batch,
          max_payload_size,
        ))
        .is_err()
      {
        tracing::error!(
          target = "memberlist.transport.quic.packet",
          "failed to send computation task result back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(buf)) => self
        .next_connector()
        .open_uni(addr)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?
        .write_all(buf.freeze())
        .await
        .map_err(|e| QuicTransportError::Stream(e.into())),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(QuicTransportError::ComputationTaskFailed),
    }
  }

  pub(crate) async fn send_batch(
    &self,
    addr: SocketAddr,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    #[cfg(not(feature = "compression"))]
    return self.send_batch_without_compression(addr, batch).await;

    #[cfg(feature = "compression")]
    self.send_batch_with_compression(addr, batch).await
  }
}
