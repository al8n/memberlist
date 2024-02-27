use bytes::{BufMut, BytesMut};
use memberlist_utils::LabelBufMutExt;

use super::*;

impl<I, A, S, W, R> QuicTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: Runtime,
{
  fn encode_batch(
    buf: &mut [u8],
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    let mut offset = 0;
    let num_packets = batch.len();
    // Encode messages to buffer
    if num_packets <= 1 {
      let packet = batch.into_iter().next().unwrap();
      let expected_packet_encoded_size = W::encoded_len(&packet);
      let actual_packet_encoded_size =
        W::encode_message(packet, &mut buf[offset..]).map_err(QuicTransportError::Wire)?;
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
    let total_packets_len_offset = offset;
    // just add a place holder for total message length
    NetworkEndian::write_u32(&mut buf[offset..], 0);
    offset += MAX_MESSAGE_LEN_SIZE;

    let packets_offset = offset;
    buf[offset] = num_packets as u8;
    offset += 1;

    for packet in batch {
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
        "expected packet encoded size {} is not match the actual encoded size {}",
        expected_packet_encoded_size, actual_packet_encoded_size
      );
      offset += actual_packet_encoded_size;
    }

    let actual_encoded_size = offset - packets_offset;
    NetworkEndian::write_u32(
      &mut buf[total_packets_len_offset..total_packets_len_offset + MAX_MESSAGE_LEN_SIZE],
      actual_encoded_size as u32,
    );

    Ok(offset)
  }

  async fn send_batch_without_compression(
    &self,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<Bytes, QuicTransportError<A, S, W>> {
    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_size());
    buf.put_u8(super::StreamType::Packet as u8);
    offset += 1;
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    let offset = buf.len();

    buf.resize(batch.estimate_encoded_size(), 0);

    Self::encode_batch(&mut buf[offset..], batch)?;

    Ok(buf.freeze())
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

    buf.resize(batch.estimate_encoded_size(), 0);
    let encoded_size = Self::encode_batch(&mut buf[offset..], batch)?;
    offset += encoded_size;

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
      return Err(QuicTransportError::PacketTooLarge(buf.len()));
    }
    Ok(buf)
  }

  #[cfg(feature = "compression")]
  async fn send_batch_with_compression(
    &self,
    batch: Batch<I, A::ResolvedAddress>,
  ) -> Result<Bytes, QuicTransportError<A, S, W>> {
    let Some(compressor) = self.opts.compressor else {
      return self.send_batch_without_compression(batch).await;
    };

    let mut offset = 0;
    let mut buf = BytesMut::with_capacity(batch.estimate_encoded_size());
    buf.put_u8(super::StreamType::Packet as u8);
    offset += 1;
    buf.add_label_header(&self.opts.label);
    offset += self.opts.label.encoded_overhead();

    debug_assert_eq!(offset, buf.len(), "wrong label encoded length");

    if buf.len() <= self.opts.offload_size {
      let buf = Self::encode_and_compress_batch(compressor, buf, batch, self.max_payload_size())?;

      return Ok(buf.freeze());
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
      Ok(Ok(buf)) => Ok(buf.freeze()),
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
    return {
      let src = self.send_batch_without_compression(batch).await?;
      self.send_batch_in(addr, src).await
    };

    #[cfg(feature = "compression")]
    {
      let src = self.send_batch_with_compression(batch).await?;
      self.send_batch_in(addr, src).await
    }
  }

  async fn send_batch_in(
    &self,
    addr: SocketAddr,
    src: Bytes,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    let mut stream = self.fetch_stream(addr, None).await?;

    tracing::error!(
      target = "memberlist.transport.quic",
      total_bytes = %src.len(),
      sent = ?src.as_ref(),
    );
    let written = stream
      .write_all(src)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    Ok(written)
  }
}
