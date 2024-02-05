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
  #[cfg(feature = "compression")]
  fn encode_and_compress(
    label: &Label,
    msg_encoded_size: usize,
    msg: Message<I, A::ResolvedAddress>,
    compressor: Compressor,
  ) -> Result<BytesMut, QuicTransportError<A, S, W>> {
    let label_encoded_size = label.encoded_overhead();
    let total_len = label_encoded_size + COMPRESS_HEADER + HEADER_SIZE + msg_encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    buf.add_label_header(label);

    let mut offset = label.encoded_overhead();

    // write compress header
    buf[offset] = compressor as u8;
    offset += 1;
    // write compressed data size placeholder
    let compressed_len_offset = offset;
    buf[offset..offset + MAX_MESSAGE_LEN_SIZE].fill(0);
    offset += MAX_MESSAGE_LEN_SIZE;

    buf.resize(total_len, 0);
    buf[offset] = msg.tag();
    NetworkEndian::write_u32(&mut buf[offset + 1..], msg_encoded_size as u32);
    offset += HEADER_SIZE;
    let data = W::encode_message(msg, &mut buf[offset..]).map_err(QuicTransportError::Wire)?;
    let written = data + offset;

    debug_assert_eq!(
      written, total_len,
      "actual encoded size {} does not match expected encoded size {}",
      written, total_len
    );

    let compressed = compressor.compress_into_bytes(&buf[offset..offset + data])?;
    let compressed_size = compressed.len() as u32;
    NetworkEndian::write_u32(&mut buf[compressed_len_offset..], compressed_size);
    buf[offset..offset + compressed.len()].copy_from_slice(&compressed);
    buf.truncate(offset + compressed.len());
    Ok(buf)
  }

  #[cfg(feature = "compression")]
  pub(crate) async fn send_message_with_compression(
    &self,
    conn: &mut S::Stream,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    let Some(compressor) = self.opts.compressor else {
      return self.send_message_without_compression(conn, msg).await;
    };

    let msg_encoded_size = W::encoded_len(&msg);
    let buf = if msg_encoded_size <= self.opts.offload_size {
      Self::encode_and_compress(&self.opts.label, msg_encoded_size, msg, compressor)?
    } else {
      let (tx, rx) = futures::channel::oneshot::channel();
      let label = self.opts.label.cheap_clone();
      rayon::spawn(move || {
        if tx
          .send(Self::encode_and_compress(
            &label,
            msg_encoded_size,
            msg,
            compressor,
          ))
          .is_err()
        {
          tracing::error!(
            target = "memberlist.quic",
            "failed to send compressed message back to the main thread"
          );
        }
      });

      match rx.await {
        Ok(Ok(buf)) => buf,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(QuicTransportError::ComputationTaskFailed),
      }
    };

    let written = conn
      .write_all(buf.freeze())
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;

    conn
      .flush()
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))
      .map(|_| written)
  }

  pub(crate) async fn send_message_without_compression(
    &self,
    conn: &mut S::Stream,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<usize, QuicTransportError<A, S, W>> {
    let label_encoded_size = self.opts.label.encoded_overhead();
    let msg_encoded_size = W::encoded_len(&msg);
    let total_len = label_encoded_size + HEADER_SIZE + msg_encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    buf.add_label_header(&self.opts.label);
    buf.resize(total_len, 0);
    buf[label_encoded_size] = msg.tag();

    NetworkEndian::write_u32(&mut buf[label_encoded_size + 1..], msg_encoded_size as u32);
    let data = W::encode_message(msg, &mut buf[label_encoded_size + HEADER_SIZE..])
      .map_err(QuicTransportError::Wire)?;
    let written = data + label_encoded_size + HEADER_SIZE;

    debug_assert_eq!(
      written, total_len,
      "actual encoded size {} does not match expected encoded size {}",
      written, total_len
    );

    let written = conn
      .write_all(buf.freeze())
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;

    conn
      .flush()
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))
      .map(|_| written)
  }
}
