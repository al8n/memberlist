use bytes::{BufMut, BytesMut};
use memberlist_core::types::LabelBufMutExt;

use super::*;

impl<I, A, S, W, R> QuicTransport<I, A, S, W, R>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
  R: RuntimeLite,
{
  #[cfg(feature = "compression")]
  fn encode_and_compress(
    label: &Label,
    msg_encoded_size: usize,
    msg: Message<I, A::ResolvedAddress>,
    compressor: Compressor,
  ) -> Result<BytesMut, QuicTransportError<A, S, W>> {
    let label_encoded_size = label.encoded_overhead();
    let total_len = 1 + label_encoded_size + COMPRESS_HEADER + msg_encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_u8(StreamType::Stream as u8);
    buf.add_label_header(label);

    let mut offset = 1 + label.encoded_overhead();

    // write compress header
    buf.put_u8(compressor as u8);
    offset += 1;
    // write compressed data size placeholder
    let compressed_len_offset = offset;
    buf.put_u32(0);
    offset += MAX_MESSAGE_LEN_SIZE;

    buf.resize(total_len, 0);
    let data = W::encode_message(msg, &mut buf[offset..]).map_err(QuicTransportError::Wire)?;
    let written = data + offset;

    debug_assert_eq!(
      written, total_len,
      "actual encoded size {} does not match expected encoded size {}",
      written, total_len
    );

    let compressed = compressor.compress_into_bytes(&buf[offset..])?;
    let compressed_size = compressed.len() as u32;
    NetworkEndian::write_u32(&mut buf[compressed_len_offset..], compressed_size);
    buf.truncate(offset);
    buf.put_slice(&compressed);
    Ok(buf)
  }

  #[cfg(feature = "compression")]
  pub(crate) async fn send_message_with_compression(
    &self,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<Bytes, QuicTransportError<A, S, W>> {
    let Some(compressor) = self.opts.compressor else {
      return self.send_message_without_compression(msg).await;
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

    Ok(buf.freeze())
  }

  pub(crate) async fn send_message_without_compression(
    &self,
    msg: Message<I, A::ResolvedAddress>,
  ) -> Result<Bytes, QuicTransportError<A, S, W>> {
    let label_encoded_size = self.opts.label.encoded_overhead();
    let msg_encoded_size = W::encoded_len(&msg);
    let total_len = 1 + label_encoded_size + msg_encoded_size;
    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_u8(StreamType::Stream as u8);
    buf.add_label_header(&self.opts.label);
    let offset = buf.len();
    buf.resize(total_len, 0);

    let data = W::encode_message(msg, &mut buf[offset..]).map_err(QuicTransportError::Wire)?;
    let written = data + offset;

    debug_assert_eq!(
      written, total_len,
      "actual encoded size {} does not match expected encoded size {}",
      written, total_len
    );

    Ok(buf.freeze())
  }
}
