use super::*;

impl<I, A, S, W> QuicTransport<I, A, S, W>
where
  I: Id,
  A: AddressResolver<ResolvedAddress = SocketAddr>,
  S: StreamLayer,
  W: Wire<Id = I, Address = A::ResolvedAddress>,
{
  pub(crate) async fn read_message_without_compression(
    &self,
    conn: &mut S::Stream,
  ) -> Result<(usize, Message<I, A::ResolvedAddress>), QuicTransportError<A, S, W>> {
    let mut buf = [0u8; MAX_MESSAGE_LEN_SIZE];
    let mut readed = 0;
    conn
      .read_exact(&mut buf)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    readed += MAX_MESSAGE_LEN_SIZE;

    let msg_len = NetworkEndian::read_u32(&buf) as usize;
    if msg_len <= MAX_INLINED_BYTES {
      let mut data = [0u8; MAX_INLINED_BYTES];
      conn
        .read_exact(&mut data[..msg_len])
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      readed += msg_len;
      let (decoded_msg_len, msg) =
        W::decode_message(&data[..msg_len]).map_err(QuicTransportError::Wire)?;

      debug_assert_eq!(
        msg_len, decoded_msg_len,
        "expected bytes read {} is not match the actual bytes read {}",
        msg_len, decoded_msg_len
      );
      return Ok((readed, msg));
    }

    let mut data = vec![0u8; msg_len];
    conn
      .read_exact(&mut data)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    readed += msg_len;

    let (decoded_msg_len, msg) = W::decode_message(&data).map_err(QuicTransportError::Wire)?;

    debug_assert_eq!(
      msg_len, decoded_msg_len,
      "expected bytes read {} is not match the actual bytes read {}",
      msg_len, decoded_msg_len
    );
    Ok((readed, msg))
  }

  #[cfg(feature = "compression")]
  fn decompress(
    compressor: Compressor,
    data: &[u8],
  ) -> Result<Message<I, A::ResolvedAddress>, QuicTransportError<A, S, W>> {
    let uncompressed = compressor.decompress(data)?;

    W::decode_message(&uncompressed)
      .map(|(_, msg)| msg)
      .map_err(QuicTransportError::Wire)
  }

  #[cfg(feature = "compression")]
  pub(crate) async fn read_message_with_compression(
    &self,
    conn: &mut S::Stream,
  ) -> Result<(usize, Message<I, A::ResolvedAddress>), QuicTransportError<A, S, W>> {
    let mut tag = [0u8; 1];
    conn
      .peek_exact(&mut tag)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    let tag = tag[0];
    if !COMPRESS_TAG.contains(&tag) {
      return self.read_message_without_compression(conn).await;
    }

    let mut readed = 0;
    let mut compress_header = [0u8; COMPRESS_HEADER];
    conn
      .read_exact(&mut compress_header)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    readed += COMPRESS_HEADER;
    let compressor = Compressor::try_from(compress_header[0])?;
    let data_len = NetworkEndian::read_u32(&compress_header[1..]) as usize;
    if data_len <= self.opts.offload_size {
      if data_len <= MAX_INLINED_BYTES {
        let mut data = [0u8; MAX_INLINED_BYTES];
        conn
          .read_exact(&mut data[..data_len])
          .await
          .map_err(|e| QuicTransportError::Stream(e.into()))?;
        readed += data_len;
        let msg = Self::decompress(compressor, &data[..data_len])?;
        return Ok((readed, msg));
      }

      let mut data = vec![0u8; data_len];
      conn
        .read_exact(&mut data)
        .await
        .map_err(|e| QuicTransportError::Stream(e.into()))?;
      readed += data_len;
      let msg = Self::decompress(compressor, &data)?;
      return Ok((readed, msg));
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let mut data = vec![0u8; data_len];
    conn
      .read_exact(&mut data)
      .await
      .map_err(|e| QuicTransportError::Stream(e.into()))?;
    readed += data_len;
    rayon::spawn(move || {
      if tx.send(Self::decompress(compressor, &data)).is_err() {
        tracing::error!(target: "memberlist.net.promised", "failed to send computation task result back to main thread");
      }
    });

    match rx.await {
      Ok(Ok(msg)) => Ok((readed, msg)),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(QuicTransportError::ComputationTaskFailed),
    }
  }
}
