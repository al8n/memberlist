use std::time::Instant;

use crate::{
  showbiz::MessageHandoff,
  types::{AckResponse, Message, NackResponse},
};
use either::Either;
use futures_util::{Future, Stream};

use super::*;

impl<D, T> Showbiz<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) fn packet_listener(&self, shutdown_rx: async_channel::Receiver<()>) {
    let this = self.clone();
    let transport_rx = this.inner.transport.packet();
    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          packet = transport_rx.recv().fuse() => {
            match packet {
              Ok(packet) => this.ingest_packet(packet).await,
              Err(e) => {
                tracing::error!(target = "showbiz.packet", "failed to receive packet: {}", e);
                // If we got an error, which means on the other side the transport has been closed,
                // so we need to return and shutdown the packet listener
                return;
              },
            }
          }
        }
      }
    });
  }

  async fn ingest_packet(&self, packet: Packet) {
    let addr = packet.from();
    let timestamp = packet.timestamp();
    let (mut buf, mut packet_label) = match Label::remove_label_header_from(packet.into_inner()) {
      Ok((buf, packet_label)) => (buf, packet_label),
      Err(e) => {
        tracing::error!(target = "showbiz.packet", err = %e, addr = %addr);
        return;
      }
    };

    if self.inner.opts.skip_inbound_label_check {
      if !packet_label.is_empty() {
        tracing::error!(target = "showbiz.packet", addr = %addr, err = "unexpected double packet label header");
      }

      // set this from config so that the auth data assertions work below.
      packet_label = self.inner.opts.label.clone();
    }

    if self.inner.opts.label != packet_label {
      tracing::error!(target = "showbiz.packet", addr = %addr, err = "discarding packet with unacceptable label", label = ?packet_label);
      return;
    }

    // Check if encryption is enabled
    if self.encryption_enabled() {
      let keyring = self.keyring().unwrap();
      if buf.len() < ENCODE_HEADER_SIZE {
        tracing::error!(target = "showbiz.packet", addr = %addr, "truncated packet");
        return;
      }

      if let Err(e) = EncodeHeader::from_bytes(&buf.split_to(ENCODE_HEADER_SIZE)) {
        tracing::error!(target = "showbiz.packet", addr = %addr, err=%e, "invalid packet header");
        return;
      }

      // Decrypt the payload
      if !keyring.is_empty() {
        // msg too large, offload decrypt to rayon thread pool
        if buf.len() > self.inner.opts.offload_size {
          let kr = keyring.clone();
          let (tx, rx) = futures_channel::oneshot::channel();
          rayon::spawn(move || {
            if tx
              .send(
                kr.decrypt_payload(&mut buf, packet_label.as_bytes())
                  .map(|_| buf),
              )
              .is_err()
            {
              tracing::error!(
                target = "showbiz.packet",
                err = "fail to send decrypted packet, receiver end closed"
              );
            }
          });
          buf = match rx.await {
            Ok(Ok(buf)) => buf,
            Ok(Err(e)) => {
              tracing::error!(target = "showbiz.packet", addr = %addr, err = %e, "fail to decrypt packet");
              return;
            }
            Err(_) => {
              tracing::error!(target = "showbiz.packet", addr = %addr, err = %Error::<T, D>::OffloadPanic);
              return;
            }
          };
        } else if let Err(e) = keyring.decrypt_payload(&mut buf, packet_label.as_bytes()) {
          if self.inner.opts.gossip_verify_incoming {
            tracing::error!(target = "showbiz.packet", addr = %addr, err = %e, "decrypt packet failed");
            return;
          }
        }
      }
    }

    // if we need to verify checksum
    if buf.len() < ENCODE_HEADER_SIZE {
      tracing::error!(target = "showbiz.packet", addr = %addr, "truncated packet");
      return;
    }

    let header = match EncodeHeader::from_bytes(&buf[..ENCODE_HEADER_SIZE]) {
      Ok(header) => header,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", addr = %addr, err=%e, "invalid packet header");
        return;
      }
    };

    let has_crc = header.meta.marker == MessageType::HasCrc as u8;

    if header.len as usize + ENCODE_HEADER_SIZE + if has_crc { CHECKSUM_SIZE } else { 0 }
      > buf.len()
    {
      tracing::error!(target = "showbiz.packet", addr = %addr, "truncated packet");
      return;
    }

    if has_crc {
      let len = buf.len();
      let cks = crc32fast::hash(&buf[ENCODE_HEADER_SIZE..len - CHECKSUM_SIZE]);
      if cks != u32::from_be_bytes(buf[len - CHECKSUM_SIZE..len].try_into().unwrap()) {
        tracing::error!(target = "showbiz.packet", addr = %addr, err=%DecodeError::ChecksumMismatch);
        return;
      }

      // remove checksum
      buf.truncate(len - CHECKSUM_SIZE);
    }

    self.handle_command(buf.freeze(), addr, timestamp).await
  }

  #[async_recursion::async_recursion]
  async fn handle_command(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    if !buf.has_remaining() {
      tracing::error!(target = "showbiz.packet", addr = %from, err = "missing message type byte");
      return;
    }

    // Decode the message type
    let mt = buf[0];
    let msg_type = match MessageType::try_from(mt) {
      Ok(msg_type) => msg_type,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", addr = %from, err = %e, "message type ({}) not supported", mt);
        return;
      }
    };

    match msg_type {
      MessageType::Compound => self.handle_compound(buf, from, timestamp).await,
      MessageType::Compress => self.handle_compressed(buf, from, timestamp).await,
      MessageType::Ping => self.handle_ping(buf, from).await,
      MessageType::IndirectPing => self.handle_indirect_ping(buf, from).await,
      MessageType::AckResponse => self.handle_ack(buf, from, timestamp).await,
      MessageType::NackResponse => self.handle_nack(buf, from).await,
      MessageType::Suspect | MessageType::Alive | MessageType::Dead | MessageType::User => {
        // Determine the message queue, prioritize alive
        {
          let mut mq = self.inner.queue.lock().await;
          let mut queue = &mut mq.low;
          if msg_type == MessageType::Alive {
            queue = &mut mq.high;
          }

          // Check for overflow and append if not full
          if queue.len() >= self.inner.opts.handoff_queue_depth {
            tracing::warn!(target = "showbiz.packet", addr = %from, "handler queue full, dropping message ({})", msg_type);
          } else {
            queue.push_back(MessageHandoff {
              msg_ty: msg_type,
              buf,
              from,
            });
          }
        }

        // notify of pending message
        if let Err(e) = self.inner.handoff_tx.send(()).await {
          tracing::error!(target = "showbiz.packet", addr = %from, err = %e, "failed to notify of pending message");
        }
      }

      mt => {
        tracing::error!(target = "showbiz.packet", addr = %from, err = "unexpected message type", message_type=%mt);
      }
    }
  }

  async fn handle_compound(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    // Decode the parts
    if !buf.has_remaining() {
      tracing::error!(target = "showbiz.packet", addr = %from, err = %CompoundError::MissingLengthByte, "failed to decode compound request");
      return;
    }

    let num_parts = buf.get_u8() as usize;

    // check we have enough bytes
    if buf.len() < num_parts * 2 {
      tracing::error!(target = "showbiz.packet", addr = %from, err = %CompoundError::Truncated, "failed to decode compound request");
      return;
    }

    // Decode the lengths
    let mut trunc = 0usize;
    let mut lengths = buf.split_to(num_parts * 2);
    for msg in (0..num_parts).filter_map(|_| {
      let len = lengths.get_u16();
      if buf.len() < len as usize {
        trunc += 1;
        return None;
      }

      Some(buf.split_to(len as usize))
    }) {
      self.handle_command(msg, from, timestamp).await;
    }

    if trunc > 0 {
      let num = num_parts - trunc;
      tracing::warn!(target = "showbiz.packet", addr = %from, err = %CompoundError::TruncatedMsgs(num), "failed to decode compound request");
    }
  }

  #[inline]
  async fn handle_compressed(&self, buf: Bytes, from: SocketAddr, timestamp: Instant) {
    // Try to decode the payload
    if !self.inner.opts.compression_algo.is_none() {
      let (_, compress) = match Compress::decode_from_bytes(buf) {
        Ok(compress) => compress,
        Err(e) => {
          tracing::error!(target = "showbiz.packet", remote = %from, err = %e, "failed to decode compressed message");
          return;
        }
      };

      match compress.decompress() {
        Ok(payload) => self.handle_command(payload.into(), from, timestamp).await,
        Err(e) => {
          tracing::error!(target = "showbiz.packet", remote = %from, err = %e, "failed to decompress payload");
        }
      }
    } else {
      self.handle_command(buf, from, timestamp).await
    }
  }

  async fn handle_ping(&self, buf: Bytes, from: SocketAddr) {
    // Decode the ping
    let (_, p) = match Ping::decode_archived::<T::Checksumer>(&buf) {
      Ok(ping) => ping,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", local=%self.inner.id, remote = %from, err = %e, "failed to decode ping request");
        return;
      }
    };

    // If node is provided, verify that it is for us
    if p.target != self.inner.id {
      tracing::error!(target = "showbiz.packet", local=%self.inner.id, remote = %from, "got ping for unexpected node '{}'", p.target);
      return;
    }

    let msg = if let Some(delegate) = &self.delegate {
      let payload = match delegate.ack_payload().await {
        Ok(payload) => payload,
        Err(e) => {
          tracing::error!(target = "showbiz.packet", local=%self.inner.id, remote = %from, err = %e, "failed to get ack payload from delegate");
          return;
        }
      };
      AckResponse {
        seq_no: p.seq_no,
        payload,
      }
      .encode(0, 0)
    } else {
      AckResponse {
        seq_no: p.seq_no,
        payload: Bytes::new(),
      }
      .encode(0, 0)
    };

    if let Err(e) = self.send_msg((&p.source).into(), msg).await {
      tracing::error!(target = "showbiz.packet", addr = %from, err = %e, "failed to send ack response");
    }
  }

  async fn handle_indirect_ping(&self, buf: Bytes, from: SocketAddr) {
    let (h, ind) = match IndirectPing::decode::<T::Checksumer>(&buf) {
      Ok(ind) => ind,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", addr = %from, err = %e, "failed to decode indirect ping request");
        return;
      }
    };

    // TODO: check protocol version and delegate version, currently we do not need to do this
    // because we only have one version

    // Send a ping to the correct host.
    let local_seq_no = self.next_seq_no();

    let ping = Ping {
      seq_no: local_seq_no,
      source: self.inner.id.clone(),
      target: ind.target.clone(),
    };

    // Forward the ack back to the requestor. If the request encodes an origin
    // use that otherwise assume that the other end of the UDP socket is
    // usable.

    let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
    // Setup a response handler to relay the ack
    let this = self.clone();
    let ind_source = ind.source.clone();
    let ind_seq_no = ind.seq_no;

    self
      .set_ack_handler(
        local_seq_no,
        self.inner.opts.probe_timeout,
        move |_payload, _timestamp| {
          async move {
            let _ = cancel_tx.send(());

            // Try to prevent the nack if we've caught it in time.
            let ack = AckResponse::empty(ind_seq_no);
            if let Err(e) = this
              .send_msg(
                ind_source.into(),
                ack.encode(h.meta.r1, h.meta.r2),
              )
              .await
            {
              tracing::error!(target = "showbiz.packet", addr = %from, err = %e, "failed to forward ack");
            }
          }
          .boxed()
        },
      )
      .await;

    if let Err(e) = self
      .send_msg(ind.target.into(), ping.encode(h.meta.r1, h.meta.r2))
      .await
    {
      tracing::error!(target = "showbiz.packet", addr = %from, err = %e, "failed to send ping");
    }

    // Setup a timer to fire off a nack if no ack is seen in time.
    let this = self.clone();
    let probe_timeout = self.inner.opts.probe_timeout;
    <T::Runtime as Runtime>::spawn_detach(async move {
      futures_util::select! {
        _ = <T::Runtime as Runtime>::sleep(probe_timeout).fuse() => {
          // We've not received an ack, so send a nack.
          let nack = NackResponse::new(ind.seq_no);
          if let Err(e) = this.send_msg((&ind.source).into(), nack.encode(h.meta.r1, h.meta.r2)).await {
            tracing::error!(target = "showbiz.packet", local = %ind.source, remote = %from, err = %e, "failed to send nack");
          }
        }
        _ = cancel_rx.fuse() => {
          // We've received an ack, so we can cancel the nack.
        }
      }
    });
  }

  async fn handle_ack(&self, buf: Bytes, from: SocketAddr, timestamp: Instant) {
    match AckResponse::decode::<T::Checksumer>(&buf) {
      Ok((_, ack)) => self.invoke_ack_handler(ack, timestamp).await,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", addr = %from, err=%e, "failed to decode ack response");
      }
    }
  }

  async fn handle_nack(&self, buf: Bytes, from: SocketAddr) {
    match NackResponse::decode::<T::Checksumer>(&buf) {
      Ok((_, nack)) => self.invoke_nack_handler(nack).await,
      Err(e) => {
        tracing::error!(target = "showbiz.packet", addr = %from, err=%e, "failed to decode nack response");
      }
    }
  }

  pub(crate) async fn send_msg(
    &self,
    addr: CowNodeId<'_>,
    msg: Message,
  ) -> Result<(), Error<T, D>> {
    // Check if we can piggy back any messages
    let bytes_avail = self.inner.opts.packet_buffer_size
      - msg.len()
      - COMPOUND_HEADER_OVERHEAD
      - self.inner.opts.label.label_overhead();

    let mut msgs = self
      .get_broadcast_with_prepend(vec![msg], COMPOUND_OVERHEAD, bytes_avail)
      .await?;

    // Fast path if nothing to piggypack
    if msgs.len() == 1 {
      return self.raw_send_msg_packet(&addr, msgs.pop().unwrap()).await;
    }

    // Create a compound message
    let compound = Message::compound(msgs);

    // Send the message
    self.raw_send_msg_packet(&addr, compound).await
  }

  /// Used to send message via packet to another host without
  /// modification, other than compression or encryption if enabled.
  pub(crate) async fn raw_send_msg_packet(
    &self,
    addr: &CowNodeId<'_>,
    mut msg: Message,
  ) -> Result<(), Error<T, D>> {
    macro_rules! return_bail {
      ($this:ident, $buf: ident, $addr: ident) => {{
        #[cfg(feature = "metrics")]
        {
          incr_udp_sent_counter($buf.len() as u64, self.inner.opts.metric_labels.iter());
        }

        $this
          .inner
          .transport
          .write_to(&$buf, $addr.addr())
          .await
          .map(|_| ())
          .map_err(Error::transport)
      }};
    }

    async fn encrypt_packet_offload<TT: Transport, DD: Delegate>(
      kr: SecretKeyring,
      algo: EncryptionAlgo,
      msg: Either<Vec<u8>, Message>,
      label: Label,
    ) -> Result<BytesMut, Error<TT, DD>> {
      let (tx, rx) = futures_channel::oneshot::channel();
      rayon::spawn(move || {
        let pk = kr.primary_key();
        let mut buf = algo.header(msg.len());
        let msg = match &msg {
          Either::Left(src) => src.as_slice(),
          Either::Right(src) => src.underlying_bytes(),
        };
        match kr.encrypt_payload(pk, algo, msg, label.as_bytes(), &mut buf) {
          Ok(_) => {
            if tx.send(Ok(buf)).is_err() {
              tracing::error!(
                target = "showbiz.packet",
                err = "fail to send encrypt packet, receiver end closed"
              );
            }
          }
          Err(e) => {
            tracing::error!(target = "showbiz.packet", err = %e, "failed to compress message");
            if tx.send(Err(e.into())).is_err() {
              tracing::error!(
                target = "showbiz.packet",
                err = "fail to send encryption error, receiver end closed"
              );
            }
          }
        }
      });
      match rx.await {
        Ok(Ok(b)) => Ok(b),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(Error::OffloadPanic),
      }
    }

    let encryption_enabled = self.encryption_enabled();
    // Check if we have compression enabled
    if !self.inner.opts.compression_algo.is_none() {
      // offload compression to a background thread
      let compressed = if msg.len() > self.inner.opts.offload_size {
        let (tx, rx) = futures_channel::oneshot::channel();
        let compression_algo = self.inner.opts.compression_algo;
        rayon::spawn(move || {
          match Compress::encode_slice_with_checksum(compression_algo, 0, 0, msg.underlying_bytes())
          {
            Ok(compressed) => {
              if tx.send(Ok(compressed)).is_err() {
                tracing::error!(
                  target = "showbiz.packet",
                  err = "fail to send compressed packet, receiver end closed"
                );
              }
            }
            Err(e) => {
              tracing::error!(target = "showbiz.packet", err = %e, "failed to compress message");
              if tx.send(Err(e)).is_err() {
                tracing::error!(
                  target = "showbiz.packet",
                  err = "fail to send compression error, receiver end closed"
                );
              }
            }
          }
        });
        match rx.await {
          Ok(Ok(b)) => b,
          Ok(Err(e)) => return Err(e.into()),
          Err(_) => return Err(Error::OffloadPanic),
        }
      } else {
        Compress::encode_slice_with_checksum(
          self.inner.opts.compression_algo,
          0,
          0,
          msg.underlying_bytes(),
        )?
      };

      // Check if we also have encryption enabled
      if encryption_enabled && self.inner.opts.gossip_verify_outgoing {
        let buf = if compressed.len() > self.inner.opts.offload_size {
          let kr = self.keyring().unwrap().clone();
          encrypt_packet_offload(
            kr,
            self.inner.opts.encryption_algo,
            Either::Left(compressed),
            self.inner.opts.label.clone(),
          )
          .await?
        } else {
          let kr = self.keyring().unwrap();
          let pk = kr.primary_key();
          let mut buf = self.inner.opts.encryption_algo.header(compressed.len());
          kr.encrypt_payload(
            pk,
            self.inner.opts.encryption_algo,
            &compressed,
            self.inner.opts.label.as_bytes(),
            &mut buf,
          )?;
          buf
        };
        return return_bail!(self, buf, addr);
      } else {
        return return_bail!(self, compressed, addr);
      }
    }

    // mark this message should be checksumed
    msg.0[1] = MessageType::HasCrc as u8;
    // append checksum
    msg.put_u32(crc32fast::hash(&msg[ENCODE_HEADER_SIZE..]));

    // Check if only encryption is enabled
    if !self.inner.opts.encryption_algo.is_none() && self.inner.opts.gossip_verify_outgoing {
      let buf = if msg.underlying_bytes().len() > self.inner.opts.offload_size {
        let kr = self.keyring().unwrap().clone();
        encrypt_packet_offload(
          kr,
          self.inner.opts.encryption_algo,
          Either::Right(msg),
          self.inner.opts.label.clone(),
        )
        .await?
      } else {
        let kr = self.keyring().unwrap();
        let pk = kr.primary_key();
        let src = msg.underlying_bytes();
        let mut buf = self.inner.opts.encryption_algo.header(src.len());
        kr.encrypt_payload(
          pk,
          self.inner.opts.encryption_algo,
          src,
          self.inner.opts.label.as_bytes(),
          &mut buf,
        )?;
        buf
      };
      return return_bail!(self, buf, addr);
    }

    let buf = msg.underlying_bytes();
    return_bail!(self, buf, addr)
  }
}

#[derive(Debug, thiserror::Error)]
enum CompoundError {
  #[error("missing compound length byte")]
  MissingLengthByte,
  #[error("truncated len slice")]
  Truncated,
  #[error("compound request had {0} truncated messages")]
  TruncatedMsgs(usize),
}
