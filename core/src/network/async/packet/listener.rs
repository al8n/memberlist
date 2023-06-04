use std::time::Instant;

use crate::{
  checksum::Checksumer,
  security::{decrypt_payload, pkcs7encode, BLOCK_SIZE, NONCE_SIZE},
  showbiz::MessageHandoff,
  types::{Message, NodeId},
  util::decompress_payload,
};
use futures_timer::Delay;
use rand::Rng;

use super::*;

impl<D, T, S> Showbiz<D, T, S>
where
  T: Transport,
  S: Spawner,
  D: Delegate,
{
  pub(crate) fn packet_listener(&self) {
    let this = self.clone();
    let shutdown_rx = this.inner.shutdown_rx.clone();
    let transport_rx = this.inner.transport.packet().clone();
    let spawner = self.inner.spawner;
    spawner.spawn(Box::pin(async move {
      loop {
        futures_util::select! {
          _ = shutdown_rx.recv().fuse() => {
            return;
          }
          packet = transport_rx.recv().fuse() => {
            match packet {
              Ok(packet) => this.ingest_packet(packet).await,
              Err(e) => tracing::error!(target = "showbiz", "failed to receive packet: {}", e),
            }
          }
        }
      }
    }));
  }

  async fn ingest_packet(&self, packet: Packet) {
    let addr = packet.from();
    let timestamp = packet.timestamp();
    let (mut buf, mut packet_label) = match Self::remove_label_header_from(packet.into_inner()) {
      Ok((buf, packet_label)) => (buf, packet_label),
      Err(e) => {
        tracing::error!(target = "showbiz", err = %e, addr = %addr);
        return;
      }
    };

    if self.inner.opts.skip_inbound_label_check {
      if !packet_label.is_empty() {
        tracing::error!(target = "showbiz", addr = %addr, err = "unexpected double packet label header");
      }

      // set this from config so that the auth data assertions work below.
      packet_label = self.inner.opts.label.clone();
    }

    if self.inner.opts.label != packet_label {
      tracing::error!(target = "showbiz", addr = %addr, err = "discarding packet with unacceptable label", label = ?packet_label);
      return;
    }

    // Check if encryption is enabled
    if let Some(keyring) = &self.inner.keyring {
      let keys = keyring.lock().await;
      // Decrypt the payload
      if !keys.is_empty() {
        if let Err(e) = decrypt_payload(
          &keys.keys(),
          &mut buf,
          &packet_label,
          self.inner.opts.encryption_algo,
        ) {
          if self.inner.opts.gossip_verify_incoming {
            tracing::error!(target = "showbiz", addr = %addr, err = %e, "decrypt packet failed");
            return;
          }
        }
      }
    }

    // See if there's a checksum included to verify the contents of the message
    let meta_size = MessageType::SIZE + core::mem::size_of::<u32>();
    if buf.len() >= meta_size && buf[0] == MessageType::HasCrc as u8 {
      let mut buf = buf.freeze();
      buf.advance(1);
      match decode_u32_from_buf(&mut buf) {
        Ok((size, _)) => {
          if size as usize + 4 > buf.remaining() {
            tracing::error!(target = "showbiz", addr = %addr, err = "truncated message");
            return;
          }
          let msg = buf.split_to(size as usize);
          let mut hasher = T::Checksumer::new();
          hasher.update(&msg);
          let crc = hasher.finalize();
          let expected_crc = buf.get_u32();
          if crc != expected_crc {
            tracing::warn!(target = "showbiz", addr = %addr, "got invalid checksum for UDP packet: {} vs. {}", crc, expected_crc);
            return;
          }
          self.handle_command(msg, addr, timestamp).await
        }
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %addr, err = %e, "failed to decode plain message")
        }
      }
    } else {
      self.handle_command(buf.freeze(), addr, timestamp).await
    }
  }

  #[async_recursion::async_recursion]
  async fn handle_command(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    if !buf.has_remaining() {
      tracing::error!(target = "showbiz", addr = %from, err = "missing message type byte");
      return;
    }

    // Decode the message type
    let mt = buf.get_u8();
    let msg_type = match MessageType::try_from(mt) {
      Ok(msg_type) => msg_type,
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err = %e, "message type ({}) not supported", mt);
        return;
      }
    };

    match msg_type {
      MessageType::Compound => self.handle_compound(buf, from, timestamp).await,
      MessageType::Compress => self.handle_compressed(buf, from, timestamp).await,
      MessageType::Ping => match decode_u32_from_buf(&mut buf) {
        Ok((len, _)) => self.handle_ping(buf.split_to(len as usize), from).await,
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decode ping")
        }
      },
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
            tracing::warn!(target = "showbiz", addr = %from, "handler queue full, dropping message ({})", msg_type);
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
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to notify of pending message");
        }
      }

      mt => {
        tracing::error!(target = "showbiz", addr = %from, err = "unexpected message type", message_type=%mt);
      }
    }
  }

  async fn handle_compound(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    // Decode the parts
    if !buf.has_remaining() {
      tracing::error!(target = "showbiz", addr = %from, err = %CompoundError::MissingLengthByte, "failed to decode compound request");
      return;
    }

    let num_parts = buf.get_u8() as usize;

    // check we have enough bytes
    if buf.len() < num_parts * 2 {
      tracing::error!(target = "showbiz", addr = %from, err = %CompoundError::Truncated, "failed to decode compound request");
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
      tracing::warn!(target = "showbiz", addr = %from, err = %CompoundError::TruncatedMsgs(num), "failed to decode compound request");
    }
  }

  #[inline]
  async fn handle_compressed(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    // Try to decode the payload
    if !self.inner.opts.compression_algo.is_none() {
      let algo = match CompressionAlgo::try_from(buf.get_u8()) {
        Ok(algo) => algo,
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decode compression algorithm");
          return;
        }
      };
      let size = buf.get_u32() as usize;
      match decompress_payload(algo, &buf) {
        Ok(payload) => self.handle_command(payload.into(), from, timestamp).await,
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decompress payload");
        }
      }
    } else {
      self.handle_command(buf, from, timestamp).await
    }
  }

  async fn handle_ping(&self, buf: Bytes, from: SocketAddr) {
    // Decode the ping
    let p = match Ping::decode_from(buf) {
      Ok(ping) => ping,
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decode ping request");
        return;
      }
    };

    // If node is provided, verify that it is for us
    if let Some(target) = &p.target {
      if target != &self.inner.id {
        tracing::error!(target = "showbiz", addr = %from, "got ping for unexpected node '{}'", target);
        return;
      }
    }

    let msg = if let Some(delegate) = &self.inner.delegate {
      let payload = match delegate.ack_payload().await {
        Ok(payload) => payload,
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to get ack payload from delegate");
          return;
        }
      };
      let mut out = BytesMut::with_capacity(
        MessageType::SIZE
          + AckResponseEncoder::<BytesMut, T::Checksumer>::encoded_len(p.seq_no, &payload),
      );
      out.put_u8(MessageType::AckResponse as u8);
      let mut enc = AckResponseEncoder::<_, T::Checksumer>::new(&mut out);
      enc.encode_seq_no(p.seq_no);
      enc.encode_payload(&payload);
      Message(out)
    } else {
      let ack = AckResponse::empty(p.seq_no);
      let mut out = BytesMut::with_capacity(MessageType::SIZE + ack.encoded_len());
      out.put_u8(MessageType::AckResponse as u8);
      ack.encode_to::<T::Checksumer>(&mut out);
      Message(out)
    };

    if let Err(e) = self.send_msg(&p.source, msg).await {
      tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to send ack response");
    }
  }

  async fn handle_indirect_ping(&self, mut buf: Bytes, from: SocketAddr) {
    let len = match IndirectPing::decode_len(&mut buf) {
      Ok(len) => len,
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decode indirect ping request");
        return;
      }
    };
    let ind = match IndirectPing::decode_from(buf.split_to(len)) {
      Ok(ind) => ind,
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decode indirect ping request");
        return;
      }
    };

    // Send a ping to the correct host.
    let local_seq_no = self.next_seq_no();

    let ping = Ping {
      seq_no: local_seq_no,
      source: self.inner.id.clone(),
      target: ind.ping.target.clone(),
    };

    // Forward the ack back to the requestor. If the request encodes an origin
    // use that otherwise assume that the other end of the UDP socket is
    // usable.

    let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
    // Setup a response handler to relay the ack
    let this = self.clone();
    let ind_source = ind.ping.source.clone();
    let ind_seq_no = ind.ping.seq_no;

    self
      .set_ack_handler(
        local_seq_no,
        self.inner.opts.probe_timeout,
        move |_payload, _timestamp| {
          async move {
            let _ = cancel_tx.send(());

            // Try to prevent the nack if we've caught it in time.
            let ack = AckResponse::empty(ind_seq_no);
            let mut out = BytesMut::with_capacity(MessageType::SIZE + ack.encoded_len());
            out.put_u8(MessageType::AckResponse as u8);
            ack.encode_to::<T::Checksumer>(&mut out);
            if let Err(e) = this.send_msg(&ind_source, Message(out)).await {
              tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to forward ack");
            }
          }
          .boxed()
        },
      )
      .await;

    let spawner = self.inner.spawner;

    let mut out = BytesMut::with_capacity(MessageType::SIZE + ping.encoded_len());
    out.put_u8(MessageType::Ping as u8);
    ping.encode_to(&mut out);

    // TODO: change ind ping struct encoding
    if let Err(e) = self.send_msg(&ind.ping.target.unwrap(), Message(out)).await {
      tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to send ping");
    }

    // Setup a timer to fire off a nack if no ack is seen in time.
    if ind.nack {
      let this = self.clone();
      let probe_timeout = self.inner.opts.probe_timeout;
      spawner.spawn(
        async move {
          let timer = Delay::new(probe_timeout);
          futures_util::select! {
            _ = timer.fuse() => {
              // We've not received an ack, so send a nack.
              let nack = NackResponse::new(ind.ping.seq_no);
              let mut out = BytesMut::with_capacity(MessageType::SIZE + nack.encoded_len());
              out.put_u8(MessageType::NackResponse as u8);
              nack.encode_to::<T::Checksumer>(&mut out);
              let addr = ind.ping.source;
              if let Err(e) = this.send_msg(&addr, Message(out)).await {
                tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to send nack");
              }
            }
            _ = cancel_rx.fuse() => {
              // We've received an ack, so we can cancel the nack.
            }
          }
        }
        .boxed(),
      );
    }
  }

  async fn handle_ack(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    match decode_u32_from_buf(&mut buf) {
      Ok((size, _)) => {
        buf.truncate(size as usize);
        match AckResponse::decode_from::<T::Checksumer>(buf) {
          Ok(ack) => self.invoke_ack_handler(ack, timestamp).await,
          Err(e) => {
            tracing::error!(target = "showbiz", addr = %from, err=%e, "failed to decode ack response");
          }
        }
      }
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err=%e, "failed to decode ack response");
      }
    }
  }

  async fn handle_nack(&self, mut buf: Bytes, from: SocketAddr) {
    match decode_u32_from_buf(&mut buf) {
      Ok((size, _)) => {
        buf.truncate(size as usize);
        match NackResponse::decode_from::<T::Checksumer>(buf) {
          Ok(nack) => self.invoke_nack_handler(nack).await,
          Err(e) => {
            tracing::error!(target = "showbiz", addr = %from, err=%e, "failed to decode nack response");
          }
        }
      }
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err=%e, "failed to decode nack response");
      }
    }
  }

  pub(crate) async fn send_msg(&self, addr: &NodeId, msg: Message) -> Result<(), Error<D, T>> {
    // Check if we can piggy back any messages
    let bytes_avail = self.inner.opts.packet_buffer_size
      - msg.len()
      - COMPOUND_HEADER_OVERHEAD
      - Self::label_overhead(&self.inner.opts.label);

    let mut msgs = self
      .get_broadcast_with_prepend(vec![msg], COMPOUND_OVERHEAD, bytes_avail)
      .await?;

    // Fast path if nothing to piggypack
    if msgs.len() == 1 {
      return self.raw_send_msg_packet(&addr, msgs.pop().unwrap().0).await;
    }

    // Create a compound message
    let compound = Message::compound(msgs);

    // Send the message
    self.raw_send_msg_packet(addr, compound).await
  }

  /// Used to send message via packet to another host without
  /// modification, other than compression or encryption if enabled.
  pub(crate) async fn raw_send_msg_packet(
    &self,
    addr: &NodeId,
    msg: BytesMut,
  ) -> Result<(), Error<D, T>> {
    macro_rules! encrypt_bail {
      ($msg: ident.len($compressed_msg_len: ident) -> $this:ident.$node:ident.$addr:ident -> $block: expr) => {{
        let basic_encrypt_len = MessageType::SIZE // MessageType::Encryption
        + EncryptionAlgo::SIZE // Encryption algo length
        + encrypted_length(self.inner.opts.encryption_algo, $compressed_msg_len);
        let encrypted_msg_len = encoded_u32_len(basic_encrypt_len as u32) + basic_encrypt_len;

        let mut buf = BytesMut::with_capacity(encrypted_msg_len);
        buf.put_u8(MessageType::Encrypt as u8);
        encode_u32_to_buf(&mut buf, encrypted_msg_len as u32);
        let offset = buf.len();
        buf.put_u8(self.inner.opts.encryption_algo as u8);
        // Add a random nonce
        let mut nonce = [0u8; NONCE_SIZE];
        rand::thread_rng().fill(&mut nonce);
        buf.put_slice(&nonce);
        let after_nonce = buf.len();

        $block(&mut buf);

        if $this.inner.opts.encryption_algo == EncryptionAlgo::PKCS7 {
          let buf_len = buf.len();
          pkcs7encode(&mut buf, buf_len, offset + EncryptionAlgo::SIZE + NONCE_SIZE, BLOCK_SIZE);
        }

        let keyring = $this.inner.keyring.as_ref().unwrap();
        let mut bytes = buf.split_off(after_nonce);
        let Some(pk) = keyring.lock().await.primary_key() else {
          let err = Error::Security(SecurityError::MissingPrimaryKey);
          tracing::error!(target = "showbiz", addr = %$addr, err = %err, "failed to encrypt message");
          return Err(err);
        };

        if let Err(e) = keyring.encrypt_to(pk, &nonce, &self.inner.opts.label, &mut bytes)
          .map(|_| {
            buf.unsplit(bytes);
          }) {
          tracing::error!(target = "showbiz", addr = %$addr, err = %e, "failed to encrypt message");
          return Err(Error::Security(e));
        }
        buf
      }};
    }

    macro_rules! return_bail {
      ($this:ident, $buf: ident, $addr: ident) => {{
        #[cfg(feature = "metrics")]
        {
          incr_udp_sent_counter($buf.len() as u64, self.inner.metrics_labels.iter());
        }

        $this
          .inner
          .transport
          .write_to_address(&$buf, &$addr)
          .await
          .map(|_| ())
          .map_err(Error::transport)
      }};
    }

    if addr.name().is_empty() && self.inner.opts.require_node_names {
      return Err(Error::MissingNodeName);
    }

    // Check if we have compression enabled
    if !self.inner.opts.compression_algo.is_none() {
      let data = compress_payload(self.inner.opts.compression_algo, &msg).map_err(|e| {
        tracing::error!(target = "showbiz", addr = %addr, err = %e, "failed to compress message");
        Error::Compression(e)
      })?;

      let compressed_msg_len = CompressEncoder::<BytesMut, T::Checksumer>::encoded_len(&data) + 1;
      if !self.inner.opts.encryption_algo.is_none() && self.inner.opts.gossip_verify_outgoing {
        let buf = encrypt_bail!(data.len(compressed_msg_len) -> self.node.addr -> |mut buf: &mut BytesMut| {
          buf.put_u8(MessageType::Compress as u8);
          encode_u32_to_buf(&mut buf, compressed_msg_len as u32);
          let mut enc = CompressEncoder::<_, T::Checksumer>::new(buf);
          enc.encode_algo(self.inner.opts.compression_algo);
          enc.encode_payload(&data);
        });

        return return_bail!(self, buf, addr);
      } else {
        let compressed_msg_len = CompressEncoder::<BytesMut, T::Checksumer>::encoded_len(&data) + 1;
        let mut buf = BytesMut::with_capacity(compressed_msg_len);
        buf.put_u8(MessageType::Compress as u8);
        encode_u32_to_buf(&mut buf, compressed_msg_len as u32);
        let mut enc = CompressEncoder::<_, T::Checksumer>::new(&mut buf);
        enc.encode_algo(self.inner.opts.compression_algo);
        enc.encode_payload(&data);
        return return_bail!(self, buf, addr);
      }
    }

    // Check if encryption is enabled
    if !self.inner.opts.encryption_algo.is_none() && self.inner.opts.gossip_verify_outgoing {
      let len = msg.len();
      let buf = encrypt_bail!(msg.len(len) -> self.node.addr -> |buf: &mut BytesMut| {
        buf.put_slice(&msg);
      });

      return return_bail!(self, buf, addr);
    }

    let msg_len = msg.len();
    let mut hasher = <T::Checksumer as Checksumer>::new();
    hasher.update(&msg);
    let crc = hasher.finalize();
    let msg_encoded_len = encoded_u32_len(msg_len as u32) + msg_len + 1;
    let mut buf = BytesMut::with_capacity(msg_encoded_len);
    buf.put_u8(MessageType::HasCrc as u8);
    encode_u32_to_buf(&mut buf, msg_len as u32);
    buf.put_slice(&msg);
    buf.put_u32(crc);
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