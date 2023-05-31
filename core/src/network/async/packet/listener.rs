use std::{sync::Arc, time::Instant};

use crate::{
  security::{decrypt_payload, pkcs7encode, BLOCK_SIZE, NONCE_SIZE},
  showbiz::MessageHandoff,
  types::{Message, Node, NodeId},
  util::decompress_payload,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use rand::Rng;

use super::*;

impl<T, D> Showbiz<T, D>
where
  T: Transport,
  D: Delegate,
{
  pub(crate) fn packet_listener<R, S>(&self, spawner: S)
  where
    R: Send + Sync + 'static,
    S: Fn(BoxFuture<'static, ()>) -> R + Copy + Send + Sync + 'static,
  {
    let this = self.clone();
    let shutdown_rx = this.inner.shutdown_rx.clone();
    let transport_rx = this.inner.transport.packet().clone();
    (spawner)(Box::pin(async move {
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
      let crc = crc32fast::hash(&buf[meta_size..]);
      let expected_crc = u32::from_be_bytes(buf[MessageType::SIZE..meta_size].try_into().unwrap());
      if crc != expected_crc {
        tracing::warn!(target = "showbiz", addr = %addr, "got invalid checksum for UDP packet: {} vs. {}", crc, expected_crc);
        return;
      }
      buf.advance(meta_size);
      self.handle_command(buf.freeze(), addr, timestamp).await
    } else {
      self.handle_command(buf.freeze(), addr, timestamp).await
    }
  }

  #[async_recursion::async_recursion]
  async fn handle_command(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    if buf.len() < 1 {
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
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decode ping");
          return;
        }
      },
      MessageType::IndirectPing => {}
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
    if buf.len() < 1 {
      tracing::error!(target = "showbiz", addr = %from, err = %CompoundError::MissingLengthByte, "failed to decode compound request");
      return;
    }

    let num_parts = buf.get_u8() as usize;

    // check we have enough bytes
    if buf.len() < num_parts as usize * 2 {
      tracing::error!(target = "showbiz", addr = %from, err = %CompoundError::Truncated, "failed to decode compound request");
      return;
    }

    // Decode the lengths
    let mut trunc = 0usize;
    let mut lengths = buf.split_to(num_parts * 2);
    let mut futs = (0..num_parts)
      .filter_map(|_| {
        let len = lengths.get_u16();
        if buf.len() < len as usize {
          trunc += 1;
          return None;
        }

        let msg = buf.split_to(len as usize);
        Some(self.handle_command(msg, from, timestamp))
      })
      .collect::<FuturesUnordered<_>>();
    if trunc > 0 {
      let num = num_parts - trunc;
      tracing::warn!(target = "showbiz", addr = %from, err = %CompoundError::TruncatedMsgs(num), "failed to decode compound request");
    }
    while let Some(_) = futs.next().await {}
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
      // TODO: add new method to delegate e.g. read_ack_payload_to_buf, to avoid copying and allocating
      let payload = match delegate.ack_payload().await {
        Ok(payload) => payload,
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to get ack payload from delegate");
          return;
        }
      };
      let mut out = BytesMut::with_capacity(
        MessageType::SIZE + AckResponseEncoder::<BytesMut>::encoded_len(&payload),
      );
      out.put_u8(MessageType::AckResponse as u8);
      let mut enc = AckResponseEncoder::new(&mut out);
      enc.encode_seq_no(p.seq_no);
      enc.encode_payload(&payload);
      Message(out)
    } else {
      let ack = AckResponse::empty(p.seq_no);
      let mut out = BytesMut::with_capacity(MessageType::SIZE + ack.encoded_len());
      out.put_u8(MessageType::AckResponse as u8);
      ack.encode_to(&mut out);
      Message(out)
    };

    if let Err(e) = self.send_msg(p.source, msg).await {
      tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to send ack response");
    }
  }

  async fn handle_indirect_ping(&self, buf: Bytes, from: SocketAddr) {
    todo!()
  }

  async fn handle_ack(&self, mut buf: Bytes, from: SocketAddr, timestamp: Instant) {
    let size = buf.get_u32() as usize;
    if size > buf.remaining() {
      tracing::error!(target = "showbiz", addr = %from, err="truncated ack message", "failed to decode ack response");
      return;
    }
    let seq_no = buf.get_u32();
    let ack = if buf.remaining() == size - 4 {
      AckResponse {
        seq_no,
        payload: buf,
      }
    } else {
      AckResponse {
        seq_no,
        payload: buf.slice(..size),
      }
    };
    self.invoke_ack_handler(ack, timestamp).await
  }

  async fn handle_nack(&self, mut buf: Bytes, from: SocketAddr) {
    if buf.remaining() < core::mem::size_of::<u32>() {
      tracing::error!(target = "showbiz", addr = %from, err="truncated nack message", "failed to decode nack response");
      return;
    }
    let seq_no = buf.get_u32();
    let nack = NackResponse { seq_no };
    self.invoke_nack_handler(nack).await
  }

  async fn send_msg(&self, addr: NodeId, msg: Message) -> Result<(), Error<T, D>> {
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
      return self
        .raw_send_msg_packet(addr, None, msgs.pop().unwrap().0)
        .await;
    }

    // Create a compound message
    let compound = Message::compound(msgs);

    // Send the message
    self.raw_send_msg_packet(addr, None, compound).await
  }

  /// Used to send message via packet to another host without
  /// modification, other than compression or encryption if enabled.
  pub(crate) async fn raw_send_msg_packet(
    &self,
    addr: NodeId,
    mut node: Option<Arc<Node>>,
    msg: BytesMut,
  ) -> Result<(), Error<T, D>> {
    macro_rules! crc_bail {
      ($node: ident, $data:ident, $msg_len: ident) => {
        match $node {
          Some(node) => {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[
              MessageType::Compress as u8,
              ($msg_len >> 24) as u8,
              ($msg_len >> 16) as u8,
              ($msg_len >> 8) as u8,
              $msg_len as u8,
              self.inner.opts.compression_algo as u8,
            ]);
            hasher.update(&$data);
            let crc = hasher.finalize();
            // compressed msg len should add the crc msg len
            $msg_len += MessageType::SIZE // MessageType::HasCrc
              + core::mem::size_of::<u32>(); // crc
            Some(crc)
          },
          _ => None,
        }
      };
      ($crc: ident, $buf: ident) => {
        if let Some(crc) = $crc {
          $buf.put_u8(MessageType::HasCrc as u8);
          $buf.put_u32(crc);
        }
      }
    }

    macro_rules! encrypt_bail {
      ($msg: ident -> $this:ident.$node:ident.$addr:ident -> $block: expr) => {{
        // Try to look up the destination node. Note this will only work if the
        // bare ip address is used as the node name, which is not guaranteed.
        if $node.is_none() {
          $node = $this.inner.nodes.read().await.node_map.get(&addr).map(|ls| ls.node.clone());
        }

        // Add a CRC to the end of the payload
        let mut after_crc_msg_len = $msg.len();
        let crc = crc_bail!($node, $msg, after_crc_msg_len);

        let encrypted_msg_len = MessageType::SIZE // MessageType::Encryption
        + core::mem::size_of::<u32>() // Encrypted message length
        + EncryptionAlgo::SIZE // Encryption algo length
        + encrypted_length(self.inner.opts.encryption_algo, after_crc_msg_len);

        let mut buf = BytesMut::with_capacity(encrypted_msg_len);
        buf.put_u8(MessageType::Encrypt as u8);
        buf.put_u32(encrypted_msg_len as u32);
        let offset = buf.len();
        buf.put_u8(self.inner.opts.encryption_algo as u8);
        // Add a random nonce
        let mut nonce = [0u8; NONCE_SIZE];
        rand::thread_rng().fill(&mut nonce);
        buf.put_slice(&nonce);
        let after_nonce = buf.len();
        crc_bail!(crc, buf);

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
      ($this:ident, $buf: ident, $addr: ident) => {
        $this
          .inner
          .transport
          .write_to_address(&$buf, &$addr)
          .await
          .map(|_| ())
          .map_err(Error::transport)
      };
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
      let compressed_msg_len = MessageType::SIZE // MessageType::Compression
        + core::mem::size_of::<u32>() // Compressed message length
        + CompressionAlgo::SIZE // CompressionAlgo length
        + data.len(); // data length

      if !self.inner.opts.encryption_algo.is_none() && self.inner.opts.gossip_verify_outgoing {
        let buf = encrypt_bail!(data -> self.node.addr -> |buf: &mut BytesMut| {
          buf.put_u8(MessageType::Compress as u8);
          buf.put_u32((data.len() + CompressionAlgo::SIZE) as u32);
          buf.put_u8(self.inner.opts.compression_algo as u8);
          buf.put_slice(&data);
        });

        return return_bail!(self, buf, addr);
      } else {
        // Add a CRC to the end of the payload
        let mut after_crc_msg_len = compressed_msg_len;
        let crc = crc_bail!(node, data, after_crc_msg_len);
        let mut buf = BytesMut::with_capacity(after_crc_msg_len);
        crc_bail!(crc, buf);
        buf.put_u8(MessageType::Compress as u8);
        buf.put_u32((data.len() + CompressionAlgo::SIZE) as u32);
        buf.put_u8(self.inner.opts.compression_algo as u8);
        buf.put_slice(&data);

        return return_bail!(self, buf, addr);
      }
    }

    // Check if encryption is enabled
    if !self.inner.opts.encryption_algo.is_none() && self.inner.opts.gossip_verify_outgoing {
      let buf = encrypt_bail!(msg -> self.node.addr -> |buf: &mut BytesMut| {
        buf.put_slice(&msg);
      });

      return return_bail!(self, buf, addr);
    }

    // Add a CRC to the end of the payload
    let mut msg_len = msg.len();
    match crc_bail!(node, msg, msg_len) {
      Some(crc) => {
        let mut buf = BytesMut::with_capacity(msg_len);
        buf.put_u8(MessageType::HasCrc as u8);
        buf.put_u32(crc);
        buf.put_slice(&msg);
        return_bail!(self, buf, addr)
      }
      None => return_bail!(self, msg, addr),
    }
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
