use std::{sync::Arc, time::Instant};

use crate::{
  security::decrypt_payload,
  showbiz::MessageHandoff,
  types::{Message, Node, NodeId},
  util::decompress_payload,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use prost::Message as _;

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
    let msg_type = match MessageType::try_from(buf[0]) {
      Ok(msg_type) => msg_type,
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err = %e, "message type ({}) not supported", buf[0]);
        return;
      }
    };
    buf.advance(1);

    match msg_type {
      MessageType::Compound => self.handle_compound(buf, from, timestamp).await,
      MessageType::Compress => self.handle_compressed(buf, from, timestamp).await,
      MessageType::Ping => self.handle_ping(buf, from).await,
      MessageType::IndirectPing => {}
      MessageType::AckResponse => {}
      MessageType::NackResponse => {}
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
  async fn handle_compressed(&self, buf: Bytes, from: SocketAddr, timestamp: Instant) {
    // Try to decode the payload
    if !self.inner.opts.compression_algo.is_none() {
      match decompress_payload(self.inner.opts.compression_algo, &buf) {
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
    let p = match Ping::decode(buf) {
      Ok(ping) => ping,
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to decode ping request");
        return;
      }
    };

    // If node is provided, verify that it is for us
    if !p.node.is_empty() && p.node != self.inner.opts.name {
      tracing::error!(target = "showbiz", addr = %from, "got ping for unexpected node '{}'", p.node.as_ref());
      return;
    }

    let ack = if let Some(delegate) = &self.inner.delegate {
      let payload = match delegate.ack_payload().await {
        Ok(payload) => payload,
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to get ack payload from delegate");
          return;
        }
      };
      AckResponse::new(p.seq_no, payload)
    } else {
      AckResponse::new(p.seq_no, Bytes::new())
    };

    let msg = match Message::encode(&ack, MessageType::AckResponse) {
      Ok(msg) => msg,
      Err(e) => {
        tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to encode ack response");
        return;
      }
    };

    if let Err(e) = self.send_msg(p.source, msg).await {
      tracing::error!(target = "showbiz", addr = %from, err = %e, "failed to send ack response");
    }
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
        .raw_send_msg_packet(addr, None, msgs.pop().unwrap().freeze())
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
    node: Option<Arc<Node>>,
    mut msg: Bytes,
  ) -> Result<(), Error<T, D>> {
    if addr.name().is_empty() && self.inner.opts.require_node_names {
      return Err(Error::MissingNodeName);
    }

    // Check if we have compression enabled
    if !self.inner.opts.compression_algo.is_none() {
      let c = Compress {
        algo: self.inner.opts.compression_algo,
        buf: compress_payload(self.inner.opts.compression_algo, &msg).map(Into::into).map_err(|e| {
          tracing::error!(target = "showbiz", addr = %addr, err = %e, "failed to compress message");
          Error::Compression(e)
        })?,
      };
      msg = match Message::encode(&c, MessageType::Compress) {
        Ok(msg) => msg.0.freeze(),
        Err(e) => {
          tracing::error!(target = "showbiz", addr = %addr, err = %e, "failed to encode compress message");
          return Err(Error::Encode(e));
        }
      };
    }

    // Try to look up the destination node. Note this will only work if the
    // bare ip address is used as the node name, which is not guaranteed.
    if node.is_none() {}

    // Check if encryption is enabled
    // if let Some(keyring) = &self.inner.keyring {
    //   if self.inner.opts.gossip_verify_outgoing {
    //     let mu = keyring.lock().await;
    //     match mu.primary_key() {
    //       Some(pk) => {
    //         keyring.encrypt_payload(self.inner.opts.encryption_algo, msg, data, dst)
    //       },
    //       None => {},
    //     }
    //   }
    // }

    self
      .inner
      .transport
      .write_to_address(&msg, &addr)
      .await
      .map(|_| ())
      .map_err(Error::transport)
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
