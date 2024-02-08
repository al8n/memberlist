use std::{
  net::SocketAddr,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::Instant,
};

use agnostic::{
  net::{Net, UdpSocket as _},
  Runtime,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, Bytes, BytesMut};
use futures::FutureExt;
use memberlist_core::{
  transport::{stream::PacketProducer, Transport, Wire},
  types::{Message, Packet},
};
use memberlist_utils::{Label, LabelBufExt, OneOrMore};
use nodecraft::resolver::AddressResolver;
use wg::AsyncWaitGroup;

#[cfg(feature = "encryption")]
use super::security::*;


#[cfg(feature = "compression")]
use super::compressor::*;

use super::{Checksumer, NetTransportError, CHECKSUM_TAG, PACKET_OVERHEAD, PACKET_RECV_BUF_SIZE};

pub(super) struct PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
{
  pub(super) wg: AsyncWaitGroup,
  pub(super) packet_tx: PacketProducer<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  pub(super) socket: Arc<<<T::Runtime as Runtime>::Net as Net>::UdpSocket>,
  pub(super) local_addr: SocketAddr,
  pub(super) shutdown: Arc<AtomicBool>,
  pub(super) shutdown_rx: async_channel::Receiver<()>,
  pub(super) label: Label,
  #[cfg(feature = "encryption")]
  pub(super) encryptor: Option<super::security::SecretKeyring>,
  #[cfg(any(feature = "compression", feature = "encryption"))]
  pub(super) offload_size: usize,
  pub(super) skip_inbound_label_check: bool,
  #[cfg(feature = "encryption")]
  pub(super) verify_incoming: bool,
  #[cfg(feature = "metrics")]
  pub(super) metric_labels: std::sync::Arc<memberlist_utils::MetricLabels>,
}

impl<A, T> PacketProcessor<A, T>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = T::Runtime>,
  T: Transport<Resolver = A>,
{
  pub(super) fn run(self) {
    let Self {
      wg,
      packet_tx,
      socket,
      shutdown,
      local_addr,
      ..
    } = self;

    <T::Runtime as Runtime>::spawn_detach(async move {
      scopeguard::defer!(wg.done());
      tracing::info!(
        target = "memberlist.transport.net",
        "udp listening on {local_addr}"
      );

      loop {
        // Do a blocking read into a fresh buffer. Grab a time stamp as
        // close as possible to the I/O.
        let mut buf = BytesMut::new();
        buf.resize(PACKET_RECV_BUF_SIZE, 0);
        futures::select! {
          _ = self.shutdown_rx.recv().fuse() => {
            break;
          }
          rst = socket.recv_from(&mut buf).fuse() => {
            match rst {
              Ok((n, addr)) => {
                // Check the length - it needs to have at least one byte to be a
                // proper message.
                if n < 1 {
                  tracing::error!(target = "memberlist.packet", local=%local_addr, from=%addr, err = "UDP packet too short (0 bytes)");
                  continue;
                }
                buf.truncate(n);
                let start = Instant::now();
                let msg = match Self::handle_remote_bytes(
                  buf,
                  &self.label,
                  self.skip_inbound_label_check,
                  #[cfg(feature = "encryption")]
                  self.encryptor.as_ref(),
                  #[cfg(feature = "encryption")]
                  self.verify_incoming,
                  #[cfg(any(feature = "compression", feature = "encryption"))]
                  self.offload_size,
                ).await {
                  Ok(msg) => msg,
                  Err(e) => {
                    tracing::error!(target = "memberlist.packet", local=%local_addr, from=%addr, err = %e, "fail to handle UDP packet");
                    continue;
                  }
                };

                #[cfg(feature = "metrics")]
                {
                  metrics::counter!("memberlist.packet.bytes.processing", self.metric_labels.iter()).increment(start.elapsed().as_secs_f64().round() as u64);
                }

                if let Err(e) = packet_tx.send(Packet::new(msg, addr, start)).await {
                  tracing::error!(target = "memberlist.packet", local=%local_addr, from=%addr, err = %e, "failed to send packet");
                }

                #[cfg(feature = "metrics")]
                metrics::counter!("memberlist.packet.received", self.metric_labels.iter()).increment(n as u64);
              }
              Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                  break;
                }

                tracing::error!(target = "memberlist.transport.net", peer=%local_addr, err = %e, "error reading UDP packet");
                continue;
              }
            };
          }
        }
      }
    });
  }

  async fn handle_remote_bytes(
    mut buf: BytesMut,
    label: &Label,
    skip_inbound_label_check: bool,
    #[cfg(feature = "encryption")] encryptor: Option<&super::security::SecretKeyring>,
    #[cfg(feature = "encryption")] verify_incoming: bool,
    #[cfg(any(feature = "encryption", feature = "compression"))] offload_size: usize,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    let packet_label = buf.remove_label_header()?.unwrap_or_else(Label::empty);

    #[cfg(not(feature = "encryption"))]
    if !skip_inbound_label_check && packet_label.ne(label) {
      tracing::error!(target = "memberlist.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
      return Err(memberlist_utils::LabelError::mismatch(label.clone(), packet_label).into());
    }

    #[cfg(not(any(feature = "compression", feature = "encryption")))]
    return Self::read_from_packet_without_compression_and_encryption(buf);

    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    return Self::read_from_packet_with_compression_without_encryption(buf, offload_size).await;

    #[cfg(all(not(feature = "compression"), feature = "encryption"))]
    return Self::read_from_packet_with_encryption_without_compression(
      buf,
      encryptor,
      packet_label,
      label,
      skip_inbound_label_check,
      offload_size,
      verify_incoming,
    )
    .await;

    #[cfg(all(feature = "compression", feature = "encryption"))]
    Self::read_from_packet_with_compression_and_encryption(
      buf,
      encryptor,
      packet_label,
      label,
      skip_inbound_label_check,
      offload_size,
      verify_incoming,
    )
    .await
  }

  #[cfg(all(feature = "compression", feature = "encryption"))]
  async fn read_from_packet_with_compression_and_encryption(
    mut buf: BytesMut,
    encryptor: Option<&super::security::SecretKeyring>,
    mut packet_label: Label,
    label: &Label,
    skip_inbound_label_check: bool,
    offload_size: usize,
    verify_incoming: bool,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    use memberlist_utils::LabelError;
    use nodecraft::CheapClone;

    use crate::{COMPRESS_TAG, ENCRYPT_TAG, MAX_MESSAGE_LEN_SIZE};

    if !ENCRYPT_TAG.contains(&buf[0]) {
      if verify_incoming {
        tracing::error!(
          target = "memberlist.net.packet",
          "incoming packet is not encrypted, and verify incoming is forced"
        );
        return Err(super::security::SecurityError::Disabled.into());
      } else {
        if !skip_inbound_label_check && packet_label.ne(label) {
          tracing::error!(target = "memberlist.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
          return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
        }
        return Self::read_from_packet_with_compression_without_encryption(buf, offload_size).await;
      }
    }

    if skip_inbound_label_check {
      if !packet_label.is_empty() {
        tracing::error!(
          target = "memberlist.net.promised",
          "unexpected double stream label header"
        );
        return Err(LabelError::duplicate(label.cheap_clone(), packet_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      packet_label = label.cheap_clone();
    }

    if packet_label.ne(label) {
      tracing::error!(target = "memberlist.net.promised", local_label=%label, remote_label=%packet_label, "discarding stream with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
    }

    let algo = EncryptionAlgo::try_from(buf.get_u8())?;
    let encrypted_message_size = NetworkEndian::read_u32(&buf[..MAX_MESSAGE_LEN_SIZE]) as usize;
    buf.advance(MAX_MESSAGE_LEN_SIZE);
    let mut encrypted_message = buf.split_to(encrypted_message_size);

    let encryptor = match encryptor {
      Some(encryptor) => encryptor,
      None => {
        return Err(SecurityError::Disabled.into());
      }
    };
    let keys = encryptor.keys().await;
    if encrypted_message_size <= offload_size {
      Self::decrypt(
        encryptor,
        algo,
        keys,
        packet_label.as_bytes(),
        &mut encrypted_message,
      )?;
      return Self::read_from_packet_with_compression_without_encryption(
        encrypted_message,
        offload_size,
      )
      .await;
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let encryptor = encryptor.clone();

    rayon::spawn(move || {
      let then = |mut buf: BytesMut| {
        Self::read_and_check_checksum(&mut buf)?;

        if !COMPRESS_TAG.contains(&buf[0]) {
          return Self::read_from_packet_without_compression_and_encryption(buf);
        }

        let compressor = Compressor::try_from(buf.get_u8())?;
        let compressd_message_len = NetworkEndian::read_u32(&buf[..MAX_MESSAGE_LEN_SIZE]) as usize;
        buf.advance(MAX_MESSAGE_LEN_SIZE);
        if compressd_message_len > buf.len() {
          return Err(NetTransportError::not_enough_bytes_to_decompress());
        }

        Self::decompress_and_decode(compressor, buf)
      };
      if tx
        .send(
          Self::decrypt(
            &encryptor,
            algo,
            keys,
            packet_label.as_bytes(),
            &mut encrypted_message,
          )
          .and_then(|_| then(encrypted_message)),
        )
        .is_err()
      {
        tracing::error!(
          target = "memberlist.net.packet",
          "failed to send back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(msgs)) => Ok(msgs),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(all(not(feature = "compression"), feature = "encryption"))]
  async fn read_from_packet_with_encryption_without_compression(
    mut buf: BytesMut,
    encryptor: Option<&SecretKeyring>,
    mut packet_label: Label,
    label: &Label,
    skip_inbound_label_check: bool,
    offload_size: usize,
    verify_incoming: bool,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    use nodecraft::CheapClone;
    use memberlist_utils::LabelError;
    use super::{security, MAX_MESSAGE_LEN_SIZE};

    if !super::ENCRYPT_TAG.contains(&buf[0]) {
      if verify_incoming {
        tracing::error!(
          target = "memberlist.net.packet",
          "incoming packet is not encrypted, and verify incoming is forced"
        );
        return Err(security::SecurityError::Disabled.into());
      } else {
        if !skip_inbound_label_check && packet_label.ne(label) {
          tracing::error!(target = "memberlist.net.packet", local_label=%label, remote_label=%packet_label, "discarding packet with unacceptable label");
          return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
        }
        return Self::read_from_packet_without_compression_and_encryption(buf);
      }
    }

    if skip_inbound_label_check {
      if !packet_label.is_empty() {
        tracing::error!(
          target = "memberlist.net.promised",
          "unexpected double stream label header"
        );
        return Err(LabelError::duplicate(label.cheap_clone(), packet_label).into());
      }

      // Set this from config so that the auth data assertions work below.
      packet_label = label.cheap_clone();
    }

    if packet_label.ne(label) {
      tracing::error!(target = "memberlist.net.promised", local_label=%label, remote_label=%packet_label, "discarding stream with unacceptable label");
      return Err(LabelError::mismatch(label.cheap_clone(), packet_label).into());
    }

    let algo = EncryptionAlgo::try_from(buf.get_u8())?;
    let encrypted_message_size = NetworkEndian::read_u32(&buf[..MAX_MESSAGE_LEN_SIZE]) as usize;
    buf.advance(MAX_MESSAGE_LEN_SIZE);
    let mut encrypted_message = buf.split_to(encrypted_message_size);

    let encryptor = match encryptor {
      Some(encryptor) => encryptor,
      None => {
        return Err(security::SecurityError::Disabled.into());
      }
    };
    let keys = encryptor.keys().await;
    if encrypted_message_size <= offload_size {
      return Self::decrypt(
        encryptor,
        algo,
        keys,
        packet_label.as_bytes(),
        &mut encrypted_message,
      )
      .and_then(|_| Self::read_from_packet_without_compression_and_encryption(encrypted_message));
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    let encryptor = encryptor.clone();

    rayon::spawn(move || {
      if tx
        .send(
          Self::decrypt(
            &encryptor,
            algo,
            keys,
            packet_label.as_bytes(),
            &mut encrypted_message,
          )
          .and_then(|_| {
            Self::read_from_packet_without_compression_and_encryption(encrypted_message)
          }),
        )
        .is_err()
      {
        tracing::error!(
          target = "memberlist.net.packet",
          "failed to send back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(msgs)) => Ok(msgs),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  #[cfg(feature = "compression")]
  async fn read_from_packet_with_compression_without_encryption(
    mut buf: BytesMut,
    offload_size: usize,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    use crate::{COMPRESS_TAG, MAX_MESSAGE_LEN_SIZE};

    Self::read_and_check_checksum(&mut buf)?;
    if !COMPRESS_TAG.contains(&buf[0]) {
      return Self::read_from_packet_without_compression_and_encryption(buf);
    }

    let compressor = Compressor::try_from(buf.get_u8())?;
    let compressd_message_len = NetworkEndian::read_u32(&buf[..MAX_MESSAGE_LEN_SIZE]) as usize;
    buf.advance(MAX_MESSAGE_LEN_SIZE);
    if compressd_message_len > buf.len() {
      return Err(NetTransportError::not_enough_bytes_to_decompress());
    }
    buf.truncate(compressd_message_len);
    if buf.len() <= offload_size {
      return Self::decompress_and_decode(compressor, buf);
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    rayon::spawn(move || {
      if tx
        .send(Self::decompress_and_decode(compressor, buf))
        .is_err()
      {
        tracing::error!(
          target = "memberlist.net.packet",
          "failed to send back to main thread"
        );
      }
    });

    match rx.await {
      Ok(Ok(msgs)) => Ok(msgs),
      Ok(Err(e)) => Err(e),
      Err(_) => Err(NetTransportError::ComputationTaskFailed),
    }
  }

  fn read_from_packet_without_compression_and_encryption(
    mut buf: BytesMut,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    Self::read_and_check_checksum(&mut buf)?;
    Self::decode(buf.freeze())
  }

  fn read_and_check_checksum(
    buf: &mut BytesMut,
  ) -> Result<(), NetTransportError<T::Resolver, T::Wire>> {
    if !CHECKSUM_TAG.contains(&buf[0]) {
      return Ok(());
    }

    let checksumer = Checksumer::try_from(buf.get_u8())?;
    let expected = NetworkEndian::read_u32(&buf[..super::checksum::CHECKSUM_SIZE]);
    buf.advance(super::checksum::CHECKSUM_SIZE);
    let actual = checksumer.checksum(buf);
    if actual != expected {
      return Err(NetTransportError::PacketChecksumMismatch);
    }

    Ok(())
  }

  #[cfg(feature = "encryption")]
  fn decrypt(
    encryptor: &SecretKeyring,
    algo: EncryptionAlgo,
    keys: impl Iterator<Item = SecretKey>,
    auth_data: &[u8],
    data: &mut BytesMut,
  ) -> Result<(), NetTransportError<T::Resolver, T::Wire>> {
    let nonce = encryptor.read_nonce(data);
    for key in keys {
      match encryptor.decrypt(key, algo, nonce, auth_data, data) {
        Ok(_) => return Ok(()),
        Err(e) => {
          tracing::error!(
            target = "memberlist.net.promised",
            "failed to decrypt message: {}",
            e
          );
          continue;
        }
      }
    }
    Err(SecurityError::NoInstalledKeys.into())
  }

  #[cfg(feature = "compression")]
  fn decompress_and_decode(
    compressor: Compressor,
    buf: BytesMut,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    let buf: Bytes = compressor.decompress(&buf)?.into();
    Self::decode(buf)
  }

  fn decode(
    mut buf: Bytes,
  ) -> Result<
    OneOrMore<Message<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    NetTransportError<T::Resolver, T::Wire>,
  > {
    if buf[0] == Message::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::COMPOUND_TAG {
      buf.advance(1);
      let num_msgs = buf[0] as usize;
      buf.advance(1);
      let mut msgs = OneOrMore::with_capacity(num_msgs);
      while msgs.len() != num_msgs {
        let msg_len = NetworkEndian::read_u16(&buf[..PACKET_OVERHEAD]) as usize;
        buf.advance(PACKET_OVERHEAD);
        let msg_bytes = buf.split_to(msg_len);
        let (_, msg) =
          <T::Wire as Wire>::decode_message(&msg_bytes).map_err(NetTransportError::Wire)?;
        msgs.push(msg);
      }
      Ok(msgs)
    } else {
      <T::Wire as Wire>::decode_message(&buf)
        .map(|(_, msg)| msg.into())
        .map_err(NetTransportError::Wire)
    }
  }
}
