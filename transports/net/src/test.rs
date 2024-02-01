use std::{future::Future, net::SocketAddr, time::Duration};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{FutureExt, Stream};
use memberlist_core::{
  delegate::VoidDelegate,
  tests::{get_memberlist, AnyError},
  transport::{AddressResolver, Node, Wire},
  types::{Ack, Ping},
  Options,
};
use memberlist_utils::Label;
use smol_str::SmolStr;

use crate::{
  security::{EncryptionAlgo, SecretKey, SecretKeyring},
  Checksumer, Compressor, NetTransport, NetTransportOptions, StreamLayer,
};

async fn listen_udp<R>(addr: SocketAddr) -> Result<<R::Net as Net>::UdpSocket, std::io::Error>
where
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
{
  <<R::Net as Net>::UdpSocket as UdpSocket>::bind(addr).await
}

/// Unit test for handling [`Ping`] message
pub async fn handle_ping<A, S, R, PE, AD, W>(
  resolver: A,
  stream_layer: S,
  opts: NetTransportOptions<SmolStr, A>,
  ping_encoder: PE,
  ack_decoder: AD,
  client_addr: SocketAddr,
) -> Result<(), AnyError>
where
  A: AddressResolver<ResolvedAddress = SocketAddr, Runtime = R>,
  S: StreamLayer,
  R: Runtime,
  <R::Sleep as Future>::Output: Send,
  <R::Interval as Stream>::Item: Send,
  PE: FnOnce(Ping<SmolStr, SocketAddr>) -> Vec<u8>,
  AD: FnOnce(&[u8]) -> Result<Ack, AnyError>,
  W: Wire<Id = SmolStr, Address = SocketAddr>,
{
  let trans = NetTransport::<_, _, S, W>::new(resolver, stream_layer, opts).await?;
  let m = get_memberlist(trans, VoidDelegate::default(), Options::default()).await?;

  let udp = listen_udp::<R>(client_addr).await?;

  let udp_addr = udp.local_addr()?;

  // Encode a ping
  let ping = Ping {
    seq_no: 42,
    source: Node::new("test".into(), udp_addr),
    target: m.advertise_node(),
  };

  let buf = ping_encoder(ping);
  // Send
  udp.send_to(&buf, m.advertise_addr()).await?;

  // Wait for response
  let (tx, rx) = futures::channel::oneshot::channel();
  R::spawn_detach(async {
    futures::select! {
      _ = rx.fuse() => {},
      _ = R::sleep(Duration::from_secs(2)).fuse() => {
        panic!("timeout")
      }
    }
  });

  let mut in_ = vec![0; 1500];
  let (n, _) = udp.recv_from(&mut in_).await?;

  in_.truncate(n);
  let ack = ack_decoder(&in_)?;
  assert_eq!(ack.seq_no, 42, "bad sequence no: {}", ack.seq_no);
  tx.send(()).unwrap();
  Ok(())
}

/// A helper function to read label from the given source.
pub fn read_label(src: &[u8]) -> Result<Label, AnyError> {
  let tag = src[0];
  assert_eq!(tag, Label::TAG, "invalid label tag");
  let len = src[1] as usize;
  let label = &src[2..2 + len];
  Label::try_from(label).map_err(Into::into)
}

/// A helper function to verify data checksum from the given source.
pub fn verify_checksum(src: &[u8]) -> Result<(), AnyError> {
  let checksumer = Checksumer::try_from(src[0])?;
  let expected_checksum = NetworkEndian::read_u32(&src[1..]);
  let actual_checksum = checksumer.checksum(&src[5..]);
  assert_eq!(expected_checksum, actual_checksum, "checksum mismatch");
  Ok(())
}

/// A helper function to decompress data from the given source.
pub fn read_compressed_data(src: &[u8]) -> Result<Vec<u8>, AnyError> {
  let compressor = Compressor::try_from(src[0])?;
  let compressed_data_len = NetworkEndian::read_u32(&src[1..]) as usize;
  assert_eq!(
    compressed_data_len,
    src.len() - 5,
    "compressed data length mismatch"
  );
  compressor.decompress(&src[5..]).map_err(Into::into)
}

/// A helper function to decrypt data from the given source.
pub fn read_encrypted_data(
  pk: SecretKey,
  auth_data: &[u8],
  mut src: &[u8],
) -> Result<Bytes, AnyError> {
  let algo = EncryptionAlgo::try_from(src[0])?;
  let encrypted_data_len = NetworkEndian::read_u32(&src[1..]) as usize;
  assert_eq!(
    encrypted_data_len,
    src.len() - 5,
    "encrypted data length mismatch"
  );
  src = &src[5..];
  let mut buf = BytesMut::new();
  buf.put_slice(src);
  let kr = SecretKeyring::new(pk);
  let nonce = kr.read_nonce(&mut buf);
  kr.decrypt(pk, algo, nonce, auth_data, &mut buf)?;
  Ok(buf.freeze())
}
