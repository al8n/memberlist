#![allow(missing_docs)]

use std::{future::Future, net::SocketAddr};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::Stream;
use memberlist_core::{
  tests::AnyError,
  transport::tests::{AddressKind, TestPacketClient, TestPromisedClient},
  types::Message,
};
use memberlist_utils::{Label, LabelBufMutExt};
use nodecraft::Transformable;
use smol_str::SmolStr;

use crate::{
  security::{EncryptionAlgo, SecretKey, SecretKeyring},
  Checksumer, Compressor, Listener, StreamLayer,
};

/// Unit test for handling [`Ping`] message
pub mod handle_ping;

/// Unit test for handling compound ping message
pub mod handle_compound_ping;

/// Unit test for handling indirect ping message
pub mod handle_indirect_ping;

/// Unit test for handling ping from wrong node
pub mod handle_ping_wrong_node;

/// Unit test for handling send packet with piggyback
pub mod packet_piggyback;

/// Unit test for handling transport with label or not.
pub mod label;

/// Unit test for handling promised ping
pub mod promised_ping;

/// Unit test for handling promised push pull
pub mod promised_push_pull;

/// A test client for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct NetTransporTestClient<R: Runtime> {
  #[viewit(getter(skip), setter(skip))]
  socket: <R::Net as Net>::UdpSocket,
  #[viewit(getter(skip), setter(skip))]
  local_addr: SocketAddr,
  checksumer: Checksumer,
  label: Label,
  send_label: bool,
  send_compressed: Option<Compressor>,
  send_encrypted: Option<(EncryptionAlgo, SecretKey)>,
  receive_verify_label: bool,
  receive_compressed: bool,
  receive_encrypted: Option<SecretKey>,
}

impl<R: Runtime> NetTransporTestClient<R> {
  /// Creates a new test client with the given address
  pub async fn new(addr: SocketAddr) -> Result<Self, AnyError> {
    let socket = <<R::Net as Net>::UdpSocket as UdpSocket>::bind(addr).await?;
    Ok(Self {
      local_addr: socket.local_addr()?,
      socket,
      label: Label::empty(),
      send_label: false,
      checksumer: Checksumer::Crc32,
      send_compressed: None,
      send_encrypted: None,
      receive_verify_label: false,
      receive_compressed: false,
      receive_encrypted: None,
    })
  }
}

impl<R: Runtime> TestPacketClient for NetTransporTestClient<R> {
  async fn send_to(&mut self, addr: &SocketAddr, src: &[u8]) -> Result<(), AnyError> {
    let mut out = BytesMut::new();
    if self.send_label {
      out.add_label_header(&self.label);
    }

    let mut data = BytesMut::new();
    data.put_u8(self.checksumer as u8);
    // put checksum placeholder
    data.put_u32(0);

    if let Some(compressor) = self.send_compressed {
      data.put_u8(compressor as u8);
      let compressed = compressor.compress_into_bytes(src)?;
      let cur = data.len();
      // put compressed data length placeholder
      data.put_u32(0);
      NetworkEndian::write_u32(&mut data[cur..], compressed.len() as u32);
      data.put_slice(&compressed);
    } else {
      data.put_slice(src);
    }

    let checksum = self.checksumer.checksum(&data[5..]);
    NetworkEndian::write_u32(&mut data[1..], checksum);

    if let Some((algo, pk)) = &self.send_encrypted {
      let kr = SecretKeyring::new(*pk);
      out.put_u8(*algo as u8);
      let cur = out.len();
      out.put_u32(0); // put encrypted data length placeholder
      let nonce_offset = out.len();
      let nonce = kr.write_header(&mut out);
      let data_offset = out.len();
      out.put_slice(&data);
      let mut dst = out.split_off(data_offset);
      kr.encrypt(*algo, *pk, nonce, self.label.as_bytes(), &mut dst)?;
      out.unsplit(dst);
      let encrypted_data_len = (out.len() - nonce_offset) as u32;
      NetworkEndian::write_u32(&mut out[cur..], encrypted_data_len);
    } else {
      out.put_slice(&data);
    }
    self.socket.send_to(&out, addr).await?;
    Ok(())
  }

  async fn recv_from(&mut self) -> Result<(Bytes, SocketAddr), AnyError> {
    let mut in_ = vec![0; 1500];
    let (n, addr) = self.socket.recv_from(&mut in_).await?;
    in_.truncate(n);
    let mut src: Bytes = in_.into();
    if self.receive_verify_label {
      let received_label = read_label(&src)?;
      assert_eq!(received_label, self.label);
      src.advance(received_label.encoded_overhead());
    }

    let mut unencrypted = if let Some(pk) = self.receive_encrypted {
      read_encrypted_data(pk, self.label.as_bytes(), &src)?
    } else {
      src
    };

    verify_checksum(&unencrypted)?;
    unencrypted.advance(5);

    let uncompressed = if self.receive_compressed {
      read_compressed_data(&unencrypted)?.into()
    } else {
      unencrypted
    };
    Ok((uncompressed, addr))
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
}

/// A test client for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct NetTransporTestPromisedClient<S: StreamLayer> {
  ln: S::Listener,
  layer: S,
  local_addr: SocketAddr,
}

impl<S: StreamLayer> NetTransporTestPromisedClient<S> {
  /// Creates a new test client with the given address
  pub fn new(addr: SocketAddr, layer: S, ln: S::Listener) -> Self {
    Self {
      local_addr: addr,
      layer,
      ln,
    }
  }
}

impl<S: StreamLayer> TestPromisedClient for NetTransporTestPromisedClient<S> {
  type Stream = S::Stream;

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, AnyError> {
    self.layer.connect(addr).await.map_err(Into::into)
  }

  async fn accept(&self) -> Result<(Self::Stream, SocketAddr), AnyError> {
    let (stream, addr) = self.ln.accept().await?;
    Ok((stream, addr))
  }

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }
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

fn compound_encoder(msgs: &[Message<SmolStr, SocketAddr>]) -> Result<Bytes, AnyError> {
  let num_msgs = msgs.len() as u8;
  let total_bytes = 2 + msgs.iter().map(|m| m.encoded_len() + 2).sum::<usize>();
  let mut out = BytesMut::with_capacity(total_bytes);
  out.put_u8(Message::<SmolStr, SocketAddr>::COMPOUND_TAG);
  out.put_u8(num_msgs);

  let mut cur = out.len();
  out.resize(total_bytes, 0);

  for msg in msgs {
    let len = msg.encoded_len() as u16;
    NetworkEndian::write_u16(&mut out[cur..], len);
    cur += 2;
    let len = msg.encode(&mut out[cur..])?;
    cur += len;
  }

  Ok(out.freeze())
}
