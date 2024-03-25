#![allow(missing_docs)]

use std::{net::SocketAddr, sync::Arc};

use agnostic::{
  net::{Net, UdpSocket},
  Runtime,
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use memberlist_core::{
  tests::AnyError,
  transport::tests::{
    AddressKind, TestPacketClient, TestPacketConnection, TestPacketStream, TestPromisedClient,
    TestPromisedConnection, TestPromisedStream,
  },
  types::{Label, LabelBufMutExt, Message},
};
use nodecraft::Transformable;
use smol_str::SmolStr;

use crate::{Checksumer, Listener, StreamLayer};

#[cfg(feature = "encryption")]
use crate::security::{EncryptionAlgo, SecretKey};

#[cfg(feature = "compression")]
use crate::compressor::Compressor;

pub use super::promised_processor::listener_backoff;

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

/// Unit test for handling gossip with mismatched keys
#[cfg(feature = "encryption")]
pub mod gossip_mismatch_keys;

/// Unit test for sending
pub mod send;

/// Unit test for joining
pub mod join;

/// Unit test for joining dead node
pub mod join_dead_node;

/// A test client stream for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct NetTransportTestPacketStream<R: Runtime> {
  #[viewit(getter(skip), setter(skip))]
  remote_addr: SocketAddr,
  #[viewit(getter(skip), setter(skip))]
  client: NetTransportTestClient<R>,
}

impl<R: Runtime> Clone for NetTransportTestPacketStream<R> {
  fn clone(&self) -> Self {
    Self {
      remote_addr: self.remote_addr,
      client: self.client.clone(),
    }
  }
}

impl<R: Runtime> TestPacketConnection for NetTransportTestPacketStream<R> {
  type Stream = Self;

  async fn accept(&self) -> Result<Self::Stream, AnyError> {
    Ok(self.clone())
  }

  async fn connect(&self) -> Result<Self::Stream, AnyError> {
    Ok(self.clone())
  }
}

impl<R: Runtime> TestPacketStream for NetTransportTestPacketStream<R> {
  async fn send_to(&mut self, src: &[u8]) -> Result<(), AnyError> {
    let mut out = BytesMut::new();
    if self.client.send_label {
      out.add_label_header(&self.client.label);
    }

    let mut data = BytesMut::new();
    data.put_u8(self.client.checksumer as u8);
    // put checksum placeholder
    data.put_u32(0);

    #[cfg(feature = "compression")]
    if let Some(compressor) = self.client.send_compressed {
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

    #[cfg(not(feature = "compression"))]
    data.put_slice(src);

    let checksum = self.client.checksumer.checksum(&data[5..]);
    NetworkEndian::write_u32(&mut data[1..], checksum);

    #[cfg(feature = "encryption")]
    if let Some((algo, pk)) = &self.client.send_encrypted {
      out.put_u8(*algo as u8);
      let cur = out.len();
      out.put_u32(0); // put encrypted data length placeholder
      let nonce_offset = out.len();
      let nonce = crate::security::write_header(&mut out);
      let data_offset = out.len();
      out.put_slice(&data);
      let mut dst = out.split_off(data_offset);
      crate::security::encrypt(*algo, *pk, nonce, self.client.label.as_bytes(), &mut dst)?;
      out.unsplit(dst);
      let encrypted_data_len = (out.len() - nonce_offset) as u32;
      NetworkEndian::write_u32(&mut out[cur..], encrypted_data_len);
    } else {
      out.put_slice(&data);
    }

    #[cfg(not(feature = "encryption"))]
    out.put_slice(&data);

    self.client.socket.send_to(&out, self.remote_addr).await?;
    Ok(())
  }

  async fn recv_from(&mut self) -> Result<(Bytes, SocketAddr), AnyError> {
    let mut in_ = vec![0; 1500];
    let (n, addr) = self.client.socket.recv_from(&mut in_).await?;
    in_.truncate(n);
    let mut src: Bytes = in_.into();
    if self.client.receive_verify_label {
      let received_label = read_label(&src)?;
      assert_eq!(received_label, self.client.label);
      src.advance(received_label.encoded_overhead());
    }

    #[cfg(feature = "encryption")]
    let mut unencrypted = if let Some(pk) = self.client.receive_encrypted {
      read_encrypted_data(pk, self.client.label.as_bytes(), &src)?
    } else {
      src
    };

    #[cfg(not(feature = "encryption"))]
    let mut unencrypted = src;

    verify_checksum(&unencrypted)?;
    unencrypted.advance(5);

    #[cfg(feature = "compression")]
    let uncompressed = if self.client.receive_compressed {
      read_compressed_data(&unencrypted)?.into()
    } else {
      unencrypted
    };

    #[cfg(not(feature = "compression"))]
    let uncompressed = unencrypted;

    Ok((uncompressed, addr))
  }

  async fn finish(&mut self) -> Result<(), AnyError> {
    Ok(())
  }
}

/// A test client for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct NetTransportTestClient<R: Runtime> {
  #[viewit(getter(skip), setter(skip))]
  socket: Arc<<R::Net as Net>::UdpSocket>,
  #[viewit(getter(skip), setter(skip))]
  local_addr: SocketAddr,
  checksumer: Checksumer,
  label: Label,
  send_label: bool,
  #[cfg(feature = "compression")]
  #[viewit(
    getter(attrs(cfg(feature = "compression"))),
    setter(attrs(cfg(feature = "compression")))
  )]
  send_compressed: Option<Compressor>,
  #[cfg(feature = "encryption")]
  #[viewit(
    getter(attrs(cfg(feature = "encryption"))),
    setter(attrs(cfg(feature = "encryption")))
  )]
  send_encrypted: Option<(EncryptionAlgo, SecretKey)>,
  receive_verify_label: bool,
  #[cfg(feature = "compression")]
  #[viewit(
    getter(attrs(cfg(feature = "compression"))),
    setter(attrs(cfg(feature = "compression")))
  )]
  receive_compressed: bool,
  #[cfg(feature = "encryption")]
  #[viewit(
    getter(attrs(cfg(feature = "encryption"))),
    setter(attrs(cfg(feature = "encryption")))
  )]
  receive_encrypted: Option<SecretKey>,
}

impl<R: Runtime> Clone for NetTransportTestClient<R> {
  fn clone(&self) -> Self {
    Self {
      socket: self.socket.clone(),
      local_addr: self.local_addr,
      checksumer: self.checksumer,
      label: self.label.clone(),
      send_label: self.send_label,
      #[cfg(feature = "compression")]
      send_compressed: self.send_compressed,
      #[cfg(feature = "encryption")]
      send_encrypted: self.send_encrypted,
      receive_verify_label: self.receive_verify_label,
      #[cfg(feature = "compression")]
      receive_compressed: self.receive_compressed,
      #[cfg(feature = "encryption")]
      receive_encrypted: self.receive_encrypted,
    }
  }
}

impl<R: Runtime> NetTransportTestClient<R> {
  /// Creates a new test client with the given address
  pub async fn new(addr: SocketAddr) -> Result<Self, AnyError> {
    let socket = <<R::Net as Net>::UdpSocket as UdpSocket>::bind(addr).await?;
    Ok(Self {
      local_addr: socket.local_addr()?,
      socket: Arc::new(socket),
      label: Label::empty(),
      send_label: false,
      checksumer: Checksumer::Crc32,
      #[cfg(feature = "compression")]
      send_compressed: None,
      #[cfg(feature = "encryption")]
      send_encrypted: None,
      receive_verify_label: false,
      #[cfg(feature = "compression")]
      receive_compressed: false,
      #[cfg(feature = "encryption")]
      receive_encrypted: None,
    })
  }
}

impl<R: Runtime> TestPacketClient for NetTransportTestClient<R> {
  type Connection = NetTransportTestPacketStream<R>;

  fn local_addr(&self) -> SocketAddr {
    self.local_addr
  }

  async fn close(&mut self) {}

  async fn accept(&mut self) -> Result<Self::Connection, AnyError> {
    Ok(NetTransportTestPacketStream {
      client: self.clone(),
      remote_addr: "127.0.0.1:0".parse().unwrap(),
    })
  }

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Connection, AnyError> {
    Ok(NetTransportTestPacketStream {
      client: self.clone(),
      remote_addr: addr,
    })
  }
}

pub struct NetTestPromisedStream<S: StreamLayer> {
  stream: S::Stream,
}

impl<S: StreamLayer> AsMut<S::Stream> for NetTestPromisedStream<S> {
  fn as_mut(&mut self) -> &mut S::Stream {
    &mut self.stream
  }
}

impl<S: StreamLayer> TestPromisedStream for NetTestPromisedStream<S> {
  async fn finish(&mut self) -> Result<(), AnyError> {
    Ok(())
  }
}

pub struct NetTestPromisedConnection<S: StreamLayer> {
  layer: Arc<S>,
  remote_addr: SocketAddr,
  ln: Arc<S::Listener>,
}

impl<S: StreamLayer> TestPromisedConnection for NetTestPromisedConnection<S> {
  type Stream = NetTestPromisedStream<S>;

  async fn accept(&self) -> Result<(Self::Stream, SocketAddr), AnyError> {
    self
      .ln
      .accept()
      .await
      .map(|(stream, addr)| (NetTestPromisedStream { stream }, addr))
      .map_err(Into::into)
  }

  async fn connect(&self) -> Result<Self::Stream, AnyError> {
    self
      .layer
      .connect(self.remote_addr)
      .await
      .map(|stream| NetTestPromisedStream { stream })
      .map_err(Into::into)
  }
}

/// A test client for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct NetTransportTestPromisedClient<S: StreamLayer> {
  ln: Arc<S::Listener>,
  layer: Arc<S>,
}

impl<S: StreamLayer> NetTransportTestPromisedClient<S> {
  /// Creates a new test client with the given address
  pub fn new(layer: S, ln: S::Listener) -> Self {
    Self {
      layer: Arc::new(layer),
      ln: Arc::new(ln),
    }
  }
}

impl<S: StreamLayer> TestPromisedClient for NetTransportTestPromisedClient<S> {
  type Stream = NetTestPromisedStream<S>;
  type Connection = NetTestPromisedConnection<S>;

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Connection, AnyError> {
    Ok(NetTestPromisedConnection {
      layer: self.layer.clone(),
      remote_addr: addr,
      ln: self.ln.clone(),
    })
  }

  async fn accept(&self) -> Result<Self::Connection, AnyError> {
    Ok(NetTestPromisedConnection {
      layer: self.layer.clone(),
      remote_addr: "127.0.0.1:0".parse().unwrap(),
      ln: self.ln.clone(),
    })
  }

  async fn close(&self) -> Result<(), AnyError> {
    Ok(())
  }

  fn local_addr(&self) -> std::io::Result<SocketAddr> {
    Ok(self.ln.local_addr())
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
#[cfg(feature = "compression")]
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
#[cfg(feature = "encryption")]
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
  let nonce = crate::security::read_nonce(&mut buf);
  crate::security::decrypt(pk, algo, nonce, auth_data, &mut buf)?;
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

/// A helper function to create TLS stream layer for testing
#[cfg(feature = "tls")]
pub async fn tls_stream_layer<R: Runtime>() -> crate::tls::Tls<R> {
  use crate::tls::{rustls, NoopCertificateVerifier, Tls};
  use rustls::pki_types::{CertificateDer, PrivateKeyDer};

  let certs = test_cert_gen::gen_keys();

  let cfg = rustls::ServerConfig::builder()
    .with_no_client_auth()
    .with_single_cert(
      vec![CertificateDer::from(
        certs.server.cert_and_key.cert.get_der().to_vec(),
      )],
      #[cfg(target_os = "linux")]
      PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        certs.server.cert_and_key.key.get_der().to_vec(),
      )),
      #[cfg(not(target_os = "linux"))]
      PrivateKeyDer::from(rustls::pki_types::PrivatePkcs1KeyDer::from(
        certs.server.cert_and_key.key.get_der().to_vec(),
      )),
    )
    .expect("bad certificate/key");
  let acceptor = futures_rustls::TlsAcceptor::from(Arc::new(cfg));

  let cfg = rustls::ClientConfig::builder()
    .dangerous()
    .with_custom_certificate_verifier(NoopCertificateVerifier::new())
    .with_no_client_auth();
  let connector = futures_rustls::TlsConnector::from(Arc::new(cfg));
  Tls::new(
    rustls::pki_types::ServerName::IpAddress(
      "127.0.0.1".parse::<std::net::IpAddr>().unwrap().into(),
    ),
    acceptor,
    connector,
  )
}

/// A helper function to create native TLS stream layer for testing
#[cfg(feature = "native-tls")]
pub async fn native_tls_stream_layer<R: Runtime>() -> crate::native_tls::NativeTls<R> {
  use async_native_tls::{Identity, TlsAcceptor, TlsConnector};

  use crate::native_tls::NativeTls;

  let keys = test_cert_gen::gen_keys();

  let identity = Identity::from_pkcs12(
    &keys.server.cert_and_key_pkcs12.pkcs12.0,
    &keys.server.cert_and_key_pkcs12.password,
  )
  .unwrap();

  let acceptor = TlsAcceptor::from(::native_tls::TlsAcceptor::new(identity).unwrap());
  let connector = TlsConnector::new().danger_accept_invalid_certs(true);

  NativeTls::new("localhost".to_string(), acceptor, connector)
}
