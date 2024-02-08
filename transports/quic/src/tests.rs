#![allow(missing_docs, warnings)]

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

use crate::{QuicBiAcceptor, QuicBiStream, QuicConnector, StreamLayer};

#[cfg(feature = "compression")]
use crate::compressor::Compressor;

// /// Unit test for handling [`Ping`] message
// pub mod handle_ping;

// /// Unit test for handling compound ping message
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod handle_compound_ping;

// /// Unit test for handling indirect ping message
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod handle_indirect_ping;

// /// Unit test for handling ping from wrong node
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod handle_ping_wrong_node;

// /// Unit test for handling send packet with piggyback
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod packet_piggyback;

// /// Unit test for handling transport with label or not.
// pub mod label;

// /// Unit test for handling promised ping
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod promised_ping;

// /// Unit test for handling promised push pull
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod promised_push_pull;

// /// Unit test for sending
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod send;

// /// Unit test for joining
// #[cfg(all(feature = "compression", feature = "encryption"))]
// pub mod join;

/// A test client for network transport
#[viewit::viewit(
  vis_all = "",
  getters(vis_all = "pub", style = "ref"),
  setters(vis_all = "pub", prefix = "with")
)]
pub struct QuicTransportTestClient<S: StreamLayer> {
  #[viewit(getter(skip), setter(skip))]
  socket: S::Stream,
  #[viewit(getter(skip), setter(skip))]
  local_addr: SocketAddr,
  #[viewit(getter(skip), setter(skip))]
  remote_addr: SocketAddr,
  label: Label,
  send_label: bool,
  #[cfg(feature = "compression")]
  send_compressed: Option<Compressor>,
  receive_verify_label: bool,
  #[cfg(feature = "compression")]
  receive_compressed: bool,
}

impl<S: StreamLayer> QuicTransportTestClient<S> {
  /// Creates a new test client with the given address
  pub async fn new(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    conn: S::Stream,
  ) -> Result<Self, AnyError> {
    Ok(Self {
      local_addr,
      remote_addr,
      socket: conn,
      label: Label::empty(),
      send_label: false,
      #[cfg(feature = "compression")]
      send_compressed: None,
      receive_verify_label: false,
      #[cfg(feature = "compression")]
      receive_compressed: false,
    })
  }
}

impl<S: StreamLayer> TestPacketClient for QuicTransportTestClient<S> {
  async fn send_to(&mut self, _addr: &SocketAddr, src: &[u8]) -> Result<(), AnyError> {
    let mut out = BytesMut::new();
    if self.send_label {
      out.add_label_header(&self.label);
    }

    let mut data = BytesMut::new();

    #[cfg(feature = "compression")]
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

    #[cfg(not(feature = "compression"))]
    data.put_slice(src);

    out.put_slice(&data);
    self.socket.write_all(out.freeze()).await?;
    Ok(())
  }

  async fn recv_from(&mut self) -> Result<(Bytes, SocketAddr), AnyError> {
    let mut tag_buf = [0; 2];
    self.socket.peek_exact(&mut tag_buf).await?;
    if tag_buf[0] == Label::TAG {
      let len = tag_buf[1] as usize;
      self.socket.read_exact(&mut tag_buf).await?;
      let mut label_buf = vec![0; len];
      self.socket.read_exact(&mut label_buf).await?;

      let bytes: Bytes = label_buf.into();
      let label = Label::try_from(bytes)?;
      if self.receive_verify_label {
        assert_eq!(label, self.label);
      }
    }

    if self.receive_compressed {
      let mut buf = [0; 5];
      self.socket.read_exact(&mut buf).await?;
      let compressor = Compressor::try_from(buf[0])?;
      let compressed_data_len = NetworkEndian::read_u32(&buf[1..]) as usize;
      let mut src = vec![0; compressed_data_len];
      self.socket.read_exact(&mut src).await?;
      let uncompressed = compressor.decompress(&src)?;
      Ok((uncompressed.into(), self.local_addr))
    } else {
      let mut buf = [0; 5];
      self.socket.read_exact(&mut buf).await?;
      let data_len = NetworkEndian::read_u32(&buf[1..]) as usize;
      let mut src = vec![0; data_len + 5];
      src[..5].copy_from_slice(&buf);
      self.socket.read_exact(&mut src[5..]).await?;
      Ok((src.into(), self.local_addr))
    }
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
pub struct QuicTransportTestPromisedClient<S: StreamLayer> {
  ln: S::BiAcceptor,
  connector: S::Connector,
  layer: S,
}

impl<S: StreamLayer> QuicTransportTestPromisedClient<S> {
  /// Creates a new test client with the given address
  pub fn new(layer: S, ln: S::BiAcceptor, connector: S::Connector) -> Self {
    Self {
      layer,
      ln,
      connector,
    }
  }
}

impl<S: StreamLayer> TestPromisedClient for QuicTransportTestPromisedClient<S> {
  type Stream = S::Stream;

  async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, AnyError> {
    self.connector.open_bi(addr).await.map_err(Into::into)
  }

  async fn accept(&self) -> Result<(Self::Stream, SocketAddr), AnyError> {
    let (stream, addr) = self.ln.accept_bi().await?;
    Ok((stream, addr))
  }

  fn local_addr(&self) -> std::io::Result<SocketAddr> {
    Ok(self.ln.local_addr())
  }
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

// fn compound_encoder(msgs: &[Message<SmolStr, SocketAddr>]) -> Result<Bytes, AnyError> {
//   let num_msgs = msgs.len() as u8;
//   let total_bytes = 2 + msgs.iter().map(|m| m.encoded_len() + 2).sum::<usize>();
//   let mut out = BytesMut::with_capacity(total_bytes);
//   out.put_u8(Message::<SmolStr, SocketAddr>::COMPOUND_TAG);
//   out.put_u8(num_msgs);

//   let mut cur = out.len();
//   out.resize(total_bytes, 0);

//   for msg in msgs {
//     let len = msg.encoded_len() as u16;
//     NetworkEndian::write_u16(&mut out[cur..], len);
//     cur += 2;
//     let len = msg.encode(&mut out[cur..])?;
//     cur += len;
//   }

//   Ok(out.freeze())
// }

/// A helper function to create TLS stream layer for testing
#[cfg(feature = "tls")]
pub async fn tls_stream_layer<R: Runtime>() -> crate::tls::Tls<R> {
  use std::sync::Arc;

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
