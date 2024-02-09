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
  transport::{Transport, tests::{AddressKind, TestPacketClient, TestPromisedClient}},
  types::Message,
};
use memberlist_utils::{Label, LabelBufMutExt};
use nodecraft::Transformable;
use smol_str::SmolStr;

use crate::{QuicAcceptor, QuicConnector, QuicReadStream, QuicUniAcceptor, QuicWriteStream, StreamLayer};

#[cfg(feature = "compression")]
use crate::compressor::Compressor;

/// Unit test for handling [`Ping`] message
pub mod handle_ping;

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
pub struct QuicTransportTestClient<S: StreamLayer, R: Runtime> {
  #[viewit(getter(skip), setter(skip))]
  connector: Option<S::Connector>,
  #[viewit(getter(skip), setter(skip))]
  recv_stream_rx: async_channel::Receiver<(S::Stream, SocketAddr)>,
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
  _runtime: std::marker::PhantomData<R>,
}

impl<S: StreamLayer, R: Runtime> QuicTransportTestClient<S, R> {
  /// Creates a new test client with the given address
  pub async fn new(
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    layer: S,
  ) -> Result<Self, AnyError> {
    let (local_addr, mut acceptor, client) = layer.bind(local_addr).await?;
    let (tx, rx) = async_channel::bounded(1);
    R::spawn_detach(async move {
      loop {
        let (stream, addr) = acceptor.accept_bi().await.expect("failed to accept response stream");
        tx.send((stream, addr)).await.expect("failed to send response stream");
      }
    });
    
    Ok(Self {
      local_addr,
      remote_addr,
      connector: Some(client),
      recv_stream_rx: rx,
      label: Label::empty(),
      send_label: false,
      #[cfg(feature = "compression")]
      send_compressed: None,
      receive_verify_label: false,
      #[cfg(feature = "compression")]
      receive_compressed: false,
      _runtime: std::marker::PhantomData,
    })
  }
}

impl<S: StreamLayer, R: Runtime> TestPacketClient for QuicTransportTestClient<S, R> {
  async fn send_to(&mut self, _addr: &SocketAddr, src: &[u8]) -> Result<(), AnyError> {
    let mut out = BytesMut::new();
    out.put_u8(1);
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
    let mut client = self.connector.take().expect("connector is not set");
    let mut stream = client.open_bi(self.remote_addr).await?;
    stream.write_all(out.freeze()).await?;
    stream.flush().await?;
    stream.close().await;
    Ok(())
  }

  async fn recv_from(&mut self) -> Result<(Bytes, SocketAddr), AnyError> {
    let (mut stream, _) = self.recv_stream_rx.recv().await?;
    let mut all = stream.read_to_end().await?;
    assert_eq!(all[0], super::StreamType::Packet as u8);
    all.advance(1);
    if all[0] == Label::TAG {
      let len = all[1] as usize;
      all.advance(2);
      let label = Label::try_from(all.split_off(len))?;
      if self.receive_verify_label {
        assert_eq!(label, self.label);
      }
    }

    if self.receive_compressed {
      let compressor = Compressor::try_from(all[0])?;
      let compressed_data_len = NetworkEndian::read_u32(&all[1..]) as usize;
      all.advance(5);
      
      let uncompressed = compressor.decompress(&all[..compressed_data_len])?;
      Ok((uncompressed.into(), self.local_addr))
    } else {
      Ok((all, self.local_addr))
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
  ln: S::Acceptor,
  connector: S::Connector,
  layer: S,
}

impl<S: StreamLayer> QuicTransportTestPromisedClient<S> {
  /// Creates a new test client with the given address
  pub fn new(layer: S, ln: S::Acceptor, connector: S::Connector) -> Self {
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
    // let (stream, addr) = self.ln.accept_bi().await?;
    // Ok((stream, addr))
    todo!()
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


#[cfg(feature = "quinn")]
pub use quinn_stream_layer::quinn_stream_layer;

#[cfg(feature = "quinn")]
mod quinn_stream_layer {
  use super::*;
  use crate::stream_layer::quinn::*;
  use ::quinn::{ClientConfig, ServerConfig};
  use futures::Future;
  use smol_str::SmolStr;
  use std::{
    error::Error,
    net::SocketAddr,
    sync::{
      atomic::{AtomicU16, Ordering},
      Arc,
    },
  };

  struct SkipServerVerification;

  impl SkipServerVerification {
    fn new() -> Arc<Self> {
      Arc::new(Self)
    }
  }

  impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
      &self,
      _end_entity: &rustls::Certificate,
      _intermediates: &[rustls::Certificate],
      _server_name: &rustls::ServerName,
      _scts: &mut dyn Iterator<Item = &[u8]>,
      _ocsp_response: &[u8],
      _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::ServerCertVerified::assertion())
    }
  }

  fn configures() -> Result<(rustls::ServerConfig, rustls::ClientConfig), Box<dyn Error>> {
    let server_config = configure_server()?;
    let client_config = configure_client();
    Ok((server_config, client_config))
  }

  fn configure_client() -> rustls::ClientConfig {
    rustls::ClientConfig::builder()
      .with_safe_defaults()
      .with_custom_certificate_verifier(SkipServerVerification::new())
      .with_no_client_auth()
  }

  fn configure_server() -> Result<rustls::ServerConfig, Box<dyn Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut cfg = rustls::ServerConfig::builder()
      .with_safe_default_cipher_suites()
      .with_safe_default_kx_groups()
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .with_no_client_auth()
      .with_single_cert(cert_chain, priv_key)?;
    cfg.max_early_data_size = u32::MAX;
    Ok(cfg)
  }

  #[allow(unused)]
  const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

  /// Returns a new quinn stream layer
  pub async fn quinn_stream_layer<R: Runtime>() -> Quinn<R> {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Quinn::new(Options::new(server_name, server_config, client_config, Default::default()))
  }
}

#[cfg(feature = "s2n")]
pub use s2n_stream_layer::s2n_stream_layer;

#[cfg(feature = "s2n")]
mod s2n_stream_layer {
  use agnostic::Runtime;

  use crate::stream_layer::s2n::*;
  
  pub async fn s2n_stream_layer<R: Runtime>() -> S2n<R> {
    let p = std::env::current_dir().unwrap().join("tests");
    S2n::new(Options::new(p.join("s2n-tests.crt"), p.join("s2n-tests.key"))).unwrap()
  }
}
