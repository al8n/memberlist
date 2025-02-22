#![allow(missing_docs, warnings)]

use core::panic;
use std::{future::Future, net::SocketAddr, sync::Arc};

use agnostic_lite::RuntimeLite;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{lock::Mutex, FutureExt, Stream};
use memberlist_core::{
  proto::{Label, Message},
  tests::AnyError,
  transport::Transport,
};
use nodecraft::CheapClone;
use smol_str::SmolStr;

use crate::{QuicAcceptor, QuicConnection, QuicConnector, QuicStream, StreamLayer};

#[cfg(feature = "quinn")]
pub use quinn_stream_layer::*;

#[cfg(feature = "quinn")]
mod quinn_stream_layer {
  use super::*;
  use crate::stream_layer::quinn::*;
  use ::quinn::{ClientConfig, Endpoint, ServerConfig};
  use futures::Future;
  use quinn::{crypto::rustls::QuicClientConfig, EndpointConfig};
  use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
  use smol_str::SmolStr;
  use std::{
    error::Error,
    net::SocketAddr,
    sync::{
      atomic::{AtomicU16, Ordering},
      Arc,
    },
    time::Duration,
  };

  /// Dummy certificate verifier that treats any certificate as valid.
  /// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
  #[derive(Debug)]
  struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

  impl SkipServerVerification {
    fn new() -> Arc<Self> {
      Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
  }

  impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
      &self,
      _end_entity: &CertificateDer<'_>,
      _intermediates: &[CertificateDer<'_>],
      _server_name: &ServerName<'_>,
      _ocsp: &[u8],
      _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
      &self,
      message: &[u8],
      cert: &CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls12_signature(
        message,
        cert,
        dss,
        &self.0.signature_verification_algorithms,
      )
    }

    fn verify_tls13_signature(
      &self,
      message: &[u8],
      cert: &CertificateDer<'_>,
      dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      rustls::crypto::verify_tls13_signature(
        message,
        cert,
        dss,
        &self.0.signature_verification_algorithms,
      )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }

  fn configures() -> Result<(ServerConfig, ClientConfig), Box<dyn Error + Send + Sync + 'static>> {
    let (server_config, _) = configure_server()?;
    let client_config = configure_client();
    Ok((server_config, client_config))
  }

  fn configure_client() -> ClientConfig {
    ClientConfig::new(Arc::new(
      QuicClientConfig::try_from(
        rustls::ClientConfig::builder()
          .dangerous()
          .with_custom_certificate_verifier(SkipServerVerification::new())
          .with_no_client_auth(),
      )
      .unwrap(),
    ))
  }

  fn configure_server(
  ) -> Result<(ServerConfig, CertificateDer<'static>), Box<dyn Error + Send + Sync + 'static>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert);
    let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

    let mut server_config =
      ServerConfig::with_single_cert(vec![cert_der.clone()], priv_key.into())?;
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok((server_config, cert_der))
  }

  #[allow(unused)]
  const ALPN_QUIC_HTTP: &[&[u8]] = &[b"hq-29"];

  /// Returns a new quinn stream layer
  pub async fn quinn_stream_layer<R: RuntimeLite>() -> crate::quinn::Options {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Options::new(
      server_name,
      server_config,
      client_config,
      EndpointConfig::default(),
    )
  }

  /// Returns a new quinn stream layer
  pub async fn quinn_stream_layer_with_connect_timeout<R: RuntimeLite>(
    timeout: Duration,
  ) -> crate::quinn::Options {
    let server_name = "localhost".to_string();
    let (server_config, client_config) = configures().unwrap();
    Options::new(
      server_name,
      server_config,
      client_config,
      EndpointConfig::default(),
    )
    .with_connect_timeout(timeout)
  }
}
