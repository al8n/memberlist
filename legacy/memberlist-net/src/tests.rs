#![allow(missing_docs, warnings)]

use std::{net::SocketAddr, sync::Arc};

use agnostic::{
  Runtime,
  net::{Net, UdpSocket},
};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use memberlist_core::{
  proto::{Label, Message},
  tests::AnyError,
  transport::Transport,
};
use smol_str::SmolStr;

use crate::{Listener, StreamLayer};

/// A helper function to create TLS stream layer for testing
#[cfg(feature = "tls")]
pub async fn tls_stream_layer<R: Runtime>() -> crate::tls::TlsOptions {
  use crate::tls::{NoopCertificateVerifier, TlsOptions, rustls};
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
  TlsOptions::new(
    rustls::pki_types::ServerName::IpAddress(
      "127.0.0.1".parse::<std::net::IpAddr>().unwrap().into(),
    ),
    acceptor,
    connector,
  )
}
