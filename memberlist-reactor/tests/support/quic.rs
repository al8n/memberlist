//! QUIC test fixtures — a self-signed localhost-SAN cert + caller-built
//! `quinn_proto` configs assembled into a `memberlist_reactor::QuicOptions`.

#![cfg(feature = "quic-rustls-ring")]
#![allow(dead_code)] // Each test binary uses a subset of these helpers.

use std::{sync::Arc, time::Duration};

use memberlist_proto::UnreliableTransport;
use memberlist_reactor::QuicOptions;
use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

const ALPN: &[u8] = b"memberlist-quic";

/// Generates a fresh self-signed cert with `subject_alt_names = ["localhost"]`,
/// returning the DER certificate and its PKCS#8 private key.
pub fn generate_localhost_cert() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen self-sign");
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  (cert, key)
}

/// Builds a `QuicOptions` presenting `cert`/`key` and trusting `trusted_roots`,
/// with `memberlist-quic` ALPN and short test-friendly timeouts.
pub fn build_quic_config(
  cert: CertificateDer<'static>,
  key: PrivateKeyDer<'static>,
  trusted_roots: RootCertStore,
) -> QuicOptions {
  let provider = Arc::new(rustls::crypto::ring::default_provider());

  let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .expect("server single cert");
  rustls_server.alpn_protocols = vec![ALPN.to_vec()];
  let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
    .expect("QuicServerConfig");
  let server_cfg = ServerConfig::with_crypto(Arc::new(qsc));

  let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_root_certificates(trusted_roots)
    .with_no_client_auth();
  rustls_client.alpn_protocols = vec![ALPN.to_vec()];
  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
    .expect("QuicClientConfig");
  let client_cfg = ClientConfig::new(Arc::new(qcc));

  let reset_key = [0x5au8; 32];
  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &reset_key);
  let endpoint_cfg = EndpointConfig::new(Arc::new(hmac));

  let mut transport = TransportConfig::default();
  transport
    .max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(5)).expect("idle timeout"),
    ))
    .keep_alive_interval(Some(Duration::from_secs(1)));

  QuicOptions::new(
    endpoint_cfg,
    server_cfg,
    client_cfg,
    transport,
    "localhost",
    UnreliableTransport::Datagram,
  )
}

/// Generates a self-signed cert and builds a `QuicOptions` that trusts it as its
/// own root (the common single-node / single-cluster test case).
pub fn self_trusted_quic_config() -> QuicOptions {
  let (cert, key) = generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("add root cert");
  build_quic_config(cert, key, roots)
}

/// Like [`build_quic_config`] but pins the gossip unreliable transport to plain
/// UDP (`UnreliableTransport::Udp`) instead of QUIC datagrams, exercising the
/// driver's UDP-fallback drain branch.
pub fn build_quic_config_udp(
  cert: CertificateDer<'static>,
  key: PrivateKeyDer<'static>,
  trusted_roots: RootCertStore,
) -> QuicOptions {
  let provider = Arc::new(rustls::crypto::ring::default_provider());

  let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .expect("server single cert");
  rustls_server.alpn_protocols = vec![ALPN.to_vec()];
  let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
    .expect("QuicServerConfig");
  let server_cfg = ServerConfig::with_crypto(Arc::new(qsc));

  let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_root_certificates(trusted_roots)
    .with_no_client_auth();
  rustls_client.alpn_protocols = vec![ALPN.to_vec()];
  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
    .expect("QuicClientConfig");
  let client_cfg = ClientConfig::new(Arc::new(qcc));

  let reset_key = [0x5au8; 32];
  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &reset_key);
  let endpoint_cfg = EndpointConfig::new(Arc::new(hmac));

  let mut transport = TransportConfig::default();
  transport
    .max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(5)).expect("idle timeout"),
    ))
    .keep_alive_interval(Some(Duration::from_secs(1)));

  QuicOptions::new(
    endpoint_cfg,
    server_cfg,
    client_cfg,
    transport,
    "localhost",
    UnreliableTransport::Udp,
  )
}

/// Generates a shared cert + trust root and builds a matched pair of
/// UDP-unreliable-transport `QuicOptions` for a two-node cluster.
pub fn shared_udp_quic_pair() -> (QuicOptions, QuicOptions) {
  let (cert, key) = generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let a = build_quic_config_udp(cert.clone(), key.clone_key(), roots.clone());
  let b = build_quic_config_udp(cert, key, roots);
  (a, b)
}
