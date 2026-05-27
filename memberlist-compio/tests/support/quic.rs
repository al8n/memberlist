//! QUIC test fixtures — self-signed localhost-SAN cert + caller-built
//! `quinn_proto` configs assembled into a `memberlist_compio::QuicConfig`.

#![cfg(feature = "quic-rustls-ring")]
#![allow(dead_code)] // Each test binary uses a subset of these helpers.

use std::{sync::Arc, time::Duration};

use memberlist_compio::QuicConfig;
use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

const ALPN: &[u8] = b"memberlist-quic";

/// Generate a fresh self-signed cert with `subject_alt_names = ["localhost"]`.
///
/// Returns the DER-encoded certificate and its PKCS#8 private key.
/// Uses rcgen 0.14's `generate_simple_self_signed` which returns a
/// `CertifiedKey`; the private key is the `signing_key` field.
pub fn generate_localhost_cert() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
  generate_cert_with_sans(vec!["localhost".into()])
}

/// Generate a fresh self-signed cert with caller-supplied SANs.
///
/// Returns the DER-encoded certificate and its PKCS#8 private key. Used by
/// negative tests that need a cert whose subject-alt-names do NOT match the
/// SNI value the [`memberlist_machine::QuicConfig`] SNI closure returns
/// (which the compio test setup always wires to `"localhost"`), forcing
/// rustls to reject the peer's certificate during the TLS handshake.
pub fn generate_cert_with_sans(
  sans: Vec<String>,
) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
  let ck = rcgen::generate_simple_self_signed(sans).expect("rcgen self-sign");
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  (cert, key)
}

/// Build a `QuicConfig` with the supplied cert + key, accepting any cert
/// signed by `trusted_roots` on the client side.
///
/// Uses no-client-auth on the server (trusted-network deployment model).
/// ALPN is set to `memberlist-quic` on both sides. Transport is configured
/// with fast idle timeout and keep-alive for test environments.
pub fn build_quic_config(
  cert: CertificateDer<'static>,
  key: PrivateKeyDer<'static>,
  trusted_roots: RootCertStore,
) -> QuicConfig {
  let provider = Arc::new(rustls::crypto::ring::default_provider());

  let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .expect("server single cert");
  rustls_server.alpn_protocols = vec![ALPN.to_vec()];

  // quinn_proto 0.11: QuicServerConfig::try_from takes Arc<rustls::ServerConfig>.
  let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
    .expect("QuicServerConfig");
  let server_cfg = ServerConfig::with_crypto(Arc::new(qsc));

  let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_root_certificates(trusted_roots)
    .with_no_client_auth();
  rustls_client.alpn_protocols = vec![ALPN.to_vec()];

  // quinn_proto 0.11: QuicClientConfig::try_from takes Arc<rustls::ClientConfig>.
  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
    .expect("QuicClientConfig");
  let client_cfg = ClientConfig::new(Arc::new(qcc));

  // Stateless-reset HMAC key via ring — matches the pattern in
  // `memberlist-machine/src/quic/crypto.rs` test helpers and
  // `memberlist-simulation/src/quic_net.rs`.
  let reset_key = [0x5au8; 32];
  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &reset_key);
  let endpoint_cfg = EndpointConfig::new(Arc::new(hmac));

  // Fast timeouts keep test runs short; `QuicConfig::new` forces
  // `max_concurrent_uni_streams = 0` and `grease_quic_bit = false`.
  let mut transport = TransportConfig::default();
  transport
    .max_idle_timeout(Some(
      quinn_proto::IdleTimeout::try_from(Duration::from_secs(5)).expect("idle timeout"),
    ))
    .keep_alive_interval(Some(Duration::from_secs(1)));

  QuicConfig::new(endpoint_cfg, server_cfg, client_cfg, transport, "localhost")
}

/// Convenience: generate a self-signed cert and build a `QuicConfig` that
/// trusts that cert as its own root (the common case for single-cluster tests).
pub fn self_trusted_quic_config() -> QuicConfig {
  let (cert, key) = generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("add root cert");
  build_quic_config(cert, key, roots)
}
