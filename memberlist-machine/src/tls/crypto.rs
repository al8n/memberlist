//! TLS crypto/config bundling for the coordinator.
//!
//! # Cluster authentication on the TLS reliable path
//!
//! The composed super-state-machine treats the **TLS handshake as the
//! security layer** for the reliable path (push/pull, reliable-ping fallback,
//! reliable user messages). Memberlist's wire-level AES-GCM and wire-label are
//! not applied inside a TLS connection — the TLS record layer supersedes them;
//! cluster isolation comes from the operator's choice of TLS authentication
//! policy on the supplied `ServerConfig` / `ClientConfig`.
//!
//! Two deployment models, both expressed by the caller-built configs:
//!
//! - **Trusted-network**: a `ServerConfig` built with `with_no_client_auth()`.
//!   Any client that completes the server-authenticated TLS handshake can open
//!   a reliable exchange. Use only when the network enforces cluster
//!   membership (private VPC, mTLS-terminating ingress, …).
//!
//! - **Cluster-CA mTLS** (recommended on open networks): a `ServerConfig`
//!   built with a `ClientCertVerifier` (e.g.
//!   `WebPkiClientVerifier::builder_with_provider(cluster_ca_roots, provider)
//!       .build()?`) whose roots are the cluster CA, and a `ClientConfig` that
//!   presents a cluster-CA-signed client cert (`with_client_auth_cert`). A
//!   peer without a cluster-CA-signed cert fails the mutual handshake before
//!   any `Stream` exists — so no merge, no `UserDataReceived`, no reliable-ping
//!   ack.
//!
//! `TlsConfig::new` does NOT make this choice for the operator. It accepts a
//! fully caller-built `rustls::ServerConfig` / `ClientConfig` and only
//! Arc-wraps them. The crypto backend is the caller's choice (ring / aws-lc-rs
//! / FIPS via the matching `tls-rustls-*` feature); the coordinator names no
//! crypto crate.

use std::sync::Arc;

/// Immutable TLS config bundle handed to the coordinator. Accessor-only.
pub struct TlsConfig {
  server: Arc<rustls::ServerConfig>,
  client: Arc<rustls::ClientConfig>,
}

impl TlsConfig {
  /// Bundle a caller-built server config (carrying its `ClientCertVerifier`)
  /// and client config. Arc-wraps them and mutates nothing security-relevant —
  /// the operator owns the mTLS policy. See the module docs for the two
  /// supported cluster-auth deployment models.
  pub fn new(server: rustls::ServerConfig, client: rustls::ClientConfig) -> Self {
    Self {
      server: Arc::new(server),
      client: Arc::new(client),
    }
  }

  /// The server config used to accept inbound reliable connections.
  pub fn server(&self) -> &Arc<rustls::ServerConfig> {
    &self.server
  }

  /// The client config used for outbound dials.
  pub fn client(&self) -> &Arc<rustls::ClientConfig> {
    &self.client
  }
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;
  use rustls::pki_types::{CertificateDer, PrivateKeyDer};

  pub(crate) fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
  }

  pub(crate) fn self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
    let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
    (chain, key)
  }

  /// Accept-any server-cert verifier — sim/test only.
  #[derive(Debug)]
  pub(crate) struct AnyServer(pub(crate) Arc<rustls::crypto::CryptoProvider>);
  impl rustls::client::danger::ServerCertVerifier for AnyServer {
    fn verify_server_cert(
      &self,
      _e: &CertificateDer<'_>,
      _i: &[CertificateDer<'_>],
      _n: &rustls::pki_types::ServerName<'_>,
      _o: &[u8],
      _t: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
      Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
      &self,
      _m: &[u8],
      _c: &CertificateDer<'_>,
      _d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
      Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
      self.0.signature_verification_algorithms.supported_schemes()
    }
  }

  /// Self-signed server + accept-any client config — sim/test only.
  pub(crate) fn test_server() -> rustls::ServerConfig {
    let (chain, key) = self_signed();
    rustls::ServerConfig::builder_with_provider(provider())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .with_no_client_auth()
      .with_single_cert(chain, key)
      .unwrap()
  }

  pub(crate) fn test_client() -> rustls::ClientConfig {
    let p = provider();
    rustls::ClientConfig::builder_with_provider(p.clone())
      .with_protocol_versions(&[&rustls::version::TLS13])
      .unwrap()
      .dangerous()
      .with_custom_certificate_verifier(Arc::new(AnyServer(p)))
      .with_no_client_auth()
  }

  #[test]
  fn builds_a_usable_config_bundle() {
    let cfg = TlsConfig::new(test_server(), test_client());
    // Prove the bundle is usable: both rustls connections construct.
    let _server = rustls::ServerConnection::new(cfg.server().clone()).unwrap();
    let _client = rustls::ClientConnection::new(
      cfg.client().clone(),
      rustls::pki_types::ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();
  }
}
