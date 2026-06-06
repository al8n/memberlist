//! TLS smoke-test fixtures: a self-signed localhost cert and the rustls
//! server/client configs bundled into `TlsOptions`. Dev-only; production callers
//! supply their own rustls configs.

#![allow(dead_code)] // Each test binary uses a subset of these helpers.

use std::sync::{Arc, OnceLock};

use memberlist_reactor::TlsOptions;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

/// Generates a fresh self-signed cert with `subject_alt_names = ["localhost"]`,
/// returning the DER certificate and its PKCS#8 private key.
pub fn generate_localhost_cert() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen self-sign");
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  (cert, key)
}

/// Builds `TlsOptions` presenting `cert`/`key` as the server (no client auth) and
/// verifying the peer's cert against `roots` as the client (TLS 1.3).
pub fn build_tls_options(
  cert: CertificateDer<'static>,
  key: PrivateKeyDer<'static>,
  roots: RootCertStore,
) -> TlsOptions {
  let provider = Arc::new(rustls::crypto::ring::default_provider());
  let server = ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .expect("server single cert");
  let client = ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_root_certificates(roots)
    .with_no_client_auth();
  TlsOptions::new(server, client)
}

/// Generates a self-signed cert and builds `TlsOptions` that trusts it as its own
/// root (the single-node / shared-cluster-cert test case).
pub fn self_trusted_tls_options() -> TlsOptions {
  let (cert, key) = generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("add root cert");
  build_tls_options(cert, key, roots)
}

/// `TlsOptions` built from a process-wide shared self-signed cert, so every node
/// in a multi-node suite cluster trusts every other node's identical server cert
/// under real root verification. Caches the cert DER and rebuilds the consumed
/// config per node.
pub fn shared_tls_options() -> TlsOptions {
  static SHARED: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
  let (cert_der, key_der) = SHARED.get_or_init(|| {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen self-sign");
    (ck.cert.der().to_vec(), ck.signing_key.serialize_der())
  });
  let cert = CertificateDer::from(cert_der.clone());
  let key = PrivateKeyDer::Pkcs8(key_der.clone().into());
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("add root cert");
  build_tls_options(cert, key, roots)
}
