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
fn builds_a_usable_options_bundle() {
  let cfg = TlsOptions::new(test_server(), test_client());
  // Prove the bundle is usable: both rustls connections construct.
  let _server = rustls::ServerConnection::new(cfg.server_arc()).unwrap();
  let _client = rustls::ClientConnection::new(
    cfg.client_arc(),
    rustls::pki_types::ServerName::try_from("localhost").unwrap(),
  )
  .unwrap();
}

/// The borrowing accessors (`server_ref` / `client_ref`) hand back the same
/// configs the Arc accessors do — they alias the one stored `Arc`, so a
/// borrow and an arc-clone point at the same allocation.
#[test]
fn borrowing_accessors_alias_the_stored_configs() {
  let cfg = TlsOptions::new(test_server(), test_client());
  assert!(
    core::ptr::eq(cfg.server_ref(), Arc::as_ptr(&cfg.server_arc())),
    "server_ref borrows the same ServerConfig the server_arc clone wraps",
  );
  assert!(
    core::ptr::eq(cfg.client_ref(), Arc::as_ptr(&cfg.client_arc())),
    "client_ref borrows the same ClientConfig the client_arc clone wraps",
  );
  // A server connection constructs from the borrowed-then-cloned arc.
  let _server = rustls::ServerConnection::new(cfg.server_arc()).unwrap();
}
