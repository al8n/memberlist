use std::{net::SocketAddr, sync::Arc};

use memberlist_proto::TlsOptions;

use super::*;
use crate::{FirstAddrResolver, MaybeResolved, OsResolver, SocketAddrResolver};
use rustls::{
  client::danger::{HandshakeSignatureValid, ServerCertVerified},
  crypto::CryptoProvider,
  pki_types::CertificateDer,
  version::TLS13,
};
use std::io::ErrorKind;

/// Accept-any server-cert verifier for the construction test.
///
/// The transport-construction test does NOT exercise the TLS handshake;
/// the verifier just has to type-check inside a `ClientConfig`.
#[derive(Debug)]
struct AcceptAnyServer(Arc<CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServer {
  fn verify_server_cert(
    &self,
    _e: &CertificateDer<'_>,
    _i: &[CertificateDer<'_>],
    _n: &rustls::pki_types::ServerName<'_>,
    _o: &[u8],
    _t: rustls::pki_types::UnixTime,
  ) -> Result<ServerCertVerified, rustls::Error> {
    Ok(ServerCertVerified::assertion())
  }
  fn verify_tls12_signature(
    &self,
    _m: &[u8],
    _c: &CertificateDer<'_>,
    _d: &rustls::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    Ok(HandshakeSignatureValid::assertion())
  }
  fn verify_tls13_signature(
    &self,
    _m: &[u8],
    _c: &CertificateDer<'_>,
    _d: &rustls::DigitallySignedStruct,
  ) -> Result<HandshakeSignatureValid, rustls::Error> {
    Ok(HandshakeSignatureValid::assertion())
  }
  fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
    self.0.signature_verification_algorithms.supported_schemes()
  }
}

fn crypto_provider() -> Arc<CryptoProvider> {
  CryptoProvider::get_default()
    .cloned()
    .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()))
}

/// Build a self-signed localhost-SAN `ServerConfig` + accept-any
/// `ClientConfig`. Mirrors the existing `tls_smoke` fixture; the
/// construction test does not perform a handshake.
fn test_tls_options() -> TlsOptions {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()])
    .expect("rcgen generate_simple_self_signed");
  let chain = vec![CertificateDer::from(ck.cert.der().to_vec())];
  let key = rustls::pki_types::PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());

  let provider = crypto_provider();

  let server_cfg = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&TLS13])
    .expect("TLS 1.3 supported")
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .expect("valid self-signed cert");

  let client_cfg = rustls::ClientConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&TLS13])
    .expect("TLS 1.3 supported")
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AcceptAnyServer(provider)))
    .with_no_client_auth();

  TlsOptions::new(server_cfg, client_cfg)
}

fn test_tls_opts() -> TlsTransportOptions {
  let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
  TlsTransportOptions::new()
    .with_local_id(smol_str::SmolStr::new("test-node"))
    .with_advertise_addr(MaybeResolved::Resolved(bind))
    .with_tls_options(test_tls_options())
}

#[compio::test]
async fn new_with_resolved_advertise_skips_resolver() {
  let opts = test_tls_opts();
  let t: TlsTransport = TlsTransport::new(opts, &OsResolver, &FirstAddrResolver)
    .await
    .expect("construct TlsTransport");
  assert_eq!(t.local_id().as_str(), "test-node");
  assert!(t.local_address().is_resolved());
  let _: &SocketAddr = t.advertise_address();
}

/// The `TlsTransportOptions` getters reflect what the builders set, including
/// the SNI provider closure and the TLS options bundle. The pre-build state is
/// `None` for the required fields (the arms `new()`'s `ok_or_else` checks).
#[test]
fn options_accessors_reflect_builders() {
  let addr: SocketAddr = "127.0.0.1:7946".parse().unwrap();
  // Default options: required fields unset, but the SNI provider has a default
  // (`localhost`) so `sni_provider()` returns a live closure even before build.
  let empty = TlsTransportOptions::<smol_str::SmolStr, SocketAddr>::new();
  assert!(empty.local_id().is_none());
  assert!(empty.advertise_addr().is_none());
  assert!(empty.tls_options().is_none());
  assert!(empty.stream().validate().is_ok());
  assert_eq!((empty.sni_provider())(&addr), Some("localhost".to_string()));

  let opts = TlsTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
    .with_local_id(smol_str::SmolStr::new("acc-node"))
    .with_advertise_addr(MaybeResolved::Resolved(addr))
    .with_tls_options(test_tls_options())
    .with_sni_provider(Box::new(|_| Some("peer.example".to_string())));
  assert_eq!(opts.local_id().map(|s| s.as_str()), Some("acc-node"));
  match opts.advertise_addr() {
    Some(MaybeResolved::Resolved(s)) => assert_eq!(*s, addr),
    other => panic!("expected a resolved advertise addr, got {other:?}"),
  }
  assert!(opts.tls_options().is_some());
  // The custom SNI provider overrides the default for every peer.
  assert_eq!(
    (opts.sni_provider())(&addr),
    Some("peer.example".to_string())
  );
}

/// `Default` is the `new()` state: required fields `None`, default SNI provider
/// installed.
#[test]
fn default_matches_new() {
  let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
  let d = TlsTransportOptions::<smol_str::SmolStr, SocketAddr>::default();
  assert!(d.local_id().is_none());
  assert!(d.advertise_addr().is_none());
  assert!(d.tls_options().is_none());
  assert_eq!((d.sni_provider())(&addr), Some("localhost".to_string()));
}

/// `new` rejects a missing `local_id` with `InvalidInput` BEFORE any
/// resolution or socket bind — the field is required.
#[compio::test]
async fn new_without_local_id_errors() {
  let opts = TlsTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
    .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()))
    .with_tls_options(test_tls_options());
  let res = TlsTransport::<smol_str::SmolStr, SocketAddr>::new(
    opts,
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("local_id"));
    }
    Err(other) => panic!("expected InvalidInput(local_id), got {other:?}"),
    Ok(_) => panic!("a missing local_id must be rejected, but construction succeeded"),
  }
}

/// `new` rejects a missing `advertise_addr` with `InvalidInput`.
#[compio::test]
async fn new_without_advertise_addr_errors() {
  let opts = TlsTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
    .with_local_id(smol_str::SmolStr::new("no-adv"))
    .with_tls_options(test_tls_options());
  let res = TlsTransport::<smol_str::SmolStr, SocketAddr>::new(
    opts,
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("advertise_addr"));
    }
    Err(other) => panic!("expected InvalidInput(advertise_addr), got {other:?}"),
    Ok(_) => panic!("a missing advertise_addr must be rejected, but construction succeeded"),
  }
}

/// `new` rejects a missing `tls_options` with `InvalidInput`.
#[compio::test]
async fn new_without_tls_options_errors() {
  let opts = TlsTransportOptions::<smol_str::SmolStr, SocketAddr>::new()
    .with_local_id(smol_str::SmolStr::new("no-tls"))
    .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap()));
  let res = TlsTransport::<smol_str::SmolStr, SocketAddr>::new(
    opts,
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::Io(e)) => {
      assert_eq!(e.kind(), ErrorKind::InvalidInput);
      assert!(e.to_string().contains("tls_options"));
    }
    Err(other) => panic!("expected InvalidInput(tls_options), got {other:?}"),
    Ok(_) => panic!("a missing tls_options must be rejected, but construction succeeded"),
  }
}
