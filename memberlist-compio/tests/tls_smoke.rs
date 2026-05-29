//! Smoke tests for TlsMemberlist — construct, snapshot, shutdown.
//!
//! Exercises the single-node lifecycle over the TLS-backed transport: bind the
//! UDP gossip socket with a caller-built `TlsOptions`, verify the initial
//! snapshot counts, and shut down cleanly. No peer connections are attempted.
//!
//! The TLS configs use a self-signed localhost-SAN certificate (generated at
//! test time via `rcgen`) and an accept-any client verifier, matching the
//! pattern in `memberlist-machine/src/tls/options.rs` tests.

#![cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::Arc,
};

use std::time::{Duration, Instant};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, MemberlistError, Options, SocketAddrResolver, TlsMemberlist,
  TlsTransportOptions, VoidDelegate,
};
use memberlist_machine::TlsOptions;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

/// Build a `TlsMemberlist` advertising `addr` with a fresh self-signed
/// `TlsOptions`. The membership-input address type is `SocketAddr`, so the
/// construction resolver is the identity `SocketAddrResolver` (never invoked
/// for a resolved advertise).
async fn make_tls(
  id: &str,
  addr: SocketAddr,
) -> Result<TlsMemberlist<SmolStr, SocketAddr>, MemberlistError> {
  let opts = Options::new(
    TlsTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_tls_options(smoke_tls_options()),
  );
  TlsMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
}

/// Accept-any server-cert verifier for smoke tests.
///
/// Skips chain validation entirely — appropriate only for single-node smoke
/// tests where there is no remote peer to verify.
#[derive(Debug)]
struct AcceptAnyServer(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServer {
  fn verify_server_cert(
    &self,
    _e: &rustls::pki_types::CertificateDer<'_>,
    _i: &[rustls::pki_types::CertificateDer<'_>],
    _n: &rustls::pki_types::ServerName<'_>,
    _o: &[u8],
    _t: rustls::pki_types::UnixTime,
  ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
    Ok(rustls::client::danger::ServerCertVerified::assertion())
  }
  fn verify_tls12_signature(
    &self,
    _m: &[u8],
    _c: &rustls::pki_types::CertificateDer<'_>,
    _d: &rustls::DigitallySignedStruct,
  ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
    Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
  }
  fn verify_tls13_signature(
    &self,
    _m: &[u8],
    _c: &rustls::pki_types::CertificateDer<'_>,
    _d: &rustls::DigitallySignedStruct,
  ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
    Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
  }
  fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
    self.0.signature_verification_algorithms.supported_schemes()
  }
}

fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
  // Use the first registered default, falling back to ring when no provider
  // has called `install_default_provider` yet.
  rustls::crypto::CryptoProvider::get_default()
    .cloned()
    .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()))
}

/// Build a self-signed localhost-SAN `ServerConfig` + accept-any `ClientConfig`.
///
/// Mirrors the `test_server` / `test_client` helpers in
/// `memberlist-machine/src/tls/options.rs`, which are `pub(crate)` and
/// therefore not accessible from this crate's integration tests.
fn smoke_tls_options() -> TlsOptions {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()])
    .expect("rcgen generate_simple_self_signed");
  let chain = vec![rustls::pki_types::CertificateDer::from(
    ck.cert.der().to_vec(),
  )];
  let key = rustls::pki_types::PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());

  let provider = crypto_provider();

  let server_cfg = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3 supported")
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .expect("valid self-signed cert");

  let client_cfg = rustls::ClientConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3 supported")
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(AcceptAnyServer(provider)))
    .with_no_client_auth();

  TlsOptions::new(server_cfg, client_cfg)
}

#[compio::test]
async fn construct_returns_handle_with_initial_snapshot() {
  let m = make_tls("n-7200", loopback_addr(7200))
    .await
    .expect("construct");

  let snap = m.snapshot();
  assert_eq!(
    snap.alive_count(),
    1,
    "initial snapshot has only the local node"
  );
  assert_eq!(snap.member_count(), 1);
  assert_eq!(snap.members_slice().len(), 1);

  m.shutdown().await.expect("shutdown");
}

#[compio::test]
async fn clone_handle_works() {
  let m = make_tls("n-7201", loopback_addr(7201))
    .await
    .expect("construct");

  let m2 = m.clone();
  assert_eq!(m2.alive_count(), 1);

  m.shutdown().await.expect("shutdown");
  // m2 holds the Arc<JoinHandle> too; driver task has already exited but
  // cloned handle reads remain valid (just return the final snapshot).
}

/// Verify a port collision on the UDP gossip socket surfaces as
/// `MemberlistError::Io` with TLS options.
#[compio::test]
async fn double_bind_returns_io_error() {
  let addr = loopback_addr(7202);

  let m1 = make_tls("n-7202a", addr).await.expect("first bind");
  // Use map_err + unwrap instead of expect_err — Memberlist<..> does not
  // implement Debug, so expect_err's T: Debug bound would fail to compile.
  let err = make_tls("n-7202b", addr)
    .await
    .map(|_| ())
    .expect_err("second bind on same port must fail");
  assert!(
    matches!(err, MemberlistError::Io(_)),
    "expected Io error, got {err:?}"
  );

  m1.shutdown().await.expect("shutdown");
}

/// Spin-wait up to `deadline` for `predicate` to return true.
async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

/// Multi-node TLS join — exercises TLS handshake on the reliable bridge
/// plus the full push/pull exchange. The TLS handshake bytes flow through
/// the same byte path as application bytes; `TlsRecords` inside the
/// coordinator decrypts them before surfacing plaintext to the membership
/// FSM.
#[compio::test]
async fn two_node_join_converges_member_counts_tls() {
  let seed_addr = loopback_addr(7300);
  let joiner_addr = loopback_addr(7301);

  let seed = make_tls("tls-seed", seed_addr)
    .await
    .expect("seed construct");
  let joiner = make_tls("tls-joiner", joiner_addr)
    .await
    .expect("joiner construct");

  let count = joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join");
  assert_eq!(count, 1, "exactly one seed handed to the driver");

  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "TLS cluster did not converge: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  assert_eq!(joiner.alive_count(), 2);
  assert_eq!(seed.alive_count(), 2);

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}
