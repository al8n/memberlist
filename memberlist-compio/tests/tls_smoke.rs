//! Smoke tests for TlsMemberlist — construct, snapshot, shutdown.
//!
//! Exercises the single-node lifecycle over the TLS-backed transport: bind the
//! UDP gossip socket with a caller-built `TlsOptions`, verify the initial
//! snapshot counts, and shut down cleanly. No peer connections are attempted.
//!
//! The TLS configs use a self-signed localhost-SAN certificate (generated at
//! test time via `rcgen`) and an accept-any client verifier, matching the
//! pattern in `memberlist-proto/src/tls/options.rs` tests.

#![cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

use std::time::{Duration, Instant};

use bytes::Bytes;
use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, MemberlistError, Options, SocketAddrResolver,
  StreamTransportOptions, TlsMemberlist, TlsTransportOptions, VoidDelegate,
};
use memberlist_proto::TlsOptions;
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
/// `memberlist-proto/src/tls/options.rs`, which are `pub(crate)` and
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

/// A zero `close_timeout` makes the post-`StreamAction::Close` graceful drain
/// abandon (RST) its queued push/pull response bytes instead of draining them.
/// The TLS backend funnels through the same `StreamTransportOptions::validate`
/// choke point as TCP, so construction must reject it with
/// `InvalidOption("close_timeout", _)`; a nonzero `close_timeout` constructs.
#[compio::test]
async fn tls_construct_rejects_zero_close_timeout() {
  let opts = Options::new(
    TlsTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("tls-zero-close"))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_tls_options(smoke_tls_options())
      .with_stream(StreamTransportOptions::new().with_close_timeout(Duration::ZERO)),
  );
  let res = TlsMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  match res {
    Err(MemberlistError::InvalidOption(e)) => {
      assert_eq!(
        e.option(),
        "close_timeout",
        "carries the rejected knob name"
      );
      assert!(!e.reason().is_empty(), "carries a reason");
    }
    Err(other) => panic!("expected InvalidOption(close_timeout), got {other:?}"),
    Ok(_) => panic!("a zero close_timeout must be rejected, but TLS construction succeeded"),
  }

  // A nonzero close_timeout constructs (the gossip socket binds an ephemeral
  // loopback port).
  let ok_opts = Options::new(
    TlsTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("tls-nonzero-close"))
      .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
      .with_tls_options(smoke_tls_options())
      .with_stream(StreamTransportOptions::new().with_close_timeout(Duration::from_secs(10))),
  );
  let m = TlsMemberlist::<SmolStr, SocketAddr>::new(
    ok_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("a nonzero close_timeout must construct over TLS");
  m.shutdown().await.expect("shutdown");
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

/// `send_many_reliable` where one payload connects and a LATER payload fails
/// BEFORE `StreamAction::Connect` must reply `Err(SendFailed)` — not a false
/// `Ok` once the connected exchange succeeds.
///
/// The pre-`Connect` failure is provoked deterministically with a stateful
/// `sni_provider` that returns `Some("localhost")` on its FIRST call and `None`
/// thereafter: a `None` SNI makes the TLS `dial_context` fail before any
/// `Connect`, so the second payload's dial is retired pre-`Connect` and never
/// surfaces an `ExchangeCompleted`. With two payloads (N = 2) and one captured
/// exchange (M = 1), the driver seeds `failed = N - M = 1`; even though the
/// first payload's exchange succeeds against the live seed, the final reply is
/// `Err(SendFailed)`.
///
/// Without the seeding fix the driver would park with `failed = 0`, observe the
/// single connected exchange succeed, and falsely report `Ok(())` though the
/// second payload was never sent.
#[compio::test]
async fn tls_send_many_reliable_partial_pre_connect_failure_reports_err() {
  let seed_addr = loopback_addr(7310);
  let sender_addr = loopback_addr(7311);

  // Live TLS seed the first payload connects to.
  let seed = make_tls("tls-pcf-seed", seed_addr)
    .await
    .expect("seed construct");

  // Sender's SNI provider: Some on the first dial, None on every later dial.
  let sni_calls = Arc::new(AtomicUsize::new(0));
  let sni_calls_for_closure = sni_calls.clone();
  let sender_opts = Options::new(
    TlsTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("tls-pcf-sender"))
      .with_advertise_addr(MaybeResolved::Resolved(sender_addr))
      .with_tls_options(smoke_tls_options())
      .with_sni_provider(Box::new(move |_addr: &SocketAddr| {
        // First dial → Some (connects); subsequent dials → None (the TLS
        // dial_context fails before Connect).
        if sni_calls_for_closure.fetch_add(1, Ordering::SeqCst) == 0 {
          Some("localhost".to_string())
        } else {
          None
        }
      })),
  );
  let sender = TlsMemberlist::<SmolStr, SocketAddr>::new(
    sender_opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("sender construct");

  // Two payloads to the SAME live seed: payload 0 dials with SNI Some →
  // Connect → bridge → succeeds; payload 1 dials with SNI None → pre-Connect
  // failure (no exchange). The seeded `failed` must force an Err result.
  let result = compio::time::timeout(
    Duration::from_secs(20),
    sender.send_many_reliable(
      seed_addr,
      [
        Bytes::from_static(b"tls-pcf-first"),
        Bytes::from_static(b"tls-pcf-second"),
      ],
    ),
  )
  .await
  .expect("send_many_reliable must resolve, not hang");
  assert!(
    matches!(result, Err(MemberlistError::SendFailed)),
    "a partial pre-Connect failure must reply Err(SendFailed), got {result:?}"
  );
  // Sanity: the SNI provider was consulted at least twice (one Some, one None).
  assert!(
    sni_calls.load(Ordering::SeqCst) >= 2,
    "both payload dials must consult the SNI provider; calls={}",
    sni_calls.load(Ordering::SeqCst)
  );

  sender.shutdown().await.expect("sender shutdown");
  seed.shutdown().await.expect("seed shutdown");
}

/// A synchronous `join` whose every seed fails BEFORE a `Connect` (the TLS
/// `sni_provider` returns `None`, so `dial_context` fails at dial setup and no
/// exchange is ever created) must resolve `JoinAllFailed` carrying the full
/// resolved seed count, and must resolve PROMPTLY rather than idle until the
/// join deadline.
///
/// The seed count is the denominator the caller sees. Deriving it from the
/// captured-exchange count would report `requested == 0` here (every seed
/// retired pre-`Connect`, so nothing is captured); with two seeds the payload
/// must report `requested == 2`.
#[compio::test]
async fn tls_join_all_pre_connect_failure_reports_full_seed_count() {
  let joiner_addr = loopback_addr(7312);
  // SNI provider always `None` → every TLS dial fails at `dial_context`, before
  // any `Connect`, so no exchange is created and `exchange_ids` stays empty.
  let opts = Options::new(
    TlsTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("tls-nosni-joiner"))
      .with_advertise_addr(MaybeResolved::Resolved(joiner_addr))
      .with_tls_options(smoke_tls_options())
      .with_sni_provider(Box::new(|_addr: &SocketAddr| None)),
  );
  let joiner = TlsMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("joiner construct");

  // Two seeds; both dials fail pre-`Connect`. The addresses need not be
  // reachable — `dial_context` fails before any connection is attempted.
  let seeds = [
    MaybeResolved::Resolved(loopback_addr(65534)),
    MaybeResolved::Resolved(loopback_addr(65535)),
  ];
  let result = compio::time::timeout(
    Duration::from_secs(10),
    joiner.join(&SocketAddrResolver, &seeds),
  )
  .await
  .expect("join must resolve promptly, not idle until the deadline");
  match result {
    Err(MemberlistError::JoinAllFailed(payload)) => {
      assert_eq!(
        payload.requested(),
        2,
        "requested must reflect the full resolved seed count, not the captured-exchange count",
      );
      assert_eq!(payload.contacted(), 0, "no seed was contacted");
    }
    other => panic!("expected JoinAllFailed with requested == 2, got {other:?}"),
  }

  joiner.shutdown().await.expect("joiner shutdown");
}
