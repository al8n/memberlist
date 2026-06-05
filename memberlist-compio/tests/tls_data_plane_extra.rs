//! Extra TLS data-plane / lifecycle integration tests.
//!
//! tls_smoke.rs covers construction, two-node join, label isolation, and SNI
//! failures. These drive the TLS record-layer reliable plane harder: directed
//! reliable / unreliable sends round-tripping through `TlsRecords`,
//! `set_local_state` surfacing on a peer via `merge_remote_state`, a directed
//! ping RTT, and a three-node TLS convergence.

#![cfg(any(feature = "tls-rustls-ring", feature = "tls-rustls-aws-lc-rs"))]

use std::{
  borrow::Cow,
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use bytes::Bytes;
use memberlist_compio::{
  ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved, Memberlist,
  NodeDelegate, Options, PingDelegate, SocketAddrResolver, TlsMemberlist, TlsTransport,
  TlsTransportOptions, VoidDelegate,
};
use memberlist_proto::{Node, TlsOptions};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

// ── accept-any TLS config (mirrors tls_smoke.rs) ──

/// Accept-any server-cert verifier for these tests — skips chain validation,
/// appropriate for loopback self-signed certs.
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
  rustls::crypto::CryptoProvider::get_default()
    .cloned()
    .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()))
}

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

async fn make_tls(id: &str, addr: SocketAddr) -> TlsMemberlist<SmolStr, SocketAddr> {
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
  .expect("construct tls memberlist")
}

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

// ── recording delegate ──

#[derive(Default)]
struct Sink {
  user_msgs: Mutex<Vec<Bytes>>,
  merges: Mutex<Vec<Bytes>>,
}

struct RecordingDelegate {
  sink: Arc<Sink>,
}

impl EventDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl NodeDelegate for RecordingDelegate {
  async fn notify_user_msg(&self, msg: Cow<'_, [u8]>) {
    self
      .sink
      .user_msgs
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(msg.as_ref()));
  }

  async fn merge_remote_state(&self, buf: &[u8], _join: bool) {
    self
      .sink
      .merges
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(buf));
  }
}

impl PingDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl ConflictDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl Delegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

type RecordingMemberlist = Memberlist<TlsTransport<SmolStr, SocketAddr>, RecordingDelegate>;

async fn make_recording_tls(id: &str, addr: SocketAddr) -> (RecordingMemberlist, Arc<Sink>) {
  let sink = Arc::new(Sink::default());
  let opts = Options::new(
    TlsTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_tls_options(smoke_tls_options()),
  );
  let node = RecordingMemberlist::new(
    opts,
    RecordingDelegate { sink: sink.clone() },
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct recording tls memberlist");
  (node, sink)
}

// ── reliable directed send over the TLS record layer ──

/// A reliable directed `send_reliable` round-trips through the TLS record layer
/// to the peer's `notify_user_msg`. Exercises the TLS reliable-bridge encrypt /
/// decrypt path for application bytes (not just handshake / push-pull).
#[compio::test]
async fn tls_send_reliable_round_trips_to_delegate() {
  let (node_b, b_sink) = make_recording_tls("tls-rel-b", loopback_addr(0)).await;
  let b_addr = *node_b.local_node().addr_ref();
  let node_a = make_tls("tls-rel-a", loopback_addr(0)).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"tls-hello-reliable");
  node_a
    .send_reliable(b_addr, payload.clone())
    .await
    .expect("send_reliable over TLS should succeed");

  let saw = wait_until(
    || {
      b_sink
        .user_msgs
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_ref() == payload.as_ref())
    },
    Duration::from_secs(40),
  )
  .await;
  assert!(
    saw,
    "TLS notify_user_msg did not fire on node_b; received: {:?}",
    b_sink.user_msgs.lock().unwrap()
  );

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

// ── unreliable directed send (gossip plane, label-free smoke parity) ──

/// An unreliable directed `send` reaches the peer's `notify_user_msg` over the
/// TLS node's gossip (UDP) plane — the gossip plane is independent of the TLS
/// reliable record layer, so this confirms the TLS backend still routes
/// unreliable user data.
#[compio::test]
async fn tls_send_unreliable_round_trips_to_delegate() {
  let (node_b, b_sink) = make_recording_tls("tls-unrel-b", loopback_addr(0)).await;
  let b_addr = *node_b.local_node().addr_ref();
  let node_a = make_tls("tls-unrel-a", loopback_addr(0)).await;

  node_a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await
    .expect("join");

  let payload = Bytes::from_static(b"tls-hello-unreliable");
  node_a
    .send(b_addr, payload.clone())
    .await
    .expect("send over TLS gossip plane should succeed");

  let saw = wait_until(
    || {
      b_sink
        .user_msgs
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_ref() == payload.as_ref())
    },
    Duration::from_secs(40),
  )
  .await;
  assert!(
    saw,
    "TLS unreliable notify_user_msg did not fire on node_b; received: {:?}",
    b_sink.user_msgs.lock().unwrap()
  );

  node_b.shutdown().await.expect("node_b shutdown");
  node_a.shutdown().await.expect("node_a shutdown");
}

// ── set_local_state surfaces on peer via the TLS push/pull ──

/// `set_local_state` on a running TLS node surfaces on the peer's
/// `merge_remote_state` via the next push/pull — driven over the TLS reliable
/// record layer.
#[compio::test]
async fn tls_set_local_state_surfaces_on_peer_merge() {
  let (peer, peer_sink) = make_recording_tls("tls-sls-peer", loopback_addr(0)).await;
  let peer_addr = *peer.local_node().addr_ref();
  let setter = make_tls("tls-sls-setter", loopback_addr(0)).await;

  let snapshot = Bytes::from_static(b"tls-app-state");
  setter
    .set_local_state(snapshot.clone())
    .await
    .expect("set_local_state must succeed");

  setter
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(peer_addr)])
    .await
    .expect("join");

  let saw = wait_until(
    || {
      peer_sink
        .merges
        .lock()
        .unwrap()
        .iter()
        .any(|m| m.as_ref() == snapshot.as_ref())
    },
    Duration::from_secs(40),
  )
  .await;
  assert!(
    saw,
    "TLS peer's merge_remote_state never saw the snapshot; recorded: {:?}",
    peer_sink.merges.lock().unwrap()
  );

  setter.shutdown().await.expect("setter shutdown");
  peer.shutdown().await.expect("peer shutdown");
}

// ── directed ping over a TLS cluster ──

/// A directed `ping` between two converged TLS nodes returns a positive RTT.
/// The ping itself rides the gossip plane; this confirms the TLS backend's
/// directed-ping command path resolves against a live peer.
#[compio::test]
async fn tls_ping_live_node_returns_positive_rtt() {
  let seed = make_tls("tls-ping-seed", loopback_addr(0)).await;
  let seed_addr = *seed.local_node().addr_ref();
  let node = make_tls("tls-ping-node", loopback_addr(0)).await;

  node
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join");
  let joined = wait_until(
    || node.by_id(&SmolStr::new("tls-ping-seed")).is_some(),
    Duration::from_secs(40),
  )
  .await;
  assert!(joined, "seed did not appear in membership");

  let target = Node::new(SmolStr::new("tls-ping-seed"), seed_addr);
  let rtt = node.ping(target).await.expect("ping should succeed");
  assert!(rtt > Duration::ZERO, "RTT must be positive, got {rtt:?}");

  seed.shutdown().await.expect("seed shutdown");
  node.shutdown().await.expect("node shutdown");
}

// ── three-node TLS convergence ──

/// A three-node TLS cluster converges, exercising the TLS reliable plane's
/// multi-peer handshake + push/pull fan-out beyond the two-node base case.
#[compio::test]
async fn tls_three_node_cluster_converges() {
  let a = make_tls("tls-tri-a", loopback_addr(0)).await;
  let a_addr = *a.local_node().addr_ref();
  let b = make_tls("tls-tri-b", loopback_addr(0)).await;
  let c = make_tls("tls-tri-c", loopback_addr(0)).await;

  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("b joins a");
  c.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("c joins a");

  let converged = wait_until(
    || a.member_count() == 3 && b.member_count() == 3 && c.member_count() == 3,
    Duration::from_secs(40),
  )
  .await;
  assert!(
    converged,
    "three-node TLS cluster did not converge: a={} b={} c={}",
    a.member_count(),
    b.member_count(),
    c.member_count()
  );
  assert_eq!(a.alive_count(), 3);

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
  c.shutdown().await.expect("c shutdown");
}
