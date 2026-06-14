//! QUIC lifecycle / post-leave gating tests for the reactor QUIC driver, plus
//! the UDP-unreliable-transport drain branch.
//!
//! Brings up a converged two-node QUIC cluster (gossip + probes over QUIC
//! datagrams by default), gracefully `leave()` one node, and verifies every
//! command the driver gates on a running node is rejected with `NotRunning`.
//! Also covers an oversized `send` (`UserPacketExceedsMtu`), the live-node policy
//! setters, a silent-peer ping timeout, and convergence over the plain-UDP
//! gossip transport.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use bytes::Bytes;
use memberlist_reactor::{
  CompressionOptions, EncryptionOptions, Error, MaybeResolved, Memberlist, Node, Options,
  QuicOptions, SocketAddrResolver, VoidDelegate,
};
use rustls::RootCertStore;
use smol_str::SmolStr;

async fn make(id: &str, qcfg: QuicOptions) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  Memberlist::<SmolStr, _, TokioRuntime>::quic(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
    qcfg,
  )
  .await
  .expect("bind quic memberlist")
}

/// A shared-cert pair (each trusts the other's identical self-signed cert).
fn shared_quic_pair() -> (QuicOptions, QuicOptions) {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let b = support::build_quic_config(cert, key, roots);
  (a, b)
}

/// Brings up a converged QUIC pair and returns `(a, b, a_addr)`.
async fn converged_pair() -> (
  Memberlist<SmolStr, SocketAddr, TokioRuntime>,
  Memberlist<SmolStr, SocketAddr, TokioRuntime>,
  SocketAddr,
) {
  let (qcfg_a, qcfg_b) = shared_quic_pair();
  let a = make("qlife-a", qcfg_a).await;
  let b = make("qlife-b", qcfg_b).await;
  let a_addr = *a.local().addr_ref();
  b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join");
  let converged = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "QUIC pair did not converge");
  (a, b, a_addr)
}

/// After `leave()`, `ping` over QUIC is rejected with `NotRunning` (the probe
/// scheduler is stopped).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_ping_after_leave_is_rejected() {
  let (a, b, a_addr) = converged_pair().await;
  let target = Node::new(SmolStr::new("qlife-a"), a_addr);

  b.leave().await.expect("leave");

  let res = tokio::time::timeout(Duration::from_secs(5), b.ping(target))
    .await
    .expect("ping must not hang after leave");
  assert!(
    matches!(res, Err(Error::NotRunning)),
    "expected NotRunning from QUIC ping after leave, got {res:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// After `leave()`, both directed-send paths over QUIC are rejected with
/// `NotRunning`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_directed_sends_after_leave_are_rejected() {
  let (a, b, a_addr) = converged_pair().await;

  b.leave().await.expect("leave");

  let unreliable = tokio::time::timeout(
    Duration::from_secs(5),
    b.send(a_addr, Bytes::from_static(b"x")),
  )
  .await
  .expect("send must not hang after leave");
  assert!(
    matches!(unreliable, Err(Error::NotRunning)),
    "expected NotRunning from QUIC send after leave, got {unreliable:?}"
  );

  let reliable = tokio::time::timeout(
    Duration::from_secs(5),
    b.send_reliable(a_addr, Bytes::from_static(b"y")),
  )
  .await
  .expect("send_reliable must not hang after leave");
  assert!(
    matches!(reliable, Err(Error::NotRunning)),
    "expected NotRunning from QUIC send_reliable after leave, got {reliable:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// After `leave()`, both QUIC policy mutations are rejected with `NotRunning`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_set_policy_options_after_leave_is_rejected() {
  let (a, b, _a_addr) = converged_pair().await;

  b.leave().await.expect("leave");

  let res_compr = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_compression_options(CompressionOptions::new()),
  )
  .await
  .expect("set_compression_options must not hang after leave");
  assert!(
    matches!(res_compr, Err(Error::NotRunning)),
    "expected NotRunning from QUIC set_compression_options after leave, got {res_compr:?}"
  );

  let res_enc = tokio::time::timeout(
    Duration::from_secs(5),
    b.set_encryption_options(EncryptionOptions::new()),
  )
  .await
  .expect("set_encryption_options must not hang after leave");
  assert!(
    matches!(res_enc, Err(Error::NotRunning)),
    "expected NotRunning from QUIC set_encryption_options after leave, got {res_enc:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// After `leave()`, both `join` and `join_detached` over QUIC are rejected with
/// `NotRunning`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_join_after_leave_is_rejected() {
  let (a, b, a_addr) = converged_pair().await;

  b.leave().await.expect("leave");

  let res = tokio::time::timeout(
    Duration::from_secs(5),
    b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)]),
  )
  .await
  .expect("join must not hang after leave");
  assert!(
    matches!(res, Err(Error::NotRunning)),
    "expected NotRunning from QUIC join after leave, got {res:?}"
  );

  let res_detached = tokio::time::timeout(
    Duration::from_secs(5),
    b.join_detached(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)]),
  )
  .await
  .expect("join_detached must not hang after leave");
  assert!(
    matches!(res_detached, Err(Error::NotRunning)),
    "expected NotRunning from QUIC join_detached after leave, got {res_detached:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A second `leave()` racing the first over QUIC joins the in-flight leave;
/// both replies resolve on the single `LeftCluster`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_concurrent_leave_both_succeed() {
  let (a, b, _a_addr) = converged_pair().await;
  let b2 = b.clone();

  let (r1, r2) = tokio::join!(
    tokio::time::timeout(Duration::from_secs(8), b.leave()),
    tokio::time::timeout(Duration::from_secs(8), b2.leave()),
  );
  assert!(matches!(r1, Ok(Ok(()))), "first QUIC leave: {r1:?}");
  assert!(
    matches!(r2, Ok(Ok(()))),
    "second QUIC leave joined the first: {r2:?}"
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// An oversized unreliable `send` over QUIC is rejected as `UserPacketExceedsMtu`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_oversized_send_is_rejected() {
  let a = make("q-oversized-a", support::self_trusted_quic_config()).await;
  let huge = Bytes::from(vec![0xABu8; 1024 * 1024]);

  let res = tokio::time::timeout(
    Duration::from_secs(5),
    a.send("127.0.0.1:9".parse::<SocketAddr>().unwrap(), huge),
  )
  .await
  .expect("send must resolve, not hang");
  assert!(
    matches!(
      res,
      Err(Error::Proto(memberlist_proto::Error::UserPacketExceedsMtu(
        ..
      )))
    ),
    "an oversized QUIC send must be rejected as UserPacketExceedsMtu, got {res:?}"
  );

  let _ = a.shutdown().await;
}

/// `set_compression_options` / `set_encryption_options` on a LIVE QUIC node
/// succeed (the success arm of the gated setters).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_live_policy_setters_succeed() {
  let a = make("q-set-a", support::self_trusted_quic_config()).await;

  let compr = tokio::time::timeout(
    Duration::from_secs(5),
    a.set_compression_options(CompressionOptions::new()),
  )
  .await
  .expect("set_compression_options must resolve");
  assert!(
    compr.is_ok(),
    "live QUIC set_compression_options: {compr:?}"
  );

  let enc = tokio::time::timeout(
    Duration::from_secs(5),
    a.set_encryption_options(EncryptionOptions::new()),
  )
  .await
  .expect("set_encryption_options must resolve");
  assert!(enc.is_ok(), "live QUIC set_encryption_options: {enc:?}");

  let _ = a.shutdown().await;
}

/// `join_detached` over QUIC dispatches its seed and returns immediately; the
/// cluster still converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_join_detached_dispatches_and_converges() {
  let (qcfg_a, qcfg_b) = shared_quic_pair();
  let a = make("q-detach-a", qcfg_a).await;
  let b = make("q-detach-b", qcfg_b).await;
  let a_addr = *a.local().addr_ref();

  let dispatched = b
    .join_detached(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join_detached");
  assert_eq!(dispatched, 1, "one seed dispatched (fire-and-forget)");

  let converged = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(converged.is_ok(), "detached QUIC join did not converge");

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// A directed QUIC `ping` to a silent peer (a UDP socket that never speaks
/// memberlist) times out with `PingTimeout` rather than hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_ping_silent_peer_times_out() {
  let a = make("q-ping-silent-a", support::self_trusted_quic_config()).await;
  let silent = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind silent socket");
  let silent_addr = silent.local_addr().expect("silent local_addr");
  let target = Node::new(SmolStr::new("ghost"), silent_addr);

  let res = tokio::time::timeout(Duration::from_secs(25), a.ping(target))
    .await
    .expect("QUIC ping to a silent peer must resolve (timeout), not hang");
  assert!(
    matches!(res, Err(Error::PingTimeout)),
    "expected PingTimeout pinging a silent QUIC peer, got {res:?}"
  );

  let _ = a.shutdown().await;
}

/// Two QUIC nodes configured with `UnreliableTransport::Udp` must converge:
/// gossip + probes ride the shared UDP socket rather than QUIC datagrams,
/// exercising the driver's `UnreliableTransport::Udp` drain branch. Reliable
/// push/pull still rides QUIC streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_udp_unreliable_transport_converges() {
  let (qcfg_a, qcfg_b) = support::shared_udp_quic_pair();
  let a = make("q-udp-a", qcfg_a).await;
  let b = make("q-udp-b", qcfg_b).await;
  let a_addr = *a.local().addr_ref();

  let n = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("join over UDP-gossip QUIC");
  assert_eq!(n, 1, "one seed dispatched");

  let converged = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      if a.num_members() == 2 && b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    converged.is_ok(),
    "UDP-gossip QUIC cluster did not converge: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}
