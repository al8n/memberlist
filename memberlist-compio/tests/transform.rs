//! Transform (label / compression) integration tests for the compio driver.
//!
//! Verifies that the cluster label is applied to the gossip (UDP) plane, that
//! compression round-trips correctly end-to-end, and that the default
//! plaintext/unlabeled path is not broken by any transform wiring.

#![cfg(feature = "tcp")]

use std::{net::SocketAddr, time::Duration};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, MemberlistOptions, Options, SocketAddrResolver, TcpMemberlist,
  TcpTransportOptions, VoidDelegate,
};
use smol_str::SmolStr;

/// Build a TCP node with the given `MemberlistOptions`.
async fn make_with_opts(
  id: &str,
  ml_opts: MemberlistOptions,
) -> TcpMemberlist<SmolStr, SocketAddr> {
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
      )),
  )
  .with_memberlist(ml_opts);
  TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind tcp memberlist")
}

async fn wait_converged(
  a: &TcpMemberlist<SmolStr, SocketAddr>,
  b: &TcpMemberlist<SmolStr, SocketAddr>,
  want: usize,
  deadline: Duration,
) -> bool {
  let start = std::time::Instant::now();
  while start.elapsed() < deadline {
    if a.member_count() == want && b.member_count() == want {
      return true;
    }
    compio::time::sleep(Duration::from_millis(50)).await;
  }
  a.member_count() == want && b.member_count() == want
}

/// A labeled cluster node must reject gossip datagrams that carry no label.
///
/// Before the gossip codec is wired to the cluster label, the driver uses no
/// label on inbound gossip, which accepts any unlabeled datagram — including
/// traffic from outside the cluster. After the label is threaded in, the driver
/// decodes with a label check, which rejects an unlabeled datagram, preventing
/// cross-cluster contamination via the gossip plane.
///
/// The test demonstrates this by sending a crafted unlabeled Alive datagram
/// directly to the alpha node's UDP socket. The fake "ghost" node would appear
/// in the membership if the datagram is accepted (before the fix) and must not
/// appear after the fix.
///
/// Positive path: two alpha-labeled nodes join and converge via labeled gossip.
#[compio::test]
async fn gossip_label_isolates_clusters() {
  use memberlist_proto::{EncodeOptions, Node, encode_outgoing, typed::Message};

  // An alpha-labeled node. The cluster label is threaded into both the TCP
  // stream and — after this task — the gossip codec.
  let alpha_opts = MemberlistOptions::new()
    .with_label(Some(b"alpha".to_vec()))
    .expect("valid label");
  let alpha = make_with_opts("alpha-a", alpha_opts).await;
  let alpha_udp = alpha.advertise_address();

  // Craft an unlabeled Alive gossip datagram for a fake "ghost" node. Any
  // incarnation > 0 passes the SWIM freshness check for a node the alpha
  // cluster has never seen.
  let ghost_addr = "127.0.0.1:19999".parse::<SocketAddr>().unwrap();
  let ghost_node = Node::new(SmolStr::new("ghost"), ghost_addr);
  let alive_msg = memberlist_proto::typed::Alive::new(1, ghost_node);
  let datagram = encode_outgoing::<SmolStr, SocketAddr>(
    &Message::Alive(alive_msg),
    &EncodeOptions::default(), // no label — the gap this test closes
  )
  .expect("encode unlabeled Alive");

  // Fire the datagram at alpha's UDP port from an ephemeral socket.
  {
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind sender");
    sender.send_to(&datagram, alpha_udp).expect("send_to alpha");
  }

  // Give the driver several poll cycles to process the datagram.
  compio::time::sleep(Duration::from_millis(500)).await;

  // A labeled node must reject the unlabeled datagram: "ghost" must NOT appear.
  assert_eq!(
    alpha.member_count(),
    1,
    "labeled node must reject unlabeled gossip — got {} members (ghost leaked in)",
    alpha.member_count()
  );

  // Positive path: two alpha nodes join and converge via labeled gossip.
  let alpha2_opts = MemberlistOptions::new()
    .with_label(Some(b"alpha".to_vec()))
    .expect("valid label");
  let alpha2 = make_with_opts("alpha-b", alpha2_opts).await;
  alpha2
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(alpha_udp)])
    .await
    .expect("same-label join must succeed");

  let converged = wait_converged(&alpha, &alpha2, 2, Duration::from_secs(10)).await;
  assert!(
    converged,
    "same-label cluster did not converge: alpha={}, alpha2={}",
    alpha.member_count(),
    alpha2.member_count()
  );

  // Ignoring Err: best-effort teardown; convergence assertion already passed.
  let _ = alpha.shutdown().await;
  let _ = alpha2.shutdown().await;
}

#[cfg(feature = "compression-lz4")]
mod compression {
  use super::*;
  use memberlist_proto::{CompressAlgorithm, CompressionOptions};

  /// Two nodes both configured with LZ4 compression converge via the compressed
  /// gossip path. Both sides must decompress each other's datagrams, which proves
  /// `CompressionOptions` is threaded through construction on both the encode
  /// (compress_gossip) and decode (decompress_gossip) sides.
  ///
  /// The threshold is set to 0 so that every datagram — including small probe
  /// messages — is passed through the lz4 codec. A threshold-gated failure
  /// (the default 512-byte threshold bypassing small messages) would still let
  /// the cluster converge, masking a broken decoder. Zero threshold forces every
  /// payload through the full compress→decompress path.
  #[compio::test]
  async fn compressed_gossip_round_trips() {
    let comp = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(0);
    let ml_opts = MemberlistOptions::new().with_compression(comp);

    let a = make_with_opts("lz4-a", ml_opts.clone()).await;
    let b = make_with_opts("lz4-b", ml_opts).await;
    let a_addr = a.advertise_address();

    let n = b
      .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
      .await
      .expect("join with lz4 compression");
    assert_eq!(n, 1, "one seed contacted");

    let converged = wait_converged(&a, &b, 2, Duration::from_secs(10)).await;
    assert!(
      converged,
      "lz4-compressed cluster did not converge: a={}, b={}",
      a.member_count(),
      b.member_count()
    );

    // Ignoring Err: best-effort teardown; convergence assertion already passed.
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
  }
}

/// Two nodes built with `MemberlistOptions::default()` (no compression, no
/// encryption, no label) converge to 2 members. This is the regression guard
/// that the transform wiring did not alter the default plaintext/unlabeled path.
#[compio::test]
async fn default_options_are_unchanged() {
  let a = make_with_opts("def-a", MemberlistOptions::default()).await;
  let b = make_with_opts("def-b", MemberlistOptions::default()).await;
  let a_addr = a.advertise_address();

  let n = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("default-options join must succeed");
  assert_eq!(n, 1, "one seed contacted");

  let converged = wait_converged(&a, &b, 2, Duration::from_secs(10)).await;
  assert!(
    converged,
    "default-options cluster did not converge: a={}, b={}",
    a.member_count(),
    b.member_count()
  );

  // Ignoring Err: best-effort teardown; convergence assertion already passed.
  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}
