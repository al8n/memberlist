//! Transform (encryption / compression / label) integration tests for the reactor
//! driver.
//!
//! These tests build real clusters with non-default transforms and verify that
//! gossip round-trips correctly under those transforms.

#![cfg(feature = "tcp")]

use std::{net::SocketAddr, time::Duration};

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{
  MaybeResolved, Memberlist, MemberlistOptions, Options, SocketAddrResolver, VoidDelegate,
};
use smol_str::SmolStr;

/// Build a TCP node with the given `MemberlistOptions`.
async fn make_with_opts(
  id: &str,
  ml_opts: MemberlistOptions,
) -> Memberlist<SmolStr, SocketAddr, TokioRuntime> {
  Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("bind tcp memberlist")
}

/// A labeled cluster node must reject gossip datagrams that carry no label.
///
/// Before the gossip codec is wired to the cluster label, the driver uses
/// `DecodeOptions::default()` (label=None) on inbound gossip, which accepts
/// any unlabeled datagram — including traffic from outside the cluster. After
/// the label is threaded in, the driver decodes with
/// `DecodeOptions::new(Some(label))`, which rejects an unlabeled datagram with
/// `LabelMismatch`, preventing cross-cluster contamination via the gossip plane.
///
/// The test demonstrates this by sending a crafted unlabeled Alive datagram
/// directly to the alpha node's UDP socket. The fake "ghost" node would appear
/// in the membership if the datagram is accepted (before the fix) and must not
/// appear after the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gossip_label_isolates_clusters() {
  use memberlist_proto::{EncodeOptions, Node, encode_outgoing, typed::Message};

  // An alpha-labeled node. The cluster label is threaded into both the TCP
  // stream (for join isolation) and the gossip codec via the single validated label source.
  let alpha_opts = MemberlistOptions::new()
    .with_label(Some(b"alpha".to_vec()))
    .expect("valid label");
  let alpha = make_with_opts("alpha-a", alpha_opts).await;
  let alpha_udp = alpha.advertise_address();

  // Craft an unlabeled Alive gossip datagram for a fake "ghost" node.
  // Any incarnation > 0 suffices to pass the SWIM freshness check for a node
  // the alpha cluster has never seen before.
  let ghost_addr = "127.0.0.1:19999".parse::<SocketAddr>().unwrap();
  let ghost_node = Node::new(SmolStr::new("ghost"), ghost_addr);
  let alive_msg = memberlist_proto::typed::Alive::new(1, ghost_node);
  let datagram = encode_outgoing::<SmolStr, SocketAddr>(
    &Message::Alive(alive_msg),
    &EncodeOptions::default(), // no label — the gap this task closes
  )
  .expect("encode unlabeled Alive");

  // Fire the datagram at alpha's UDP port from an ephemeral socket. The send
  // is synchronous and instantaneous; no join or TCP handshake is involved.
  {
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind sender");
    sender.send_to(&datagram, alpha_udp).expect("send_to alpha");
  }

  // Give the driver several poll cycles to process the inbound datagram.
  tokio::time::sleep(Duration::from_millis(500)).await;

  // After the label is threaded through the gossip codec, the alpha node
  // must reject the unlabeled datagram: the "ghost" member must NOT appear.
  assert_eq!(
    alpha.num_members(),
    1,
    "labeled node must reject unlabeled gossip — got {} members (ghost leaked in)",
    alpha.num_members()
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

  let converged = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      if alpha.num_members() == 2 && alpha2.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    converged.is_ok(),
    "same-label cluster did not converge: alpha={}, alpha2={}",
    alpha.num_members(),
    alpha2.num_members()
  );

  // Ignoring Err: best-effort teardown; convergence assertion already passed.
  let _ = alpha.shutdown().await;
  let _ = alpha2.shutdown().await;
}

#[cfg(feature = "lz4")]
mod compression {
  use super::*;
  use memberlist_proto::CompressAlgorithm;
  use memberlist_reactor::CompressionOptions;

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
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn compressed_gossip_round_trips() {
    let comp = CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(0);
    let ml_opts = MemberlistOptions::new().with_compression(comp);

    let a = make_with_opts("lz4-a", ml_opts.clone()).await;
    let b = make_with_opts("lz4-b", ml_opts).await;
    let a_addr = *a.local().addr_ref();

    let n = b
      .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
      .await
      .expect("join with lz4 compression");
    assert_eq!(n, 1, "one seed contacted");

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
      "lz4-compressed cluster did not converge: a={}, b={}",
      a.num_members(),
      b.num_members()
    );

    // Ignoring Err: best-effort teardown; convergence assertion already passed.
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
  }
}

#[cfg(feature = "crc32")]
mod checksum {
  use super::*;
  use memberlist_reactor::{ChecksumAlgorithm, ChecksumOptions};

  /// Two nodes both configured with a CRC32 gossip checksum converge via the
  /// checksummed gossip path. Both sides must verify each other's datagram
  /// digests, which proves `ChecksumOptions` is threaded through construction
  /// on both the encode (checksum_gossip) and decode (verify) sides.
  ///
  /// Checksum is a gossip-plane transform only; the reliable stream path
  /// carries no checksum (it relies on the transport's own integrity), so this
  /// convergence rides the checksummed UDP gossip and the unchecksummed TCP
  /// push/pull together.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn checksummed_gossip_round_trips() {
    let checksum = ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32);
    let ml_opts = MemberlistOptions::new().with_checksum(checksum);

    let a = make_with_opts("crc32-a", ml_opts.clone()).await;
    let b = make_with_opts("crc32-b", ml_opts).await;
    let a_addr = *a.local().addr_ref();

    let n = b
      .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
      .await
      .expect("join with crc32 checksum");
    assert_eq!(n, 1, "one seed contacted");

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
      "crc32-checksummed cluster did not converge: a={}, b={}",
      a.num_members(),
      b.num_members()
    );

    // Ignoring Err: best-effort teardown; convergence assertion already passed.
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
  }
}

#[cfg(feature = "aes-gcm")]
mod encryption {
  use super::*;
  use memberlist_reactor::{EncryptionOptions, Keyring, SecretKey};

  /// Two nodes configured with the same AES-256-GCM keyring converge via the
  /// encrypted gossip path. Both sides must decrypt each other's datagrams,
  /// which proves the keyring is threaded through construction on both the
  /// encode (encrypt_gossip) and decode (decrypt_gossip) sides.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn encrypted_gossip_round_trips() {
    // A 32-byte AES-256 key shared between both nodes.
    let key = SecretKey::Aes256([0x42u8; 32]);
    let keyring = Keyring::new(key);
    let enc_opts = EncryptionOptions::new().with_keyring(keyring);
    let ml_opts = MemberlistOptions::new().with_encryption(enc_opts);

    let a = make_with_opts("enc-a", ml_opts.clone()).await;
    let b = make_with_opts("enc-b", ml_opts).await;
    let a_addr = *a.local().addr_ref();

    let n = b
      .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
      .await
      .expect("join with encryption");
    assert_eq!(n, 1, "one seed contacted");

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
      "encrypted cluster did not converge: a={}, b={}",
      a.num_members(),
      b.num_members()
    );

    // Ignoring Err: best-effort teardown; convergence assertion already passed.
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
  }

  /// Rotating the encryption key at runtime actually changes what the node
  /// encrypts with: two nodes that start with different keys cannot join each
  /// other, but after rotating one to match the other they can join and converge.
  ///
  /// The test demonstrates fail → pass:
  ///   1. Node A starts with key1; node C starts with key2 (different key).
  ///   2. C.join(A) fails — the push/pull exchange is encrypted under key2,
  ///      which A cannot decrypt, so C stays at 1 member.
  ///   3. C.set_encryption_options(key1).await succeeds (the rotation).
  ///   4. C.join(A) now succeeds because both sides use key1, and the cluster
  ///      converges to 2 members.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn runtime_key_rotation_takes_effect() {
    let key1 = SecretKey::Aes256([0xAAu8; 32]);
    let key2 = SecretKey::Aes256([0xBBu8; 32]);

    // A uses key1; C uses key2 — different keyrings, the push/pull handshake
    // is encrypted under incompatible keys.
    let opts_a = MemberlistOptions::new()
      .with_encryption(EncryptionOptions::new().with_keyring(Keyring::new(key1)));
    let opts_c = MemberlistOptions::new()
      .with_encryption(EncryptionOptions::new().with_keyring(Keyring::new(key2)));

    let a = make_with_opts("rot-a", opts_a).await;
    let c = make_with_opts("rot-c", opts_c).await;
    let a_addr = *a.local().addr_ref();

    // Join attempt with the wrong key: the push/pull exchange fails to decode
    // on A's side. C must stay at 1 member (itself only).
    // Ignoring Err: join failure is expected here due to the key mismatch.
    let _ = c
      .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
      .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
      c.num_members(),
      1,
      "join with mismatched key must not add A: c={}",
      c.num_members()
    );

    // Rotate C onto key1 — now both sides share the same key.
    c.set_encryption_options(EncryptionOptions::new().with_keyring(Keyring::new(key1)))
      .await
      .expect("key rotation must succeed");

    // Re-join after rotation: the push/pull now decodes correctly and the
    // cluster converges to 2 members.
    c.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
      .await
      .expect("join after key rotation must succeed");

    let converged = tokio::time::timeout(Duration::from_secs(10), async {
      loop {
        if a.num_members() == 2 && c.num_members() == 2 {
          break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
    })
    .await;
    assert!(
      converged.is_ok(),
      "cluster did not converge after key rotation: a={}, c={}",
      a.num_members(),
      c.num_members()
    );

    // Ignoring Err: best-effort teardown; convergence assertion already passed.
    let _ = a.shutdown().await;
    let _ = c.shutdown().await;
  }

  /// An encrypted `leave()` must put the node's Dead-self broadcast on the wire
  /// before it resolves — the leave-completion fence. The egress transform runs
  /// synchronously on the pump precisely so this fence holds: the leave/shutdown
  /// datagrams reach the socket before the fence fires. This exercises the
  /// encrypted leave end to end — the peer must observe the departure (its
  /// membership drops the leaver).
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn encrypted_leave_reaches_peer() {
    let keyring = Keyring::new(SecretKey::Aes256([0x42u8; 32]));
    let ml_opts =
      MemberlistOptions::new().with_encryption(EncryptionOptions::new().with_keyring(keyring));

    let a = make_with_opts("enc-leave-a", ml_opts.clone()).await;
    let b = make_with_opts("enc-leave-b", ml_opts).await;
    let a_addr = *a.local().addr_ref();

    b.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
      .await
      .expect("join with encryption");
    tokio::time::timeout(Duration::from_secs(10), async {
      loop {
        if a.num_online_members() == 2 && b.num_online_members() == 2 {
          break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
    })
    .await
    .expect("encrypted cluster converges to 2 online");

    // A leaves: `leave()` resolving means its encrypted Dead-self was popped and
    // (inline) sent. B must observe the departure via the leave broadcast — A
    // drops out of B's online membership (`num_members` still lists A as Left
    // until reaped, so check the ONLINE count).
    a.leave().await.expect("encrypted leave");
    let observed = tokio::time::timeout(Duration::from_secs(10), async {
      loop {
        if b.num_online_members() == 1 {
          break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
      }
    })
    .await;
    assert!(
      observed.is_ok(),
      "B did not observe A's encrypted leave (Dead-self not delivered): online={}",
      b.num_online_members(),
    );

    // Ignoring Err: best-effort teardown; the assertion already passed.
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
  }

  /// Construction with an invalid keyring (algorithm not compiled in) is
  /// rejected at construction time rather than silently discarding datagrams.
  ///
  /// This test requires the `chacha20-poly1305` feature to NOT be
  /// built, so that tag 2 is `Unknown`. We emulate that by directly verifying
  /// the `Error::Encryption` path fires when validate_encryption rejects the key.
  ///
  /// For now: verify that an AES-256 key on a node built with
  /// `aes-gcm` constructs cleanly (the positive-control check).
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn valid_keyring_constructs() {
    let key = SecretKey::Aes256([0x11u8; 32]);
    let keyring = Keyring::new(key);
    let enc_opts = EncryptionOptions::new().with_keyring(keyring);
    let ml_opts = MemberlistOptions::new().with_encryption(enc_opts);

    let m = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
      &SocketAddrResolver,
      SmolStr::new("enc-valid"),
      MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
      Options::new().with_memberlist(ml_opts),
      VoidDelegate::<SmolStr, SocketAddr>::new(),
    )
    .await;
    assert!(m.is_ok(), "valid AES-256-GCM keyring must construct");
    // Ignoring Err: best-effort teardown; the construction assertion already passed.
    let _ = m.unwrap().shutdown().await;
  }
}

/// Two nodes built with `MemberlistOptions::default()` (no compression, no
/// encryption, no label) converge to 2 members. This is the regression guard
/// that the transform wiring did not alter the default plaintext/unlabeled path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_options_are_unchanged() {
  let a = make_with_opts("def-a", MemberlistOptions::default()).await;
  let b = make_with_opts("def-b", MemberlistOptions::default()).await;
  let a_addr = *a.local().addr_ref();

  let n = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("default-options join must succeed");
  assert_eq!(n, 1, "one seed contacted");

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
    "default-options cluster did not converge: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  // Ignoring Err: best-effort teardown; convergence assertion already passed.
  let _ = a.shutdown().await;
  let _ = b.shutdown().await;
}

/// The largest plaintext `gossip_mtu` whose on-wire datagram (after the
/// checksum and encryption wrappers) still fits one 65507-byte UDP packet — the
/// reactor construction ceiling. Computed from the same proto constants the
/// driver uses, so the test tracks any wrapper-overhead change automatically.
fn gossip_mtu_ceiling() -> usize {
  65507
    - memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD
    - memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD
}

/// A `gossip_mtu` at exactly the checksum-and-encryption-aware ceiling
/// constructs cleanly: its near-MTU wire datagram is the largest that still fits
/// one UDP packet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gossip_mtu_at_ceiling_constructs() {
  let ml_opts = MemberlistOptions::new().with_gossip_mtu(gossip_mtu_ceiling());
  let m = make_with_opts("mtu-ceiling", ml_opts).await;
  assert_eq!(
    m.num_members(),
    1,
    "node at the gossip_mtu ceiling constructs"
  );
  // Ignoring Err: best-effort teardown; the construction assertion already passed.
  let _ = m.shutdown().await;
}

/// A `gossip_mtu` one byte above the ceiling is rejected at construction with
/// the typed `Error::InvalidGossipMtu`, carrying the configured value and the
/// ceiling — the reactor parity with compio / embedded / smoltcp.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gossip_mtu_above_ceiling_is_rejected() {
  use memberlist_reactor::Error;

  let over = gossip_mtu_ceiling() + 1;
  let ml_opts = MemberlistOptions::new().with_gossip_mtu(over);

  let result = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("mtu-over"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  // Discard a wrongly-constructed handle so the match does not require `Debug`
  // on `Memberlist`; the error path is what this test asserts.
  .map(|_| ());
  match result {
    Err(Error::InvalidGossipMtu(e)) => {
      assert_eq!(e.configured(), over, "carries the configured gossip_mtu");
      assert_eq!(e.ceiling(), gossip_mtu_ceiling(), "carries the ceiling");
    }
    other => panic!("expected Err(InvalidGossipMtu), got {other:?}"),
  }
}

/// A `gossip_mtu` below the mandatory-control-packet floor (e.g. 1 byte) is
/// rejected at construction with the typed `Error::GossipMtuTooSmall`, carrying
/// the configured value and the required minimum — reactor parity with compio.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gossip_mtu_below_floor_is_rejected() {
  use memberlist_reactor::Error;

  let ml_opts = MemberlistOptions::new().with_gossip_mtu(1);

  let result = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("mtu-tiny"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .map(|_| ());
  match result {
    Err(Error::GossipMtuTooSmall(e)) => {
      assert_eq!(e.configured(), 1, "carries the configured gossip_mtu");
      assert!(e.minimum() >= 512, "carries the control-packet floor");
    }
    other => panic!("expected Err(GossipMtuTooSmall), got {other:?}"),
  }
}

/// A zero `max_stream_frame_size` rejects every reliable frame, so it is rejected
/// at construction with the typed `Error::InvalidOption` naming the knob —
/// reactor parity with compio.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_stream_frame_size_zero_is_rejected() {
  use memberlist_reactor::Error;

  let ml_opts = MemberlistOptions::new().with_max_stream_frame_size(0);

  let result = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("frame-zero"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .map(|_| ());
  match result {
    Err(Error::InvalidOption(e)) => {
      assert_eq!(
        e.option(),
        "max_stream_frame_size",
        "names the rejected knob"
      );
    }
    other => panic!("expected Err(InvalidOption), got {other:?}"),
  }
}

/// A `max_stream_frame_size` above the `u32` wire limit is unreachable as a
/// receive gate, so it is rejected at construction with `Error::InvalidOption`
/// naming the knob — reactor parity with compio.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_stream_frame_size_above_u32_is_rejected() {
  use memberlist_reactor::Error;

  let over = (u32::MAX as usize) + 1;
  let ml_opts = MemberlistOptions::new().with_max_stream_frame_size(over);

  let result = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("frame-over"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .map(|_| ());
  match result {
    Err(Error::InvalidOption(e)) => {
      assert_eq!(
        e.option(),
        "max_stream_frame_size",
        "names the rejected knob"
      );
    }
    other => panic!("expected Err(InvalidOption), got {other:?}"),
  }
}

/// A `gossip_mtu` that passes the fixed floor but is too small to carry THIS
/// node's own mandatory control packets (built from the actual id and advertise
/// address) is rejected at construction with `Error::GossipMtuTooSmall`, whose
/// `minimum` reflects the identity-aware required size — reactor parity with
/// compio's identity-aware floor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gossip_mtu_below_identity_floor_is_rejected() {
  use memberlist_reactor::Error;

  // A long node id whose mandatory Ping (two Nodes carrying this id) frames
  // larger than this just-at-the-fixed-floor budget.
  let long_id = "x".repeat(1024);
  let ml_opts = MemberlistOptions::new().with_gossip_mtu(512);

  let result = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new(long_id),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .map(|_| ());
  match result {
    Err(Error::GossipMtuTooSmall(e)) => {
      assert_eq!(
        e.configured(),
        512,
        "carries the effective gossip_mtu budget"
      );
      assert!(
        e.minimum() > 512,
        "the identity-aware required size exceeds the budget"
      );
    }
    other => panic!("expected Err(GossipMtuTooSmall), got {other:?}"),
  }
}

/// Over-long (>253 byte) and non-UTF-8 labels must be rejected at the setter
/// with `Error::InvalidLabel`, not at construction and not via a panic.
#[test]
fn with_label_rejects_invalid_at_setter() {
  use memberlist_reactor::Error;

  let too_long = vec![b'x'; 254];
  let result = MemberlistOptions::new().with_label(Some(too_long));
  assert!(
    matches!(result, Err(Error::InvalidLabel(_))),
    "a label exceeding 253 bytes must be rejected at the setter"
  );

  let non_utf8 = vec![0xff, 0xfe];
  let result = MemberlistOptions::new().with_label(Some(non_utf8));
  assert!(
    matches!(result, Err(Error::InvalidLabel(_))),
    "a non-UTF-8 label must be rejected at the setter"
  );
}

/// `with_skip_inbound_label_check(true)` round-trips through the options and
/// verifies the knob reaches the reliable `LabelOptions`.
///
/// `skip_inbound_label_check` suppresses the inbound-stream missing-label
/// rejection on the ACCEPTING side: a labeled accepting node with skip=true
/// allows a peer that sends no label header to complete a push/pull. For the
/// joiner to also accept the labeled acceptor's outbound stream, the joiner
/// must itself carry a matching label OR have skip=true. The test exercises
/// the common deployment shape: both nodes labeled the same, with skip=true
/// on one to verify the knob round-trips and doesn't break same-label behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_inbound_label_check_round_trips_and_reaches_label_options() {
  // A labeled node with skip_inbound_label_check=true — the key knob under test.
  let opts_a = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid label")
    .with_skip_inbound_label_check(true);
  // Confirm the knob round-tripped.
  assert!(
    opts_a.skip_inbound_label_check(),
    "skip_inbound_label_check must round-trip through MemberlistOptions"
  );

  // A second node with the same label (no skip needed for same-label joins).
  let opts_b = MemberlistOptions::new()
    .with_label(Some(b"cluster-x".to_vec()))
    .expect("valid label");

  let node_a = make_with_opts("skip-a", opts_a).await;
  let node_b = make_with_opts("skip-b", opts_b).await;
  let a_addr = node_a.advertise_address();

  // Same-label join with skip=true on one side must succeed — skip does not
  // break valid labeled exchanges, only relaxes the missing-label check.
  let joined = node_b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await;
  assert!(
    joined.is_ok(),
    "same-label join must succeed with skip_inbound_label_check=true on one side: {:?}",
    joined.err()
  );

  // Gossip convergence confirms the reliable exchange completed end-to-end.
  let converged = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      if node_a.num_members() == 2 && node_b.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    converged.is_ok(),
    "cluster did not converge: a={}, b={}",
    node_a.num_members(),
    node_b.num_members()
  );

  // Ignoring Err: best-effort teardown; convergence assertion already passed.
  let _ = node_a.shutdown().await;
  let _ = node_b.shutdown().await;
}

/// A labeled TCP node must reject a join from a node carrying a different
/// cluster label, isolating the reliable plane just like the gossip plane.
///
/// Node A is labeled "cluster-a". Node B is labeled "cluster-b". B's TCP
/// join attempt to A should fail — A's stream decoder rejects B's label.
/// Two same-label nodes must still join successfully (positive path).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_label_isolates_reliable_plane() {
  let a_opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-a".to_vec()))
    .expect("valid label");
  let b_opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-b".to_vec()))
    .expect("valid label");
  let a2_opts = MemberlistOptions::new()
    .with_label(Some(b"cluster-a".to_vec()))
    .expect("valid label");

  let a = make_with_opts("tcp-iso-a", a_opts).await;
  let b = make_with_opts("tcp-iso-b", b_opts).await;
  let a2 = make_with_opts("tcp-iso-a2", a2_opts).await;
  let a_addr = a.advertise_address();

  // Cross-label join must not produce convergence: B's TCP label mismatch
  // causes A to reject the stream, so the push/pull never completes.
  // Ignoring Err: join failure on the wrong label is expected.
  let _ = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await;
  tokio::time::sleep(Duration::from_millis(300)).await;
  assert_eq!(
    a.num_members(),
    1,
    "cross-label join must not add B: a has {} members",
    a.num_members()
  );
  assert_eq!(
    b.num_members(),
    1,
    "cross-label join must not add A: b has {} members",
    b.num_members()
  );

  // Same-label join must succeed.
  a2.join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("same-label TCP join must succeed");

  let converged = tokio::time::timeout(Duration::from_secs(10), async {
    loop {
      if a.num_members() == 2 && a2.num_members() == 2 {
        break;
      }
      tokio::time::sleep(Duration::from_millis(50)).await;
    }
  })
  .await;
  assert!(
    converged.is_ok(),
    "same-label cluster did not converge: a={}, a2={}",
    a.num_members(),
    a2.num_members()
  );

  // Ignoring Err: best-effort teardown; convergence assertion already passed.
  let _ = a.shutdown().await;
  let _ = a2.shutdown().await;
  let _ = b.shutdown().await;
}

/// An oversized `initial_local_state` is rejected by the construction gate and
/// surfaced as `Err`, not a panic from the public async constructor — the
/// reactor now drives the machine through the fallible `Endpoint::try_new`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_initial_local_state_is_rejected_not_panic() {
  use memberlist_reactor::{EndpointInitError, Error};

  // A frame cap just above the 1 MiB membership reserve leaves a tiny budget; an
  // 8 KiB snapshot's framed PushPull overflows it.
  let snapshot = bytes::Bytes::from(vec![0u8; 8 * 1024]);
  let ml_opts = MemberlistOptions::new()
    .with_max_stream_frame_size(1024 * 1024 + 1024)
    .with_initial_local_state(snapshot);

  let result = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("snap-big"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    Options::new().with_memberlist(ml_opts),
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .map(|_| ());
  assert!(
    matches!(
      result,
      Err(Error::EndpointInit(
        EndpointInitError::LocalStateExceedsFrame(_)
      ))
    ),
    "an oversized initial_local_state must return Err, not panic; got {result:?}"
  );
}
