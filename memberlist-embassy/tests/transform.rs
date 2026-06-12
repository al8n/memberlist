//! Transform (encryption / label) integration tests for the embassy driver.
//!
//! Three tests exercise the gossip and reliable planes through real
//! embassy-net stacks wired by a channel-backed paired [`Driver`], driven
//! under one [`block_on`] raced against a wall-clock timeout. Each test
//! targets one distinct security invariant that must hold end-to-end through
//! the full driver, not just in unit isolation.

mod support;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration as StdDuration,
};
use std::time::Instant as StdInstant;

use embassy_futures::select::{Either, select};
use embassy_net::{
  Config as NetConfig, Ipv4Cidr, Runner as NetRunner, Stack, StackResources, StaticConfigV4,
  tcp::TcpSocket,
  udp::{PacketMetadata, UdpSocket},
};
use embassy_time::{Duration, Timer};
use futures::executor::block_on;
use memberlist_embassy::{
  EncryptionOptions, EndpointOptions, Keyring, LabelError, Memberlist, Options, Runner, SecretKey,
  TransformOptions, now,
};
use smol_str::SmolStr;

use support::paired_device::{PairedDevice, pair};

/// TCP socket pool size per node (a listener plus dial/accept sockets).
const POOL: usize = 4;
/// Per-TCP-socket rx/tx buffer bytes.
const TCP_BUF: usize = 4096;
/// Wall-clock cap on each test so a wedged plane fails fast.
const TEST_TIMEOUT: Duration = Duration::from_secs(8);

fn addr(last: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(169, 254, 1, last)), port)
}

/// All the owned buffers one node's sockets borrow for the whole `block_on`.
struct NodeBufs {
  udp_rx_meta: [PacketMetadata; 16],
  udp_rx: [u8; 16 * 1024],
  udp_tx_meta: [PacketMetadata; 16],
  udp_tx: [u8; 16 * 1024],
  tcp_rx: [[u8; TCP_BUF]; POOL],
  tcp_tx: [[u8; TCP_BUF]; POOL],
}

impl NodeBufs {
  fn new() -> Self {
    Self {
      udp_rx_meta: [PacketMetadata::EMPTY; 16],
      udp_rx: [0u8; 16 * 1024],
      udp_tx_meta: [PacketMetadata::EMPTY; 16],
      udp_tx: [0u8; 16 * 1024],
      tcp_rx: [[0u8; TCP_BUF]; POOL],
      tcp_tx: [[0u8; TCP_BUF]; POOL],
    }
  }
}

/// Build one node's `UdpSocket` + `[TcpSocket; POOL]` over its `Stack` and bufs.
fn build_sockets<'a>(
  stack: Stack<'a>,
  bufs: &'a mut NodeBufs,
) -> (UdpSocket<'a>, [TcpSocket<'a>; POOL]) {
  let udp = UdpSocket::new(
    stack,
    &mut bufs.udp_rx_meta,
    &mut bufs.udp_rx,
    &mut bufs.udp_tx_meta,
    &mut bufs.udp_tx,
  );
  let mut rx_iter = bufs.tcp_rx.iter_mut();
  let mut tx_iter = bufs.tcp_tx.iter_mut();
  let tcp = core::array::from_fn::<_, POOL, _>(|_| {
    let rx = rx_iter.next().expect("POOL rx buffers");
    let tx = tx_iter.next().expect("POOL tx buffers");
    TcpSocket::new(stack, rx, tx)
  });
  (udp, tcp)
}

/// Build a static-IPv4 embassy-net stack over a paired device.
fn build_stack<'a>(
  device: PairedDevice,
  resources: &'a mut StackResources<{ POOL + 2 }>,
  last: u8,
  seed: u64,
) -> (Stack<'a>, NetRunner<'a, PairedDevice>) {
  let config = NetConfig::ipv4_static(StaticConfigV4 {
    address: Ipv4Cidr::new(Ipv4Addr::new(169, 254, 1, last), 16),
    gateway: None,
    dns_servers: Default::default(),
  });
  embassy_net::new(device, config, resources, seed)
}

/// Drive `op` to completion against both memberlist run loops, both
/// embassy-net stack run loops, and the test timeout. Returns the op's
/// value, or panics on timeout.
async fn drive<T>(
  op: impl core::future::Future<Output = T>,
  ml_a: Runner<'_, SmolStr, POOL>,
  ml_b: Runner<'_, SmolStr, POOL>,
  net_a: &mut NetRunner<'_, PairedDevice>,
  net_b: &mut NetRunner<'_, PairedDevice>,
) -> T {
  let nets = select(net_a.run(), net_b.run());
  let mls = select(ml_a.run(), ml_b.run());
  let infra = select(nets, mls);
  match select(op, select(infra, Timer::after(TEST_TIMEOUT))).await {
    Either::First(v) => v,
    Either::Second(_) => panic!("test timed out after {TEST_TIMEOUT:?}"),
  }
}

/// A shared AES-256-GCM key used across the encryption tests.
fn shared_key() -> EncryptionOptions {
  let key = SecretKey::Aes256([0x42u8; 32]);
  EncryptionOptions::new().with_keyring(Keyring::new(key))
}

/// Both nodes share the same AES-256-GCM keyring; their encrypted gossip and
/// reliable-plane messages round-trip, so the pair converges via join and
/// STAYS converged on a 2-member view.
///
/// The embassy driver routes every gossip datagram through the engine's
/// encryption codec. A broken codec (or a key mismatch) would prevent the
/// join push/pull exchange from decoding, so a successful join + 2-member
/// view is a real end-to-end encrypted-path assertion.
#[test]
fn encrypted_gossip_round_trips() {
  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

  let enc = shared_key();
  let clock = now();
  let (ml_a, run_a) = Memberlist::new::<POOL>(
    Options::new(),
    TransformOptions::default().with_encryption(enc.clone()),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    udp_a,
    tcp_a,
    clock,
  )
  .expect("build node a");
  let (ml_b, run_b) = Memberlist::new::<POOL>(
    Options::new(),
    TransformOptions::default().with_encryption(enc),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946)).with_rng_seed(2),
    udp_b,
    tcp_b,
    clock,
  )
  .expect("build node b");

  let start = StdInstant::now();
  block_on(async {
    let op = async {
      ml_b
        .join(&[addr(1, 7946)])
        .await
        .expect("join from a running node");
      loop {
        if ml_a.num_members() == 2 && ml_b.num_members() == 2 {
          break;
        }
        Timer::after(Duration::from_millis(10)).await;
      }
    };
    drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
  });

  assert_eq!(ml_a.num_members(), 2, "A did not converge to 2 members");
  assert_eq!(ml_b.num_members(), 2, "B did not converge to 2 members");
  println!(
    "encrypted_gossip_round_trips: converged in {:?}",
    start.elapsed()
  );
}

/// Both nodes apply the same gossip-plane CRC32 checksum; their checksummed
/// gossip and push/pull exchanges round-trip, so the pair converges via join
/// and STAYS converged on a 2-member view.
///
/// The embassy driver wraps every outbound gossip datagram with the engine's
/// checksum transform and verifies the digest on receipt. A broken wrap/verify
/// (or a digest mismatch) would drop the join push/pull exchange, so a
/// successful join + 2-member view is a real end-to-end checksummed-gossip
/// assertion. Checksum is a gossip-plane transform only; the reliable stream
/// path carries no checksum.
///
/// Gated on `checksum-crc32`: constructing with `Crc32` while its backend is
/// absent now fails fast at `Engine::try_new_at`, so the test must enable the
/// backend it exercises.
#[cfg(feature = "checksum-crc32")]
#[test]
fn checksummed_gossip_round_trips() {
  use memberlist_embassy::{ChecksumAlgorithm, ChecksumOptions};

  let (dev_a, dev_b) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_b = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 2, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_b = NodeBufs::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

  let checksum = ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32);
  let clock = now();
  let (ml_a, run_a) = Memberlist::new::<POOL>(
    Options::new(),
    TransformOptions::default().with_checksum(checksum),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    udp_a,
    tcp_a,
    clock,
  )
  .expect("build node a");
  let (ml_b, run_b) = Memberlist::new::<POOL>(
    Options::new(),
    TransformOptions::default().with_checksum(checksum),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946)).with_rng_seed(2),
    udp_b,
    tcp_b,
    clock,
  )
  .expect("build node b");

  let start = StdInstant::now();
  block_on(async {
    let op = async {
      ml_b
        .join(&[addr(1, 7946)])
        .await
        .expect("join from a running node");
      loop {
        if ml_a.num_members() == 2 && ml_b.num_members() == 2 {
          break;
        }
        Timer::after(Duration::from_millis(10)).await;
      }
    };
    drive(op, run_a, run_b, &mut net_a, &mut net_b).await;
  });

  assert_eq!(ml_a.num_members(), 2, "A did not converge to 2 members");
  assert_eq!(ml_b.num_members(), 2, "B did not converge to 2 members");
  println!(
    "checksummed_gossip_round_trips: converged in {:?}",
    start.elapsed()
  );
}

/// A cluster label isolates gossip traffic across differently-labelled
/// embassy nodes.
///
/// The engine stamps every outbound gossip datagram AND the reliable-plane
/// stream handshake with the configured TCP-options label. Both sides reject
/// traffic that does not carry the matching label.
///
/// The test exercises two scenarios through real embassy-net stacks:
///
/// - **Cross-label isolation**: an "alpha" node and an unlabelled node are
///   pre-seeded as Alive via `inject_alive`. Fast probe timers (100 ms
///   interval) ensure failure detection completes within the wall-clock
///   budget. The alpha node rejects the unlabelled node's gossip and probes
///   (DoubleLabel), so its failure detector eventually drives the unlabelled
///   peer to NOT-Alive. After the failure window, alpha must NOT still
///   consider the unlabelled node Alive.
///
/// - **Same-label convergence**: two "alpha"-labelled nodes join each other
///   over the TCP push/pull path (same label passes the inbound check on
///   both sides). Both converge to 2 members.
#[test]
fn gossip_label_isolates_clusters() {
  // ── Cross-label isolation ──────────────────────────────────────────────
  // Fast probe timers: probe_interval=100ms, probe_timeout=50ms,
  // suspicion_mult=2 → worst-case Dead ≈ 550ms. Run the infra for 2 000ms
  // (3.6× margin) so the failure detector has time to fire.
  {
    let (dev_alpha, dev_plain) = pair();
    let mut res_alpha = StackResources::<{ POOL + 2 }>::new();
    let mut res_plain = StackResources::<{ POOL + 2 }>::new();
    let (stack_alpha, mut net_alpha) = build_stack(dev_alpha, &mut res_alpha, 1, 0xAAAA_0001);
    let (stack_plain, mut net_plain) = build_stack(dev_plain, &mut res_plain, 2, 0xBBBB_0002);

    let mut bufs_alpha = NodeBufs::new();
    let mut bufs_plain = NodeBufs::new();
    let (udp_alpha, tcp_alpha) = build_sockets(stack_alpha, &mut bufs_alpha);
    let (udp_plain, tcp_plain) = build_sockets(stack_plain, &mut bufs_plain);

    let transform_alpha = TransformOptions::default()
      .with_label(Some(b"alpha".to_vec()))
      .expect("valid label");
    let clock = now();
    let (ml_alpha, run_alpha) = Memberlist::new::<POOL>(
      Options::new(),
      transform_alpha,
      // Short probe timers so failure detection completes within 2 000ms.
      EndpointOptions::new(SmolStr::new("alpha"), addr(1, 7946))
        .with_rng_seed(1)
        .with_probe_interval(StdDuration::from_millis(100))
        .with_probe_timeout(StdDuration::from_millis(50))
        .with_suspicion_mult(2)
        .with_suspicion_max_timeout_mult(2)
        .with_gossip_interval(StdDuration::from_millis(50)),
      udp_alpha,
      tcp_alpha,
      clock,
    )
    .expect("build alpha node");
    let (ml_plain, run_plain) = Memberlist::new::<POOL>(
      Options::new(),
      TransformOptions::default(),
      EndpointOptions::new(SmolStr::new("plain"), addr(2, 7946))
        .with_rng_seed(2)
        .with_probe_interval(StdDuration::from_millis(100))
        .with_probe_timeout(StdDuration::from_millis(50))
        .with_suspicion_mult(2)
        .with_suspicion_max_timeout_mult(2)
        .with_gossip_interval(StdDuration::from_millis(50)),
      udp_plain,
      tcp_plain,
      clock,
    )
    .expect("build plain node");

    // Pre-seed each node with the other via inject_alive, then run the infra.
    // The alpha node emits "alpha"-labelled gossip and probes; plain, which
    // expects no label, drops them (DoubleLabel) and cannot keep alpha alive.
    // Alpha drops plain's unlabelled probes (LabelMismatch), driving plain Dead.
    ml_alpha.inject_alive(SmolStr::new("plain"), addr(2, 7946));
    ml_plain.inject_alive(SmolStr::new("alpha"), addr(1, 7946));

    // Run the infra for 2 000ms — well past the ~550ms worst-case Dead path.
    block_on(async {
      let nets = select(net_alpha.run(), net_plain.run());
      let mls = select(run_alpha.run(), run_plain.run());
      let _ = select(select(nets, mls), Timer::after(Duration::from_millis(2000))).await;
    });

    assert!(
      !ml_alpha.is_alive(&SmolStr::new("plain")),
      "alpha (labelled) must NOT keep plain (unlabelled) Alive after 2 000ms \
       — label mismatch rejects every probe; got is_alive=true"
    );
  }

  // ── Same-label convergence ─────────────────────────────────────────────
  {
    let (dev_a, dev_b) = pair();
    let mut res_a = StackResources::<{ POOL + 2 }>::new();
    let mut res_b = StackResources::<{ POOL + 2 }>::new();
    let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 3, 0xAAAA_0003);
    let (stack_b, mut net_b) = build_stack(dev_b, &mut res_b, 4, 0xAAAA_0004);

    let mut bufs_a = NodeBufs::new();
    let mut bufs_b = NodeBufs::new();
    let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
    let (udp_b, tcp_b) = build_sockets(stack_b, &mut bufs_b);

    let transform_a2 = TransformOptions::default()
      .with_label(Some(b"alpha".to_vec()))
      .expect("valid label");
    let transform_b2 = TransformOptions::default()
      .with_label(Some(b"alpha".to_vec()))
      .expect("valid label");

    let clock = now();
    let (ml_a2, run_a2) = Memberlist::new::<POOL>(
      Options::new(),
      transform_a2,
      EndpointOptions::new(SmolStr::new("a2"), addr(3, 7946)).with_rng_seed(3),
      udp_a,
      tcp_a,
      clock,
    )
    .expect("build a2 node");
    let (ml_b2, run_b2) = Memberlist::new::<POOL>(
      Options::new(),
      transform_b2,
      EndpointOptions::new(SmolStr::new("b2"), addr(4, 7946)).with_rng_seed(4),
      udp_b,
      tcp_b,
      clock,
    )
    .expect("build b2 node");

    let start = StdInstant::now();
    block_on(async {
      let op = async {
        ml_b2
          .join(&[addr(3, 7946)])
          .await
          .expect("join from a running node");
        loop {
          if ml_a2.num_members() == 2 && ml_b2.num_members() == 2 {
            break;
          }
          Timer::after(Duration::from_millis(10)).await;
        }
      };
      drive(op, run_a2, run_b2, &mut net_a, &mut net_b).await;
    });

    assert_eq!(
      ml_a2.num_members(),
      2,
      "same-label alpha nodes must converge to 2 members"
    );
    assert_eq!(
      ml_b2.num_members(),
      2,
      "same-label alpha nodes must converge to 2 members"
    );
    println!(
      "gossip_label_isolates_clusters (same-label): converged in {:?}",
      start.elapsed()
    );
  }
}

/// Runtime key rotation takes effect immediately on the next push/pull
/// exchange.
///
/// The test exercises the fail→pass pattern through a real embassy-net join:
///
/// 1. A(key1) + C(key2): C attempts `join(A)` under a 1 s timeout. The
///    push/pull is encrypted under key2; A cannot decrypt it; the join
///    never completes within the window → the timer fires. C has 1 member.
///
/// 2. C rotates to key1 via `set_encryption_options`.
///
/// 3. C issues `join(A)` again. The push/pull is now key1-encrypted; A
///    decodes it; both converge to 2 members.
///
/// This proves `set_encryption_options` is reflected by the next TCP
/// push/pull attempt, and that fail→pass is achievable within a single
/// test lifetime.
#[test]
fn runtime_set_encryption_rotates_key() {
  let key1 = SecretKey::Aes256([0xAAu8; 32]);
  let key2 = SecretKey::Aes256([0xBBu8; 32]);

  let enc1 = EncryptionOptions::new().with_keyring(Keyring::new(key1));
  let enc2 = EncryptionOptions::new().with_keyring(Keyring::new(key2));
  // Clone enc1 for the rotation call inside the async block (the original is
  // moved into the Memberlist constructor).
  let enc1_for_rotation = enc1.clone();

  let (dev_a, dev_c) = pair();
  let mut res_a = StackResources::<{ POOL + 2 }>::new();
  let mut res_c = StackResources::<{ POOL + 2 }>::new();
  let (stack_a, mut net_a) = build_stack(dev_a, &mut res_a, 1, 0x1111_2222);
  let (stack_c, mut net_c) = build_stack(dev_c, &mut res_c, 3, 0x3333_4444);

  let mut bufs_a = NodeBufs::new();
  let mut bufs_c = NodeBufs::new();
  let (udp_a, tcp_a) = build_sockets(stack_a, &mut bufs_a);
  let (udp_c, tcp_c) = build_sockets(stack_c, &mut bufs_c);

  let clock = now();
  let (ml_a, run_a) = Memberlist::new::<POOL>(
    Options::new(),
    TransformOptions::default().with_encryption(enc1),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    udp_a,
    tcp_a,
    clock,
  )
  .expect("build node a");
  let (ml_c, run_c) = Memberlist::new::<POOL>(
    Options::new(),
    // C starts with key2 — incompatible with A's key1.
    TransformOptions::default().with_encryption(enc2),
    EndpointOptions::new(SmolStr::new("c"), addr(3, 7946)).with_rng_seed(3),
    udp_c,
    tcp_c,
    clock,
  )
  .expect("build node c");

  let start = StdInstant::now();
  block_on(async {
    let op = async {
      // Phase 1: attempt to join with the wrong key. The push/pull
      // is key2-encrypted; A cannot decrypt it and never acks. The join
      // therefore never resolves (it waits for `is_joined()`). We race it
      // against a 1 s window: the timer fires, proving the key mismatch
      // prevents convergence.
      let seed_a = [addr(1, 7946)];
      let join_attempt = ml_c.join(&seed_a);
      match select(join_attempt, Timer::after(Duration::from_millis(1000))).await {
        Either::First(_) => {
          // Join completed — that should not happen with mismatched keys.
          assert_eq!(
            ml_c.num_members(),
            1,
            "join with key2 must NOT reach A (key1); but ml_c joined with \
             {} members — push/pull must have decoded incorrectly",
            ml_c.num_members()
          );
        }
        Either::Second(()) => {
          // Timer fired first: join is still pending, as expected.
          assert_eq!(
            ml_c.num_members(),
            1,
            "C (key2) must NOT have joined A (key1) within the window; \
             got {} members",
            ml_c.num_members()
          );
        }
      }

      // Phase 2: rotate C to key1 — now both sides share the same key.
      ml_c
        .set_encryption_options(enc1_for_rotation)
        .expect("key rotation to key1 must succeed");

      // Phase 3: re-join after rotation — must converge.
      let seed_a2 = [addr(1, 7946)];
      ml_c.join(&seed_a2).await.expect("join from a running node");
      loop {
        if ml_a.num_members() == 2 && ml_c.num_members() == 2 {
          break;
        }
        Timer::after(Duration::from_millis(10)).await;
      }
    };
    drive(op, run_a, run_c, &mut net_a, &mut net_c).await;
  });

  assert_eq!(
    ml_a.num_members(),
    2,
    "A did not converge to 2 members after C rotated to key1"
  );
  assert_eq!(
    ml_c.num_members(),
    2,
    "C did not converge to 2 members after rotating to key1"
  );
  println!(
    "runtime_set_encryption_rotates_key: converged in {:?}",
    start.elapsed()
  );
}

/// Over-long or non-UTF-8 labels are rejected at the `TransformOptions` setter
/// with `LabelError`, not at construction and not via a panic.
#[test]
fn with_label_rejects_invalid_at_setter() {
  let too_long = vec![b'x'; 254];
  let result = TransformOptions::default().with_label(Some(too_long));
  assert!(
    matches!(result, Err(LabelError::TooLong(_))),
    "a label exceeding 253 bytes must be rejected at the setter"
  );

  let non_utf8 = vec![0xff, 0xfe];
  let result = TransformOptions::default().with_label(Some(non_utf8));
  assert!(
    matches!(result, Err(LabelError::NotUtf8)),
    "a non-UTF-8 label must be rejected at the setter"
  );
}
