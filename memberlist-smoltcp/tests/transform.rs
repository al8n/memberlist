//! Gossip transform layer: compression and encryption applied to the UDP
//! gossip plane (and, via the machine's bridge, the reliable plane).
//!
//! A [`TransformOptions`] carrying a `Keyring` makes every gossip datagram an
//! AEAD-encrypted frame; one carrying a `CompressionOptions` packs it. Both are
//! cross-transport and both round-trip end to end. Critically, an encrypted
//! node refuses a plaintext datagram (strict-mode ingress), so a node outside
//! the keyring cannot inject SWIM state — the trust boundary.
//!
//! These tests reuse the deterministic paired-`Device` + virtual-clock harness
//! from `gossip.rs`. Each node is seeded with the other as Alive (bypassing the
//! join path, as in `gossip.rs`); whether the pair STAYS converged then depends
//! entirely on whether their gossip/probe datagrams survive the transform layer.

mod harness;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use memberlist_proto::{EndpointOptions, Instant};
use memberlist_smoltcp::{
  CompressAlgorithm, CompressionOptions, EncryptionOptions, Keyring, LabelError, Memberlist,
  Options, SecretKey, TransformOptions,
};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// Short-timeout `EndpointOptions` for deterministic virtual-time convergence
/// (identical timings to `gossip.rs`): probes every 100 ms, 50 ms ack window,
/// suspicion fires within 400 ms, gossip every 50 ms. A node that can no longer
/// read its peer's datagrams therefore demotes that peer to Dead within ~550 ms.
fn mk(id: &str, ip: u8) -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
    .with_rng_seed(ip as u64)
    .with_probe_interval(Duration::from_millis(100))
    .with_probe_timeout(Duration::from_millis(50))
    .with_suspicion_mult(2)
    .with_suspicion_max_timeout_mult(2)
    .with_gossip_interval(Duration::from_millis(50))
}

/// A single shared 256-bit AES-GCM key — the cluster secret both encrypted
/// nodes hold.
fn shared_encryption() -> EncryptionOptions {
  let key = SecretKey::Aes256([0x42; 32]);
  EncryptionOptions::new().with_keyring(Keyring::new(key))
}

/// Both nodes share a keyring: their AES-GCM-wrapped gossip round-trips, so the
/// pair converges and STAYS Alive across a window long enough that a broken
/// transform (dropped datagrams) would have demoted each peer to Dead.
#[test]
fn encrypted_gossip_round_trips() {
  // 80 ticks × 10 ms = 800 ms — past the ~550 ms worst-case probe→dead path, so
  // "still Alive" is a real signal that the encrypted probes/acks got through.
  const TICKS: u32 = 80;

  let (mut dev_a, mut dev_b) = harness::link(1500);
  let mut clock = harness::Clock::new();
  let now: Instant = clock.now();

  let enc = shared_encryption();
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default().with_encryption(enc.clone()),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default().with_encryption(enc),
    mk("b", 2),
    &mut dev_b,
    now,
  );

  a.start(now);
  b.start(now);
  a.inject_alive(SmolStr::new("b"), addr(2, 7946), clock.now());
  b.inject_alive(SmolStr::new("a"), addr(1, 7946), clock.now());

  for _ in 0..TICKS {
    let _ = a.poll(clock.now(), &mut dev_a);
    let _ = b.poll(clock.now(), &mut dev_b);
    clock.advance_ms(10);
  }

  assert_eq!(a.num_members(), 2, "A must still see 2 members");
  assert_eq!(b.num_members(), 2, "B must still see 2 members");
  assert!(
    a.is_alive(&SmolStr::new("b")),
    "A must keep B Alive — encrypted gossip/probes must round-trip"
  );
  assert!(
    b.is_alive(&SmolStr::new("a")),
    "B must keep A Alive — encrypted gossip/probes must round-trip"
  );
}

/// Trust boundary: an encrypted node refuses a plaintext peer's datagrams.
///
/// A holds the keyring; B is plaintext (default `TransformOptions`). A emits
/// AES-GCM frames B cannot parse, and A's strict-mode ingress drops B's
/// plaintext frames — so neither node can read the other. A's failure detector
/// therefore demotes B: A does NOT keep B Alive. (With both plaintext — the
/// pre-transform behaviour — A would have happily kept B Alive, which is the
/// hole this test closes.)
#[test]
fn encrypted_node_rejects_plaintext_gossip() {
  // Long enough for A's probe→suspect→dead path (~550 ms worst case) to run to
  // completion against the unreachable plaintext peer.
  const TICKS: u32 = 120;

  let (mut dev_a, mut dev_b) = harness::link(1500);
  let mut clock = harness::Clock::new();
  let now: Instant = clock.now();

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default().with_encryption(shared_encryption()),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  // B is plaintext: no keyring, no compression, no label.
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    mk("b", 2),
    &mut dev_b,
    now,
  );

  a.start(now);
  b.start(now);
  // Seed BOTH so the only thing that can keep B Alive on A is a readable
  // datagram from B — which, being plaintext, A rejects.
  a.inject_alive(SmolStr::new("b"), addr(2, 7946), clock.now());
  b.inject_alive(SmolStr::new("a"), addr(1, 7946), clock.now());

  for _ in 0..TICKS {
    let _ = a.poll(clock.now(), &mut dev_a);
    let _ = b.poll(clock.now(), &mut dev_b);
    clock.advance_ms(10);
  }

  assert!(
    !a.is_alive(&SmolStr::new("b")),
    "A (encrypted) must NOT keep B (plaintext) Alive — A rejects B's \
     unencrypted datagrams, so its failure detector demotes B"
  );
  assert!(
    a.is_dead(&SmolStr::new("b")),
    "A's failure detector must drive the unreachable plaintext peer B to Dead"
  );
}

/// A cluster label isolates gossip traffic between differently-labelled nodes.
///
/// The smoltcp engine stamps the configured TCP-options label onto every
/// outbound gossip datagram AND rejects inbound datagrams that do not carry
/// the matching label. This test exercises both enforcement edges end-to-end
/// through the paired-device harness:
///
/// - **Cross-label isolation**: an "alpha"-labelled node and an unlabelled
///   node pre-seed each other as Alive.  Because the alpha node rejects the
///   unlabelled node's gossip and probes, its failure detector eventually
///   drives the unlabelled peer to Dead (the only state it can reach when
///   every inbound packet is discarded).
///
/// - **Same-label convergence**: two "alpha"-labelled nodes pre-seed each
///   other and exchange labelled gossip across the virtual link; both stay
///   Alive for the full discriminating window.
#[test]
fn gossip_label_isolates_clusters() {
  // ── Cross-label isolation ──────────────────────────────────────────────
  // 150 ticks × 10 ms = 1 500 ms — past the ~550 ms worst-case
  // probe→suspect→dead path, so "Dead" is a real failure-detection signal.
  const ISOLATION_TICKS: u32 = 150;

  let (mut dev_alpha, mut dev_plain) = harness::link(1500);
  let mut clock = harness::Clock::new();
  let now: Instant = clock.now();

  let transform_alpha = TransformOptions::default()
    .with_label(Some(b"alpha".to_vec()))
    .expect("valid label");

  let mut alpha: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    transform_alpha,
    mk("alpha", 1),
    &mut dev_alpha,
    now,
  );
  // Plaintext node: no label, so alpha rejects its gossip and probes.
  let mut plain: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    mk("plain", 2),
    &mut dev_plain,
    now,
  );

  alpha.start(now);
  plain.start(now);
  alpha.inject_alive(SmolStr::new("plain"), addr(2, 7946), clock.now());
  plain.inject_alive(SmolStr::new("alpha"), addr(1, 7946), clock.now());

  for _ in 0..ISOLATION_TICKS {
    let _ = alpha.poll(clock.now(), &mut dev_alpha);
    let _ = plain.poll(clock.now(), &mut dev_plain);
    clock.advance_ms(10);
  }

  assert!(
    alpha.is_dead(&SmolStr::new("plain")),
    "alpha (labelled) must drive the unlabelled plain node to Dead — \
     every inbound probe/gossip from plain is rejected by the label check"
  );

  // ── Same-label convergence ─────────────────────────────────────────────
  // 80 ticks × 10 ms = 800 ms — past the ~550 ms worst-case dead path, so
  // "still Alive" is a real signal that labelled gossip round-trips.
  const CONVERGENCE_TICKS: u32 = 80;

  let (mut dev_a, mut dev_b) = harness::link(1500);
  let mut clock2 = harness::Clock::new();
  let now2: Instant = clock2.now();

  let transform_a = TransformOptions::default()
    .with_label(Some(b"alpha".to_vec()))
    .expect("valid label");
  let transform_b = TransformOptions::default()
    .with_label(Some(b"alpha".to_vec()))
    .expect("valid label");

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3))),
    transform_a,
    mk("a", 3),
    &mut dev_a,
    now2,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4))),
    transform_b,
    mk("b", 4),
    &mut dev_b,
    now2,
  );

  a.start(now2);
  b.start(now2);
  a.inject_alive(SmolStr::new("b"), addr(4, 7946), clock2.now());
  b.inject_alive(SmolStr::new("a"), addr(3, 7946), clock2.now());

  for _ in 0..CONVERGENCE_TICKS {
    let _ = a.poll(clock2.now(), &mut dev_a);
    let _ = b.poll(clock2.now(), &mut dev_b);
    clock2.advance_ms(10);
  }

  assert!(
    a.is_alive(&SmolStr::new("b")),
    "two alpha-labelled nodes must keep each other Alive — \
     labelled gossip/probes must round-trip"
  );
  assert!(
    b.is_alive(&SmolStr::new("a")),
    "two alpha-labelled nodes must keep each other Alive — \
     labelled gossip/probes must round-trip"
  );
}

/// Runtime key rotation takes effect: a node whose keyring is changed at
/// runtime no longer gossips under the old key.
///
/// The test exercises two scenarios end-to-end through the paired-device
/// harness:
///
/// - **Mismatch → Dead**: A(key1) and C(key2) are pre-seeded as Alive.
///   Because they encrypt with different keys, neither can read the other's
///   gossip or probes. A's failure detector therefore drives C to Dead within
///   the failure window.
///
/// - **Pre-rotation convergence**: D starts with key2, but
///   `set_encryption_options(key1)` is called BEFORE any gossip goes out.
///   The first datagram D emits is therefore already key1-encrypted, and A
///   (which holds key1) can read it. Both nodes stay Alive for the full
///   discriminating window — proving the rotation is reflected immediately on
///   the next gossip emission.
#[test]
fn runtime_set_encryption_rotates_key() {
  let key1 = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0xAA; 32])));
  let key2 = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0xBB; 32])));

  // ── Mismatch → Dead ────────────────────────────────────────────────────
  // 150 ticks × 10 ms = 1 500 ms — past the ~550 ms worst-case dead path.
  const FAIL_TICKS: u32 = 150;

  let (mut dev_a, mut dev_c) = harness::link(1500);
  let mut clock = harness::Clock::new();
  let now: Instant = clock.now();

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default().with_encryption(key1.clone()),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  let mut c: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3))),
    TransformOptions::default().with_encryption(key2.clone()),
    mk("c", 3),
    &mut dev_c,
    now,
  );

  a.start(now);
  c.start(now);
  a.inject_alive(SmolStr::new("c"), addr(3, 7946), clock.now());
  c.inject_alive(SmolStr::new("a"), addr(1, 7946), clock.now());

  for _ in 0..FAIL_TICKS {
    let _ = a.poll(clock.now(), &mut dev_a);
    let _ = c.poll(clock.now(), &mut dev_c);
    clock.advance_ms(10);
  }

  assert!(
    a.is_dead(&SmolStr::new("c")),
    "A (key1) must drive C (key2) to Dead — mismatched-key gossip is \
     dropped by the AEAD layer, so the failure detector fires"
  );

  // ── Pre-rotation convergence ───────────────────────────────────────────
  // D starts with key2, then rotates to key1 BEFORE any gossip is emitted;
  // the first on-wire frame is therefore already key1-encrypted and A
  // (re-constructed here with a fresh device and clock) can read it.
  // 80 ticks × 10 ms = 800 ms — past the ~550 ms worst-case dead path, so
  // "still Alive" is a real signal.
  const PASS_TICKS: u32 = 80;

  let (mut dev_a2, mut dev_d) = harness::link(1500);
  let mut clock2 = harness::Clock::new();
  let now2: Instant = clock2.now();

  let mut a2: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))),
    TransformOptions::default().with_encryption(key1.clone()),
    mk("a2", 5),
    &mut dev_a2,
    now2,
  );
  let mut d: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 6))),
    // D starts with key2 …
    TransformOptions::default().with_encryption(key2.clone()),
    mk("d", 6),
    &mut dev_d,
    now2,
  );

  // … but rotates to key1 before the first poll, so every datagram D
  // emits is key1-encrypted from the very first tick.
  d.set_encryption_options(key1.clone())
    .expect("key rotation to key1 must succeed");

  a2.start(now2);
  d.start(now2);
  a2.inject_alive(SmolStr::new("d"), addr(6, 7946), clock2.now());
  d.inject_alive(SmolStr::new("a2"), addr(5, 7946), clock2.now());

  for _ in 0..PASS_TICKS {
    let _ = a2.poll(clock2.now(), &mut dev_a2);
    let _ = d.poll(clock2.now(), &mut dev_d);
    clock2.advance_ms(10);
  }

  assert!(
    a2.is_alive(&SmolStr::new("d")),
    "A2 (key1) must keep D Alive after D pre-rotated to key1 — \
     set_encryption_options takes effect on the next gossip emission"
  );
  assert!(
    d.is_alive(&SmolStr::new("a2")),
    "D (rotated to key1) must keep A2 Alive — \
     set_encryption_options takes effect on the next gossip emission"
  );
}

/// Both nodes enable LZ4 compression: their compressed gossip round-trips, so
/// the pair converges and STAYS Alive across a discriminating window.
#[test]
fn compressed_gossip_round_trips() {
  const TICKS: u32 = 80;

  let (mut dev_a, mut dev_b) = harness::link(1500);
  let mut clock = harness::Clock::new();
  let now: Instant = clock.now();

  // `CompressionOptions` is `Copy`, so both nodes take the same value directly.
  let comp = CompressionOptions::new().with_algorithm(CompressAlgorithm::Lz4);
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default().with_compression(comp),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default().with_compression(comp),
    mk("b", 2),
    &mut dev_b,
    now,
  );

  a.start(now);
  b.start(now);
  a.inject_alive(SmolStr::new("b"), addr(2, 7946), clock.now());
  b.inject_alive(SmolStr::new("a"), addr(1, 7946), clock.now());

  for _ in 0..TICKS {
    let _ = a.poll(clock.now(), &mut dev_a);
    let _ = b.poll(clock.now(), &mut dev_b);
    clock.advance_ms(10);
  }

  assert_eq!(a.num_members(), 2, "A must still see 2 members");
  assert_eq!(b.num_members(), 2, "B must still see 2 members");
  assert!(
    a.is_alive(&SmolStr::new("b")),
    "A must keep B Alive — compressed gossip/probes must round-trip"
  );
  assert!(
    b.is_alive(&SmolStr::new("a")),
    "B must keep A Alive — compressed gossip/probes must round-trip"
  );
}

/// Over-long or non-UTF-8 labels are rejected at the `TransformOptions` setter
/// with `LabelError`, not at construction and not via a panic.
#[test]
fn with_label_rejects_invalid_at_setter() {
  let too_long = vec![b'x'; 254];
  let result = TransformOptions::default().with_label(Some(too_long));
  assert!(
    matches!(result, Err(LabelError::TooLong)),
    "a label exceeding 253 bytes must be rejected at the setter"
  );

  let non_utf8 = vec![0xff, 0xfe];
  let result = TransformOptions::default().with_label(Some(non_utf8));
  assert!(
    matches!(result, Err(LabelError::NotUtf8)),
    "a non-UTF-8 label must be rejected at the setter"
  );
}
