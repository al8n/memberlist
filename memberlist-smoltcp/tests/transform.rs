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
//! from `gossip.rs`. Each node is seeded with the other as Alive (Milestone 1
//! has no join path); whether the pair STAYS converged then depends entirely on
//! whether their gossip/probe datagrams survive the transform layer.

mod harness;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use memberlist_proto::{EndpointConfig, Instant};
use memberlist_smoltcp::{
  CompressAlgorithm, CompressionOptions, Config, EncryptionOptions, Keyring, Memberlist, SecretKey,
  TransformOptions,
};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// Short-timeout `EndpointConfig` for deterministic virtual-time convergence
/// (identical timings to `gossip.rs`): probes every 100 ms, 50 ms ack window,
/// suspicion fires within 400 ms, gossip every 50 ms. A node that can no longer
/// read its peer's datagrams therefore demotes that peer to Dead within ~550 ms.
fn mk(id: &str, ip: u8) -> EndpointConfig<SmolStr, SocketAddr> {
  EndpointConfig::new(SmolStr::new(id), addr(ip, 7946))
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
    Config::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default().with_encryption(enc.clone()),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
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
    Config::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default().with_encryption(shared_encryption()),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  // B is plaintext: no keyring, no compression, no label.
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
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
    Config::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default().with_compression(comp),
    mk("a", 1),
    &mut dev_a,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Config::new(),
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
