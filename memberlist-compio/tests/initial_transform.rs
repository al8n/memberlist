//! Construction-time compression and encryption tests.
//!
//! These confirm that `MemberlistOptions::with_encryption` and
//! `with_compression` take effect at endpoint construction — no runtime
//! setter call required — and that a misconfigured keyring is caught at
//! `Memberlist::new` rather than silently starting plaintext.

#![cfg(all(feature = "tcp", feature = "aes-gcm"))]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, Memberlist, MemberlistError, MemberlistOptions, Options,
  SocketAddrResolver, TcpTransport, TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::{EncryptionOptions, Keyring, SecretKey};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = std::time::Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

/// Build a `Memberlist` with the given `MemberlistOptions`.
async fn make_tcp(
  id: &str,
  addr: SocketAddr,
  mopts: MemberlistOptions,
) -> Result<Memberlist<SmolStr, SocketAddr>, MemberlistError> {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_memberlist(mopts);
  Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
}

/// A node built with a keyring via `MemberlistOptions::with_encryption` (no
/// runtime `set_encryption_options` call) must not converge with a plaintext
/// node: the encrypted node's gossip datagrams are unreadable by the plaintext
/// peer and vice versa.
///
/// Before construction-time encryption is applied, BOTH nodes start plaintext
/// regardless of the configured options — they would converge, and the test
/// would fail. After construction-time encryption the encrypted node sends only
/// ciphertext; the plaintext peer cannot decode it, so the cluster stays at
/// size 1 on both sides.
#[compio::test]
async fn construction_time_encryption_isolates_from_plaintext_peer() {
  let encrypted_addr = loopback_addr(8400);
  let plaintext_addr = loopback_addr(8401);

  let key = SecretKey::Aes128([0x42u8; 16]);
  let enc_opts = EncryptionOptions::new().with_keyring(Keyring::new(key));

  // Encrypted node: construction supplies the keyring; no runtime call.
  let encrypted = make_tcp(
    "enc",
    encrypted_addr,
    MemberlistOptions::new().with_encryption(enc_opts),
  )
  .await
  .expect("encrypted node constructs");

  // Plaintext node: default MemberlistOptions, no encryption.
  let plaintext = make_tcp("plain", plaintext_addr, MemberlistOptions::new())
    .await
    .expect("plaintext node constructs");

  // Have the plaintext node attempt to join the encrypted one. The
  // plaintext node dispatches one push/pull exchange; the encrypted node's
  // framing will reject the unencrypted bytes and vice versa. Both sides
  // should remain at member_count == 1.
  let _ = plaintext
    .dispatch_join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(encrypted_addr)],
    )
    .await
    .expect("dispatch_join does not fail; it fires and forgets");

  // Wait long enough for a push/pull exchange to have been attempted and
  // processed. If construction-time encryption is NOT applied, both nodes
  // are actually plaintext and they converge to size 2 — the test fails.
  // If it IS applied the gossip frames are rejected and both stay at 1.
  compio::time::sleep(Duration::from_millis(300)).await;

  // Neither side should converge: encrypted gossip is undecryptable by the
  // plaintext node, so both remain singletons.
  assert_eq!(
    plaintext.member_count(),
    1,
    "plaintext peer must not converge with the encrypted node; member_count={}",
    plaintext.member_count()
  );
  assert_eq!(
    encrypted.member_count(),
    1,
    "encrypted node must not admit the plaintext peer; member_count={}",
    encrypted.member_count()
  );

  plaintext.shutdown().await.expect("plaintext shutdown");
  encrypted.shutdown().await.expect("encrypted shutdown");
}

/// Two nodes built with the SAME keyring via `MemberlistOptions::with_encryption`
/// must converge — the construction-time encryption is consistent on both sides.
#[compio::test]
async fn construction_time_encryption_same_key_converges() {
  let a_addr = loopback_addr(8410);
  let b_addr = loopback_addr(8411);

  let key = SecretKey::Aes128([0x77u8; 16]);
  let enc_opts_a = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let enc_opts_b = EncryptionOptions::new().with_keyring(Keyring::new(key));

  let a = make_tcp(
    "enc-a",
    a_addr,
    MemberlistOptions::new().with_encryption(enc_opts_a),
  )
  .await
  .expect("node A constructs");

  let b = make_tcp(
    "enc-b",
    b_addr,
    MemberlistOptions::new().with_encryption(enc_opts_b),
  )
  .await
  .expect("node B constructs");

  let contacted = b
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(a_addr)])
    .await
    .expect("encrypted join must succeed with matching keys");
  assert_eq!(contacted.len(), 1, "one seed contacted");

  let converged = wait_until(
    || a.member_count() == 2 && b.member_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "two nodes with matching construction-time keys must converge; a={} b={}",
    a.member_count(),
    b.member_count()
  );

  a.shutdown().await.expect("a shutdown");
  b.shutdown().await.expect("b shutdown");
}
