//! Smoke tests for TcpMemberlist — construct, snapshot, shutdown.
//!
//! Exercises the single-node lifecycle: bind the UDP gossip socket, publish
//! the initial snapshot, verify the observable counts, and clean up. No peer
//! connections are attempted; multi-node cluster tests are future scope.

#![cfg(feature = "tcp")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use memberlist_compio::{
  FirstAddrResolver, MaybeResolved, MemberlistError, MemberlistOptions, Options,
  SocketAddrResolver, TcpMemberlist, TcpTransportOptions, VoidDelegate,
};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

/// Build a `TcpMemberlist` advertising `addr` with the given cluster label.
///
/// The membership-input address type is `SocketAddr`, so the construction
/// resolver is the identity `SocketAddrResolver` — never actually invoked
/// here because the advertise address is already `MaybeResolved::Resolved`.
async fn make_tcp(
  id: &str,
  addr: SocketAddr,
  label: Option<Vec<u8>>,
) -> Result<TcpMemberlist<SmolStr, SocketAddr>, MemberlistError> {
  let ml_opts = MemberlistOptions::new().with_label(label)?;
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_memberlist(ml_opts);
  TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
}

#[compio::test]
async fn construct_returns_handle_with_initial_snapshot() {
  let m = make_tcp("node-a", loopback_addr(0), Some(b"cluster-x".to_vec()))
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
  let m = make_tcp("node-b", loopback_addr(0), Some(b"cluster-x".to_vec()))
    .await
    .expect("construct");

  let m2 = m.clone();
  assert_eq!(m2.alive_count(), 1);

  m.shutdown().await.expect("shutdown");
  // m2 holds the Arc<JoinHandle> too; driver task has already exited but
  // cloned handle reads remain valid (just return the final snapshot).
}

/// Verify the error type is well-formed: a port collision surfaces as
/// `MemberlistError::Io`.
#[compio::test]
async fn double_bind_returns_io_error() {
  let m1 = make_tcp("node-dup-1", loopback_addr(0), None)
    .await
    .expect("first bind");
  // The second node targets the first node's OS-assigned address, so the
  // intentional collision still surfaces.
  let addr = m1.advertise_address();
  // Use map_err + unwrap instead of expect_err — Memberlist<..> does not
  // implement Debug, so expect_err's T: Debug bound would fail to compile.
  let err = make_tcp("node-dup-2", addr, None)
    .await
    .map(|_| ())
    .expect_err("second bind on same port must fail");
  assert!(
    matches!(err, MemberlistError::Io(_)),
    "expected Io error, got {err:?}"
  );

  m1.shutdown().await.expect("shutdown");
}
