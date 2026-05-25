//! Smoke tests for TcpMemberlist — construct, snapshot, shutdown.
//!
//! Exercises the single-node lifecycle: bind the UDP gossip socket, publish
//! the initial snapshot, verify the observable counts, and clean up. No peer
//! connections are attempted; multi-node cluster tests are future scope.

#![cfg(feature = "tcp")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use memberlist_compio::{MemberlistError, TcpMemberlist};
use memberlist_machine::{TcpOptions, config::EndpointConfig};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

#[compio::test]
async fn construct_returns_handle_with_initial_snapshot() {
  let config = EndpointConfig::new(SmolStr::new("n-7100"), loopback_addr(7100));
  let opts = TcpOptions::new(Some(b"cluster-x".to_vec()));
  let m = TcpMemberlist::new(config, opts).await.expect("construct");

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
  let config = EndpointConfig::new(SmolStr::new("n-7101"), loopback_addr(7101));
  let opts = TcpOptions::new(Some(b"cluster-x".to_vec()));
  let m = TcpMemberlist::new(config, opts).await.expect("construct");

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
  let addr = loopback_addr(7102);
  let config1 = EndpointConfig::new(SmolStr::new("n-7102a"), addr);
  let config2 = EndpointConfig::new(SmolStr::new("n-7102b"), addr);

  let m1 = TcpMemberlist::new(config1, TcpOptions::new(None))
    .await
    .expect("first bind");
  // Use map_err + unwrap instead of expect_err — Memberlist<..> does not
  // implement Debug, so expect_err's T: Debug bound would fail to compile.
  let err = TcpMemberlist::new(config2, TcpOptions::new(None))
    .await
    .map(|_| ())
    .expect_err("second bind on same port must fail");
  assert!(
    matches!(err, MemberlistError::Io(_)),
    "expected Io error, got {err:?}"
  );

  m1.shutdown().await.expect("shutdown");
}
