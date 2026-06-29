use std::net::SocketAddr;

use smol_str::SmolStr;

use super::*;
use crate::{
  FirstAddrResolver, OsResolver, TcpTransport, TcpTransportOptions, delegate::VoidDelegate,
};

/// Build a TCP memberlist bound to an ephemeral loopback port.
async fn spawn_node(id: &str) -> Memberlist<SmolStr, crate::Address> {
  let bind: SocketAddr = "127.0.0.1:0".parse().expect("parse loopback");
  Memberlist::new(
    Options::<TcpTransport<SmolStr, crate::Address>>::new(
      TcpTransportOptions::new()
        .with_local_id(SmolStr::new(id))
        .with_advertise_addr(MaybeResolved::Resolved(bind)),
    ),
    VoidDelegate::default(),
    &OsResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct memberlist")
}

/// Single-node construct + shutdown round trip through `Memberlist::new`.
#[compio::test]
async fn tcp_single_node_construct_and_shutdown() {
  let n1 = spawn_node("solo").await;
  assert_eq!(n1.local_node().id_ref().as_str(), "solo");
  assert_eq!(n1.alive_count(), 1);
  assert_eq!(n1.member_count(), 1);
  n1.shutdown().await.expect("shutdown");
}

/// The gate: two TCP-backed [`Memberlist`] nodes join end-to-end over real
/// TCP, with `n2.join` contacting `n1`.
#[compio::test]
async fn tcp_two_node_join_via_new() {
  let n1 = spawn_node("n1").await;
  // The ephemeral port the OS assigned is recorded in the published
  // snapshot's local node — bind was `127.0.0.1:0`.
  let n1_addr = *n1.local_node().addr_ref();
  assert_ne!(n1_addr.port(), 0, "OS-assigned port should be concrete");

  let n2 = spawn_node("n2").await;
  let contacted = n2
    .join(&OsResolver, &[MaybeResolved::Resolved(n1_addr)])
    .await
    .expect("join n1");
  assert!(
    !contacted.is_empty(),
    "expected at least one contact, got {contacted:?}"
  );

  n1.shutdown().await.ok();
  n2.shutdown().await.ok();
}

// The handle is thread-per-core: its `Rc` / `Cell` / `RefCell` bookkeeping pins
// it to the driver's thread, so `Memberlist` must stay `!Send` — it cannot be
// moved to another thread. This fails to compile if a field is ever switched
// back to a `Send` type (e.g. `Arc`), which would silently break that contract.
#[test]
fn handle_is_not_send() {
  trait AmbiguousIfSend<A> {
    fn check() {}
  }
  impl<T: ?Sized> AmbiguousIfSend<()> for T {}
  impl<T: ?Sized + Send> AmbiguousIfSend<u8> for T {}
  // Resolves only while `Memberlist` is `!Send` (one matching impl); the moment
  // it becomes `Send`, both impls match and this is an ambiguity compile error.
  let _ = <Memberlist<SmolStr, SocketAddr> as AmbiguousIfSend<_>>::check;
}
