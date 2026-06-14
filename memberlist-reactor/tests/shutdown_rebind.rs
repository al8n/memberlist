//! Regression: `shutdown().await` must not return until the driver has released
//! its bound ports, so an immediate rebind on the same address succeeds.
//!
//! The reactor TCP driver binds a UDP gossip socket and a TCP listener on one
//! advertise address. If `shutdown()` acked the caller before those file
//! descriptors dropped, a second node constructed on the same address — with no
//! intervening grace period — would race a still-open socket and fail with
//! `AddrInUse`. This test pins that ordering: it constructs a node, reads its
//! bound address, shuts it down, and IMMEDIATELY (no sleep) constructs a second
//! node on that exact address, asserting the rebind succeeds.

#![cfg(all(feature = "tokio", feature = "tcp"))]

use std::net::SocketAddr;

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{MaybeResolved, Memberlist, Options, SocketAddrResolver, VoidDelegate};
use smol_str::SmolStr;

/// Construct a reactor TCP node bound to `bind` over the tokio runtime.
async fn spawn(
  id: &str,
  bind: SocketAddr,
) -> Result<Memberlist<SmolStr, SocketAddr>, memberlist_reactor::Error> {
  Memberlist::<SmolStr, _>::tcp::<TokioRuntime, _, _>(
    &SocketAddrResolver,
    SmolStr::new(id),
    MaybeResolved::Resolved(bind),
    Options::new(),
    VoidDelegate::<SmolStr, SocketAddr>::default(),
  )
  .await
}

/// After `shutdown().await`, the same address is immediately rebindable: both the
/// gossip UDP socket and the TCP listener drop before the shutdown ack fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebind_same_address_without_grace() {
  // Bind to an ephemeral loopback port and discover the concrete address the
  // kernel assigned (shared by the UDP socket and the TCP listener).
  let first = spawn("rebind-1", "127.0.0.1:0".parse().unwrap())
    .await
    .expect("first node binds");
  let addr = *first.local().addr_ref();
  assert_ne!(addr.port(), 0, "the bound port is concrete");

  // Stop the first node and wait for its shutdown to resolve. With the fix, the
  // ack lands only after the UDP socket and TCP listener FDs are released.
  first.shutdown().await.expect("first node shuts down");

  // IMMEDIATELY rebind a fresh node on the SAME address — no sleep, no retry. A
  // still-open socket from the first node would surface here as `AddrInUse`.
  let second = spawn("rebind-2", addr)
    .await
    .expect("immediate rebind on the same address succeeds (no AddrInUse)");
  assert_eq!(
    *second.local().addr_ref(),
    addr,
    "the second node bound the same address"
  );

  second.shutdown().await.expect("second node shuts down");
}
