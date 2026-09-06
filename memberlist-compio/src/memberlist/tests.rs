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

/// The snapshot a join waiter observes on completion reflects the joined peer.
/// The driver publishes the post-transition snapshot BEFORE resolving the
/// `join()` waiter, so the seed is already counted the moment `join().await`
/// returns — a caller woken by its own join completion never reads the
/// pre-transition snapshot.
///
/// This end-to-end read is taken AFTER `join().await` returns, on the single-
/// thread compio executor where the woken caller only runs once the driver poll
/// has completed — so it observes post-poll state and cannot distinguish
/// publish-before-notify from a publish-AFTER-notify reorder. It anchors that the
/// publish is not REMOVED; the deterministic reorder guard is the white-box
/// `helper_publishes_snapshot_before_resolving_waiter_inline`, which
/// captures the snapshot at the exact resolution instant.
#[compio::test]
async fn join_completion_observes_post_transition_snapshot() {
  let n1 = spawn_node("seed").await;
  let n1_addr = *n1.local_node().addr_ref();
  let n2 = spawn_node("joiner").await;

  n2.join(&OsResolver, &[MaybeResolved::Resolved(n1_addr)])
    .await
    .expect("join seed");

  // No poll: the waiter resolved only after the driver republished the snapshot.
  assert_eq!(
    n2.member_count(),
    2,
    "the snapshot observed on join completion already counts the seed"
  );
  assert!(
    n2.snapshot().by_id(&SmolStr::new("seed")).is_some(),
    "the joined seed appears in the post-join snapshot"
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

/// Dropping the last handle WITHOUT `shutdown()` must run the driver's orderly
/// teardown, not hard-cancel it. The detached task survives the last handle drop,
/// observes the closed command channel, and reaches the post-loop cleanup that
/// flips `shutdown_flag`. A cancel-on-drop handle (the pre-fix design) would kill
/// the task before that cleanup, leaving the flag unset.
#[compio::test]
async fn dropping_last_handle_runs_orderly_teardown() {
  let n = spawn_node("orderly-drop").await;
  // Clone the internal latches BEFORE the drop — neither holds a command sender,
  // so the dropped `n` is genuinely the last handle and the command channel
  // closes, which the driver observes as a shutdown request.
  let flag = n.shutdown_flag.clone();
  let done = n.shutdown_done_rx.clone();
  drop(n);

  // Yield to the runtime so the detached driver observes the closed command
  // channel, tears down, and drops the done latch when `run` returns
  // (`recv_async` then resolves with `Disconnected`).
  done
    .recv_async()
    .await
    .expect_err("the driver drops the done latch when `run` returns");

  assert!(
    flag.get(),
    "the driver reached its post-loop cleanup (shutdown_flag set) — it ran the \
       orderly teardown instead of being cancelled mid-flight"
  );
}

/// A stream-driver teardown that cannot prove its ports were released reports
/// that, instead of a `Ok(())` the caller would read as "rebind now".
///
/// The whole reason the driver holds the shutdown ack until after the closes is
/// so a caller may rebind the same address the instant `shutdown().await`
/// returns. When the completion protocols fall back to the drop-based path, an
/// abandoned accept or receive may still own its descriptor and the port can
/// stay bound until the process exits — so the ack has to say so. The fallback
/// itself is unreachable against a healthy loopback listener, so it is forced
/// through the seam; the happy path is covered by
/// `rebind_after_shutdown_releases_listener_port`, which rebinds for real.
#[compio::test]
async fn shutdown_reports_unproven_release_for_both_stream_sockets() {
  let node = spawn_node("unproven-release").await;

  // The driver task runs on the thread that spawned it, so the seam this test
  // sets is the one its own driver reads.
  crate::driver::shared::set_force_teardown_fallback(true);
  let res = node.shutdown().await;
  crate::driver::shared::set_force_teardown_fallback(false);

  match res {
    Err(MemberlistError::ShutdownReleaseUnproven(e)) => assert_eq!(
      e.socket(),
      crate::error::UnreleasedSocket::Both,
      "a stream node binds a listener and a gossip socket, and neither was proven",
    ),
    other => panic!("expected an unproven-release shutdown reply, got {other:?}"),
  }
}

/// The ordinary teardown still proves both ports free, and the address really
/// is rebindable the moment the ack lands.
///
/// The companion to the forced-fallback case above: it pins that the new proof
/// is not vacuously failing — a healthy node completes its accept and its
/// receive, both closes return, and `Ok(())` is followed by a successful
/// rebind of the very same address.
#[compio::test]
async fn shutdown_proves_release_and_the_address_rebinds() {
  let node = spawn_node("proven-release").await;
  let addr = node.advertise_address();
  node
    .shutdown()
    .await
    .expect("shutdown proves both releases");

  let listener = compio::net::TcpListener::bind(addr)
    .await
    .expect("the listening port was proven released");
  let gossip = compio::net::UdpSocket::bind(addr)
    .await
    .expect("the gossip port was proven released");
  drop(listener);
  drop(gossip);
}
