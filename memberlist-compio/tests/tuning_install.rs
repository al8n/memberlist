//! End-to-end proof that `MemberlistOptions::tuning` is validated on the real
//! `Memberlist::new` path, and travels into the machine when sane.
//!
//! compio's `Transport::run` builds the machine through the PANICKING
//! `Endpoint::new` on a detached task, so the one newly-exposed tuning knob the
//! machine rejects at construction (`awareness_max_multiplier == 0`) is caught
//! up-front by `Memberlist::new`'s validator and surfaced as a clean
//! `MemberlistError::InvalidOption` — never a task panic. A sane value
//! constructs cleanly through the same path.

#![cfg(feature = "tcp")]

use std::net::SocketAddr;

use memberlist_compio::{
  EndpointTuning, FirstAddrResolver, MaybeResolved, Memberlist, MemberlistError, MemberlistOptions,
  Options, SocketAddrResolver, TcpTransport, TcpTransportOptions, VoidDelegate,
};
use smol_str::SmolStr;

#[compio::test]
async fn tcp_new_rejects_zero_awareness_multiplier_in_tuning() {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("node-zero"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(
    MemberlistOptions::new().with_tuning(EndpointTuning::new().with_awareness_max_multiplier(0)),
  );
  let res = Memberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  assert!(
    matches!(res, Err(MemberlistError::InvalidOption(_))),
    "awareness_max_multiplier=0 in tuning must be rejected up-front by Memberlist::new"
  );
}

#[compio::test]
async fn tcp_new_accepts_sane_awareness_multiplier_in_tuning() {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("node-ok"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(
    MemberlistOptions::new().with_tuning(EndpointTuning::new().with_awareness_max_multiplier(4)),
  );
  let m = Memberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("a nonzero awareness multiplier constructs cleanly through the same path");
  // Ignoring Err: best-effort teardown after the construction assertion; a
  // shutdown error would not change what this test proves.
  let _ = m.shutdown().await;
}
