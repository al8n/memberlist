//! End-to-end proof that `MemberlistOptions::tuning` travels the real standard
//! driver path into the machine's `Endpoint::try_new`.
//!
//! `awareness_max_multiplier = 0` is the one newly-exposed tuning knob the
//! machine rejects at construction. Building a node through the ordinary
//! `tcp(...)` constructor with that value must surface the machine's
//! `EndpointInitError::AwarenessMultiplierZero` — which can only happen if the
//! tuning override was copied into the `EndpointOptions` that `try_new` received.
//! A sane value constructs cleanly through the same path.

#![cfg(feature = "tcp")]

use std::net::SocketAddr;

use agnostic::tokio::TokioRuntime;
use memberlist_reactor::{
  EndpointInitError, EndpointTuning, Error, MaybeResolved, Memberlist, MemberlistOptions, Options,
  SocketAddrResolver, VoidDelegate,
};
use smol_str::SmolStr;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_tuning_awareness_multiplier_zero_fails_construction() {
  let options = Options::new().with_memberlist(
    MemberlistOptions::new().with_tuning(EndpointTuning::new().with_awareness_max_multiplier(0)),
  );
  let res = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("node-zero"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    options,
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await;
  assert!(
    matches!(
      res,
      Err(Error::EndpointInit(
        EndpointInitError::AwarenessMultiplierZero
      ))
    ),
    "awareness_max_multiplier=0 in tuning must fail construction via the machine's try_new"
  );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_tuning_sane_awareness_multiplier_constructs() {
  let options = Options::new().with_memberlist(
    MemberlistOptions::new().with_tuning(EndpointTuning::new().with_awareness_max_multiplier(4)),
  );
  let m = Memberlist::<SmolStr, _, TokioRuntime>::tcp(
    &SocketAddrResolver,
    SmolStr::new("node-ok"),
    MaybeResolved::Resolved("127.0.0.1:0".parse::<SocketAddr>().unwrap()),
    options,
    VoidDelegate::<SmolStr, SocketAddr>::new(),
  )
  .await
  .expect("a nonzero awareness multiplier constructs cleanly through the same path");
  // Ignoring Err: best-effort teardown after the construction assertion; a
  // shutdown error would not change what this test proves.
  let _ = m.shutdown().await;
}
