use std::future::Future;

#[cfg(feature = "async-std")]
use agnostic::async_std::AsyncStdRuntime;
#[cfg(feature = "smol")]
use agnostic::smol::SmolRuntime;
#[cfg(feature = "tokio")]
use agnostic::tokio::TokioRuntime;
use agnostic::Runtime;
use memberlist_quic::tests::quinn_stream_layer;

use memberlist_core::tests::run as run_unit_test;

#[cfg(feature = "tokio")]
fn tokio_run(fut: impl Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[cfg(feature = "smol")]
fn smol_run(fut: impl Future<Output = ()>) {
  run_unit_test(SmolRuntime::block_on, fut);
}

#[cfg(feature = "async-std")]
fn async_std_run(fut: impl Future<Output = ()>) {
  run_unit_test(AsyncStdRuntime::block_on, fut);
}

#[path = "quinn/handle_ping.rs"]
mod handle_ping;

#[path = "quinn/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "quinn/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "quinn/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "quinn/piggyback.rs"]
mod piggyback;

#[path = "quinn/send.rs"]
mod send;

#[path = "quinn/join.rs"]
mod join;

#[path = "quinn/promised_ping.rs"]
mod promise_ping;

#[path = "quinn/promised_push_pull.rs"]
mod promised_push_pull;
