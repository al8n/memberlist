use super::*;
use agnostic::tokio::TokioRuntime;
use memberlist_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[path = "tokio/handle_ping.rs"]
mod handle_ping;

#[path = "tokio/handle_compound_ping.rs"]
mod handle_compound_ping;