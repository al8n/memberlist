#[cfg(feature = "async-std")]
use agnostic::async_std::AsyncStdRuntime;
#[cfg(feature = "smol")]
use agnostic::smol::SmolRuntime;
#[cfg(feature = "tokio")]
use agnostic::tokio::TokioRuntime;
use agnostic::Runtime;
use std::future::Future;


use memberlist_core::tests::run as run_unit_test;

#[cfg(feature = "tokio")]
fn tokio_run(fut: impl Future<Output = ()>) {
  let runtime = ::tokio::runtime::Builder::new_multi_thread()
    .worker_threads(16)
    .enable_all()
    .build()
    .unwrap();
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

#[path = "main/net.rs"]
#[cfg(feature = "memberlist-net")]
mod net;

#[path = "main/quinn.rs"]
#[cfg(feature = "quinn")]
mod quinn;

#[path = "main/s2n.rs"]
#[cfg(feature = "s2n")]
mod s2n;
