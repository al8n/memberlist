#[cfg(feature = "smol")]
use agnostic::smol::SmolRuntime;

use std::future::Future;

use memberlist_core::tests::{run as run_unit_test, state::*};
pub use memberlist_core::*;

#[cfg(feature = "tokio")]
fn tokio_run(fut: impl Future<Output = ()>) {
  let runtime = ::tokio::runtime::Builder::new_multi_thread()
    .worker_threads(32)
    .enable_all()
    .build()
    .unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[cfg(feature = "smol")]
fn smol_run(fut: impl Future<Output = ()>) {
  use agnostic::RuntimeLite;
  run_unit_test(SmolRuntime::block_on, fut);
}
#[path = "main/net.rs"]
#[cfg(feature = "memberlist-net")]
mod net;

#[path = "main/quic.rs"]
#[cfg(feature = "memberlist-quic")]
mod quic;

#[path = "main/set_probe_channels.rs"]
mod set_probe_channels;

#[path = "main/set_ack_handler.rs"]
mod set_ack_handler;

#[path = "main/invoke_ack_handler.rs"]
mod invoke_ack_handler;

#[path = "main/invoke_ack_handler_channel_ack.rs"]
mod invoke_ack_handler_channel_ack;

#[path = "main/invoke_ack_handler_channel_nack.rs"]
mod invoke_ack_handler_channel_nack;
