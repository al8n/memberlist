use agnostic::{async_std::AsyncStdRuntime, Runtime};
use memberlist_core::tests::run as run_unit_test;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
use memberlist_net::stream_layer::tcp::Tcp;

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(AsyncStdRuntime::block_on, fut);
}

#[path = "async_std/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "async_std/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "async_std/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "async_std/handle_ping.rs"]
mod handle_ping;

#[path = "async_std/label.rs"]
mod label;

#[path = "async_std/join.rs"]
mod join;

#[path = "async_std/promised_listener_backoff.rs"]
mod promised_listener_backoff;

#[path = "async_std/promised_ping.rs"]
mod promised_ping;

#[path = "async_std/promised_push_pull.rs"]
mod promised_push_pull;

#[path = "async_std/send_packet_piggyback.rs"]
mod send_packet_piggyback;

#[path = "async_std/send.rs"]
mod send;
