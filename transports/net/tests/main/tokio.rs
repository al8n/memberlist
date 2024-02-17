use agnostic::tokio::TokioRuntime;
use memberlist_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[cfg(not(any(feature = "tls", feature = "native-tls")))]
use memberlist_net::stream_layer::tcp::Tcp;

#[path = "tokio/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "tokio/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "tokio/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "tokio/handle_ping.rs"]
mod handle_ping;

#[path = "tokio/label.rs"]
mod label;

#[path = "tokio/join.rs"]
mod join;

#[path = "tokio/promised_listener_backoff.rs"]
mod promised_listener_backoff;

#[path = "tokio/promised_ping.rs"]
mod promised_ping;

#[path = "tokio/promised_push_pull.rs"]
mod promised_push_pull;

#[path = "tokio/send_packet_piggyback.rs"]
mod send_packet_piggyback;

#[path = "tokio/send.rs"]
mod send;
