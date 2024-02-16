use super::*;
use agnostic::tokio::TokioRuntime;
use memberlist_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  console_subscriber::init();
  let runtime = ::tokio::runtime::Builder::new_multi_thread()
    .worker_threads(32)
    .enable_all()
    .build()
    .unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[path = "tokio/handle_ping_no_label_no_compression.rs"]
mod handle_ping_no_label_no_compression;

#[path = "tokio/handle_ping_with_label_no_compression.rs"]
mod handle_ping_with_label_no_compression;

#[path = "tokio/handle_ping_no_label_with_compression.rs"]
mod handle_ping_no_label_with_compression;

#[path = "tokio/handle_ping_with_label_and_compression.rs"]
mod handle_ping_with_label_and_compression;

#[path = "tokio/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "tokio/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "tokio/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "tokio/piggyback.rs"]
mod piggyback;

#[path = "tokio/send.rs"]
mod send;

#[path = "tokio/join.rs"]
mod join;

#[path = "tokio/promised_ping.rs"]
mod promised_ping;

#[path = "tokio/promised_push_pull.rs"]
mod promised_push_pull;
