use super::*;
use agnostic::{smol::SmolRuntime, RuntimeLite};
use memberlist_core::tests::run as run_unit_test;

fn run(fut: impl std::future::Future<Output = ()>) {
  run_unit_test(SmolRuntime::block_on, fut);
}

#[path = "smol/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "smol/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "smol/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "smol/handle_ping.rs"]
mod handle_ping;

#[path = "smol/label.rs"]
mod label;

#[path = "smol/join.rs"]
mod join;

#[path = "smol/join_dead_node.rs"]
mod join_dead_node;

#[path = "smol/promised_listener_backoff.rs"]
mod promised_listener_backoff;

#[path = "smol/promised_ping.rs"]
mod promised_ping;

#[path = "smol/promised_push_pull.rs"]
mod promised_push_pull;

#[path = "smol/send_packet_piggyback.rs"]
mod send_packet_piggyback;

#[path = "smol/send.rs"]
mod send;
