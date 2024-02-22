#[path = "tests/label.rs"]
mod label;

#[path = "tests/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "tests/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "tests/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "tests/handle_ping.rs"]
mod handle_ping;

#[path = "tests/send_packet_piggyback.rs"]
mod send_packet_piggyback;

// #[path = "tests/gossip_mismatched_keys.rs"]
// #[cfg(feature = "encryption")]
// mod gossip_mismatched_keys;

#[path = "tests/promised_ping.rs"]
mod promised_ping;

#[path = "tests/promised_push_pull.rs"]
mod promised_push_pull;

#[path = "tests/send.rs"]
mod send;

#[path = "tests/join.rs"]
mod join;

#[path = "tests/join_dead_node.rs"]
mod join_dead_node;

#[path = "tests/promised_listener_backoff.rs"]
mod promised_listener_backoff;
