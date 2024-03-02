#[path = "tests/handle_compound_ping.rs"]
mod handle_compound_ping;

#[path = "tests/handle_ping_no_label_no_compression.rs"]
mod handle_ping_no_label_no_compression;

#[path = "tests/handle_ping_no_label_with_compression.rs"]
mod handle_ping_no_label_with_compression;

#[path = "tests/handle_ping_with_label_no_compression.rs"]
mod handle_ping_with_label_no_compression;

#[path = "tests/handle_ping_with_label_and_compression.rs"]
mod handle_ping_with_label_and_compression;

#[path = "tests/handle_indirect_ping.rs"]
mod handle_indirect_ping;

#[path = "tests/handle_ping_wrong_node.rs"]
mod handle_ping_wrong_node;

#[path = "tests/piggyback.rs"]
mod piggyback;

#[path = "tests/send.rs"]
mod send;

#[path = "tests/join.rs"]
mod join;

#[path = "tests/join_dead_node.rs"]
mod join_dead_node;

#[path = "tests/promised_ping.rs"]
mod promised_ping;

#[path = "tests/promised_push_pull.rs"]
mod promised_push_pull;
