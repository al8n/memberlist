use memberlist::{
  transport::{resolver::socket_addr::SocketAddrResolver, Node, Transport},
  Options,
};
use memberlist_core::{
  tests::{memberlist::*, next_socket_addr_v4, state::*},
  transport::Lpe,
};
use memberlist_quic::{QuicTransport, QuicTransportOptions};
use smol_str::SmolStr;

#[path = "quinn/probe.rs"]
mod probe;

#[path = "quinn/probe_node.rs"]
mod probe_node;

#[path = "quinn/probe_node_dogpile.rs"]
mod probe_node_dogpile;

#[path = "quinn/probe_node_buddy.rs"]
mod probe_node_buddy;

#[path = "quinn/probe_node_suspect.rs"]
mod probe_node_suspect;

#[path = "quinn/probe_node_awareness_missed_nack.rs"]
mod probe_node_awareness_missed_nack;

#[path = "quinn/probe_node_awareness_improved.rs"]
mod probe_node_awareness_improved;

#[path = "quinn/probe_node_awareness_degraded.rs"]
mod probe_node_awareness_degraded;

#[path = "quinn/ping.rs"]
mod ping;

#[path = "quinn/reset_nodes.rs"]
mod reset_nodes;

#[path = "quinn/alive_node_new_node.rs"]
mod alive_node_new_node;

#[path = "quinn/alive_node_conflict.rs"]
mod alive_node_conflict;

#[path = "quinn/alive_node_suspect_node.rs"]
mod alive_node_suspect_node;

#[path = "quinn/alive_node_idempotent.rs"]
mod alive_node_idempotent;

#[path = "quinn/alive_node_change_meta.rs"]
mod alive_node_change_meta;

#[path = "quinn/alive_node_refute.rs"]
mod alive_node_refute;

#[path = "quinn/suspect_node_no_node.rs"]
mod suspect_node_no_node;

#[path = "quinn/suspect_node.rs"]
mod suspect_node;

#[path = "quinn/suspect_node_double_suspect.rs"]
mod suspect_node_double_suspect;

#[path = "quinn/suspect_node_refute.rs"]
mod suspect_node_refute;

#[path = "quinn/dead_node_no_node.rs"]
mod dead_node_no_node;

#[path = "quinn/dead_node_left.rs"]
mod dead_node_left;

#[path = "quinn/dead_node.rs"]
mod dead_node;

#[path = "quinn/dead_node_double.rs"]
mod dead_node_double;

#[path = "quinn/dead_node_old_dead.rs"]
mod dead_node_old_dead;

#[path = "quinn/dead_node_alive_replay.rs"]
mod dead_node_alive_replay;

#[path = "quinn/dead_node_refute.rs"]
mod dead_node_refute;

#[path = "quinn/merge_state.rs"]
mod merge_state;

#[path = "quinn/gossip.rs"]
mod gossip;

#[path = "quinn/gossip_to_dead.rs"]
mod gossip_to_dead;

#[path = "quinn/push_pull.rs"]
mod push_pull;

// ------- memberlist tests ----------

#[path = "quinn/join.rs"]
mod join;

#[path = "quinn/join_with_labels.rs"]
mod join_with_labels;

#[path = "quinn/join_with_labels_and_compression.rs"]
#[cfg(feature = "compression")]
mod join_with_labels_and_compression;

#[path = "quinn/join_different_quinnworks_unique_mask.rs"]
mod join_different_quinnworks_unique_mask;

#[path = "quinn/join_different_quinnworks_multi_masks.rs"]
mod join_different_quinnworks_multi_masks;

#[path = "quinn/join_cancel.rs"]
mod join_cancel;

#[path = "quinn/join_cancel_passive.rs"]
mod join_cancel_passive;

#[path = "quinn/join_shutdown.rs"]
mod join_shutdown;

#[path = "quinn/leave.rs"]
mod leave;

#[path = "quinn/conflict_delegate.rs"]
mod conflict_delegate;

#[path = "quinn/create_shutdown.rs"]
mod create_shutdown;

#[path = "quinn/create.rs"]
mod create;

#[path = "quinn/send.rs"]
mod send;

#[path = "quinn/user_data.rs"]
mod user_data;

#[path = "quinn/node_delegate_meta.rs"]
mod node_delegate_meta;

#[path = "quinn/node_delegate_meta_update.rs"]
mod node_delegate_meta_update;

#[path = "quinn/ping_delegate.rs"]
mod ping_delegate;

