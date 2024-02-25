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

#[path = "s2n/probe.rs"]
mod probe;

#[path = "s2n/probe_node.rs"]
mod probe_node;

#[path = "s2n/probe_node_dogpile.rs"]
mod probe_node_dogpile;

#[path = "s2n/probe_node_buddy.rs"]
mod probe_node_buddy;

#[path = "s2n/probe_node_suspect.rs"]
mod probe_node_suspect;

#[path = "s2n/probe_node_awareness_missed_nack.rs"]
mod probe_node_awareness_missed_nack;

#[path = "s2n/probe_node_awareness_improved.rs"]
mod probe_node_awareness_improved;

#[path = "s2n/probe_node_awareness_degraded.rs"]
mod probe_node_awareness_degraded;

#[path = "s2n/ping.rs"]
mod ping;

#[path = "s2n/reset_nodes.rs"]
mod reset_nodes;

#[path = "s2n/alive_node_new_node.rs"]
mod alive_node_new_node;

#[path = "s2n/alive_node_conflict.rs"]
mod alive_node_conflict;

#[path = "s2n/alive_node_suspect_node.rs"]
mod alive_node_suspect_node;

#[path = "s2n/alive_node_idempotent.rs"]
mod alive_node_idempotent;

#[path = "s2n/alive_node_change_meta.rs"]
mod alive_node_change_meta;

#[path = "s2n/alive_node_refute.rs"]
mod alive_node_refute;

#[path = "s2n/suspect_node_no_node.rs"]
mod suspect_node_no_node;

#[path = "s2n/suspect_node.rs"]
mod suspect_node;

#[path = "s2n/suspect_node_double_suspect.rs"]
mod suspect_node_double_suspect;

#[path = "s2n/suspect_node_refute.rs"]
mod suspect_node_refute;

#[path = "s2n/dead_node_no_node.rs"]
mod dead_node_no_node;

#[path = "s2n/dead_node_left.rs"]
mod dead_node_left;

#[path = "s2n/dead_node.rs"]
mod dead_node;

#[path = "s2n/dead_node_double.rs"]
mod dead_node_double;

#[path = "s2n/dead_node_old_dead.rs"]
mod dead_node_old_dead;

#[path = "s2n/dead_node_alive_replay.rs"]
mod dead_node_alive_replay;

#[path = "s2n/dead_node_refute.rs"]
mod dead_node_refute;

#[path = "s2n/merge_state.rs"]
mod merge_state;

#[path = "s2n/gossip.rs"]
mod gossip;

#[path = "s2n/gossip_to_dead.rs"]
mod gossip_to_dead;

#[path = "s2n/push_pull.rs"]
mod push_pull;

// ------- memberlist tests ----------

#[path = "s2n/join.rs"]
mod join;

#[path = "s2n/join_with_labels.rs"]
mod join_with_labels;

#[path = "s2n/join_with_labels_and_compression.rs"]
#[cfg(feature = "compression")]
mod join_with_labels_and_compression;

#[path = "s2n/join_different_s2nworks_unique_mask.rs"]
mod join_different_s2nworks_unique_mask;

#[path = "s2n/join_different_s2nworks_multi_masks.rs"]
mod join_different_s2nworks_multi_masks;

#[path = "s2n/join_cancel.rs"]
mod join_cancel;

#[path = "s2n/join_cancel_passive.rs"]
mod join_cancel_passive;

#[path = "s2n/join_shutdown.rs"]
mod join_shutdown;

#[path = "s2n/leave.rs"]
mod leave;

#[path = "s2n/conflict_delegate.rs"]
mod conflict_delegate;

#[path = "s2n/create_shutdown.rs"]
mod create_shutdown;

#[path = "s2n/create.rs"]
mod create;

#[path = "s2n/send.rs"]
mod send;

#[path = "s2n/user_data.rs"]
mod user_data;

#[path = "s2n/node_delegate_meta.rs"]
mod node_delegate_meta;

#[path = "s2n/node_delegate_meta_update.rs"]
mod node_delegate_meta_update;

#[path = "s2n/ping_delegate.rs"]
mod ping_delegate;


