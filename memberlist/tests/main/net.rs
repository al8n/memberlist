use memberlist::{
  transport::{resolver::socket_addr::SocketAddrResolver, Node, Transport},
  Options,
};
use memberlist_core::{
  tests::{next_socket_addr_v4, state::*},
  transport::Lpe,
};
use memberlist_net::{NetTransport, NetTransportOptions};
use smol_str::SmolStr;

#[path = "net/probe.rs"]
mod probe;

#[path = "net/probe_node.rs"]
mod probe_node;

#[path = "net/probe_node_dogpile.rs"]
mod probe_node_dogpile;

#[path = "net/probe_node_buddy.rs"]
mod probe_node_buddy;

#[path = "net/probe_node_suspect.rs"]
mod probe_node_suspect;

#[path = "net/probe_node_awareness_missed_nack.rs"]
mod probe_node_awareness_missed_nack;

#[path = "net/probe_node_awareness_improved.rs"]
mod probe_node_awareness_improved;

#[path = "net/probe_node_awareness_degraded.rs"]
mod probe_node_awareness_degraded;

#[path = "net/ping.rs"]
mod ping;

#[path = "net/reset_nodes.rs"]
mod reset_nodes;

#[path = "net/alive_node_new_node.rs"]
mod alive_node_new_node;

#[path = "net/alive_node_conflict.rs"]
mod alive_node_conflict;

#[path = "net/alive_node_suspect_node.rs"]
mod alive_node_suspect_node;

#[path = "net/alive_node_idempotent.rs"]
mod alive_node_idempotent;

#[path = "net/alive_node_change_meta.rs"]
mod alive_node_change_meta;

#[path = "net/alive_node_refute.rs"]
mod alive_node_refute;

#[path = "net/suspect_node_no_node.rs"]
mod suspect_node_no_node;

#[path = "net/suspect_node.rs"]
mod suspect_node;

#[path = "net/suspect_node_double_suspect.rs"]
mod suspect_node_double_suspect;

#[path = "net/suspect_node_refute.rs"]
mod suspect_node_refute;

#[path = "net/dead_node_no_node.rs"]
mod dead_node_no_node;

#[path = "net/dead_node_left.rs"]
mod dead_node_left;

#[path = "net/dead_node.rs"]
mod dead_node;

#[path = "net/dead_node_double.rs"]
mod dead_node_double;

#[path = "net/dead_node_old_dead.rs"]
mod dead_node_old_dead;

#[path = "net/dead_node_alive_replay.rs"]
mod dead_node_alive_replay;

#[path = "net/dead_node_refute.rs"]
mod dead_node_refute;

#[path = "net/merge_state.rs"]
mod merge_state;

#[path = "net/gossip.rs"]
mod gossip;

#[path = "net/gossip_to_dead.rs"]
mod gossip_to_dead;

#[path = "net/push_pull.rs"]
mod push_pull;
