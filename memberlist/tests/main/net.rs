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

#[path = "net/set_probe_channels.rs"]
mod set_probe_channels;

#[path = "net/set_ack_handler.rs"]
mod set_ack_handler;

#[path = "net/invoke_ack_handler.rs"]
mod invoke_ack_handler;

#[path = "net/invoke_ack_handler_channel_ack.rs"]
mod invoke_ack_handler_channel_ack;

#[path = "net/invoke_ack_handler_channel_nack.rs"]
mod invoke_ack_handler_channel_nack;
