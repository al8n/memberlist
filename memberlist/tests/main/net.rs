use memberlist_core::{transport::Lpe, tests::{next_socket_addr_v4, state::*}};
use memberlist_net::{NetTransport, NetTransportOptions};
use memberlist::{transport::{resolver::socket_addr::SocketAddrResolver, Node, Transport}, Options};
use smol_str::SmolStr;


#[path = "net/probe.rs"]
mod probe;

#[path = "net/probe_node.rs"]
mod probe_node;

#[path = "net/probe_node_buddy.rs"]
mod probe_node_buddy;

#[path = "net/probe_node_suspect.rs"]
mod probe_node_suspect;