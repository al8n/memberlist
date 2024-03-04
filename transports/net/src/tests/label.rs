use memberlist_core::transport::{tests::handle_ping, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{NetTransport, NetTransportOptions, StreamLayer};

use super::*;

/// No label, no compression, no encryption
pub mod with_label;

/// No label, no compression, with encryption
#[cfg(feature = "encryption")]
pub mod with_label_and_encryption;
