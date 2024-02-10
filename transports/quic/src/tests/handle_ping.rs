use memberlist_core::transport::{tests::handle_ping, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{QuicTransport, QuicTransportOptions, StreamLayer};

use super::*;

/// No label, no compression
pub mod no_label_no_compression;

/// With label, no compression
pub mod with_label_no_compression;

/// No label, with compression
#[cfg(feature = "compression")]
pub mod no_label_with_compression;

/// With label, with compression, no encryption
#[cfg(feature = "compression")]
pub mod with_label_with_compression;
