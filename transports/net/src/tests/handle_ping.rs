use memberlist_core::transport::{tests::handle_ping, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{NetTransport, NetTransportOptions, StreamLayer};

use super::*;

/// No label, no compression, no encryption
pub mod no_label_no_compression_no_encryption;

/// No label, no compression, with encryption
#[cfg(feature = "encryption")]
pub mod no_label_no_compression_with_encryption;

/// No label, with compression, no encryption
#[cfg(feature = "compression")]
pub mod no_label_with_compression_no_encryption;

/// No label, with compression, with encryption
#[cfg(all(feature = "compression", feature = "encryption"))]
pub mod no_label_with_compression_with_encryption;

/// With label, no compression, no encryption
pub mod with_label_no_compression_no_encryption;

/// With label, with compression, no encryption
#[cfg(feature = "compression")]
pub mod with_label_with_compression_no_encryption;
