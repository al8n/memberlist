use memberlist_core::transport::{tests::handle_ping, Lpe};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{NetTransport, NetTransportOptions, StreamLayer};

use super::*;

/// No label, no compression, no encryption
pub mod no_label_no_compression_no_encryption;

/// No label, no compression, with encryption
pub mod no_label_no_compression_with_encryption;

/// No label, with compression, no encryption
pub mod no_label_with_compression_no_encryption;

/// No label, with compression, with encryption
pub mod no_label_with_compression_with_encryption;

/// With label, no compression, no encryption
pub mod with_label_no_compression_no_encryption;

// mod with_label_no_compression_with_encryption;
// mod with_label_with_compression_no_encryption;
// mod with_label_with_compression_with_encryption;
