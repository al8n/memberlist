use memberlist_core::{
  tests::{next_socket_addr_v4, next_socket_addr_v6},
  transport::{tests::handle_ping, Lpe},
};
use nodecraft::{resolver::socket_addr::SocketAddrResolver, CheapClone};

use crate::{NetTransport, NetTransportOptions, StreamLayer};

use super::*;

/// No label, no compression, no encryption
pub mod no_label_no_compression_no_encryption;

/// No label, no compression, with encryption
pub mod no_label_no_compression_with_encryption;

// mod no_label_with_compression_no_encryption;
// mod no_label_with_compression_with_encryption;

/// With label, no compression, no encryption
pub mod with_label_no_compression_no_encryption;

// mod with_label_no_compression_with_encryption;
// mod with_label_with_compression_no_encryption;
// mod with_label_with_compression_with_encryption;
