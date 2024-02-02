use super::*;
use memberlist_net::stream_layer::tcp::Tcp;

#[path = "handle_ping/no_label_no_compression_no_encryption.rs"]
mod no_label_no_compression_no_encryption;
#[path = "handle_ping/no_label_no_compression_with_encryption.rs"]
mod no_label_no_compression_with_encryption;
// #[path = "handle_ping/no_label_with_compression_no_encryption.rs"]
// mod no_label_with_compression_no_encryption;
// #[path = "handle_ping/no_label_with_compression_with_encryption.rs"]
// mod no_label_with_compression_with_encryption;
// #[path = "handle_ping/with_label_no_compression_no_encryption.rs"]
// mod with_label_no_compression_no_encryption;
#[path = "handle_ping/with_label_no_compression_with_encryption.rs"]
mod with_label_no_compression_with_encryption;
// #[path = "handle_ping/with_label_with_compression_no_encryption.rs"]
// mod with_label_with_compression_no_encryption;
// #[path = "handle_ping/with_label_with_compression_with_encryption.rs"]
// mod with_label_with_compression_with_encryption;
