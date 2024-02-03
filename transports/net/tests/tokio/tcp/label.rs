use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

#[path = "label/with_label.rs"]
mod with_label;

#[path = "label/with_label_and_encryption.rs"]
mod with_label_and_encryption;
