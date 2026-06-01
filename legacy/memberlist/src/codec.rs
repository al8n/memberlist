//! Datagram (UDP/gossip) codec, re-exported from the no_std
//! [`memberlist_proto::codec`] so every driver shares one gossip wire format.
pub use memberlist_proto::codec::{
  CodecError, DecodeOptions, EncodeOptions, MAX_LABEL_LEN, decode_incoming, encode_outgoing,
  encode_outgoing_compound, parse_message, parse_messages,
};
