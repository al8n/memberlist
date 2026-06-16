//! Property test: an encoded gossip message survives the full outbound transform
//! stack (compress -> checksum -> encrypt) and the inbound unwrap loop unchanged.
//! This complements the fuzz targets (which prove decode does not panic on
//! garbage) by proving encode/decode SYMMETRY on valid frames.
//!
//! The transform stack wraps a MESSAGE FRAME, not arbitrary bytes — the unwrap
//! loop strips transform tags until it reaches a message tag — so the payload is
//! a real `UserData` message carrying an arbitrary byte string.

#![cfg(all(
  feature = "lz4",
  feature = "crc32",
  feature = "aes-gcm",
))]

use core::net::SocketAddr;

use bytes::Bytes;
use memberlist_proto::{
  ChecksumAlgorithm, ChecksumOptions, CompressAlgorithm, CompressionOptions, EncodeOptions,
  EncryptionOptions, Keyring, SecretKey, encode_outgoing, parse_message,
  streams::{checksum_gossip_datagram, compress_gossip_datagram, encrypt_gossip_datagram},
  typed::Message,
  unwrap_transforms_with_encryption,
};
use proptest::prelude::*;
use smol_str::SmolStr;

const MAX_ORIG_LEN: usize = 1 << 20;

fn user_message(payload: &[u8]) -> Bytes {
  let msg: Message<SmolStr, SocketAddr> = Message::UserData(Bytes::copy_from_slice(payload));
  encode_outgoing(&msg, &EncodeOptions::new(None)).expect("encode UserData frame")
}

fn assert_roundtrips(frame: &[u8], expected: &[u8]) {
  let parsed: Message<SmolStr, SocketAddr> =
    parse_message(Bytes::copy_from_slice(frame)).expect("parse unwrapped frame");
  match parsed {
    Message::UserData(b) => assert_eq!(b.as_ref(), expected),
    other => panic!("expected UserData, got {other:?}"),
  }
}

proptest! {
  /// `unwrap(encrypt(checksum(compress(frame)))) == frame` for every message.
  #[test]
  fn transform_stack_roundtrips(payload in proptest::collection::vec(any::<u8>(), 0..4096)) {
    let comp = CompressionOptions::new().with_algorithm(CompressAlgorithm::Lz4);
    let csum = ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32);
    let enc = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42u8; 32])));

    let frame = user_message(&payload);
    let compressed = compress_gossip_datagram(&comp, &frame);
    let checksummed = checksum_gossip_datagram(&csum, &compressed).expect("checksum");
    let encrypted = encrypt_gossip_datagram(&enc, &checksummed).expect("encrypt");
    let unwrapped =
      unwrap_transforms_with_encryption(&encrypted, MAX_ORIG_LEN, &enc).expect("unwrap");

    assert_roundtrips(&unwrapped, &payload);
  }

  /// Checksum-only (no compression / encryption) is also an exact inverse — the
  /// unwrap loop must accept an unencrypted frame when no keyring is set.
  #[test]
  fn checksum_only_roundtrips(payload in proptest::collection::vec(any::<u8>(), 0..4096)) {
    let csum = ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32);
    let enc = EncryptionOptions::new();

    let frame = user_message(&payload);
    let checksummed = checksum_gossip_datagram(&csum, &frame).expect("checksum");
    let unwrapped =
      unwrap_transforms_with_encryption(&checksummed, MAX_ORIG_LEN, &enc).expect("unwrap");

    assert_roundtrips(&unwrapped, &payload);
  }
}
