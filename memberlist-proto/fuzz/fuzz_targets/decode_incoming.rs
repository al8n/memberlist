#![no_main]

//! Fuzz the full inbound datagram pipeline: cluster-label strip → transform
//! unwrap → message parse. Arbitrary `[label?][frame]` bytes are driven through
//! the real decode chain a driver applies to an inbound packet:
//! [`decode_incoming`] strips the optional cluster label, then the inner frame
//! feeds [`unwrap_transforms_with_encryption`] (the checksum / decompress
//! stripping loop) and, on success, [`parse_messages`] (the compound / message
//! parser). Every stage must reject malformed input cleanly — no panic, abort,
//! or unbounded allocation — not just the outer label.
//!
//! The first byte selects whether a cluster label is expected, exercising both
//! the labeled and unlabeled strip paths. This target drives the plaintext
//! pipeline with an empty [`EncryptionOptions`]; the keyed AEAD decrypt path is
//! fuzzed separately by the `unwrap_transforms` target.

use core::net::SocketAddr;

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use memberlist_proto::{
  decode_incoming, parse_messages, typed::Message, unwrap_transforms_with_encryption,
  DecodeOptions, EncryptionOptions,
};
use smol_str::SmolStr;

const MAX_ORIG_LEN: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
  let (label, rest) = match data.split_first() {
    Some((selector, rest)) if selector & 1 == 1 => {
      (Some(Bytes::from_static(b"fuzz-cluster")), rest)
    }
    Some((_, rest)) => (None, rest),
    None => (None, data),
  };
  let encryption = EncryptionOptions::new();
  let Ok(inner) = decode_incoming(Bytes::copy_from_slice(rest), &DecodeOptions::new(label)) else {
    return;
  };
  let Ok(plain) = unwrap_transforms_with_encryption(&inner, MAX_ORIG_LEN, &encryption) else {
    return;
  };
  let _: Result<Vec<Message<SmolStr, SocketAddr>>, _> =
    parse_messages(Bytes::copy_from_slice(&plain));
});
