#![no_main]

//! Fuzz the tag-driven transform unwrap loop (checksum / decompress / decrypt
//! stripping) on the unencrypted gossip path: a crafted nesting, a decompression
//! bomb declaring a huge output, or a malformed checksum wrapper must be rejected
//! within the `max_orig_len` ceiling without panicking or unbounded allocation.

use libfuzzer_sys::fuzz_target;
use memberlist_proto::{EncryptionOptions, unwrap_transforms_with_encryption};

const MAX_ORIG_LEN: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
  let encryption = EncryptionOptions::new();
  let _ = unwrap_transforms_with_encryption(data, MAX_ORIG_LEN, &encryption);
});
