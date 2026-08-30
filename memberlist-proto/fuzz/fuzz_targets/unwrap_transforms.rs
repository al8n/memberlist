#![no_main]

//! Fuzz the tag-driven transform unwrap loop (checksum / decompress / decrypt
//! stripping): a crafted nesting, a decompression bomb declaring a huge output,
//! a malformed checksum wrapper, or a forged AEAD frame must be rejected within
//! the `max_orig_len` ceiling without panicking or unbounded allocation.
//!
//! The first byte selects the keyring, so both branches are fuzzed:
//! - selector bit set — a fixed AES-256 key is installed, so an
//!   `Encrypted`-tagged payload reaches the AEAD decrypt path (tag verification,
//!   nonce handling, the ciphertext-bomb guard, and the inner checksum /
//!   decompress unwrap) instead of short-circuiting at `NoMatchingKey`. The key
//!   is fixed on purpose: the goal is to fuzz the decrypt code path, not key
//!   recovery.
//! - selector bit clear — an empty keyring drives the unencrypted
//!   checksum / decompress unwrap.
//!
//! The remaining bytes (after the selector) are the payload.

use libfuzzer_sys::fuzz_target;
use memberlist_proto::{unwrap_transforms_with_encryption, EncryptionOptions, Keyring, SecretKey};

const MAX_ORIG_LEN: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
  let (encryption, payload) = match data.split_first() {
    Some((selector, rest)) if selector & 1 == 1 => (
      EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x24; 32]))),
      rest,
    ),
    Some((_, rest)) => (EncryptionOptions::new(), rest),
    None => (EncryptionOptions::new(), data),
  };
  let _ = unwrap_transforms_with_encryption(payload, MAX_ORIG_LEN, &encryption);
});
