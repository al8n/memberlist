use memberlist_proto::{Keyring, SecretKey};

use super::*;

/// A disabled encryption config (no keyring) validates trivially — every
/// payload would ride the wire as plaintext, so there is no AEAD backend to
/// probe.
#[test]
fn validate_encryption_no_keyring_is_ok() {
  let opts = EncryptionOptions::new();
  assert!(
    validate_encryption(&opts).is_ok(),
    "a keyring-less config needs no AEAD backend"
  );
}

/// A single-key keyring whose AES-GCM backend is compiled in validates: the
/// trial-encrypt of the primary key succeeds.
#[test]
fn validate_encryption_single_key_is_ok() {
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x11; 32])));
  assert!(
    validate_encryption(&opts).is_ok(),
    "a compiled-in AES-256-GCM primary must validate"
  );
}

/// A multi-key keyring exercises the `chain(secondaries())` arm: the primary
/// and every secondary are each trial-encrypted.
#[test]
fn validate_encryption_walks_secondaries() {
  let keyring = Keyring::with_secondaries(
    SecretKey::Aes256([0x01; 32]),
    [SecretKey::Aes192([0x02; 24]), SecretKey::Aes128([0x03; 16])],
  );
  assert_eq!(keyring.secondaries().len(), 2, "both secondaries retained");
  let opts = EncryptionOptions::new().with_keyring(keyring);
  assert!(
    validate_encryption(&opts).is_ok(),
    "every key in a multi-key ring with compiled-in backends must validate"
  );
}

/// `encryption_err` wraps an `EncryptionError` into the driver `Error` variant
/// without altering it.
#[test]
fn encryption_err_wraps_into_error_variant() {
  let err = encryption_err(EncryptionError::UnsupportedAlgorithm(222));
  assert!(
    matches!(
      err,
      Error::Encryption(EncryptionError::UnsupportedAlgorithm(222))
    ),
    "encryption_err must wrap the underlying error verbatim"
  );
}
