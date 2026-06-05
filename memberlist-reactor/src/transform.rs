//! Construction-time transform validation helpers shared by the backend
//! constructors and the runtime transform setters.

#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
use memberlist_proto::{ChecksumOptions, EncryptionError, EncryptionOptions};

#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
use crate::error::Error;

/// Probe every key in `opts`'s keyring by encrypting an empty frame. A key
/// whose AEAD backend was not compiled into this binary fails here with
/// [`EncryptionError::UnsupportedAlgorithm`], so the node refuses to start
/// rather than silently run plaintext on an encrypted-cluster configuration.
///
/// Returns `Ok(())` when encryption is disabled (no keyring) or every key
/// in the ring is usable.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub(crate) fn validate_encryption(opts: &EncryptionOptions) -> Result<(), Error> {
  if let Some(keyring) = opts.keyring() {
    for key in core::iter::once(keyring.primary_ref()).chain(keyring.secondaries()) {
      memberlist_proto::encode_encrypted_frame(key.algorithm(), key, b"")
        .map_err(Error::Encryption)?;
    }
  }
  Ok(())
}

/// Probe a [`ChecksumOptions`] for usability in THIS build by trial-applying it
/// to an empty payload. A configured algorithm whose backend feature was not
/// compiled into this binary fails here with a
/// [`ChecksumError`](memberlist_proto::ChecksumError) (the options builder
/// accepts it, but every later `checksum_gossip` would fail and the driver would
/// drop the datagram — a "successful" checksum config would silently disable ALL
/// gossip), so the node refuses to start rather than running gossipless after a
/// false `Ok`.
///
/// Returns `Ok(())` when checksumming is disabled (no algorithm) — always usable
/// (identity codec) — or the configured algorithm's backend is present.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
pub(crate) fn validate_checksum(opts: &ChecksumOptions) -> Result<(), Error> {
  opts.apply(&[]).map(|_| ()).map_err(Error::Checksum)
}

/// Convert an [`EncryptionError`] from `encode_encrypted_frame` into the
/// driver [`Error`] variant for uniform error propagation.
///
/// This is a free-standing helper used by the validation path above and by
/// the inline encryption callers in the driver pumps — avoids a duplicate
/// `map_err(Error::Encryption)` at each call site.
#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
#[allow(dead_code)]
pub(crate) fn encryption_err(e: EncryptionError) -> Error {
  Error::Encryption(e)
}

#[cfg(all(
  test,
  any(feature = "quic", feature = "tcp", feature = "tls"),
  feature = "encryption-aes-gcm"
))]
mod tests {
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
}
