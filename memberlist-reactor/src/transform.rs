//! Construction-time transform validation helpers shared by the backend
//! constructors and the runtime transform setters.

#[cfg(any(feature = "quic", feature = "tcp", feature = "tls"))]
use memberlist_proto::{EncryptionError, EncryptionOptions};

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
