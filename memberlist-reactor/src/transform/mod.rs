//! Construction-time transform validation helpers shared by the backend
//! constructors and the runtime transform setters.

#[cfg(all(any(feature = "quic", feature = "tcp", feature = "tls"), checksum))]
use memberlist_proto::ChecksumOptions;
#[cfg(all(any(feature = "quic", feature = "tcp", feature = "tls"), encryption))]
use memberlist_proto::{EncryptionError, EncryptionOptions};

#[cfg(all(
  any(feature = "quic", feature = "tcp", feature = "tls"),
  any(encryption, checksum)
))]
use crate::error::Error;

/// Probe every key in `opts`'s keyring by encrypting an empty frame. A key
/// whose AEAD backend was not compiled into this binary fails here with
/// [`EncryptionError::UnsupportedAlgorithm`], so the node refuses to start
/// rather than silently run plaintext on an encrypted-cluster configuration.
///
/// Returns `Ok(())` when encryption is disabled (no keyring) or every key
/// in the ring is usable.
#[cfg(all(any(feature = "quic", feature = "tcp", feature = "tls"), encryption))]
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
#[cfg(all(any(feature = "quic", feature = "tcp", feature = "tls"), checksum))]
pub(crate) fn validate_checksum(opts: &ChecksumOptions) -> Result<(), Error> {
  opts.apply(&[]).map(|_| ()).map_err(Error::Checksum)
}

/// Convert an [`EncryptionError`] from `encode_encrypted_frame` into the
/// driver [`Error`] variant for uniform error propagation.
///
/// This is a free-standing helper used by the validation path above and by
/// the inline encryption callers in the driver pumps — avoids a duplicate
/// `map_err(Error::Encryption)` at each call site.
#[cfg(all(any(feature = "quic", feature = "tcp", feature = "tls"), encryption))]
pub(crate) fn encryption_err(e: EncryptionError) -> Error {
  Error::Encryption(e)
}

#[cfg(all(
  test,
  any(feature = "quic", feature = "tcp", feature = "tls"),
  feature = "aes-gcm"
))]
mod tests;
