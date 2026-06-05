//! Wire-traffic transforms: compression, encryption, and the reliable-plane
//! cluster label.

pub use memberlist_proto::{
  ChecksumAlgorithm, ChecksumOptions, CompressAlgorithm, CompressionOptions, EncryptionOptions,
  Keyring, SecretKey,
};

#[cfg(not(feature = "std"))]
use std::vec::Vec;

use bytes::Bytes;
use memberlist_proto::label::validate_label;

pub use memberlist_proto::LabelError;

/// How the driver compresses, encrypts, and labels its wire traffic.
///
/// Compression and encryption are cross-transport: applied to gossip datagrams
/// in this driver's UDP path and to the reliable plane automatically via the
/// machine's stream bridge. The cluster label is the reliable-plane cluster
/// discriminator (a one-time stream-start prefix) and is also applied to the
/// gossip codec so both planes share a single validated label source.
///
/// [`Default`] is fully disabled and unlabelled — byte-identical to an
/// unsecured cluster: compression off, encryption off, no label.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct TransformOptions {
  /// Cross-transport gossip + reliable-plane compression.
  pub compression: CompressionOptions,
  /// Gossip (unreliable) plane checksum.
  ///
  /// Applied only to outbound gossip datagrams on the UDP path; reliable
  /// streams carry no checksum (they rely on the stream transport's own
  /// integrity), matching memberlist-core and the legacy driver. [`Default`]
  /// selects no algorithm, leaving the gossip codec identity.
  pub checksum: ChecksumOptions,
  /// Cross-transport gossip + reliable-plane AEAD encryption.
  pub encryption: EncryptionOptions,
  /// Validated cluster label, or `None` for an unlabeled node.
  pub(crate) label: Option<Bytes>,
  /// When `true`, an inbound stream that presents no label header is accepted
  /// rather than rejected. Defaults to `false`.
  pub(crate) skip_inbound_label_check: bool,
}

impl TransformOptions {
  /// A fully-disabled, unlabelled configuration (see [`Default`]).
  #[must_use]
  pub fn new() -> Self {
    Self::default()
  }

  /// Set the cross-transport compression.
  #[must_use]
  pub fn with_compression(mut self, compression: CompressionOptions) -> Self {
    self.compression = compression;
    self
  }

  /// Set the gossip (unreliable) plane checksum.
  ///
  /// Applies to outbound gossip datagrams only; reliable streams carry no
  /// checksum.
  #[must_use]
  pub fn with_checksum(mut self, checksum: ChecksumOptions) -> Self {
    self.checksum = checksum;
    self
  }

  /// Set the cross-transport encryption.
  #[must_use]
  pub fn with_encryption(mut self, encryption: EncryptionOptions) -> Self {
    self.encryption = encryption;
    self
  }

  /// Set the cluster label for both the gossip and reliable planes.
  ///
  /// The label is validated immediately: it must be ≤253 bytes and valid
  /// UTF-8. An empty slice normalizes to `None` (no label). Returns
  /// `Err(LabelError)` when either constraint is violated.
  ///
  /// The validated label feeds both the reliable-plane `LabelOptions` and the
  /// gossip codec `EncodeOptions` from a single source, so the two planes
  /// cannot diverge.
  pub fn with_label(mut self, label: Option<Vec<u8>>) -> Result<Self, LabelError> {
    self.label = match label {
      None => None,
      Some(v) if v.is_empty() => None,
      Some(v) => {
        validate_label(&v)?;
        Some(Bytes::from(v))
      }
    };
    Ok(self)
  }

  /// Suppress the inbound reliable-plane label check.
  ///
  /// When set, an inbound TCP stream that presents no label header is accepted
  /// rather than rejected. Defaults to `false`. Faithful to memberlist-core
  /// `Options::skip_inbound_label_check`.
  #[must_use]
  pub fn with_skip_inbound_label_check(mut self, skip: bool) -> Self {
    self.skip_inbound_label_check = skip;
    self
  }

  /// The cluster label, if configured.
  #[inline]
  pub fn label(&self) -> Option<&[u8]> {
    self.label.as_deref()
  }

  /// Whether the inbound reliable-plane label check is suppressed.
  #[inline]
  pub const fn skip_inbound_label_check(&self) -> bool {
    self.skip_inbound_label_check
  }
}

#[cfg(test)]
mod tests {
  use super::{
    ChecksumAlgorithm, ChecksumOptions, CompressAlgorithm, CompressionOptions, EncryptionOptions,
    TransformOptions,
  };

  #[test]
  fn default_is_disabled_and_unlabelled() {
    for opts in [TransformOptions::new(), TransformOptions::default()] {
      assert!(opts.label().is_none());
      assert!(!opts.skip_inbound_label_check());
      assert!(opts.compression.algorithm().is_none());
      assert!(opts.checksum.algorithm().is_none());
    }
  }

  #[test]
  fn label_validation_and_skip_flag_round_trip() {
    let valid = TransformOptions::new()
      .with_label(Some(b"cluster-x".to_vec()))
      .expect("a valid ASCII label is accepted")
      .with_skip_inbound_label_check(true);
    assert_eq!(valid.label(), Some(b"cluster-x".as_slice()));
    assert!(valid.skip_inbound_label_check());

    // An empty slice and `None` both normalize to no label.
    assert!(
      TransformOptions::new()
        .with_label(Some(Vec::new()))
        .unwrap()
        .label()
        .is_none()
    );
    assert!(
      TransformOptions::new()
        .with_label(None)
        .unwrap()
        .label()
        .is_none()
    );

    // Over-long (>253 bytes) and non-UTF-8 labels are rejected at the setter.
    assert!(
      TransformOptions::new()
        .with_label(Some(std::vec![b'x'; 254]))
        .is_err()
    );
    assert!(
      TransformOptions::new()
        .with_label(Some(std::vec![0xff, 0xfe]))
        .is_err()
    );
  }

  #[test]
  fn compression_checksum_and_encryption_builders_set_their_fields() {
    let opts = TransformOptions::new()
      .with_compression(CompressionOptions::new().with_algorithm(CompressAlgorithm::Lz4))
      .with_checksum(ChecksumOptions::new().with_algorithm(ChecksumAlgorithm::Crc32))
      .with_encryption(EncryptionOptions::default());
    assert_eq!(opts.compression.algorithm(), Some(CompressAlgorithm::Lz4));
    assert_eq!(opts.checksum.algorithm(), Some(ChecksumAlgorithm::Crc32));
  }
}
