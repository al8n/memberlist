//! Wire-traffic transforms: compression, encryption, and the reliable-plane
//! cluster label.

pub use memberlist_proto::{
  CompressAlgorithm, CompressionOptions, EncryptionOptions, Keyring, SecretKey,
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
