//! Wire-traffic transforms: compression, encryption, and the reliable-plane
//! cluster label.

pub use memberlist_proto::{
  CompressAlgorithm, CompressionOptions, EncryptionOptions, Keyring, SecretKey, TcpOptions,
};

/// How the driver compresses, encrypts, and labels its wire traffic.
///
/// Compression and encryption are cross-transport: applied to gossip datagrams
/// in this driver's UDP path and to the reliable plane automatically via the
/// machine's stream bridge. The TCP label is the reliable-plane cluster
/// discriminator (a one-time stream-start prefix; it does not apply to gossip).
///
/// [`Default`] is fully disabled and unlabelled — byte-identical to an
/// unsecured cluster: compression off, encryption off, no label.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TransformOptions {
  /// Cross-transport gossip + reliable-plane compression.
  pub compression: CompressionOptions,
  /// Cross-transport gossip + reliable-plane AEAD encryption.
  pub encryption: EncryptionOptions,
  /// Reliable-plane (TCP) cluster label.
  pub tcp: TcpOptions,
}

impl Default for TransformOptions {
  fn default() -> Self {
    Self {
      compression: CompressionOptions::default(),
      encryption: EncryptionOptions::default(),
      // `TcpOptions` does not implement `Default`. An absent label is
      // unconditionally valid (`try_new(None)` only ever fails for an
      // over-long or non-UTF-8 label), so this never panics and yields the
      // no-label reliable plane that matches today's behaviour.
      tcp: TcpOptions::try_new(None).expect("absent TCP label is always valid"),
    }
  }
}

impl TransformOptions {
  /// A fully-disabled, unlabelled configuration (see [`Default`]).
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

  /// Set the reliable-plane (TCP) cluster label options.
  #[must_use]
  pub fn with_tcp_options(mut self, tcp: TcpOptions) -> Self {
    self.tcp = tcp;
    self
  }
}
