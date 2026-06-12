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
