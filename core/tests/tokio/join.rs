use super::*;

#[test]
fn join() {
  run(test_join::<TokioRuntime>());
}

#[test]
fn join_with_compression() {
  run(test_join_with_compression::<TokioRuntime>());
}

#[test]
fn join_with_encryption() {
  run(test_join_with_encryption::<TokioRuntime>(
    EncryptionAlgo::None,
  ));

  run(test_join_with_encryption::<TokioRuntime>(
    EncryptionAlgo::NoPadding,
  ));

  run(test_join_with_encryption::<TokioRuntime>(
    EncryptionAlgo::PKCS7,
  ));
}

#[test]
fn join_with_encryption_and_compression() {
  run(test_join_with_encryption_and_compression::<TokioRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::Lzw,
  ));

  run(test_join_with_encryption_and_compression::<TokioRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::Lzw,
  ));
}

#[test]
fn join_with_labels() {
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::None,
    None,
  ));
}

#[test]
fn join_with_labels_and_compression() {
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::Lzw,
    None,
  ));
}

#[test]
fn join_with_labels_and_encryption() {
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
}

#[test]
fn join_with_labels_and_compression_and_encryption() {
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<TokioRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
}

#[test]
fn join_different_networks_unique_mask() {
  run(test_join_different_networks_unique_mask::<TokioRuntime>());
}

#[test]
fn join_different_networks_multi_masks() {
  run(test_join_different_networks_multi_masks::<TokioRuntime>());
}
