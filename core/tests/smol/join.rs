use super::*;

#[test]
fn join() {
  run(test_join::<SmolRuntime>());
}

#[test]
fn join_with_compression() {
  run(test_join_with_compression::<SmolRuntime>());
}

#[test]
fn join_with_encryption() {
  run(test_join_with_encryption::<SmolRuntime>(
    EncryptionAlgo::None,
  ));

  run(test_join_with_encryption::<SmolRuntime>(
    EncryptionAlgo::NoPadding,
  ));

  run(test_join_with_encryption::<SmolRuntime>(
    EncryptionAlgo::PKCS7,
  ));
}

#[test]
fn join_with_encryption_and_compression() {
  run(test_join_with_encryption_and_compression::<SmolRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::Lzw,
  ));

  run(test_join_with_encryption_and_compression::<SmolRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::Lzw,
  ));
}

#[test]
fn join_with_labels() {
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::None,
    None,
  ));
}

#[test]
fn join_with_labels_and_compression() {
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::Lzw,
    None,
  ));
}

#[test]
fn join_with_labels_and_encryption() {
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
}

#[test]
fn join_with_labels_and_compression_and_encryption() {
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<SmolRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
}
