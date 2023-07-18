use super::*;

#[test]
fn join() {
  run(test_join::<AsyncStdRuntime>());
}

#[test]
fn join_with_compression() {
  run(test_join_with_compression::<AsyncStdRuntime>());
}

#[test]
fn join_with_encryption() {
  run(test_join_with_encryption::<AsyncStdRuntime>(
    EncryptionAlgo::None,
  ));

  run(test_join_with_encryption::<AsyncStdRuntime>(
    EncryptionAlgo::NoPadding,
  ));

  run(test_join_with_encryption::<AsyncStdRuntime>(
    EncryptionAlgo::PKCS7,
  ));
}

#[test]
fn join_with_encryption_and_compression() {
  run(
    test_join_with_encryption_and_compression::<AsyncStdRuntime>(
      EncryptionAlgo::NoPadding,
      CompressionAlgo::Lzw,
    ),
  );

  run(
    test_join_with_encryption_and_compression::<AsyncStdRuntime>(
      EncryptionAlgo::PKCS7,
      CompressionAlgo::Lzw,
    ),
  );
}

#[test]
fn join_with_labels() {
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::None,
    None,
  ));
}

#[test]
fn join_with_labels_and_compression() {
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::Lzw,
    None,
  ));
}

#[test]
fn join_with_labels_and_encryption() {
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::None,
    Some(TEST_KEYS[0]),
  ));
}

#[test]
fn join_with_labels_and_compression_and_encryption() {
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::NoPadding,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::PKCS7,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
  run(test_join_with_labels::<AsyncStdRuntime>(
    EncryptionAlgo::None,
    CompressionAlgo::Lzw,
    Some(TEST_KEYS[0]),
  ));
}
