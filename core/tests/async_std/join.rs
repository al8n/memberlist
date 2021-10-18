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

#[test]
fn join_different_networks_unique_mask() {
  run(test_join_different_networks_unique_mask::<AsyncStdRuntime>());
}

#[test]
fn join_different_networks_multi_masks() {
  run(test_join_different_networks_multi_masks::<AsyncStdRuntime>());
}

#[test]
fn join_cancel() {
  run(test_join_cancel::<AsyncStdRuntime>());
}

#[test]
fn join_cancel_passive() {
  run(test_join_cancel_passive::<AsyncStdRuntime>());
}

#[test]
fn join_shutdown() {
  run(test_join_shutdown::<AsyncStdRuntime>());
}

#[test]
fn join_dead_node() {
  run(test_join_dead_node::<AsyncStdRuntime>());
}

#[test]
fn join_ipv6() {
  run(test_join_ipv6::<AsyncStdRuntime>());
}

#[test]
fn send_to() {
  run(test_send_to::<AsyncStdRuntime>());
}
