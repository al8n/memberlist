use super::*;

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn algorithm_tag_roundtrip() {
  for algo in [EncryptAlgorithm::AesGcm, EncryptAlgorithm::ChaCha20Poly1305] {
    assert_eq!(EncryptAlgorithm::from_tag(algo.tag()), algo);
    assert_eq!(EncryptAlgorithm::from(u8::from(algo)), algo);
  }
}

#[test]
fn unrecognized_tag_is_unknown() {
  assert_eq!(EncryptAlgorithm::from_tag(0), EncryptAlgorithm::Unknown(0));
  assert_eq!(
    EncryptAlgorithm::from_tag(99),
    EncryptAlgorithm::Unknown(99)
  );
  // `Unknown` round-trips its carried byte.
  assert_eq!(EncryptAlgorithm::Unknown(99).tag(), 99);
}

/// Pinned numeric algorithm tags. A change here is a wire-protocol break:
/// a new-build node would decode a peer's old-build frame to the wrong
/// algorithm. The pinning is enforced as a unit test so a refactor that
/// reorders the constants is caught at test time, not at deploy time.
#[test]
#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
fn algorithm_tags_have_pinned_numeric_values() {
  assert_eq!(EncryptAlgorithm::AesGcm.tag(), 1);
  assert_eq!(EncryptAlgorithm::ChaCha20Poly1305.tag(), 2);
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn secret_key_variants_imply_algorithm() {
  let aes128 = SecretKey::Aes128([0u8; 16]);
  let aes192 = SecretKey::Aes192([0u8; 24]);
  let aes256 = SecretKey::Aes256([0u8; 32]);
  let chacha = SecretKey::ChaCha20Poly1305([0u8; 32]);
  assert_eq!(aes128.algorithm(), EncryptAlgorithm::AesGcm);
  assert_eq!(aes192.algorithm(), EncryptAlgorithm::AesGcm);
  assert_eq!(aes256.algorithm(), EncryptAlgorithm::AesGcm);
  assert_eq!(chacha.algorithm(), EncryptAlgorithm::ChaCha20Poly1305);
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn secret_key_as_bytes_returns_full_key() {
  let aes128 = SecretKey::Aes128([7u8; 16]);
  let aes192 = SecretKey::Aes192([7u8; 24]);
  let aes256 = SecretKey::Aes256([7u8; 32]);
  let chacha = SecretKey::ChaCha20Poly1305([7u8; 32]);
  assert_eq!(aes128.as_bytes().len(), 16);
  assert_eq!(aes192.as_bytes().len(), 24);
  assert_eq!(aes256.as_bytes().len(), 32);
  assert_eq!(chacha.as_bytes().len(), 32);
  assert!(aes128.as_bytes().iter().all(|b| *b == 7));
}

/// `Debug` on a `SecretKey` MUST NOT disclose the raw bytes — a single
/// `{:?}` of a configured `Keyring` / `EncryptionOptions` in a log line would
/// otherwise dump the symmetric key. The custom impl renders the variant
/// name with a `<redacted>` placeholder; the byte-leak guard spot-checks
/// both `0xXX` hex form and the array Debug's decimal form.
#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn secret_key_debug_redacts_raw_bytes() {
  let aes128 = SecretKey::Aes128([0xAB; 16]);
  let aes192 = SecretKey::Aes192([0xCD; 24]);
  let aes256 = SecretKey::Aes256([0xEF; 32]);
  let chacha = SecretKey::ChaCha20Poly1305([0x99; 32]);

  for key in [aes128, aes192, aes256, chacha] {
    let s = format!("{key:?}");
    assert!(s.contains("<redacted>"), "{s:?}");
    // The raw byte sequence must NOT appear in any form. Spot-check that
    // the suspect byte does not appear as a hex literal or as the decimal
    // form `[u8; N]`'s default Debug would emit.
    assert!(!s.contains("0xAB"), "raw byte leak in {s:?}");
    assert!(!s.contains("0xCD"), "raw byte leak in {s:?}");
    assert!(!s.contains("0xEF"), "raw byte leak in {s:?}");
    assert!(!s.contains("0x99"), "raw byte leak in {s:?}");
    // `[u8; N]` default Debug uses decimal in a comma-separated array form
    // (e.g. `[171, 171, ...]` for `[0xAB; 16]`). Check those don't appear.
    assert!(!s.contains("171"), "raw byte (decimal) leak in {s:?}");
    assert!(!s.contains("205"), "raw byte (decimal) leak in {s:?}");
    assert!(!s.contains("239"), "raw byte (decimal) leak in {s:?}");
    assert!(!s.contains("153"), "raw byte (decimal) leak in {s:?}");
  }
}

/// The custom `SecretKey::Debug` impl composes through containers that
/// derive `Debug` field-wise — `Keyring` (and therefore `EncryptionOptions`
/// once it carries a `Keyring`) renders each `SecretKey` field via the
/// redacted impl, so the raw key bytes never reach a log line.
#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn keyring_debug_redacts_through_secret_key() {
  let kr = Keyring::with_secondaries(
    SecretKey::Aes256([0xAB; 32]),
    vec![SecretKey::ChaCha20Poly1305([0xCD; 32])],
  );
  let s = format!("{kr:?}");
  assert!(s.contains("<redacted>"), "{s:?}");
  assert!(!s.contains("171"), "raw byte leak through Keyring");
  assert!(!s.contains("205"), "raw byte leak through Keyring");
}

#[cfg(feature = "aes-gcm")]
#[test]
fn random_aes_constructors_produce_keys_of_the_right_length() {
  assert_eq!(SecretKey::random_aes128().as_bytes().len(), 16);
  assert_eq!(SecretKey::random_aes192().as_bytes().len(), 24);
  assert_eq!(SecretKey::random_aes256().as_bytes().len(), 32);
  // Random keys differ across calls — collision probability is ~2^-128.
  assert_ne!(
    SecretKey::random_aes128().as_bytes(),
    SecretKey::random_aes128().as_bytes()
  );
}

#[cfg(feature = "chacha20-poly1305")]
#[test]
fn random_chacha20poly1305_constructor_produces_key_of_the_right_length() {
  let key = SecretKey::random_chacha20poly1305();
  assert_eq!(key.as_bytes().len(), 32);
  assert_eq!(key.algorithm(), EncryptAlgorithm::ChaCha20Poly1305);
}

#[test]
fn encryption_error_display_carries_expected_substrings() {
  let e = EncryptionError::AuthFailed;
  assert!(e.to_string().contains("auth"));

  let e = EncryptionError::UnsupportedAlgorithm(99);
  assert!(e.to_string().contains("99"));

  let e = EncryptionError::Oversize(OversizeCiphertext::new(1024, 64));
  let s = e.to_string();
  assert!(s.contains("1024"), "display carries claimed length: {s}");
  assert!(s.contains("64"), "display carries max ceiling: {s}");

  let e = EncryptionError::NoMatchingKey;
  assert!(!e.to_string().is_empty());

  let e = EncryptionError::KeyMismatch;
  assert!(!e.to_string().is_empty());

  let e = EncryptionError::MalformedFrame;
  assert!(!e.to_string().is_empty());

  let e = EncryptionError::EncryptionRequired;
  assert!(!e.to_string().is_empty());
}

#[test]
fn oversize_ciphertext_accessors_round_trip() {
  let o = OversizeCiphertext::new(2048, 1024);
  assert_eq!(o.claimed(), 2048);
  assert_eq!(o.max(), 1024);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn aes_gcm_aes128_roundtrip() {
  let key = SecretKey::Aes128([1u8; 16]);
  let nonce = [2u8; 12];
  let plaintext = b"hello memberlist gossip";
  let aad = b"\x0d\x01"; // [Encrypted tag = 13, AesGcm tag = 1]
  let ct = encrypt(EncryptAlgorithm::AesGcm, &key, &nonce, plaintext, aad).expect("encrypt");
  assert_eq!(
    ct.len(),
    plaintext.len() + 16,
    "AES-GCM appends a 16-byte tag"
  );
  let pt = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, aad).expect("decrypt");
  assert_eq!(pt, plaintext);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn aes_gcm_aes192_roundtrip() {
  let key = SecretKey::Aes192([3u8; 24]);
  let nonce = [4u8; 12];
  let plaintext = b"another payload";
  let aad = b"\x0d\x01";
  let ct = encrypt(EncryptAlgorithm::AesGcm, &key, &nonce, plaintext, aad).expect("encrypt");
  let pt = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, aad).expect("decrypt");
  assert_eq!(pt, plaintext);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn aes_gcm_aes256_roundtrip() {
  let key = SecretKey::Aes256([5u8; 32]);
  let nonce = [6u8; 12];
  let plaintext = b"a third payload to exercise AES-256";
  let aad = b"\x0d\x01";
  let ct = encrypt(EncryptAlgorithm::AesGcm, &key, &nonce, plaintext, aad).expect("encrypt");
  let pt = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, aad).expect("decrypt");
  assert_eq!(pt, plaintext);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn aes_gcm_wrong_key_fails_auth() {
  let k1 = SecretKey::Aes256([0xAA; 32]);
  let k2 = SecretKey::Aes256([0xBB; 32]);
  let nonce = [7u8; 12];
  let aad = b"\x0d\x01";
  let ct = encrypt(EncryptAlgorithm::AesGcm, &k1, &nonce, b"secret", aad).expect("encrypt");
  let err = decrypt(EncryptAlgorithm::AesGcm, &k2, &nonce, &ct, aad).expect_err("auth must fail");
  assert!(matches!(err, EncryptionError::AuthFailed));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn aes_gcm_aad_mismatch_fails_auth() {
  let key = SecretKey::Aes256([0xCC; 32]);
  let nonce = [8u8; 12];
  let ct = encrypt(
    EncryptAlgorithm::AesGcm,
    &key,
    &nonce,
    b"protected",
    b"\x0d\x01",
  )
  .expect("encrypt");
  // AAD swapped (e.g. ChaCha20-Poly1305 tag = 2 instead of AES-GCM tag = 1).
  let err = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, b"\x0d\x02")
    .expect_err("AAD mismatch must fail auth");
  assert!(matches!(err, EncryptionError::AuthFailed));
}

#[cfg(feature = "chacha20-poly1305")]
#[test]
fn chacha20poly1305_roundtrip() {
  let key = SecretKey::ChaCha20Poly1305([0x42; 32]);
  let nonce = [9u8; 12];
  let plaintext = b"chacha20-poly1305 payload";
  let aad = b"\x0d\x02";
  let ct = encrypt(
    EncryptAlgorithm::ChaCha20Poly1305,
    &key,
    &nonce,
    plaintext,
    aad,
  )
  .expect("encrypt");
  assert_eq!(
    ct.len(),
    plaintext.len() + 16,
    "ChaCha20-Poly1305 appends a 16-byte tag"
  );
  let pt = decrypt(EncryptAlgorithm::ChaCha20Poly1305, &key, &nonce, &ct, aad).expect("decrypt");
  assert_eq!(pt, plaintext);
}

#[cfg(feature = "chacha20-poly1305")]
#[test]
fn chacha20poly1305_wrong_key_fails_auth() {
  let k1 = SecretKey::ChaCha20Poly1305([0xAA; 32]);
  let k2 = SecretKey::ChaCha20Poly1305([0xBB; 32]);
  let nonce = [0x11; 12];
  let aad = b"\x0d\x02";
  let ct = encrypt(
    EncryptAlgorithm::ChaCha20Poly1305,
    &k1,
    &nonce,
    b"secret",
    aad,
  )
  .expect("encrypt");
  let err =
    decrypt(EncryptAlgorithm::ChaCha20Poly1305, &k2, &nonce, &ct, aad).expect_err("auth must fail");
  assert!(matches!(err, EncryptionError::AuthFailed));
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn key_variant_must_match_algorithm() {
  // An AES key passed to the ChaCha algorithm path is a misuse caught by
  // the key-mismatch branch — surfaced as KeyMismatch, never a panic.
  let key = SecretKey::Aes128([0u8; 16]);
  let err = encrypt(
    EncryptAlgorithm::ChaCha20Poly1305,
    &key,
    &[0u8; 12],
    b"data",
    b"\x0d\x02",
  )
  .expect_err("variant/algo mismatch must err");
  assert!(matches!(err, EncryptionError::KeyMismatch));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn unknown_algorithm_fails_both_directions() {
  let algo = EncryptAlgorithm::Unknown(99);
  let key = SecretKey::Aes128([0u8; 16]);
  assert!(matches!(
    encrypt(algo, &key, &[0u8; 12], b"data", b""),
    Err(EncryptionError::UnsupportedAlgorithm(99))
  ));
  assert!(matches!(
    decrypt(algo, &key, &[0u8; 12], b"data", b""),
    Err(EncryptionError::UnsupportedAlgorithm(99))
  ));
}

#[cfg(feature = "aes-gcm")]
fn k_a() -> SecretKey {
  SecretKey::Aes128([0xAA; 16])
}

#[cfg(feature = "aes-gcm")]
fn k_b() -> SecretKey {
  SecretKey::Aes256([0xBB; 32])
}

#[cfg(feature = "chacha20-poly1305")]
#[allow(dead_code)]
fn k_c() -> SecretKey {
  SecretKey::ChaCha20Poly1305([0xCC; 32])
}

#[cfg(feature = "aes-gcm")]
#[test]
fn keyring_new_has_only_primary() {
  let kr = Keyring::new(k_a());
  assert_eq!(kr.primary_ref().as_bytes(), k_a().as_bytes());
  assert!(kr.secondaries().is_empty());
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn keyring_with_secondaries_seeds_them() {
  let kr = Keyring::with_secondaries(k_a(), vec![k_b(), k_c()]);
  assert_eq!(kr.primary_ref().as_bytes(), k_a().as_bytes());
  assert_eq!(kr.secondaries().len(), 2);
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn keyring_insert_secondary_grows_list() {
  let mut kr = Keyring::new(k_a());
  kr.insert_secondary(k_b());
  kr.insert_secondary(k_c());
  assert_eq!(kr.secondaries().len(), 2);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn keyring_insert_secondary_is_no_op_if_already_present() {
  let mut kr = Keyring::with_secondaries(k_a(), vec![k_b()]);
  kr.insert_secondary(k_b());
  assert_eq!(kr.secondaries().len(), 1, "duplicates are not added");
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn keyring_remove_secondary_drops_the_named_key() {
  let mut kr = Keyring::with_secondaries(k_a(), vec![k_b(), k_c()]);
  kr.remove_secondary(k_b().as_bytes()).expect("remove ok");
  assert_eq!(kr.secondaries().len(), 1);
  assert_eq!(
    kr.secondaries()[0].as_bytes(),
    k_c().as_bytes(),
    "the remaining secondary is k_c"
  );
}

#[cfg(feature = "aes-gcm")]
#[test]
fn keyring_remove_primary_errors() {
  let mut kr = Keyring::with_secondaries(k_a(), vec![k_b()]);
  let err = kr
    .remove_secondary(k_a().as_bytes())
    .expect_err("cannot remove primary");
  assert!(matches!(err, KeyringError::IsPrimary));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn keyring_remove_unknown_errors() {
  let mut kr = Keyring::new(k_a());
  let err = kr
    .remove_secondary(k_b().as_bytes())
    .expect_err("missing key");
  assert!(matches!(err, KeyringError::NotInRing));
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn keyring_promote_swaps_primary_and_secondary() {
  let mut kr = Keyring::with_secondaries(k_a(), vec![k_b(), k_c()]);
  kr.promote(k_b().as_bytes()).expect("promote ok");
  assert_eq!(kr.primary_ref().as_bytes(), k_b().as_bytes());
  assert_eq!(kr.secondaries().len(), 2);
  // Old primary lands at index 0 (prepend, not append).
  assert_eq!(
    kr.secondaries()[0].as_bytes(),
    k_a().as_bytes(),
    "old primary lands at index 0 after promote"
  );
  assert_eq!(
    kr.secondaries()[1].as_bytes(),
    k_c().as_bytes(),
    "the un-promoted secondary shifts to index 1"
  );
}

#[cfg(feature = "aes-gcm")]
#[test]
fn keyring_promote_primary_is_no_op_ok() {
  let mut kr = Keyring::with_secondaries(k_a(), vec![k_b()]);
  kr.promote(k_a().as_bytes())
    .expect("promote primary is a no-op");
  assert_eq!(kr.primary_ref().as_bytes(), k_a().as_bytes());
  assert_eq!(kr.secondaries().len(), 1);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn keyring_promote_unknown_errors() {
  let mut kr = Keyring::new(k_a());
  let err = kr.promote(k_b().as_bytes()).expect_err("missing key");
  assert!(matches!(err, KeyringError::NotInRing));
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn keyring_allows_mixed_ciphers() {
  let kr = Keyring::with_secondaries(k_a(), vec![k_c()]);
  assert_eq!(kr.primary_ref().algorithm(), EncryptAlgorithm::AesGcm);
  assert_eq!(
    kr.secondaries()[0].algorithm(),
    EncryptAlgorithm::ChaCha20Poly1305
  );
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn keyring_insert_distinguishes_aes256_from_chacha20_with_same_bytes() {
  // SecretKey::Aes256 and SecretKey::ChaCha20Poly1305 both carry [u8; 32]; the
  // bytes can coincide but the cipher choice must not. PartialEq on the enum
  // compares the variant tag too — so the second insert lands as a distinct key.
  let bytes = [0xFF; 32];
  let aes = SecretKey::Aes256(bytes);
  let chacha = SecretKey::ChaCha20Poly1305(bytes);
  let mut kr = Keyring::new(aes);
  kr.insert_secondary(chacha);
  assert_eq!(
    kr.secondaries().len(),
    1,
    "different-variant same-byte keys are NOT duplicates"
  );
  assert_eq!(
    kr.secondaries()[0].algorithm(),
    EncryptAlgorithm::ChaCha20Poly1305,
    "the chacha secondary survives the insert (it is not collapsed into the AES primary)"
  );
}

#[test]
fn encryption_options_default_is_disabled() {
  let opts = EncryptionOptions::new();
  assert!(opts.keyring().is_none());
  assert!(!opts.is_enabled());
  // `Default::default()` is `new()` by explicit delegation.
  assert!(!EncryptionOptions::default().is_enabled());
  assert!(EncryptionOptions::default().keyring().is_none());
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encryption_options_with_keyring_enables() {
  let kr = Keyring::new(SecretKey::Aes128([0u8; 16]));
  let opts = EncryptionOptions::new().with_keyring(kr.clone());
  assert!(opts.is_enabled());
  assert!(opts.keyring().is_some());
  assert_eq!(
    opts.keyring().unwrap().primary_ref().as_bytes(),
    kr.primary_ref().as_bytes()
  );
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encryption_options_set_and_clear_keyring() {
  let mut opts = EncryptionOptions::new();
  assert!(!opts.is_enabled());
  opts.set_keyring(Keyring::new(SecretKey::Aes256([1u8; 32])));
  assert!(opts.is_enabled());
  opts.clear_keyring();
  assert!(!opts.is_enabled());
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encrypted_frame_roundtrip_aes_gcm() {
  let key = SecretKey::Aes256([0x42; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let plaintext = b"a memberlist gossip datagram body".repeat(4);
  let frame = encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, &plaintext).expect("encode");
  // [Encrypted tag][algo tag][nonce 12B][ciphertext + 16B tag]
  assert_eq!(
    frame[0], ENCRYPTED_TAG,
    "leading byte is the Encrypted wrapper tag"
  );
  assert_eq!(
    frame[1],
    EncryptAlgorithm::AesGcm.tag(),
    "second byte is the algorithm tag"
  );
  assert_eq!(
    frame.len(),
    2 + 12 + plaintext.len() + 16,
    "wrapper overhead is 30 bytes"
  );
  let back = decode_encrypted_frame(&opts, &frame, 1 << 20).expect("decode");
  assert_eq!(back, plaintext);
}

#[cfg(feature = "chacha20-poly1305")]
#[test]
fn encrypted_frame_roundtrip_chacha20poly1305() {
  let key = SecretKey::ChaCha20Poly1305([0x9A; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let plaintext = b"a chacha-encrypted payload".to_vec();
  let frame =
    encode_encrypted_frame(EncryptAlgorithm::ChaCha20Poly1305, &key, &plaintext).expect("encode");
  assert_eq!(frame[0], ENCRYPTED_TAG);
  assert_eq!(frame[1], EncryptAlgorithm::ChaCha20Poly1305.tag());
  let back = decode_encrypted_frame(&opts, &frame, 1 << 20).expect("decode");
  assert_eq!(back, plaintext);
}

#[cfg(feature = "aes-gcm")]
#[test]
fn oversize_ciphertext_is_rejected_before_allocation() {
  // A hand-built frame: Encrypted tag, AES-GCM tag, 12-byte nonce, then a
  // payload whose post-decrypt plaintext length is far over the ceiling.
  // The decoder must reject on the `plaintext_len > max` bound BEFORE
  // running AEAD (the test passing rather than OOM-aborting is the
  // assertion). The carried `claimed` is the plaintext length implied by
  // the wrapper, not the raw ciphertext length.
  let key = SecretKey::Aes256([0u8; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let mut frame = vec![ENCRYPTED_TAG, EncryptAlgorithm::AesGcm.tag()];
  frame.extend_from_slice(&[0u8; 12]);
  // 100 KiB of zeros — ciphertext (100 KiB) - AUTH_TAG_LEN (16) =
  // implied plaintext 100 KiB - 16; ceiling = 64 KiB.
  frame.resize(frame.len() + 100 * 1024, 0);
  let max = 64 * 1024;
  let err = decode_encrypted_frame(&opts, &frame, max).expect_err("oversize must err");
  match err {
    EncryptionError::Oversize(o) => {
      assert_eq!(o.claimed(), 100 * 1024 - AUTH_TAG_LEN);
      assert_eq!(o.max(), max);
    }
    other => panic!("expected Oversize, got {other:?}"),
  }
}

#[test]
fn encrypted_frame_unknown_algorithm_fails_decode() {
  let opts = EncryptionOptions::new();
  let mut frame = vec![ENCRYPTED_TAG, 222u8];
  frame.extend_from_slice(&[0u8; 12]); // nonce
  frame.extend_from_slice(b"trailing"); // body
  let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("unknown algo must err");
  assert!(matches!(err, EncryptionError::UnsupportedAlgorithm(222)));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn aes_gcm_trailing_junk_in_body_is_rejected() {
  let key = SecretKey::Aes256([0x55; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let mut frame = encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, b"data").expect("encode");
  // Append a trailing byte; AES-GCM auth must reject (the appended byte
  // either corrupts the tag or runs as un-authenticated overhang).
  frame.push(0xFF);
  let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("trailing junk must err");
  assert!(matches!(err, EncryptionError::AuthFailed));
}

#[cfg(feature = "chacha20-poly1305")]
#[test]
fn chacha20poly1305_trailing_junk_in_body_is_rejected() {
  let key = SecretKey::ChaCha20Poly1305([0x77; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let mut frame =
    encode_encrypted_frame(EncryptAlgorithm::ChaCha20Poly1305, &key, b"data").expect("encode");
  frame.push(0xFF);
  let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("trailing junk must err");
  assert!(matches!(err, EncryptionError::AuthFailed));
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn mixed_cipher_keyring_decrypts_chacha_with_chacha_secondary() {
  // Primary AES, secondary ChaCha20-Poly1305. A frame encrypted under the
  // ChaCha secondary must decode (the variant filter selects the matching
  // key; the primary is skipped because its variant does not match the wire
  // algorithm tag).
  let aes_primary = SecretKey::Aes256([0x12; 32]);
  let chacha_sec = SecretKey::ChaCha20Poly1305([0x34; 32]);
  let kr = Keyring::with_secondaries(aes_primary, vec![chacha_sec]);
  let opts = EncryptionOptions::new().with_keyring(kr);
  let frame = encode_encrypted_frame(
    EncryptAlgorithm::ChaCha20Poly1305,
    &chacha_sec,
    b"protected by the secondary",
  )
  .expect("encode");
  let back = decode_encrypted_frame(&opts, &frame, 1 << 20).expect("decode via secondary");
  assert_eq!(back, b"protected by the secondary");
}

#[cfg(feature = "aes-gcm")]
#[test]
fn decode_with_no_matching_key_errs() {
  // A frame encrypted under an outside key; the keyring carries two AES
  // candidates (primary + one secondary), both wrong; the variant filter
  // selects both, both fail AEAD, the loop exhausts → NoMatchingKey (the
  // multi-key fallback's "we tried, none worked" verdict). With a single
  // variant-matching candidate the decoder would surface the AEAD's own
  // `AuthFailed` instead — see `aes_gcm_trailing_junk_in_body_is_rejected`.
  let outside = SecretKey::Aes256([0x10; 32]);
  let k_primary = SecretKey::Aes256([0x11; 32]);
  let k_secondary = SecretKey::Aes256([0x22; 32]);
  let kr = Keyring::with_secondaries(k_primary, vec![k_secondary]);
  let opts = EncryptionOptions::new().with_keyring(kr);
  let frame =
    encode_encrypted_frame(EncryptAlgorithm::AesGcm, &outside, b"unrelated").expect("encode");
  let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("no matching key");
  assert!(matches!(err, EncryptionError::NoMatchingKey));
}

#[test]
fn encrypted_wrapper_overhead_constant_is_correct() {
  // Pinned value — a change is a wire-protocol or sizing-contract break.
  assert_eq!(ENCRYPTED_WRAPPER_OVERHEAD, 30);
  assert_eq!(
    ENCRYPTED_WRAPPER_OVERHEAD,
    WRAPPER_HEADER_LEN + AUTH_TAG_LEN
  );
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encrypted_gossip_at_mtu_minus_overhead_roundtrips_and_at_mtu_does_too() {
  // A plaintext payload sized `MTU - overhead` and a plaintext payload
  // sized `MTU` both must round-trip when `max_plaintext_len = MTU`:
  // the bound is on POST-DECRYPT plaintext length, not on the ciphertext
  // envelope. Bounding on `ciphertext.len()` would spuriously reject a
  // near-MTU plaintext because the AEAD tag inflates the envelope past
  // `MTU` even though the plaintext is in-bounds.
  let key = SecretKey::Aes256([0x42; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  const GOSSIP_MTU: usize = 1400;
  for plaintext_len in [GOSSIP_MTU - ENCRYPTED_WRAPPER_OVERHEAD, GOSSIP_MTU] {
    let plaintext = vec![0xCD; plaintext_len];
    let frame = encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, &plaintext).expect("encode");
    let back = decode_encrypted_frame(&opts, &frame, GOSSIP_MTU)
      .unwrap_or_else(|e| panic!("plaintext_len={plaintext_len} must round-trip, got {e}"));
    assert_eq!(back, plaintext, "plaintext_len={plaintext_len}");
  }
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encrypted_one_byte_past_plaintext_max_is_rejected() {
  // Symmetric guard for the new bound: a plaintext sized `max + 1` must
  // be rejected with `Oversize`, with the carried `claimed` equal to the
  // post-decrypt plaintext length (not the on-wire ciphertext length).
  let key = SecretKey::Aes256([0x77; 32]);
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
  let max = 256;
  let plaintext = vec![0xEF; max + 1];
  let frame = encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, &plaintext).expect("encode");
  let err = decode_encrypted_frame(&opts, &frame, max).expect_err("over-bound must err");
  match err {
    EncryptionError::Oversize(o) => {
      assert_eq!(
        o.claimed(),
        max + 1,
        "claimed is post-decrypt plaintext length"
      );
      assert_eq!(o.max(), max);
    }
    other => panic!("expected Oversize, got {other:?}"),
  }
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn encrypt_algorithm_u8_conversions_roundtrip() {
  for algo in [
    EncryptAlgorithm::AesGcm,
    EncryptAlgorithm::ChaCha20Poly1305,
    EncryptAlgorithm::Unknown(200),
  ] {
    let byte: u8 = algo.into();
    assert_eq!(byte, algo.tag());
    assert_eq!(EncryptAlgorithm::from(byte), algo);
  }
  // The named tags pin their numeric values through the From<u8> path too.
  assert_eq!(EncryptAlgorithm::from(1u8), EncryptAlgorithm::AesGcm);
  assert_eq!(
    EncryptAlgorithm::from(2u8),
    EncryptAlgorithm::ChaCha20Poly1305
  );
  assert_eq!(EncryptAlgorithm::from(9u8), EncryptAlgorithm::Unknown(9));
}

#[test]
fn encryption_error_entropy_display_is_nonempty() {
  assert!(!EncryptionError::Entropy.to_string().is_empty());
}

#[test]
fn keyring_error_display_strings_are_nonempty() {
  assert!(!KeyringError::IsPrimary.to_string().is_empty());
  assert!(!KeyringError::NotInRing.to_string().is_empty());
}

#[cfg(feature = "aes-gcm")]
#[test]
fn encryption_options_update_and_maybe_keyring() {
  let kr = Keyring::new(SecretKey::Aes128([3u8; 16]));
  // update_keyring assigns the raw Option in place.
  let mut opts = EncryptionOptions::new();
  opts.update_keyring(Some(kr.clone()));
  assert!(opts.is_enabled());
  opts.update_keyring(None);
  assert!(!opts.is_enabled());

  // maybe_keyring is the consuming-builder twin.
  let enabled = EncryptionOptions::new().maybe_keyring(Some(kr));
  assert!(enabled.is_enabled());
  let disabled = EncryptionOptions::new()
    .with_keyring(Keyring::new(SecretKey::Aes256([4u8; 32])))
    .maybe_keyring(None);
  assert!(!disabled.is_enabled());
}

#[cfg(feature = "aes-gcm")]
#[test]
fn decode_encrypted_frame_rejects_malformed_headers() {
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes128([0u8; 16])));
  // Shorter than the 2-byte minimum.
  assert!(matches!(
    decode_encrypted_frame(&opts, &[ENCRYPTED_TAG], 1 << 20),
    Err(EncryptionError::MalformedFrame)
  ));
  // Wrong leading tag.
  assert!(matches!(
    decode_encrypted_frame(&opts, &[0xAB, AES_GCM_TAG], 1 << 20),
    Err(EncryptionError::MalformedFrame)
  ));
  // Correct tag + known algorithm, but shorter than header + auth tag.
  let short = [ENCRYPTED_TAG, AES_GCM_TAG, 0, 0, 0];
  assert!(matches!(
    decode_encrypted_frame(&opts, &short, 1 << 20),
    Err(EncryptionError::MalformedFrame)
  ));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn decode_encrypted_frame_unknown_algo_beats_short_length() {
  // The algorithm-tag check runs BEFORE the full-header length bound, so an
  // unknown-algorithm frame is UnsupportedAlgorithm even when it is far too
  // short to be a real wrapper.
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes128([0u8; 16])));
  let frame = [ENCRYPTED_TAG, 222u8];
  assert!(matches!(
    decode_encrypted_frame(&opts, &frame, 1 << 20),
    Err(EncryptionError::UnsupportedAlgorithm(222))
  ));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn decode_encrypted_frame_with_disabled_encryption_is_no_matching_key() {
  // A well-formed-length encrypted frame but no keyring configured: the
  // decoder cannot decrypt it and surfaces NoMatchingKey (the gossip-path
  // drop disposition), not a panic.
  let opts = EncryptionOptions::new();
  let mut frame = vec![ENCRYPTED_TAG, AES_GCM_TAG];
  frame.extend_from_slice(&[0u8; NONCE_LEN]);
  frame.extend_from_slice(&[0u8; AUTH_TAG_LEN]); // minimal ciphertext+tag
  assert!(matches!(
    decode_encrypted_frame(&opts, &frame, 1 << 20),
    Err(EncryptionError::NoMatchingKey)
  ));
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn decrypt_rejects_key_variant_algorithm_mismatch() {
  // The variant/algorithm precheck guards the decrypt path too (symmetric
  // with `key_variant_must_match_algorithm`, which covers encrypt): an AES
  // key handed to the ChaCha algorithm is KeyMismatch, never a panic.
  let key = SecretKey::Aes128([0u8; 16]);
  let err = decrypt(
    EncryptAlgorithm::ChaCha20Poly1305,
    &key,
    &[0u8; 12],
    b"ciphertext-bytes",
    b"\x0d\x02",
  )
  .expect_err("variant/algo mismatch must err");
  assert!(matches!(err, EncryptionError::KeyMismatch));
}

#[cfg(feature = "aes-gcm")]
#[test]
fn aes_gcm_aes128_and_aes192_decrypt_fail_auth_on_corrupt_ciphertext() {
  // The AES-128 and AES-192 decrypt arms (distinct from the AES-256 arm the
  // other failure tests use) must surface AuthFailed when the ciphertext is
  // corrupted, not panic.
  let nonce = [7u8; 12];
  let aad = b"\x0d\x01";
  for key in [SecretKey::Aes128([0x11; 16]), SecretKey::Aes192([0x22; 24])] {
    let mut ct = encrypt(EncryptAlgorithm::AesGcm, &key, &nonce, b"secret", aad).expect("encrypt");
    // Flip a ciphertext byte so the AEAD tag check fails.
    ct[0] ^= 0xFF;
    let err = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, aad)
      .expect_err("corrupt ciphertext must fail auth");
    assert!(matches!(err, EncryptionError::AuthFailed), "got {err:?}");
  }
}

#[cfg(feature = "aes-gcm")]
#[test]
fn keyring_insert_secondary_equal_to_primary_is_dropped() {
  // Inserting a key byte-equal to the primary hits the primary-equality
  // guard (distinct from the existing-secondary guard) and is a no-op.
  let mut kr = Keyring::new(k_a());
  kr.insert_secondary(k_a());
  assert!(
    kr.secondaries().is_empty(),
    "a duplicate of the primary must not be added as a secondary"
  );
}

#[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
#[test]
fn decode_skips_secondary_whose_algorithm_does_not_match() {
  // Frame encrypted under an outside ChaCha key. Keyring: a ChaCha primary
  // (matches the algo, tried, fails auth → falls through) and an AES
  // secondary (algorithm mismatch → skipped via the variant filter). With
  // the only variant-matching candidate having failed, the verdict is the
  // AEAD's own AuthFailed.
  let outside = SecretKey::ChaCha20Poly1305([0x10; 32]);
  let chacha_primary = SecretKey::ChaCha20Poly1305([0x11; 32]);
  let aes_secondary = SecretKey::Aes256([0x22; 32]);
  let kr = Keyring::with_secondaries(chacha_primary, vec![aes_secondary]);
  let opts = EncryptionOptions::new().with_keyring(kr);
  let frame = encode_encrypted_frame(
    EncryptAlgorithm::ChaCha20Poly1305,
    &outside,
    b"unrelated bytes",
  )
  .expect("encode");
  let err = decode_encrypted_frame(&opts, &frame, 1 << 20)
    .expect_err("no usable key: AES secondary is skipped, ChaCha primary fails auth");
  assert!(matches!(err, EncryptionError::AuthFailed), "got {err:?}");
}

#[cfg(all(feature = "serde", feature = "aes-gcm"))]
#[test]
fn secret_key_serde_is_tagged_base64() {
  let key = SecretKey::Aes256([7u8; 32]);
  let json = serde_json::to_string(&key).unwrap();
  assert!(
    json.contains("\"aes256\""),
    "tagged by cipher; json = {json}"
  );
  assert!(
    !json.contains('['),
    "key is base64, not a byte array; json = {json}"
  );
  assert_eq!(serde_json::from_str::<SecretKey>(&json).unwrap(), key);
}

#[cfg(all(feature = "serde", feature = "aes-gcm"))]
#[test]
fn encryption_options_serde_roundtrip_and_default() {
  // Empty config = the default (no keyring).
  assert_eq!(
    serde_json::from_str::<EncryptionOptions>("{}").unwrap(),
    EncryptionOptions::new()
  );
  let keyring =
    Keyring::with_secondaries(SecretKey::Aes128([1u8; 16]), [SecretKey::Aes256([2u8; 32])]);
  let opts = EncryptionOptions::new().with_keyring(keyring);
  let json = serde_json::to_string(&opts).unwrap();
  assert_eq!(
    serde_json::from_str::<EncryptionOptions>(&json).unwrap(),
    opts
  );
}

#[cfg(all(feature = "serde", feature = "aes-gcm"))]
#[test]
fn encryption_options_serde_rejects_unknown_field() {
  // A misspelled `keyring` (here `key_ring`) must be rejected rather than
  // silently ignored — silently dropping it would start the node with no
  // encryption while the operator believes a keyring was configured.
  let misspelled = r#"{"key_ring":{"primary":{"aes256":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},"secondaries":[]}}"#;
  assert!(serde_json::from_str::<EncryptionOptions>(misspelled).is_err());
}

#[cfg(all(feature = "clap", feature = "aes-gcm"))]
#[test]
fn secret_key_from_str_is_cipher_base64() {
  use base64::Engine as _;
  let b64 = base64::engine::general_purpose::STANDARD.encode([9u8; 32]);
  let parsed: SecretKey = format!("aes256:{b64}").parse().unwrap();
  assert_eq!(parsed, SecretKey::Aes256([9u8; 32]));
  // A length that does not match the named cipher is rejected.
  assert!("aes128:AAAA".parse::<SecretKey>().is_err());
  // An unknown cipher tag is rejected.
  assert!("rot13:AAAA".parse::<SecretKey>().is_err());
}

#[cfg(all(feature = "clap", feature = "aes-gcm"))]
#[test]
fn encryption_options_clap_flattens_with_no_flags() {
  use clap::Parser;

  #[derive(Parser)]
  struct Cli {
    #[command(flatten)]
    encryption: EncryptionOptions,
  }

  // The keyring is clap-skipped (no secrets on the CLI), so this parses with no
  // flags and the keyring defaults to absent.
  let cli = Cli::try_parse_from(["app"]).unwrap();
  assert!(cli.encryption.keyring().is_none());
}

#[cfg(all(feature = "serde", feature = "aes-gcm"))]
#[test]
fn secret_key_serde_rejects_multi_cipher_map() {
  // A map naming two ciphers is ambiguous and must be rejected outright.
  let two = r#"{"aes128":"AAAAAAAAAAAAAAAAAAAAAA==","aes256":"AAAA"}"#;
  assert!(serde_json::from_str::<SecretKey>(two).is_err());
}
