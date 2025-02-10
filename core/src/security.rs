use aead::{generic_array::GenericArray, AeadInPlace, KeyInit};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use bytes::{Buf, BufMut, BytesMut};
use rand::Rng;

use super::{
  keyring::{Keyring, KeyringError},
  types::{EncryptionAlgorithm, SecretKey, SecretKeys},
};

const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const BLOCK_SIZE: usize = 16;

type Aes192Gcm = AesGcm<Aes192, U12>;

/// Security errors
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EncryptionError {
  /// Unknown encryption algorithm
  #[error("unknown encryption algorithm: {0}")]
  UnknownEncryptionAlgorithm(u8),
  /// Encryt/Decrypt errors
  #[error("failed to encrypt/decrypt")]
  Encryptor,
  /// Security feature is disabled
  #[error("security related feature is disabled")]
  Disabled,
  /// Payload is too small to decrypt
  #[error("payload is too small to decrypt")]
  SmallPayload,
  /// No installed keys could decrypt the message
  #[error("no installed keys could decrypt the message")]
  NoInstalledKeys,
  /// Secret key is not in the keyring
  #[error(transparent)]
  Keyring(#[from] KeyringError),
}

impl From<aead::Error> for EncryptionError {
  fn from(_: aead::Error) -> Self {
    Self::Encryptor
  }
}

#[inline]
const fn encrypt_overhead(algo: EncryptionAlgorithm) -> usize {
  match algo {
    EncryptionAlgorithm::Pkcs7 => 44, // IV: 12, Padding: 16, Tag: 16
    EncryptionAlgorithm::NoPadding => 28, // IV: 12, Tag: 16
    _ => 0,
  }
}

#[inline]
const fn encrypted_length(algo: EncryptionAlgorithm, inp: usize) -> usize {
  match algo {
    EncryptionAlgorithm::Pkcs7 => {
      // Determine the padding size
      let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

      // Sum the extra parts to get total size
      NONCE_SIZE + inp + padding + TAG_SIZE
    }
    EncryptionAlgorithm::NoPadding => NONCE_SIZE + inp + TAG_SIZE,
    _ => inp,
  }
}

/// pkcs7encode is used to pad a byte buffer to a specific block size using
/// the PKCS7 algorithm. "Ignores" some bytes to compensate for IV
#[inline]
pub(crate) fn pkcs7encode(buf: &mut impl BufMut, buf_len: usize, ignore: usize, block_size: usize) {
  let n = buf_len - ignore;
  let more = block_size - (n % block_size);
  buf.put_bytes(more as u8, more);
}

/// pkcs7decode is used to decode a buffer that has been padded
#[inline]
fn pkcs7decode(buf: &mut BytesMut) {
  if buf.is_empty() {
    panic!("Cannot decode a PKCS7 buffer of zero length");
  }
  let n = buf.len();
  let last = buf[n - 1];
  let n = n - (last as usize);
  buf.truncate(n);
}

pub(super) fn write_header(dst: &mut BytesMut) -> [u8; NONCE_SIZE] {
  // Add a random nonce
  let mut nonce = [0u8; NONCE_SIZE];
  rand::rng().fill(&mut nonce);
  dst.put_slice(&nonce);

  nonce
}

pub(super) fn read_nonce(src: &mut BytesMut) -> [u8; NONCE_SIZE] {
  let mut nonce = [0u8; NONCE_SIZE];
  nonce.copy_from_slice(&src[..NONCE_SIZE]);
  src.advance(NONCE_SIZE);
  nonce
}

pub(super) fn encrypt(
  algo: EncryptionAlgorithm,
  pk: SecretKey,
  nonce: [u8; NONCE_SIZE],
  auth_data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), EncryptionError> {
  match algo {
    EncryptionAlgorithm::NoPadding => {}
    EncryptionAlgorithm::Pkcs7 => {
      let buf_len = dst.len();
      pkcs7encode(dst, buf_len, 0, BLOCK_SIZE);
    }
    _ => return Err(EncryptionError::UnknownEncryptionAlgorithm(algo.as_u8())),
  }

  match pk {
    SecretKey::Aes128(pk) => {
      let gcm = Aes128Gcm::new(GenericArray::from_slice(&pk));
      gcm
        .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
        .map_err(Into::into)
    }
    SecretKey::Aes192(pk) => {
      let gcm = Aes192Gcm::new(GenericArray::from_slice(&pk));
      gcm
        .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
        .map_err(Into::into)
    }
    SecretKey::Aes256(pk) => {
      let gcm = Aes256Gcm::new(GenericArray::from_slice(&pk));
      gcm
        .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
        .map_err(Into::into)
    }
  }
}

pub(super) fn decrypt(
  key: SecretKey,
  algo: EncryptionAlgorithm,
  nonce: [u8; NONCE_SIZE],
  auth_data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), EncryptionError> {
  if algo.is_unknown() {
    return Err(EncryptionError::UnknownEncryptionAlgorithm(algo.as_u8()));
  }

  // Get the AES block cipher
  match key {
    SecretKey::Aes128(pk) => {
      let gcm = Aes128Gcm::new(GenericArray::from_slice(&pk));
      gcm
        .decrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
        .map_err(Into::into)
    }
    SecretKey::Aes192(pk) => {
      let gcm = Aes192Gcm::new(GenericArray::from_slice(&pk));
      gcm
        .decrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
        .map_err(Into::into)
    }
    SecretKey::Aes256(pk) => {
      let gcm = Aes256Gcm::new(GenericArray::from_slice(&pk));
      gcm
        .decrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
        .map_err(Into::into)
    }
  }
  .inspect(|_| {
    if algo.is_pkcs_7() {
      pkcs7decode(dst);
    }
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  fn encrypt_decrypt_versioned(vsn: EncryptionAlgorithm) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    let nonce = write_header(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    encrypt(vsn, k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = encrypted_length(vsn, plain_text.len());
    assert_eq!(encrypted.len(), exp_len);

    read_nonce(&mut encrypted);
    decrypt(k1, vsn, nonce, extra, &mut encrypted).unwrap();
    assert_eq!(encrypted.as_ref(), plain_text);
  }

  async fn decrypt_by_other_key(algo: EncryptionAlgorithm) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    let nonce = write_header(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    encrypt(algo, k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = encrypted_length(algo, plain_text.len());
    assert_eq!(encrypted.len(), exp_len);
    read_nonce(&mut encrypted);

    for (idx, k) in TEST_KEYS.iter().rev().enumerate() {
      if idx == TEST_KEYS.len() - 1 {
        decrypt(*k, algo, nonce, extra, &mut encrypted).unwrap();
        assert_eq!(encrypted.as_ref(), plain_text);
        return;
      }
      let e = decrypt(*k, algo, nonce, extra, &mut encrypted).unwrap_err();
      assert_eq!(e.to_string(), "aead::Error");
    }
  }

  #[test]
  fn test_encrypt_decrypt_v0() {
    encrypt_decrypt_versioned(EncryptionAlgorithm::Pkcs7);
  }

  #[test]
  fn test_encrypt_decrypt_v1() {
    encrypt_decrypt_versioned(EncryptionAlgorithm::NoPadding);
  }

  #[tokio::test]
  async fn test_decrypt_by_other_key_v0() {
    let algo = EncryptionAlgorithm::Pkcs7;
    decrypt_by_other_key(algo).await;
  }

  #[tokio::test]
  async fn test_decrypt_by_other_key_v1() {
    let algo = EncryptionAlgorithm::NoPadding;
    decrypt_by_other_key(algo).await;
  }

  const TEST_KEYS: &[SecretKey] = &[
    SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
  ];
}
