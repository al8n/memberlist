use aead::{generic_array::GenericArray, AeadInPlace, KeyInit};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use bytes::{Buf, BufMut, BytesMut};
use memberlist_core::transport::Wire;
pub use memberlist_core::types::{SecretKey, Keyring, KeyringError, SecretKeys};
use nodecraft::resolver::AddressResolver;
use rand::Rng;

use crate::{NetTransportError, ENCRYPT_TAG};

type Aes192Gcm = AesGcm<Aes192, U12>;

impl<A: AddressResolver, W: Wire> From<UnknownEncryptionAlgo> for NetTransportError<A, W> {
  fn from(value: UnknownEncryptionAlgo) -> Self {
    Self::Security(SecurityError::UnknownEncryptionAlgo(value))
  }
}

impl<A: AddressResolver, W: Wire> From<aead::Error> for NetTransportError<A, W> {
  fn from(value: aead::Error) -> Self {
    Self::Security(value.into())
  }
}

impl<A: AddressResolver, W: Wire> From<EncryptorError> for NetTransportError<A, W> {
  fn from(value: EncryptorError) -> Self {
    Self::Security(value.into())
  }
}

/// Encrypt/Decrypt errors.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EncryptorError {
  /// AEAD ciphers error
  #[error(transparent)]
  Aead(#[from] aead::Error),
}

/// Security errors
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum SecurityError {
  /// Unknown encryption algorithm
  #[error("security: unknown encryption version: {0}")]
  UnknownEncryptionAlgo(#[from] UnknownEncryptionAlgo),
  /// Encryt/Decrypt errors
  #[error("security: {0}")]
  Encryptor(#[from] EncryptorError),
  /// Security feature is disabled
  #[error("security: security related feature is disabled")]
  Disabled,
  /// Payload is too small to decrypt
  #[error("security: payload is too small to decrypt")]
  SmallPayload,
  /// No installed keys could decrypt the message
  #[error("security: no installed keys could decrypt the message")]
  NoInstalledKeys,
  /// Secret key is not in the keyring
  #[error("security: {0}")]
  Keyring(#[from] KeyringError),
}

impl From<aead::Error> for SecurityError {
  fn from(value: aead::Error) -> Self {
    Self::Encryptor(EncryptorError::Aead(value))
  }
}

/// Unknown encryption algorithm
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct UnknownEncryptionAlgo(u8);

impl core::fmt::Display for UnknownEncryptionAlgo {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "unknown encryption algorithm {}", self.0)
  }
}

impl std::error::Error for UnknownEncryptionAlgo {}

/// Encryption algorithm
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
#[non_exhaustive]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum EncryptionAlgo {
  /// AES-GCM, using PKCS7 padding
  #[default]
  PKCS7 = { *ENCRYPT_TAG.start() },
  /// AES-GCM, no padding. Padding not needed,
  NoPadding = { *ENCRYPT_TAG.start() + 1 },
}

impl TryFrom<u8> for EncryptionAlgo {
  type Error = UnknownEncryptionAlgo;

  fn try_from(val: u8) -> Result<Self, Self::Error> {
    match val {
      val if val.eq(ENCRYPT_TAG.start()) => Ok(Self::PKCS7),
      val if val == *ENCRYPT_TAG.start() + 1 => Ok(Self::NoPadding),
      val => Err(UnknownEncryptionAlgo(val)),
    }
  }
}

impl EncryptionAlgo {
  pub(crate) fn encrypt_overhead(&self) -> usize {
    match self {
      Self::PKCS7 => 49,     // Algo: 1, Len: 4, IV: 12, Padding: 16, Tag: 16
      Self::NoPadding => 33, // Algo: 1, Len: 4, IV: 12, Tag: 16
    }
  }

  pub(crate) const fn encrypted_length(&self, inp: usize) -> usize {
    match self {
      Self::PKCS7 => {
        // Determine the padding size
        let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

        // Sum the extra parts to get total size
        NONCE_SIZE + inp + padding + TAG_SIZE
      }
      Self::NoPadding => NONCE_SIZE + inp + TAG_SIZE,
    }
  }
}

pub(crate) const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
pub(crate) const BLOCK_SIZE: usize = 16;

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
  algo: EncryptionAlgo,
  pk: SecretKey,
  nonce: [u8; NONCE_SIZE],
  auth_data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), SecurityError> {
  match algo {
    EncryptionAlgo::NoPadding => {}
    EncryptionAlgo::PKCS7 => {
      let buf_len = dst.len();
      pkcs7encode(dst, buf_len, 0, BLOCK_SIZE);
    }
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
  algo: EncryptionAlgo,
  nonce: [u8; NONCE_SIZE],
  auth_data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), SecurityError> {
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
  .map(|_| match algo {
    EncryptionAlgo::NoPadding => {}
    EncryptionAlgo::PKCS7 => {
      pkcs7decode(dst);
    }
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  fn encrypt_decrypt_versioned(vsn: EncryptionAlgo) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    encrypted.put_u8(vsn as u8);
    encrypted.put_u32(0);
    let nonce = write_header(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    encrypt(vsn, k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = vsn.encrypted_length(plain_text.len()) + 5;
    assert_eq!(encrypted.len(), exp_len);

    encrypted.advance(5);
    read_nonce(&mut encrypted);
    decrypt(k1, vsn, nonce, extra, &mut encrypted).unwrap();
    assert_eq!(encrypted.as_ref(), plain_text);
  }

  async fn decrypt_by_other_key(algo: EncryptionAlgo) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    encrypted.put_u8(algo as u8);
    encrypted.put_u32(0);
    let nonce = write_header(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    encrypt(algo, k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = algo.encrypted_length(plain_text.len()) + 5;
    assert_eq!(encrypted.len(), exp_len);

    encrypted.advance(5);
    read_nonce(&mut encrypted);

    for (idx, k) in TEST_KEYS.iter().rev().enumerate() {
      if idx == TEST_KEYS.len() - 1 {
        decrypt(*k, algo, nonce, extra, &mut encrypted).unwrap();
        assert_eq!(encrypted.as_ref(), plain_text);
        return;
      }
      let e = decrypt(*k, algo, nonce, extra, &mut encrypted).unwrap_err();
      assert_eq!(e.to_string(), "security: aead::Error");
    }
  }

  #[test]
  fn test_encrypt_decrypt_v0() {
    encrypt_decrypt_versioned(EncryptionAlgo::PKCS7);
  }

  #[test]
  fn test_encrypt_decrypt_v1() {
    encrypt_decrypt_versioned(EncryptionAlgo::NoPadding);
  }

  #[tokio::test]
  async fn test_decrypt_by_other_key_v0() {
    let algo = EncryptionAlgo::PKCS7;
    decrypt_by_other_key(algo).await;
  }

  #[tokio::test]
  async fn test_decrypt_by_other_key_v1() {
    let algo = EncryptionAlgo::NoPadding;
    decrypt_by_other_key(algo).await;
  }

  const TEST_KEYS: &[SecretKey] = &[
    SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
  ];
}
