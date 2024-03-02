use aead::{generic_array::GenericArray, AeadInPlace, KeyInit};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use async_lock::RwLock;
use bytes::{Buf, BufMut, BytesMut};
use indexmap::IndexSet;
use memberlist_core::transport::Wire;
pub use memberlist_core::types::{SecretKey, SecretKeys};
use nodecraft::resolver::AddressResolver;
use rand::Rng;

use std::{iter::once, sync::Arc};

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
  #[error("{0}")]
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
  #[error("security: secret key is not in the keyring")]
  SecretKeyNotFound,
  /// Removing the primary key is not allowed
  #[error("security: removing the primary key is not allowed")]
  RemovePrimaryKey,
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

impl SecretKeyring {
  pub(super) fn write_header(&self, dst: &mut BytesMut) -> [u8; NONCE_SIZE] {
    // Add a random nonce
    let mut nonce = [0u8; NONCE_SIZE];
    rand::thread_rng().fill(&mut nonce);
    dst.put_slice(&nonce);

    nonce
  }

  pub(super) fn read_nonce(&self, src: &mut BytesMut) -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    nonce.copy_from_slice(&src[..NONCE_SIZE]);
    src.advance(NONCE_SIZE);
    nonce
  }

  pub(super) fn encrypt(
    &self,
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
    &self,
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
}

#[derive(Debug)]
pub(super) struct SecretKeyringInner {
  pub(super) primary_key: SecretKey,
  pub(super) keys: IndexSet<SecretKey>,
}

/// A lock-free and thread-safe container for a set of encryption keys.
/// The keyring contains all key data used internally by memberlist.
///
/// If creating a keyring with multiple keys, one key must be designated
/// primary by passing it as the primaryKey. If the primaryKey does not exist in
/// the list of secondary keys, it will be automatically added at position 0.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct SecretKeyring {
  pub(super) inner: Arc<RwLock<SecretKeyringInner>>,
}

impl SecretKeyring {
  /// Constructs a new container for a primary key. The
  /// keyring contains all key data used internally by memberlist.
  ///
  /// If only a primary key is passed, then it will be automatically added to the
  /// keyring.
  ///
  /// A key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn new(primary_key: SecretKey) -> Self {
    Self {
      inner: Arc::new(RwLock::new(SecretKeyringInner {
        primary_key,
        keys: IndexSet::new(),
      })),
    }
  }

  /// Constructs a new container for a set of encryption keys. The
  /// keyring contains all key data used internally by memberlist.
  ///
  /// If only a primary key is passed, then it will be automatically added to the
  /// keyring. If creating a keyring with multiple keys, one key must be designated
  /// primary by passing it as the primaryKey. If the primaryKey does not exist in
  /// the list of secondary keys, it will be automatically added.
  ///
  /// A key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn with_keys(
    primary_key: SecretKey,
    keys: impl Iterator<Item = impl Into<SecretKey>>,
  ) -> Self {
    if keys.size_hint().0 != 0 {
      return Self {
        inner: Arc::new(RwLock::new(SecretKeyringInner {
          primary_key,
          keys: keys
            .filter_map(|k| {
              let k = k.into();
              if k == primary_key {
                None
              } else {
                Some(k)
              }
            })
            .collect(),
        })),
      };
    }

    Self::new(primary_key)
  }

  /// Returns the key on the ring at position 0. This is the key used
  /// for encrypting messages, and is the first key tried for decrypting messages.
  #[inline]
  pub async fn primary_key(&self) -> SecretKey {
    self.inner.read().await.primary_key
  }

  /// Drops a key from the keyring. This will return an error if the key
  /// requested for removal is currently at position 0 (primary key).
  #[inline]
  pub async fn remove(&self, key: &[u8]) -> Result<(), SecurityError> {
    let mut inner = self.inner.write().await;
    if &inner.primary_key == key {
      return Err(SecurityError::RemovePrimaryKey);
    }
    inner.keys.shift_remove(key);
    Ok(())
  }

  /// Install a new key on the ring. Adding a key to the ring will make
  /// it available for use in decryption. If the key already exists on the ring,
  /// this function will just return noop.
  ///
  /// key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub async fn insert(&self, key: SecretKey) {
    self.inner.write().await.keys.insert(key);
  }

  /// Changes the key used to encrypt messages. This is the only key used to
  /// encrypt messages, so peers should know this key before this method is called.
  #[inline]
  pub async fn use_key(&self, key_data: &[u8]) -> Result<(), SecurityError> {
    let mut inner = self.inner.write().await;
    if key_data == inner.primary_key.as_ref() {
      return Ok(());
    }

    // Try to find the key to set as primary
    let Some(&key) = inner.keys.get(key_data) else {
      return Err(SecurityError::SecretKeyNotFound);
    };

    let old_pk = inner.primary_key;
    inner.keys.insert(old_pk);
    inner.primary_key = key;
    inner.keys.swap_remove(key_data);
    Ok(())
  }

  /// Returns the current set of keys on the ring.
  #[inline]
  pub async fn keys(&self) -> impl Iterator<Item = SecretKey> + 'static {
    let inner = self.inner.read().await;

    // we must promise the first key is the primary key
    // so that when decrypt messages, we can try the primary key first
    once(inner.primary_key).chain(inner.keys.clone().into_iter())
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

impl SecretKeyring {}

#[cfg(test)]
mod tests {
  use super::*;

  fn encrypt_decrypt_versioned(vsn: EncryptionAlgo) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    let kr = SecretKeyring::new(k1);
    encrypted.put_u8(vsn as u8);
    encrypted.put_u32(0);
    let nonce = kr.write_header(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    kr.encrypt(vsn, k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = vsn.encrypted_length(plain_text.len()) + 5;
    assert_eq!(encrypted.len(), exp_len);

    encrypted.advance(5);
    kr.read_nonce(&mut encrypted);
    kr.decrypt(k1, vsn, nonce, extra, &mut encrypted).unwrap();
    assert_eq!(encrypted.as_ref(), plain_text);
  }

  async fn decrypt_by_other_key(algo: EncryptionAlgo) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    let kr = SecretKeyring::new(k1);
    encrypted.put_u8(algo as u8);
    encrypted.put_u32(0);
    let nonce = kr.write_header(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    kr.encrypt(algo, k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = algo.encrypted_length(plain_text.len()) + 5;
    assert_eq!(encrypted.len(), exp_len);

    encrypted.advance(5);
    kr.read_nonce(&mut encrypted);

    for (idx, k) in TEST_KEYS.iter().rev().enumerate() {
      if idx == TEST_KEYS.len() - 1 {
        kr.decrypt(*k, algo, nonce, extra, &mut encrypted).unwrap();
        assert_eq!(encrypted.as_ref(), plain_text);
        return;
      }
      let e = kr
        .decrypt(*k, algo, nonce, extra, &mut encrypted)
        .unwrap_err();
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

  #[tokio::test]
  async fn test_primary_only() {
    let keyring = SecretKeyring::new(TEST_KEYS[1]);
    assert_eq!(keyring.keys().await.collect::<Vec<_>>().len(), 1);
  }

  #[tokio::test]
  async fn test_get_primary_key() {
    let keyring = SecretKeyring::with_keys(TEST_KEYS[1], TEST_KEYS.iter().copied());
    assert_eq!(keyring.primary_key().await.as_ref(), TEST_KEYS[1].as_ref());
  }

  #[tokio::test]
  async fn test_insert_remove_use() {
    let keyring = SecretKeyring::new(TEST_KEYS[1]);

    // Use non-existent key throws error
    keyring.use_key(&TEST_KEYS[2]).await.unwrap_err();

    // Add key to ring
    keyring.insert(TEST_KEYS[2]).await;
    assert_eq!(keyring.inner.read().await.keys.len() + 1, 2);
    assert_eq!(keyring.keys().await.next().unwrap(), TEST_KEYS[1]);

    // Use key that exists should succeed
    keyring.use_key(&TEST_KEYS[2]).await.unwrap();
    assert_eq!(keyring.keys().await.next().unwrap(), TEST_KEYS[2]);

    let primary_key = keyring.primary_key().await;
    assert_eq!(primary_key.as_ref(), TEST_KEYS[2].as_ref());

    // Removing primary key should fail
    keyring.remove(&TEST_KEYS[2]).await.unwrap_err();

    // Removing non-primary key should succeed
    keyring.remove(&TEST_KEYS[1]).await.unwrap();
    assert_eq!(keyring.inner.read().await.keys.len() + 1, 1);
  }
}
