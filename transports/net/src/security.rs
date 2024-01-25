use aead::{generic_array::GenericArray, AeadInPlace, KeyInit};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use async_lock::RwLock;
use base64::Engine;
use bytes::{Buf, BufMut, BytesMut};
use indexmap::IndexSet;
use rand::Rng;
use serde::{Deserialize, Serialize};
use showbiz_utils::smallvec_wrapper;

use std::{iter::once, sync::Arc};

use crate::ENCRYPT_TAG;

type Aes192Gcm = AesGcm<Aes192, U12>;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SecurityError {
  #[error("security: unknown encryption version: {0}")]
  UnknownEncryptionAlgo(#[from] UnknownEncryptionAlgo),
  #[error("security: {0}")]
  AeadError(#[from] aead::Error),
  #[error("security: security related feature is disabled")]
  Disabled,
  #[error("security: cannot decode empty payload")]
  EmptyPayload,
  #[error("security: payload is too small to decrypt")]
  SmallPayload,
  #[error("security: no installed keys could decrypt the message")]
  NoInstalledKeys,
  #[error("security: no primary key installed")]
  MissingPrimaryKey,
  #[error("security: remote state is encrypted and encryption is not configured")]
  NotConfigured,
  #[error("security: encryption is configured but remote state is not encrypted")]
  PlainRemoteState,
  #[error("security: secret key is not in the keyring")]
  SecretKeyNotFound,
  #[error("security: removing the primary key is not allowed")]
  RemovePrimaryKey,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct UnknownEncryptionAlgo(u8);

impl core::fmt::Display for UnknownEncryptionAlgo {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "unknown encryption type {}", self.0)
  }
}

impl std::error::Error for UnknownEncryptionAlgo {}

#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(u8)]
#[non_exhaustive]
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
  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();

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

/// The key used while attempting to encrypt/decrypt a message
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecretKey {
  /// secret key for AES128
  Aes128([u8; 16]),
  /// secret key for AES192
  Aes192([u8; 24]),
  /// secret key for AES256
  Aes256([u8; 32]),
}

#[cfg(feature = "serde")]
impl Serialize for SecretKey {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    if serializer.is_human_readable() {
      base64::engine::general_purpose::STANDARD
        .encode(self)
        .serialize(serializer)
    } else {
      serializer.serialize_bytes(self)
    }
  }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for SecretKey {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    macro_rules! parse {
      ($key:ident) => {{
        match $key.len() {
          16 => Ok(Self::Aes128($key.try_into().unwrap())),
          24 => Ok(Self::Aes192($key.try_into().unwrap())),
          32 => Ok(Self::Aes256($key.try_into().unwrap())),
          _ => Err(<D::Error as serde::de::Error>::custom(
            "invalid secret key length",
          )),
        }
      }};
    }

    if deserializer.is_human_readable() {
      <String as Deserialize<'de>>::deserialize(deserializer).and_then(|val| {
        base64::engine::general_purpose::STANDARD
          .decode(val)
          .map_err(serde::de::Error::custom)
          .and_then(|key| parse!(key))
      })
    } else {
      <Vec<u8> as Deserialize<'de>>::deserialize(deserializer).and_then(|val| parse!(val))
    }
  }
}

impl core::hash::Hash for SecretKey {
  fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state);
  }
}

impl core::borrow::Borrow<[u8]> for SecretKey {
  fn borrow(&self) -> &[u8] {
    self.as_ref()
  }
}

impl PartialEq<[u8]> for SecretKey {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref() == other
  }
}

impl core::ops::Deref for SecretKey {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl core::ops::DerefMut for SecretKey {
  fn deref_mut(&mut self) -> &mut Self::Target {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl SecretKey {
  /// Returns the size of the key in bytes
  #[inline]
  pub const fn len(&self) -> usize {
    match self {
      Self::Aes128(_) => 16,
      Self::Aes192(_) => 24,
      Self::Aes256(_) => 32,
    }
  }

  /// Returns true if the key is empty
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.len() == 0
  }

  #[inline]
  pub fn to_vec(&self) -> Vec<u8> {
    self.as_ref().to_vec()
  }
}

impl From<[u8; 16]> for SecretKey {
  fn from(k: [u8; 16]) -> Self {
    Self::Aes128(k)
  }
}

impl From<[u8; 24]> for SecretKey {
  fn from(k: [u8; 24]) -> Self {
    Self::Aes192(k)
  }
}

impl From<[u8; 32]> for SecretKey {
  fn from(k: [u8; 32]) -> Self {
    Self::Aes256(k)
  }
}

impl TryFrom<&[u8]> for SecretKey {
  type Error = String;

  fn try_from(k: &[u8]) -> Result<Self, Self::Error> {
    match k.len() {
      16 => Ok(Self::Aes128(k.try_into().unwrap())),
      24 => Ok(Self::Aes192(k.try_into().unwrap())),
      32 => Ok(Self::Aes256(k.try_into().unwrap())),
      x => Err(format!(
        "invalid key size: {}, secret key size must be 16, 24 or 32 bytes",
        x
      )),
    }
  }
}

impl AsRef<[u8]> for SecretKey {
  fn as_ref(&self) -> &[u8] {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

impl AsMut<[u8]> for SecretKey {
  fn as_mut(&mut self) -> &mut [u8] {
    match self {
      Self::Aes128(k) => k,
      Self::Aes192(k) => k,
      Self::Aes256(k) => k,
    }
  }
}

smallvec_wrapper!(
  /// A collection of secret keys, you can just treat it as a `Vec<SecretKey>`.
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
  #[repr(transparent)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  pub SecretKeys([SecretKey; 3]);
);

#[derive(Clone)]
pub(super) struct Encryptor {
  pub(super) algo: EncryptionAlgo,
  pub(super) kr: SecretKeyring,
}

impl core::ops::Deref for Encryptor {
  type Target = EncryptionAlgo;

  fn deref(&self) -> &Self::Target {
    &self.algo
  }
}

impl Encryptor {
  pub(super) fn new(alg: EncryptionAlgo, kr: SecretKeyring) -> Self {
    Self { algo: alg, kr }
  }

  pub(super) fn write_header(&self, dst: &mut BytesMut) -> [u8; NONCE_SIZE] {
    let offset = dst.len();

    // Add a random nonce
    let mut nonce = [0u8; NONCE_SIZE];
    rand::thread_rng().fill(&mut nonce);
    dst.put_slice(&nonce);

    // Ensure we are correctly padded (now, only for PKCS7)
    match self.algo {
      EncryptionAlgo::PKCS7 => {
        let buf_len = dst.len();
        pkcs7encode(
          dst,
          buf_len,
          offset + EncryptionAlgo::SIZE + NONCE_SIZE,
          BLOCK_SIZE,
        );
        nonce
      }
      EncryptionAlgo::NoPadding => nonce,
    }
  }

  pub(super) fn read_nonce(&self, src: &mut BytesMut) -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    nonce.copy_from_slice(&src[..NONCE_SIZE]);
    src.advance(NONCE_SIZE);
    nonce
  }

  pub(super) fn encrypt(
    &self,
    pk: SecretKey,
    nonce: [u8; NONCE_SIZE],
    auth_data: &[u8],
    dst: &mut BytesMut,
  ) -> Result<(), SecurityError> {
    match pk {
      SecretKey::Aes128(pk) => {
        let gcm = Aes128Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
          .map_err(SecurityError::AeadError)
      }
      SecretKey::Aes192(pk) => {
        let gcm = Aes192Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
          .map_err(SecurityError::AeadError)
      }
      SecretKey::Aes256(pk) => {
        let gcm = Aes256Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
          .map_err(SecurityError::AeadError)
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
          .map_err(SecurityError::AeadError)
      }
      SecretKey::Aes192(pk) => {
        let gcm = Aes192Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .decrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
          .map_err(SecurityError::AeadError)
      }
      SecretKey::Aes256(pk) => {
        let gcm = Aes256Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .decrypt_in_place(GenericArray::from_slice(&nonce), auth_data, dst)
          .map_err(SecurityError::AeadError)
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
/// The keyring contains all key data used internally by showbiz.
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
  /// keyring contains all key data used internally by showbiz.
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
        keys: once(primary_key).collect(),
      })),
    }
  }

  /// Constructs a new container for a set of encryption keys. The
  /// keyring contains all key data used internally by showbiz.
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
    inner.keys.remove(key);
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

    inner.primary_key = key;
    inner.keys.remove(key_data);
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

// #[cfg(test)]
// mod tests {
//   use super::*;

//   fn encrypt_decrypt_versioned(vsn: EncryptionAlgo) {
//     let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
//     let plain_text = b"this is a plain text message";
//     let extra = b"random data";
//     let mut encrypted = BytesMut::new();
//     encrypt_payload(&k1, vsn, plain_text, extra, &mut encrypted).unwrap();

//     let exp_len = vsn.encrypted_length(plain_text.len());
//     assert_eq!(encrypted.len(), exp_len);

//     decrypt_payload([k1].into_iter(), &mut encrypted, extra, vsn).unwrap();
//     assert_eq!(&encrypted[EncryptionAlgo::SIZE + NONCE_SIZE..], plain_text);
//   }

//   #[test]
//   fn test_encrypt_decrypt_v0() {
//     encrypt_decrypt_versioned(EncryptionAlgo::PKCS7);
//   }

//   #[test]
//   fn test_encrypt_decrypt_v1() {
//     encrypt_decrypt_versioned(EncryptionAlgo::NoPadding);
//   }

//   const TEST_KEYS: &[SecretKey] = &[
//     SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
//     SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
//     SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
//   ];

//   #[test]
//   fn test_primary_only() {
//     let keyring = SecretKeyring::new(TEST_KEYS[1]);
//     assert_eq!(keyring.keys().collect::<Vec<_>>().len(), 1);
//   }

//   #[test]
//   fn test_get_primary_key() {
//     let keyring = SecretKeyring::with_keys(TEST_KEYS[1], TEST_KEYS.to_vec());
//     assert_eq!(keyring.primary_key().as_ref(), TEST_KEYS[1].as_ref());
//   }

//   #[test]
//   fn test_insert_remove_use() {
//     let keyring = SecretKeyring::new(TEST_KEYS[1]);

//     // Use non-existent key throws error
//     keyring.use_key(&TEST_KEYS[2]).unwrap_err();

//     // Add key to ring
//     keyring.insert(TEST_KEYS[2]);
//     assert_eq!(keyring.len(), 2);
//     assert_eq!(keyring.keys().next().unwrap(), TEST_KEYS[1]);

//     // Use key that exists should succeed
//     keyring.use_key(&TEST_KEYS[2]).unwrap();
//     assert_eq!(keyring.keys().next().unwrap(), TEST_KEYS[2]);

//     let primary_key = keyring.primary_key();
//     assert_eq!(primary_key.as_ref(), TEST_KEYS[2].as_ref());

//     // Removing primary key should fail
//     keyring.remove(&TEST_KEYS[2]).unwrap_err();

//     // Removing non-primary key should succeed
//     keyring.remove(&TEST_KEYS[1]).unwrap();
//     assert_eq!(keyring.len(), 1);
//   }
// }
