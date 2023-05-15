use indexmap::IndexSet;

#[cfg(feature = "async")]
use async_lock::{Mutex, MutexGuard};

#[cfg(not(feature = "async"))]
use parking_lot::{Mutex, MutexGuard};

use std::sync::Arc;

/// The key used while attempting to encrypt/decrypt a message
#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum SecretKey {
  /// secret key for AES128
  Aes128([u8; 16]),
  /// secret key for AES192
  Aes192([u8; 24]),
  /// secret key for AES256
  Aes256([u8; 32]),
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

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct SecretKeys(IndexSet<SecretKey>);

impl SecretKeys {
  /// Returns the key on the ring at position 0. This is the key used
  /// for encrypting messages, and is the first key tried for decrypting messages.
  #[inline]
  pub fn primary_key(&self) -> Option<SecretKey> {
    self.0.get_index(0).copied()
  }

  /// Drops a key from the keyring. This will return an error if the key
  /// requested for removal is currently at position 0 (primary key).
  #[inline]
  pub fn remove(&mut self, key: &[u8]) -> Result<(), SecretKeyringError> {
    if let Some(k) = self.0.get_index(0) {
      if k == key {
        return Err(SecretKeyringError::RemovePrimaryKey);
      }
    }
    self.0.remove(key);
    Ok(())
  }

  /// Install a new key on the ring. Adding a key to the ring will make
  /// it available for use in decryption. If the key already exists on the ring,
  /// this function will just return noop.
  ///
  /// key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn insert(&mut self, key: SecretKey) {
    self.0.insert(key);
  }

  /// Changes the key used to encrypt messages. This is the only key used to
  /// encrypt messages, so peers should know this key before this method is called.
  #[inline]
  pub fn use_key(&mut self, key: &[u8]) -> Result<(), SecretKeyringError> {
    if let Some((idx, _)) = self.0.get_full(key) {
      self.0.move_index(idx, 0);
      return Ok(());
    }
    Err(SecretKeyringError::NotFound)
  }

  /// Returns the current set of keys on the ring.
  #[inline]
  pub fn keys(&self) -> Box<[SecretKey]> {
    self.0.iter().copied().collect::<Box<[_]>>()
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretKeyringError {
  #[error("secret key size must be 16, 24 or 32 bytes")]
  InvalidKeySize,
  #[error("secret key is not in the keyring")]
  NotFound,
  #[error("removing the primary key is not allowed")]
  RemovePrimaryKey,
  #[error("empty primary key is not allowed")]
  EmptyPrimaryKey,
}

#[derive(Debug, Default, Clone)]
#[repr(transparent)]
pub struct SecretKeyring {
  keys: Arc<Mutex<SecretKeys>>,
}

impl SecretKeyring {
  /// Constructs a new container for a set of encryption keys. The
  /// keyring contains all key data used internally by memberlist.
  ///
  /// While creating a new keyring, you must do one of:
  ///   - Omit keys and primary key, effectively disabling encryption
  ///   - Pass a set of keys plus the primary key
  ///   - Pass only a primary key
  ///
  /// If only a primary key is passed, then it will be automatically added to the
  /// keyring. If creating a keyring with multiple keys, one key must be designated
  /// primary by passing it as the primaryKey. If the primaryKey does not exist in
  /// the list of secondary keys, it will be automatically added at position 0.
  ///
  /// A key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn new(keys: Vec<SecretKey>, primary_key: SecretKey) -> Self {
    if !keys.is_empty() || !primary_key.is_empty() {
      return Self {
        keys: Arc::new(Mutex::new(SecretKeys(
          [primary_key]
            .into_iter()
            .chain(keys.into_iter())
            .collect::<IndexSet<SecretKey>>(),
        ))),
      };
    }

    Self {
      keys: Arc::new(Mutex::new(SecretKeys(IndexSet::new()))),
    }
  }

  #[cfg(feature = "async")]
  #[inline]
  pub async fn lock(&self) -> MutexGuard<'_, SecretKeys> {
    self.keys.lock().await
  }

  #[cfg(not(feature = "async"))]
  #[inline]
  pub fn lock(&self) -> MutexGuard<'_, SecretKeys> {
    self.keys.lock()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const TEST_KEYS: &[SecretKey] = &[
    SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
  ];

  #[cfg(feature = "async")]
  #[tokio::test]
  async fn test_empty_keyring() {
    let ring = SecretKeyring::default();
    let keys = ring.lock().await;
    assert_eq!(keys.keys().len(), 0);
  }

  #[cfg(not(feature = "async"))]
  #[test]
  fn test_empty_keyring() {
    let ring = SecretKeyring::default();
    let keys = ring.lock();
    assert_eq!(keys.keys().len(), 0);
  }

  #[cfg(feature = "async")]
  #[tokio::test]
  async fn test_primary_only() {
    let keying = SecretKeyring::new(vec![], TEST_KEYS[1]);

    let keys = keying.lock().await;
    assert_eq!(keys.keys().len(), 1);
  }

  #[cfg(not(feature = "async"))]
  #[test]
  fn test_get_primary_key() {
    let keying = SecretKeyring::new(TEST_KEYS.to_vec(), TEST_KEYS[1]);

    let keys = keying.lock();
    assert_eq!(keys.keys().len(), 1);
  }

  #[cfg(feature = "async")]
  #[tokio::test]
  async fn test_get_primary_key() {
    let keying = SecretKeyring::new(TEST_KEYS.to_vec(), TEST_KEYS[1]);

    let keys = keying.lock().await;
    assert_eq!(keys.primary_key().unwrap().as_ref(), TEST_KEYS[1].as_ref());
  }

  #[cfg(not(feature = "async"))]
  #[test]
  fn test_get_primary_key() {
    let keying = SecretKeyring::new(TEST_KEYS.to_vec(), TEST_KEYS[1]);

    let keys = keying.lock();
    assert_eq!(keys.primary_key().unwrap().as_ref(), TEST_KEYS[1].as_ref());
  }

  #[cfg(feature = "async")]
  #[tokio::test]
  async fn test_insert_remove_use() {
    let keying = SecretKeyring::new(vec![], TEST_KEYS[1]);

    // Use non-existent key throws error
    let mut keys = keying.lock().await;
    keys.use_key(&TEST_KEYS[2]).unwrap_err();

    // Add key to ring
    keys.insert(TEST_KEYS[2]);
    assert_eq!(keys.keys().len(), 2);
    assert_eq!(keys.keys()[0], TEST_KEYS[1]);

    // Use key that exists should succeed
    keys.use_key(&TEST_KEYS[2]).unwrap();

    let primary_key = keys.primary_key().unwrap();
    assert_eq!(primary_key.as_ref(), TEST_KEYS[2].as_ref());

    // Removing primary key should fail
    keys.remove(&TEST_KEYS[2]).unwrap_err();

    // Removing non-primary key should succeed
    keys.remove(&TEST_KEYS[1]).unwrap();
    assert_eq!(keys.keys().len(), 1);
  }

  #[cfg(not(feature = "async"))]
  #[test]
  fn test_insert_remove_use() {
    let keying = SecretKeyring::new(vec![], TEST_KEYS[1]);

    // Use non-existent key throws error
    let mut keys = keying.lock();
    keys.use_key(&TEST_KEYS[2]).unwrap_err();

    // Add key to ring
    keys.insert(TEST_KEYS[2]);
    assert_eq!(keys.keys().len(), 2);
    assert_eq!(keys.keys().next().unwrap().as_ref(), TEST_KEYS[1].as_ref());

    // Use key that exists should succeed
    keys.use_key(&TEST_KEYS[2]).unwrap();

    let primary_key = keys.primary_key().unwrap();
    assert_eq!(primary_key.as_ref(), TEST_KEYS[2].as_ref());

    // Removing primary key should fail
    keys.remove(&TEST_KEYS[2]).unwrap_err();

    // Removing non-primary key should succeed
    keys.remove(&TEST_KEYS[1]).unwrap();
    assert_eq!(keys.keys().size_hint().0, 1);
  }
}
