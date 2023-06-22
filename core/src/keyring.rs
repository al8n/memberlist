use atomic::{Atomic, Ordering};
use crossbeam_skiplist::SkipSet;

use std::sync::{Arc, atomic::AtomicU64};

/// The key used while attempting to encrypt/decrypt a message
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug)]
struct SecretKeyringInner {
  primary_key: Atomic<SecretKey>,
  update_sequence: AtomicU64,
  keys: SkipSet<SecretKey>,
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct SecretKeyring {
  inner: Arc<SecretKeyringInner>,
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
        inner: Arc::new(SecretKeyringInner {
          primary_key: Atomic::new(primary_key),
          update_sequence: AtomicU64::new(0),
          keys: [primary_key]
          .into_iter()
          .chain(keys.into_iter())
          .collect::<SkipSet<SecretKey>>(),
        })
      };
    }

    Self {
      inner: Arc::new(SecretKeyringInner {
        primary_key: Atomic::new(primary_key),
        update_sequence: AtomicU64::new(0),
        keys: [primary_key].into_iter().collect::<SkipSet<SecretKey>>(),
      })
    }
  }

/// Returns the key on the ring at position 0. This is the key used
  /// for encrypting messages, and is the first key tried for decrypting messages.
  #[inline]
  pub fn primary_key(&self) -> SecretKey {
    self.inner.primary_key.load(Ordering::SeqCst)
  }

  /// Drops a key from the keyring. This will return an error if the key
  /// requested for removal is currently at position 0 (primary key).
  #[inline]
  pub fn remove(&self, key: &[u8]) -> Result<(), SecretKeyringError> {
    if &self.primary_key() == key {
      return Err(SecretKeyringError::RemovePrimaryKey);
    }
    self.inner.keys.remove(key);
    Ok(())
  }

  /// Install a new key on the ring. Adding a key to the ring will make
  /// it available for use in decryption. If the key already exists on the ring,
  /// this function will just return noop.
  ///
  /// key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub fn insert(&self, key: SecretKey) {
    self.inner.keys.insert(key);
  }

  /// Changes the key used to encrypt messages. This is the only key used to
  /// encrypt messages, so peers should know this key before this method is called.
  #[inline]
  pub fn use_key(&self, key_data: &[u8]) -> Result<(), SecretKeyringError> {
    if key_data == self.primary_key().as_ref() {
      return Ok(());
    }

    // Try to find the key to set as primary
    let Some(entry) = self.inner.keys.get(key_data) else {
      return Err(SecretKeyringError::NotFound);
    };
    
    let key = *entry.value();

    // Increment the sequence number for the new update
    let new_sequence = self.inner.update_sequence.fetch_add(1, Ordering::AcqRel) + 1;

    // Try to update the primary key
    let mut seq = self.inner.update_sequence.load(Ordering::Acquire);
    loop {
      if new_sequence <= seq {
          return Ok(());
      }

      match self.inner.update_sequence.compare_exchange_weak(
        seq,
        new_sequence,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        Ok(_) => {
          self.inner.primary_key.store(key, Ordering::Release);
          return Ok(());
        }
        Err(current_seq) => {
          seq = current_seq;
        }
      }
    }
  }


  /// Returns the current set of keys on the ring.
  #[inline]
  pub fn keys(&self) -> crossbeam_skiplist::set::Iter<'_, SecretKey> {
    self.inner.keys.iter()
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.inner.keys.len()
  }

  #[inline]
  pub fn is_empty(&self) -> bool {
    self.inner.keys.is_empty()
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
  async fn test_primary_only() {
    let keyring = SecretKeyring::new(vec![], TEST_KEYS[1]);
    assert_eq!(keyring.keys().collect::<Vec<_>>().len(), 1);
  }

  #[cfg(feature = "async")]
  #[tokio::test]
  async fn test_get_primary_key() {
    let keyring = SecretKeyring::new(TEST_KEYS.to_vec(), TEST_KEYS[1]);
    assert_eq!(keyring.primary_key().as_ref(), TEST_KEYS[1].as_ref());
  }

  #[cfg(feature = "async")]
  #[tokio::test]
  async fn test_insert_remove_use() {
    let keyring = SecretKeyring::new(vec![], TEST_KEYS[1]);

    // Use non-existent key throws error
    keyring.use_key(&TEST_KEYS[2]).unwrap_err();

    // Add key to ring
    keyring.insert(TEST_KEYS[2]);
    assert_eq!(keyring.len(), 2);
    assert_eq!(keyring.keys().peekable().next().unwrap().as_ref(), TEST_KEYS[1].as_ref());

    // Use key that exists should succeed
    keyring.use_key(&TEST_KEYS[2]).unwrap();

    let primary_key = keyring.primary_key();
    assert_eq!(primary_key.as_ref(), TEST_KEYS[2].as_ref());

    // Removing primary key should fail
    keyring.remove(&TEST_KEYS[2]).unwrap_err();

    // Removing non-primary key should succeed
    keyring.remove(&TEST_KEYS[1]).unwrap();
    assert_eq!(keyring.len(), 1);
  }
}
