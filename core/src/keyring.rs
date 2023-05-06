use bytes::Bytes;
use indexmap::IndexSet;

#[cfg(feature = "async")]
use async_lock::{Mutex, MutexGuard};

#[cfg(not(feature = "async"))]
use parking_lot::{Mutex, MutexGuard};

use std::sync::Arc;

#[derive(Debug, Default)]
pub struct SecretKeys(IndexSet<Bytes>);

impl SecretKeys {
  /// Returns the key on the ring at position 0. This is the key used
  /// for encrypting messages, and is the first key tried for decrypting messages.
  #[inline]
  pub fn primary_key(&self) -> Option<&Bytes> {
    self.0.get_index(0)
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
  pub fn insert(&mut self, key: Bytes) -> Result<(), SecretKeyringError> {
    SecretKeyring::validate_key(&key).map(|_| {
      self.0.insert(key);
    })
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
  pub fn get(&self) -> impl Iterator<Item = &Bytes> {
    self.0.iter()
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
  pub fn new(keys: Vec<Bytes>, primary_key: Bytes) -> Result<Self, SecretKeyringError> {
    if !keys.is_empty() || !primary_key.is_empty() {
      if primary_key.is_empty() {
        return Err(SecretKeyringError::EmptyPrimaryKey);
      }

      return [primary_key]
        .into_iter()
        .chain(keys.into_iter())
        .map(|k| SecretKeyring::validate_key(&k).map(|_| k))
        .collect::<Result<IndexSet<Bytes>, _>>()
        .map(|keys| Self {
          keys: Arc::new(Mutex::new(SecretKeys(keys))),
        });
    }

    Ok(Self {
      keys: Arc::new(Mutex::new(SecretKeys(IndexSet::new()))),
    })
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

  /// Check to see if the key is valid and returns an error if not.
  ///
  /// key should be either 16, 24, or 32 bytes to select AES-128,
  /// AES-192, or AES-256.
  #[inline]
  pub const fn validate_key(key: &[u8]) -> Result<(), SecretKeyringError> {
    match key.len() {
      16 | 24 | 32 => Ok(()),
      _ => Err(SecretKeyringError::InvalidKeySize),
    }
  }
}
