use std::iter::once;

use indexmap::IndexSet;
use memberlist_proto::SecretKey;
use parking_lot::RwLock;
use triomphe::Arc;

/// Error for [`Keyring`]
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum KeyringError {
  /// Secret key is not in the keyring
  #[error("secret key is not in the keyring")]
  SecretKeyNotFound,
  /// Removing the primary key is not allowed
  #[error("removing the primary key is not allowed")]
  RemovePrimaryKey,
}

#[derive(Debug)]
pub(super) struct KeyringInner {
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
pub struct Keyring {
  pub(super) inner: Arc<RwLock<KeyringInner>>,
}

impl Keyring {
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
      inner: Arc::new(RwLock::new(KeyringInner {
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
        inner: Arc::new(RwLock::new(KeyringInner {
          primary_key,
          keys: keys
            .filter_map(|k| {
              let k = k.into();
              if k == primary_key { None } else { Some(k) }
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
  pub fn primary_key(&self) -> SecretKey {
    self.inner.read().primary_key
  }

  /// Drops a key from the keyring. This will return an error if the key
  /// requested for removal is currently at position 0 (primary key).
  #[inline]
  pub fn remove(&self, key: &[u8]) -> Result<(), KeyringError> {
    let mut inner = self.inner.write();
    if &inner.primary_key == key {
      return Err(KeyringError::RemovePrimaryKey);
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
  pub fn insert(&self, key: SecretKey) {
    self.inner.write().keys.insert(key);
  }

  /// Changes the key used to encrypt messages. This is the only key used to
  /// encrypt messages, so peers should know this key before this method is called.
  #[inline]
  pub fn use_key(&self, key_data: &[u8]) -> Result<(), KeyringError> {
    let mut inner = self.inner.write();
    if key_data == inner.primary_key.as_ref() {
      return Ok(());
    }

    // Try to find the key to set as primary
    let Some(&key) = inner.keys.get(key_data) else {
      return Err(KeyringError::SecretKeyNotFound);
    };

    let old_pk = inner.primary_key;
    inner.keys.insert(old_pk);
    inner.primary_key = key;
    inner.keys.swap_remove(key_data);
    Ok(())
  }

  /// Returns the current set of keys on the ring.
  #[inline]
  pub fn keys(&self) -> impl Iterator<Item = SecretKey> + Send + 'static {
    let inner = self.inner.read();

    // we must promise the first key is the primary key
    // so that when decrypt messages, we can try the primary key first
    once(inner.primary_key).chain(inner.keys.clone())
  }
}

#[cfg(feature = "serde")]
const _: () = {
  use memberlist_proto::SecretKeys;
  use serde::{Deserialize, Serialize};

  impl Serialize for Keyring {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: serde::Serializer,
    {
      let keys = self.keys();
      serializer.collect_seq(keys)
    }
  }

  impl<'de> Deserialize<'de> for Keyring {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: serde::Deserializer<'de>,
    {
      let keys = SecretKeys::deserialize(deserializer)?;
      if keys.is_empty() {
        return Err(serde::de::Error::custom(
          "keyring must have at least one key",
        ));
      }

      Ok(Keyring::with_keys(keys[0], keys.into_iter().skip(1)))
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  const TEST_KEYS: &[SecretKey] = &[
    SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
  ];

  #[test]
  fn test_primary_only() {
    let keyring = Keyring::new(TEST_KEYS[1]);
    assert_eq!(keyring.keys().collect::<Vec<_>>().len(), 1);
  }

  #[test]
  fn test_get_primary_key() {
    let keyring = Keyring::with_keys(TEST_KEYS[1], TEST_KEYS.iter().copied());
    assert_eq!(keyring.primary_key().as_ref(), TEST_KEYS[1].as_ref());
  }

  #[test]
  fn test_insert_remove_use() {
    let keyring = Keyring::new(TEST_KEYS[1]);

    // Use non-existent key throws error
    keyring.use_key(&TEST_KEYS[2]).unwrap_err();

    // Add key to ring
    keyring.insert(TEST_KEYS[2]);
    assert_eq!(keyring.inner.read().keys.len() + 1, 2);
    assert_eq!(keyring.keys().next().unwrap(), TEST_KEYS[1]);

    // Use key that exists should succeed
    keyring.use_key(&TEST_KEYS[2]).unwrap();
    assert_eq!(keyring.keys().next().unwrap(), TEST_KEYS[2]);

    let primary_key = keyring.primary_key();
    assert_eq!(primary_key.as_ref(), TEST_KEYS[2].as_ref());

    // Removing primary key should fail
    keyring.remove(&TEST_KEYS[2]).unwrap_err();

    // Removing non-primary key should succeed
    keyring.remove(&TEST_KEYS[1]).unwrap();
    assert_eq!(keyring.inner.read().keys.len() + 1, 1);
  }
}
