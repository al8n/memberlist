use std::{iter::once, sync::Arc};

use async_lock::RwLock;
use byteorder::{ByteOrder, NetworkEndian};
use indexmap::IndexSet;
use transformable::Transformable;

/// Unknown secret key kind error
#[derive(Debug, thiserror::Error)]
#[error("unknown secret key kind: {0}")]
pub struct UnknownSecretKeyKind(u8);

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

/// Error occurred while transforming the [`SecretKey`].
#[derive(Debug, thiserror::Error)]
pub enum SecretKeyTransformError {
  /// Returned when the buffer is too small to encode the key
  #[error("encode buffer is too small")]
  BufferTooSmall,
  /// Returned when the buffer is too small to decode the key
  #[error("not enough bytes to decode")]
  NotEnoughBytes,

  /// Returned when the key is not a valid length
  #[error("invalid key length")]
  UnknownSecretKeyKind(#[from] UnknownSecretKeyKind),
}

impl Transformable for SecretKey {
  type Error = SecretKeyTransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    let len = match self {
      Self::Aes128(_) => 16,
      Self::Aes192(_) => 24,
      Self::Aes256(_) => 32,
    };
    dst[0] = len as u8;

    match self {
      Self::Aes128(k) => dst[1..17].copy_from_slice(k),
      Self::Aes192(k) => dst[1..25].copy_from_slice(k),
      Self::Aes256(k) => dst[1..33].copy_from_slice(k),
    }

    Ok(len + 1)
  }

  fn encoded_len(&self) -> usize {
    self.len() + 1
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
    match self {
      Self::Aes128(k) => {
        let mut buf = [0; 17];
        buf[0] = 16;
        buf[1..17].copy_from_slice(k);
        writer.write_all(&buf).map(|_| 17)
      }
      Self::Aes192(k) => {
        let mut buf = [0; 25];
        buf[0] = 24;
        buf[1..25].copy_from_slice(k);
        writer.write_all(&buf).map(|_| 25)
      }
      Self::Aes256(k) => {
        let mut buf = [0; 33];
        buf[0] = 32;
        buf[1..33].copy_from_slice(k);
        writer.write_all(&buf).map(|_| 33)
      }
    }
  }

  async fn encode_to_async_writer<W: futures::io::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<usize> {
    use futures::io::AsyncWriteExt;

    match self {
      Self::Aes128(k) => {
        let mut buf = [0; 17];
        buf[0] = 16;
        buf[1..17].copy_from_slice(k);
        writer.write_all(&buf).await.map(|_| 17)
      }
      Self::Aes192(k) => {
        let mut buf = [0; 25];
        buf[0] = 24;
        buf[1..25].copy_from_slice(k);
        writer.write_all(&buf).await.map(|_| 25)
      }
      Self::Aes256(k) => {
        let mut buf = [0; 33];
        buf[0] = 32;
        buf[1..33].copy_from_slice(k);
        writer.write_all(&buf).await.map(|_| 33)
      }
    }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < 17 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let len = src[0];
    let key = match len {
      16 => Self::Aes128(src[1..17].try_into().unwrap()),
      24 => Self::Aes192(src[1..25].try_into().unwrap()),
      32 => Self::Aes256(src[1..33].try_into().unwrap()),
      x => return Err(Self::Error::UnknownSecretKeyKind(UnknownSecretKeyKind(x))),
    };

    Ok((len as usize + 1, key))
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut buf = [0; 17];
    reader.read_exact(&mut buf)?;
    let len = buf[0] as usize;
    match len {
      16 => Ok((17, Self::Aes128(buf[1..17].try_into().unwrap()))),
      24 => {
        let mut key = [0; 24];
        key[..16].copy_from_slice(&buf[1..]);
        reader.read_exact(&mut key[16..])?;
        Ok((25, Self::Aes192(key)))
      }
      32 => {
        let mut key = [0; 32];
        key[..16].copy_from_slice(&buf[1..]);
        reader.read_exact(&mut key[16..])?;
        Ok((33, Self::Aes256(key)))
      }
      x => Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        Self::Error::UnknownSecretKeyKind(UnknownSecretKeyKind(x as u8)),
      )),
    }
  }

  async fn decode_from_async_reader<R: futures::io::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    use futures::io::AsyncReadExt;

    let mut buf = [0; 17];
    reader.read_exact(&mut buf).await?;
    let len = buf[0] as usize;
    match len {
      16 => Ok((17, Self::Aes128(buf[1..17].try_into().unwrap()))),
      24 => {
        let mut key = [0; 24];
        key[..16].copy_from_slice(&buf[1..]);
        reader.read_exact(&mut key[16..]).await?;
        Ok((25, Self::Aes192(key)))
      }
      32 => {
        let mut key = [0; 32];
        key[..16].copy_from_slice(&buf[1..]);
        reader.read_exact(&mut key[16..]).await?;
        Ok((33, Self::Aes256(key)))
      }
      x => Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        Self::Error::UnknownSecretKeyKind(UnknownSecretKeyKind(x as u8)),
      )),
    }
  }
}

#[cfg(feature = "serde")]
const _: () = {
  use base64::Engine;
  use serde::{Deserialize, Serialize};

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
};

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

smallvec_wrapper::smallvec_wrapper!(
  /// A collection of secret keys, you can just treat it as a `Vec<SecretKey>`.
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
  #[repr(transparent)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  pub SecretKeys([SecretKey; 3]);
);

/// Error occurred while transforming the [`SecretKeys`].
#[derive(Debug, thiserror::Error)]
pub enum SecretKeysTransformError {
  /// Returned when the buffer is too small to encode the keys
  #[error("encode buffer is too small")]
  BufferTooSmall,
  /// Returned when the buffer is too small to decode the keys
  #[error("not enough bytes to decode")]
  NotEnoughBytes,

  /// Returned when transforming the secret key
  #[error(transparent)]
  SecretKey(#[from] SecretKeyTransformError),

  /// Returned when missing keys
  #[error("expect {expected} keys, but actual decode {actual} keys")]
  MissingKeys {
    /// Expected number of keys
    expected: usize,
    /// Actual number of keys
    actual: usize,
  },
}

impl Transformable for SecretKeys {
  type Error = SecretKeysTransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += 4;

    let num_keys = self.len();
    NetworkEndian::write_u32(&mut dst[offset..], num_keys as u32);
    offset += 4;

    for key in self.iter() {
      let len = key.encode(&mut dst[offset..])?;
      offset += len;
    }

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, but actual write {} bytes",
      encoded_len, offset
    );

    Ok(encoded_len)
  }

  fn encoded_len(&self) -> usize {
    4 + 4 + self.iter().map(SecretKey::encoded_len).sum::<usize>()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < 4 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let len = NetworkEndian::read_u32(&src[0..4]) as usize;
    if src.len() < len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 4;
    let keys_len = NetworkEndian::read_u32(&src[offset..]) as usize;
    offset += 4;

    let mut keys = SecretKeys::with_capacity(keys_len);
    for _ in 0..keys_len {
      let (len, key) = SecretKey::decode(&src[offset..])?;
      offset += len;
      keys.push(key);
    }

    debug_assert_eq!(
      offset,
      src.len(),
      "expect read {} bytes, but actual read {} bytes",
      src.len(),
      offset
    );

    Ok((offset, keys))
  }
}

/// Error for [`SecretKeyring`]
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum SecretKeyringError {
  /// Secret key is not in the keyring
  #[error("secret key is not in the keyring")]
  SecretKeyNotFound,
  /// Removing the primary key is not allowed
  #[error("removing the primary key is not allowed")]
  RemovePrimaryKey,
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
  pub async fn remove(&self, key: &[u8]) -> Result<(), SecretKeyringError> {
    let mut inner = self.inner.write().await;
    if &inner.primary_key == key {
      return Err(SecretKeyringError::RemovePrimaryKey);
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
  pub async fn use_key(&self, key_data: &[u8]) -> Result<(), SecretKeyringError> {
    let mut inner = self.inner.write().await;
    if key_data == inner.primary_key.as_ref() {
      return Ok(());
    }

    // Try to find the key to set as primary
    let Some(&key) = inner.keys.get(key_data) else {
      return Err(SecretKeyringError::SecretKeyNotFound);
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

#[cfg(test)]
mod tests {
  use std::ops::{Deref, DerefMut};

  use super::*;

  impl SecretKey {
    fn random(kind: u8) -> Self {
      match kind {
        16 => Self::Aes128(rand::random()),
        24 => Self::Aes192(rand::random()),
        32 => Self::Aes256(rand::random()),
        x => panic!("invalid key kind: {}", x),
      }
    }
  }

  impl SecretKeys {
    fn random(num_keys: usize) -> Self {
      let mut keys = SecretKeys::new();
      for i in 0..num_keys {
        let kind = match i % 3 {
          0 => 16,
          1 => 24,
          2 => 32,
          _ => unreachable!(),
        };
        keys.push(SecretKey::random(kind));
      }
      keys
    }
  }

  #[test]
  fn test_secret_key() {
    let mut key = SecretKey::from([0; 16]);
    assert_eq!(key.deref(), &[0; 16]);
    assert_eq!(key.deref_mut(), &mut [0; 16]);
    assert_eq!(key.as_ref(), &[0; 16]);
    assert_eq!(key.as_mut(), &mut [0; 16]);
    assert_eq!(key.len(), 16);
    assert!(!key.is_empty());
    assert_eq!(key.to_vec(), vec![0; 16]);

    let mut key = SecretKey::from([0; 24]);
    assert_eq!(key.deref(), &[0; 24]);
    assert_eq!(key.deref_mut(), &mut [0; 24]);
    assert_eq!(key.as_ref(), &[0; 24]);
    assert_eq!(key.as_mut(), &mut [0; 24]);
    assert_eq!(key.len(), 24);
    assert!(!key.is_empty());
    assert_eq!(key.to_vec(), vec![0; 24]);

    let mut key = SecretKey::from([0; 32]);
    assert_eq!(key.deref(), &[0; 32]);
    assert_eq!(key.deref_mut(), &mut [0; 32]);
    assert_eq!(key.as_ref(), &[0; 32]);
    assert_eq!(key.as_mut(), &mut [0; 32]);
    assert_eq!(key.len(), 32);
    assert!(!key.is_empty());
    assert_eq!(key.to_vec(), vec![0; 32]);

    let mut key = SecretKey::from([0; 16]);
    assert_eq!(key.as_ref(), &[0; 16]);
    assert_eq!(key.as_mut(), &mut [0; 16]);

    let mut key = SecretKey::from([0; 24]);
    assert_eq!(key.as_ref(), &[0; 24]);
    assert_eq!(key.as_mut(), &mut [0; 24]);

    let mut key = SecretKey::from([0; 32]);
    assert_eq!(key.as_ref(), &[0; 32]);
    assert_eq!(key.as_mut(), &mut [0; 32]);

    let key = SecretKey::Aes128([0; 16]);
    assert_eq!(key.to_vec(), vec![0; 16]);

    let key = SecretKey::Aes192([0; 24]);
    assert_eq!(key.to_vec(), vec![0; 24]);

    let key = SecretKey::Aes256([0; 32]);
    assert_eq!(key.to_vec(), vec![0; 32]);
  }

  #[test]
  fn test_try_from() {
    assert!(SecretKey::try_from([0; 15].as_slice()).is_err());
    assert!(SecretKey::try_from([0; 16].as_slice()).is_ok());
    assert!(SecretKey::try_from([0; 23].as_slice()).is_err());
    assert!(SecretKey::try_from([0; 24].as_slice()).is_ok());
    assert!(SecretKey::try_from([0; 31].as_slice()).is_err());
    assert!(SecretKey::try_from([0; 32].as_slice()).is_ok());
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

  #[tokio::test]
  async fn test_secret_key_transform() {
    for i in 0..100 {
      let kind = match i % 3 {
        0 => 16,
        1 => 24,
        2 => 32,
        _ => unreachable!(),
      };
      let key = SecretKey::random(kind);
      let mut buf = vec![0; key.encoded_len()];
      let encoded_len = key.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, key.encoded_len());
      let mut buf1 = vec![];
      let encoded_len1 = key.encode_to_writer(&mut buf1).unwrap();
      assert_eq!(encoded_len1, key.encoded_len());
      let mut buf2 = vec![];
      let encoded_len2 = key.encode_to_async_writer(&mut buf2).await.unwrap();
      assert_eq!(encoded_len2, key.encoded_len());

      let (decoded_len, decoded) = SecretKey::decode(&buf).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);
      let (decoded_len, decoded) = SecretKey::decode(&buf1).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);
      let (decoded_len, decoded) = SecretKey::decode(&buf2).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);

      let (decoded_len, decoded) =
        SecretKey::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);
      let (decoded_len, decoded) =
        SecretKey::decode_from_reader(&mut std::io::Cursor::new(&buf1)).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);
      let (decoded_len, decoded) =
        SecretKey::decode_from_reader(&mut std::io::Cursor::new(&buf2)).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);

      let (decoded_len, decoded) =
        SecretKey::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
          .await
          .unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);
      let (decoded_len, decoded) =
        SecretKey::decode_from_async_reader(&mut futures::io::Cursor::new(&buf1))
          .await
          .unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);
      let (decoded_len, decoded) =
        SecretKey::decode_from_async_reader(&mut futures::io::Cursor::new(&buf2))
          .await
          .unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, key);
    }
  }

  #[tokio::test]
  async fn test_secret_keys_transform() {
    for i in 0..100 {
      let keys = SecretKeys::random(i);
      let mut buf = vec![0; keys.encoded_len()];
      let encoded_len = keys.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, keys.encoded_len());

      let (decoded_len, decoded) = SecretKeys::decode(&buf).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, keys);

      let (decoded_len, decoded) =
        SecretKeys::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, keys);

      let (decoded_len, decoded) =
        SecretKeys::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
          .await
          .unwrap();
      assert_eq!(decoded_len, encoded_len);
      assert_eq!(decoded, keys);
    }
  }
}
