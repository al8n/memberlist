use std::borrow::Cow;

use aead::{AeadInPlace, KeyInit, generic_array::GenericArray};
use aes_gcm::{
  Aes128Gcm, Aes256Gcm, AesGcm,
  aes::{Aes192, cipher::consts::U12},
};
use bytes::{Buf, BufMut};
use rand::Rng;
use varing::decode_u32_varint;

use crate::{WireType, utils::merge};

use super::{Data, DataRef, DecodeError, EncodeError};

const NOPADDING_TAG: u8 = 1;
const PKCS7_TAG: u8 = 2;

const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const BLOCK_SIZE: usize = 16;

type Aes192Gcm = AesGcm<Aes192, U12>;

use std::str::FromStr;

use base64::{Engine as _, engine::general_purpose::STANDARD as b64};

/// An error type when parsing the encryption algorithm from str
#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("unknown encryption algorithm: {0}")]
pub struct ParseEncryptionAlgorithmError(String);

impl FromStr for EncryptionAlgorithm {
  type Err = ParseEncryptionAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(match s {
      "aes-gcm-no-padding" | "aes-gcm-nopadding" | "nopadding" | "NOPADDING" | "no-padding"
      | "NoPadding" | "no_padding" => Self::NoPadding,
      "aes-gcm-pkcs7" | "PKCS7" | "pkcs7" => Self::Pkcs7,
      s if s.starts_with("unknown") => {
        let v = s
          .trim_start_matches("unknown(")
          .trim_end_matches(')')
          .parse()
          .map_err(|_| ParseEncryptionAlgorithmError(s.to_string()))?;
        Self::Unknown(v)
      }
      e => return Err(ParseEncryptionAlgorithmError(e.to_string())),
    })
  }
}

/// Parse error for [`SecretKey`] from bytes slice
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[error("invalid key length({0}) - must be 16, 24, or 32 bytes for AES-128/192/256")]
pub struct InvalidKeyLength(pub(crate) usize);

/// Parse error for [`SecretKey`] from a base64 string
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseSecretKeyError {
  /// Invalid base64 string
  #[cfg_attr(feature = "std", error(transparent))]
  #[cfg_attr(not(feature = "std"), error("{0}"))]
  Base64(#[cfg_attr(feature = "std", from)] base64::DecodeError),
  /// Invalid key length
  #[error(transparent)]
  InvalidKeyLength(#[from] InvalidKeyLength),
}

#[cfg(not(feature = "std"))]
impl From<base64::DecodeError> for ParseSecretKeyError {
  fn from(e: base64::DecodeError) -> Self {
    Self::Base64(e)
  }
}

/// The key used while attempting to encrypt/decrypt a message
#[derive(
  Debug,
  Copy,
  Clone,
  PartialEq,
  Eq,
  PartialOrd,
  Ord,
  derive_more::IsVariant,
  derive_more::TryUnwrap,
  derive_more::Unwrap,
)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
#[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
pub enum SecretKey {
  /// secret key for AES128
  Aes128([u8; 16]),
  /// secret key for AES192
  Aes192([u8; 24]),
  /// secret key for AES256
  Aes256([u8; 32]),
}

impl SecretKey {
  /// Returns the base64 encoded secret key
  ///
  /// It is recommended to use [`SecretKey::encode_base64`] if you want to encode the key to a buffer
  /// without allocating a new string.
  pub fn to_base64(&self) -> String {
    b64.encode(self)
  }

  /// Returns the base64 encoded length of the secret key
  ///
  /// ## Example
  ///
  /// ```rust
  /// use memberlist_proto::encryption::SecretKey;
  ///
  /// let key = SecretKey::random_aes128();
  /// assert_eq!(key.base64_len(), 24);
  ///
  /// let key = SecretKey::random_aes192();
  /// assert_eq!(key.base64_len(), 32);
  ///
  /// let key = SecretKey::random_aes256();
  /// assert_eq!(key.base64_len(), 44);
  /// ```
  #[inline]
  pub const fn base64_len(&self) -> usize {
    match self {
      Self::Aes128(_) => 24,
      Self::Aes192(_) => 32,
      Self::Aes256(_) => 44,
    }
  }

  /// Encodes the secret key to the buffer in base64 format
  ///
  /// ## Example
  ///
  /// ```rust
  /// use memberlist_proto::encryption::SecretKey;
  ///
  /// let key = SecretKey::random_aes128();
  /// let mut buf = [0u8; 24];
  /// key.encode_base64(&mut buf).unwrap();
  /// assert_eq!(&buf, key.to_base64().as_bytes());
  ///
  /// let key = SecretKey::random_aes192();
  /// let mut buf = [0u8; 32];
  /// key.encode_base64(&mut buf).unwrap();
  /// assert_eq!(&buf, key.to_base64().as_bytes());
  ///
  /// let key = SecretKey::random_aes256();
  /// let mut buf = [0u8; 44];
  /// key.encode_base64(&mut buf).unwrap();
  /// assert_eq!(&buf, key.to_base64().as_bytes());
  /// ```
  #[inline]
  pub fn encode_base64(&self, buf: &mut [u8]) -> Result<usize, base64::EncodeSliceError> {
    b64.encode_slice(self.as_ref(), buf)
  }

  /// Creates a random secret key
  #[inline]
  pub fn random_aes128() -> Self {
    let mut key = [0u8; 16];
    rand::rng().fill(&mut key);
    Self::Aes128(key)
  }

  /// Creates a random secret key
  #[inline]
  pub fn random_aes192() -> Self {
    let mut key = [0u8; 24];
    rand::rng().fill(&mut key);
    Self::Aes192(key)
  }

  /// Creates a random secret key
  #[inline]
  pub fn random_aes256() -> Self {
    let mut key = [0u8; 32];
    rand::rng().fill(&mut key);
    Self::Aes256(key)
  }
}

impl TryFrom<&str> for SecretKey {
  type Error = ParseSecretKeyError;

  fn try_from(s: &str) -> Result<Self, Self::Error> {
    s.parse()
  }
}

impl TryFrom<&[u8]> for SecretKey {
  type Error = InvalidKeyLength;

  fn try_from(k: &[u8]) -> Result<Self, Self::Error> {
    Ok(match k.len() {
      16 => Self::Aes128(k.try_into().unwrap()),
      24 => Self::Aes192(k.try_into().unwrap()),
      32 => Self::Aes256(k.try_into().unwrap()),
      v => return Err(InvalidKeyLength(v)),
    })
  }
}

impl FromStr for SecretKey {
  type Err = ParseSecretKeyError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let mut buf = [0u8; 44];
    let readed = b64.decode_slice(s, &mut buf).map_err(|e| match e {
      base64::DecodeSliceError::DecodeError(decode_error) => decode_error.into(),
      base64::DecodeSliceError::OutputSliceTooSmall => {
        ParseSecretKeyError::InvalidKeyLength(InvalidKeyLength(s.len()))
      }
    })?;

    let bytes = &buf[..readed];
    Ok(match readed {
      16 => SecretKey::Aes128(bytes[..readed].try_into().unwrap()),
      24 => SecretKey::Aes192(bytes[..readed].try_into().unwrap()),
      32 => SecretKey::Aes256(bytes[..readed].try_into().unwrap()),
      v => return Err(ParseSecretKeyError::InvalidKeyLength(InvalidKeyLength(v))),
    })
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

impl<'a> DataRef<'a, Self> for SecretKey {
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let buf_len = buf.len();
    let mut key = None;

    while offset < buf_len {
      match buf[offset] {
        AES128_BYTE => {
          if key.is_some() {
            return Err(DecodeError::duplicate_field("SecretKey", "key", 0));
          }
          offset += 1;

          let (bytes_read, val) = decode_u32_varint(&buf[offset..])?;
          offset += bytes_read;

          let val: [u8; 16] = buf[offset..offset + val as usize]
            .try_into()
            .map_err(|_| DecodeError::buffer_underflow())?;
          offset += 16;
          key = Some(SecretKey::Aes128(val));
        }
        AES192_BYTE => {
          if key.is_some() {
            return Err(DecodeError::duplicate_field("SecretKey", "key", 0));
          }
          offset += 1;

          let (bytes_read, val) = decode_u32_varint(&buf[offset..])?;
          offset += bytes_read;

          let val: [u8; 24] = buf[offset..offset + val as usize]
            .try_into()
            .map_err(|_| DecodeError::buffer_underflow())?;
          offset += 24;

          key = Some(SecretKey::Aes192(val));
        }
        AES256_BYTE => {
          if key.is_some() {
            return Err(DecodeError::duplicate_field("SecretKey", "key", 0));
          }
          offset += 1;

          let (bytes_read, val) = decode_u32_varint(&buf[offset..])?;
          offset += bytes_read;

          let val: [u8; 32] = buf[offset..offset + val as usize]
            .try_into()
            .map_err(|_| DecodeError::buffer_underflow())?;
          offset += 32;

          key = Some(SecretKey::Aes256(val));
        }
        _ => offset += super::skip("SecretKey", &buf[offset..])?,
      }
    }

    let key = key.ok_or_else(|| DecodeError::missing_field("SecretKey", "key"))?;
    Ok((offset, key))
  }
}

const AES128_TAG: u8 = 1;
const AES192_TAG: u8 = 2;
const AES256_TAG: u8 = 3;

const AES128_BYTE: u8 = merge(WireType::LengthDelimited, AES128_TAG);
const AES192_BYTE: u8 = merge(WireType::LengthDelimited, AES192_TAG);
const AES256_BYTE: u8 = merge(WireType::LengthDelimited, AES256_TAG);

impl Data for SecretKey {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    1 + varing::encoded_u32_varint_len(self.len() as u32) + self.len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let buf_len = buf.len();
    let mut offset = 0;

    if buf_len < 1 {
      return Err(EncodeError::insufficient_buffer(
        self.encoded_len(),
        buf_len,
      ));
    }

    buf[offset] = match self {
      Self::Aes128(_) => AES128_BYTE,
      Self::Aes192(_) => AES192_BYTE,
      Self::Aes256(_) => AES256_BYTE,
    };
    offset += 1;

    let self_len = self.len();
    let len = varing::encode_u32_varint_to(self_len as u32, &mut buf[offset..])
      .map_err(|_| EncodeError::insufficient_buffer(self.encoded_len(), buf_len))?;
    offset += len;

    buf[offset..offset + self_len].copy_from_slice(self.as_ref());
    offset += self_len;

    #[cfg(debug_assertions)]
    super::debug_assert_write_eq::<Self>(offset, self.encoded_len());

    Ok(offset)
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

impl SecretKeys {
  /// Returns `true` if the collection is empty
  ///
  /// # Example
  ///
  /// ```
  /// use memberlist_proto::encryption::SecretKeys;
  ///
  /// let keys = SecretKeys::default();
  /// assert!(keys.is_empty());
  /// ```
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.0.is_empty()
  }

  /// Returns the length of the collection
  ///
  /// # Example
  ///
  /// ```
  /// use memberlist_proto::encryption::SecretKeys;
  ///
  /// let keys = SecretKeys::default();
  /// assert_eq!(keys.len(), 0);
  /// ```
  #[inline]
  pub fn len(&self) -> usize {
    self.0.len()
  }
}

/// Security errors
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EncryptionError {
  /// Unknown encryption algorithm
  #[error("unknown encryption algorithm: {0}")]
  UnknownAlgorithm(EncryptionAlgorithm),
  /// Encryt/Decrypt errors
  #[error("failed to encrypt/decrypt")]
  Encryptor,
}

impl From<aead::Error> for EncryptionError {
  fn from(_: aead::Error) -> Self {
    Self::Encryptor
  }
}

/// The encryption algorithm used to encrypt the message.
#[derive(
  Debug, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum EncryptionAlgorithm {
  /// AES-GCM, using no padding
  #[default]
  #[display("aes-gcm-nopadding")]
  NoPadding,
  /// AES-GCM, using PKCS7 padding
  #[display("aes-gcm-pkcs7")]
  Pkcs7,
  /// Unknwon encryption version
  #[display("unknown({_0})")]
  Unknown(u8),
}

#[cfg(any(feature = "quickcheck", test))]
const _: () = {
  use quickcheck::Arbitrary;

  impl EncryptionAlgorithm {
    const MAX: Self = Self::NoPadding;
    const MIN: Self = Self::Pkcs7;
  }

  impl Arbitrary for EncryptionAlgorithm {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
      let val = (u8::arbitrary(g) % Self::MAX.as_u8()) + Self::MIN.as_u8();
      match val {
        NOPADDING_TAG => Self::NoPadding,
        PKCS7_TAG => Self::Pkcs7,
        _ => unreachable!(),
      }
    }
  }
};

impl EncryptionAlgorithm {
  /// Returns the encryption version as a `u8`.
  #[inline]
  pub const fn as_u8(&self) -> u8 {
    match self {
      Self::NoPadding => NOPADDING_TAG,
      Self::Pkcs7 => PKCS7_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Returns the encryption version as a `&'static str`.
  #[inline]
  pub fn as_str(&self) -> Cow<'static, str> {
    let val = match self {
      Self::NoPadding => "aes-gcm-nopadding",
      Self::Pkcs7 => "aes-gcm-pkcs7",
      Self::Unknown(e) => return Cow::Owned(format!("unknown({})", e)),
    };
    Cow::Borrowed(val)
  }
}

impl From<u8> for EncryptionAlgorithm {
  fn from(value: u8) -> Self {
    match value {
      NOPADDING_TAG => Self::NoPadding,
      PKCS7_TAG => Self::Pkcs7,
      e => Self::Unknown(e),
    }
  }
}

impl EncryptionAlgorithm {
  /// Returns the nonce size of the encryption algorithm
  #[inline]
  pub const fn nonce_size(&self) -> usize {
    // only 12 bytes for nonce accepted currently
    NONCE_SIZE
  }

  /// Writes the nonce to the buffer, returning the random generated nonce
  pub fn write_nonce(dst: &mut impl BufMut) -> [u8; NONCE_SIZE] {
    // Add a random nonce
    let mut nonce = [0u8; NONCE_SIZE];
    rand::rng().fill(&mut nonce);
    dst.put_slice(&nonce);

    nonce
  }

  /// Generates a random nonce
  pub fn random_nonce() -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    rand::rng().fill(&mut nonce);
    nonce
  }

  /// Reads the nonce from the buffer
  pub fn read_nonce(src: &mut impl Buf) -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    nonce.copy_from_slice(&src.chunk()[..NONCE_SIZE]);
    src.advance(NONCE_SIZE);
    nonce
  }

  /// Encrypts the data using the provided secret key, nonce, and the authentication data
  pub fn encrypt<B>(
    &self,
    pk: SecretKey,
    nonce: [u8; NONCE_SIZE],
    auth_data: &[u8],
    buf: &mut B,
  ) -> Result<(), EncryptionError>
  where
    B: aead::Buffer,
  {
    match self {
      EncryptionAlgorithm::NoPadding => {}
      EncryptionAlgorithm::Pkcs7 => {
        let buf_len = buf.len();
        pkcs7encode(buf, buf_len, 0)?;
      }
      _ => return Err(EncryptionError::UnknownAlgorithm(*self)),
    }

    match pk {
      SecretKey::Aes128(pk) => {
        let gcm = Aes128Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, buf)
          .map_err(Into::into)
      }
      SecretKey::Aes192(pk) => {
        let gcm = Aes192Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, buf)
          .map_err(Into::into)
      }
      SecretKey::Aes256(pk) => {
        let gcm = Aes256Gcm::new(GenericArray::from_slice(&pk));
        gcm
          .encrypt_in_place(GenericArray::from_slice(&nonce), auth_data, buf)
          .map_err(Into::into)
      }
    }
  }

  /// Decrypts the data using the provided secret key, nonce, and the authentication data
  pub fn decrypt(
    &self,
    key: &SecretKey,
    nonce: &[u8],
    auth_data: &[u8],
    dst: &mut impl aead::Buffer,
  ) -> Result<(), EncryptionError> {
    if self.is_unknown() {
      return Err(EncryptionError::UnknownAlgorithm(*self));
    }

    // Get the AES block cipher
    match key {
      SecretKey::Aes128(pk) => {
        let gcm = Aes128Gcm::new(GenericArray::from_slice(pk));
        gcm
          .decrypt_in_place(GenericArray::from_slice(nonce), auth_data, dst)
          .map_err(Into::into)
      }
      SecretKey::Aes192(pk) => {
        let gcm = Aes192Gcm::new(GenericArray::from_slice(pk));
        gcm
          .decrypt_in_place(GenericArray::from_slice(nonce), auth_data, dst)
          .map_err(Into::into)
      }
      SecretKey::Aes256(pk) => {
        let gcm = Aes256Gcm::new(GenericArray::from_slice(pk));
        gcm
          .decrypt_in_place(GenericArray::from_slice(nonce), auth_data, dst)
          .map_err(Into::into)
      }
    }
    .inspect(|_| {
      if self.is_pkcs_7() {
        pkcs7decode(dst);
      }
    })
  }

  /// Returns the overhead of the encryption
  #[inline]
  pub(crate) const fn encrypt_overhead(&self) -> usize {
    match self {
      EncryptionAlgorithm::Pkcs7 => 44, // IV: 12, Padding: 16, Tag: 16
      EncryptionAlgorithm::NoPadding => 28, // IV: 12, Tag: 16
      _ => unreachable!(),
    }
  }

  /// Returns the encrypted suffix length of the input size
  #[inline]
  pub const fn encrypted_suffix_len(&self, inp: usize) -> usize {
    match self {
      EncryptionAlgorithm::Pkcs7 => {
        // Determine the padding size
        let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

        // Sum the extra parts to get total size
        padding + TAG_SIZE
      }
      EncryptionAlgorithm::NoPadding => TAG_SIZE,
      _ => unreachable!(),
    }
  }
}

/// pkcs7encode is used to pad a byte buffer to a specific block size using
/// the PKCS7 algorithm. "Ignores" some bytes to compensate for IV
#[inline]
fn pkcs7encode(
  buf: &mut impl aead::Buffer,
  buf_len: usize,
  ignore: usize,
) -> Result<(), aead::Error> {
  let n = buf_len - ignore;
  let more = BLOCK_SIZE - (n % BLOCK_SIZE);
  let mut block_buf = [0u8; BLOCK_SIZE];
  block_buf
    .iter_mut()
    .take(more)
    .for_each(|b| *b = more as u8);
  buf.extend_from_slice(&block_buf[..more])
}

/// pkcs7decode is used to decode a buffer that has been padded
#[inline]
fn pkcs7decode(buf: &mut impl aead::Buffer) {
  if buf.is_empty() {
    panic!("Cannot decode a PKCS7 buffer of zero length");
  }
  let n = buf.len();
  let last = buf.as_ref()[n - 1];
  let n = n - (last as usize);
  buf.truncate(n);
}

#[cfg(feature = "serde")]
const _: () = {
  use serde::{Deserialize, Deserializer, Serialize, Serializer};

  impl Serialize for EncryptionAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: Serializer,
    {
      if serializer.is_human_readable() {
        serializer.serialize_str(self.as_str().as_ref())
      } else {
        serializer.serialize_u8(self.as_u8())
      }
    }
  }

  impl<'de> Deserialize<'de> for EncryptionAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<EncryptionAlgorithm, D::Error>
    where
      D: Deserializer<'de>,
    {
      if deserializer.is_human_readable() {
        <&str>::deserialize(deserializer).and_then(|s| s.parse().map_err(serde::de::Error::custom))
      } else {
        u8::deserialize(deserializer).map(EncryptionAlgorithm::from)
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use bytes::BytesMut;

  use core::ops::{Deref, DerefMut};

  use arbitrary::Arbitrary;

  use super::*;

  impl super::EncryptionAlgorithm {
    /// Returns the encrypted length of the input size
    #[inline]
    const fn encrypted_len(&self, inp: usize) -> usize {
      match self {
        EncryptionAlgorithm::Pkcs7 => {
          // Determine the padding size
          let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

          // Sum the extra parts to get total size
          4 + 1 + NONCE_SIZE + inp + padding + TAG_SIZE
        }
        EncryptionAlgorithm::NoPadding => 4 + 1 + NONCE_SIZE + inp + TAG_SIZE,
        _ => unreachable!(),
      }
    }
  }

  #[test]
  fn arbitrary_secret_key() {
    let key = SecretKey::arbitrary(&mut arbitrary::Unstructured::new(&[0; 128])).unwrap();
    assert!(matches!(
      key,
      SecretKey::Aes128(_) | SecretKey::Aes192(_) | SecretKey::Aes256(_)
    ));
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

  fn encrypt_decrypt_versioned(vsn: EncryptionAlgorithm) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    let nonce = EncryptionAlgorithm::write_nonce(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    println!("before encrypted: {} {:?}", dst.len(), dst.as_ref());
    vsn.encrypt(k1, nonce, extra, &mut dst).unwrap();
    println!("encrypted: {} {:?}", dst.len(), dst.as_ref());
    encrypted.unsplit(dst);

    let exp_len = vsn.encrypted_len(plain_text.len());
    assert_eq!(encrypted.len(), exp_len - 5); // minus 5 for header

    EncryptionAlgorithm::read_nonce(&mut encrypted);
    vsn.decrypt(&k1, &nonce, extra, &mut encrypted).unwrap();
    assert_eq!(encrypted.as_ref(), plain_text);
  }

  fn decrypt_by_other_key(algo: EncryptionAlgorithm) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let mut encrypted = BytesMut::new();
    let nonce = EncryptionAlgorithm::write_nonce(&mut encrypted);
    let data_offset = encrypted.len();
    encrypted.put_slice(plain_text);

    let mut dst = encrypted.split_off(data_offset);
    algo.encrypt(k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = algo.encrypted_len(plain_text.len());
    assert_eq!(encrypted.len(), exp_len - 5); // minus 5 for header
    EncryptionAlgorithm::read_nonce(&mut encrypted);

    for (idx, k) in TEST_KEYS.iter().rev().enumerate() {
      if idx == TEST_KEYS.len() - 1 {
        algo.decrypt(k, &nonce, extra, &mut encrypted).unwrap();
        assert_eq!(encrypted.as_ref(), plain_text);
        return;
      }
      let e = algo.decrypt(k, &nonce, extra, &mut encrypted).unwrap_err();
      assert_eq!(e.to_string(), "failed to encrypt/decrypt");
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

  #[test]
  fn test_decrypt_by_other_key_v0() {
    let algo = EncryptionAlgorithm::Pkcs7;
    decrypt_by_other_key(algo);
  }

  #[test]
  fn test_decrypt_by_other_key_v1() {
    let algo = EncryptionAlgorithm::NoPadding;
    decrypt_by_other_key(algo);
  }

  #[test]
  fn test_encrypt_algorithm_from_str() {
    assert_eq!(
      "aes-gcm-no-padding".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::NoPadding
    );
    assert_eq!(
      "aes-gcm-nopadding".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::NoPadding
    );
    assert_eq!(
      "aes-gcm-pkcs7".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::Pkcs7
    );
    assert_eq!(
      "NoPadding".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::NoPadding
    );
    assert_eq!(
      "no-padding".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::NoPadding
    );
    assert_eq!(
      "nopadding".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::NoPadding
    );
    assert_eq!(
      "no_padding".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::NoPadding
    );
    assert_eq!(
      "NOPADDING".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::NoPadding
    );
    assert_eq!(
      "unknown(33)".parse::<EncryptionAlgorithm>().unwrap(),
      EncryptionAlgorithm::Unknown(33)
    );
    assert!("unknown".parse::<EncryptionAlgorithm>().is_err());
  }

  #[cfg(feature = "serde")]
  #[quickcheck_macros::quickcheck]
  fn encryption_algorithm_serde(algo: EncryptionAlgorithm) -> bool {
    use bincode::config::standard;

    let Ok(serialized) = serde_json::to_string(&algo) else {
      return false;
    };
    let Ok(deserialized) = serde_json::from_str(&serialized) else {
      return false;
    };
    if algo != deserialized {
      return false;
    }

    let Ok(serialized) = bincode::serde::encode_to_vec(algo, standard()) else {
      return false;
    };

    let Ok((deserialized, _)) = bincode::serde::decode_from_slice(&serialized, standard()) else {
      return false;
    };

    algo == deserialized
  }

  #[test]
  fn test_encode_base64() {
    for k in [
      SecretKey::random_aes128(),
      SecretKey::random_aes192(),
      SecretKey::random_aes256(),
    ] {
      let mut buf = vec![0; k.base64_len()];
      k.encode_base64(&mut buf).unwrap();
      assert_eq!(&buf, k.to_base64().as_bytes());
    }
  }

  #[test]
  fn test_try_from_str() {
    for k in &[
      SecretKey::random_aes128(),
      SecretKey::random_aes192(),
      SecretKey::random_aes256(),
    ] {
      let s = k.to_base64();
      let key = SecretKey::try_from(s.as_str()).unwrap();
      assert_eq!(k, key.as_ref());
    }

    let buf = "invalid base64 string";
    let key = SecretKey::try_from(buf);
    assert!(key.is_err());

    let mut buf = SecretKey::random_aes256().to_base64();
    buf.push_str(SecretKey::random_aes128().to_base64().as_str());
    let key = SecretKey::try_from(buf.as_str());
    assert!(key.is_err());
  }

  const TEST_KEYS: &[SecretKey] = &[
    SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
  ];
}
