use std::borrow::Cow;

use aead::{generic_array::GenericArray, AeadInPlace, KeyInit};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use bytes::{Buf, BufMut};
use rand::Rng;

const NOPADDING_TAG: u8 = 0;
const PKCS7_TAG: u8 = 1;

const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const BLOCK_SIZE: usize = 16;

type Aes192Gcm = AesGcm<Aes192, U12>;

use std::str::FromStr;

use base64::{engine::general_purpose::STANDARD as b64, Engine as _};

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
    Self::InvalidBase64(e)
  }
}

/// The key used while attempting to encrypt/decrypt a message
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
  pub fn to_base64(&self) -> String {
    b64.encode(self)
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
    let bytes = b64.decode(s)?;

    match bytes.len() {
      16 => {
        let mut key = [0u8; 16];
        key.copy_from_slice(&bytes);
        Ok(SecretKey::Aes128(key))
      }
      24 => {
        let mut key = [0u8; 24];
        key.copy_from_slice(&bytes);
        Ok(SecretKey::Aes192(key))
      }
      32 => {
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(SecretKey::Aes256(key))
      }
      v => Err(ParseSecretKeyError::InvalidKeyLength(InvalidKeyLength(v))),
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

smallvec_wrapper::smallvec_wrapper!(
  /// A collection of secret keys, you can just treat it as a `Vec<SecretKey>`.
  #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
  #[repr(transparent)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  pub SecretKeys([SecretKey; 3]);
);

/// Security errors
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EncryptionError {
  /// Unknown encryption algorithm
  #[error("unknown encryption algorithm: {0}")]
  UnknownEncryptionAlgorithm(EncryptionAlgorithm),
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
      _ => return Err(EncryptionError::UnknownEncryptionAlgorithm(*self)),
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
    key: SecretKey,
    nonce: [u8; NONCE_SIZE],
    auth_data: &[u8],
    dst: &mut impl aead::Buffer,
  ) -> Result<(), EncryptionError> {
    if self.is_unknown() {
      return Err(EncryptionError::UnknownEncryptionAlgorithm(*self));
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
      if self.is_pkcs_7() {
        pkcs7decode(dst);
      }
    })
  }

  /// Returns the overhead of the encryption
  #[inline]
  pub const fn encrypt_overhead(&self) -> usize {
    match self {
      EncryptionAlgorithm::Pkcs7 => 49, // ALGO: 1, LEN: 4, IV: 12, Padding: 16, Tag: 16
      EncryptionAlgorithm::NoPadding => 33, // ALGO: 1, LEN: 4, IV: 12, Tag: 16
      _ => unreachable!(),
    }
  }

  /// Returns the encrypted length of the input size
  #[inline]
  pub const fn encrypted_len(&self, inp: usize) -> usize {
    match self {
      EncryptionAlgorithm::Pkcs7 => {
        // Determine the padding size
        let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

        // Sum the extra parts to get total size
        4 + NONCE_SIZE + inp + padding + TAG_SIZE
      }
      EncryptionAlgorithm::NoPadding => 4 + NONCE_SIZE + inp + TAG_SIZE,
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

#[cfg(test)]
mod tests {
  use bytes::BytesMut;

  use core::ops::{Deref, DerefMut};

  use arbitrary::Arbitrary;

  use super::*;

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
    vsn.encrypt(k1, nonce, extra, &mut dst).unwrap();
    encrypted.unsplit(dst);
    let exp_len = vsn.encrypted_len(plain_text.len());
    assert_eq!(encrypted.len(), exp_len);

    EncryptionAlgorithm::read_nonce(&mut encrypted);
    vsn.decrypt(k1, nonce, extra, &mut encrypted).unwrap();
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
    assert_eq!(encrypted.len(), exp_len);
    EncryptionAlgorithm::read_nonce(&mut encrypted);

    for (idx, k) in TEST_KEYS.iter().rev().enumerate() {
      if idx == TEST_KEYS.len() - 1 {
        algo.decrypt(*k, nonce, extra, &mut encrypted).unwrap();
        assert_eq!(encrypted.as_ref(), plain_text);
        return;
      }
      let e = algo.decrypt(*k, nonce, extra, &mut encrypted).unwrap_err();
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

  const TEST_KEYS: &[SecretKey] = &[
    SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
    SecretKey::Aes128([15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]),
    SecretKey::Aes128([8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7]),
  ];
}
