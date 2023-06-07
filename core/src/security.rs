use aead::KeyInit;
use aead::{generic_array::GenericArray, Aead, AeadInPlace};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use bytes::{BufMut, BytesMut};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{SecretKey, SecretKeyring};

type Aes192Gcm = AesGcm<Aes192, U12>;

#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
  #[error("security: unknown encryption version: {0}")]
  UnknownEncryptionAlgo(#[from] UnknownEncryptionAlgo),
  #[error("security: {0}")]
  AeadError(aead::Error),
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
pub enum EncryptionAlgo {
  /// No encryption
  #[default]
  None = 0,

  /// AES-GCM 128, using PKCS7 padding
  PKCS7 = 1,
  /// AES-GCM 128, no padding. Padding not needed,
  NoPadding = 2,
}

impl EncryptionAlgo {
  pub const MAX: Self = Self::NoPadding;
  pub const MIN: Self = Self::None;

  pub(crate) const SIZE: usize = core::mem::size_of::<Self>();

  pub fn from_u8(val: u8) -> Result<Self, UnknownEncryptionAlgo> {
    match val {
      0 => Ok(EncryptionAlgo::None),
      1 => Ok(EncryptionAlgo::PKCS7),
      2 => Ok(EncryptionAlgo::NoPadding),
      _ => Err(UnknownEncryptionAlgo(val)),
    }
  }

  pub fn is_none(&self) -> bool {
    *self == EncryptionAlgo::None
  }
}

const VERSION_SIZE: usize = 1;
pub(crate) const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
pub(crate) const BLOCK_SIZE: usize = 16;

// pkcs7encode is used to pad a byte buffer to a specific block size using
// the PKCS7 algorithm. "Ignores" some bytes to compensate for IV
#[inline]
pub(crate) fn pkcs7encode(buf: &mut impl BufMut, buf_len: usize, ignore: usize, block_size: usize) {
  let n = buf_len - ignore;
  let more = block_size - (n % block_size);
  buf.put_bytes(more as u8, more);
}

// pkcs7decode is used to decode a buffer that has been padded
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

pub(crate) fn encrypt_overhead(vsn: EncryptionAlgo) -> usize {
  match vsn {
    EncryptionAlgo::PKCS7 => 45, // Version: 1, IV: 12, Padding: 16, Tag: 16
    EncryptionAlgo::NoPadding => 29, // Version: 1, IV: 12, Tag: 16
    EncryptionAlgo::None => unreachable!(),
  }
}

pub(crate) fn encrypted_length(vsn: EncryptionAlgo, inp: usize) -> usize {
  // If we are on version 2, there is no padding
  if vsn == EncryptionAlgo::NoPadding {
    return VERSION_SIZE + NONCE_SIZE + inp + TAG_SIZE;
  }

  // Determine the padding size
  let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

  // Sum the extra parts to get total size
  VERSION_SIZE + NONCE_SIZE + inp + padding + TAG_SIZE
}

macro_rules! bail {
  (enum $ty:ident: $key:ident { $($var:ident($algo: ident)),+ $(,)? } -> $fn:expr) => {
    match $key {
      $($ty::$var(key) => $fn($algo::new(GenericArray::from_slice(key)))),*
    }
  };
  (enum $ty:ident: &$key:ident { $($var:ident($algo: ident)),+ $(,)? } -> $fn:expr) => {
    match $key {
      $($ty::$var(key) => $fn($algo::new(GenericArray::from_slice(&key)))),*
    }
  };
}

#[inline]
fn decrypt_message(key: &SecretKey, msg: &mut BytesMut, data: &[u8]) -> Result<(), SecurityError> {
  bail! {
    enum SecretKey: key {
      Aes128(Aes128Gcm),
      Aes192(Aes192Gcm),
      Aes256(Aes256Gcm),
    } -> |algo| decrypt_message_in(algo, msg, data)
  }
}

#[inline]
fn encrypt_payload_in<A: AeadInPlace + Aead>(
  gcm: A,
  vsn: EncryptionAlgo,
  msg: &[u8],
  data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), SecurityError> {
  let offset = dst.len();
  dst.reserve(encrypted_length(vsn, msg.len()));

  // Write the encryption version
  dst.put_u8(vsn as u8);
  // Add a random nonce
  let mut nonce = [0u8; NONCE_SIZE];
  rand::thread_rng().fill(&mut nonce);
  dst.put_slice(&nonce);
  let after_nonce = offset + NONCE_SIZE + VERSION_SIZE;
  dst.put_slice(msg);

  // Encrypt message using GCM
  let nonce = GenericArray::from_slice(&nonce);
  // Ensure we are correctly padded (now, only for PKCS7)
  if vsn == EncryptionAlgo::PKCS7 {
    let buf_len = dst.len();
    pkcs7encode(dst, buf_len, offset + VERSION_SIZE + NONCE_SIZE, BLOCK_SIZE);
    let mut bytes = dst.split_off(after_nonce);
    gcm
      .encrypt_in_place(nonce, data, &mut bytes)
      .map(|_| {
        dst.unsplit(bytes);
      })
      .map_err(SecurityError::AeadError)
  } else {
    let mut bytes = dst.split_off(after_nonce);
    gcm
      .encrypt_in_place(nonce, data, &mut bytes)
      .map(|_| {
        dst.unsplit(bytes);
      })
      .map_err(SecurityError::AeadError)
  }
}

#[inline]
fn decrypt_message_in<A: Aead + AeadInPlace>(
  gcm: A,
  msg: &mut BytesMut,
  data: &[u8],
) -> Result<(), SecurityError> {
  // Decrypt the message
  let mut ciphertext = msg.split_off(VERSION_SIZE + NONCE_SIZE);
  let nonce = GenericArray::from_slice(&msg[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE]);

  gcm
    .decrypt_in_place(nonce, data, &mut ciphertext)
    .map_err(SecurityError::AeadError)?;
  msg.unsplit(ciphertext);
  Ok(())
}

pub(crate) fn encrypt_payload(
  key: &SecretKey,
  vsn: EncryptionAlgo,
  msg: &[u8],
  data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), SecurityError> {
  bail! {
    enum SecretKey: key {
      Aes128(Aes128Gcm),
      Aes192(Aes192Gcm),
      Aes256(Aes256Gcm),
    } -> |algo| encrypt_payload_in(algo, vsn, msg, data, dst)
  }
}

#[inline]
fn encrypt_to<A: AeadInPlace + Aead>(
  gcm: A,
  nonce: &[u8],
  data: &[u8],
  dst: &mut BytesMut,
) -> Result<(), SecurityError> {
  let nonce = GenericArray::from_slice(nonce);
  gcm
    .encrypt_in_place(nonce, data, dst)
    .map_err(SecurityError::AeadError)
}

impl SecretKeyring {
  pub(crate) fn encrypt_to(
    &self,
    key: SecretKey,
    nonce: &[u8],
    auth_data: &[u8],
    dst: &mut BytesMut,
  ) -> Result<(), SecurityError> {
    bail! {
      enum SecretKey: &key {
        Aes128(Aes128Gcm),
        Aes192(Aes192Gcm),
        Aes256(Aes256Gcm),
      } -> |algo| encrypt_to(algo, nonce, auth_data, dst)
    }
  }

  pub(crate) async fn encrypt_payload(
    &self,
    vsn: EncryptionAlgo,
    msg: &[u8],
    data: &[u8],
    dst: &mut BytesMut,
  ) -> Result<(), SecurityError> {
    let Some(key) = self.lock().await.primary_key() else {
      return Err(SecurityError::MissingPrimaryKey)
    };
    encrypt_payload(&key, vsn, msg, data, dst)
  }

  pub(crate) async fn decrypt_payload(
    &self,
    msg: &mut BytesMut,
    data: &[u8],
  ) -> Result<(), SecurityError> {
    // Ensure we have at least one byte
    if msg.is_empty() {
      return Err(SecurityError::EmptyPayload);
    }

    // Verify the version
    let vsn = EncryptionAlgo::from_u8(msg[0])?;

    // Ensure the length is sane
    if msg.len() < encrypted_length(vsn, 0) {
      return Err(SecurityError::SmallPayload);
    }

    let keys = {
      let mu = self.lock().await;
      mu.keys()
    };
    decrypt_payload(&keys, msg, data, vsn)
  }
}

pub(crate) fn decrypt_payload(
  keys: &[SecretKey],
  msg: &mut BytesMut,
  data: &[u8],
  vsn: EncryptionAlgo,
) -> Result<(), SecurityError> {
  for key in keys {
    match decrypt_message(key, msg, data) {
      Ok(_) => {
        // Remove the PKCS7 padding for vsn 0
        if vsn as u8 == 0 {
          pkcs7decode(msg);
        }
        return Ok(());
      }
      Err(_) => continue,
    }
  }

  Err(SecurityError::NoInstalledKeys)
}

pub(crate) fn append_bytes(first: &[u8], second: &[u8]) -> Vec<u8> {
  let has_first = !first.is_empty();
  let has_second = !second.is_empty();

  match (has_first, has_second) {
    (true, true) => [first, second].concat(),
    (true, false) => first.to_vec(),
    (false, true) => second.to_vec(),
    (false, false) => Vec::new(),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn encrypt_decrypt_versioned(vsn: EncryptionAlgo) {
    let k1 = SecretKey::Aes128([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";
    let mut encrypted = BytesMut::new();
    encrypt_payload(&k1, vsn, plain_text, extra, &mut encrypted).unwrap();

    let exp_len = encrypted_length(vsn, plain_text.len());
    assert_eq!(encrypted.len(), exp_len);

    decrypt_payload(&[k1], &mut encrypted, extra, vsn).unwrap();
    assert_eq!(&encrypted[VERSION_SIZE + NONCE_SIZE..], plain_text);
  }

  #[test]
  fn test_encrypt_decrypt_v0() {
    encrypt_decrypt_versioned(EncryptionAlgo::PKCS7);
  }

  #[test]
  fn test_encrypt_decrypt_v1() {
    encrypt_decrypt_versioned(EncryptionAlgo::NoPadding);
  }
}
