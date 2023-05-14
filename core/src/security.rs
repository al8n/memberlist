use aead::KeyInit;
use aead::{generic_array::GenericArray, Aead, AeadInPlace, Payload};
use aes_gcm::{
  aes::{cipher::consts::U12, Aes192},
  Aes128Gcm, Aes256Gcm, AesGcm,
};
use bytes::{BufMut, Bytes, BytesMut};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{SecretKey, SecretKeyring};

type Aes192Gcm = AesGcm<Aes192, U12>;

#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
  #[error("showbiz security: unknown encryption version: {0}")]
  UnknownEncryptionVersion(u8),
  #[error("showbiz security: {0}")]
  AeadError(aead::Error),
  #[error("showbiz security: cannot decode empty payload")]
  EmptyPayload,
  #[error("showbiz security: payload is too small to decrypt")]
  SmallPayload,
  #[error("showbiz security: no installed keys could decrypt the message")]
  NoInstalledKeys,
  #[error("showbiz security: no primary key installed")]
  MissingPrimaryKey,
}

#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum EncryptionVersion {
  /// AES-GCM 128, using PKCS7 padding
  #[default]
  PKCS7 = 0,
  /// AES-GCM 128, no padding. Padding not needed,
  NoPadding = 1,
}

impl EncryptionVersion {
  pub fn from_u8(val: u8) -> Result<Self, SecurityError> {
    match val {
      0 => Ok(EncryptionVersion::PKCS7),
      1 => Ok(EncryptionVersion::NoPadding),
      _ => Err(SecurityError::UnknownEncryptionVersion(val)),
    }
  }
}

const MIN_ENCRYPTION_VERSION: EncryptionVersion = EncryptionVersion::PKCS7;
pub(crate) const MAX_ENCRYPTION_VERSION: EncryptionVersion = EncryptionVersion::NoPadding;

const VERSION_SIZE: usize = 1;
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const MAX_PAD_OVERHEAD: usize = 16;
const BLOCK_SIZE: usize = 16;

// pkcs7encode is used to pad a byte buffer to a specific block size using
// the PKCS7 algorithm. "Ignores" some bytes to compensate for IV
#[inline]
fn pkcs7encode(buf: &mut BytesMut, ignore: usize, block_size: usize) {
  let n = buf.len() - ignore;
  let more = block_size - (n % block_size);
  buf.put_bytes(more as u8, more);
}

// pkcs7decode is used to decode a buffer that has been padded
#[inline]
fn pkcs7decode(mut buf: Vec<u8>) -> Vec<u8> {
  if buf.is_empty() {
    panic!("Cannot decode a PKCS7 buffer of zero length");
  }
  let n = buf.len();
  let last = buf[n - 1];
  let n = n - (last as usize);
  buf.truncate(n);
  buf
}

pub(crate) fn encrypt_overhead(vsn: EncryptionVersion) -> usize {
  match vsn {
    EncryptionVersion::PKCS7 => 45, // Version: 1, IV: 12, Padding: 16, Tag: 16
    EncryptionVersion::NoPadding => 29, // Version: 1, IV: 12, Tag: 16
  }
}

pub(crate) fn encrypted_length(vsn: EncryptionVersion, inp: usize) -> usize {
  // If we are on version 1, there is no padding
  if vsn as u8 >= 1 {
    return VERSION_SIZE + NONCE_SIZE + inp + TAG_SIZE;
  }

  // Determine the padding size
  let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

  // Sum the extra parts to get total size
  VERSION_SIZE + NONCE_SIZE + inp + padding + TAG_SIZE
}

macro_rules! bail {
  (enum $ty:ident:$key:ident { $($var:ident($algo: ident)),+ $(,)? } -> $fn:expr) => {
    match $key {
      $($ty::$var(key) => $fn($algo::new(GenericArray::from_slice(key)))),*
    }
  };
}

pub(crate) fn encrypt_payload(
  vsn: EncryptionVersion,
  key: &SecretKey,
  msg: &[u8],
  data: &[u8],
) -> Result<Bytes, SecurityError> {
  bail! {
    enum SecretKey: key {
      Aes128(Aes128Gcm),
      Aes192(Aes192Gcm),
      Aes256(Aes256Gcm),
    } -> |algo| encrypt_payload_in(algo, vsn, msg, data)
  }
}

#[inline]
fn decrypt_message(key: &SecretKey, msg: &[u8], data: &[u8]) -> Result<Vec<u8>, SecurityError> {
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
  vsn: EncryptionVersion,
  msg: &[u8],
  data: &[u8],
) -> Result<Bytes, SecurityError> {
  // Create the buffer to hold everything
  let mut buf = BytesMut::with_capacity(encrypted_length(vsn, msg.len()));
  // Write the encryption version
  buf.put_u8(vsn as u8);

  // Add a random nonce
  let mut nonce = [0u8; NONCE_SIZE];
  rand::thread_rng().fill(&mut nonce);
  buf.put_slice(&nonce);
  let after_nonce = buf.len();

  // Ensure we are correctly padded (only version 0)
  if vsn as u8 == 0 {
    buf.put_slice(msg);
    pkcs7encode(&mut buf, VERSION_SIZE + NONCE_SIZE, BLOCK_SIZE);
  }

  // Encrypt message using GCM
  let nonce = GenericArray::from_slice(&nonce);
  let src = if vsn as u8 == 0 {
    &buf[VERSION_SIZE + NONCE_SIZE..]
  } else {
    msg
  };
  let out = gcm
    .encrypt(
      nonce,
      Payload {
        msg: src,
        aad: data,
      },
    )
    .map_err(|e| SecurityError::AeadError(e))?;
  // Truncate the plaintext, and write the cipher text
  buf.truncate(after_nonce);
  buf.put_slice(&out);

  Ok(buf.freeze())
}

#[inline]
fn decrypt_message_in<A: Aead + AeadInPlace>(
  gcm: A,
  msg: &[u8],
  data: &[u8],
) -> Result<Vec<u8>, SecurityError> {
  // Decrypt the message
  let nonce = GenericArray::from_slice(&msg[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE]);
  let ciphertext = &msg[VERSION_SIZE + NONCE_SIZE..];
  let plain = gcm
    .decrypt(
      nonce,
      Payload {
        msg: ciphertext,
        aad: data,
      },
    )
    .map_err(|e| SecurityError::AeadError(e))?;

  Ok(plain)
}

impl SecretKeyring {
  pub(crate) async fn encrypt_payload(
    &self,
    vsn: EncryptionVersion,
    msg: &[u8],
    data: &[u8],
  ) -> Result<Bytes, SecurityError> {
    let Some(key) = self.lock().await.primary_key() else {
      return Err(SecurityError::MissingPrimaryKey)
    };

    bail! {
      enum SecretKey: key {
        Aes128(Aes128Gcm),
        Aes192(Aes192Gcm),
        Aes256(Aes256Gcm),
      } -> |algo| encrypt_payload_in(algo, vsn, msg, data)
    }
  }

  pub(crate) async fn decrypt_payload(
    &self,
    msg: &[u8],
    data: &[u8],
  ) -> Result<Vec<u8>, SecurityError> {
    // Ensure we have at least one byte
    if msg.is_empty() {
      return Err(SecurityError::EmptyPayload);
    }
  
    // Verify the version
    let vsn = EncryptionVersion::from_u8(msg[0])?;
    if vsn > MAX_ENCRYPTION_VERSION {
      return Err(SecurityError::UnknownEncryptionVersion(vsn as u8));
    }
  
    // Ensure the length is sane
    if msg.len() < encrypted_length(vsn, 0) {
      return Err(SecurityError::SmallPayload);
    }
  
    for key in self.lock().await.keys() {
      match decrypt_message(key, msg, data) {
        Ok(mut plain) => {
          // Remove the PKCS7 padding for vsn 0
          if vsn as u8 == 0 {
            plain = pkcs7decode(plain);
          }
          return Ok(plain);
        }
        Err(_) => continue,
      }
    }

    Err(SecurityError::NoInstalledKeys)
  }
}

pub(crate) fn decrypt_payload(
  keys: &[SecretKey],
  msg: &[u8],
  data: &[u8],
) -> Result<Vec<u8>, SecurityError> {
  // Ensure we have at least one byte
  if msg.is_empty() {
    return Err(SecurityError::EmptyPayload);
  }

  // Verify the version
  let vsn = EncryptionVersion::from_u8(msg[0])?;
  if vsn > MAX_ENCRYPTION_VERSION {
    return Err(SecurityError::UnknownEncryptionVersion(vsn as u8));
  }

  // Ensure the length is sane
  if msg.len() < encrypted_length(vsn, 0) {
    return Err(SecurityError::SmallPayload);
  }

  for key in keys {
    match decrypt_message(key, msg, data) {
      Ok(mut plain) => {
        // Remove the PKCS7 padding for vsn 0
        if vsn as u8 == 0 {
          plain = pkcs7decode(plain);
        }
        return Ok(plain);
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

  fn encrypt_decrypt_versioned(vsn: EncryptionVersion) {
    let k1 = SecretKey::Aes256([
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
      12, 13, 14, 15,
    ]);
    let plain_text = b"this is a plain text message";
    let extra = b"random data";

    let encrypted = encrypt_payload(vsn, &k1, plain_text, extra).unwrap();
    let exp_len = encrypted_length(vsn, plain_text.len());
    assert_eq!(encrypted.len(), exp_len);

    let msg = decrypt_payload(&[k1], encrypted.as_ref(), extra).unwrap();
    assert_eq!(msg, plain_text);
  }
  #[test]
  fn test_encrypt_decrypt_v0() {
    encrypt_decrypt_versioned(EncryptionVersion::PKCS7);
  }

  #[test]
  fn test_encrypt_decrypt_v1() {
    encrypt_decrypt_versioned(EncryptionVersion::NoPadding);
  }
}
