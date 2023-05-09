use aead::KeyInit;
use aead::{generic_array::GenericArray, AeadInPlace};
use aes_gcm::Aes128Gcm;
use rand::Rng;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
const MAX_ENCRYPTION_VERSION: EncryptionVersion = EncryptionVersion::NoPadding;

const VERSION_SIZE: usize = 1;
const NONCE_SIZE: usize = 12;
const TAG_SIZE: usize = 16;
const MAX_PAD_OVERHEAD: usize = 16;
const BLOCK_SIZE: usize = 16;

fn encrypt_overhead(vsn: EncryptionVersion) -> usize {
  match vsn {
    EncryptionVersion::PKCS7 => 45, // Version: 1, IV: 12, Padding: 16, Tag: 16
    EncryptionVersion::NoPadding => 29, // Version: 1, IV: 12, Tag: 16
    _ => panic!("unsupported version"),
  }
}

fn encrypted_length(vsn: EncryptionVersion, inp: usize) -> usize {
  // If we are on version 1, there is no padding
  if vsn as u8 >= 1 {
    return VERSION_SIZE + NONCE_SIZE + inp + TAG_SIZE;
  }

  // Determine the padding size
  let padding = BLOCK_SIZE - (inp % BLOCK_SIZE);

  // Sum the extra parts to get total size
  VERSION_SIZE + NONCE_SIZE + inp + padding + TAG_SIZE
}

fn encrypt_payload(
  vsn: EncryptionVersion,
  key: &[u8],
  msg: &[u8],
  data: &[u8],
) -> Result<(), SecurityError> {
  let key = GenericArray::from_slice(key);
  let cipher = Aes128Gcm::new(key);

  let mut nonce = [0u8; NONCE_SIZE];
  rand::thread_rng().fill(&mut nonce[..]);
  let nonce = GenericArray::from_slice(&nonce);

  let mut src = msg.to_vec();

  // Ensure we are correctly padded (only version 0)
  if vsn == EncryptionVersion::PKCS7 {
    let pad_size = BLOCK_SIZE - (src.len() % BLOCK_SIZE);
    src.resize(src.len() + pad_size, pad_size as u8);
  }

  let mut dst = bytes::BytesMut::with_capacity(encrypted_length(vsn, msg.len()));
  dst.fill(0);

  // Write the encryption version
  dst[0] = vsn as u8;

  // Add the nonce
  dst[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE].copy_from_slice(nonce.as_slice());

  // Encrypt message using GCM
  cipher
    .encrypt_in_place(nonce, data, &mut dst)
    .map_err(SecurityError::AeadError)
}

fn decrypt_message(key: &[u8], msg: &[u8], data: &[u8]) -> Result<Vec<u8>, SecurityError> {
  let key = GenericArray::from_slice(key);
  let cipher = Aes128Gcm::new(key);

  let nonce = GenericArray::from_slice(&msg[VERSION_SIZE..VERSION_SIZE + NONCE_SIZE]);
  let ciphertext = &msg[VERSION_SIZE + NONCE_SIZE..];

  let mut plaintext = ciphertext.to_vec();

  cipher
    .decrypt_in_place(nonce, data, &mut plaintext)
    .map(|_| plaintext)
    .map_err(SecurityError::AeadError)
}

fn decrypt_payload(keys: &[&[u8]], msg: &[u8], data: &[u8]) -> Result<Vec<u8>, SecurityError> {
  // Ensure we have at least one byte
  if msg.is_empty() {
    return Err(SecurityError::EmptyPayload);
  }

  // Verify the version
  let vsn = EncryptionVersion::from_u8(msg[0])?;

  // Ensure the length is sane
  if msg.len() < encrypted_length(vsn, 0) {
    return Err(SecurityError::SmallPayload);
  }

  for key in keys {
    match decrypt_message(key, msg, data) {
      Ok(mut plain) => {
        if vsn == EncryptionVersion::PKCS7 {
          let last = plain[plain.len() - 1] as usize;
          plain.truncate(plain.len() - last);
        }
        return Ok(plain);
      }
      Err(_) => continue,
    }
  }

  Err(SecurityError::NoInstalledKeys)
}

fn append_bytes(first: &[u8], second: &[u8]) -> Vec<u8> {
  let has_first = !first.is_empty();
  let has_second = !second.is_empty();

  match (has_first, has_second) {
    (true, true) => [first, second].concat(),
    (true, false) => first.to_vec(),
    (false, true) => second.to_vec(),
    (false, false) => Vec::new(),
  }
}
