//! Encryption — a tagged, feature-gated AEAD byte transform that wraps the
//! plain-frame / compound / compression codec.
//!
//! An encrypted payload is a tagged wrapper frame nesting outside the message,
//! compound, and compression framing:
//!
//! ```text
//! [Encrypted tag][algorithm tag][nonce: 12 bytes][ciphertext + 16-byte auth tag]
//! ```
//!
//! The algorithm tag identifies the AEAD backend; the 12-byte nonce is random
//! per encryption (no counter, no per-peer state). The 16-byte authenticator
//! is appended inline by the AEAD constructions (AES-GCM / ChaCha20-Poly1305).
//!
//! Nonce-reuse safety: with random 96-bit nonces the birthday bound caps a
//! single key at roughly 2^32 encryptions before the nonce-collision
//! probability becomes non-negligible (the NIST SP 800-38D guidance for
//! random-IV AES-GCM). At realistic gossip rates that is many years per key;
//! deployments approaching it should rotate the keyring's primary key (which
//! re-randomises the key, not just the nonce) well before the bound.
//!
//! The two-byte AAD `[Encrypted tag, algorithm tag]` binds the wrapper
//! identity and the cipher choice to AEAD authentication: a peer cannot
//! downgrade-swap algorithms without breaking the tag check.
//!
//! Stage-in-the-codec stack: encryption nests OUTSIDE compression (compress
//! the plaintext for ratio, then encrypt the result — compressing ciphertext
//! yields no gain). The framing tag-driven unwrap loop strips `Encrypted`
//! first, then `Compressed` (if present), then decodes the inner frame.

use std::vec::Vec;

use derive_more::{IsVariant, TryUnwrap, Unwrap};

/// Identifies the AEAD backend an encrypted frame was produced with. Each
/// backend is opt-in behind its own feature; a node that decodes a tag it was
/// not built with yields [`EncryptAlgorithm::Unknown`] and fails the decode
/// cleanly rather than panicking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IsVariant)]
pub enum EncryptAlgorithm {
  /// AES-GCM (96-bit nonce, 128-bit auth tag). The `SecretKey` variant
  /// (`Aes128` / `Aes192` / `Aes256`) picks the AES key length. Feature
  /// `aes-gcm`, backed by the `aes-gcm` crate.
  #[cfg(feature = "aes-gcm")]
  #[cfg_attr(docsrs, doc(cfg(feature = "aes-gcm")))]
  AesGcm,
  /// ChaCha20-Poly1305 (96-bit nonce, 128-bit auth tag, 256-bit key).
  /// Feature `chacha20-poly1305`, backed by the `chacha20poly1305` crate.
  #[cfg(feature = "chacha20-poly1305")]
  #[cfg_attr(docsrs, doc(cfg(feature = "chacha20-poly1305")))]
  ChaCha20Poly1305,
  /// An algorithm tag the local node was not built with.
  Unknown(u8),
}

/// Algorithm wire tags. Stable across builds — a node built with one backend
/// must agree with a peer built with another on the tag numbering.
#[cfg(feature = "aes-gcm")]
const AES_GCM_TAG: u8 = 1;
#[cfg(feature = "chacha20-poly1305")]
const CHACHA20_POLY1305_TAG: u8 = 2;

impl EncryptAlgorithm {
  /// The one-byte wire tag for this algorithm.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    match self {
      #[cfg(feature = "aes-gcm")]
      Self::AesGcm => AES_GCM_TAG,
      #[cfg(feature = "chacha20-poly1305")]
      Self::ChaCha20Poly1305 => CHACHA20_POLY1305_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Decode an algorithm from its one-byte wire tag. An unrecognized tag
  /// becomes [`EncryptAlgorithm::Unknown`] — the decode fails cleanly
  /// downstream rather than panicking.
  #[inline(always)]
  pub const fn from_tag(tag: u8) -> Self {
    match tag {
      #[cfg(feature = "aes-gcm")]
      AES_GCM_TAG => Self::AesGcm,
      #[cfg(feature = "chacha20-poly1305")]
      CHACHA20_POLY1305_TAG => Self::ChaCha20Poly1305,
      other => Self::Unknown(other),
    }
  }
}

impl From<EncryptAlgorithm> for u8 {
  fn from(a: EncryptAlgorithm) -> u8 {
    a.tag()
  }
}

impl From<u8> for EncryptAlgorithm {
  fn from(b: u8) -> Self {
    Self::from_tag(b)
  }
}

/// The symmetric key used to encrypt / decrypt a frame. The variant
/// *is* the cipher choice: an `Aes128` / `Aes192` / `Aes256` key drives
/// AES-GCM (key length picks the AES key size); a `ChaCha20Poly1305` key
/// drives ChaCha20-Poly1305. There is no independent "algorithm" field on
/// the key.
///
/// Variants are newtype only (no struct / multi-field-tuple variants). Each
/// carries the raw key bytes inline as a fixed-size array.
///
/// `Debug` is implemented manually so a log line printing a `SecretKey` (or
/// any container that recursively derives `Debug`, like [`Keyring`] or
/// [`EncryptionOptions`]) renders `SecretKey::<variant>(<redacted>)` instead
/// of disclosing the raw key bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, IsVariant, TryUnwrap, Unwrap)]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
#[non_exhaustive]
pub enum SecretKey {
  /// AES-128-GCM (128-bit key).
  #[cfg(feature = "aes-gcm")]
  #[cfg_attr(docsrs, doc(cfg(feature = "aes-gcm")))]
  Aes128([u8; 16]),
  /// AES-192-GCM (192-bit key).
  #[cfg(feature = "aes-gcm")]
  #[cfg_attr(docsrs, doc(cfg(feature = "aes-gcm")))]
  Aes192([u8; 24]),
  /// AES-256-GCM (256-bit key).
  #[cfg(feature = "aes-gcm")]
  #[cfg_attr(docsrs, doc(cfg(feature = "aes-gcm")))]
  Aes256([u8; 32]),
  /// ChaCha20-Poly1305 (256-bit key).
  #[cfg(feature = "chacha20-poly1305")]
  #[cfg_attr(docsrs, doc(cfg(feature = "chacha20-poly1305")))]
  ChaCha20Poly1305([u8; 32]),
}

impl core::fmt::Debug for SecretKey {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      #[cfg(feature = "aes-gcm")]
      Self::Aes128(_) => write!(f, "SecretKey::Aes128(<redacted>)"),
      #[cfg(feature = "aes-gcm")]
      Self::Aes192(_) => write!(f, "SecretKey::Aes192(<redacted>)"),
      #[cfg(feature = "aes-gcm")]
      Self::Aes256(_) => write!(f, "SecretKey::Aes256(<redacted>)"),
      #[cfg(feature = "chacha20-poly1305")]
      Self::ChaCha20Poly1305(_) => write!(f, "SecretKey::ChaCha20Poly1305(<redacted>)"),
    }
  }
}

impl SecretKey {
  /// The AEAD algorithm this key drives.
  #[inline(always)]
  pub const fn algorithm(&self) -> EncryptAlgorithm {
    match self {
      #[cfg(feature = "aes-gcm")]
      Self::Aes128(_) | Self::Aes192(_) | Self::Aes256(_) => EncryptAlgorithm::AesGcm,
      #[cfg(feature = "chacha20-poly1305")]
      Self::ChaCha20Poly1305(_) => EncryptAlgorithm::ChaCha20Poly1305,
    }
  }

  /// The raw key bytes (length 16 / 24 / 32 depending on the variant).
  #[inline(always)]
  pub fn as_bytes(&self) -> &[u8] {
    match self {
      #[cfg(feature = "aes-gcm")]
      Self::Aes128(k) => k.as_slice(),
      #[cfg(feature = "aes-gcm")]
      Self::Aes192(k) => k.as_slice(),
      #[cfg(feature = "aes-gcm")]
      Self::Aes256(k) => k.as_slice(),
      #[cfg(feature = "chacha20-poly1305")]
      Self::ChaCha20Poly1305(k) => k.as_slice(),
    }
  }

  /// Generate a random AES-128-GCM key.
  ///
  /// # Panics
  /// Panics if the platform entropy source is unavailable. Where entropy can
  /// fail at runtime (e.g. an embedded getrandom backend), build the key from
  /// explicit bytes via [`SecretKey::Aes128`] instead.
  #[cfg(feature = "aes-gcm")]
  #[cfg_attr(docsrs, doc(cfg(feature = "aes-gcm")))]
  pub fn random_aes128() -> Self {
    let mut k = [0u8; 16];
    getrandom::fill(&mut k).expect("system entropy source unavailable");
    Self::Aes128(k)
  }

  /// Generate a random AES-192-GCM key.
  ///
  /// # Panics
  /// Panics if the platform entropy source is unavailable. Where entropy can
  /// fail at runtime (e.g. an embedded getrandom backend), build the key from
  /// explicit bytes via [`SecretKey::Aes192`] instead.
  #[cfg(feature = "aes-gcm")]
  #[cfg_attr(docsrs, doc(cfg(feature = "aes-gcm")))]
  pub fn random_aes192() -> Self {
    let mut k = [0u8; 24];
    getrandom::fill(&mut k).expect("system entropy source unavailable");
    Self::Aes192(k)
  }

  /// Generate a random AES-256-GCM key.
  ///
  /// # Panics
  /// Panics if the platform entropy source is unavailable. Where entropy can
  /// fail at runtime (e.g. an embedded getrandom backend), build the key from
  /// explicit bytes via [`SecretKey::Aes256`] instead.
  #[cfg(feature = "aes-gcm")]
  #[cfg_attr(docsrs, doc(cfg(feature = "aes-gcm")))]
  pub fn random_aes256() -> Self {
    let mut k = [0u8; 32];
    getrandom::fill(&mut k).expect("system entropy source unavailable");
    Self::Aes256(k)
  }

  /// Generate a random ChaCha20-Poly1305 key.
  ///
  /// # Panics
  /// Panics if the platform entropy source is unavailable. Where entropy can
  /// fail at runtime (e.g. an embedded getrandom backend), build the key from
  /// explicit bytes via [`SecretKey::ChaCha20Poly1305`] instead.
  #[cfg(feature = "chacha20-poly1305")]
  #[cfg_attr(docsrs, doc(cfg(feature = "chacha20-poly1305")))]
  pub fn random_chacha20poly1305() -> Self {
    let mut k = [0u8; 32];
    getrandom::fill(&mut k).expect("system entropy source unavailable");
    Self::ChaCha20Poly1305(k)
  }
}

/// The `(claimed, max)` post-decrypt plaintext-length pair carried by
/// [`EncryptionError::Oversize`]. Newtype payload so the enum variant stays
/// newtype-only (no struct variants in this codebase).
///
/// The carried `claimed` is the post-decrypt plaintext byte length the
/// wrapper implies (`ciphertext.len() - AUTH_TAG_LEN`) — what the caller's
/// `max_plaintext_len` ceiling actually bounds. The on-wire envelope is
/// wider by [`ENCRYPTED_WRAPPER_OVERHEAD`]; the wrapper's outer length is
/// gated separately by the caller's reliable / gossip framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OversizeCiphertext(usize, usize);

impl OversizeCiphertext {
  /// Build the payload from `(claimed, max)`. Fields are private; external
  /// callers see read-only `claimed()` / `max()` accessors only.
  #[inline(always)]
  pub const fn new(claimed: usize, max: usize) -> Self {
    Self(claimed, max)
  }

  /// The post-decrypt plaintext byte length the frame's ciphertext implies.
  #[inline(always)]
  pub const fn claimed(&self) -> usize {
    self.0
  }

  /// The caller's hard ceiling on the plaintext length.
  #[inline(always)]
  pub const fn max(&self) -> usize {
    self.1
  }
}

/// An encryption or decryption failure. Variants are unit or newtype only.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum EncryptionError {
  /// AEAD authentication failed — corrupt ciphertext, mismatched AAD, or a
  /// wrong key. Surfaced for every algorithm; never a panic.
  #[error("AEAD authentication failed")]
  AuthFailed,
  /// The algorithm tag is unknown or its backend feature is not built in.
  #[error("unsupported encryption algorithm: tag {0}")]
  UnsupportedAlgorithm(u8),
  /// A frame's post-decrypt plaintext length exceeds the caller's hard
  /// ceiling (gossip MTU or `max_stream_frame_size`). Rejected before any
  /// AEAD allocation. The carried length is the plaintext length the
  /// wrapper implies, not the on-wire envelope size.
  #[error("plaintext length {} exceeds maximum {}", _0.claimed(), _0.max())]
  Oversize(OversizeCiphertext),
  /// Either zero variant-matching keys in the keyring (the wire algorithm tag
  /// has no candidate to attempt) or two-or-more variant-matching candidates
  /// all failing AEAD auth (the multi-key fallback was exhausted). A single
  /// candidate failing AEAD surfaces as [`EncryptionError::AuthFailed`]
  /// instead. Coordinators treat all of these identically — drop the frame.
  #[error("no matching key in the keyring")]
  NoMatchingKey,
  /// The supplied key's cipher variant does not match the requested algorithm
  /// (e.g. an AES key passed to the ChaCha20-Poly1305 dispatch path). This
  /// is a caller programming error, never a wire-level condition.
  #[error("key cipher variant does not match the requested algorithm")]
  KeyMismatch,
  /// The byte slice does not carry the expected encrypted-frame header
  /// (wrong leading tag or shorter than the minimum wrapper size).
  #[error("byte slice is not a valid encrypted frame")]
  MalformedFrame,
  /// Encryption is enforced on this path but the outermost frame is not
  /// wrapped in [`MessageTag::Encrypted`]. Rejected before any decoding so
  /// an unauthenticated datagram cannot inject SWIM membership traffic.
  #[error("encryption is enabled but the inbound frame is not wrapped in Encrypted")]
  EncryptionRequired,
  /// The platform entropy source failed while drawing the per-frame AEAD
  /// nonce. Recoverable: the frame is simply not produced. On no_std targets
  /// this reflects an integrator-provided getrandom backend that errored or
  /// was not yet ready.
  #[error("entropy source failed while generating an encryption nonce")]
  Entropy,
}

/// Returns `true` when `key`'s cipher variant matches `algo`. `Unknown`
/// algorithms are accepted (the dispatch returns `UnsupportedAlgorithm` for
/// them); a known algorithm paired with a wrong-cipher key is rejected.
///
/// This is the always-on precheck called at the top of `encrypt` and
/// `decrypt` before the feature-gated `match algo` dispatch.
const fn key_matches_algorithm(algo: EncryptAlgorithm, key: &SecretKey) -> bool {
  match algo {
    #[cfg(feature = "aes-gcm")]
    EncryptAlgorithm::AesGcm => matches!(
      key,
      SecretKey::Aes128(_) | SecretKey::Aes192(_) | SecretKey::Aes256(_)
    ),
    #[cfg(feature = "chacha20-poly1305")]
    EncryptAlgorithm::ChaCha20Poly1305 => matches!(key, SecretKey::ChaCha20Poly1305(_)),
    EncryptAlgorithm::Unknown(_) => true,
  }
}

/// Encrypt `plaintext` with `algo` and `key` using `nonce` and `aad`. A pure
/// transform — no I/O. Returns ciphertext with the 16-byte AEAD tag appended.
///
/// Returns [`EncryptionError::UnsupportedAlgorithm`] for an algorithm whose
/// backend feature is not built in (including `EncryptAlgorithm::Unknown`).
/// Returns [`EncryptionError::KeyMismatch`] if `key`'s cipher variant does not
/// match `algo` (e.g. an AES key passed to the ChaCha20-Poly1305 path).
#[allow(unused_variables)]
pub fn encrypt(
  algo: EncryptAlgorithm,
  key: &SecretKey,
  nonce: &[u8; 12],
  plaintext: &[u8],
  aad: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
  if !key_matches_algorithm(algo, key) {
    return Err(EncryptionError::KeyMismatch);
  }
  match algo {
    #[cfg(feature = "aes-gcm")]
    EncryptAlgorithm::AesGcm => aes_gcm_encrypt(key, nonce, plaintext, aad),
    #[cfg(feature = "chacha20-poly1305")]
    EncryptAlgorithm::ChaCha20Poly1305 => chacha20poly1305_encrypt(key, nonce, plaintext, aad),
    EncryptAlgorithm::Unknown(t) => Err(EncryptionError::UnsupportedAlgorithm(t)),
    #[allow(unreachable_patterns)]
    other => Err(EncryptionError::UnsupportedAlgorithm(other.tag())),
  }
}

/// Decrypt `ciphertext` with `algo` and `key` using `nonce` and `aad`. A pure
/// transform — no I/O. The AEAD tag is the trailing 16 bytes of `ciphertext`
/// (the standard AES-GCM / ChaCha20-Poly1305 layout); a failing tag check
/// surfaces as [`EncryptionError::AuthFailed`].
#[allow(unused_variables)]
pub fn decrypt(
  algo: EncryptAlgorithm,
  key: &SecretKey,
  nonce: &[u8; 12],
  ciphertext: &[u8],
  aad: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
  if !key_matches_algorithm(algo, key) {
    return Err(EncryptionError::KeyMismatch);
  }
  match algo {
    #[cfg(feature = "aes-gcm")]
    EncryptAlgorithm::AesGcm => aes_gcm_decrypt(key, nonce, ciphertext, aad),
    #[cfg(feature = "chacha20-poly1305")]
    EncryptAlgorithm::ChaCha20Poly1305 => chacha20poly1305_decrypt(key, nonce, ciphertext, aad),
    EncryptAlgorithm::Unknown(t) => Err(EncryptionError::UnsupportedAlgorithm(t)),
    #[allow(unreachable_patterns)]
    other => Err(EncryptionError::UnsupportedAlgorithm(other.tag())),
  }
}

#[cfg(feature = "aes-gcm")]
fn aes_gcm_encrypt(
  key: &SecretKey,
  nonce: &[u8; 12],
  plaintext: &[u8],
  aad: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
  use aead::{AeadInPlace, KeyInit};
  use aes_gcm::{
    Aes128Gcm, Aes256Gcm, AesGcm,
    aes::{Aes192, cipher::consts::U12},
  };

  type Aes192Gcm = AesGcm<Aes192, U12>;

  // The owned key/nonce byte arrays convert into the cipher's `GenericArray`-backed
  // `Key`/`Nonce` types through `From<[u8; N]>`, keeping us off the deprecated
  // `GenericArray::from_slice` constructor (`generic-array` 0.14 nudges toward 1.x).
  let mut buf = plaintext.to_vec();
  match key {
    #[cfg(feature = "aes-gcm")]
    SecretKey::Aes128(k) => {
      let cipher = Aes128Gcm::new(&(*k).into());
      cipher
        .encrypt_in_place(&(*nonce).into(), aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    #[cfg(feature = "aes-gcm")]
    SecretKey::Aes192(k) => {
      let cipher = Aes192Gcm::new(&(*k).into());
      cipher
        .encrypt_in_place(&(*nonce).into(), aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    #[cfg(feature = "aes-gcm")]
    SecretKey::Aes256(k) => {
      let cipher = Aes256Gcm::new(&(*k).into());
      cipher
        .encrypt_in_place(&(*nonce).into(), aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    #[cfg(feature = "chacha20-poly1305")]
    SecretKey::ChaCha20Poly1305(_) => {
      // Precheck in encrypt() guarantees this arm is never reached.
      unreachable!("variant mismatch should be caught by precheck in encrypt()/decrypt()")
    }
  }
  Ok(buf)
}

#[cfg(feature = "aes-gcm")]
fn aes_gcm_decrypt(
  key: &SecretKey,
  nonce: &[u8; 12],
  ciphertext: &[u8],
  aad: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
  use aead::{AeadInPlace, KeyInit};
  use aes_gcm::{
    Aes128Gcm, Aes256Gcm, AesGcm,
    aes::{Aes192, cipher::consts::U12},
  };

  type Aes192Gcm = AesGcm<Aes192, U12>;

  // See `aes_gcm_encrypt`: `From<[u8; N]>` builds the cipher key/nonce, avoiding
  // the deprecated `GenericArray::from_slice`.
  let mut buf = ciphertext.to_vec();
  match key {
    SecretKey::Aes128(k) => {
      let cipher = Aes128Gcm::new(&(*k).into());
      cipher
        .decrypt_in_place(&(*nonce).into(), aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    SecretKey::Aes192(k) => {
      let cipher = Aes192Gcm::new(&(*k).into());
      cipher
        .decrypt_in_place(&(*nonce).into(), aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    SecretKey::Aes256(k) => {
      let cipher = Aes256Gcm::new(&(*k).into());
      cipher
        .decrypt_in_place(&(*nonce).into(), aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    SecretKey::ChaCha20Poly1305(_) => {
      // Precheck in decrypt() guarantees this arm is never reached.
      unreachable!("variant mismatch should be caught by precheck in encrypt()/decrypt()")
    }
  }
  Ok(buf)
}

#[cfg(feature = "chacha20-poly1305")]
fn chacha20poly1305_encrypt(
  key: &SecretKey,
  nonce: &[u8; 12],
  plaintext: &[u8],
  aad: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
  use aead::{AeadInPlace, KeyInit};
  use chacha20poly1305::ChaCha20Poly1305;

  let k = match key {
    SecretKey::ChaCha20Poly1305(k) => k,
    // Precheck in encrypt() guarantees this arm is never reached.
    _ => unreachable!("variant mismatch should be caught by precheck in encrypt()/decrypt()"),
  };
  // `From<[u8; N]>` builds the key/nonce, avoiding the deprecated
  // `GenericArray::from_slice` (see `aes_gcm_encrypt`).
  let cipher = ChaCha20Poly1305::new(&(*k).into());
  let mut buf = plaintext.to_vec();
  cipher
    .encrypt_in_place(&(*nonce).into(), aad, &mut buf)
    .map_err(|_| EncryptionError::AuthFailed)?;
  Ok(buf)
}

#[cfg(feature = "chacha20-poly1305")]
fn chacha20poly1305_decrypt(
  key: &SecretKey,
  nonce: &[u8; 12],
  ciphertext: &[u8],
  aad: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
  use aead::{AeadInPlace, KeyInit};
  use chacha20poly1305::ChaCha20Poly1305;

  let k = match key {
    SecretKey::ChaCha20Poly1305(k) => k,
    // Precheck in decrypt() guarantees this arm is never reached.
    _ => unreachable!("variant mismatch should be caught by precheck in encrypt()/decrypt()"),
  };
  // See `chacha20poly1305_encrypt`: `From<[u8; N]>` avoids the deprecated
  // `GenericArray::from_slice`.
  let cipher = ChaCha20Poly1305::new(&(*k).into());
  let mut buf = ciphertext.to_vec();
  cipher
    .decrypt_in_place(&(*nonce).into(), aad, &mut buf)
    .map_err(|_| EncryptionError::AuthFailed)?;
  Ok(buf)
}

/// A `Keyring` operation rejection. Variants are unit-only.
#[non_exhaustive]
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyringError {
  /// The named key matched the primary — it cannot be removed (operators
  /// `promote` a secondary first, then remove the former primary).
  #[error("cannot remove the primary key; promote a secondary first")]
  IsPrimary,
  /// The named key is not present in the ring.
  #[error("the named key is not in the keyring")]
  NotInRing,
}

/// A primary key + an ordered list of secondaries. Lock-free plain data.
///
/// Encrypt always uses the primary. Decrypt filters keys by variant matching
/// the wire algorithm tag, then tries each in order (primary first, then
/// secondaries). Mixed-cipher rings are allowed — a deployment can carry an
/// AES primary and a ChaCha20-Poly1305 secondary during a cipher migration;
/// the variant filter selects which keys to try for a given inbound frame.
///
/// Zero `pub` fields — accessor-only.
///
/// `PartialEq` / `Eq` are derived so callers can compare two configurations
/// (e.g. a config reconciler reapplying the same options): equality is
/// primary-and-secondaries-in-order. A reordering of secondaries via
/// `promote` makes two rings unequal even though they accept the same keys,
/// which is correct — outbound traffic uses the primary, and a change in
/// which key is primary is a policy change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Keyring {
  primary: SecretKey,
  secondaries: Vec<SecretKey>,
}

impl Keyring {
  /// Build a ring with just a primary key (no secondaries).
  pub fn new(primary: SecretKey) -> Self {
    Self {
      primary,
      secondaries: Vec::new(),
    }
  }

  /// Build a ring with a primary and an iterator of seed secondaries.
  /// Duplicates of the primary or of an earlier secondary are dropped.
  #[must_use]
  pub fn with_secondaries<I>(primary: SecretKey, secondaries: I) -> Self
  where
    I: IntoIterator<Item = SecretKey>,
  {
    let mut kr = Self::new(primary);
    for s in secondaries {
      kr.insert_secondary(s);
    }
    kr
  }

  /// The primary key (used for every outbound encryption).
  #[inline(always)]
  pub const fn primary_ref(&self) -> &SecretKey {
    &self.primary
  }

  /// The secondary keys in decrypt-trial order. After `promote`, the former
  /// primary appears at index 0; otherwise the order is insertion order.
  #[inline(always)]
  pub fn secondaries(&self) -> &[SecretKey] {
    &self.secondaries
  }

  /// Add a secondary key. A duplicate of the primary or of an existing
  /// secondary is dropped (idempotent insert; no error).
  pub fn insert_secondary(&mut self, key: SecretKey) {
    if self.primary == key {
      return;
    }
    if self.secondaries.contains(&key) {
      return;
    }
    self.secondaries.push(key);
  }

  /// Remove a secondary key by its raw byte value. Errors if the byte value
  /// matches the primary or is not in the ring.
  ///
  /// The lookup is byte-only: a 32-byte AES-256 key and a 32-byte ChaCha20-Poly1305
  /// key whose inner arrays coincide are indistinguishable to this method; the
  /// first secondary whose bytes match is removed. Matches the legacy
  /// `memberlist-core::Keyring::remove` contract.
  pub fn remove_secondary(&mut self, key: &[u8]) -> Result<(), KeyringError> {
    if self.primary.as_bytes() == key {
      return Err(KeyringError::IsPrimary);
    }
    let pos = self
      .secondaries
      .iter()
      .position(|s| s.as_bytes() == key)
      .ok_or(KeyringError::NotInRing)?;
    self.secondaries.remove(pos);
    Ok(())
  }

  /// Promote a secondary to primary; the old primary becomes a secondary at
  /// index 0. Promoting the current primary is a no-op (returns `Ok(())`).
  /// Errors if the named key is not in the ring.
  ///
  /// The lookup is byte-only — see `remove_secondary` for the collision note.
  pub fn promote(&mut self, key: &[u8]) -> Result<(), KeyringError> {
    if self.primary.as_bytes() == key {
      return Ok(());
    }
    let pos = self
      .secondaries
      .iter()
      .position(|s| s.as_bytes() == key)
      .ok_or(KeyringError::NotInRing)?;
    let new_primary = self.secondaries.remove(pos);
    let old_primary = core::mem::replace(&mut self.primary, new_primary);
    self.secondaries.insert(0, old_primary);
    Ok(())
  }
}

/// Transport-agnostic encryption configuration handed to each coordinator at
/// construction. Zero `pub` fields — accessor-only.
///
/// An `EncryptionOptions` with no keyring is the default: every payload is
/// left unencrypted and the codec paths reduce to identity. The operator
/// opts in by attaching a `Keyring` via `with_keyring` / `set_keyring`.
///
/// `PartialEq` / `Eq` are derived so a coordinator's `set_encryption_options`
/// can detect a no-op reapply (config reconciler republishes the same
/// effective policy) and skip its bridge-fan-out — without that early
/// return an insecure-transport reapply would tear down every live
/// reliable exchange for no security gain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionOptions {
  keyring: Option<Keyring>,
}

impl Default for EncryptionOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl EncryptionOptions {
  /// A new, disabled configuration — no keyring. Every payload is left
  /// unencrypted.
  #[inline(always)]
  pub const fn new() -> Self {
    Self { keyring: None }
  }

  /// Setter: attach a keyring in place (enables encryption). Replaces any
  /// previously installed keyring; use `clear_keyring` to disable.
  #[inline(always)]
  pub fn set_keyring(&mut self, keyring: Keyring) -> &mut Self {
    self.keyring = Some(keyring);
    self
  }

  /// Builder: attach a keyring (enables encryption).
  #[must_use]
  #[inline(always)]
  pub fn with_keyring(mut self, keyring: Keyring) -> Self {
    self.keyring = Some(keyring);
    self
  }

  /// Setter: assign the raw `Option<Keyring>` wrapper in place.
  /// `None` disables encryption; `Some(keyring)` enables it.
  #[inline(always)]
  pub fn update_keyring(&mut self, val: Option<Keyring>) -> &mut Self {
    self.keyring = val;
    self
  }

  /// Builder: assign the raw `Option<Keyring>` wrapper (consuming).
  /// `None` disables encryption; `Some(keyring)` enables it.
  #[must_use]
  #[inline(always)]
  pub fn maybe_keyring(mut self, val: Option<Keyring>) -> Self {
    self.keyring = val;
    self
  }

  /// Setter: drop the keyring (disable encryption) in place.
  #[inline(always)]
  pub fn clear_keyring(&mut self) -> &mut Self {
    self.keyring = None;
    self
  }

  /// The attached keyring, or `None` when encryption is disabled.
  #[inline(always)]
  pub const fn keyring(&self) -> Option<&Keyring> {
    self.keyring.as_ref()
  }

  /// `true` when a keyring is attached — encryption is enabled.
  #[inline(always)]
  pub const fn is_enabled(&self) -> bool {
    self.keyring.is_some()
  }
}

/// The one-byte wrapper tag that prefixes every encrypted frame. Pinned at
/// 13 — the framing layer (`framing::MessageTag::Encrypted`) wires the same
/// numeric value. A change here is a wire-protocol break.
pub const ENCRYPTED_TAG: u8 = 13;

/// The on-wire nonce length in bytes (96 bits — the standard for AES-GCM and
/// ChaCha20-Poly1305).
const NONCE_LEN: usize = 12;

/// The AEAD authentication tag length in bytes (128 bits, appended inline by
/// both backends).
const AUTH_TAG_LEN: usize = 16;

/// The fixed wrapper overhead: `[Encrypted tag][algorithm tag][nonce 12 B]`.
/// The 16-byte AEAD tag rides inside `ciphertext_with_tag` so it is not part
/// of the wrapper header.
const WRAPPER_HEADER_LEN: usize = 2 + NONCE_LEN;

/// The constant byte overhead an `Encrypted` wrapper adds on top of the
/// plaintext payload — the wrapper header
/// (`[ENCRYPTED_TAG][algorithm tag][nonce 12 B]`) plus the trailing 16-byte
/// AEAD authentication tag. 14 + 16 = 30 bytes.
///
/// Callers use this when sizing the on-wire bound for a buffer whose
/// plaintext bound is already known: a near-`max_orig_len` plaintext
/// frame inflates by exactly `ENCRYPTED_WRAPPER_OVERHEAD` after
/// encryption, so the on-wire envelope ceiling must allow that slack to
/// round-trip cleanly.
pub const ENCRYPTED_WRAPPER_OVERHEAD: usize = WRAPPER_HEADER_LEN + AUTH_TAG_LEN;

/// Build an encrypted wrapper frame:
/// `[ENCRYPTED_TAG][algorithm tag][nonce: 12 B][ciphertext + 16-B auth]`.
///
/// The 12-byte nonce is generated randomly per encryption; the receiver
/// reads it back off the wire. The AAD is the two-byte
/// `[ENCRYPTED_TAG, algorithm tag]` — binds the wrapper identity and the
/// cipher choice to AEAD authentication so a peer cannot downgrade-swap
/// algorithms without breaking the tag check.
pub fn encode_encrypted_frame(
  algo: EncryptAlgorithm,
  key: &SecretKey,
  plaintext: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
  #[cfg(any(feature = "aes-gcm", feature = "chacha20-poly1305"))]
  let nonce: [u8; NONCE_LEN] = {
    let mut n = [0u8; NONCE_LEN];
    // Recoverable on this fallible path: a failing entropy source surfaces an
    // encryption error rather than aborting the process.
    getrandom::fill(&mut n).map_err(|_| EncryptionError::Entropy)?;
    n
  };
  #[cfg(not(any(feature = "aes-gcm", feature = "chacha20-poly1305")))]
  let nonce: [u8; NONCE_LEN] = {
    // No backend is built in; the encrypt call below will surface
    // UnsupportedAlgorithm before we ever look at `nonce`.
    [0u8; NONCE_LEN]
  };

  let aad = [ENCRYPTED_TAG, algo.tag()];
  let ciphertext = encrypt(algo, key, &nonce, plaintext, &aad)?;
  let mut out = Vec::with_capacity(WRAPPER_HEADER_LEN + ciphertext.len());
  out.push(ENCRYPTED_TAG);
  out.push(algo.tag());
  out.extend_from_slice(&nonce);
  out.extend_from_slice(&ciphertext);
  Ok(out)
}

/// Decode an encrypted wrapper frame back into the original plaintext.
///
/// `max_plaintext_len` is the caller's hard ceiling on the POST-DECRYPT
/// plaintext byte length — gossip MTU on the unreliable path,
/// `max_stream_frame_size` on the reliable path. A frame whose implied
/// plaintext length (ciphertext minus the 16-byte AEAD authentication tag)
/// exceeds it is rejected with [`EncryptionError::Oversize`] BEFORE any
/// AEAD allocation — the ciphertext-bomb guard, parallel to compression's
/// `orig_len` bound.
///
/// The bound is on PLAINTEXT length, not on the wire envelope: the AEAD
/// authentication tag adds [`AUTH_TAG_LEN`] bytes inline so a legitimate
/// near-bound plaintext frame's ciphertext is slightly larger than the
/// plaintext bound. Checking on the post-decrypt length keeps near-MTU
/// plaintext frames round-tripping cleanly.
///
/// The keyring is consulted by variant: only keys whose
/// [`SecretKey::algorithm`] matches the wire algorithm tag are tried; the
/// primary is tried first, then each secondary in insertion order. Result
/// disposition when no key decrypts:
/// - zero variant-matching candidates → [`EncryptionError::NoMatchingKey`]
/// - exactly one candidate, AEAD fails → [`EncryptionError::AuthFailed`]
///   (the AEAD's own verdict; the caller had no alternative attempt)
/// - two or more candidates, all AEAD fail → [`EncryptionError::NoMatchingKey`]
///   (the multi-key fallback was exhausted)
///
/// The driver/coordinator treats all three identically on the gossip path
/// (drop the datagram); the variant distinction is a debugging aid.
pub fn decode_encrypted_frame(
  opts: &EncryptionOptions,
  frame: &[u8],
  max_plaintext_len: usize,
) -> Result<Vec<u8>, EncryptionError> {
  // First pass: just enough to recognise the wrapper and read the algorithm
  // byte. The algorithm-tag check runs BEFORE the full-header length bound
  // so an unknown-algorithm frame surfaces as `UnsupportedAlgorithm`
  // regardless of how short the payload is.
  if frame.len() < 2 || frame[0] != ENCRYPTED_TAG {
    return Err(EncryptionError::MalformedFrame);
  }
  let algo = EncryptAlgorithm::from_tag(frame[1]);
  if let EncryptAlgorithm::Unknown(t) = algo {
    return Err(EncryptionError::UnsupportedAlgorithm(t));
  }
  if frame.len() < WRAPPER_HEADER_LEN + AUTH_TAG_LEN {
    return Err(EncryptionError::MalformedFrame);
  }

  let mut nonce = [0u8; NONCE_LEN];
  nonce.copy_from_slice(&frame[2..2 + NONCE_LEN]);
  let ciphertext = &frame[WRAPPER_HEADER_LEN..];
  // The AEAD authentication tag rides as the trailing `AUTH_TAG_LEN` bytes
  // of `ciphertext`; the post-decrypt plaintext length is therefore
  // `ciphertext.len() - AUTH_TAG_LEN`. Saturate to 0 so a frame with no
  // ciphertext beyond the tag bytes is treated as a zero-length plaintext
  // rather than wrapping the subtract.
  let plaintext_len = ciphertext.len().saturating_sub(AUTH_TAG_LEN);
  if plaintext_len > max_plaintext_len {
    return Err(EncryptionError::Oversize(OversizeCiphertext::new(
      plaintext_len,
      max_plaintext_len,
    )));
  }
  let aad = [ENCRYPTED_TAG, algo.tag()];

  let keyring = match opts.keyring() {
    Some(kr) => kr,
    // Encryption disabled but the frame is encrypted — the decoder cannot
    // possibly decrypt it. Surface as NoMatchingKey (matches the disposition
    // the driver takes on the gossip path: drop the datagram).
    None => return Err(EncryptionError::NoMatchingKey),
  };

  let mut tried = 0usize;
  if keyring.primary_ref().algorithm() == algo {
    tried += 1;
    match decrypt(algo, keyring.primary_ref(), &nonce, ciphertext, &aad) {
      Ok(pt) => return Ok(pt),
      // The primary failed auth; fall through and try the secondaries.
      Err(EncryptionError::AuthFailed) => {}
      Err(other) => return Err(other),
    }
  }
  for sec in keyring.secondaries() {
    if sec.algorithm() != algo {
      continue;
    }
    tried += 1;
    match decrypt(algo, sec, &nonce, ciphertext, &aad) {
      Ok(pt) => return Ok(pt),
      Err(EncryptionError::AuthFailed) => continue,
      Err(other) => return Err(other),
    }
  }
  // Disposition: 0 variant-matching keys is "no matching key in the ring";
  // 1 candidate failing AEAD is the AEAD's own verdict (`AuthFailed`) — the
  // caller has no alternative attempt that could have succeeded, so a
  // distinguishable corruption/wrong-key signal is more useful than a
  // collapsed "no matching key"; 2+ candidates failing is the multi-key
  // fallback exhausted (`NoMatchingKey`). The driver treats all three the
  // same on the gossip path.
  match tried {
    0 | 2.. => Err(EncryptionError::NoMatchingKey),
    1 => Err(EncryptionError::AuthFailed),
  }
}

#[cfg(test)]
mod tests;
