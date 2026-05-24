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
//! The two-byte AAD `[Encrypted tag, algorithm tag]` binds the wrapper
//! identity and the cipher choice to AEAD authentication: a peer cannot
//! downgrade-swap algorithms without breaking the tag check.
//!
//! Stage-in-the-codec stack: encryption nests OUTSIDE compression (compress
//! the plaintext for ratio, then encrypt the result — compressing ciphertext
//! yields no gain). The framing tag-driven unwrap loop strips `Encrypted`
//! first, then `Compressed` (if present), then decodes the inner frame.

/// Identifies the AEAD backend an encrypted frame was produced with. Each
/// backend is opt-in behind its own feature; a node that decodes a tag it was
/// not built with yields [`EncryptAlgorithm::Unknown`] and fails the decode
/// cleanly rather than panicking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncryptAlgorithm {
  /// AES-GCM (96-bit nonce, 128-bit auth tag). The `SecretKey` variant
  /// (`Aes128` / `Aes192` / `Aes256`) picks the AES key length. Feature
  /// `aes-gcm`, backed by the `aes-gcm` crate.
  AesGcm,
  /// ChaCha20-Poly1305 (96-bit nonce, 128-bit auth tag, 256-bit key).
  /// Feature `chacha20-poly1305`, backed by the `chacha20poly1305` crate.
  ChaCha20Poly1305,
  /// An algorithm tag the local node was not built with.
  Unknown(u8),
}

/// Algorithm wire tags. Stable across builds — a node built with one backend
/// must agree with a peer built with another on the tag numbering.
const AES_GCM_TAG: u8 = 1;
const CHACHA20_POLY1305_TAG: u8 = 2;

impl EncryptAlgorithm {
  /// The one-byte wire tag for this algorithm.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    match self {
      Self::AesGcm => AES_GCM_TAG,
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
      AES_GCM_TAG => Self::AesGcm,
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
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SecretKey {
  /// AES-128-GCM (128-bit key).
  Aes128([u8; 16]),
  /// AES-192-GCM (192-bit key).
  Aes192([u8; 24]),
  /// AES-256-GCM (256-bit key).
  Aes256([u8; 32]),
  /// ChaCha20-Poly1305 (256-bit key).
  ChaCha20Poly1305([u8; 32]),
}

impl core::fmt::Debug for SecretKey {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Aes128(_) => write!(f, "SecretKey::Aes128(<redacted>)"),
      Self::Aes192(_) => write!(f, "SecretKey::Aes192(<redacted>)"),
      Self::Aes256(_) => write!(f, "SecretKey::Aes256(<redacted>)"),
      Self::ChaCha20Poly1305(_) => write!(f, "SecretKey::ChaCha20Poly1305(<redacted>)"),
    }
  }
}

impl SecretKey {
  /// The AEAD algorithm this key drives.
  #[inline(always)]
  pub const fn algorithm(&self) -> EncryptAlgorithm {
    match self {
      Self::Aes128(_) | Self::Aes192(_) | Self::Aes256(_) => EncryptAlgorithm::AesGcm,
      Self::ChaCha20Poly1305(_) => EncryptAlgorithm::ChaCha20Poly1305,
    }
  }

  /// The raw key bytes (length 16 / 24 / 32 depending on the variant).
  #[inline(always)]
  pub fn as_bytes(&self) -> &[u8] {
    match self {
      Self::Aes128(k) => k.as_slice(),
      Self::Aes192(k) => k.as_slice(),
      Self::Aes256(k) => k.as_slice(),
      Self::ChaCha20Poly1305(k) => k.as_slice(),
    }
  }

  /// Generate a random AES-128-GCM key.
  #[cfg(feature = "aes-gcm")]
  pub fn random_aes128() -> Self {
    use rand::RngExt;
    let mut k = [0u8; 16];
    rand::rng().fill(&mut k);
    Self::Aes128(k)
  }

  /// Generate a random AES-192-GCM key.
  #[cfg(feature = "aes-gcm")]
  pub fn random_aes192() -> Self {
    use rand::RngExt;
    let mut k = [0u8; 24];
    rand::rng().fill(&mut k);
    Self::Aes192(k)
  }

  /// Generate a random AES-256-GCM key.
  #[cfg(feature = "aes-gcm")]
  pub fn random_aes256() -> Self {
    use rand::RngExt;
    let mut k = [0u8; 32];
    rand::rng().fill(&mut k);
    Self::Aes256(k)
  }

  /// Generate a random ChaCha20-Poly1305 key.
  #[cfg(feature = "chacha20-poly1305")]
  pub fn random_chacha20poly1305() -> Self {
    use rand::RngExt;
    let mut k = [0u8; 32];
    rand::rng().fill(&mut k);
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
#[derive(Debug, thiserror::Error)]
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
}

/// Returns `true` when `key`'s cipher variant matches `algo`. `Unknown`
/// algorithms are accepted (the dispatch returns `UnsupportedAlgorithm` for
/// them); a known algorithm paired with a wrong-cipher key is rejected.
///
/// This is the always-on precheck called at the top of `encrypt` and
/// `decrypt` before the feature-gated `match algo` dispatch.
const fn key_matches_algorithm(algo: EncryptAlgorithm, key: &SecretKey) -> bool {
  match (algo, key) {
    (EncryptAlgorithm::AesGcm, SecretKey::Aes128(_))
    | (EncryptAlgorithm::AesGcm, SecretKey::Aes192(_))
    | (EncryptAlgorithm::AesGcm, SecretKey::Aes256(_)) => true,
    (EncryptAlgorithm::ChaCha20Poly1305, SecretKey::ChaCha20Poly1305(_)) => true,
    (EncryptAlgorithm::Unknown(_), _) => true,
    _ => false,
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
    aes::{
      cipher::{consts::U12, generic_array::GenericArray},
      Aes192,
    },
    Aes128Gcm, Aes256Gcm, AesGcm,
  };

  type Aes192Gcm = AesGcm<Aes192, U12>;

  let nonce_ga = GenericArray::from_slice(nonce);
  let mut buf = plaintext.to_vec();
  match key {
    SecretKey::Aes128(k) => {
      let cipher = Aes128Gcm::new(GenericArray::from_slice(k));
      cipher
        .encrypt_in_place(nonce_ga, aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    SecretKey::Aes192(k) => {
      let cipher = Aes192Gcm::new(GenericArray::from_slice(k));
      cipher
        .encrypt_in_place(nonce_ga, aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    SecretKey::Aes256(k) => {
      let cipher = Aes256Gcm::new(GenericArray::from_slice(k));
      cipher
        .encrypt_in_place(nonce_ga, aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
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
    aes::{
      cipher::{consts::U12, generic_array::GenericArray},
      Aes192,
    },
    Aes128Gcm, Aes256Gcm, AesGcm,
  };

  type Aes192Gcm = AesGcm<Aes192, U12>;

  let nonce_ga = GenericArray::from_slice(nonce);
  let mut buf = ciphertext.to_vec();
  match key {
    SecretKey::Aes128(k) => {
      let cipher = Aes128Gcm::new(GenericArray::from_slice(k));
      cipher
        .decrypt_in_place(nonce_ga, aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    SecretKey::Aes192(k) => {
      let cipher = Aes192Gcm::new(GenericArray::from_slice(k));
      cipher
        .decrypt_in_place(nonce_ga, aad, &mut buf)
        .map_err(|_| EncryptionError::AuthFailed)?;
    }
    SecretKey::Aes256(k) => {
      let cipher = Aes256Gcm::new(GenericArray::from_slice(k));
      cipher
        .decrypt_in_place(nonce_ga, aad, &mut buf)
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
  use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};

  let k = match key {
    SecretKey::ChaCha20Poly1305(k) => k,
    // Precheck in encrypt() guarantees this arm is never reached.
    _ => unreachable!("variant mismatch should be caught by precheck in encrypt()/decrypt()"),
  };
  let cipher = ChaCha20Poly1305::new(Key::from_slice(k));
  let mut buf = plaintext.to_vec();
  cipher
    .encrypt_in_place(Nonce::from_slice(nonce), aad, &mut buf)
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
  use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};

  let k = match key {
    SecretKey::ChaCha20Poly1305(k) => k,
    // Precheck in decrypt() guarantees this arm is never reached.
    _ => unreachable!("variant mismatch should be caught by precheck in encrypt()/decrypt()"),
  };
  let cipher = ChaCha20Poly1305::new(Key::from_slice(k));
  let mut buf = ciphertext.to_vec();
  cipher
    .decrypt_in_place(Nonce::from_slice(nonce), aad, &mut buf)
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
    if self.secondaries.iter().any(|s| *s == key) {
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
    use rand::RngExt;
    let mut n = [0u8; NONCE_LEN];
    rand::rng().fill(&mut n);
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
mod tests {
  use super::*;

  #[test]
  fn algorithm_tag_roundtrip() {
    for algo in [EncryptAlgorithm::AesGcm, EncryptAlgorithm::ChaCha20Poly1305] {
      assert_eq!(EncryptAlgorithm::from_tag(algo.tag()), algo);
      assert_eq!(EncryptAlgorithm::from(u8::from(algo)), algo);
    }
  }

  #[test]
  fn unrecognized_tag_is_unknown() {
    assert_eq!(EncryptAlgorithm::from_tag(0), EncryptAlgorithm::Unknown(0));
    assert_eq!(
      EncryptAlgorithm::from_tag(99),
      EncryptAlgorithm::Unknown(99)
    );
    // `Unknown` round-trips its carried byte.
    assert_eq!(EncryptAlgorithm::Unknown(99).tag(), 99);
  }

  /// Pinned numeric algorithm tags. A change here is a wire-protocol break:
  /// a new-build node would decode a peer's old-build frame to the wrong
  /// algorithm. The pinning is enforced as a unit test so a refactor that
  /// reorders the constants is caught at test time, not at deploy time.
  #[test]
  fn algorithm_tags_have_pinned_numeric_values() {
    assert_eq!(EncryptAlgorithm::AesGcm.tag(), 1);
    assert_eq!(EncryptAlgorithm::ChaCha20Poly1305.tag(), 2);
  }

  #[test]
  fn secret_key_variants_imply_algorithm() {
    let aes128 = SecretKey::Aes128([0u8; 16]);
    let aes192 = SecretKey::Aes192([0u8; 24]);
    let aes256 = SecretKey::Aes256([0u8; 32]);
    let chacha = SecretKey::ChaCha20Poly1305([0u8; 32]);
    assert_eq!(aes128.algorithm(), EncryptAlgorithm::AesGcm);
    assert_eq!(aes192.algorithm(), EncryptAlgorithm::AesGcm);
    assert_eq!(aes256.algorithm(), EncryptAlgorithm::AesGcm);
    assert_eq!(chacha.algorithm(), EncryptAlgorithm::ChaCha20Poly1305);
  }

  #[test]
  fn secret_key_as_bytes_returns_full_key() {
    let aes128 = SecretKey::Aes128([7u8; 16]);
    let aes192 = SecretKey::Aes192([7u8; 24]);
    let aes256 = SecretKey::Aes256([7u8; 32]);
    let chacha = SecretKey::ChaCha20Poly1305([7u8; 32]);
    assert_eq!(aes128.as_bytes().len(), 16);
    assert_eq!(aes192.as_bytes().len(), 24);
    assert_eq!(aes256.as_bytes().len(), 32);
    assert_eq!(chacha.as_bytes().len(), 32);
    assert!(aes128.as_bytes().iter().all(|b| *b == 7));
  }

  /// `Debug` on a `SecretKey` MUST NOT disclose the raw bytes — a single
  /// `{:?}` of a configured `Keyring` / `EncryptionOptions` in a log line would
  /// otherwise dump the symmetric key. The custom impl renders the variant
  /// name with a `<redacted>` placeholder; the byte-leak guard spot-checks
  /// both `0xXX` hex form and the array Debug's decimal form.
  #[test]
  fn secret_key_debug_redacts_raw_bytes() {
    let aes128 = SecretKey::Aes128([0xAB; 16]);
    let aes192 = SecretKey::Aes192([0xCD; 24]);
    let aes256 = SecretKey::Aes256([0xEF; 32]);
    let chacha = SecretKey::ChaCha20Poly1305([0x99; 32]);

    for key in [aes128, aes192, aes256, chacha] {
      let s = format!("{key:?}");
      assert!(s.contains("<redacted>"), "{s:?}");
      // The raw byte sequence must NOT appear in any form. Spot-check that
      // the suspect byte does not appear as a hex literal or as the decimal
      // form `[u8; N]`'s default Debug would emit.
      assert!(!s.contains("0xAB"), "raw byte leak in {s:?}");
      assert!(!s.contains("0xCD"), "raw byte leak in {s:?}");
      assert!(!s.contains("0xEF"), "raw byte leak in {s:?}");
      assert!(!s.contains("0x99"), "raw byte leak in {s:?}");
      // `[u8; N]` default Debug uses decimal in a comma-separated array form
      // (e.g. `[171, 171, ...]` for `[0xAB; 16]`). Check those don't appear.
      assert!(!s.contains("171"), "raw byte (decimal) leak in {s:?}");
      assert!(!s.contains("205"), "raw byte (decimal) leak in {s:?}");
      assert!(!s.contains("239"), "raw byte (decimal) leak in {s:?}");
      assert!(!s.contains("153"), "raw byte (decimal) leak in {s:?}");
    }
  }

  /// The custom `SecretKey::Debug` impl composes through containers that
  /// derive `Debug` field-wise — `Keyring` (and therefore `EncryptionOptions`
  /// once it carries a `Keyring`) renders each `SecretKey` field via the
  /// redacted impl, so the raw key bytes never reach a log line.
  #[test]
  fn keyring_debug_redacts_through_secret_key() {
    let kr = Keyring::with_secondaries(
      SecretKey::Aes256([0xAB; 32]),
      vec![SecretKey::ChaCha20Poly1305([0xCD; 32])],
    );
    let s = format!("{kr:?}");
    assert!(s.contains("<redacted>"), "{s:?}");
    assert!(!s.contains("171"), "raw byte leak through Keyring");
    assert!(!s.contains("205"), "raw byte leak through Keyring");
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn random_aes_constructors_produce_keys_of_the_right_length() {
    assert_eq!(SecretKey::random_aes128().as_bytes().len(), 16);
    assert_eq!(SecretKey::random_aes192().as_bytes().len(), 24);
    assert_eq!(SecretKey::random_aes256().as_bytes().len(), 32);
    // Random keys differ across calls — collision probability is ~2^-128.
    assert_ne!(
      SecretKey::random_aes128().as_bytes(),
      SecretKey::random_aes128().as_bytes()
    );
  }

  #[cfg(feature = "chacha20-poly1305")]
  #[test]
  fn random_chacha20poly1305_constructor_produces_key_of_the_right_length() {
    let key = SecretKey::random_chacha20poly1305();
    assert_eq!(key.as_bytes().len(), 32);
    assert_eq!(key.algorithm(), EncryptAlgorithm::ChaCha20Poly1305);
  }

  #[test]
  fn encryption_error_display_carries_expected_substrings() {
    let e = EncryptionError::AuthFailed;
    assert!(e.to_string().contains("auth"));

    let e = EncryptionError::UnsupportedAlgorithm(99);
    assert!(e.to_string().contains("99"));

    let e = EncryptionError::Oversize(OversizeCiphertext::new(1024, 64));
    let s = e.to_string();
    assert!(s.contains("1024"), "display carries claimed length: {s}");
    assert!(s.contains("64"), "display carries max ceiling: {s}");

    let e = EncryptionError::NoMatchingKey;
    assert!(!e.to_string().is_empty());

    let e = EncryptionError::KeyMismatch;
    assert!(!e.to_string().is_empty());

    let e = EncryptionError::MalformedFrame;
    assert!(!e.to_string().is_empty());

    let e = EncryptionError::EncryptionRequired;
    assert!(!e.to_string().is_empty());
  }

  #[test]
  fn oversize_ciphertext_accessors_round_trip() {
    let o = OversizeCiphertext::new(2048, 1024);
    assert_eq!(o.claimed(), 2048);
    assert_eq!(o.max(), 1024);
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn aes_gcm_aes128_roundtrip() {
    let key = SecretKey::Aes128([1u8; 16]);
    let nonce = [2u8; 12];
    let plaintext = b"hello memberlist gossip";
    let aad = b"\x0d\x01"; // [Encrypted tag = 13, AesGcm tag = 1]
    let ct = encrypt(EncryptAlgorithm::AesGcm, &key, &nonce, plaintext, aad).expect("encrypt");
    assert_eq!(
      ct.len(),
      plaintext.len() + 16,
      "AES-GCM appends a 16-byte tag"
    );
    let pt = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, aad).expect("decrypt");
    assert_eq!(pt, plaintext);
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn aes_gcm_aes192_roundtrip() {
    let key = SecretKey::Aes192([3u8; 24]);
    let nonce = [4u8; 12];
    let plaintext = b"another payload";
    let aad = b"\x0d\x01";
    let ct = encrypt(EncryptAlgorithm::AesGcm, &key, &nonce, plaintext, aad).expect("encrypt");
    let pt = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, aad).expect("decrypt");
    assert_eq!(pt, plaintext);
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn aes_gcm_aes256_roundtrip() {
    let key = SecretKey::Aes256([5u8; 32]);
    let nonce = [6u8; 12];
    let plaintext = b"a third payload to exercise AES-256";
    let aad = b"\x0d\x01";
    let ct = encrypt(EncryptAlgorithm::AesGcm, &key, &nonce, plaintext, aad).expect("encrypt");
    let pt = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, aad).expect("decrypt");
    assert_eq!(pt, plaintext);
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn aes_gcm_wrong_key_fails_auth() {
    let k1 = SecretKey::Aes256([0xAA; 32]);
    let k2 = SecretKey::Aes256([0xBB; 32]);
    let nonce = [7u8; 12];
    let aad = b"\x0d\x01";
    let ct = encrypt(EncryptAlgorithm::AesGcm, &k1, &nonce, b"secret", aad).expect("encrypt");
    let err = decrypt(EncryptAlgorithm::AesGcm, &k2, &nonce, &ct, aad).expect_err("auth must fail");
    assert!(matches!(err, EncryptionError::AuthFailed));
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn aes_gcm_aad_mismatch_fails_auth() {
    let key = SecretKey::Aes256([0xCC; 32]);
    let nonce = [8u8; 12];
    let ct = encrypt(
      EncryptAlgorithm::AesGcm,
      &key,
      &nonce,
      b"protected",
      b"\x0d\x01",
    )
    .expect("encrypt");
    // AAD swapped (e.g. ChaCha20-Poly1305 tag = 2 instead of AES-GCM tag = 1).
    let err = decrypt(EncryptAlgorithm::AesGcm, &key, &nonce, &ct, b"\x0d\x02")
      .expect_err("AAD mismatch must fail auth");
    assert!(matches!(err, EncryptionError::AuthFailed));
  }

  #[cfg(feature = "chacha20-poly1305")]
  #[test]
  fn chacha20poly1305_roundtrip() {
    let key = SecretKey::ChaCha20Poly1305([0x42; 32]);
    let nonce = [9u8; 12];
    let plaintext = b"chacha20-poly1305 payload";
    let aad = b"\x0d\x02";
    let ct = encrypt(
      EncryptAlgorithm::ChaCha20Poly1305,
      &key,
      &nonce,
      plaintext,
      aad,
    )
    .expect("encrypt");
    assert_eq!(
      ct.len(),
      plaintext.len() + 16,
      "ChaCha20-Poly1305 appends a 16-byte tag"
    );
    let pt = decrypt(EncryptAlgorithm::ChaCha20Poly1305, &key, &nonce, &ct, aad).expect("decrypt");
    assert_eq!(pt, plaintext);
  }

  #[cfg(feature = "chacha20-poly1305")]
  #[test]
  fn chacha20poly1305_wrong_key_fails_auth() {
    let k1 = SecretKey::ChaCha20Poly1305([0xAA; 32]);
    let k2 = SecretKey::ChaCha20Poly1305([0xBB; 32]);
    let nonce = [0x11; 12];
    let aad = b"\x0d\x02";
    let ct = encrypt(
      EncryptAlgorithm::ChaCha20Poly1305,
      &k1,
      &nonce,
      b"secret",
      aad,
    )
    .expect("encrypt");
    let err = decrypt(EncryptAlgorithm::ChaCha20Poly1305, &k2, &nonce, &ct, aad)
      .expect_err("auth must fail");
    assert!(matches!(err, EncryptionError::AuthFailed));
  }

  #[test]
  fn key_variant_must_match_algorithm() {
    // An AES key passed to the ChaCha algorithm path is a misuse caught by
    // the key-mismatch branch — surfaced as KeyMismatch, never a panic.
    let key = SecretKey::Aes128([0u8; 16]);
    let err = encrypt(
      EncryptAlgorithm::ChaCha20Poly1305,
      &key,
      &[0u8; 12],
      b"data",
      b"\x0d\x02",
    )
    .expect_err("variant/algo mismatch must err");
    assert!(matches!(err, EncryptionError::KeyMismatch));
  }

  #[test]
  fn unknown_algorithm_fails_both_directions() {
    let algo = EncryptAlgorithm::Unknown(99);
    let key = SecretKey::Aes128([0u8; 16]);
    assert!(matches!(
      encrypt(algo, &key, &[0u8; 12], b"data", b""),
      Err(EncryptionError::UnsupportedAlgorithm(99))
    ));
    assert!(matches!(
      decrypt(algo, &key, &[0u8; 12], b"data", b""),
      Err(EncryptionError::UnsupportedAlgorithm(99))
    ));
  }

  fn k_a() -> SecretKey {
    SecretKey::Aes128([0xAA; 16])
  }
  fn k_b() -> SecretKey {
    SecretKey::Aes256([0xBB; 32])
  }
  fn k_c() -> SecretKey {
    SecretKey::ChaCha20Poly1305([0xCC; 32])
  }

  #[test]
  fn keyring_new_has_only_primary() {
    let kr = Keyring::new(k_a());
    assert_eq!(kr.primary_ref().as_bytes(), k_a().as_bytes());
    assert!(kr.secondaries().is_empty());
  }

  #[test]
  fn keyring_with_secondaries_seeds_them() {
    let kr = Keyring::with_secondaries(k_a(), vec![k_b(), k_c()]);
    assert_eq!(kr.primary_ref().as_bytes(), k_a().as_bytes());
    assert_eq!(kr.secondaries().len(), 2);
  }

  #[test]
  fn keyring_insert_secondary_grows_list() {
    let mut kr = Keyring::new(k_a());
    kr.insert_secondary(k_b());
    kr.insert_secondary(k_c());
    assert_eq!(kr.secondaries().len(), 2);
  }

  #[test]
  fn keyring_insert_secondary_is_no_op_if_already_present() {
    let mut kr = Keyring::with_secondaries(k_a(), vec![k_b()]);
    kr.insert_secondary(k_b());
    assert_eq!(kr.secondaries().len(), 1, "duplicates are not added");
  }

  #[test]
  fn keyring_remove_secondary_drops_the_named_key() {
    let mut kr = Keyring::with_secondaries(k_a(), vec![k_b(), k_c()]);
    kr.remove_secondary(k_b().as_bytes()).expect("remove ok");
    assert_eq!(kr.secondaries().len(), 1);
    assert_eq!(
      kr.secondaries()[0].as_bytes(),
      k_c().as_bytes(),
      "the remaining secondary is k_c"
    );
  }

  #[test]
  fn keyring_remove_primary_errors() {
    let mut kr = Keyring::with_secondaries(k_a(), vec![k_b()]);
    let err = kr
      .remove_secondary(k_a().as_bytes())
      .expect_err("cannot remove primary");
    assert!(matches!(err, KeyringError::IsPrimary));
  }

  #[test]
  fn keyring_remove_unknown_errors() {
    let mut kr = Keyring::new(k_a());
    let err = kr
      .remove_secondary(k_b().as_bytes())
      .expect_err("missing key");
    assert!(matches!(err, KeyringError::NotInRing));
  }

  #[test]
  fn keyring_promote_swaps_primary_and_secondary() {
    let mut kr = Keyring::with_secondaries(k_a(), vec![k_b(), k_c()]);
    kr.promote(k_b().as_bytes()).expect("promote ok");
    assert_eq!(kr.primary_ref().as_bytes(), k_b().as_bytes());
    assert_eq!(kr.secondaries().len(), 2);
    // Old primary lands at index 0 (prepend, not append).
    assert_eq!(
      kr.secondaries()[0].as_bytes(),
      k_a().as_bytes(),
      "old primary lands at index 0 after promote"
    );
    assert_eq!(
      kr.secondaries()[1].as_bytes(),
      k_c().as_bytes(),
      "the un-promoted secondary shifts to index 1"
    );
  }

  #[test]
  fn keyring_promote_primary_is_no_op_ok() {
    let mut kr = Keyring::with_secondaries(k_a(), vec![k_b()]);
    kr.promote(k_a().as_bytes())
      .expect("promote primary is a no-op");
    assert_eq!(kr.primary_ref().as_bytes(), k_a().as_bytes());
    assert_eq!(kr.secondaries().len(), 1);
  }

  #[test]
  fn keyring_promote_unknown_errors() {
    let mut kr = Keyring::new(k_a());
    let err = kr.promote(k_b().as_bytes()).expect_err("missing key");
    assert!(matches!(err, KeyringError::NotInRing));
  }

  #[test]
  fn keyring_allows_mixed_ciphers() {
    let kr = Keyring::with_secondaries(k_a(), vec![k_c()]);
    assert_eq!(kr.primary_ref().algorithm(), EncryptAlgorithm::AesGcm);
    assert_eq!(
      kr.secondaries()[0].algorithm(),
      EncryptAlgorithm::ChaCha20Poly1305
    );
  }

  #[test]
  fn keyring_insert_distinguishes_aes256_from_chacha20_with_same_bytes() {
    // SecretKey::Aes256 and SecretKey::ChaCha20Poly1305 both carry [u8; 32]; the
    // bytes can coincide but the cipher choice must not. PartialEq on the enum
    // compares the variant tag too — so the second insert lands as a distinct key.
    let bytes = [0xFF; 32];
    let aes = SecretKey::Aes256(bytes);
    let chacha = SecretKey::ChaCha20Poly1305(bytes);
    let mut kr = Keyring::new(aes);
    kr.insert_secondary(chacha);
    assert_eq!(
      kr.secondaries().len(),
      1,
      "different-variant same-byte keys are NOT duplicates"
    );
    assert_eq!(
      kr.secondaries()[0].algorithm(),
      EncryptAlgorithm::ChaCha20Poly1305,
      "the chacha secondary survives the insert (it is not collapsed into the AES primary)"
    );
  }

  #[test]
  fn encryption_options_default_is_disabled() {
    let opts = EncryptionOptions::new();
    assert!(opts.keyring().is_none());
    assert!(!opts.is_enabled());
    // `Default::default()` is `new()` by explicit delegation.
    assert!(!EncryptionOptions::default().is_enabled());
    assert!(EncryptionOptions::default().keyring().is_none());
  }

  #[test]
  fn encryption_options_with_keyring_enables() {
    let kr = Keyring::new(SecretKey::Aes128([0u8; 16]));
    let opts = EncryptionOptions::new().with_keyring(kr.clone());
    assert!(opts.is_enabled());
    assert!(opts.keyring().is_some());
    assert_eq!(
      opts.keyring().unwrap().primary_ref().as_bytes(),
      kr.primary_ref().as_bytes()
    );
  }

  #[test]
  fn encryption_options_set_and_clear_keyring() {
    let mut opts = EncryptionOptions::new();
    assert!(!opts.is_enabled());
    opts.set_keyring(Keyring::new(SecretKey::Aes256([1u8; 32])));
    assert!(opts.is_enabled());
    opts.clear_keyring();
    assert!(!opts.is_enabled());
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn encrypted_frame_roundtrip_aes_gcm() {
    let key = SecretKey::Aes256([0x42; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key.clone()));
    let plaintext = b"a memberlist gossip datagram body".repeat(4);
    let frame = encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, &plaintext).expect("encode");
    // [Encrypted tag][algo tag][nonce 12B][ciphertext + 16B tag]
    assert_eq!(
      frame[0], ENCRYPTED_TAG,
      "leading byte is the Encrypted wrapper tag"
    );
    assert_eq!(
      frame[1],
      EncryptAlgorithm::AesGcm.tag(),
      "second byte is the algorithm tag"
    );
    assert_eq!(
      frame.len(),
      2 + 12 + plaintext.len() + 16,
      "wrapper overhead is 30 bytes"
    );
    let back = decode_encrypted_frame(&opts, &frame, 1 << 20).expect("decode");
    assert_eq!(back, plaintext);
  }

  #[cfg(feature = "chacha20-poly1305")]
  #[test]
  fn encrypted_frame_roundtrip_chacha20poly1305() {
    let key = SecretKey::ChaCha20Poly1305([0x9A; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key.clone()));
    let plaintext = b"a chacha-encrypted payload".to_vec();
    let frame =
      encode_encrypted_frame(EncryptAlgorithm::ChaCha20Poly1305, &key, &plaintext).expect("encode");
    assert_eq!(frame[0], ENCRYPTED_TAG);
    assert_eq!(frame[1], EncryptAlgorithm::ChaCha20Poly1305.tag());
    let back = decode_encrypted_frame(&opts, &frame, 1 << 20).expect("decode");
    assert_eq!(back, plaintext);
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn oversize_ciphertext_is_rejected_before_allocation() {
    // A hand-built frame: Encrypted tag, AES-GCM tag, 12-byte nonce, then a
    // payload whose post-decrypt plaintext length is far over the ceiling.
    // The decoder must reject on the `plaintext_len > max` bound BEFORE
    // running AEAD (the test passing rather than OOM-aborting is the
    // assertion). The carried `claimed` is the plaintext length implied by
    // the wrapper, not the raw ciphertext length.
    let key = SecretKey::Aes256([0u8; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key));
    let mut frame = vec![ENCRYPTED_TAG, EncryptAlgorithm::AesGcm.tag()];
    frame.extend_from_slice(&[0u8; 12]);
    // 100 KiB of zeros — ciphertext (100 KiB) - AUTH_TAG_LEN (16) =
    // implied plaintext 100 KiB - 16; ceiling = 64 KiB.
    frame.resize(frame.len() + 100 * 1024, 0);
    let max = 64 * 1024;
    let err = decode_encrypted_frame(&opts, &frame, max).expect_err("oversize must err");
    match err {
      EncryptionError::Oversize(o) => {
        assert_eq!(o.claimed(), 100 * 1024 - AUTH_TAG_LEN);
        assert_eq!(o.max(), max);
      }
      other => panic!("expected Oversize, got {other:?}"),
    }
  }

  #[test]
  fn encrypted_frame_unknown_algorithm_fails_decode() {
    let opts = EncryptionOptions::new();
    let mut frame = vec![ENCRYPTED_TAG, 222u8];
    frame.extend_from_slice(&[0u8; 12]); // nonce
    frame.extend_from_slice(b"trailing"); // body
    let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("unknown algo must err");
    assert!(matches!(err, EncryptionError::UnsupportedAlgorithm(222)));
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn aes_gcm_trailing_junk_in_body_is_rejected() {
    let key = SecretKey::Aes256([0x55; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key.clone()));
    let mut frame =
      encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, b"data").expect("encode");
    // Append a trailing byte; AES-GCM auth must reject (the appended byte
    // either corrupts the tag or runs as un-authenticated overhang).
    frame.push(0xFF);
    let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("trailing junk must err");
    assert!(matches!(err, EncryptionError::AuthFailed));
  }

  #[cfg(feature = "chacha20-poly1305")]
  #[test]
  fn chacha20poly1305_trailing_junk_in_body_is_rejected() {
    let key = SecretKey::ChaCha20Poly1305([0x77; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key.clone()));
    let mut frame =
      encode_encrypted_frame(EncryptAlgorithm::ChaCha20Poly1305, &key, b"data").expect("encode");
    frame.push(0xFF);
    let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("trailing junk must err");
    assert!(matches!(err, EncryptionError::AuthFailed));
  }

  #[cfg(all(feature = "aes-gcm", feature = "chacha20-poly1305"))]
  #[test]
  fn mixed_cipher_keyring_decrypts_chacha_with_chacha_secondary() {
    // Primary AES, secondary ChaCha20-Poly1305. A frame encrypted under the
    // ChaCha secondary must decode (the variant filter selects the matching
    // key; the primary is skipped because its variant does not match the wire
    // algorithm tag).
    let aes_primary = SecretKey::Aes256([0x12; 32]);
    let chacha_sec = SecretKey::ChaCha20Poly1305([0x34; 32]);
    let kr = Keyring::with_secondaries(aes_primary, vec![chacha_sec.clone()]);
    let opts = EncryptionOptions::new().with_keyring(kr);
    let frame = encode_encrypted_frame(
      EncryptAlgorithm::ChaCha20Poly1305,
      &chacha_sec,
      b"protected by the secondary",
    )
    .expect("encode");
    let back = decode_encrypted_frame(&opts, &frame, 1 << 20).expect("decode via secondary");
    assert_eq!(back, b"protected by the secondary");
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn decode_with_no_matching_key_errs() {
    // A frame encrypted under an outside key; the keyring carries two AES
    // candidates (primary + one secondary), both wrong; the variant filter
    // selects both, both fail AEAD, the loop exhausts → NoMatchingKey (the
    // multi-key fallback's "we tried, none worked" verdict). With a single
    // variant-matching candidate the decoder would surface the AEAD's own
    // `AuthFailed` instead — see `aes_gcm_trailing_junk_in_body_is_rejected`.
    let outside = SecretKey::Aes256([0x10; 32]);
    let k_primary = SecretKey::Aes256([0x11; 32]);
    let k_secondary = SecretKey::Aes256([0x22; 32]);
    let kr = Keyring::with_secondaries(k_primary, vec![k_secondary]);
    let opts = EncryptionOptions::new().with_keyring(kr);
    let frame =
      encode_encrypted_frame(EncryptAlgorithm::AesGcm, &outside, b"unrelated").expect("encode");
    let err = decode_encrypted_frame(&opts, &frame, 1 << 20).expect_err("no matching key");
    assert!(matches!(err, EncryptionError::NoMatchingKey));
  }

  #[test]
  fn encrypted_wrapper_overhead_constant_is_correct() {
    // Pinned value — a change is a wire-protocol or sizing-contract break.
    assert_eq!(ENCRYPTED_WRAPPER_OVERHEAD, 30);
    assert_eq!(
      ENCRYPTED_WRAPPER_OVERHEAD,
      WRAPPER_HEADER_LEN + AUTH_TAG_LEN
    );
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn encrypted_gossip_at_mtu_minus_overhead_roundtrips_and_at_mtu_does_too() {
    // A plaintext payload sized `MTU - overhead` and a plaintext payload
    // sized `MTU` both must round-trip when `max_plaintext_len = MTU`:
    // the bound is on POST-DECRYPT plaintext length, not on the ciphertext
    // envelope. Bounding on `ciphertext.len()` would spuriously reject a
    // near-MTU plaintext because the AEAD tag inflates the envelope past
    // `MTU` even though the plaintext is in-bounds.
    let key = SecretKey::Aes256([0x42; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key.clone()));
    const GOSSIP_MTU: usize = 1400;
    for plaintext_len in [GOSSIP_MTU - ENCRYPTED_WRAPPER_OVERHEAD, GOSSIP_MTU] {
      let plaintext = vec![0xCD; plaintext_len];
      let frame =
        encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, &plaintext).expect("encode");
      let back = decode_encrypted_frame(&opts, &frame, GOSSIP_MTU)
        .unwrap_or_else(|e| panic!("plaintext_len={plaintext_len} must round-trip, got {e}"));
      assert_eq!(back, plaintext, "plaintext_len={plaintext_len}");
    }
  }

  #[cfg(feature = "aes-gcm")]
  #[test]
  fn encrypted_one_byte_past_plaintext_max_is_rejected() {
    // Symmetric guard for the new bound: a plaintext sized `max + 1` must
    // be rejected with `Oversize`, with the carried `claimed` equal to the
    // post-decrypt plaintext length (not the on-wire ciphertext length).
    let key = SecretKey::Aes256([0x77; 32]);
    let opts = EncryptionOptions::new().with_keyring(Keyring::new(key.clone()));
    let max = 256;
    let plaintext = vec![0xEF; max + 1];
    let frame = encode_encrypted_frame(EncryptAlgorithm::AesGcm, &key, &plaintext).expect("encode");
    let err = decode_encrypted_frame(&opts, &frame, max).expect_err("over-bound must err");
    match err {
      EncryptionError::Oversize(o) => {
        assert_eq!(
          o.claimed(),
          max + 1,
          "claimed is post-decrypt plaintext length"
        );
        assert_eq!(o.max(), max);
      }
      other => panic!("expected Oversize, got {other:?}"),
    }
  }
}
