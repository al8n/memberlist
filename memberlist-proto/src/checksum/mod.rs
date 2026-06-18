//! Checksum — a tagged, feature-gated integrity digest over a byte payload.
//!
//! A checksum is a small fixed-size digest computed over the plaintext frame
//! and carried alongside it so a receiver can detect corruption. This module
//! is the reusable transform primitive only: the wrapper-frame layout and the
//! encode/decode pipeline integration live elsewhere.
//!
//! The algorithm tag identifies the backend; each backend is opt-in behind its
//! own feature, and a node that is asked for a tag it was not built with fails
//! cleanly ([`ChecksumError::Disabled`]) rather than panicking. The digest
//! itself is returned on the stack ([`ChecksumDigest`]) — the widest digest is
//! eight bytes, so no heap allocation is needed.

use std::vec::Vec;

use crate::framing::{FrameError, MessageTag};

/// Identifies the checksum backend a digest was produced with. Each backend is
/// opt-in behind its own feature; a node that is handed a tag it was not built
/// with yields [`ChecksumAlgorithm::Unknown`] and fails the operation cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
  /// CRC32 (IEEE). Feature `crc32`, backed by `crc32fast`. 4-byte
  /// digest.
  #[cfg(feature = "crc32")]
  #[cfg_attr(docsrs, doc(cfg(feature = "crc32")))]
  Crc32 = 1,
  /// XXHash32. Feature `xxhash32`, backed by `xxhash-rust`. 4-byte
  /// digest.
  #[cfg(feature = "xxhash32")]
  #[cfg_attr(docsrs, doc(cfg(feature = "xxhash32")))]
  XxHash32 = 2,
  /// XXHash64. Feature `xxhash64`, backed by `xxhash-rust`. 8-byte
  /// digest.
  #[cfg(feature = "xxhash64")]
  #[cfg_attr(docsrs, doc(cfg(feature = "xxhash64")))]
  XxHash64 = 3,
  /// XXHash3 (64-bit). Feature `xxhash3`, backed by `xxhash-rust`.
  /// 8-byte digest.
  #[cfg(feature = "xxhash3")]
  #[cfg_attr(docsrs, doc(cfg(feature = "xxhash3")))]
  XxHash3 = 4,
  /// Murmur3. Feature `murmur3`, backed by `hash32`. 4-byte digest.
  #[cfg(feature = "murmur3")]
  #[cfg_attr(docsrs, doc(cfg(feature = "murmur3")))]
  Murmur3 = 5,
  /// An algorithm tag the local node was not built with.
  Unknown(u8),
}

/// Algorithm wire tags. Stable across builds — a node built with one backend
/// must agree with a peer built with another on the tag numbering.
#[cfg(feature = "crc32")]
const CRC32_TAG: u8 = 1;
#[cfg(feature = "xxhash32")]
const XXHASH32_TAG: u8 = 2;
#[cfg(feature = "xxhash64")]
const XXHASH64_TAG: u8 = 3;
#[cfg(feature = "xxhash3")]
const XXHASH3_TAG: u8 = 4;
#[cfg(feature = "murmur3")]
const MURMUR3_TAG: u8 = 5;

impl ChecksumAlgorithm {
  /// The one-byte wire tag for this algorithm.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    match self {
      #[cfg(feature = "crc32")]
      Self::Crc32 => CRC32_TAG,
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => XXHASH32_TAG,
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => XXHASH64_TAG,
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => XXHASH3_TAG,
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => MURMUR3_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Decode an algorithm from its one-byte wire tag. An unrecognized tag
  /// becomes [`ChecksumAlgorithm::Unknown`] — the operation fails cleanly
  /// downstream rather than panicking.
  #[inline(always)]
  pub const fn from_tag(tag: u8) -> Self {
    match tag {
      #[cfg(feature = "crc32")]
      CRC32_TAG => Self::Crc32,
      #[cfg(feature = "xxhash32")]
      XXHASH32_TAG => Self::XxHash32,
      #[cfg(feature = "xxhash64")]
      XXHASH64_TAG => Self::XxHash64,
      #[cfg(feature = "xxhash3")]
      XXHASH3_TAG => Self::XxHash3,
      #[cfg(feature = "murmur3")]
      MURMUR3_TAG => Self::Murmur3,
      other => Self::Unknown(other),
    }
  }

  /// The digest size in bytes this algorithm produces. `crc32`, `xxhash32`,
  /// and `murmur3` digest to four bytes; `xxhash64` and `xxhash3` to eight.
  /// An [`ChecksumAlgorithm::Unknown`] has no defined size and reports zero.
  #[inline(always)]
  pub const fn digest_size(&self) -> usize {
    match self {
      #[cfg(feature = "crc32")]
      Self::Crc32 => 4,
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => 4,
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => 8,
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => 8,
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => 4,
      Self::Unknown(_) => 0,
    }
  }
}

impl Default for ChecksumAlgorithm {
  /// The default algorithm is CRC32 when its backend is built in, otherwise
  /// the first built-in backend in declaration order. A build with no checksum
  /// backend defaults to `Unknown(CRC32_TAG)` — selecting it then fails with
  /// [`ChecksumError::Disabled`], the same fail-cleanly contract as decoding a
  /// tag whose backend is absent.
  #[inline]
  fn default() -> Self {
    #[cfg(feature = "crc32")]
    {
      Self::Crc32
    }
    #[cfg(all(not(feature = "crc32"), feature = "xxhash32"))]
    {
      Self::XxHash32
    }
    #[cfg(all(
      not(feature = "crc32"),
      not(feature = "xxhash32"),
      feature = "xxhash64"
    ))]
    {
      Self::XxHash64
    }
    #[cfg(all(
      not(feature = "crc32"),
      not(feature = "xxhash32"),
      not(feature = "xxhash64"),
      feature = "xxhash3"
    ))]
    {
      Self::XxHash3
    }
    #[cfg(all(
      not(feature = "crc32"),
      not(feature = "xxhash32"),
      not(feature = "xxhash64"),
      not(feature = "xxhash3"),
      feature = "murmur3"
    ))]
    {
      Self::Murmur3
    }
    #[cfg(not(checksum))]
    {
      Self::Unknown(0)
    }
  }
}

/// A computed checksum digest carried on the stack. The widest digest is eight
/// bytes; [`ChecksumDigest::as_bytes`] borrows exactly the `len` significant
/// bytes for the algorithm that produced it, so callers compare and serialize
/// without a heap allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChecksumDigest {
  bytes: [u8; 8],
  len: usize,
}

impl ChecksumDigest {
  /// Construct a digest from the low `len` bytes of `bytes`. Only the
  /// `pack32` / `pack64` packers (each gated on a built-in backend) call this,
  /// so it is gated on at least one backend being present.
  #[cfg(checksum)]
  #[inline(always)]
  const fn new(bytes: [u8; 8], len: usize) -> Self {
    Self { bytes, len }
  }

  /// The significant digest bytes — four for `crc32`/`xxhash32`/`murmur3`,
  /// eight for `xxhash64`/`xxhash3`, in big-endian order.
  #[inline(always)]
  pub fn as_bytes(&self) -> &[u8] {
    &self.bytes[..self.len]
  }

  /// The digest length in bytes.
  #[inline(always)]
  pub const fn len(&self) -> usize {
    self.len
  }

  /// Whether the digest is empty (only an [`ChecksumAlgorithm::Unknown`]
  /// produces a zero-length digest, and that path errors before construction).
  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }
}

/// A checksum computation or verification failure.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChecksumError {
  /// The algorithm is recognized but its backend feature was not built into
  /// this binary. The algorithm names the missing `checksum-*` feature.
  #[error("checksum algorithm {0} is supported but its backend feature is not enabled")]
  Disabled(ChecksumAlgorithm),
  /// The algorithm tag is not a known checksum algorithm.
  #[error("unknown checksum algorithm: tag {0}")]
  UnknownAlgorithm(u8),
  /// A recomputed digest did not match the expected digest.
  #[error("checksum mismatch")]
  Mismatch,
}

impl core::fmt::Display for ChecksumAlgorithm {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      #[cfg(feature = "crc32")]
      Self::Crc32 => f.write_str("crc32"),
      #[cfg(feature = "xxhash32")]
      Self::XxHash32 => f.write_str("xxhash32"),
      #[cfg(feature = "xxhash64")]
      Self::XxHash64 => f.write_str("xxhash64"),
      #[cfg(feature = "xxhash3")]
      Self::XxHash3 => f.write_str("xxhash3"),
      #[cfg(feature = "murmur3")]
      Self::Murmur3 => f.write_str("murmur3"),
      Self::Unknown(v) => write!(f, "unknown({v})"),
    }
  }
}

/// Compute the checksum of `payload` under `algo`. A pure transform — no I/O.
///
/// Returns [`ChecksumError::Disabled`] when the algorithm is recognized but its
/// backend feature is not built in, and [`ChecksumError::UnknownAlgorithm`] for
/// an [`ChecksumAlgorithm::Unknown`] tag. Digest sizes: `crc32` / `xxhash32` /
/// `murmur3` produce four bytes, `xxhash64` / `xxhash3` produce eight, each in
/// big-endian order.
pub fn digest(algo: ChecksumAlgorithm, _payload: &[u8]) -> Result<ChecksumDigest, ChecksumError> {
  match algo {
    #[cfg(feature = "crc32")]
    ChecksumAlgorithm::Crc32 => {
      let v = crc32fast::hash(_payload);
      Ok(pack32(v))
    }
    #[cfg(feature = "xxhash32")]
    ChecksumAlgorithm::XxHash32 => {
      let v = xxhash_rust::xxh32::xxh32(_payload, 0);
      Ok(pack32(v))
    }
    #[cfg(feature = "xxhash64")]
    ChecksumAlgorithm::XxHash64 => {
      let v = xxhash_rust::xxh64::xxh64(_payload, 0);
      Ok(pack64(v))
    }
    #[cfg(feature = "xxhash3")]
    ChecksumAlgorithm::XxHash3 => {
      let v = xxhash_rust::xxh3::xxh3_64(_payload);
      Ok(pack64(v))
    }
    #[cfg(feature = "murmur3")]
    ChecksumAlgorithm::Murmur3 => {
      use core::hash::Hasher as _;
      let mut hasher = hash32::Murmur3Hasher::default();
      hasher.write(_payload);
      // `hash32::Murmur3Hasher` is a 32-bit hasher whose `finish` zero-extends
      // the 32-bit state into the `u64` return; the low 32 bits are the digest.
      Ok(pack32(hasher.finish() as u32))
    }
    ChecksumAlgorithm::Unknown(tag) => Err(ChecksumError::UnknownAlgorithm(tag)),
  }
}

/// Recompute the checksum of `payload` under `algo` and compare it against
/// `expected`. Returns `Ok(())` when they match, [`ChecksumError::Mismatch`]
/// when they differ, and propagates [`ChecksumError::Disabled`] /
/// [`ChecksumError::UnknownAlgorithm`] from [`digest`].
pub fn verify(
  algo: ChecksumAlgorithm,
  payload: &[u8],
  expected: &[u8],
) -> Result<(), ChecksumError> {
  let got = digest(algo, payload)?;
  if got.as_bytes() == expected {
    Ok(())
  } else {
    Err(ChecksumError::Mismatch)
  }
}

// A checksumed payload is a tagged wrapper frame nesting just outside the
// compression wrapper and inside the encryption wrapper:
//
// ```text
// [Checksumed tag][algorithm tag][digest bytes][payload]
// ```
//
// Unlike the compression wrapper there is no `orig_len` varint — the digest
// length is derived from the algorithm tag (`ChecksumAlgorithm::digest_size`)
// and the payload runs to the end of the frame, so the receiver can locate the
// payload boundary from the tag alone. The digest is computed over `payload`
// (the bytes that follow the digest), so a receiver recomputes it over the
// remaining bytes and rejects a mismatch.

/// The one-byte wrapper tag that prefixes every checksumed frame
/// ([`MessageTag::Checksumed`]).
pub const CHECKSUMED_TAG: u8 = MessageTag::Checksumed as u8;

/// The maximum byte overhead a `Checksumed` wrapper adds on top of its payload:
/// the two-byte header (`[CHECKSUMED_TAG][algorithm tag]`) plus the widest
/// digest (eight bytes, for `xxhash64` / `xxhash3`).
///
/// Checksum is a gossip-plane transform nested *inside* encryption
/// (`message → compress → checksum → encrypt`), so a near-`gossip_mtu` frame
/// that is both checksummed and encrypted inflates the encrypted plaintext —
/// and the on-wire datagram — by up to this much beyond
/// `gossip_mtu + ENCRYPTED_WRAPPER_OVERHEAD`. Callers sizing a gossip receive
/// buffer or an on-wire envelope ceiling reserve this so such a frame
/// round-trips. The constant is the worst case over all algorithms and is
/// always defined (independent of which `checksum-*` backends are built), so a
/// node can receive and cleanly reject a peer's checksummed datagram even when
/// it has no checksum backend of its own.
pub const CHECKSUMED_WRAPPER_OVERHEAD: usize = 2 + 8;

/// Build a checksumed wrapper frame:
/// `[CHECKSUMED_TAG][algorithm tag][digest bytes][payload]`.
///
/// The digest is computed over `payload` under `algo` and carried inline ahead
/// of it; the receiver recomputes it over the trailing payload and rejects a
/// mismatch. Returns the algorithm's error
/// ([`ChecksumError::Disabled`] / [`ChecksumError::UnknownAlgorithm`]) when its
/// backend is not built in, so a misconfigured sender fails rather than
/// emitting an unverifiable frame.
pub fn encode_checksummed_frame(
  algo: ChecksumAlgorithm,
  payload: &[u8],
) -> Result<Vec<u8>, ChecksumError> {
  let d = digest(algo, payload)?;
  let digest_bytes = d.as_bytes();
  let mut out = Vec::with_capacity(2 + digest_bytes.len() + payload.len());
  out.push(CHECKSUMED_TAG);
  out.push(algo.tag());
  out.extend_from_slice(digest_bytes);
  out.extend_from_slice(payload);
  Ok(out)
}

/// Decode a checksumed wrapper frame
/// (`[CHECKSUMED_TAG][algorithm tag][digest bytes][payload]`), verify the
/// carried digest over the trailing payload, and return the payload slice
/// (borrowed from `frame`).
///
/// Rejections:
/// - a slice shorter than the two header bytes, or a wrong leading tag, is
///   [`FrameError::Checksum`] carrying [`ChecksumError::UnknownAlgorithm`] /
///   [`ChecksumError::Disabled`] when the algorithm byte cannot be sized, or a
///   structural error otherwise;
/// - an algorithm tag whose backend is not built in ([`ChecksumError::Disabled`])
///   or is unrecognized ([`ChecksumError::UnknownAlgorithm`]) — a frame whose
///   digest cannot be recomputed is unverifiable and rejected;
/// - a recomputed digest that does not match the carried digest
///   ([`ChecksumError::Mismatch`]).
pub fn decode_checksummed_frame(frame: &[u8]) -> Result<&[u8], FrameError> {
  // [tag][algo] — two header bytes minimum.
  if frame.len() < 2 || frame[0] != CHECKSUMED_TAG {
    // A leading byte that is not the checksum tag is a malformed wrapper; an
    // unknown algorithm byte is the precise reason once the tag is right.
    return Err(FrameError::Checksum(ChecksumError::UnknownAlgorithm(
      frame.first().copied().unwrap_or(0),
    )));
  }
  let algo = ChecksumAlgorithm::from_tag(frame[1]);
  // An unknown algorithm has no defined digest size (it reports zero), so the
  // payload boundary cannot be located: reject before slicing rather than
  // treating the whole remainder as payload under a zero-length digest. A
  // recognized-but-not-built-in algorithm still has a defined `digest_size`
  // (the const is feature-independent), so slicing is correct; the `verify`
  // below then surfaces its `Disabled` error.
  if let ChecksumAlgorithm::Unknown(tag) = algo {
    return Err(FrameError::Checksum(ChecksumError::UnknownAlgorithm(tag)));
  }
  let digest_end = 2 + algo.digest_size();
  if frame.len() < digest_end {
    // The header promises more digest bytes than are present — a truncated /
    // malformed wrapper. A recomputed digest over the (necessarily shorter)
    // remaining bytes could never match, so reject as a mismatch.
    return Err(FrameError::Checksum(ChecksumError::Mismatch));
  }
  let expected = &frame[2..digest_end];
  let payload = &frame[digest_end..];
  verify(algo, payload, expected).map_err(FrameError::Checksum)?;
  Ok(payload)
}

/// The result of running [`ChecksumOptions::apply`] over a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChecksumOutput {
  /// No checksum is configured — the caller emits the payload with no wrapper.
  Plain,
  /// The payload was wrapped; the carried bytes are the full
  /// `[CHECKSUMED_TAG][algorithm tag][digest][payload]` frame.
  Checksumed(Vec<u8>),
}

/// Transport-agnostic checksum configuration handed to each coordinator at
/// construction. Zero `pub` fields — accessor-only.
///
/// A `ChecksumOptions` with no algorithm ([`ChecksumOptions::new`]) is the
/// default: every payload is left unwrapped and the codec paths reduce to
/// identity (the operator opts in by selecting an algorithm). Mirrors
/// [`crate::CompressionOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChecksumOptions {
  algorithm: Option<ChecksumAlgorithm>,
}

impl ChecksumOptions {
  /// A new, disabled configuration — no algorithm. Every payload is left
  /// [`ChecksumOutput::Plain`]. The operator opts in by chaining
  /// `.with_algorithm(algo)` or `.maybe_algorithm(Some(algo))`.
  #[inline(always)]
  pub const fn new() -> Self {
    Self { algorithm: None }
  }

  /// The selected algorithm, or `None` when checksumming is disabled.
  #[inline(always)]
  pub const fn algorithm(&self) -> Option<ChecksumAlgorithm> {
    self.algorithm
  }

  /// Whether a checksum algorithm is configured.
  #[inline(always)]
  pub const fn is_enabled(&self) -> bool {
    self.algorithm.is_some()
  }

  /// Setter: enable checksumming with the given algorithm (present state).
  #[inline(always)]
  pub const fn set_algorithm(&mut self, val: ChecksumAlgorithm) -> &mut Self {
    self.algorithm = Some(val);
    self
  }

  /// Builder: enable checksumming with the given algorithm (present state).
  #[must_use]
  #[inline(always)]
  pub const fn with_algorithm(mut self, val: ChecksumAlgorithm) -> Self {
    self.algorithm = Some(val);
    self
  }

  /// Setter: assign the raw `Option<ChecksumAlgorithm>` wrapper in place.
  /// `None` disables checksumming; `Some(algo)` enables it.
  #[inline(always)]
  pub const fn update_algorithm(&mut self, val: Option<ChecksumAlgorithm>) -> &mut Self {
    self.algorithm = val;
    self
  }

  /// Builder: assign the raw `Option<ChecksumAlgorithm>` wrapper (consuming).
  /// `None` disables checksumming; `Some(algo)` enables it.
  #[must_use]
  #[inline(always)]
  pub const fn maybe_algorithm(mut self, val: Option<ChecksumAlgorithm>) -> Self {
    self.algorithm = val;
    self
  }

  /// Setter: disable checksumming (clear the algorithm).
  #[inline(always)]
  pub const fn clear_algorithm(&mut self) -> &mut Self {
    self.algorithm = None;
    self
  }

  /// Wrap `payload` in a checksumed frame when an algorithm is configured.
  /// When none is set the payload is always [`ChecksumOutput::Plain`].
  /// Surfaces the algorithm's error when its backend is not built in (the
  /// caller fails rather than emitting an unverifiable frame).
  pub fn apply(&self, payload: &[u8]) -> Result<ChecksumOutput, ChecksumError> {
    match self.algorithm {
      Some(algo) => Ok(ChecksumOutput::Checksumed(encode_checksummed_frame(
        algo, payload,
      )?)),
      None => Ok(ChecksumOutput::Plain),
    }
  }
}

/// Pack a 32-bit digest into the big-endian first four bytes of a stack digest.
#[cfg(any(feature = "crc32", feature = "xxhash32", feature = "murmur3"))]
#[inline(always)]
fn pack32(v: u32) -> ChecksumDigest {
  let mut bytes = [0u8; 8];
  bytes[..4].copy_from_slice(&v.to_be_bytes());
  ChecksumDigest::new(bytes, 4)
}

/// Pack a 64-bit digest into the big-endian eight bytes of a stack digest.
#[cfg(any(feature = "xxhash64", feature = "xxhash3"))]
#[inline(always)]
fn pack64(v: u64) -> ChecksumDigest {
  ChecksumDigest::new(v.to_be_bytes(), 8)
}

#[cfg(test)]
mod tests;
