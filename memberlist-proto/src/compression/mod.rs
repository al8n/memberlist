//! Compression — a tagged, feature-gated byte transform that wraps the
//! plain-frame / compound codec.
//!
//! A compressed payload is a tagged wrapper frame nesting outside the
//! message and compound framing:
//!
//! ```text
//! [Compressed tag][algorithm tag][orig_len: varint][compressed bytes]
//! ```
//!
//! The algorithm tag identifies the backend; the zstd / brotli compression
//! level is a sender-side choice and is NOT on the wire (decompression is
//! level-agnostic). `orig_len` is the single size authority — every backend
//! decodes into exactly `orig_len` bytes and a produced length that differs is
//! a decode error, so a frame's compressed body can never drive an allocation
//! larger than its declared `orig_len`.

/// Identifies the compression backend a compressed frame
/// was produced with. Each backend is opt-in behind its own feature; a node
/// that decodes a tag it was not built with yields [`CompressAlgorithm::Unknown`]
/// and fails the decode cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressAlgorithm {
  /// LZ4 — fast, low CPU. Feature `lz4`, backed by `lz4_flex`.
  Lz4,
  /// Snappy — fast. Feature `snappy`, backed by `snap`.
  Snappy,
  /// Zstd — high ratio. Feature `zstd`, backed by `zstd`.
  Zstd,
  /// Brotli — high ratio. Feature `brotli`, backed by `brotli`.
  Brotli,
  /// An algorithm tag the local node was not built with.
  Unknown(u8),
}

/// Algorithm wire tags. Stable across builds — a node built with one backend
/// must agree with a peer built with another on the tag numbering.
const LZ4_TAG: u8 = 1;
const SNAPPY_TAG: u8 = 2;
const ZSTD_TAG: u8 = 3;
const BROTLI_TAG: u8 = 4;

impl CompressAlgorithm {
  /// The one-byte wire tag for this algorithm.
  #[inline(always)]
  pub const fn tag(&self) -> u8 {
    match self {
      Self::Lz4 => LZ4_TAG,
      Self::Snappy => SNAPPY_TAG,
      Self::Zstd => ZSTD_TAG,
      Self::Brotli => BROTLI_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Decode an algorithm from its one-byte wire tag. An unrecognized tag
  /// becomes [`CompressAlgorithm::Unknown`] — the decode fails cleanly
  /// downstream rather than panicking.
  #[inline(always)]
  pub const fn from_tag(tag: u8) -> Self {
    match tag {
      LZ4_TAG => Self::Lz4,
      SNAPPY_TAG => Self::Snappy,
      ZSTD_TAG => Self::Zstd,
      BROTLI_TAG => Self::Brotli,
      other => Self::Unknown(other),
    }
  }
}

/// Default zstd compression level — zstd's own default. Sender-side only;
/// never on the wire.
#[cfg(feature = "zstd")]
const ZSTD_DEFAULT_LEVEL: i32 = 0;

/// Default brotli quality — a balanced ratio/CPU choice. Sender-side only.
#[cfg(feature = "brotli")]
const BROTLI_DEFAULT_QUALITY: u32 = 6;

/// Brotli window bits used for both compression and decompression.
#[cfg(feature = "brotli")]
const BROTLI_WINDOW_BITS: u32 = 22;

/// Brotli streaming buffer size.
#[cfg(feature = "brotli")]
const BROTLI_BUFFER_SIZE: usize = 4096;

/// Payload for [`CompressionError::UnitLenExceedsMax`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitLenExceedsMaxInfo {
  unit_len: usize,
  max: usize,
}

impl UnitLenExceedsMaxInfo {
  /// Construct a unit-len-exceeds-max payload.
  #[inline(always)]
  pub const fn new(unit_len: usize, max: usize) -> Self {
    Self { unit_len, max }
  }

  /// The on-wire unit length that was declared.
  #[inline(always)]
  pub const fn unit_len(&self) -> usize {
    self.unit_len
  }

  /// The caller's hard ceiling.
  #[inline(always)]
  pub const fn max(&self) -> usize {
    self.max
  }
}

impl core::fmt::Display for UnitLenExceedsMaxInfo {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(
      f,
      "on-wire unit length {} exceeds the maximum {}",
      self.unit_len, self.max
    )
  }
}

/// A compression or decompression failure.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum CompressionError {
  /// The algorithm tag is unknown or its backend feature is not built in.
  #[error("unsupported compression algorithm: tag {0}")]
  UnsupportedAlgorithm(u8),
  /// A backend rejected the bytes (corrupt frame, or compress failure).
  #[error("compression backend failure: {0}")]
  Backend(std::borrow::Cow<'static, str>),
  /// A compressed frame's declared original length exceeds the caller's
  /// hard maximum — rejected before any buffer is allocated. Carries
  /// `(claimed, max)`.
  #[error("declared decompressed length {} exceeds maximum {}", _0.0, _0.1)]
  OversizeOriginal(OversizeOriginal),
  /// The byte slice does not carry the expected compressed-frame header
  /// (wrong leading tag or shorter than the minimum wrapper size).
  #[error("byte slice is not a valid compressed frame")]
  MalformedFrame,
  /// An on-wire unit length exceeds the caller's hard ceiling. Carries
  /// `(unit_len, max)`.
  #[error("on-wire unit length {} exceeds the maximum {}", _0.unit_len, _0.max)]
  UnitLenExceedsMax(UnitLenExceedsMaxInfo),
}

/// The `(claimed, max)` length pair carried by
/// [`CompressionError::OversizeOriginal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OversizeOriginal(usize, usize);

impl OversizeOriginal {
  /// The `orig_len` the compressed frame claimed.
  #[inline(always)]
  pub const fn claimed(&self) -> usize {
    self.0
  }

  /// The caller's hard maximum.
  #[inline(always)]
  pub const fn max(&self) -> usize {
    self.1
  }
}

/// Compress `input` with `algo`. A pure transform — no I/O. Returns
/// [`CompressionError::UnsupportedAlgorithm`] for an algorithm whose backend
/// feature is not built in.
pub fn compress(algo: CompressAlgorithm, _input: &[u8]) -> Result<Vec<u8>, CompressionError> {
  match algo {
    #[cfg(feature = "lz4")]
    // Raw LZ4 block with no embedded size prefix — the wrapper frame already
    // carries `orig_len`, which is the single decompression size authority.
    CompressAlgorithm::Lz4 => Ok(lz4_flex::compress(_input)),
    #[cfg(feature = "snappy")]
    CompressAlgorithm::Snappy => snap::raw::Encoder::new()
      .compress_vec(_input)
      .map_err(|e| CompressionError::Backend(e.to_string().into())),
    #[cfg(feature = "zstd")]
    CompressAlgorithm::Zstd => zstd::stream::encode_all(_input, ZSTD_DEFAULT_LEVEL)
      .map_err(|e| CompressionError::Backend(e.to_string().into())),
    #[cfg(feature = "brotli")]
    CompressAlgorithm::Brotli => {
      use std::io::Write;
      let mut out = Vec::new();
      {
        let mut w = brotli::CompressorWriter::new(
          &mut out,
          BROTLI_BUFFER_SIZE,
          BROTLI_DEFAULT_QUALITY,
          BROTLI_WINDOW_BITS,
        );
        w.write_all(_input)
          .map_err(|e| CompressionError::Backend(e.to_string().into()))?;
      }
      Ok(out)
    }
    other => Err(CompressionError::UnsupportedAlgorithm(other.tag())),
  }
}

/// Decompress `input` with `algo` into a fresh buffer. `_orig_len` is the
/// decompressed length the wrapper frame declared AND the hard output bound:
/// every backend decodes into exactly `_orig_len` bytes, and a produced length
/// that differs from `_orig_len` is a decode error
/// ([`CompressionError::Backend`]). Bounding the output here — not just the
/// wrapper's declared length at the frame decoder — is the decompression-bomb
/// guard: the backends size their own allocation from lengths embedded in the
/// compressed body, so `_orig_len` must cap the buffer directly rather than
/// merely hint at its capacity. A pure transform — no I/O.
pub fn decompress(
  algo: CompressAlgorithm,
  _input: &[u8],
  _orig_len: usize,
) -> Result<Vec<u8>, CompressionError> {
  match algo {
    #[cfg(feature = "lz4")]
    CompressAlgorithm::Lz4 => {
      let mut out = vec![0u8; _orig_len];
      let n = lz4_flex::decompress_into(_input, &mut out)
        .map_err(|e| CompressionError::Backend(e.to_string().into()))?;
      if n != _orig_len {
        return Err(CompressionError::Backend(
          "lz4: decompressed length does not match the declared original length".into(),
        ));
      }
      Ok(out)
    }
    #[cfg(feature = "snappy")]
    CompressAlgorithm::Snappy => {
      let mut out = vec![0u8; _orig_len];
      let n = snap::raw::Decoder::new()
        .decompress(_input, &mut out)
        .map_err(|e| CompressionError::Backend(e.to_string().into()))?;
      if n != _orig_len {
        return Err(CompressionError::Backend(
          "snappy: decompressed length does not match the declared original length".into(),
        ));
      }
      Ok(out)
    }
    #[cfg(feature = "zstd")]
    CompressAlgorithm::Zstd => {
      // `zstd::bulk::decompress` allocates exactly `_orig_len` bytes and
      // hard-caps the output there — a frame decompressing past `_orig_len`
      // fails (the underlying `ZSTD_decompressDCtx` rejects a too-small
      // destination) rather than growing the buffer.
      let out = zstd::bulk::decompress(_input, _orig_len)
        .map_err(|e| CompressionError::Backend(e.to_string().into()))?;
      if out.len() != _orig_len {
        return Err(CompressionError::Backend(
          "zstd: decompressed length does not match the declared original length".into(),
        ));
      }
      Ok(out)
    }
    #[cfg(feature = "brotli")]
    CompressAlgorithm::Brotli => {
      use std::io::Read;
      let mut decoder = brotli::Decompressor::new(_input, BROTLI_BUFFER_SIZE);
      let mut out = Vec::with_capacity(_orig_len);
      // `.take(_orig_len + 1)` bounds the read so `read_to_end` cannot grow
      // `out` past one byte over `_orig_len`; the exact-length check below
      // then rejects anything that is not exactly `_orig_len`.
      decoder
        .by_ref()
        .take(_orig_len as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::Backend(e.to_string().into()))?;
      if out.len() != _orig_len {
        return Err(CompressionError::Backend(
          "brotli: decompressed length does not match the declared original length".into(),
        ));
      }
      // `brotli::Decompressor` is a streaming `Read` adapter that EOFs at the
      // end of the brotli stream without examining bytes after it, so a body
      // shaped `[valid stream][trailing junk]` would otherwise decode to
      // exactly `_orig_len` and pass the check above. One read past the
      // stream's end forces the decoder to inspect what follows: it yields
      // `Ok(0)` for a body that is exactly one complete stream, and `Err` (or
      // a non-zero count) when unconsumed trailing bytes remain — which must
      // fail the frame.
      let mut probe = [0u8; 1];
      match decoder.read(&mut probe) {
        Ok(0) => Ok(out),
        Ok(_) | Err(_) => Err(CompressionError::Backend(
          "brotli: trailing bytes after the compressed stream".into(),
        )),
      }
    }
    other => Err(CompressionError::UnsupportedAlgorithm(other.tag())),
  }
}

/// The result of running [`apply_compression`] over a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionOutcome {
  /// The payload was left uncompressed — it was below the size threshold, or
  /// compressing it did not produce a smaller result. The caller emits the
  /// original bytes with no wrapper.
  Plain,
  /// The payload was compressed; the carried bytes are the compressed form.
  Compressed(Vec<u8>),
}

/// Decide whether `input` should be compressed under `algo`, applying the
/// `threshold` minimum size and the don't-expand fallback.
///
/// - An `input` shorter than `threshold` is left [`CompressionOutcome::Plain`]
///   — tiny gossip packets do not benefit and the wrapper has overhead.
/// - Otherwise `input` is compressed; if the compressed form is not strictly
///   smaller than `input`, the result is still [`CompressionOutcome::Plain`].
///   Compression can therefore never inflate a payload.
pub fn apply_compression(
  algo: CompressAlgorithm,
  threshold: usize,
  input: &[u8],
) -> Result<CompressionOutcome, CompressionError> {
  if input.len() < threshold {
    return Ok(CompressionOutcome::Plain);
  }
  let packed = compress(algo, input)?;
  if packed.len() < input.len() {
    Ok(CompressionOutcome::Compressed(packed))
  } else {
    Ok(CompressionOutcome::Plain)
  }
}

/// The default size threshold below which a payload is not compressed.
const DEFAULT_COMPRESSION_THRESHOLD: usize = 512;

/// Transport-agnostic compression configuration handed to each coordinator at
/// construction. Zero `pub` fields — accessor-only.
///
/// A `CompressionOptions` with no algorithm ([`CompressionOptions::new`])
/// is the default: every payload is left uncompressed and the codec paths
/// reduce to identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionOptions {
  algorithm: Option<CompressAlgorithm>,
  threshold: usize,
}

impl Default for CompressionOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl CompressionOptions {
  /// A new, disabled configuration — no algorithm, the default threshold.
  /// Every payload is left [`CompressionOutcome::Plain`]. The operator opts in
  /// by chaining `.with_algorithm(algo)` or `.maybe_algorithm(Some(algo))`.
  #[inline(always)]
  pub const fn new() -> Self {
    Self {
      algorithm: None,
      threshold: DEFAULT_COMPRESSION_THRESHOLD,
    }
  }

  /// The selected algorithm, or `None` when compression is disabled.
  #[inline(always)]
  pub const fn algorithm(&self) -> Option<CompressAlgorithm> {
    self.algorithm
  }

  /// The size threshold below which a payload is left uncompressed.
  #[inline(always)]
  pub const fn threshold(&self) -> usize {
    self.threshold
  }

  /// Setter: enable compression with the given algorithm (present state).
  #[inline(always)]
  pub const fn set_algorithm(&mut self, val: CompressAlgorithm) -> &mut Self {
    self.algorithm = Some(val);
    self
  }

  /// Builder: enable compression with the given algorithm (present state).
  #[must_use]
  #[inline(always)]
  pub const fn with_algorithm(mut self, val: CompressAlgorithm) -> Self {
    self.algorithm = Some(val);
    self
  }

  /// Setter: assign the raw `Option<CompressAlgorithm>` wrapper in place.
  /// `None` disables compression; `Some(algo)` enables it.
  #[inline(always)]
  pub const fn update_algorithm(&mut self, val: Option<CompressAlgorithm>) -> &mut Self {
    self.algorithm = val;
    self
  }

  /// Builder: assign the raw `Option<CompressAlgorithm>` wrapper (consuming).
  /// `None` disables compression; `Some(algo)` enables it.
  #[must_use]
  #[inline(always)]
  pub const fn maybe_algorithm(mut self, val: Option<CompressAlgorithm>) -> Self {
    self.algorithm = val;
    self
  }

  /// Setter: disable compression (clear the algorithm).
  #[inline(always)]
  pub const fn clear_algorithm(&mut self) -> &mut Self {
    self.algorithm = None;
    self
  }

  /// Builder: set the size threshold.
  #[must_use]
  #[inline(always)]
  pub const fn with_threshold(mut self, threshold: usize) -> Self {
    self.threshold = threshold;
    self
  }

  /// Setter: set the size threshold in place.
  #[inline(always)]
  pub const fn set_threshold(&mut self, threshold: usize) -> &mut Self {
    self.threshold = threshold;
    self
  }

  /// Run the configured compression over `payload`. When no algorithm is set
  /// the payload is always [`CompressionOutcome::Plain`]; otherwise this
  /// applies the threshold + don't-expand fallback via [`apply_compression`].
  pub fn apply(&self, payload: &[u8]) -> Result<CompressionOutcome, CompressionError> {
    match self.algorithm {
      Some(algo) => apply_compression(algo, self.threshold, payload),
      None => Ok(CompressionOutcome::Plain),
    }
  }
}

#[cfg(not(feature = "std"))]
use std::vec::Vec;
// `ToString` only feeds the backend error paths; lz4 is the only no_std backend.
#[cfg(all(not(feature = "std"), feature = "lz4"))]
use std::string::ToString;

use crate::framing::{
  FrameError, MessageTag, decode_varint_u32, encode_varint_u32, unwrap_transforms,
};

/// The one-byte wrapper tag that prefixes every compressed frame
/// ([`MessageTag::Compressed`]).
pub const COMPRESSED_TAG: u8 = MessageTag::Compressed as u8;

/// Build a compressed wrapper frame:
/// `[COMPRESSED_TAG][algorithm tag][orig_len varint][compressed bytes]`.
///
/// `orig_len` is the decompressed length of the original payload; the decoder
/// uses it to bound and size the decompression buffer.
pub fn encode_compressed_frame(
  algo: CompressAlgorithm,
  orig_len: usize,
  compressed: &[u8],
) -> Vec<u8> {
  let mut out = Vec::with_capacity(2 + 5 + compressed.len());
  out.push(COMPRESSED_TAG);
  out.push(algo.tag());
  encode_varint_u32(orig_len as u32, &mut out);
  out.extend_from_slice(compressed);
  out
}

/// Decode a compressed wrapper frame
/// (`[COMPRESSED_TAG][algorithm tag][orig_len varint][compressed bytes]`)
/// back into the original payload.
///
/// `max_orig_len` is the caller's hard ceiling on the decompressed length —
/// the UDP max-packet-size on the gossip path, the reliable max-frame-size on
/// the reliable path. A frame whose declared `orig_len` exceeds it is rejected
/// with [`CompressionError::OversizeOriginal`] BEFORE any buffer is allocated
/// — the decompression-bomb guard.
pub fn decode_compressed_frame(
  frame: &[u8],
  max_orig_len: usize,
) -> Result<Vec<u8>, CompressionError> {
  // [tag][algo] — two header bytes minimum.
  if frame.len() < 2 || frame[0] != COMPRESSED_TAG {
    return Err(CompressionError::MalformedFrame);
  }
  let algo = CompressAlgorithm::from_tag(frame[1]);
  if let CompressAlgorithm::Unknown(tag) = algo {
    return Err(CompressionError::UnsupportedAlgorithm(tag));
  }
  let (orig_len, varint_bytes) = decode_varint_u32(&frame[2..])
    .map_err(|e| CompressionError::Backend(format!("orig_len varint: {e}").into()))?;
  let orig_len = orig_len as usize;
  // Decompression-bomb guard — bound `orig_len` BEFORE allocating.
  if orig_len > max_orig_len {
    return Err(CompressionError::OversizeOriginal(OversizeOriginal(
      orig_len,
      max_orig_len,
    )));
  }
  let body = &frame[2 + varint_bytes..];
  decompress(algo, body, orig_len)
}

/// Encode one reliable-exchange unit for a byte-stream transport: a
/// length-delimited frame `[unit_len: varint][payload]` where `payload` is a
/// `[Compressed]` wrapper (compression won) or the plain bytes (disabled /
/// don't-expand). The length prefix makes the unit self-delimiting on a stream
/// that does not preserve write/read boundaries.
///
/// The wrapped payload is guaranteed never to exceed `framed`: the
/// `[CompressionOutcome::Compressed]` branch wraps the compressed bytes in a
/// `[Compressed]` frame whose header can push the wrapped length past `framed`
/// even when the raw compressed bytes are smaller, so the wrapped payload is
/// emitted only when it is shorter than `framed` — otherwise `framed` is sent
/// plain. `payload.len() <= framed.len()` therefore holds for every outcome,
/// so a unit's `unit_len` never exceeds the size of the bytes it carries and
/// compression can never inflate a reliable unit.
pub fn encode_reliable_unit(opts: &CompressionOptions, framed: &[u8]) -> Vec<u8> {
  let payload = match opts.apply(framed) {
    Ok(CompressionOutcome::Compressed(packed)) => {
      let wrapped = encode_compressed_frame(
        opts
          .algorithm()
          .expect("a Compressed outcome implies an algorithm is set"),
        framed.len(),
        &packed,
      );
      // The wrapper header (tag + algorithm + `orig_len` varint) is overhead
      // on top of the raw compressed bytes; if it pushes the wrapped frame to
      // `framed`'s size or larger, send `framed` plain so the unit can never
      // inflate. The receiver's `unwrap_transforms` passes a non-wrapper
      // buffer through unchanged.
      if wrapped.len() < framed.len() {
        wrapped
      } else {
        framed.to_vec()
      }
    }
    // Plain outcome, or a backend compress error: emit the framed bytes
    // uncompressed. The uncompressed payload is always valid and the receiver's
    // `unwrap_transforms` passes a non-wrapper buffer through.
    _ => framed.to_vec(),
  };
  let mut out = Vec::with_capacity(5 + payload.len());
  encode_varint_u32(payload.len() as u32, &mut out);
  out.extend_from_slice(&payload);
  out
}

/// Try to take one complete reliable unit from the front of `buf`.
///
/// `Ok(Some((plaintext, consumed)))` when a full `[unit_len][payload]` is
/// present — the payload is run through the transform stack
/// ([`unwrap_transforms`]), bomb-guarded by `max_orig_len`, and `consumed` is
/// the byte count to drain from the front of the accumulation buffer.
/// `Ok(None)` when more bytes are needed (a truncated leading varint or a
/// `unit_len` not yet fully arrived). `Err` on a corrupt unit or a `unit_len`
/// over the ceiling.
pub fn take_reliable_unit(
  buf: &[u8],
  max_orig_len: usize,
) -> Result<Option<(Vec<u8>, usize)>, FrameError> {
  let (unit_len, vbytes) = match decode_varint_u32(buf) {
    Ok(v) => v,
    // A truncated/empty leading varint just means "need more bytes".
    Err(FrameError::Incomplete(..)) | Err(FrameError::Empty) => return Ok(None),
    Err(e) => return Err(e),
  };
  let unit_len = unit_len as usize;
  // Bound the on-wire unit size BEFORE waiting for it — caps accumulation
  // growth so a malicious huge `unit_len` cannot pin unbounded memory.
  if unit_len > max_orig_len {
    return Err(FrameError::Compression(
      CompressionError::UnitLenExceedsMax(UnitLenExceedsMaxInfo::new(unit_len, max_orig_len)),
    ));
  }
  let total = vbytes + unit_len;
  if buf.len() < total {
    return Ok(None);
  }
  let plaintext = unwrap_transforms(&buf[vbytes..total], max_orig_len)?.into_owned();
  Ok(Some((plaintext, total)))
}

/// Encryption-aware reliable-unit encode. Outbound the codec stack is:
/// (1) compress (if `compression` is enabled and the result shrank);
/// (2) encrypt (if `encryption` is enabled);
/// (3) prepend `[unit_len: varint]`.
///
/// The on-wire byte order is `[unit_len][Encrypted[[Compressed][frame]]]`
/// when both transforms win, `[unit_len][Encrypted[frame]]` when compression
/// is disabled or did not shrink, `[unit_len][Compressed[frame]]` when only
/// compression is enabled, and `[unit_len][frame]` when both are disabled
/// (byte-identical to the encryption-unaware [`encode_reliable_unit`]).
///
/// The reliable plane carries no checksum transform: stream transports
/// provide their own end-to-end integrity, so corruption detection is an
/// unreliable-plane (connectionless datagram) concern and lives on the gossip
/// path only.
///
/// An encryption backend error (typically
/// [`crate::encryption::EncryptionError::UnsupportedAlgorithm`] for a key
/// whose backend was not built into this binary) is surfaced as `Err` so
/// callers fail the exchange — emitting plaintext on an encrypted-cluster
/// path would silently bypass authentication.
pub fn encode_reliable_unit_with_encryption(
  compression: &CompressionOptions,
  encryption: &crate::encryption::EncryptionOptions,
  framed: &[u8],
) -> Result<Vec<u8>, crate::encryption::EncryptionError> {
  // Step 1: compression — yields a `[Compressed]` wrapper or the plain bytes.
  let compressed_or_plain: Vec<u8> = match compression.apply(framed) {
    Ok(CompressionOutcome::Compressed(packed)) => {
      let wrapped = encode_compressed_frame(
        compression
          .algorithm()
          .expect("a Compressed outcome implies an algorithm is set"),
        framed.len(),
        &packed,
      );
      // Don't-expand fallback: if the `[Compressed]` wrapper is not smaller
      // than the raw `framed`, drop back to plain. The wrapper header is
      // overhead — same rule the encryption-unaware path applies.
      if wrapped.len() < framed.len() {
        wrapped
      } else {
        framed.to_vec()
      }
    }
    // Plain outcome, or a backend compress error: emit the framed bytes
    // uncompressed. A non-wrapper buffer passes through the receiver's
    // `unwrap_transforms_with_encryption` unchanged.
    _ => framed.to_vec(),
  };

  // Step 2: encryption (if enabled). Encrypt the result of step 1, producing
  // an `[Encrypted]` wrapper. A backend error here (e.g. a primary key whose
  // backend feature was not built in) is fatal: silently emitting plaintext
  // would let an encrypted-cluster exchange go out unauthenticated.
  let payload: Vec<u8> = match encryption.keyring() {
    Some(kr) => {
      let key = kr.primary_ref();
      let algo = key.algorithm();
      crate::encryption::encode_encrypted_frame(algo, key, &compressed_or_plain)?
    }
    None => compressed_or_plain,
  };

  // Step 3: length-delimit.
  let mut out = Vec::with_capacity(5 + payload.len());
  encode_varint_u32(payload.len() as u32, &mut out);
  out.extend_from_slice(&payload);
  Ok(out)
}

/// Encryption-aware reliable-unit decode. Inbound the codec stack is:
/// (1) strip `[unit_len: varint]`; (2) [`unwrap_transforms_with_encryption`],
/// which strips `Encrypted` first (if present), then `Compressed` (if
/// present), then returns the inner bytes. Bomb-guarded by `max_orig_len`
/// on every wrapper.
///
/// The reliable encoder emits no `Checksumed` wrapper — checksums are an
/// unreliable-plane concern — so that layer is never present on this path.
/// The shared [`unwrap_transforms_with_encryption`] still recognizes it
/// defensively, but a well-formed reliable unit will not exercise that arm.
pub fn take_reliable_unit_with_encryption(
  buf: &[u8],
  encryption: &crate::encryption::EncryptionOptions,
  max_orig_len: usize,
) -> Result<Option<(Vec<u8>, usize)>, FrameError> {
  use crate::framing::unwrap_transforms_with_encryption;
  let (unit_len, vbytes) = match decode_varint_u32(buf) {
    Ok(v) => v,
    // A truncated/empty leading varint just means "need more bytes".
    Err(FrameError::Incomplete(..)) | Err(FrameError::Empty) => return Ok(None),
    Err(e) => return Err(e),
  };
  let unit_len = unit_len as usize;
  // The on-wire envelope ceiling. `max_orig_len` is the plaintext bound;
  // when encryption is enabled the wrapper inflates the payload by a fixed
  // `ENCRYPTED_WRAPPER_OVERHEAD` (header + nonce + AEAD tag), so a
  // legitimate near-`max_orig_len` plaintext frame's wrapped `unit_len`
  // exceeds `max_orig_len` by that much. Allow the slack so near-bound
  // plaintext frames round-trip. The post-decrypt plaintext is still
  // bounded by `unwrap_transforms_with_encryption` (which calls
  // `decode_encrypted_frame` with `max_orig_len` as the plaintext ceiling).
  let effective_unit_max = if encryption.is_enabled() {
    max_orig_len.saturating_add(crate::encryption::ENCRYPTED_WRAPPER_OVERHEAD)
  } else {
    max_orig_len
  };
  // Bound the on-wire unit size BEFORE waiting for it — caps accumulation
  // growth so a malicious huge `unit_len` cannot pin unbounded memory.
  if unit_len > effective_unit_max {
    return Err(FrameError::Compression(
      CompressionError::UnitLenExceedsMax(UnitLenExceedsMaxInfo::new(unit_len, effective_unit_max)),
    ));
  }
  let total = vbytes + unit_len;
  if buf.len() < total {
    return Ok(None);
  }
  let plaintext =
    unwrap_transforms_with_encryption(&buf[vbytes..total], max_orig_len, encryption)?.into_owned();
  Ok(Some((plaintext, total)))
}

#[cfg(test)]
mod tests;
