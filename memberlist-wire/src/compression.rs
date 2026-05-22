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

/// A compression or decompression failure.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
  /// The algorithm tag is unknown or its backend feature is not built in.
  #[error("unsupported compression algorithm: tag {0}")]
  UnsupportedAlgorithm(u8),
  /// A backend rejected the bytes (corrupt frame, or compress failure).
  #[error("compression backend failure: {0}")]
  Backend(String),
  /// A compressed frame's declared original length exceeds the caller's
  /// hard maximum — rejected before any buffer is allocated. Carries
  /// `(claimed, max)`.
  #[error("declared decompressed length {} exceeds maximum {}", _0.0, _0.1)]
  OversizeOriginal(OversizeOriginal),
}

/// The `(claimed, max)` length pair carried by
/// [`CompressionError::OversizeOriginal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OversizeOriginal(usize, usize);

impl OversizeOriginal {
  /// The `orig_len` the compressed frame claimed.
  pub const fn claimed(&self) -> usize {
    self.0
  }

  /// The caller's hard maximum.
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
      .map_err(|e| CompressionError::Backend(e.to_string())),
    #[cfg(feature = "zstd")]
    CompressAlgorithm::Zstd => zstd::stream::encode_all(_input, ZSTD_DEFAULT_LEVEL)
      .map_err(|e| CompressionError::Backend(e.to_string())),
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
          .map_err(|e| CompressionError::Backend(e.to_string()))?;
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
        .map_err(|e| CompressionError::Backend(e.to_string()))?;
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
        .map_err(|e| CompressionError::Backend(e.to_string()))?;
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
        .map_err(|e| CompressionError::Backend(e.to_string()))?;
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
        .map_err(|e| CompressionError::Backend(e.to_string()))?;
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
/// A `CompressionOptions` with no algorithm ([`CompressionOptions::disabled`])
/// is the default: every payload is left uncompressed and the codec paths
/// reduce to identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionOptions {
  algorithm: Option<CompressAlgorithm>,
  threshold: usize,
}

impl Default for CompressionOptions {
  fn default() -> Self {
    Self::disabled()
  }
}

impl CompressionOptions {
  /// A disabled configuration — no algorithm, the default threshold. Every
  /// payload is left [`CompressionOutcome::Plain`].
  pub const fn disabled() -> Self {
    Self {
      algorithm: None,
      threshold: DEFAULT_COMPRESSION_THRESHOLD,
    }
  }

  /// The selected algorithm, or `None` when compression is disabled.
  pub const fn algorithm(&self) -> Option<CompressAlgorithm> {
    self.algorithm
  }

  /// The size threshold below which a payload is left uncompressed.
  pub const fn threshold(&self) -> usize {
    self.threshold
  }

  /// Builder: set (or clear) the algorithm.
  pub const fn with_algorithm(mut self, algorithm: Option<CompressAlgorithm>) -> Self {
    self.algorithm = algorithm;
    self
  }

  /// Builder: set the size threshold.
  pub const fn with_threshold(mut self, threshold: usize) -> Self {
    self.threshold = threshold;
    self
  }

  /// Setter: select (or clear) the algorithm in place.
  pub const fn set_algorithm(&mut self, algorithm: Option<CompressAlgorithm>) {
    self.algorithm = algorithm;
  }

  /// Setter: set the size threshold in place.
  pub const fn set_threshold(&mut self, threshold: usize) {
    self.threshold = threshold;
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

use crate::framing::{
  decode_varint_u32, encode_varint_u32, unwrap_transforms, FrameError, MessageTag,
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
    return Err(CompressionError::Backend(
      "not a compressed frame".to_string(),
    ));
  }
  let algo = CompressAlgorithm::from_tag(frame[1]);
  if let CompressAlgorithm::Unknown(tag) = algo {
    return Err(CompressionError::UnsupportedAlgorithm(tag));
  }
  let (orig_len, varint_bytes) = decode_varint_u32(&frame[2..])
    .map_err(|e| CompressionError::Backend(format!("orig_len varint: {e}")))?;
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
    return Err(FrameError::Compression(format!(
      "reliable unit_len {unit_len} exceeds maximum {max_orig_len}"
    )));
  }
  let total = vbytes + unit_len;
  if buf.len() < total {
    return Ok(None);
  }
  let plaintext = unwrap_transforms(&buf[vbytes..total], max_orig_len)?.into_owned();
  Ok(Some((plaintext, total)))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn algorithm_tag_roundtrip() {
    for algo in [
      CompressAlgorithm::Lz4,
      CompressAlgorithm::Snappy,
      CompressAlgorithm::Zstd,
      CompressAlgorithm::Brotli,
    ] {
      assert_eq!(CompressAlgorithm::from_tag(algo.tag()), algo);
    }
  }

  #[test]
  fn algorithm_tags_have_pinned_numeric_values() {
    // The numeric tags are a stable wire contract: a frame compressed by one
    // node must decode on a peer built with a different backend set, so the
    // tag numbering may never silently drift.
    assert_eq!(CompressAlgorithm::Lz4.tag(), 1);
    assert_eq!(CompressAlgorithm::Snappy.tag(), 2);
    assert_eq!(CompressAlgorithm::Zstd.tag(), 3);
    assert_eq!(CompressAlgorithm::Brotli.tag(), 4);
  }

  #[test]
  fn unrecognized_tag_is_unknown() {
    assert_eq!(
      CompressAlgorithm::from_tag(0),
      CompressAlgorithm::Unknown(0)
    );
    assert_eq!(
      CompressAlgorithm::from_tag(200),
      CompressAlgorithm::Unknown(200)
    );
    // An `Unknown` round-trips its carried byte.
    assert_eq!(CompressAlgorithm::Unknown(200).tag(), 200);
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn lz4_roundtrip() {
    let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let packed = compress(CompressAlgorithm::Lz4, &input).expect("lz4 compress");
    let back = decompress(CompressAlgorithm::Lz4, &packed, input.len()).expect("lz4 decompress");
    assert_eq!(back, input);
  }

  #[cfg(feature = "snappy")]
  #[test]
  fn snappy_roundtrip() {
    let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let packed = compress(CompressAlgorithm::Snappy, &input).expect("snappy compress");
    let back =
      decompress(CompressAlgorithm::Snappy, &packed, input.len()).expect("snappy decompress");
    assert_eq!(back, input);
  }

  #[cfg(feature = "zstd")]
  #[test]
  fn zstd_roundtrip() {
    let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let packed = compress(CompressAlgorithm::Zstd, &input).expect("zstd compress");
    let back = decompress(CompressAlgorithm::Zstd, &packed, input.len()).expect("zstd decompress");
    assert_eq!(back, input);
  }

  #[cfg(feature = "brotli")]
  #[test]
  fn brotli_roundtrip() {
    let input = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let packed = compress(CompressAlgorithm::Brotli, &input).expect("brotli compress");
    let back =
      decompress(CompressAlgorithm::Brotli, &packed, input.len()).expect("brotli decompress");
    assert_eq!(back, input);
  }

  #[test]
  fn unknown_algorithm_fails_both_directions() {
    let algo = CompressAlgorithm::Unknown(99);
    assert!(matches!(
      compress(algo, b"data"),
      Err(CompressionError::UnsupportedAlgorithm(_))
    ));
    assert!(matches!(
      decompress(algo, b"data", 4),
      Err(CompressionError::UnsupportedAlgorithm(_))
    ));
  }

  #[test]
  fn below_threshold_payload_is_left_plain() {
    // A 5-byte payload with a 64-byte threshold: never compressed.
    let outcome = apply_compression(CompressAlgorithm::Unknown(0), 64, b"small");
    assert!(matches!(outcome, Ok(CompressionOutcome::Plain)));
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn incompressible_above_threshold_payload_is_left_plain() {
    // Hard-to-shrink bytes: even above threshold the fallback keeps them plain
    // so compression can never inflate a payload.
    let mut input = Vec::new();
    for i in 0u32..512 {
      input.extend_from_slice(&i.to_le_bytes());
    }
    let outcome = apply_compression(CompressAlgorithm::Lz4, 8, &input).expect("no backend error");
    match outcome {
      CompressionOutcome::Plain => {}
      CompressionOutcome::Compressed(packed) => {
        assert!(
          packed.len() < input.len(),
          "Compressed outcome must be smaller"
        );
      }
    }
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn compressible_above_threshold_payload_is_compressed() {
    let input = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_vec();
    let outcome = apply_compression(CompressAlgorithm::Lz4, 8, &input).expect("no backend error");
    match outcome {
      CompressionOutcome::Compressed(packed) => {
        assert!(
          packed.len() < input.len(),
          "highly compressible input must shrink"
        );
      }
      CompressionOutcome::Plain => panic!("compressible input above threshold must compress"),
    }
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn compressed_frame_has_tag_algo_origlen_header() {
    let input = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_vec();
    let packed = compress(CompressAlgorithm::Lz4, &input).expect("compress");
    let frame = encode_compressed_frame(CompressAlgorithm::Lz4, input.len(), &packed);
    assert_eq!(frame[0], COMPRESSED_TAG);
    assert_eq!(frame[1], CompressAlgorithm::Lz4.tag());
    assert_eq!(&frame[frame.len() - packed.len()..], &packed[..]);
    assert!(frame.len() > packed.len());
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn compressed_frame_roundtrips_through_decode() {
    let input = b"the quick brown fox".repeat(16);
    let packed = compress(CompressAlgorithm::Lz4, &input).expect("compress");
    let frame = encode_compressed_frame(CompressAlgorithm::Lz4, input.len(), &packed);
    let max = 1 << 20;
    let back = decode_compressed_frame(&frame, max).expect("decode");
    assert_eq!(back, input);
  }

  #[test]
  fn oversize_orig_len_is_rejected_before_allocation() {
    // Compressed tag, lz4 algo tag, orig_len varint claiming 4 GiB, 1-byte
    // body. The decoder must reject on the `orig_len > max` bound BEFORE
    // allocating — the test passing (not OOM-aborting) is the assertion.
    let mut frame = vec![COMPRESSED_TAG, CompressAlgorithm::Lz4.tag()];
    crate::framing::encode_varint_u32(u32::MAX, &mut frame);
    frame.push(0x00);
    let max = 64 * 1024;
    assert!(matches!(
      decode_compressed_frame(&frame, max),
      Err(CompressionError::OversizeOriginal(_))
    ));
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn lying_small_orig_len_rejects_without_allocating_from_body() {
    // A frame whose wrapper `orig_len` is far SMALLER than the body's true
    // decompressed length. The wrapper length passes the bomb guard
    // (`32 <= 1 MiB`), but the bounded decode into a 32-byte buffer must
    // reject — the backend may never size its allocation from a length
    // embedded in the compressed body.
    let input = b"the quick brown fox jumps over the lazy dog".repeat(2400);
    assert!(
      input.len() > 100 * 1024,
      "input is comfortably over 100 KiB"
    );
    let packed = compress(CompressAlgorithm::Lz4, &input).expect("lz4 compress");
    let frame = encode_compressed_frame(CompressAlgorithm::Lz4, 32, &packed);
    assert!(matches!(
      decode_compressed_frame(&frame, 1 << 20),
      Err(CompressionError::Backend(_))
    ));
  }

  #[test]
  fn unknown_algorithm_frame_fails_decode() {
    let mut frame = vec![COMPRESSED_TAG, 222u8];
    crate::framing::encode_varint_u32(8, &mut frame);
    frame.extend_from_slice(b"whatever");
    assert!(matches!(
      decode_compressed_frame(&frame, 1 << 20),
      Err(CompressionError::UnsupportedAlgorithm(222))
    ));
  }

  #[test]
  fn compression_options_default_is_disabled() {
    let opts = CompressionOptions::disabled();
    assert!(opts.algorithm().is_none());
    let outcome = opts.apply(&[0u8; 4096]).expect("disabled never errors");
    assert!(matches!(outcome, CompressionOutcome::Plain));
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn compression_options_builders_select_algorithm_and_threshold() {
    let opts = CompressionOptions::disabled()
      .with_algorithm(Some(CompressAlgorithm::Lz4))
      .with_threshold(16);
    assert_eq!(opts.algorithm(), Some(CompressAlgorithm::Lz4));
    assert_eq!(opts.threshold(), 16);
    let outcome = opts.apply(&b"A".repeat(256)).expect("backend ok");
    assert!(matches!(outcome, CompressionOutcome::Compressed(_)));
    let outcome = opts.apply(b"AAAA").expect("backend ok");
    assert!(matches!(outcome, CompressionOutcome::Plain));
  }

  #[test]
  fn reliable_unit_plain_roundtrips_when_disabled() {
    let opts = CompressionOptions::disabled();
    let framed = b"the quick brown fox".repeat(4);
    let unit = encode_reliable_unit(&opts, &framed);
    let (back, consumed) = take_reliable_unit(&unit, 1 << 20)
      .expect("decode ok")
      .expect("a complete unit is present");
    assert_eq!(back, framed);
    assert_eq!(consumed, unit.len());
  }

  #[test]
  fn reliable_unit_partial_buffer_returns_none() {
    let opts = CompressionOptions::disabled();
    let framed = b"the quick brown fox".repeat(4);
    let unit = encode_reliable_unit(&opts, &framed);
    let partial = &unit[..unit.len() - 1];
    assert!(take_reliable_unit(partial, 1 << 20)
      .expect("not an error")
      .is_none());
    assert!(take_reliable_unit(&[], 1 << 20)
      .expect("not an error")
      .is_none());
  }

  #[test]
  fn reliable_unit_two_back_to_back_each_extract_with_consumed() {
    let opts = CompressionOptions::disabled();
    let first = b"first-frame-bytes".to_vec();
    let second = b"second-frame".to_vec();
    let mut buf = encode_reliable_unit(&opts, &first);
    buf.extend_from_slice(&encode_reliable_unit(&opts, &second));
    let (a, n1) = take_reliable_unit(&buf, 1 << 20).unwrap().unwrap();
    assert_eq!(a, first);
    let (b, n2) = take_reliable_unit(&buf[n1..], 1 << 20).unwrap().unwrap();
    assert_eq!(b, second);
    assert_eq!(n1 + n2, buf.len());
  }

  #[test]
  fn reliable_unit_len_over_ceiling_is_rejected_before_waiting() {
    let mut buf = Vec::new();
    encode_varint_u32(64 * 1024, &mut buf);
    buf.extend_from_slice(b"only-a-few-bytes-follow");
    assert!(take_reliable_unit(&buf, 1024).is_err());
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn reliable_unit_compressed_roundtrips() {
    let opts = CompressionOptions::disabled()
      .with_algorithm(Some(CompressAlgorithm::Lz4))
      .with_threshold(8);
    let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
    let unit = encode_reliable_unit(&opts, &framed);
    assert!(
      unit.len() < framed.len(),
      "compressible unit must shrink on the wire"
    );
    let (back, consumed) = take_reliable_unit(&unit, 1 << 20).unwrap().unwrap();
    assert_eq!(back, framed);
    assert_eq!(consumed, unit.len());
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn lz4_trailing_junk_in_body_is_rejected() {
    // A compressed body shaped `[valid lz4 block][trailing junk]` is not one
    // clean stream — `decode_compressed_frame` must reject it wholesale even
    // though `orig_len` is honest.
    let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let mut body = compress(CompressAlgorithm::Lz4, &payload).expect("lz4 compress");
    body.extend_from_slice(b"trailing-junk");
    let frame = encode_compressed_frame(CompressAlgorithm::Lz4, payload.len(), &body);
    assert!(matches!(
      decode_compressed_frame(&frame, 1 << 20),
      Err(CompressionError::Backend(_))
    ));
  }

  #[cfg(feature = "snappy")]
  #[test]
  fn snappy_trailing_junk_in_body_is_rejected() {
    let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let mut body = compress(CompressAlgorithm::Snappy, &payload).expect("snappy compress");
    body.extend_from_slice(b"trailing-junk");
    let frame = encode_compressed_frame(CompressAlgorithm::Snappy, payload.len(), &body);
    assert!(matches!(
      decode_compressed_frame(&frame, 1 << 20),
      Err(CompressionError::Backend(_))
    ));
  }

  #[cfg(feature = "zstd")]
  #[test]
  fn zstd_trailing_junk_in_body_is_rejected() {
    let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let mut body = compress(CompressAlgorithm::Zstd, &payload).expect("zstd compress");
    body.extend_from_slice(b"trailing-junk");
    let frame = encode_compressed_frame(CompressAlgorithm::Zstd, payload.len(), &body);
    assert!(matches!(
      decode_compressed_frame(&frame, 1 << 20),
      Err(CompressionError::Backend(_))
    ));
  }

  #[cfg(feature = "brotli")]
  #[test]
  fn brotli_trailing_junk_in_body_is_rejected() {
    let payload = b"the quick brown fox jumps over the lazy dog".repeat(8);
    let mut body = compress(CompressAlgorithm::Brotli, &payload).expect("brotli compress");
    body.extend_from_slice(b"trailing-junk");
    let frame = encode_compressed_frame(CompressAlgorithm::Brotli, payload.len(), &body);
    assert!(matches!(
      decode_compressed_frame(&frame, 1 << 20),
      Err(CompressionError::Backend(_))
    ));
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn reliable_unit_corrupt_inner_wrapper_is_rejected() {
    let opts = CompressionOptions::disabled()
      .with_algorithm(Some(CompressAlgorithm::Lz4))
      .with_threshold(8);
    let framed = b"the quick brown fox jumps over the lazy dog".repeat(16);
    let mut unit = encode_reliable_unit(&opts, &framed);
    // Corrupt the algorithm-tag byte inside the compressed wrapper (the byte
    // immediately after the COMPRESSED_TAG). `decode_compressed_frame` always
    // validates the algorithm tag, so this corruption is always detected
    // regardless of which LZ4 backend is in use.
    //
    // Layout: [unit_len: varint][COMPRESSED_TAG][algo][orig_len: varint][lz4 data]
    // Skip the leading unit_len varint (all bytes with bit-7 set are
    // continuation bytes; the first byte with bit-7 clear terminates it).
    let vbytes = unit.iter().position(|b| b & 0x80 == 0).unwrap() + 1;
    // vbytes points to COMPRESSED_TAG; vbytes+1 is the algorithm tag.
    unit[vbytes + 1] ^= 0xff;
    assert!(take_reliable_unit(&unit, 1 << 20).is_err());
  }
}
