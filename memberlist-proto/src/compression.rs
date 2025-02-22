use std::borrow::Cow;

use super::{Data, DataRef, DecodeError, EncodeError, WireType};

#[cfg(feature = "brotli")]
macro_rules! num_to_enum {
  (
    $(#[$meta:meta])*
    $name:ident($inner:ident in [$min:expr, $max:literal]):$exp:literal:$short:literal {
      $(
        $(#[$value_meta:meta])*
        $value:literal
      ), +$(,)?
    }
  ) => {
    paste::paste! {
      $(#[$meta])*
      #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display, derive_more::IsVariant)]
      #[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
      #[non_exhaustive]
      #[repr($inner)]
      pub enum $name {
        $(
          #[doc = $exp " " $value "."]
          $(#[$value_meta])*
          [< $short:camel $value >] = $value,
        )*
      }

      impl $name {
        /// Returns the maximum value.
        #[inline]
        pub const fn max() -> Self {
          Self::[< $short:camel $max >]
        }

        /// Returns the minimum value.
        #[inline]
        pub const fn min() -> Self {
          Self::[< $short:camel $min >]
        }
      }

      impl $name {
        #[allow(unused_comparisons)]
        pub(super) const fn [< from_ $inner>](value: $inner) -> Self {
          match value {
            $(
              $value => Self::[< $short:camel $value >],
            )*
            val if val > $max => Self::[< $short:camel $max >],
            val if val < $min => Self::[< $short:camel $min >],
            _ => Self::[< $short:camel $max >],
          }
        }
      }

      impl From<$inner> for $name {
        fn from(value: $inner) -> Self {
          Self::from_u8(value as $inner)
        }
      }

      impl From<$name> for $inner {
        fn from(value: $name) -> Self {
          value as $inner
        }
      }

      #[cfg(feature = "serde")]
      const _: () = {
        use serde::{Deserialize, Serialize};

        impl Serialize for $name {
          fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
          where
            S: serde::Serializer,
          {
            serializer.[< serialize_ $inner>](*self as $inner)
          }
        }

        impl<'de> Deserialize<'de> for $name {
          fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
          where
            D: serde::Deserializer<'de> {
            <$inner>::deserialize(deserializer).map(Self::[< from_ $inner >])
          }
        }
      };
    }
  };
}

const ZSTD_TAG: u8 = 1;
const BROTLI_TAG: u8 = 2;
const LZ4_TAG: u8 = 3;
const SNAPPY_TAG: u8 = 4;

#[cfg(feature = "brotli")]
mod brotli_impl;
mod error;
mod from_str;
#[cfg(feature = "zstd")]
mod zstd_impl;

#[cfg(feature = "brotli")]
pub use brotli_impl::*;
pub use error::*;
pub use from_str::*;

#[cfg(feature = "zstd")]
pub use zstd_impl::*;

#[cfg(any(feature = "quickcheck", test))]
mod quickcheck_impl;

#[cfg(feature = "brotli")]
const BROTLI_BUFFER_SIZE: usize = 4096;

#[cfg(feature = "brotli")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn brotli_compress_error(err: std::io::Error) -> Self {
      Self::Compress(CompressError::Brotli(err))
    }

    #[inline]
    const fn brotli_decompress_error(err: std::io::Error) -> Self {
      Self::Decompress(DecompressError::Brotli(err))
    }
  }
};

#[cfg(feature = "snappy")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn snappy_compress_error(err: snap::Error) -> Self {
      Self::Compress(CompressError::Snappy(err))
    }

    #[inline]
    const fn snappy_decompress_error(err: snap::Error) -> Self {
      Self::Decompress(DecompressError::Snappy(err))
    }
  }
};

#[cfg(feature = "lz4")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn lz4_decompress_error(err: lz4_flex::block::DecompressError) -> Self {
      Self::Decompress(DecompressError::Lz4(err))
    }

    #[inline]
    const fn lz4_compress_error(err: lz4_flex::block::CompressError) -> Self {
      Self::Compress(CompressError::Lz4(err))
    }
  }
};

#[cfg(feature = "zstd")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn zstd_compress_error(err: std::io::Error) -> Self {
      Self::Compress(CompressError::Zstd(err))
    }

    #[inline]
    const fn zstd_decompress_error(err: std::io::Error) -> Self {
      Self::Decompress(DecompressError::Zstd(err))
    }
  }
};

/// The compressioned algorithm used to compression the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display)]
#[non_exhaustive]
pub enum CompressAlgorithm {
  /// Brotli
  #[display("brotli{_0}")]
  #[cfg(feature = "brotli")]
  #[cfg_attr(docsrs, doc(cfg(feature = "brotli")))]
  Brotli(BrotliAlgorithm),
  /// LZ4
  #[display("lz4")]
  #[cfg(feature = "lz4")]
  #[cfg_attr(docsrs, doc(cfg(feature = "lz4")))]
  Lz4,
  /// Snappy
  #[display("snappy")]
  #[cfg(feature = "snappy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "snappy")))]
  Snappy,
  /// Zstd
  #[display("zstd({_0})")]
  #[cfg(feature = "zstd")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
  Zstd(ZstdCompressionLevel),
  /// Unknwon compressioned algorithm
  #[display("unknown({_0})")]
  Unknown(u8),
}

#[cfg(any(feature = "quickcheck", test))]
const _: () = {
  use quickcheck::Arbitrary;

  impl CompressAlgorithm {
    const MAX: u8 = SNAPPY_TAG;
    const MIN: u8 = ZSTD_TAG;
  }

  impl Arbitrary for CompressAlgorithm {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
      let val = (u8::arbitrary(g) % Self::MAX) + Self::MIN;
      match val {
        #[cfg(feature = "zstd")]
        ZSTD_TAG => Self::Zstd(ZstdCompressionLevel::arbitrary(g)),
        #[cfg(feature = "brotli")]
        BROTLI_TAG => Self::Brotli(BrotliAlgorithm::arbitrary(g)),
        #[cfg(feature = "lz4")]
        LZ4_TAG => Self::Lz4,
        #[cfg(feature = "snappy")]
        SNAPPY_TAG => Self::Snappy,
        _ => unreachable!(),
      }
    }
  }
};

impl Default for CompressAlgorithm {
  fn default() -> Self {
    cfg_if::cfg_if! {
      if #[cfg(feature = "snappy")] {
        Self::Snappy
      } else if #[cfg(feature = "lz4")] {
        Self::Lz4
      } else if #[cfg(feature = "brotli")] {
        Self::Brotli(BrotliAlgorithm::default())
      } else if #[cfg(feature = "zstd")] {
        Self::Zstd(ZstdCompressionLevel::default())
      } else {
        Self::Unknown(u8::MAX)
      }
    }
  }
}

impl CompressAlgorithm {
  /// Decompresses the given buffer.
  pub fn decompress_to(&self, src: &[u8], dst: &mut [u8]) -> Result<usize, CompressionError> {
    match self {
      #[cfg(feature = "brotli")]
      Self::Brotli(_) => {
        let mut reader = brotli::Decompressor::new(src, BROTLI_BUFFER_SIZE);
        std::io::copy(&mut reader, &mut std::io::Cursor::new(dst))
          .map(|bytes| bytes as usize)
          .map_err(CompressionError::brotli_decompress_error)
      }
      #[cfg(feature = "lz4")]
      Self::Lz4 => {
        lz4_flex::decompress_into(src, dst).map_err(CompressionError::lz4_decompress_error)
      }
      #[cfg(feature = "snappy")]
      Self::Snappy => snap::raw::Decoder::new()
        .decompress(src, dst)
        .map_err(CompressionError::snappy_decompress_error),
      #[cfg(feature = "zstd")]
      Self::Zstd(_) => {
        let mut decoder = zstd::Decoder::new(std::io::Cursor::new(src))
          .map_err(CompressionError::zstd_decompress_error)?;
        std::io::copy(&mut decoder, &mut std::io::Cursor::new(dst))
          .map(|bytes| bytes as usize)
          .map_err(CompressionError::zstd_decompress_error)
      }
      algo => Err(CompressionError::UnknownAlgorithm(*algo)),
    }
  }

  /// Returns the maximum compressed length of the given buffer.
  ///
  /// This is useful when you want to pre-allocate the buffer before compressing.
  pub fn max_compress_len(&self, input_size: usize) -> Result<usize, CompressionError> {
    Ok(match self {
      #[cfg(feature = "brotli")]
      Self::Brotli(_) => {
        // TODO(al8n): The brotli::enc::BrotliEncoderMaxCompressedSize is not working as expected.
        // In fuzzy tests, it is returning a value less than the actual compressed size.
        let num_large_blocks = (input_size >> 4) + 12;
        let overhead = 2 + (4 * num_large_blocks) + 3 + 1;
        let result = input_size + overhead;
        if input_size == 0 {
          2
        } else if result < input_size {
          input_size
        } else {
          result
        }
      }
      #[cfg(feature = "lz4")]
      Self::Lz4 => lz4_flex::block::get_maximum_output_size(input_size),
      #[cfg(feature = "snappy")]
      Self::Snappy => snap::raw::max_compress_len(input_size),
      #[cfg(feature = "zstd")]
      Self::Zstd(_) => zstd::zstd_safe::compress_bound(input_size),
      Self::Unknown(_) => return Err(CompressionError::UnknownAlgorithm(*self)),
    })
  }

  /// Compresses the given buffer.
  ///
  /// The `dst` buffer should be pre-allocated with the [`max_compress_len`](Self::max_compress_len) method.
  pub fn compress_to(&self, src: &[u8], dst: &mut [u8]) -> Result<usize, CompressionError> {
    match self {
      #[cfg(feature = "brotli")]
      CompressAlgorithm::Brotli(_algo) => {
        use std::io::Write;

        let mut compressor = brotli::CompressorWriter::new(
          std::io::Cursor::new(dst),
          BROTLI_BUFFER_SIZE,
          _algo.quality() as u8 as u32,
          _algo.window() as u8 as u32,
        );

        compressor
          .write_all(src)
          .map(|_| compressor.into_inner().position() as usize)
          .map_err(CompressionError::brotli_compress_error)
      }
      #[cfg(feature = "lz4")]
      CompressAlgorithm::Lz4 => {
        lz4_flex::compress_into(src, dst).map_err(CompressionError::lz4_compress_error)
      }
      #[cfg(feature = "snappy")]
      CompressAlgorithm::Snappy => {
        let mut encoder = snap::raw::Encoder::new();
        encoder
          .compress(src, dst)
          .map_err(CompressionError::snappy_compress_error)
      }
      #[cfg(feature = "zstd")]
      CompressAlgorithm::Zstd(_level) => {
        let mut cursor = std::io::Cursor::new(dst);
        zstd::stream::copy_encode(src, &mut cursor, _level.level() as i32)
          .map(|_| cursor.position() as usize)
          .map_err(CompressionError::zstd_compress_error)
      }
      algo => Err(CompressionError::UnknownAlgorithm(*algo)),
    }
  }

  /// Returns the string representation of the algorithm.
  #[inline]
  pub fn as_str(&self) -> Cow<'_, str> {
    match self {
      #[cfg(feature = "brotli")]
      Self::Brotli(algo) => return Cow::Owned(format!("brotli{algo}")),
      #[cfg(feature = "lz4")]
      Self::Lz4 => "lz4",
      #[cfg(feature = "snappy")]
      Self::Snappy => "snappy",
      #[cfg(feature = "zstd")]
      Self::Zstd(val) => return Cow::Owned(format!("zstd({})", val.level())),
      Self::Unknown(val) => return Cow::Owned(format!("unknown({val})")),
    }
    .into()
  }

  /// Encodes the algorithm into a u16.
  /// First byte is the algorithm tag.
  /// Second byte stores additional configuration if any.
  #[inline]
  pub(super) const fn encode_to_u16(&self) -> u16 {
    let (tag, extra) = match self {
      #[cfg(feature = "brotli")]
      Self::Brotli(algo) => (BROTLI_TAG, algo.encode()),
      #[cfg(feature = "lz4")]
      Self::Lz4 => (LZ4_TAG, 0),
      #[cfg(feature = "snappy")]
      Self::Snappy => (SNAPPY_TAG, 0),
      #[cfg(feature = "zstd")]
      Self::Zstd(algo) => (ZSTD_TAG, algo.level() as u8),
      Self::Unknown(v) => (*v, 0),
    };
    ((tag as u16) << 8) | (extra as u16)
  }

  /// Creates a CompressAlgorithm from a u16.
  /// First byte determines the algorithm type.
  /// Second byte contains additional configuration if any.
  #[inline]
  pub(super) const fn decode_from_u16(value: u16) -> Self {
    let tag = (value >> 8) as u8;
    let extra = value as u8;

    match tag {
      #[cfg(feature = "brotli")]
      BROTLI_TAG => Self::Brotli(BrotliAlgorithm::decode(extra)),
      #[cfg(feature = "lz4")]
      LZ4_TAG => Self::Lz4,
      #[cfg(feature = "snappy")]
      SNAPPY_TAG => Self::Snappy,
      #[cfg(feature = "zstd")]
      ZSTD_TAG => Self::Zstd(ZstdCompressionLevel::with_level(extra as i8)),
      v => Self::Unknown(v),
    }
  }

  pub(crate) const fn unknown_or_disabled(&self) -> CompressionError {
    match self {
      Self::Unknown(value) => {
        let value = *value;
        #[cfg(not(all(
          feature = "brotli",
          feature = "lz4",
          feature = "snappy",
          feature = "zstd"
        )))]
        {
          let tag = (value >> 8) as u8;
          return match tag {
            #[cfg(feature = "brotli")]
            BROTLI_TAG => CompressionError::disabled(*self, "brotli"),
            #[cfg(feature = "lz4")]
            LZ4_TAG => CompressionError::disabled(*self, "lz4"),
            #[cfg(feature = "snappy")]
            SNAPPY_TAG => CompressionError::disabled(*self, "snappy"),
            #[cfg(feature = "zstd")]
            ZSTD_TAG => CompressionError::disabled(*self, "zstd"),
            _ => CompressionError::UnknownAlgorithm(*self),
          };
        }

        #[cfg(all(
          feature = "brotli",
          feature = "lz4",
          feature = "snappy",
          feature = "zstd"
        ))]
        CompressionError::UnknownAlgorithm(Self::Unknown(value))
      }
      _ => unreachable!(),
    }
  }
}

impl From<u16> for CompressAlgorithm {
  fn from(value: u16) -> Self {
    Self::decode_from_u16(value)
  }
}

impl<'a> DataRef<'a, Self> for CompressAlgorithm {
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    <u16 as DataRef<u16>>::decode(buf).map(|(bytes_read, value)| (bytes_read, Self::from(value)))
  }
}

impl Data for CompressAlgorithm {
  const WIRE_TYPE: WireType = WireType::Varint;

  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    self.encode_to_u16().encoded_len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    self.encode_to_u16().encode(buf)
  }
}

#[inline]
fn parse_or_default<I, O>(s: &str) -> Result<O, I::Err>
where
  I: core::str::FromStr,
  O: Default + From<I>,
{
  match s {
    "" | "-" | "_" => Ok(O::default()),
    val => val.parse::<I>().map(Into::into),
  }
}

#[cfg(feature = "serde")]
const _: () = {
  use serde::{Deserialize, Deserializer, Serialize, Serializer};

  #[derive(serde::Serialize, serde::Deserialize)]
  #[serde(rename_all = "snake_case")]
  enum CompressAlgorithmHelper {
    /// Brotli
    Brotli(BrotliAlgorithm),
    /// LZ4
    Lz4,
    /// Snappy
    Snappy,
    /// Zstd
    Zstd(ZstdCompressionLevel),
    /// Unknwon compressioned algorithm
    Unknown(u8),
  }

  impl From<CompressAlgorithm> for CompressAlgorithmHelper {
    fn from(algo: CompressAlgorithm) -> Self {
      match algo {
        #[cfg(feature = "brotli")]
        CompressAlgorithm::Brotli(algo) => Self::Brotli(algo),
        #[cfg(feature = "lz4")]
        CompressAlgorithm::Lz4 => Self::Lz4,
        #[cfg(feature = "snappy")]
        CompressAlgorithm::Snappy => Self::Snappy,
        #[cfg(feature = "zstd")]
        CompressAlgorithm::Zstd(algo) => Self::Zstd(algo),
        CompressAlgorithm::Unknown(v) => Self::Unknown(v),
      }
    }
  }

  impl From<CompressAlgorithmHelper> for CompressAlgorithm {
    fn from(helper: CompressAlgorithmHelper) -> Self {
      match helper {
        #[cfg(feature = "brotli")]
        CompressAlgorithmHelper::Brotli(algo) => Self::Brotli(algo),
        #[cfg(feature = "lz4")]
        CompressAlgorithmHelper::Lz4 => Self::Lz4,
        #[cfg(feature = "snappy")]
        CompressAlgorithmHelper::Snappy => Self::Snappy,
        #[cfg(feature = "zstd")]
        CompressAlgorithmHelper::Zstd(algo) => Self::Zstd(algo),
        CompressAlgorithmHelper::Unknown(v) => Self::Unknown(v),
      }
    }
  }

  impl Serialize for CompressAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: Serializer,
    {
      if serializer.is_human_readable() {
        CompressAlgorithmHelper::from(*self).serialize(serializer)
      } else {
        serializer.serialize_u16(self.encode_to_u16())
      }
    }
  }

  impl<'de> Deserialize<'de> for CompressAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: Deserializer<'de>,
    {
      if deserializer.is_human_readable() {
        CompressAlgorithmHelper::deserialize(deserializer).map(Into::into)
      } else {
        u16::deserialize(deserializer).map(Self::decode_from_u16)
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use super::CompressAlgorithm;

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "serde")]
  fn compress_algorithm_serde(algo: CompressAlgorithm) -> bool {
    let Ok(serialized) = serde_json::to_string(&algo) else {
      return false;
    };
    let Ok(deserialized) = serde_json::from_str(&serialized) else {
      return false;
    };
    if algo != deserialized {
      return false;
    }

    let Ok(serialized) = bincode::serialize(&algo) else {
      return false;
    };

    let Ok(deserialized) = bincode::deserialize(&serialized) else {
      return false;
    };

    algo == deserialized
  }

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "lz4")]
  fn lz4(data: Vec<u8>) -> bool {
    let algo = CompressAlgorithm::Lz4;

    let uncompressed_data_len = data.len();
    let max_compress_len = algo.max_compress_len(uncompressed_data_len).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let mut orig = vec![0; uncompressed_data_len];
    let decompressed = algo.decompress_to(&buffer[..written], &mut orig).unwrap();
    data == orig && uncompressed_data_len == decompressed
  }

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "brotli")]
  fn brotli(data: Vec<u8>) -> bool {
    let algo = CompressAlgorithm::Brotli(Default::default());
    let uncompressed_data_len = data.len();
    let max_compress_len = algo.max_compress_len(uncompressed_data_len).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let mut orig = vec![0; uncompressed_data_len];
    let decompressed = algo.decompress_to(&buffer[..written], &mut orig).unwrap();
    data == orig && uncompressed_data_len == decompressed
  }

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "zstd")]
  fn zstd(data: Vec<u8>) -> bool {
    let algo = CompressAlgorithm::Zstd(Default::default());
    let uncompressed_data_len = data.len();
    let max_compress_len = algo.max_compress_len(uncompressed_data_len).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let mut orig = vec![0; uncompressed_data_len];
    let decompressed = algo.decompress_to(&buffer[..written], &mut orig).unwrap();
    data == orig && uncompressed_data_len == decompressed
  }

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "snappy")]
  fn snappy(data: Vec<u8>) -> bool {
    let algo = CompressAlgorithm::Snappy;
    let uncompressed_data_len = data.len();
    let max_compress_len = algo.max_compress_len(uncompressed_data_len).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let mut orig = vec![0; uncompressed_data_len];
    let decompressed = algo.decompress_to(&buffer[..written], &mut orig).unwrap();
    data == orig && uncompressed_data_len == decompressed
  }

  #[cfg(feature = "snappy")]
  #[test]
  fn max_snappy_output_size() {
    let algo = CompressAlgorithm::Snappy;
    let max_compress_len = algo.max_compress_len(4096).unwrap();
    println!("max_compress_len: {}", max_compress_len);
    assert!(max_compress_len >= 4096);
  }

  #[cfg(feature = "lz4")]
  #[test]
  fn max_lz4_output_size() {
    let algo = CompressAlgorithm::Lz4;
    let max_compress_len = algo.max_compress_len(4096).unwrap();
    println!("max_compress_len: {}", max_compress_len);
    assert!(max_compress_len >= 4096);
  }

  #[cfg(feature = "brotli")]
  #[test]
  fn max_brotli_output_size() {
    let algo = CompressAlgorithm::Brotli(Default::default());
    let max_compress_len = algo.max_compress_len(4096).unwrap();
    println!("max_compress_len: {}", max_compress_len);
    assert!(max_compress_len >= 4096);
  }

  #[cfg(feature = "zstd")]
  #[test]
  fn max_zstd_output_size() {
    let algo = CompressAlgorithm::Zstd(Default::default());
    let max_compress_len = algo.max_compress_len(4096).unwrap();
    println!("max_compress_len: {}", max_compress_len);
    assert!(max_compress_len >= 4096);
  }
}
