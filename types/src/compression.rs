use std::borrow::Cow;

use super::{Data, DataRef, DecodeError, EncodeError, WireType};

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
      #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

mod brotli_impl;
mod error;
mod from_str;
mod zstd_impl;

pub use brotli_impl::*;
pub use error::*;
pub use from_str::*;
pub use zstd_impl::*;

#[cfg(feature = "quickcheck")]
mod quickcheck_impl;

#[cfg(feature = "brotli")]
const BROTLI_BUFFER_SIZE: usize = 4096;

#[cfg(feature = "lz4")]
const LZ4_PREPEND_LEN_SIZE: usize = 4;

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
  Brotli(BrotliAlgorithm),
  /// LZ4
  #[display("lz4")]
  Lz4,
  /// Snappy
  #[display("snappy")]
  Snappy,
  /// Zstd
  #[display("zstd({_0})")]
  Zstd(ZstdCompressionLevel),
  /// Unknwon compressioned algorithm
  #[display("unknown({_0})")]
  Unknown(u8),
}

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
  pub fn decompress(&self, src: &[u8]) -> Result<Vec<u8>, CompressionError> {
    match self {
      Self::Brotli(_) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "brotli")] {
            use std::io::Read;

            let mut reader = brotli::Decompressor::new(src, BROTLI_BUFFER_SIZE);
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).map(|_| buf).map_err(CompressionError::brotli_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "brotli"))
          }
        }
      }
      Self::Lz4 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "lz4")] {
            lz4_flex::decompress_size_prepended(src).map_err(CompressionError::lz4_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "lz4"))
          }
        }
      }
      Self::Snappy => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "snappy")] {
            snap::raw::Decoder::new().decompress_vec(src).map_err(CompressionError::snappy_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "snappy"))
          }
        }
      }
      Self::Zstd(_) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "zstd")] {
            zstd::decode_all(src).map_err(CompressionError::zstd_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "zstd"))
          }
        }
      }
      algo => Err(CompressionError::UnknownAlgorithm(*algo)),
    }
  }

  /// Returns the maximum compressed length of the given buffer.
  ///
  /// This is useful when you want to pre-allocate the buffer before compressing.
  pub fn max_compress_len(&self, input_size: usize) -> Result<usize, CompressionError> {
    Ok(match self {
      Self::Brotli(_) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "brotli")] {
            brotli::enc::BrotliEncoderMaxCompressedSize(input_size)
          } else {
            return Err(CompressionError::disabled(*self, "brotli"));
          }
        }
      }
      Self::Lz4 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "lz4")] {
            lz4_flex::block::get_maximum_output_size(input_size) + LZ4_PREPEND_LEN_SIZE
          } else {
            return Err(CompressionError::disabled(*self, "lz4"));
          }
        }
      }
      Self::Snappy => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "snappy")] {
            snap::raw::max_compress_len(input_size)
          } else {
            return Err(CompressionError::disabled(*self, "snappy"));
          }
        }
      }
      Self::Zstd(_) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "zstd")] {
            zstd::zstd_safe::compress_bound(input_size)
          } else {
            return Err(CompressionError::disabled(*self, "zstd"));
          }
        }
      }
      Self::Unknown(_) => return Err(CompressionError::UnknownAlgorithm(*self)),
    })
  }

  /// Compresses the given buffer.
  ///
  /// The `dst` buffer should be pre-allocated with the [`max_compress_len`](Self::max_compress_len) method.
  pub fn compress_to(&self, src: &[u8], dst: &mut [u8]) -> Result<usize, CompressionError> {
    match self {
      CompressAlgorithm::Brotli(algo) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "brotli")] {
            use std::io::Write;

            let mut compressor = brotli::CompressorWriter::new(
              std::io::Cursor::new(dst),
              BROTLI_BUFFER_SIZE,
              algo.quality() as u8 as u32,
              algo.window() as u8 as u32,
            );

            compressor.write_all(src)
              .map(|_| {
                compressor.into_inner().position() as usize
              })
              .map_err(CompressionError::brotli_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "brotli"))
          }
        }
      }
      CompressAlgorithm::Lz4 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "lz4")] {
            let input_len = src.len() as u32;
            dst[..LZ4_PREPEND_LEN_SIZE].copy_from_slice(&input_len.to_le_bytes());

            lz4_flex::compress_into(src, &mut dst[LZ4_PREPEND_LEN_SIZE..])
              .map(|written| {
                written + LZ4_PREPEND_LEN_SIZE
              })
              .map_err(CompressionError::lz4_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "lz4"))
          }
        }
      }
      CompressAlgorithm::Snappy => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "snappy")] {
            let mut encoder = snap::raw::Encoder::new();
            encoder.compress(src, dst)
              .map_err(CompressionError::snappy_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "snappy"))
          }
        }
      }
      CompressAlgorithm::Zstd(level) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "zstd")] {
            let mut cursor = std::io::Cursor::new(dst);
            zstd::stream::copy_encode(src, &mut cursor, level.level() as i32)
              .map(|_| cursor.position() as usize)
              .map_err(CompressionError::zstd_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "zstd"))
          }
        }
      }
      algo => Err(CompressionError::UnknownAlgorithm(*algo)),
    }
  }

  /// Returns the string representation of the algorithm.
  #[inline]
  pub fn as_str(&self) -> Cow<'_, str> {
    match self {
      Self::Brotli(algo) => return Cow::Owned(format!("brotli{algo}")),
      Self::Lz4 => "lz4",
      Self::Snappy => "snappy",
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
      Self::Brotli(algo) => (BROTLI_TAG, algo.encode()),
      Self::Lz4 => (LZ4_TAG, 0),
      Self::Snappy => (SNAPPY_TAG, 0),
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
      BROTLI_TAG => Self::Brotli(BrotliAlgorithm::decode(extra)),
      LZ4_TAG => Self::Lz4,
      SNAPPY_TAG => Self::Snappy,
      ZSTD_TAG => Self::Zstd(ZstdCompressionLevel::with_level(extra as i8)),
      v => Self::Unknown(v),
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
        CompressAlgorithm::Brotli(algo) => Self::Brotli(algo),
        CompressAlgorithm::Lz4 => Self::Lz4,
        CompressAlgorithm::Snappy => Self::Snappy,
        CompressAlgorithm::Zstd(algo) => Self::Zstd(algo),
        CompressAlgorithm::Unknown(v) => Self::Unknown(v),
      }
    }
  }

  impl From<CompressAlgorithmHelper> for CompressAlgorithm {
    fn from(helper: CompressAlgorithmHelper) -> Self {
      match helper {
        CompressAlgorithmHelper::Brotli(algo) => Self::Brotli(algo),
        CompressAlgorithmHelper::Lz4 => Self::Lz4,
        CompressAlgorithmHelper::Snappy => Self::Snappy,
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
    let max_compress_len = algo.max_compress_len(data.len()).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let decompressed = algo.decompress(&buffer[..written]).unwrap();
    data == decompressed
  }

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "brotli")]
  fn brotli(data: Vec<u8>) -> bool {
    let algo = CompressAlgorithm::Brotli(Default::default());
    let max_compress_len = algo.max_compress_len(data.len()).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let decompressed = algo.decompress(&buffer[..written]).unwrap();
    data == decompressed
  }

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "zstd")]
  fn zstd(data: Vec<u8>) -> bool {
    let algo = CompressAlgorithm::Zstd(Default::default());
    let max_compress_len = algo.max_compress_len(data.len()).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let decompressed = algo.decompress(&buffer[..written]).unwrap();
    data == decompressed
  }

  #[quickcheck_macros::quickcheck]
  #[cfg(feature = "snappy")]
  fn snappy(data: Vec<u8>) -> bool {
    let algo = CompressAlgorithm::Snappy;
    let max_compress_len = algo.max_compress_len(data.len()).unwrap();
    let mut buffer = vec![0; max_compress_len];
    let written = algo.compress_to(&data, &mut buffer).unwrap();
    assert!(written <= max_compress_len);
    let decompressed = algo.decompress(&buffer[..written]).unwrap();
    data == decompressed
  }
}
