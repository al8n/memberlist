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

const LZW_TAG: u8 = 0;
const ZSTD_TAG: u8 = 1;
const BROTLI_TAG: u8 = 2;
const DEFLATE_TAG: u8 = 3;
const GZIP_TAG: u8 = 4;
const LZ4_TAG: u8 = 5;
const SNAPPY_TAG: u8 = 6;
const ZLIB_TAG: u8 = 7;

mod brotli_impl;
mod error;
mod flate2_impl;
mod from_str;
mod lzw_impl;
mod zstd_impl;

pub use brotli_impl::*;
pub use error::*;
pub use flate2_impl::*;
pub use from_str::*;
pub use lzw_impl::*;
pub use zstd_impl::*;

#[cfg(feature = "brotli")]
const BROTLI_BUFFER_SIZE: usize = 4096;

#[cfg(feature = "lzw")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn lzw_compress_error(err: std::io::Error) -> Self {
      Self::Compress(CompressError::Lzw(err))
    }

    #[inline]
    const fn lzw_decompress_error(err: weezl::LzwError) -> Self {
      Self::Decompress(DecompressError::Lzw(err))
    }
  }
};

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

#[cfg(feature = "zlib")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn zlib_compress_error(err: std::io::Error) -> Self {
      Self::Compress(CompressError::Zlib(err))
    }

    #[inline]
    const fn zlib_decompress_error(err: std::io::Error) -> Self {
      Self::Decompress(DecompressError::Zlib(err))
    }
  }
};

#[cfg(feature = "gzip")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn gzip_compress_error(err: std::io::Error) -> Self {
      Self::Compress(CompressError::Gzip(err))
    }

    #[inline]
    const fn gzip_decompress_error(err: std::io::Error) -> Self {
      Self::Decompress(DecompressError::Gzip(err))
    }
  }
};

#[cfg(feature = "deflate")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn deflate_compress_error(err: std::io::Error) -> Self {
      Self::Compress(CompressError::Deflate(err))
    }

    #[inline]
    const fn deflate_decompress_error(err: std::io::Error) -> Self {
      Self::Decompress(DecompressError::Deflate(err))
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
  /// Deflate
  #[display("deflate")]
  Deflate(Flate2CompressionLevel),
  /// Gzip
  #[display("gzip")]
  Gzip(Flate2CompressionLevel),
  /// LAW
  #[display("lzw{_0}")]
  Lzw(LzwAlgorithm),
  /// LZ4
  #[display("lz4")]
  Lz4,
  /// Snappy
  #[display("snappy")]
  Snappy,
  /// Zlib
  #[display("zlib")]
  Zlib(Flate2CompressionLevel),
  /// Zstd
  #[display("zstd({_0})")]
  Zstd(ZstdCompressionLevel),
  /// Unknwon compressioned algorithm
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl Default for CompressAlgorithm {
  fn default() -> Self {
    // Recommended by Claude Sonnet :)
    cfg_if::cfg_if! {
      if #[cfg(feature = "lz4")] {
        Self::Lz4
      } else if #[cfg(feature = "snappy")] {
        Self::Snappy
      } else if #[cfg(feature = "lzw")] {
        Self::Lzw(LzwAlgorithm::default())
      } else if #[cfg(feature = "brotli")] {
        Self::Brotli(BrotliAlgorithm::default())
      } else if #[cfg(feature = "zstd")] {
        Self::Zstd(ZstdCompressionLevel::default())
      } else if #[cfg(feature = "deflate")] {
        Self::Deflate(Flate2CompressionLevel::default())
      } else if #[cfg(feature = "gzip")] {
        Self::Gzip(Flate2CompressionLevel::default())
      } else if #[cfg(feature = "zlib")] {
        Self::Zlib(Flate2CompressionLevel::default())
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
      Self::Lzw(algo) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "lzw")] {
            weezl::decode::Decoder::new(algo.order().into(), algo.width() as u8)
              .decode(src)
              .map_err(CompressionError::lzw_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "lzw"))
          }
        }
      }
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
      Self::Gzip(_) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "gzip")] {
            use flate2::read::GzDecoder;
            use std::io::Read;

            let mut decoder = GzDecoder::new(src);
            let mut buf = Vec::new();
            decoder.read_to_end(&mut buf).map(|_| buf).map_err(CompressionError::gzip_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "gzip"))
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
      Self::Zlib(_) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "zlib")] {
            use flate2::read::ZlibDecoder;
            use std::io::Read;

            let mut decoder = ZlibDecoder::new(src);
            let mut buf = Vec::new();
            decoder.read_to_end(&mut buf).map(|_| buf).map_err(CompressionError::zlib_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "zlib"))
          }
        }
      }
      Self::Deflate(_) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "deflate")] {
            use flate2::read::DeflateDecoder;
            use std::io::Read;

            let mut decoder = DeflateDecoder::new(src);
            let mut buf = Vec::new();
            decoder.read_to_end(&mut buf).map(|_| buf).map_err(CompressionError::deflate_decompress_error)
          } else {
            Err(CompressionError::disabled(algo, "deflate"))
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
      algo => Err(CompressionError::UnknownCompressAlgorithm(*algo)),
    }
  }

  /// Returns the maximum compressed length of the given buffer.
  ///
  /// This is useful when you want to pre-allocate the buffer before compressing.
  pub fn max_compress_len(&self, src: &[u8]) -> Result<usize, CompressionError> {
    Ok(match self {
      Self::Brotli(brotli_algorithm) => {
        return {
          cfg_if::cfg_if! {
            if #[cfg(feature = "brotli")] {
              use std::io::Write;

              let mut enc = brotli::CompressorWriter::new(
                NoopWriter::default(),
                BROTLI_BUFFER_SIZE,
                brotli_algorithm.quality() as u8 as u32,
                brotli_algorithm.window() as u8 as u32,
              );
              enc
                .write_all(src)
                .and_then(|_| enc.flush())
                .map(|_| enc.into_inner().into())
                .map_err(CompressionError::brotli_compress_error)
            } else {
              Err(CompressionError::disabled(*self, "brotli"))
            }
          }
        };
      }
      Self::Deflate(level) => {
        return {
          cfg_if::cfg_if! {
            if #[cfg(feature = "deflate")] {
              use std::io::Write;

              let mut enc = flate2::write::DeflateEncoder::new(NoopWriter::default(), (*level).into());
              enc
                .write_all(src)
                .and_then(|_| enc.finish())
                .map(Into::into)
                .map_err(CompressionError::deflate_compress_error)
            } else {
              Err(CompressionError::disabled(*self, "deflate"))
            }
          }
        };
      }
      Self::Gzip(level) => {
        return {
          cfg_if::cfg_if! {
            if #[cfg(feature = "gzip")] {
              use std::io::Write;

              let mut enc = flate2::write::GzEncoder::new(NoopWriter::default(), (*level).into());
              enc
                .write_all(src)
                .and_then(|_| enc.finish())
                .map(Into::into)
                .map_err(CompressionError::gzip_compress_error)
            } else {
              Err(CompressionError::disabled(*self, "gzip"))
            }
          }
        };
      }
      Self::Lzw(algo) => {
        return {
          cfg_if::cfg_if! {
            if #[cfg(feature = "lzw")] {
              let mut writter = NoopWriter::default();
              weezl::encode::Encoder::new(algo.order().into(), algo.width() as u8)
                .into_stream(&mut writter)
                .encode_all(src)
                .status
                .map(|_| writter.into())
                .map_err(CompressionError::lzw_compress_error)
            } else {
              Err(CompressionError::disabled(*self, "lzw"))
            }
          }
        };
      }
      Self::Lz4 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "lz4")] {
            lz4_flex::block::get_maximum_output_size(src.len())
          } else {
            return Err(CompressionError::disabled(*self, "lz4"));
          }
        }
      }
      Self::Snappy => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "snappy")] {
            snap::raw::max_compress_len(src.len())
          } else {
            return Err(CompressionError::disabled(*self, "snappy"));
          }
        }
      }
      Self::Zlib(level) => {
        return {
          cfg_if::cfg_if! {
            if #[cfg(feature = "zlib")] {
              use std::io::Write;

              let mut enc = flate2::write::ZlibEncoder::new(NoopWriter::default(), (*level).into());
              enc
                .write_all(src)
                .and_then(|_| enc.finish())
                .map(Into::into)
                .map_err(CompressionError::zlib_compress_error)
            } else {
              Err(CompressionError::disabled(*self, "zlib"))
            }
          }
        }
      }
      Self::Zstd(zstd_algorithm) => {
        return {
          cfg_if::cfg_if! {
            if #[cfg(feature = "zstd")] {
              let mut writter = NoopWriter::default();
              zstd::stream::copy_encode(src, &mut writter, zstd_algorithm.level() as i32)
                .map(|_| writter.into())
                .map_err(CompressionError::zstd_compress_error)
            } else {
              Err(CompressionError::disabled(*self, "zstd"))
            }
          }
        }
      }
      Self::Unknown(_) => return Err(CompressionError::UnknownCompressAlgorithm(*self)),
    })
  }

  /// Compresses the given buffer.
  pub fn compress_to_vec(&self, src: &[u8], dst: &mut Vec<u8>) -> Result<(), CompressionError> {
    match self {
      CompressAlgorithm::Lzw(_algo) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "lzw")] {
            // weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
            //   .into_vec(dst)
            //   .encode_all(src)
            //   .status
            //   .map(|_| ());
            todo!()
          } else {
            Err(CompressionError::disabled(algo, "lzw"))
          }
        }
      }
      CompressAlgorithm::Brotli(algo) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "brotli")] {
            use std::io::Write;

            let mut compressor = brotli::CompressorWriter::new(
              dst,
              BROTLI_BUFFER_SIZE,
              algo.quality() as u8 as u32,
              algo.window() as u8 as u32,
            );

            compressor.write_all(src).map_err(CompressionError::brotli_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "brotli"))
          }
        }
      }
      CompressAlgorithm::Deflate(level) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "deflate")] {
            use flate2::write::DeflateEncoder;
            use std::io::Write;

            let mut encoder = DeflateEncoder::new(dst, (*level).into());
            encoder.write_all(src)
              .and_then(|_| encoder.finish().map(|_| ()))
              .map_err(CompressionError::deflate_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "deflate"))
          }
        }
      }
      CompressAlgorithm::Gzip(level) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "gzip")] {
            use flate2::write::GzEncoder;
            use std::io::Write;

            let mut encoder = GzEncoder::new(dst, (*level).into());
            encoder.write_all(src)
              .and_then(|_| encoder.finish().map(|_| ()))
              .map_err(CompressionError::gzip_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "gzip"))
          }
        }
      }
      CompressAlgorithm::Lz4 => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "lz4")] {
            let required = lz4_flex::block::get_maximum_output_size(src.len());
            let len = dst.len();
            dst.resize(len + required, 0);
            lz4_flex::compress_into(src, &mut dst[len..])
              .map(|written| {
                dst.truncate(len + written);
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
            let cap = snap::raw::max_compress_len(src.len());
            let len = dst.len();
            dst.resize(len + cap, 0);
            encoder.compress(src, &mut dst[len..])
              .map(|written| {
                dst.truncate(len + written);
              })
              .map_err(CompressionError::snappy_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "snappy"))
          }
        }
      }
      CompressAlgorithm::Zlib(level) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "zlib")] {
            use flate2::write::ZlibEncoder;
            use std::io::Write;

            let mut encoder = ZlibEncoder::new(Vec::new(), (*level).into());
            encoder
              .write_all(src)
              .and_then(|_| encoder.flush_finish().map(|_| ()))
              .map_err(CompressionError::zlib_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "zlib"))
          }
        }
      }
      CompressAlgorithm::Zstd(level) => {
        cfg_if::cfg_if! {
          if #[cfg(feature = "zstd")] {
            zstd::stream::copy_encode(src, dst, level.level() as i32).map_err(CompressionError::zstd_compress_error)
          } else {
            Err(CompressionError::disabled(algo, "zstd"))
          }
        }
      }
      algo => Err(CompressionError::UnknownCompressAlgorithm(*algo)),
    }
  }

  /// Returns the string representation of the algorithm.
  #[inline]
  pub fn as_str(&self) -> Cow<'_, str> {
    match self {
      Self::Brotli(algo) => return Cow::Owned(format!("brotli{algo}")),
      Self::Deflate(algo) => return Cow::Owned(format!("deflate({algo})")),
      Self::Gzip(algo) => return Cow::Owned(format!("gzip({algo})")),
      Self::Lzw(algo) => return Cow::Owned(format!("lzw{algo}")),
      Self::Lz4 => "lz4",
      Self::Snappy => "snappy",
      Self::Zlib(algo) => return Cow::Owned(format!("zlib({algo})")),
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
      Self::Deflate(level) => (DEFLATE_TAG, *level as u8),
      Self::Gzip(level) => (GZIP_TAG, *level as u8),
      Self::Lzw(algo) => (LZW_TAG, algo.encode()),
      Self::Lz4 => (LZ4_TAG, 0),
      Self::Snappy => (SNAPPY_TAG, 0),
      Self::Zlib(level) => (ZLIB_TAG, *level as u8),
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
      DEFLATE_TAG => Self::Deflate(Flate2CompressionLevel::from_u8(extra)),
      GZIP_TAG => Self::Gzip(Flate2CompressionLevel::from_u8(extra)),
      LZW_TAG => Self::Lzw(LzwAlgorithm::decode(extra)),
      LZ4_TAG => Self::Lz4,
      SNAPPY_TAG => Self::Snappy,
      ZLIB_TAG => Self::Zlib(Flate2CompressionLevel::from_u8(extra)),
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

/// A no-op writer that does nothing, but keeps track of the length.
/// This is used to calculate the length of the compressed data for
/// some crates that does not support pre-calculating the length.
#[derive(Default, Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct NoopWriter {
  len: usize,
}

impl From<NoopWriter> for usize {
  fn from(val: NoopWriter) -> usize {
    val.len
  }
}

impl std::io::Write for NoopWriter {
  fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
    self.len += buf.len();
    Ok(buf.len())
  }

  fn flush(&mut self) -> std::io::Result<()> {
    Ok(())
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
    /// Deflate
    Deflate(Flate2CompressionLevel),
    /// Gzip
    Gzip(Flate2CompressionLevel),
    /// LAW
    Lzw(LzwAlgorithm),
    /// LZ4
    Lz4,
    /// Snappy
    Snappy,
    /// Zlib
    Zlib(Flate2CompressionLevel),
    /// Zstd
    Zstd(ZstdCompressionLevel),
    /// Unknwon compressioned algorithm
    Unknown(u8),
  }

  impl From<CompressAlgorithm> for CompressAlgorithmHelper {
    fn from(algo: CompressAlgorithm) -> Self {
      match algo {
        CompressAlgorithm::Brotli(algo) => Self::Brotli(algo),
        CompressAlgorithm::Deflate(algo) => Self::Deflate(algo),
        CompressAlgorithm::Gzip(algo) => Self::Gzip(algo),
        CompressAlgorithm::Lzw(algo) => Self::Lzw(algo),
        CompressAlgorithm::Lz4 => Self::Lz4,
        CompressAlgorithm::Snappy => Self::Snappy,
        CompressAlgorithm::Zlib(algo) => Self::Zlib(algo),
        CompressAlgorithm::Zstd(algo) => Self::Zstd(algo),
        CompressAlgorithm::Unknown(v) => Self::Unknown(v),
      }
    }
  }

  impl From<CompressAlgorithmHelper> for CompressAlgorithm {
    fn from(helper: CompressAlgorithmHelper) -> Self {
      match helper {
        CompressAlgorithmHelper::Brotli(algo) => Self::Brotli(algo),
        CompressAlgorithmHelper::Deflate(algo) => Self::Deflate(algo),
        CompressAlgorithmHelper::Gzip(algo) => Self::Gzip(algo),
        CompressAlgorithmHelper::Lzw(algo) => Self::Lzw(algo),
        CompressAlgorithmHelper::Lz4 => Self::Lz4,
        CompressAlgorithmHelper::Snappy => Self::Snappy,
        CompressAlgorithmHelper::Zlib(algo) => Self::Zlib(algo),
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
}
