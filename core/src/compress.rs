use std::io::Read;

use memberlist_types::CompressAlgorithm;

#[cfg(feature = "lzw")]
const LZW_LIT_WIDTH: u8 = 8;
#[cfg(feature = "brotli")]
const BROTLI_BUFFER_SIZE: usize = 4096;

/// Compress/Decompress errors.
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
  /// Compress errors
  #[error(transparent)]
  Compress(#[from] CompressError),
  /// Decompress errors
  #[error(transparent)]
  Decompress(#[from] DecompressError),
  /// Disabled
  #[error("the {algo} is supported but the feature {feature} is disabled")]
  Disabled {
    /// The algorithm want to use
    algo: CompressAlgorithm,
    /// The feature that is disabled
    feature: &'static str,
  },
  /// Unknown compressor
  #[error("unknown compress algorithm {0}")]
  UnknownCompressAlgorithm(CompressAlgorithm),
  /// Not enough bytes to decompress
  #[error("not enough bytes to decompress")]
  NotEnoughBytes,
}

impl CompressionError {
  #[inline]
  const fn disabled(algo: CompressAlgorithm, feature: &'static str) -> Self {
    Self::Disabled { algo, feature }
  }
}

#[cfg(feature = "lzw")]
const _: () = {
  impl CompressionError {
    #[inline]
    const fn lzw_compress_error(err: weezl::LzwError) -> Self {
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

/// Compress errors.
#[derive(Debug, thiserror::Error)]
pub enum CompressError {
  /// LZW compress errors
  #[error(transparent)]
  #[cfg(feature = "lzw")]
  #[cfg_attr(docsrs, doc(cfg(feature = "lzw")))]
  Lzw(#[from] weezl::LzwError),
  /// Brotli compress errors
  #[error(transparent)]
  #[cfg(feature = "brotli")]
  #[cfg_attr(docsrs, doc(cfg(feature = "brotli")))]
  Brotli(std::io::Error),
  /// Zlib compress errors
  #[error(transparent)]
  #[cfg(feature = "zlib")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zlib")))]
  Zlib(std::io::Error),
  /// Gzip compress errors
  #[error(transparent)]
  #[cfg(feature = "gzip")]
  #[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
  Gzip(std::io::Error),
  /// Deflate compress errors
  #[error(transparent)]
  #[cfg(feature = "deflate")]
  #[cfg_attr(docsrs, doc(cfg(feature = "deflate")))]
  Deflate(std::io::Error),
  /// Snappy compress errors
  #[error(transparent)]
  #[cfg(feature = "snappy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "snappy")))]
  Snappy(#[from] snap::Error),
  /// Zstd compress errors
  #[error(transparent)]
  #[cfg(feature = "zstd")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
  Zstd(std::io::Error),
}

/// Decompress errors.
#[derive(Debug, thiserror::Error)]
pub enum DecompressError {
  /// LZW decompress errors
  #[error(transparent)]
  #[cfg(feature = "lzw")]
  #[cfg_attr(docsrs, doc(cfg(feature = "lzw")))]
  Lzw(#[from] weezl::LzwError),
  /// Brotli decompress errors
  #[error(transparent)]
  #[cfg(feature = "brotli")]
  #[cfg_attr(docsrs, doc(cfg(feature = "brotli")))]
  Brotli(std::io::Error),
  /// Zlib decompress errors
  #[error(transparent)]
  #[cfg(feature = "zlib")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zlib")))]
  Zlib(std::io::Error),
  /// Gzip decompress errors
  #[error(transparent)]
  #[cfg(feature = "gzip")]
  #[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
  Gzip(std::io::Error),
  /// Deflate decompress errors
  #[error(transparent)]
  #[cfg(feature = "deflate")]
  #[cfg_attr(docsrs, doc(cfg(feature = "deflate")))]
  Deflate(std::io::Error),

  /// LZ4 decompress errors
  #[error(transparent)]
  #[cfg(feature = "lz4")]
  #[cfg_attr(docsrs, doc(cfg(feature = "lz4")))]
  Lz4(#[from] lz4_flex::block::DecompressError),

  /// Snappy decompress errors
  #[error(transparent)]
  #[cfg(feature = "snappy")]
  #[cfg_attr(docsrs, doc(cfg(feature = "snappy")))]
  Snappy(#[from] snap::Error),
  /// Zstd decompress errors
  #[error(transparent)]
  #[cfg(feature = "zstd")]
  #[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
  Zstd(#[from] std::io::Error),
}

/// Decompresses the given buffer.
pub(crate) fn decompress(algo: CompressAlgorithm, src: &[u8]) -> Result<Vec<u8>, CompressionError> {
  match algo {
    CompressAlgorithm::Lzw => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "lzw")] {
          weezl::decode::Decoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
            .decode(src)
            .map_err(CompressionError::lzw_decompress_error)
        } else {
          Err(CompressionError::disabled(algo, "lzw"))
        }
      }
    }
    CompressAlgorithm::Brotli(_) => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "brotli")] {
          let mut reader = brotli::Decompressor::new(src, BROTLI_BUFFER_SIZE);
          let mut buf = Vec::new();
          reader.read_to_end(&mut buf).map(|_| buf).map_err(CompressionError::brotli_decompress_error)
        } else {
          Err(CompressionError::disabled(algo, "brotli"))
        }
      }
    }
    CompressAlgorithm::Gzip => {
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
    CompressAlgorithm::Lz4 => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "lz4")] {
          lz4_flex::decompress_size_prepended(src).map_err(CompressionError::lz4_decompress_error)
        } else {
          Err(CompressionError::disabled(algo, "lz4"))
        }
      }
    }
    CompressAlgorithm::Snappy => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "snappy")] {
          snap::raw::Decoder::new().decompress_vec(src).map_err(CompressionError::snappy_decompress_error)
        } else {
          Err(CompressionError::disabled(algo, "snappy"))
        }
      }
    }
    CompressAlgorithm::Zlib => {
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
    CompressAlgorithm::Deflate => {
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
    CompressAlgorithm::Zstd(_) => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "zstd")] {
          zstd::decode_all(src).map_err(CompressionError::zstd_decompress_error)
        } else {
          Err(CompressionError::disabled(algo, "zstd"))
        }
      }
    }
    algo => Err(CompressionError::UnknownCompressAlgorithm(algo)),
  }
}

/// Compresses the given buffer.
pub(crate) fn compress_into_vec(
  algo: CompressAlgorithm,
  src: &[u8],
) -> Result<Vec<u8>, CompressionError> {
  match algo {
    CompressAlgorithm::Lzw => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "lzw")] {
          let mut buf = Vec::with_capacity(src.len());

          weezl::encode::Encoder::new(weezl::BitOrder::Lsb, LZW_LIT_WIDTH)
            .into_vec(&mut buf)
            .encode_all(src)
            .status
            .map(|_| buf)
            .map_err(CompressionError::lzw_compress_error)
        } else {
          Err(CompressionError::disabled(algo, "lzw"))
        }
      }
    }
    CompressAlgorithm::Brotli(algo) => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "brotli")] {
          use std::io::Write;
          // Create a buffer to store compressed data
          let compressed = Vec::new();

          let mut compressor = brotli::CompressorWriter::new(
            compressed,
            BROTLI_BUFFER_SIZE,
            algo.quality() as u8 as u32,
            algo.window() as u8 as u32,
          );

          // Read compressed data into the buffer
          compressor.write_all(src).map_err(CompressionError::brotli_compress_error)?;
          Ok(compressor.into_inner())
        } else {
          Err(CompressionError::disabled(algo, "brotli"))
        }
      }
    }
    CompressAlgorithm::Deflate => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "deflate")] {
          use flate2::{write::DeflateEncoder, Compression};
          use std::io::Write;

          let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
          encoder.write_all(src)
            .and_then(|_| encoder.finish())
            .map_err(CompressionError::deflate_compress_error)
        } else {
          Err(CompressionError::disabled(algo, "deflate"))
        }
      }
    }
    CompressAlgorithm::Gzip => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "gzip")] {
          use flate2::write::GzEncoder;
          use flate2::Compression;
          use std::io::Write;

          let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
          encoder.write_all(src)
            .and_then(|_| encoder.finish())
            .map_err(CompressionError::gzip_compress_error)
        } else {
          Err(CompressionError::disabled(algo, "gzip"))
        }
      }
    }
    CompressAlgorithm::Lz4 => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "lz4")] {
          Ok(lz4_flex::compress_prepend_size(src))
        } else {
          Err(CompressionError::disabled(algo, "lz4"))
        }
      }
    }
    CompressAlgorithm::Snappy => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "snappy")] {
          snap::raw::Encoder::new().compress_vec(src).map_err(CompressionError::snappy_compress_error)
        } else {
          Err(CompressionError::disabled(algo, "snappy"))
        }
      }
    }
    CompressAlgorithm::Zlib => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "zlib")] {
          use flate2::write::ZlibEncoder;
          use flate2::Compression;
          use std::io::Write;

          let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
          encoder
            .write_all(src)
            .and_then(|_| encoder.flush_finish())
            .map_err(CompressionError::zlib_compress_error)
        } else {
          Err(CompressionError::disabled(algo, "zlib"))
        }
      }
    }
    CompressAlgorithm::Zstd(level) => {
      cfg_if::cfg_if! {
        if #[cfg(feature = "zstd")] {
          zstd::encode_all(src, level.level() as i32).map_err(CompressionError::zstd_compress_error)
        } else {
          Err(CompressionError::disabled(algo, "zstd"))
        }
      }
    }
    algo => Err(CompressionError::UnknownCompressAlgorithm(algo)),
  }
}
