use core::str::FromStr;

use super::{
  BrotliAlgorithm, ChecksumAlgorithm, CompressAlgorithm, EncryptionAlgorithm, ZstdAlgorithm,
};

/// An error type for parsing checksum algorithm from str.
#[derive(Debug, thiserror::Error)]
#[error("unknown checksum algorithm {0}")]
pub struct ParseChecksumAlgorithmError(String);

/// An error type when parsing the encryption algorithm from str
#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("unknown encryption algorithm: {0}")]
pub struct ParseEncryptionAlgorithmError(String);

/// An error that occurs when parsing a brotli quality.
#[derive(Debug, thiserror::Error)]
#[error("invalid brotli: {0}")]
pub struct ParseBrotliAlgorithmError(String);

/// An error that occurs when parsing a compress algorithm.
#[derive(Debug, thiserror::Error)]
#[error("invalid compress algorithm: {0}")]
pub struct ParseCompressAlgorithmError(String);

impl FromStr for ChecksumAlgorithm {
  type Err = ParseChecksumAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(match s {
      "crc32" | "Crc32" | "CRC32" => Self::Crc32,
      "xxhash32" | "XxHash32" | "XXHASH32" | "xxh32" => Self::XxHash32,
      "xxhash64" | "XxHash64" | "XXHASH64" | "xxh64" => Self::XxHash64,
      "xxhash3" | "XxHash3" | "XXHASH3" | "xxh3" => Self::XxHash3,
      "murmur3" | "Murmur3" | "MURMUR3" | "MurMur3" => Self::Murmur3,
      val if val.starts_with("unknown") => {
        let val = val.trim_start_matches("unknown(").trim_end_matches(')');
        Self::Unknown(
          val
            .parse::<u8>()
            .map_err(|_| ParseChecksumAlgorithmError(val.to_string()))?,
        )
      }
      val => return Err(ParseChecksumAlgorithmError(val.to_string())),
    })
  }
}

impl FromStr for EncryptionAlgorithm {
  type Err = ParseEncryptionAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(match s {
      "aes-gcm-no-padding" | "aes-gcm-nopadding" | "nopadding" | "NOPADDING" | "no-padding"
      | "NoPadding" | "no_padding" => Self::NoPadding,
      "aes-gcm-pkcs7" | "PKCS7" | "pkcs7" => Self::Pkcs7,
      s if s.starts_with("unknown") => {
        let v = s
          .trim_start_matches("unknown(")
          .trim_end_matches(')')
          .parse()
          .map_err(|_| ParseEncryptionAlgorithmError(s.to_string()))?;
        Self::Unknown(v)
      }
      e => return Err(ParseEncryptionAlgorithmError(e.to_string())),
    })
  }
}

impl core::str::FromStr for BrotliAlgorithm {
  type Err = ParseBrotliAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let parts = s.trim_matches(|c| c == '(' || c == ')');

    if parts.is_empty() {
      return Ok(Self::default());
    }

    let mut parts = parts.split(',');
    let quality = parts
      .next()
      .ok_or_else(|| ParseBrotliAlgorithmError(s.to_string()))?
      .trim()
      .parse::<u8>()
      .map_err(|_| ParseBrotliAlgorithmError(s.to_string()))?;
    let window = parts
      .next()
      .ok_or_else(|| ParseBrotliAlgorithmError(s.to_string()))?
      .trim()
      .parse::<u8>()
      .map_err(|_| ParseBrotliAlgorithmError(s.to_string()))?;
    Ok(Self::with_quality_and_window(quality.into(), window.into()))
  }
}

impl FromStr for CompressAlgorithm {
  type Err = ParseCompressAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "zlib" | "Zlib" | "ZLIB" => Ok(Self::Zlib),
      "gzip" | "Gzip" | "GZIP" => Ok(Self::Gzip),
      "deflate" | "Deflate" | "DEFLATE" => Ok(Self::Deflate),
      "lz4" | "Lz4" | "LZ4" => Ok(Self::Lz4),
      val if val.starts_with("unknown") => {
        let val = val.trim_start_matches("unknown(").trim_end_matches(')').trim();
        Ok(Self::Unknown(
          val
            .parse::<u8>()
            .map_err(|_| ParseCompressAlgorithmError(val.to_string()))?,
        ))
      }
      val => {
        if let Some(suffix) = is_brotli(val) {
          if suffix.is_empty() {
            return Ok(Self::Brotli(BrotliAlgorithm::default()));
          }

          return BrotliAlgorithm::from_str(suffix)
            .map(Self::Brotli)
            .map_err(|_| ParseCompressAlgorithmError(val.to_string()));
        }

        if let Some(suffix) = is_zstd(val) {
          if suffix.is_empty() {
            return Ok(Self::Zstd(ZstdAlgorithm::default()));
          }

          let without = suffix
            .trim_start_matches("(")
            .trim_end_matches(")")
            .trim();

          if without.is_empty() {
            return Ok(Self::Zstd(ZstdAlgorithm::default()));
          }

          return without
            .parse::<i8>()
            .map(|v| Self::Zstd(ZstdAlgorithm::with_level(v)))
            .map_err(|_| ParseCompressAlgorithmError(val.to_string()));
        }

        Err(ParseCompressAlgorithmError(val.to_string()))
      }
    }
  }
}

fn is_brotli(s: &str) -> Option<&str> {
  if let Some(suffix) = s.strip_prefix("brotli") {
    return Some(suffix);
  }

  if let Some(suffix) = s.strip_prefix("Brotli") {
    return Some(suffix);
  }

  if let Some(suffix) = s.strip_prefix("BROTLI") {
    return Some(suffix);
  }

  None
}

fn is_zstd(s: &str) -> Option<&str> {
  if let Some(suffix) = s.strip_prefix("zstd") {
    return Some(suffix);
  }

  if let Some(suffix) = s.strip_prefix("Zstd") {
    return Some(suffix);
  }

  if let Some(suffix) = s.strip_prefix("ZSTD") {
    return Some(suffix);
  }

  None
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_compress_algorithm_from_str() {
    assert_eq!("zlib".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Zlib);
    assert_eq!("gzip".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Gzip);
    assert_eq!("deflate".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Deflate);
    assert_eq!("lz4".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Lz4);
    assert_eq!("unknown(33)".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Unknown(33));
    assert!("unknown".parse::<CompressAlgorithm>().is_err());
    assert_eq!("brotli(11, 22)".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Brotli(BrotliAlgorithm::with_quality_and_window(11.into(), 22.into())));
    assert_eq!("brotli".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Brotli(BrotliAlgorithm::default()));
    assert_eq!("brotli()".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Brotli(BrotliAlgorithm::default()));
    assert!("brotli(-)".parse::<CompressAlgorithm>().is_err());

    assert_eq!("zstd(3)".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Zstd(ZstdAlgorithm::with_level(3)));
    assert_eq!("zstd".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Zstd(ZstdAlgorithm::default()));
    assert_eq!("zstd()".parse::<CompressAlgorithm>().unwrap(), CompressAlgorithm::Zstd(ZstdAlgorithm::default()));
    assert!("zstd(-)".parse::<CompressAlgorithm>().is_err());

  }

  #[test]
  fn test_encrypt_algorithm_from_str() {
    assert_eq!("aes-gcm-no-padding".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::NoPadding);
    assert_eq!("aes-gcm-nopadding".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::NoPadding);
    assert_eq!("aes-gcm-pkcs7".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::Pkcs7);
    assert_eq!("NoPadding".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::NoPadding);
    assert_eq!("no-padding".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::NoPadding);
    assert_eq!("nopadding".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::NoPadding);
    assert_eq!("no_padding".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::NoPadding);
    assert_eq!("NOPADDING".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::NoPadding);
    assert_eq!("unknown(33)".parse::<EncryptionAlgorithm>().unwrap(), EncryptionAlgorithm::Unknown(33));
    assert!("unknown".parse::<EncryptionAlgorithm>().is_err());
  }

  #[test]
  fn test_checksum_algorithm_from_str() {
    assert_eq!("crc32".parse::<ChecksumAlgorithm>().unwrap(), ChecksumAlgorithm::Crc32);
    assert_eq!("xxhash32".parse::<ChecksumAlgorithm>().unwrap(), ChecksumAlgorithm::XxHash32);
    assert_eq!("xxhash64".parse::<ChecksumAlgorithm>().unwrap(), ChecksumAlgorithm::XxHash64);
    assert_eq!("xxhash3".parse::<ChecksumAlgorithm>().unwrap(), ChecksumAlgorithm::XxHash3);
    assert_eq!("murmur3".parse::<ChecksumAlgorithm>().unwrap(), ChecksumAlgorithm::Murmur3);
    assert_eq!("unknown(33)".parse::<ChecksumAlgorithm>().unwrap(), ChecksumAlgorithm::Unknown(33));
    assert!("unknown".parse::<ChecksumAlgorithm>().is_err());
  }
}
