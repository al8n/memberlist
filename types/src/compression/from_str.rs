use core::str::FromStr;

use super::CompressAlgorithm;

/// An error that occurs when parsing a compress algorithm.
#[derive(Debug, thiserror::Error)]
#[error("invalid compress algorithm: {0}")]
pub struct ParseCompressAlgorithmError(String);

// TODO(al8n): Simplify the implementation of `FromStr` for `CompressAlgorithm`? I am lazy,
// just repeat for each variant.
impl FromStr for CompressAlgorithm {
  type Err = ParseCompressAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    Ok(match s.trim() {
      "lz4" | "Lz4" | "LZ4" => Self::Lz4,
      "snappy" | "Snappy" | "SNAPPY" | "snap" | "Snap" | "SNAP" => Self::Snappy,
      val if contains(&["unknown", "Unknown", "UNKNOWN"], val) => {
        let val = strip(&["unknown", "Unknown", "UNKNOWN"], val)
          .unwrap()
          .trim_start_matches("(")
          .trim_end_matches(")");
        Self::Unknown(
          val
            .parse::<u8>()
            .map_err(|_| ParseCompressAlgorithmError(val.to_string()))?,
        )
      }
      val if contains(&["brotli", "Brotli", "BROTLI"], val) => {
        let suffix = strip(&["brotli", "Brotli", "BROTLI"], val).unwrap();
        let val = trim_parentheses(suffix).unwrap_or("");

        if val.is_empty() {
          Self::Brotli(Default::default())
        } else {
          Self::Brotli(
            val
              .parse()
              .map_err(|_| ParseCompressAlgorithmError(val.to_string()))?,
          )
        }
      }
      val if contains(&["zstd", "Zstd", "ZSTD"], val) => {
        let suffix = strip(&["zstd", "Zstd", "ZSTD"], val).unwrap();
        let val = trim_parentheses(suffix).unwrap_or("");

        if val.is_empty() {
          Self::Zstd(Default::default())
        } else {
          Self::Zstd(
            val
              .parse()
              .map_err(|_| ParseCompressAlgorithmError(val.to_string()))?,
          )
        }
      }
      val => return Err(ParseCompressAlgorithmError(val.to_string())),
    })
  }
}

#[inline]
fn strip<'a>(possible_values: &'a [&'a str], s: &'a str) -> Option<&'a str> {
  possible_values.iter().find_map(|&m| s.strip_prefix(m))
}

#[inline]
fn trim_parentheses(s: &str) -> Option<&str> {
  s.strip_prefix('(').and_then(|s| s.strip_suffix(')'))
}

#[inline]
fn contains<'a>(possible_values: &'a [&'a str], s: &'a str) -> bool {
  possible_values.iter().any(|&m| s.strip_prefix(m).is_some())
}

#[cfg(test)]
mod tests {
  use crate::{BrotliAlgorithm, ZstdCompressionLevel};

  use super::*;

  #[test]
  fn test_compress_algorithm_from_str() {
    assert_eq!(
      "lz4".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Lz4
    );
    assert_eq!(
      "unknown(33)".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Unknown(33)
    );
    assert!("unknown".parse::<CompressAlgorithm>().is_err());
    assert_eq!(
      "brotli(11, 22)".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Brotli(BrotliAlgorithm::with_quality_and_window(
        11.into(),
        22.into()
      ))
    );
    assert_eq!(
      "brotli".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Brotli(BrotliAlgorithm::default())
    );
    assert_eq!(
      "brotli()".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Brotli(BrotliAlgorithm::default())
    );
    assert!("brotli(-)".parse::<CompressAlgorithm>().is_err());
    assert_eq!(
      "zstd(3)".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Zstd(ZstdCompressionLevel::with_level(3))
    );
    assert_eq!(
      "zstd".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Zstd(ZstdCompressionLevel::default())
    );
    assert_eq!(
      "zstd()".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Zstd(ZstdCompressionLevel::default())
    );
    assert_eq!(
      "zstd(-)".parse::<CompressAlgorithm>().unwrap(),
      CompressAlgorithm::Zstd(ZstdCompressionLevel::new())
    );
  }
}
