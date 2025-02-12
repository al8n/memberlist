use core::str::FromStr;

num_to_enum! {
  /// The compression level for gzip, zlib and deflate
  #[derive(Default)]
  Flate2CompressionLevel(u8 in [0, 10]):"level":"L" {
    0, 1, 2, 3, 4, 5, #[default] 6, 7, 8, 9, #[doc = "Use this value with caution, for the reason, please see [`flate2::Compression`](flate2::Compression)."] 10,
  }
}

#[cfg(feature = "flate2")]
impl From<Flate2CompressionLevel> for flate2::Compression {
  fn from(value: Flate2CompressionLevel) -> Self {
    match value {
      Flate2CompressionLevel::L0 => flate2::Compression::new(0),
      Flate2CompressionLevel::L1 => flate2::Compression::new(1),
      Flate2CompressionLevel::L2 => flate2::Compression::new(2),
      Flate2CompressionLevel::L3 => flate2::Compression::new(3),
      Flate2CompressionLevel::L4 => flate2::Compression::new(4),
      Flate2CompressionLevel::L5 => flate2::Compression::new(5),
      Flate2CompressionLevel::L6 => flate2::Compression::new(6),
      Flate2CompressionLevel::L7 => flate2::Compression::new(7),
      Flate2CompressionLevel::L8 => flate2::Compression::new(8),
      Flate2CompressionLevel::L9 => flate2::Compression::new(9),
      Flate2CompressionLevel::L10 => flate2::Compression::new(10),
    }
  }
}

/// An error that occurs when parsing a [`Flate2CompressionLevel`].
#[derive(Debug, PartialEq, Eq, Hash, Clone, thiserror::Error)]
#[error("invalid compression level: {0}")]
pub struct ParseFlate2CompressionLevelError(String);

impl FromStr for Flate2CompressionLevel {
  type Err = ParseFlate2CompressionLevelError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    if s.is_empty() {
      return Ok(Self::default());
    }

    super::parse_or_default::<u8, _>(s).map_err(|_| ParseFlate2CompressionLevelError(s.to_string()))
  }
}
