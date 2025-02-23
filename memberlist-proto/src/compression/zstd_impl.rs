use core::str::FromStr;

/// The zstd algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
#[display("{}", level)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
pub struct ZstdCompressionLevel {
  level: i8,
}

impl Default for ZstdCompressionLevel {
  fn default() -> Self {
    Self::new()
  }
}

impl ZstdCompressionLevel {
  /// Creates a new `ZstdCompressionLevel` with the default level.
  #[inline]
  pub const fn new() -> Self {
    Self { level: 3 }
  }

  /// Creates a new `ZstdCompressionLevel` with the level.
  #[inline]
  pub const fn with_level(level: i8) -> Self {
    Self { level }
  }

  /// Returns the level of the zstd algorithm.
  #[inline]
  pub const fn level(&self) -> i8 {
    self.level
  }
}

impl From<u8> for ZstdCompressionLevel {
  fn from(value: u8) -> Self {
    if value > 22 {
      Self::with_level(22)
    } else {
      Self::with_level(value as i8)
    }
  }
}

impl From<i8> for ZstdCompressionLevel {
  fn from(value: i8) -> Self {
    if value > 22 {
      Self::with_level(22)
    } else if value < -99 {
      Self::with_level(-99)
    } else {
      Self::with_level(value)
    }
  }
}

/// An error that occurs when parsing a [`ZstdCompressionLevel`].
#[derive(Debug, PartialEq, Eq, Hash, Clone, thiserror::Error)]
#[error("invalid compression level: {0}")]
pub struct ParseZstdCompressionLevelError(String);

impl FromStr for ZstdCompressionLevel {
  type Err = ParseZstdCompressionLevelError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    if s.is_empty() {
      return Ok(Self::default());
    }

    super::parse_or_default::<i8, _>(s).map_err(|_| ParseZstdCompressionLevelError(s.to_string()))
  }
}
