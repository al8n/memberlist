use core::str::FromStr;

num_to_enum! {
  /// The brotli quality
  #[derive(Default)]
  BrotliQuality(u8 in [0, 11]):"quality":"Q" {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, #[default] 11,
  }
}

num_to_enum! {
  /// The brotli window
  #[derive(Default)]
  BrotliWindow(u8 in [10, 24]):"window":"W" {
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, #[default] 22, 23, 24,
  }
}

/// The brotli algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
#[display("({quality}, {window})")]
pub struct BrotliAlgorithm {
  quality: BrotliQuality,
  window: BrotliWindow,
}

impl Default for BrotliAlgorithm {
  fn default() -> Self {
    Self::new()
  }
}

impl BrotliAlgorithm {
  /// Creates a new `BrotliAlgorithm` with the default quality and window.
  #[inline]
  pub const fn new() -> Self {
    Self {
      quality: BrotliQuality::Q11,
      window: BrotliWindow::W22,
    }
  }

  /// Creates a new `BrotliAlgorithm` with the quality and window.
  #[inline]
  pub const fn with_quality_and_window(quality: BrotliQuality, window: BrotliWindow) -> Self {
    Self { quality, window }
  }

  /// Returns the quality of the brotli algorithm.
  #[inline]
  pub const fn quality(&self) -> BrotliQuality {
    self.quality
  }

  /// Returns the window of the brotli algorithm.
  #[inline]
  pub const fn window(&self) -> BrotliWindow {
    self.window
  }

  /// Encodes the algorithm settings into a single byte.
  /// Quality (0-11) is stored in the high 4 bits.
  /// Window (10-24) is stored in the low 4 bits, mapped to 0-15.
  #[inline]
  pub(crate) const fn encode(&self) -> u8 {
    // Quality goes in high 4 bits
    let quality_bits = (self.quality as u8) << 4;
    // Window needs to be mapped from 10-24 to 0-15
    let window_bits = (self.window as u8).saturating_sub(10);
    quality_bits | window_bits
  }

  /// Decodes a single byte into algorithm settings.
  /// Quality is extracted from high 4 bits.
  /// Window is extracted from low 4 bits and mapped back to 10-24 range.
  #[inline]
  pub(crate) const fn decode(byte: u8) -> Self {
    // Extract quality from high 4 bits
    let quality = BrotliQuality::from_u8(byte >> 4);
    // Extract window from low 4 bits and map back to 10-24 range
    let window = BrotliWindow::from_u8(10 + (byte & 0x0F));
    Self { quality, window }
  }
}

/// An error that occurs when parsing a brotli quality.
#[derive(Debug, PartialEq, Eq, Hash, Clone, thiserror::Error)]
#[error("invalid brotli: {0}")]
pub struct ParseBrotliAlgorithmError(String);

impl FromStr for BrotliAlgorithm {
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
      .trim();

    let quality = super::parse_or_default::<u8, _>(quality)
      .map_err(|_| ParseBrotliAlgorithmError(s.to_string()))?;

    let window = parts
      .next()
      .ok_or_else(|| ParseBrotliAlgorithmError(s.to_string()))?
      .trim();

    let window = super::parse_or_default::<u8, _>(window)
      .map_err(|_| ParseBrotliAlgorithmError(s.to_string()))?;

    Ok(Self::with_quality_and_window(quality, window))
  }
}

#[cfg(feature = "serde")]
const _: () = {
  use serde::{Deserialize, Serialize};

  #[derive(serde::Serialize, serde::Deserialize)]
  struct BrotliAlgorithmHelper {
    quality: BrotliQuality,
    window: BrotliWindow,
  }

  impl From<BrotliAlgorithm> for BrotliAlgorithmHelper {
    fn from(algo: BrotliAlgorithm) -> Self {
      Self {
        quality: algo.quality(),
        window: algo.window(),
      }
    }
  }

  impl From<BrotliAlgorithmHelper> for BrotliAlgorithm {
    fn from(helper: BrotliAlgorithmHelper) -> Self {
      Self::with_quality_and_window(helper.quality, helper.window)
    }
  }

  impl Serialize for BrotliAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: serde::Serializer,
    {
      if serializer.is_human_readable() {
        BrotliAlgorithmHelper::from(*self).serialize(serializer)
      } else {
        serializer.serialize_u8(self.encode())
      }
    }
  }

  impl<'de> Deserialize<'de> for BrotliAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: serde::Deserializer<'de>,
    {
      if deserializer.is_human_readable() {
        BrotliAlgorithmHelper::deserialize(deserializer).map(Into::into)
      } else {
        u8::deserialize(deserializer).map(Self::decode)
      }
    }
  }
};

#[test]
fn parse_str() {
  assert_eq!("".parse::<BrotliAlgorithm>(), Ok(BrotliAlgorithm::new()));
  assert_eq!("()".parse::<BrotliAlgorithm>(), Ok(BrotliAlgorithm::new()));
  assert_eq!(
    "(0, 10)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q0,
      BrotliWindow::W10
    ))
  );
  assert_eq!(
    "(11, 24)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q11,
      BrotliWindow::W24
    ))
  );
  assert_eq!(
    "(5, 15)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q5,
      BrotliWindow::W15
    ))
  );
  assert_eq!(
    "(10, 20)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q10,
      BrotliWindow::W20
    ))
  );
  assert_eq!(
    "(1, 11)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q1,
      BrotliWindow::W11
    ))
  );
  assert_eq!(
    "(2, 12)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q2,
      BrotliWindow::W12
    ))
  );
  assert_eq!(
    "(3, 13)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q3,
      BrotliWindow::W13
    ))
  );
  assert_eq!(
    "(4, 14)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::Q4,
      BrotliWindow::W14
    ))
  );
  assert_eq!(
    "(255, 255)".parse::<BrotliAlgorithm>(),
    Ok(BrotliAlgorithm::with_quality_and_window(
      BrotliQuality::max(),
      BrotliWindow::max()
    ))
  );
  assert!("(-, -)".parse::<BrotliAlgorithm>().is_err());
}
