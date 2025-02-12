use core::str::FromStr;

num_to_enum! {
  /// The LZW width
  #[derive(Default)]
  LzwWidth(u8 in [2, 12]):"width":"W" {
    2, 3, 4, 5, 6, 7, #[default] 8, 9, 10, 11, 12,
  }
}

impl LzwWidth {
  /// Returns a new `LzwWidth` with the default width.
  #[inline]
  pub const fn new() -> Self {
    Self::W8
  }
}

/// The order of bits in bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, derive_more::Display)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum LzwBitOrder {
  /// The most significant bit is processed first.
  #[display("msb")]
  Msb,
  /// The least significant bit is processed first.
  #[display("lsb")]
  #[default]
  Lsb,
}

#[cfg(feature = "lzw")]
impl From<LzwBitOrder> for weezl::BitOrder {
  fn from(value: LzwBitOrder) -> Self {
    match value {
      LzwBitOrder::Msb => weezl::BitOrder::Msb,
      LzwBitOrder::Lsb => weezl::BitOrder::Lsb,
    }
  }
}

/// The LZW algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
#[display("({order}, {width})")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LzwAlgorithm {
  order: LzwBitOrder,
  width: LzwWidth,
}

impl Default for LzwAlgorithm {
  fn default() -> Self {
    Self::new()
  }
}

/// An error that occurs when parsing a [`LzwAlgorithm`].
#[derive(Debug, PartialEq, Eq, Hash, Clone, thiserror::Error)]
#[error("invalid lzw algorithm: {0}")]
pub struct ParseLzwAlgorithmError(String);

impl FromStr for LzwAlgorithm {
  type Err = ParseLzwAlgorithmError;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    let parts = s.trim_matches(|c| c == '(' || c == ')');

    if parts.is_empty() {
      return Ok(Self::default());
    }

    let mut parts = parts.split(',');
    let order = parts
      .next()
      .ok_or_else(|| ParseLzwAlgorithmError(s.to_string()))?
      .trim();
    let order = match order {
      "msb" | "Msb" | "MSB" => LzwBitOrder::Msb,
      "lsb" | "Lsb" | "LSB" => LzwBitOrder::Lsb,
      "_" | "-" => LzwBitOrder::Lsb,
      _ => return Err(ParseLzwAlgorithmError(s.to_string())),
    };

    let width = parts
      .next()
      .ok_or_else(|| ParseLzwAlgorithmError(s.to_string()))?
      .trim();

    let width =
      super::parse_or_default::<u8, _>(width).map_err(|_| ParseLzwAlgorithmError(s.to_string()))?;
    Ok(Self::with_order_and_width(order, width))
  }
}

impl LzwAlgorithm {
  /// Creates a new `LzwAlgorithm` with the default order and width.
  #[inline]
  pub const fn new() -> Self {
    Self {
      order: LzwBitOrder::Lsb,
      width: LzwWidth::W8,
    }
  }

  /// Creates a new `LzwAlgorithm` with the order and width.
  #[inline]
  pub const fn with_order_and_width(order: LzwBitOrder, width: LzwWidth) -> Self {
    Self { order, width }
  }

  /// Returns the order of the LZW algorithm.
  #[inline]
  pub const fn order(&self) -> LzwBitOrder {
    self.order
  }

  /// Returns the width of the LZW algorithm.
  #[inline]
  pub const fn width(&self) -> LzwWidth {
    self.width
  }

  /// Encodes the algorithm settings into a single byte.
  /// BitOrder is stored in the first bit.
  /// Width (2 - 12) is stored in the following 4 bits.
  #[inline]
  pub(crate) const fn encode(&self) -> u8 {
    // First bit for order (0 for LSB, 1 for MSB)
    let order_bit = match self.order {
      LzwBitOrder::Msb => 0b1000_0000,
      LzwBitOrder::Lsb => 0b0000_0000,
    };

    // Width (2-12) stored in next 4 bits
    // Since width is already 2-12, we can just shift it
    let width_bits = ((self.width as u8) << 3) & 0b0111_1000;

    order_bit | width_bits
  }

  /// Decodes a single byte into algorithm settings.
  /// BitOrder is extracted from first bit.
  /// Width is extracted from the following 4 bits.
  #[inline]
  pub(crate) const fn decode(byte: u8) -> Self {
    // Extract order from first bit
    let order = if (byte & 0b1000_0000) != 0 {
      LzwBitOrder::Msb
    } else {
      LzwBitOrder::Lsb
    };

    // Extract width from next 4 bits
    // Shift right by 3 to get the original width value
    let width = (byte & 0b0111_1000) >> 3;

    Self::with_order_and_width(order, LzwWidth::from_u8(width))
  }
}

#[cfg(feature = "serde")]
const _: () = {
  use serde::{Deserialize, Serialize};

  #[derive(serde::Serialize, serde::Deserialize)]
  struct LzwAlgorithmHelper {
    order: LzwBitOrder,
    width: LzwWidth,
  }

  impl From<LzwAlgorithm> for LzwAlgorithmHelper {
    fn from(algo: LzwAlgorithm) -> Self {
      Self {
        order: algo.order(),
        width: algo.width(),
      }
    }
  }

  impl From<LzwAlgorithmHelper> for LzwAlgorithm {
    fn from(helper: LzwAlgorithmHelper) -> Self {
      Self::with_order_and_width(helper.order, helper.width)
    }
  }

  impl Serialize for LzwAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
      S: serde::Serializer,
    {
      if serializer.is_human_readable() {
        LzwAlgorithmHelper::from(*self).serialize(serializer)
      } else {
        serializer.serialize_u8(self.encode())
      }
    }
  }

  impl<'de> Deserialize<'de> for LzwAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
      D: serde::Deserializer<'de>,
    {
      if deserializer.is_human_readable() {
        LzwAlgorithmHelper::deserialize(deserializer).map(Into::into)
      } else {
        u8::deserialize(deserializer).map(Self::decode)
      }
    }
  }
};

#[test]
fn parse_str() {
  assert_eq!(
    "msb, 8".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Msb,
      LzwWidth::W8
    ))
  );
  assert_eq!(
    "lsb, 12".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Lsb,
      LzwWidth::W12
    ))
  );
  assert_eq!(
    "msb, 12".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Msb,
      LzwWidth::W12
    ))
  );
  assert_eq!(
    "msb, 2".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Msb,
      LzwWidth::W2
    ))
  );
  assert_eq!(
    "lsb, 2".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Lsb,
      LzwWidth::W2
    ))
  );
  assert_eq!(
    "lsb, 8".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Lsb,
      LzwWidth::W8
    ))
  );
  assert_eq!(
    "-, 100".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Lsb,
      LzwWidth::W12
    ))
  );
  assert_eq!(
    "msb, -".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Msb,
      LzwWidth::W8
    ))
  );
  assert_eq!(
    "_, -".parse::<LzwAlgorithm>(),
    Ok(LzwAlgorithm::with_order_and_width(
      LzwBitOrder::Lsb,
      LzwWidth::W8
    ))
  );
}
