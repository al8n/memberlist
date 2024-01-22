use super::CHECKSUM_TAG;

pub(crate) const CHECKSUM_SIZE: usize = core::mem::size_of::<u32>();

/// Checksumer used to calculate checksums.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[non_exhaustive]
#[repr(u8)]
pub enum Checksumer {
  /// Crc32 checksum.
  #[default]
  Crc32 = { *CHECKSUM_TAG.start() },
}

impl Checksumer {
  pub fn checksum(&self, src: &[u8]) -> u32 {
    match self {
      Self::Crc32 => {
        let mut crc32 = Crc32::new();
        crc32.update(src);
        crc32.finalize()
      }
    }
  }
}

impl TryFrom<u8> for Checksumer {
  type Error = UnknownChecksumer;

  fn try_from(value: u8) -> Result<Self, Self::Error> {
    Ok(match value {
      val if val == Self::Crc32 as u8 => Self::Crc32,
      _ => return Err(UnknownChecksumer(value)),
    })
  }
}

/// Unknown checksumer.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnknownChecksumer(u8);

impl core::fmt::Display for UnknownChecksumer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "unknown checksumer tag {}", self.0)
  }
}

impl std::error::Error for UnknownChecksumer {}

/// Checksumer based on crc32.
#[derive(Debug, Clone)]
pub struct Crc32(crc32fast::Hasher);

impl Crc32 {
  fn new() -> Self {
    Self(crc32fast::Hasher::new())
  }

  fn update(&mut self, data: &[u8]) {
    self.0.update(data);
  }

  fn finalize(self) -> u32 {
    self.0.finalize()
  }
}
