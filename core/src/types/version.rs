/// Unknown delegate version
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("V{0} is not a valid delegate version")]
pub struct UnknownDelegateVersion(u8);

/// Delegate version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[non_exhaustive]
#[repr(u8)]
pub enum DelegateVersion {
  /// Version 1
  #[default]
  V1 = 1,
}

impl core::fmt::Display for DelegateVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      DelegateVersion::V1 => write!(f, "V1"),
    }
  }
}

impl TryFrom<u8> for DelegateVersion {
  type Error = UnknownDelegateVersion;
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    match v {
      1 => Ok(DelegateVersion::V1),
      _ => Err(UnknownDelegateVersion(v)),
    }
  }
}


/// Unknown protocol version
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("V{0} is not a valid protocol version")]
pub struct UnknownProtocolVersion(u8);

/// Protocol version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[non_exhaustive]
#[repr(u8)]
pub enum ProtocolVersion {
  /// Version 1
  #[default]
  V1 = 1,
}

impl core::fmt::Display for ProtocolVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::V1 => write!(f, "V1"),
    }
  }
}

impl TryFrom<u8> for ProtocolVersion {
  type Error = UnknownProtocolVersion;
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    match v {
      1 => Ok(Self::V1),
      _ => Err(UnknownProtocolVersion(v)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_delegate_version() {
    assert_eq!(DelegateVersion::V1 as u8, 1);
    assert_eq!(DelegateVersion::V1.to_string(), "V1");
    assert_eq!(DelegateVersion::try_from(1), Ok(DelegateVersion::V1));
    assert_eq!(DelegateVersion::try_from(2), Err(UnknownDelegateVersion(2)));
  }

  #[test]
  fn test_protocol_version() {
    assert_eq!(ProtocolVersion::V1 as u8, 1);
    assert_eq!(ProtocolVersion::V1.to_string(), "V1");
    assert_eq!(ProtocolVersion::try_from(1), Ok(ProtocolVersion::V1));
    assert_eq!(ProtocolVersion::try_from(2), Err(UnknownProtocolVersion(2)));
  }
}
