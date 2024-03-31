/// Unknown delegate version
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("V{0} is not a valid delegate version")]
pub struct UnknownDelegateVersion(u8);

/// Delegate version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(
    derive(Debug, Copy, Clone, Eq, PartialEq, Hash),
    repr(u8),
    non_exhaustive
  )
)]
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

#[cfg(feature = "rkyv")]
const _: () = {
  impl From<ArchivedDelegateVersion> for DelegateVersion {
    fn from(value: ArchivedDelegateVersion) -> Self {
      match value {
        ArchivedDelegateVersion::V1 => Self::V1,
      }
    }
  }

  impl From<DelegateVersion> for ArchivedDelegateVersion {
    fn from(value: DelegateVersion) -> Self {
      match value {
        DelegateVersion::V1 => Self::V1,
      }
    }
  }
};

/// Unknown protocol version
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, thiserror::Error)]
#[error("V{0} is not a valid protocol version")]
pub struct UnknownProtocolVersion(u8);

/// Protocol version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(
    derive(Debug, Copy, Clone, Eq, PartialEq, Hash),
    repr(u8),
    non_exhaustive
  )
)]
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

#[cfg(feature = "rkyv")]
const _: () = {
  impl From<ArchivedProtocolVersion> for ProtocolVersion {
    fn from(value: ArchivedProtocolVersion) -> Self {
      match value {
        ArchivedProtocolVersion::V1 => Self::V1,
      }
    }
  }

  impl From<ProtocolVersion> for ArchivedProtocolVersion {
    fn from(value: ProtocolVersion) -> Self {
      match value {
        ProtocolVersion::V1 => Self::V1,
      }
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_delegate_version() {
    assert_eq!(DelegateVersion::V1 as u8, 1);
    assert_eq!(DelegateVersion::V1.to_string(), "V1");
    assert_eq!(DelegateVersion::try_from(1), Ok(DelegateVersion::V1));
    assert_eq!(DelegateVersion::try_from(1), Err(UnknownDelegateVersion(1)));
  }

  #[test]
  fn test_protocol_version() {
    assert_eq!(ProtocolVersion::V1 as u8, 1);
    assert_eq!(ProtocolVersion::V1.to_string(), "V1");
    assert_eq!(ProtocolVersion::try_from(1), Ok(ProtocolVersion::V1));
    assert_eq!(ProtocolVersion::try_from(1), Err(UnknownProtocolVersion(1)));
  }
}
