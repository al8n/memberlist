use rkyv::{Archive, Deserialize, Serialize};

pub(crate) const VSN_SIZE: usize = 2;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct InvalidDelegateVersion(u8);

impl core::fmt::Display for InvalidDelegateVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "V{} is not a valid delegate version", self.0)
  }
}

impl std::error::Error for InvalidDelegateVersion {}

#[derive(
  Debug,
  Copy,
  Clone,
  PartialEq,
  Eq,
  Hash,
  Archive,
  Serialize,
  Deserialize,
  serde::Serialize,
  serde::Deserialize,
)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug, Copy, Clone), repr(u8), non_exhaustive)]
#[non_exhaustive]
#[repr(u8)]
pub enum DelegateVersion {
  V0 = 0,
}

impl core::fmt::Display for DelegateVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      DelegateVersion::V0 => write!(f, "V0"),
    }
  }
}

impl TryFrom<u8> for DelegateVersion {
  type Error = InvalidDelegateVersion;
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    match v {
      0 => Ok(DelegateVersion::V0),
      _ => Err(InvalidDelegateVersion(v)),
    }
  }
}

impl DelegateVersion {
  pub const SIZE: usize = core::mem::size_of::<Self>();
}

impl From<ArchivedDelegateVersion> for DelegateVersion {
  fn from(value: ArchivedDelegateVersion) -> Self {
    match value {
      ArchivedDelegateVersion::V0 => Self::V0,
    }
  }
}

impl From<DelegateVersion> for ArchivedDelegateVersion {
  fn from(value: DelegateVersion) -> Self {
    match value {
      DelegateVersion::V0 => Self::V0,
    }
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct InvalidProtocolVersion(u8);

impl core::fmt::Display for InvalidProtocolVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "V{} is not a valid protocol version", self.0)
  }
}

impl std::error::Error for InvalidProtocolVersion {}

#[derive(
  Debug,
  Copy,
  Clone,
  PartialEq,
  Eq,
  Hash,
  Archive,
  Serialize,
  Deserialize,
  serde::Serialize,
  serde::Deserialize,
)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug, Copy, Clone), repr(u8), non_exhaustive)]
#[non_exhaustive]
#[repr(u8)]
pub enum ProtocolVersion {
  V0 = 0,
}

impl ProtocolVersion {
  pub const SIZE: usize = core::mem::size_of::<Self>();
}

impl core::fmt::Display for ProtocolVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::V0 => write!(f, "V0"),
    }
  }
}

impl TryFrom<u8> for ProtocolVersion {
  type Error = InvalidProtocolVersion;
  fn try_from(v: u8) -> Result<Self, Self::Error> {
    match v {
      0 => Ok(Self::V0),
      _ => Err(InvalidProtocolVersion(v)),
    }
  }
}

impl From<ArchivedProtocolVersion> for ProtocolVersion {
  fn from(value: ArchivedProtocolVersion) -> Self {
    match value {
      ArchivedProtocolVersion::V0 => Self::V0,
    }
  }
}

impl From<ProtocolVersion> for ArchivedProtocolVersion {
  fn from(value: ProtocolVersion) -> Self {
    match value {
      ProtocolVersion::V0 => Self::V0,
    }
  }
}
