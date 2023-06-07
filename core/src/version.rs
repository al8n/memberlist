pub(crate) const VSN_SIZE: usize = 2;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct InvalidDelegateVersion(u8);

impl core::fmt::Display for InvalidDelegateVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "V{} is not a valid delegate version", self.0)
  }
}

impl std::error::Error for InvalidDelegateVersion {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct InvalidProtocolVersion(u8);

impl core::fmt::Display for InvalidProtocolVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "V{} is not a valid protocol version", self.0)
  }
}

impl std::error::Error for InvalidProtocolVersion {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum ProtocolVersion {
  V0 = 0,
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
