/// Delegate version
#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[cfg_attr(
  feature = "serde",
  derive(serde::Serialize, serde::Deserialize),
  serde(into = "u8", from = "u8")
)]
#[non_exhaustive]
pub enum DelegateVersion {
  /// Version 1
  #[default]
  #[display("v1")]
  V1,
  /// Unknown version (used for forwards and backwards compatibility)
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl From<u8> for DelegateVersion {
  fn from(v: u8) -> Self {
    match v {
      1 => Self::V1,
      val => Self::Unknown(val),
    }
  }
}

impl From<DelegateVersion> for u8 {
  fn from(v: DelegateVersion) -> Self {
    match v {
      DelegateVersion::V1 => 1,
      DelegateVersion::Unknown(val) => val,
    }
  }
}

/// Protocol version
#[derive(
  Debug, Default, Copy, Clone, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[cfg_attr(
  feature = "serde",
  derive(serde::Serialize, serde::Deserialize),
  serde(into = "u8", from = "u8")
)]
#[non_exhaustive]
pub enum ProtocolVersion {
  /// Version 1
  #[default]
  #[display("v1")]
  V1,
  /// Unknown version (used for forwards and backwards compatibility)
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl From<u8> for ProtocolVersion {
  fn from(v: u8) -> Self {
    match v {
      1 => Self::V1,
      val => Self::Unknown(val),
    }
  }
}

impl From<ProtocolVersion> for u8 {
  fn from(v: ProtocolVersion) -> Self {
    match v {
      ProtocolVersion::V1 => 1,
      ProtocolVersion::Unknown(val) => val,
    }
  }
}
