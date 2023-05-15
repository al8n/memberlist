use showbiz_types::InvalidMessageType;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error("showbiz: label is too long. expected at most 255 bytes, got {0}")]
  LabelTooLong(usize),
  #[error("showbiz: invalid message type {0}")]
  InvalidMessageType(#[from] InvalidMessageType),
  #[error("showbiz: cannot decode label; packet has been truncated")]
  TruncatedLabel,
  #[error("showbiz: label header cannot be empty when present")]
  EmptyLabel,
  #[error("showbiz: io error {0}")]
  IO(#[from] std::io::Error),
  #[error("showbiz: remote node state(size {0}) is larger than limit")]
  LargeRemoteState(usize),
  #[error("showbiz: security error {0}")]
  Security(#[from] crate::security::SecurityError),
}

impl PartialEq for Error {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::LabelTooLong(a), Self::LabelTooLong(b)) => a == b,
      (Self::InvalidMessageType(a), Self::InvalidMessageType(b)) => a == b,
      (Self::TruncatedLabel, Self::TruncatedLabel) => true,
      (Self::EmptyLabel, Self::EmptyLabel) => true,
      (Self::IO(a), Self::IO(b)) => a.kind() == b.kind(),
      _ => false,
    }
  }
}
