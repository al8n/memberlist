use rkyv::{Archive, Deserialize, Serialize};

use super::*;

#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug), repr(transparent))]
#[repr(transparent)]
pub(crate) struct ErrorResponse {
  err: String,
}

impl ErrorResponse {
  pub(crate) fn new(err: String) -> Self {
    Self { err }
  }
}

impl core::fmt::Display for ErrorResponse {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.err)
  }
}

impl std::error::Error for ErrorResponse {}

impl super::Type for ErrorResponse {
  const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

  fn encode(&self) -> Message {
    super::encode::<_, { Self::PREALLOCATE }>(MessageType::ErrorResponse, self)
  }
}
