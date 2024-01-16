use super::*;

/// Ack response is sent for a ping
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(derive(Debug, Clone, PartialEq, Eq, Hash))
)]
pub(crate) struct AckResponse {
  seq_no: u32,
  payload: Bytes,
}

impl AckResponse {
  #[inline]
  pub fn empty(seq_no: u32) -> Self {
    Self {
      seq_no,
      payload: Bytes::new(),
    }
  }
}

impl super::Type for AckResponse {
  const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

  fn encode(&self) -> Message {
    super::encode::<_, { Self::PREALLOCATE }>(MessageType::AckResponse, self)
  }
}

/// nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[cfg_attr(
  feature = "rkyv",
  derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
)]
#[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
#[cfg_attr(
  feature = "rkyv",
  archive_attr(derive(Debug, Clone, PartialEq, Eq, Hash), repr(transparent))
)]
#[repr(transparent)]
pub struct NackResponse {
  seq_no: u32,
}

impl NackResponse {
  #[inline]
  pub fn new(seq_no: u32) -> Self {
    Self { seq_no }
  }
}

impl super::Type for NackResponse {
  const PREALLOCATE: usize = super::ENCODE_HEADER_SIZE + 4;

  fn encode(&self) -> Message {
    super::encode::<_, { Self::PREALLOCATE }>(MessageType::NackResponse, self)
  }
}
