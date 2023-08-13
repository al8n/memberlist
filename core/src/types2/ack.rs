use super::*;

use rkyv::{Archive, Deserialize, Serialize};

/// Ack response is sent for a ping
#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
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

  fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
    super::encode::<C, _, { Self::PREALLOCATE }>(MessageType::AckResponse, pv, dv, self)
  }
}

/// nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug, Copy, Clone), repr(transparent))]
#[repr(transparent)]
pub(crate) struct NackResponse {
  seq_no: u32,
}

impl NackResponse {
  #[inline]
  pub fn new(seq_no: u32) -> Self {
    Self { seq_no }
  }
}

impl super::Type for NackResponse {
  const PREALLOCATE: usize = super::ENCODE_META_SIZE + 4;

  fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
    super::encode::<C, _, { Self::PREALLOCATE }>(MessageType::NackResponse, pv, dv, self)
  }
}