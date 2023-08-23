use crate::{DelegateVersion, ProtocolVersion};

use super::*;

use rkyv::{Archive, Deserialize, Serialize};

#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub(crate) struct Alive {
  incarnation: u32,
  meta: Bytes,
  node: NodeId,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

impl super::Type for Alive {
  const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

  fn encode(&self, r1: u8, r2: u8, r3: u8) -> Message {
    super::encode::<_, { Self::PREALLOCATE }>(MessageType::Alive, r1, r2, r3, self)
  }
}
