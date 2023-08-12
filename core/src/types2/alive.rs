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

  fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
    super::encode::<C, _, { Self::PREALLOCATE }>(MessageType::Alive, pv, dv, self)
  }
}
