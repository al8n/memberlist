use super::*;

#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Copy, Clone, PartialEq, Eq)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub(crate) struct PushPullHeader {
  nodes: u32,
  user_state_len: u32, // Encodes the byte lengh of user state
  join: bool,          // Is this a join request or a anti-entropy run
}

impl super::Type for PushPullHeader {
  const PREALLOCATE: usize = super::ENCODE_META_SIZE + 12;

  fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
    super::encode::<C, _, { Self::PREALLOCATE }>(MessageType::PushPull, pv, dv, self)
  }
}

#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub(crate) struct PushNodeState {
  node: NodeId,
  meta: Bytes,
  incarnation: u32,
  state: NodeState,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

impl super::Type for PushNodeState {
  const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

  fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
    todo!()
  }
}
