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

#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub(crate) struct PushPullMessage {
  header: PushPullHeader,
  body: Vec<PushNodeState>,
  user_data: Option<Bytes>,
}

impl super::Type for PushPullMessage {
  const PREALLOCATE: usize = super::ENCODE_HEADER_SIZE + 12;

  fn encode(&self) -> Message {
    super::encode::<_, { Self::PREALLOCATE }>(MessageType::PushPull, self)
  }
}

impl PushPullMessage {
  pub(crate) fn new(header: PushPullHeader, body: Vec<PushNodeState>, user_data: Bytes) -> Self {
    Self {
      header,
      body,
      user_data: (!user_data.is_empty()).then_some(user_data),
    }
  }
}
