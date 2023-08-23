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
  const PREALLOCATE: usize = super::ENCODE_HEADER_SIZE + 12;

  fn encode(&self, r1: u8, r2: u8) -> Message {
    super::encode::<_, { Self::PREALLOCATE }>(MessageType::PushPull, r1, r2, self)
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

impl PushNodeState {
  pub(crate) fn encode(&self) -> Message {
    // let mut ser = AllocSerializer::<{ super::DEFAULT_ENCODE_PREALLOCATE_SIZE }>::default();
    // ser.write(&[0, 0, 0, 0]).unwrap();
    // ser
    //   .serialize_value(self)
    //   .map(|_| {
    //     let mut data = ser.into_serializer().into_inner();
    //     let len = (data.len() - 4) as u32;
    //     data[..4].copy_from_slice(&len.to_be_bytes());
    //     Message(MessageInner::Aligned(data))
    //   })
    //   .unwrap()
    todo!()
  }
}
