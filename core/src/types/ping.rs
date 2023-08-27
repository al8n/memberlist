use super::*;

macro_rules! bail_ping {
  ($name: ident) => {
    #[viewit::viewit(getters(skip), setters(skip))]
    #[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
    #[archive(compare(PartialEq), check_bytes)]
    #[archive_attr(derive(Debug))]
    pub(crate) struct $name {
      seq_no: u32,
      /// Source node, used for a direct reply
      source: NodeId,

      /// `NodeId` is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      target: NodeId,
    }

    impl super::Type for $name {
      const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

      fn encode(&self) -> Message {
        super::encode::<_, { Self::PREALLOCATE }>(MessageType::$name, self)
      }
    }
  };
}

bail_ping!(Ping);
bail_ping!(IndirectPing);

impl From<Ping> for IndirectPing {
  fn from(ping: Ping) -> Self {
    Self {
      seq_no: ping.seq_no,
      source: ping.source,
      target: ping.target,
    }
  }
}

impl From<IndirectPing> for Ping {
  fn from(ping: IndirectPing) -> Self {
    Self {
      seq_no: ping.seq_no,
      source: ping.source,
      target: ping.target,
    }
  }
}
