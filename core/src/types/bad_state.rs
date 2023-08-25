use super::*;

macro_rules! bad_bail {
  ($name: ident) => {
    #[viewit::viewit(getters(skip), setters(skip))]
    #[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
    #[archive(compare(PartialEq), check_bytes)]
    #[archive_attr(derive(Debug))]
    pub(crate) struct $name {
      incarnation: u32,
      node: NodeId,
      from: NodeId,
    }

    impl super::Type for $name {
      const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

      fn encode(&self, r1: u8, r2: u8) -> Message {
        super::encode::<_, { Self::PREALLOCATE }>(MessageType::$name, r1, r2, self)
      }
    }
  };
}

bad_bail!(Suspect);
bad_bail!(Dead);

impl Dead {
  #[inline]
  pub(crate) fn is_self(&self) -> bool {
    self.node == self.from
  }
}
