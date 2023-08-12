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

      fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
        super::encode::<C, _, { Self::PREALLOCATE }>(MessageType::$name, pv, dv, self)
      }
    }
  };
}

bad_bail!(Suspect);
bad_bail!(Dead);

impl Dead {
  #[inline]
  pub(crate) fn dead_self(&self) -> bool {
    self.node.name == self.from.name
  }
}
