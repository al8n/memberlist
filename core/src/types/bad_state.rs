use nodecraft::Node;

macro_rules! bad_bail {
  ($name: ident) => {
    #[viewit::viewit(getters(skip), setters(skip))]
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
    pub struct $name<I, A> {
      incarnation: u32,
      node: Node<I, A>,
      from: Node<I, A>,
    }
  };
}

bad_bail!(Suspect);
bad_bail!(Dead);

impl<I: PartialEq, A> Dead<I, A> {
  #[inline]
  pub(crate) fn is_self(&self) -> bool {
    self.node.id().eq(self.from.id())
  }
}
