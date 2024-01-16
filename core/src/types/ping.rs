use nodecraft::Node;

macro_rules! bail_ping {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[viewit::viewit(getters(skip), setters(skip))]
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
    #[cfg_attr(feature = "rkyv", derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive))]
    #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
    #[cfg_attr(feature = "rkyv", archive_attr(derive(Debug, Clone, PartialEq, Eq, Hash)))]
    pub struct $name<I, A> {
      seq_no: u32,
      /// Source node, used for a direct reply
      source: Node<I, A>,

      /// [`Node`] is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      target: Node<I, A>,
    }
  };
}

bail_ping!(
  #[doc = "Ping is sent to a node to check if it is alive"]
  Ping
);
bail_ping!(
  #[doc = "IndirectPing is sent to a node to check if it is alive"]
  IndirectPing
);

impl<I, A> From<Ping<I, A>> for IndirectPing<I, A> {
  fn from(ping: Ping<I, A>) -> Self {
    Self {
      seq_no: ping.seq_no,
      source: ping.source,
      target: ping.target,
    }
  }
}

impl<I, A> From<IndirectPing<I, A>> for Ping<I, A> {
  fn from(ping: IndirectPing<I, A>) -> Self {
    Self {
      seq_no: ping.seq_no,
      source: ping.source,
      target: ping.target,
    }
  }
}
