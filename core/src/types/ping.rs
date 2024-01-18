use nodecraft::{CheapClone, Node};

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
    pub struct $name<I, A> {
      seq_no: u32,
      /// Source target, used for a direct reply
      source: Node<I, A>,

      /// [`Node`] is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      target: Node<I, A>,
    }


    impl<I: CheapClone, A: CheapClone> CheapClone for $name<I, A> {
      fn cheap_clone(&self) -> Self {
        Self {
          seq_no: self.seq_no,
          source: self.source.cheap_clone(),
          target: self.target.cheap_clone(),
        }
      }
    }

    #[cfg(feature = "rkyv")]
    const _: () = {
      use core::fmt::Debug;
      use rkyv::Archive;

      paste::paste! {
        impl<I: Debug + Archive, A: Debug + Archive> core::fmt::Debug for [< Archived $name >] <I, A>
        where
          I::Archived: Debug,
          A::Archived: Debug,
        {
          fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.debug_struct(std::any::type_name::<Self>())
              .field("seq_no", &self.seq_no)
              .field("target", &self.target)
              .field("source", &self.source)
              .finish()
          }
        }

        impl<I: Archive, A: Archive> PartialEq for [< Archived $name >] <I, A>
        where
          I::Archived: PartialEq,
          A::Archived: PartialEq,
        {
          fn eq(&self, other: &Self) -> bool {
            self.seq_no == other.seq_no
              && self.target == other.target
              && self.source == other.source
          }
        }

        impl<I: Archive, A: Archive> Eq for [< Archived $name >] <I, A>
        where
          I::Archived: Eq,
          A::Archived: Eq,
        {
        }

        impl<I: Archive, A: Archive> Clone for [< Archived $name >] <I, A>
        where
          I::Archived: Clone,
          A::Archived: Clone,
        {
          fn clone(&self) -> Self {
            Self {
              seq_no: self.seq_no,
              target: self.target.clone(),
              source: self.source.clone(),
            }
          }
        }

        impl<I: Archive, A: Archive> core::hash::Hash for [< Archived $name >] <I, A>
        where
          I::Archived: core::hash::Hash,
          A::Archived: core::hash::Hash,
        {
          fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
            self.seq_no.hash(state);
            self.target.hash(state);
            self.source.hash(state);
          }
        }
      }
    };
  };
}

bail_ping!(
  #[doc = "Ping is sent to a target to check if it is alive"]
  Ping
);
bail_ping!(
  #[doc = "IndirectPing is sent to a target to check if it is alive"]
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
