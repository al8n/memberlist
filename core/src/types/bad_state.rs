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
    pub struct $name<I> {
      incarnation: u32,
      node: I,
      from: I,
    }

    #[cfg(feature = "rkyv")]
    const _: () = {
      use core::fmt::Debug;
      use rkyv::Archive;

      paste::paste! {
        impl<I: Debug + Archive> core::fmt::Debug for [< Archived $name >] <I>
        where
          I::Archived: Debug,
        {
          fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.debug_struct(std::any::type_name::<Self>())
              .field("incarnation", &self.incarnation)
              .field("node", &self.node)
              .field("from", &self.from)
              .finish()
          }
        }

        impl<I: Archive> PartialEq for [< Archived $name >] <I>
        where
          I::Archived: PartialEq,
        {
          fn eq(&self, other: &Self) -> bool {
            self.incarnation == other.incarnation
              && self.node == other.node
              && self.from == other.from
          }
        }

        impl<I: Archive> Eq for [< Archived $name >] <I>
        where
          I::Archived: Eq,
        {
        }

        impl<I: Archive> Clone for [< Archived $name >] <I>
        where
          I::Archived: Clone,
        {
          fn clone(&self) -> Self {
            Self {
              incarnation: self.incarnation,
              node: self.node.clone(),
              from: self.from.clone(),
            }
          }
        }

        impl<I: Archive> core::hash::Hash for [< Archived $name >] <I>
        where
          I::Archived: core::hash::Hash,
        {
          fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
            self.incarnation.hash(state);
            self.node.hash(state);
            self.from.hash(state);
          }
        }
      }
    };
  };
}

bad_bail!(Suspect);
bad_bail!(Dead);

impl<I: PartialEq> Dead<I> {
  #[inline]
  pub(crate) fn is_self(&self) -> bool {
    self.node.eq(&self.from)
  }
}
