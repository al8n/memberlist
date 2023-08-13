use std::borrow::Cow;

use either::Either;

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

    impl $name {
      #[inline]
      pub(crate) fn is_self(&self) -> bool {
        self.node == self.from
      }
    }

    paste::paste! {
      pub(crate) enum [<Cow $name>]<'a> {
        Owned($name),
        Archived(&'a [<Archived $name>], Bytes),
      }

      impl<'a> From<$name> for [<Cow $name>]<'a> {
        fn from(value: $name) -> Self {
          Self::Owned(value)
        }
      }

      impl<'a> From<(&'a [<Archived $name>], Bytes)> for [<Cow $name>]<'a> {
        fn from(value: (&'a [<Archived $name>], Bytes)) -> Self {
          Self::Archived(value.0, value.1)
        }
      }

      impl<'a> [<Cow $name>]<'a> {
        pub(crate) fn from(&self) -> CowNodeId<'_> {
          match self {
            Self::Owned(d) => (&d.from).into(),
            Self::Archived(d, _) => (&d.from).into(),
          }
        }

        pub(crate) fn node(&self) -> CowNodeId<'_> {
          match self {
            Self::Owned(d) => (&d.node).into(),
            Self::Archived(d, _) => (&d.node).into(),
          }
        }

        pub(crate) fn incarnation(&self) -> u32 {
          match self {
            Self::Owned(d) => d.incarnation,
            Self::Archived(d, _) => d.incarnation,
          }
        }

        pub(crate) fn is_self(&self) -> bool {
          self.from() == self.node()
        }

        pub(crate) fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
          match self {
            Self::Owned(d) => d.encode::<C>(pv, dv),
            Self::Archived(_, src) => Message(MessageInner::Bytes(src.clone())),
          }
        }
      }
    }
  };
}

bad_bail!(Suspect);
bad_bail!(Dead);
