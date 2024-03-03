use byteorder::{ByteOrder, NetworkEndian};
use transformable::Transformable;

use super::MAX_ENCODED_LEN_SIZE;

macro_rules! bad_bail {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[viewit::viewit(
      vis_all = "pub(crate)",
      getters(vis_all = "pub"),
      setters(vis_all = "pub", prefix = "with")
    )]
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
    #[cfg_attr(
      feature = "rkyv",
      derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive)
    )]
    #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
    pub struct $name<I> {
      #[viewit(
        getter(const, attrs(doc = "Returns the incarnation of the message")),
        setter(
          const,
          attrs(doc = "Sets the incarnation of the message (Builder pattern)")
        )
      )]
      incarnation: u32,
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the node of the message")),
        setter(attrs(doc = "Sets the node of the message (Builder pattern)"))
      )]
      node: I,
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the source node of the message")),
        setter(attrs(doc = "Sets the source node of the message (Builder pattern)"))
      )]
      from: I,
    }

    impl<I> $name<I> {
      /// Create a new message
      #[inline]
      pub const fn new(incarnation: u32, node: I, from: I) -> Self {
        Self {
          incarnation,
          node,
          from,
        }
      }

      /// Sets the incarnation of the message
      #[inline]
      pub fn set_incarnation(&mut self, incarnation: u32) -> &mut Self {
        self.incarnation = incarnation;
        self
      }

      /// Sets the source node of the message
      #[inline]
      pub fn set_from(&mut self, source: I) -> &mut Self {
        self.from = source;
        self
      }

      /// Sets the node which in this state
      #[inline]
      pub fn set_node(&mut self, target: I) -> &mut Self {
        self.node = target;
        self
      }
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

    paste::paste! {
      #[doc = concat!("Transform error for [`", stringify!($name), "`]")]
      pub enum [< $name TransformError >] <I: Transformable> {
        /// Transform error for node field
        Node(I::Error),
        /// Transform error for from field
        From(I::Error),
        /// Encode buffer too small
        BufferTooSmall,
        /// The buffer did not contain enough bytes to decode
        NotEnoughBytes,
        /// The encoded size is too large
        TooLarge(u64),
      }

      impl<I: Transformable> core::fmt::Debug for [< $name TransformError >] <I> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
          match self {
            Self::Node(e) => write!(f, "node: {:?}", e),
            Self::From(e) => write!(f, "from: {:?}", e),
            Self::BufferTooSmall => write!(f, "encode buffer too small"),
            Self::NotEnoughBytes => write!(f, concat!("the buffer did not contain enough bytes to decode ", stringify!($name))),
            Self::TooLarge(val) => write!(f, "encoded size too large, max {} got {val}", u32::MAX)
          }
        }
      }

      impl<I: Transformable> core::fmt::Display for [< $name TransformError >] <I> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
          match self {
            Self::Node(e) => write!(f, "node: {}", e),
            Self::From(e) => write!(f, "from: {}", e),
            Self::BufferTooSmall => write!(f, "encode buffer too small"),
            Self::NotEnoughBytes => write!(f, concat!("the buffer did not contain enough bytes to decode ", stringify!($name))),
            Self::TooLarge(val) => write!(f, "encoded message too large, max {} got {val}", u32::MAX),
          }
        }
      }

      impl<I: Transformable> std::error::Error for [< $name TransformError >] <I> {}

      impl<I: Transformable> Transformable for $name <I> {
        type Error = [< $name TransformError >] <I>;

        fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
          let encoded_len = self.encoded_len();

          if encoded_len as u64 > u32::MAX as u64 {
            return Err(Self::Error::TooLarge(encoded_len as u64));
          }

          if encoded_len > dst.len() {
            return Err(Self::Error::BufferTooSmall);
          }

          let mut offset = 0;
          NetworkEndian::write_u32(dst, encoded_len as u32);
          offset += core::mem::size_of::<u32>();
          NetworkEndian::write_u32(&mut dst[offset..], self.incarnation);
          offset += core::mem::size_of::<u32>();
          offset += self.node.encode(&mut dst[offset..]).map_err(Self::Error::Node)?;
          offset += self.from.encode(&mut dst[offset..]).map_err(Self::Error::From)?;

          debug_assert_eq!(
            offset, encoded_len,
            "expect bytes written ({encoded_len}) not match actual bytes writtend ({offset})"
          );
          Ok(offset)
        }

        fn encoded_len(&self) -> usize {
          MAX_ENCODED_LEN_SIZE + core::mem::size_of::<u32>() + self.node.encoded_len() + self.from.encoded_len()
        }

        fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
        where
          Self: Sized {
          if src.len() < core::mem::size_of::<u32>() {
            return Err(Self::Error::NotEnoughBytes);
          }

          let mut offset = 0;
          let encoded_len = NetworkEndian::read_u32(src) as usize;
          offset += core::mem::size_of::<u32>();
          if encoded_len > src.len() {
            return Err(Self::Error::NotEnoughBytes);
          }

          let incarnation = NetworkEndian::read_u32(&src[offset..]);
          offset += core::mem::size_of::<u32>();
          let (readed, node) = I::decode(&src[offset..]).map_err(Self::Error::Node)?;
          offset += readed;
          let (readed, from) = I::decode(&src[offset..]).map_err(Self::Error::From)?;
          offset += readed;

          debug_assert_eq!(
            offset, encoded_len,
            "expect bytes read ({encoded_len}) not match actual bytes read ({offset})"
          );

          Ok((offset, Self {
            incarnation,
            node,
            from,
          }))
        }
      }
    }

    #[cfg(test)]
    const _: () = {
      use rand::{random, Rng, thread_rng, distributions::Alphanumeric};
      impl $name<::smol_str::SmolStr> {
        pub(crate) fn generate(size: usize) -> Self {
          let node = thread_rng()
            .sample_iter(Alphanumeric)
            .take(size)
            .collect::<Vec<u8>>();
          let node = String::from_utf8(node).unwrap().into();

          let from = thread_rng()
            .sample_iter(Alphanumeric)
            .take(size)
            .collect::<Vec<u8>>();
          let from = String::from_utf8(from).unwrap().into();

          Self {
            incarnation: random(),
            node,
            from,
          }
        }
      }
    };
  };
}

bad_bail!(
  /// Suspect message
  Suspect
);
bad_bail!(
  /// Dead message
  Dead
);

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_suspect_encode_decode() {
    for i in 0..100 {
      // Generate and test 100 random instances
      let suspect = Suspect::generate(i);
      let mut buf = vec![0; suspect.encoded_len()];
      let encoded = suspect.encode(&mut buf).unwrap();
      assert_eq!(encoded, buf.len());
      let (read, decoded) = Suspect::<::smol_str::SmolStr>::decode(&buf).unwrap();
      assert_eq!(read, buf.len());
      assert_eq!(suspect.incarnation, decoded.incarnation);
      assert_eq!(suspect.node, decoded.node);
      assert_eq!(suspect.from, decoded.from);
    }
  }

  #[test]
  fn test_dead_encode_decode() {
    for i in 0..100 {
      // Generate and test 100 random instances
      let dead = Dead::generate(i);
      let mut buf = vec![0; dead.encoded_len()];
      let encoded = dead.encode(&mut buf).unwrap();
      assert_eq!(encoded, buf.len());
      let (read, decoded) = Dead::<::smol_str::SmolStr>::decode(&buf).unwrap();
      assert_eq!(read, buf.len());
      assert_eq!(dead.incarnation, decoded.incarnation);
      assert_eq!(dead.node, decoded.node);
      assert_eq!(dead.from, decoded.from);
    }
  }
}
