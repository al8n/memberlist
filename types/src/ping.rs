use byteorder::{ByteOrder, NetworkEndian};
use nodecraft::{CheapClone, Node};
use transformable::Transformable;

use super::MAX_ENCODED_LEN_SIZE;

macro_rules! bail_ping {
  (
    $(#[$meta:meta])*
    $name: ident
  ) => {
    $(#[$meta])*
    #[viewit::viewit(
      getters(vis_all = "pub"),
      setters(vis_all = "pub", prefix = "with")
    )]
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
    #[cfg_attr(feature = "rkyv", derive(::rkyv::Serialize, ::rkyv::Deserialize, ::rkyv::Archive))]
    #[cfg_attr(feature = "rkyv", archive(compare(PartialEq), check_bytes))]
    pub struct $name<I, A> {
      /// The sequence number of the ack
      #[viewit(
        getter(const, attrs(doc = "Returns the sequence number of the ack")),
        setter(
          const,
          attrs(doc = "Sets the sequence number of the ack (Builder pattern)")
        )
      )]
      sequence_number: u32,
      /// Source target, used for a direct reply
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the source node of the ping message")),
        setter(attrs(doc = "Sets the source node of the ping message (Builder pattern)"))
      )]
      source: Node<I, A>,

      /// [`Node`] is sent so the target can verify they are
      /// the intended recipient. This is to protect again an agent
      /// restart with a new name.
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the target node of the ping message")),
        setter(attrs(doc = "Sets the target node of the ping message (Builder pattern)"))
      )]
      target: Node<I, A>,
    }

    impl<I, A> $name<I, A> {
      /// Create a new message
      #[inline]
      pub const fn new(sequence_number: u32, source: Node<I, A>, target: Node<I, A>) -> Self {
        Self {
          sequence_number,
          source,
          target,
        }
      }

      /// Sets the sequence number of the message
      #[inline]
      pub fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
        self.sequence_number = sequence_number;
        self
      }

      /// Sets the source node of the message
      #[inline]
      pub fn set_source(&mut self, source: Node<I, A>) -> &mut Self {
        self.source = source;
        self
      }

      /// Sets the target node of the message
      #[inline]
      pub fn set_target(&mut self, target: Node<I, A>) -> &mut Self {
        self.target = target;
        self
      }
    }

    impl<I: CheapClone, A: CheapClone> CheapClone for $name<I, A> {
      fn cheap_clone(&self) -> Self {
        Self {
          sequence_number: self.sequence_number,
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
              .field("sequence_number", &self.sequence_number)
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
            self.sequence_number == other.sequence_number
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
              sequence_number: self.sequence_number,
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
            self.sequence_number.hash(state);
            self.target.hash(state);
            self.source.hash(state);
          }
        }
      }
    };

    paste::paste! {
      #[doc = concat!("Error when transforming a [`", stringify!($name), "`]")]
      #[derive(thiserror::Error)]
      pub enum [< $name TransformError >]<I: Transformable, A: Transformable> {
        /// Error transforming the source node
        #[error("source node: {0}")]
        Source(<Node<I, A> as Transformable>::Error),
        /// Error transforming the target node
        #[error("target node: {0}")]
        Target(<Node<I, A> as Transformable>::Error),
        /// Encode buffer is too small
        #[error("encode buffer is too small")]
        BufferTooSmall,
        /// Not enough bytes to decode
        #[error("not enough bytes to decode")]
        NotEnoughBytes,
        /// The encoded bytes is too large
        #[error("the encoded bytes is too large")]
        TooLarge,
      }

      impl<I: Transformable, A: Transformable> core::fmt::Debug for [< $name TransformError >]<I, A> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
          write!(f, "{}", self)
        }
      }

      impl<I: Transformable, A: Transformable> Transformable for $name<I, A> {
        type Error = [< $name TransformError >]<I, A>;

        fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
          let encoded_len = self.encoded_len();
          if encoded_len as u64 > u32::MAX as u64 {
            return Err(Self::Error::TooLarge);
          }

          if dst.len() < encoded_len {
            return Err(Self::Error::BufferTooSmall);
          }

          let mut offset = 0;
          NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
          offset += MAX_ENCODED_LEN_SIZE;
          NetworkEndian::write_u32(&mut dst[offset..], self.sequence_number);
          offset += core::mem::size_of::<u32>();
          offset += self.source.encode(&mut dst[offset..]).map_err(Self::Error::Source)?;
          offset += self.target.encode(&mut dst[offset..]).map_err(Self::Error::Target)?;

          debug_assert_eq!(
            offset, encoded_len,
            "expect bytes written ({encoded_len}) not match actual bytes writtend ({offset})"
          );
          Ok(offset)
        }

        fn encoded_len(&self) -> usize {
          MAX_ENCODED_LEN_SIZE
            + core::mem::size_of::<u32>()
            + self.source.encoded_len()
            + self.target.encoded_len()
        }

        fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
        where
          Self: Sized
        {
          if src.len() < MAX_ENCODED_LEN_SIZE {
            return Err(Self::Error::NotEnoughBytes);
          }
          let encoded_len = NetworkEndian::read_u32(src) as usize;
          if src.len() < encoded_len {
            return Err(Self::Error::NotEnoughBytes);
          }
          let mut offset = MAX_ENCODED_LEN_SIZE;
          let sequence_number = NetworkEndian::read_u32(&src[offset..]);
          offset += core::mem::size_of::<u32>();
          let (source_len, source) = Node::decode(&src[offset..]).map_err(Self::Error::Source)?;
          offset += source_len;
          let (target_len, target) = Node::decode(&src[offset..]).map_err(Self::Error::Target)?;
          offset += target_len;

          debug_assert_eq!(
            offset, encoded_len,
            "expect bytes read ({encoded_len}) not match actual bytes read ({offset})"
          );
          Ok((offset, Self { sequence_number, source, target }))
        }
      }
    }

    #[cfg(test)]
    const _: () = {
      use rand::{Rng, distributions::Alphanumeric, thread_rng, random};

      impl $name<smol_str::SmolStr, std::net::SocketAddr> {
        pub(crate) fn generate(size: usize) -> Self {
          let rng = thread_rng();
          let source = rng.sample_iter(&Alphanumeric).take(size).collect::<Vec<u8>>();
          let source = String::from_utf8(source).unwrap();
          let source = Node::new(source.into(), format!("127.0.0.1:{}", thread_rng().gen_range(0..65535))
          .parse()
          .unwrap());
          let rng = thread_rng();
          let target = rng.sample_iter(&Alphanumeric).take(size).collect::<Vec<u8>>();
          let target = String::from_utf8(target).unwrap();
          let target = Node::new(target.into(), format!("127.0.0.1:{}", thread_rng().gen_range(0..65535)).parse().unwrap());

          Self {
            sequence_number: random(),
            source,
            target,
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

impl<I, A> From<IndirectPing<I, A>> for Ping<I, A> {
  fn from(ping: IndirectPing<I, A>) -> Self {
    Self {
      sequence_number: ping.sequence_number,
      source: ping.source,
      target: ping.target,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_ping() {
    for i in 0..100 {
      let ping = Ping::<_, std::net::SocketAddr>::generate(i);
      let mut buf = vec![0; ping.encoded_len()];
      let encoded_len = ping.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, ping.encoded_len());
      let (readed, decoded) = Ping::<_, std::net::SocketAddr>::decode(&buf).unwrap();
      assert_eq!(readed, encoded_len);
      assert_eq!(decoded, ping);
    }
  }

  #[test]
  fn test_indirect_ping() {
    for i in 0..100 {
      let ping = IndirectPing::<_, std::net::SocketAddr>::generate(i);
      let mut buf = vec![0; ping.encoded_len()];
      let encoded_len = ping.encode(&mut buf).unwrap();
      assert_eq!(encoded_len, ping.encoded_len());
      let (readed, decoded) = IndirectPing::<_, std::net::SocketAddr>::decode(&buf).unwrap();
      assert_eq!(readed, encoded_len);
      assert_eq!(decoded, ping);
    }
  }
}
