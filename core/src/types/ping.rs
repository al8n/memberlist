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

    paste::paste! {
      #[doc = concat!("Error when transforming a [`", stringify!($name), "`]")]
      pub enum [< $name TransformError >]<I: Transformable, A: Transformable> {
        /// Error transforming the source node
        Source(<Node<I, A> as Transformable>::Error),
        /// Error transforming the target node
        Target(<Node<I, A> as Transformable>::Error),
        /// Encode buffer is too small
        BufferTooSmall,
        /// Not enough bytes to decode
        NotEnoughBytes,
        /// The encoded bytes is too large
        TooLarge,
      }

      impl<I: Transformable, A: Transformable> core::fmt::Debug for [< $name TransformError >]<I, A> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
          match self {
            Self::Source(err) => write!(f, "source node: {:?}", err),
            Self::Target(err) => write!(f, "target node: {:?}", err),
            Self::BufferTooSmall => write!(f, "encode buffer is too small"),
            Self::NotEnoughBytes => write!(f, "not enough bytes to decode"),
            Self::TooLarge => write!(f, "the encoded bytes is too large"),
          }
        }
      }

      impl<I: Transformable, A: Transformable> core::fmt::Display for [< $name TransformError >]<I, A> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
          match self {
            Self::Source(err) => write!(f, "source node: {}", err),
            Self::Target(err) => write!(f, "target node: {}", err),
            Self::BufferTooSmall => write!(f, "encode buffer is too small"),
            Self::NotEnoughBytes => write!(f, "not enough bytes to decode"),
            Self::TooLarge => write!(f, "the encoded bytes is too large"),
          }
        }
      }

      impl<I: Transformable, A: Transformable> std::error::Error for [< $name TransformError >]<I, A> {}

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
          NetworkEndian::write_u32(&mut dst[offset..], self.seq_no);
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
          let seq_no = NetworkEndian::read_u32(&src[offset..]);
          offset += core::mem::size_of::<u32>();
          let (source_len, source) = Node::decode(&src[offset..]).map_err(Self::Error::Source)?;
          offset += source_len;
          let (target_len, target) = Node::decode(&src[offset..]).map_err(Self::Error::Target)?;
          offset += target_len;

          debug_assert_eq!(
            offset, encoded_len,
            "expect bytes read ({encoded_len}) not match actual bytes read ({offset})"
          );
          Ok((offset, Self { seq_no, source, target }))
        }
      }
    }

    #[cfg(test)]
    const _: () = {
      use rand::{Rng, distributions::Alphanumeric, thread_rng, random};

      impl $name<smol_str::SmolStr, std::net::SocketAddr> {
        fn generate(size: usize) -> Self {
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
            seq_no: random(),
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
