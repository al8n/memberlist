use nodecraft::{CheapClone, Node};

use super::{Data, DecodeError, EncodeError};

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
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
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

    paste::paste! {
      const [< $name:upper _SEQUENCE_NUMBER_TAG >]: u8 = 1;
      const [< $name:upper _SEQUENCE_NUMBER_BYTE >]: u8 = super::merge(super::WireType::Varint, [< $name:upper _SEQUENCE_NUMBER_TAG >]);
      const [< $name:upper _SOURCE_ID_TAG >]: u8 = 2;
      const [< $name:upper _SOURCE_ADDR_TAG >]: u8 = 3;
      const [< $name:upper _TARGET_ID_TAG >]: u8 = 4;
      const [< $name:upper _TARGET_ADDR_TAG >]: u8 = 5;

      impl<I, A> $name<I, A> {
        #[inline]
        const fn source_id_byte() -> u8
        where
          I: super::Data,
        {
          super::merge(I::WIRE_TYPE, [< $name:upper _SOURCE_ID_TAG >])
        }

        #[inline]
        const fn source_addr_byte() -> u8
        where
          A: Data,
        {
          super::merge(A::WIRE_TYPE, [< $name:upper _SOURCE_ADDR_TAG >])
        }

        #[inline]
        const fn target_id_byte() -> u8
        where
          I: Data,
        {
          super::merge(I::WIRE_TYPE, [< $name:upper _TARGET_ID_TAG >])
        }

        #[inline]
        const fn target_addr_byte() -> u8
        where
          A: Data,
        {
          super::merge(A::WIRE_TYPE, [< $name:upper _TARGET_ADDR_TAG >])
        }
      }

      impl<I, A> Data for $name<I, A>
      where
        I: Data,
        A: Data,
      {
        type Ref<'a> = $name<I::Ref<'a>, A::Ref<'a>>;

        fn from_ref(val: Self::Ref<'_>) -> Self
        where
          Self: Sized,
        {
          let Self::Ref { sequence_number, source, target } = val;
          Self {
            sequence_number,
            source: Node::from_ref(source),
            target: Node::from_ref(target),
          }
        }

        fn encoded_len(&self) -> usize {
          let mut len = 1 + self.sequence_number.encoded_len();
          len += 1 + self.source.id().encoded_len_with_length_delimited();
          len += 1 + self.source.address().encoded_len_with_length_delimited();
          len += 1 + self.target.id().encoded_len_with_length_delimited();
          len += 1 + self.target.address().encoded_len_with_length_delimited();
          len
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>
        {
          macro_rules! bail {
            ($this:ident($offset:expr, $len:ident)) => {
              if $offset >= $len {
                return Err(EncodeError::insufficient_buffer($this.encoded_len(), $len));
              }
            }
          }

          let len = buf.len();
          let mut offset = 0;

          bail!(self(offset, len));
          buf[offset] = [< $name:upper _SEQUENCE_NUMBER_BYTE >];
          offset += 1;
          offset += self.sequence_number.encode(&mut buf[offset..]).map_err(|e| EncodeError::from(e).update(self.encoded_len(), len))?;

          bail!(self(offset, len));
          buf[offset] = Self::source_id_byte();
          offset += 1;
          offset += self.source.id().encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          bail!(self(offset, len));
          buf[offset] = Self::source_addr_byte();
          offset += 1;
          offset += self.source.address().encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          bail!(self(offset, len));
          buf[offset] = Self::target_id_byte();
          offset += 1;
          offset += self.target.id().encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          bail!(self(offset, len));
          buf[offset] = Self::target_addr_byte();
          offset += 1;
          offset += self.target.address().encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          #[cfg(debug_assertions)]
          super::debug_assert_write_eq(offset, self.encoded_len());
          Ok(offset)
        }

        fn decode_ref(src: &[u8]) -> Result<(usize, Self::Ref<'_>), DecodeError>
        where
          Self: Sized,
        {
          let mut sequence_number = None;
          let mut source_id = None;
          let mut source_addr = None;
          let mut target_id = None;
          let mut target_addr = None;

          let mut offset = 0;
          while offset < src.len() {
            let b = src[offset];
            offset += 1;

            match b {
              [< $name:upper _SEQUENCE_NUMBER_BYTE >] => {
                let (bytes_read, value) = u32::decode(&src[offset..])?;
                offset += bytes_read;
                sequence_number = Some(value);
              }
              b if b == Self::source_id_byte() => {
                let (bytes_read, value) = I::decode_length_delimited_ref(&src[offset..])?;
                offset += bytes_read;
                source_id = Some(value);
              }
              b if b == Self::source_addr_byte() => {
                let (bytes_read, value) = A::decode_length_delimited_ref(&src[offset..])?;
                offset += bytes_read;
                source_addr = Some(value);
              }
              b if b == Self::target_id_byte() => {
                let (bytes_read, value) = I::decode_length_delimited_ref(&src[offset..])?;
                offset += bytes_read;
                target_id = Some(value);
              }
              b if b == Self::target_addr_byte() => {
                let (bytes_read, value) = A::decode_length_delimited_ref(&src[offset..])?;
                offset += bytes_read;
                target_addr = Some(value);
              }
              b => {
                let (wire_type, _) = super::split(b);
                let wire_type = super::WireType::try_from(wire_type)
                  .map_err(|_| DecodeError::new(format!("invalid wire type value {wire_type}")))?;
                offset += super::skip(wire_type, &src[offset..])?;
              }
            }
          }

          let sequence_number = sequence_number.ok_or_else(|| DecodeError::new("missing sequence number"))?;
          let source_id = source_id.ok_or_else(|| DecodeError::new("missing source id"))?;
          let source_addr = source_addr.ok_or_else(|| DecodeError::new("missing source address"))?;
          let target_id = target_id.ok_or_else(|| DecodeError::new("missing target id"))?;
          let target_addr = target_addr.ok_or_else(|| DecodeError::new("missing target address"))?;

          Ok((offset, Self::Ref {
            sequence_number,
            source: Node::new(source_id, source_addr),
            target: Node::new(target_id, target_addr),
          }))
        }
      }
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

    #[cfg(feature = "arbitrary")]
    const _: () = {
      use arbitrary::{Arbitrary, Unstructured};

      impl<'a, I, A> Arbitrary<'a> for $name<I, A>
      where
        I: Arbitrary<'a>,
        A: Arbitrary<'a>,
      {
        fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
          let sequence_number = u.arbitrary()?;
          let source_id = I::arbitrary(u)?;
          let source_addr = A::arbitrary(u)?;
          let target_id = I::arbitrary(u)?;
          let target_addr = A::arbitrary(u)?;
          Ok(Self::new(sequence_number, Node::new(source_id, source_addr), Node::new(target_id, target_addr)))
        }
      }
    };

    #[cfg(feature = "quickcheck")]
    const _: () = {
      use quickcheck::{Arbitrary, Gen};

      impl<I: Arbitrary, A: Arbitrary> Arbitrary for $name<I, A> {
        fn arbitrary(g: &mut Gen) -> Self {
          let sequence_number = u32::arbitrary(g);
          let source_id = I::arbitrary(g);
          let source_addr = A::arbitrary(g);
          let target_id = I::arbitrary(g);
          let target_addr = A::arbitrary(g);

          Self::new(sequence_number, Node::new(source_id, source_addr), Node::new(target_id, target_addr))
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
