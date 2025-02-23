use nodecraft::{CheapClone, Node};

use super::{Data, DataRef, DecodeError, EncodeError};

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
    #[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
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
      const [< $name:upper _SOURCE_TAG >]: u8 = 2;
      const [< $name:upper _TARGET_TAG >]: u8 = 3;

      impl<I, A> $name<I, A> {
        #[inline]
        const fn source_byte() -> u8
        where
          I: Data,
          A: Data,
        {
          super::merge(super::WireType::LengthDelimited, [< $name:upper _SOURCE_TAG >])
        }

        #[inline]
        const fn target_byte() -> u8
        where
          I: Data,
          A: Data,
        {
          super::merge(super::WireType::LengthDelimited, [< $name:upper _TARGET_TAG >])
        }
      }

      impl<'a, I, A> DataRef<'a, $name<I, A>> for $name<I::Ref<'a>, A::Ref<'a>>
      where
        I: Data,
        A: Data,
      {
        fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
        where
          Self: Sized,
        {
          let mut sequence_number = None;
          let mut source = None;
          let mut target = None;

          let mut offset = 0;
          while offset < src.len() {
            match src[offset] {
              [< $name:upper _SEQUENCE_NUMBER_BYTE >] => {
                if sequence_number.is_some() {
                  return Err(DecodeError::duplicate_field(stringify!($name), "sequence number", [< $name:upper _SEQUENCE_NUMBER_TAG >]));
                }
                offset += 1;

                let (bytes_read, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
                offset += bytes_read;
                sequence_number = Some(value);
              }
              b if b == $name::<I, A>::source_byte() => {
                if source.is_some() {
                  return Err(DecodeError::duplicate_field(stringify!($name), "source", $name::<I, A>::source_byte()));
                }
                offset += 1;

                let (bytes_read, value) = <Node<I::Ref<'_>, A::Ref<'_>> as DataRef<Node<I, A>>>::decode_length_delimited(&src[offset..])?;
                offset += bytes_read;
                source = Some(value);
              }
              b if b == $name::<I, A>::target_byte() => {
                if target.is_some() {
                  return Err(DecodeError::duplicate_field(stringify!($name), "target", $name::<I, A>::target_byte()));
                }
                offset += 1;

                let (bytes_read, value) = <Node<I::Ref<'_>, A::Ref<'_>> as DataRef<Node<I, A>>>::decode_length_delimited(&src[offset..])?;
                offset += bytes_read;
                target = Some(value);
              }
              _ => offset += super::skip(stringify!($name), &src[offset..])?,
            }
          }

          let sequence_number = sequence_number.ok_or_else(|| DecodeError::missing_field(stringify!($name), "sequence number"))?;
          let source = source.ok_or_else(|| DecodeError::missing_field(stringify!($name), "source"))?;
          let target = target.ok_or_else(|| DecodeError::missing_field(stringify!($name), "target"))?;

          Ok((offset, Self {
            sequence_number,
            source,
            target,
          }))
        }
      }

      impl<I, A> Data for $name<I, A>
      where
        I: Data,
        A: Data,
      {
        type Ref<'a> = $name<I::Ref<'a>, A::Ref<'a>>;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
        where
          Self: Sized,
        {
          let Self::Ref { sequence_number, source, target } = val;
          Node::from_ref(source)
            .and_then(|source| Node::from_ref(target).map(|target| (source, target)))
            .map(|(source, target)| Self::new(sequence_number, source, target))
        }

        fn encoded_len(&self) -> usize {
          let mut len = 1 + self.sequence_number.encoded_len();
          len += 1 + self.source.encoded_len_with_length_delimited();
          len += 1 + self.target.encoded_len_with_length_delimited();
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
          buf[offset] = Self::source_byte();
          offset += 1;
          offset += self.source.encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          bail!(self(offset, len));
          buf[offset] = Self::target_byte();
          offset += 1;
          offset += self.target.encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          #[cfg(debug_assertions)]
          super::debug_assert_write_eq::<Self>(offset, self.encoded_len());
          Ok(offset)
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
