use super::{Data, DataRef, DecodeError, EncodeError, WireType};

macro_rules! bad_bail {
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
    pub struct $name<I> {
      /// The incarnation of the message.
      #[viewit(
        getter(const, attrs(doc = "Returns the incarnation of the message")),
        setter(
          const,
          attrs(doc = "Sets the incarnation of the message (Builder pattern)")
        )
      )]
      incarnation: u32,
      /// The node of the message.
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the node of the message")),
        setter(attrs(doc = "Sets the node of the message (Builder pattern)"))
      )]
      node: I,
      /// The source node of the message.
      #[viewit(
        getter(const, style = "ref", attrs(doc = "Returns the source node of the message")),
        setter(attrs(doc = "Sets the source node of the message (Builder pattern)"))
      )]
      from: I,
    }

    paste::paste! {
      const [< $name:upper _INCARNATION_TAG >]: u8 = 1;
      const [< $name:upper _INCARNATION_BYTE >]: u8 = super::merge(WireType::Varint, [< $name:upper _ INCARNATION_TAG >]);

      const [< $name:upper _NODE_TAG >]: u8 = 2;
      const [< $name:upper _FROM_TAG >]: u8 = 3;

      impl<'a, I: Data> DataRef<'a, $name<I>> for $name<I::Ref<'a>> {
        fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
        where
          Self: Sized,
        {
          let mut node = None;
          let mut from = None;
          let mut incarnation = None;

          let mut offset = 0;
          while offset < src.len() {
            match src[offset] {
              [< $name:upper _INCARNATION_BYTE >] => {
                if incarnation.is_some() {
                  return Err(DecodeError::duplicate_field(stringify!($name), "incarnation", [< $name:upper _INCARNATION_TAG >]));
                }
                offset += 1;

                let (bytes_read, value) = <u32 as DataRef<u32>>::decode(&src[offset..])?;
                offset += bytes_read;
                incarnation = Some(value);
              }
              b if b == $name::<I>::node_byte() => {
                if node.is_some() {
                  return Err(DecodeError::duplicate_field(stringify!($name), "node", $name::<I>::node_byte()));
                }
                offset += 1;

                let (bytes_read, value) = I::Ref::decode_length_delimited(&src[offset..])?;
                offset += bytes_read;
                node = Some(value);
              }
              b if b == $name::<I>::from_byte() => {
                if from.is_some() {
                  return Err(DecodeError::duplicate_field(stringify!($name), "from", $name::<I>::from_byte()));
                }
                offset += 1;

                let (bytes_read, value) = I::Ref::decode_length_delimited(&src[offset..])?;
                offset += bytes_read;
                from = Some(value);
              }
              b => {
                let (wire_type, _) = super::split(b);
                let wire_type = WireType::try_from(wire_type)
                  .map_err(|v| DecodeError::unknown_wire_type(stringify!($name), v))?;
                offset += super::skip(wire_type, &src[offset..])?;
              }
            }
          }

          let incarnation = incarnation.ok_or_else(|| DecodeError::missing_field(stringify!($name), "incarnation"))?;
          let node = node.ok_or_else(|| DecodeError::missing_field(stringify!($name), "node"))?;
          let from = from.ok_or_else(|| DecodeError::missing_field(stringify!($name), "from"))?;

          Ok((offset, Self {
            incarnation,
            node,
            from,
          }))
        }
      }

      impl<I: Data> Data for $name<I> {
        type Ref<'a> = $name<I::Ref<'a>>;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
          let Self::Ref { incarnation, node, from } = val;
          I::from_ref(node)
            .and_then(|node| I::from_ref(from).map(|from| Self::new(incarnation, node, from)))
        }

        fn encoded_len(&self) -> usize {
          let mut len = 1 + self.incarnation.encoded_len();
          len += 1 + self.node.encoded_len_with_length_delimited();
          len += 1 + self.from.encoded_len_with_length_delimited();
          len
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
          macro_rules! bail {
            ($this:ident($offset:expr, $len:ident)) => {
              if $offset >= $len {
                return Err(EncodeError::insufficient_buffer($this.encoded_len(), $len));
              }
            };
          }

          let len = buf.len();
          bail!(self(0, len));

          let mut offset = 0;
          buf[offset] = [< $name:upper _INCARNATION_BYTE >];
          offset += 1;
          offset += self.incarnation.encode(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          bail!(self(offset, len));
          buf[offset] = Self::node_byte();
          offset += 1;
          offset += self.node.encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          bail!(self(offset, len));
          buf[offset] = Self::from_byte();
          offset += 1;
          offset += self.from.encode_length_delimited(&mut buf[offset..]).map_err(|e| e.update(self.encoded_len(), len))?;

          #[cfg(debug_assertions)]
          super::debug_assert_write_eq::<Self>(offset, self.encoded_len());
          Ok(offset)
        }
      }

      impl<I> $name<I> {
        #[inline]
        const fn node_byte() -> u8
        where
          I: Data,
        {
          super::merge(I::WIRE_TYPE, [< $name:upper _NODE_TAG >])
        }

        #[inline]
        const fn from_byte() -> u8
        where
          I: Data,
        {
          super::merge(I::WIRE_TYPE, [< $name:upper _FROM_TAG >])
        }
      }
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
