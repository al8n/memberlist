use length_delimited::Varint;

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
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    #[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
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
      const [< $name:upper _INCARNATION_BYTE >]: u8 = super::merge(super::WireType::Varint, [< $name:upper _ INCARNATION_TAG >]);

      const [< $name:upper _NODE_TAG >]: u8 = 2;
      const [< $name:upper _FROM_TAG >]: u8 = 3;

      impl<I> $name<I> {
        #[inline]
        const fn node_byte() -> u8
        where
          I: super::Data,
        {
          super::merge(I::WIRE_TYPE, [< $name:upper _NODE_TAG >])
        }

        #[inline]
        const fn from_byte() -> u8
        where
          I: super::Data,
        {
          super::merge(I::WIRE_TYPE, [< $name:upper _FROM_TAG >])
        }

        /// Returns the encoded length of the message
        #[inline]
        pub fn encoded_len(&self) -> usize
        where
          I: super::Data,
        {
          let mut len = 1 + super::encoded_u32_varint_len(self.incarnation);
          len += 1 + super::encoded_data_len(&self.node);
          len += 1 + super::encoded_data_len(&self.from);
          len
        }

        /// Encodes the message into the buffer
        ///
        /// An error will be returned if the buffer does not have sufficient capacity.
        pub fn encode(&self, buf: &mut [u8]) -> Result<usize, super::EncodeError>
        where
          I: super::Data,
        {
          macro_rules! bail {
            ($this:ident($len:ident)) => {
              return Err(super::EncodeError::InsufficientBuffer(super::InsufficientBuffer::with_information($this.encoded_len() as u64, $len as u64)));
            };
          }

          let len = buf.len();
          if len < 1 {
            bail!(self(len));
          }

          let mut offset = 0;
          buf[offset] = [< $name:upper _INCARNATION_BYTE >];
          offset += 1;
          offset += self.incarnation.encode(&mut buf[offset..])?;

          if offset + 1 >= len {
            bail!(self(len));
          }
          buf[offset] = Self::node_byte();
          offset += 1;
          offset += super::encode_data(&self.node, &mut buf[offset..]).map_err(|e| e.with_information(self.encoded_len() as u64, len as u64))?;

          if offset + 1 >= len {
            bail!(self(len));
          }
          buf[offset] = Self::from_byte();
          offset += 1;
          offset += super::encode_data(&self.from, &mut buf[offset..]).map_err(|e| e.with_information(self.encoded_len() as u64, len as u64))?;

          Ok(offset)
        }

        /// Decodes the message from the buffer
        pub fn decode(src: &[u8]) -> Result<(usize, Self), super::DecodeError>
        where
          I: super::Data,
        {
          let mut node = None;
          let mut from = None;
          let mut incarnation = None;

          let mut offset = 0;
          while offset < src.len() {
            let b = src[offset];
            offset += 1;

            match b {
              [< $name:upper _INCARNATION_BYTE >] => {
                let (bytes_read, value) = super::decode_varint(super::WireType::Varint, &src[offset..])?;
                offset += bytes_read;
                incarnation = Some(value);
              }
              b if b == Self::node_byte() => {
                let (bytes_read, value) = super::decode_data::<I>(&src[offset..])?;
                offset += bytes_read;
                node = Some(value);
              }
              b if b == Self::from_byte() => {
                let (bytes_read, value) = super::decode_data::<I>(&src[offset..])?;
                offset += bytes_read;
                from = Some(value);
              }
              b => {
                let (wire_type, _) = super::split(b);
                let wire_type = super::WireType::try_from(wire_type)
                  .map_err(|_| super::DecodeError::new(format!("invalid wire type value {wire_type}")))?;
                offset += super::skip(wire_type, &src[offset..])?;
              } 
            }
          }

          let incarnation = incarnation.ok_or_else(|| super::DecodeError::new("missing incarnation"))?;
          let node = node.ok_or_else(|| super::DecodeError::new("missing node"))?;
          let from = from.ok_or_else(|| super::DecodeError::new("missing from"))?;

          Ok((offset, Self {
            incarnation,
            node,
            from,
          }))
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

    #[cfg(test)]
    const _: () = {
      use rand::{random, Rng, rng, distr::Alphanumeric};
      impl $name<::smol_str::SmolStr> {
        pub(crate) fn generate(size: usize) -> Self {
          let node = rng()
            .sample_iter(Alphanumeric)
            .take(size)
            .collect::<Vec<u8>>();
          let node = String::from_utf8(node).unwrap().into();

          let from = rng()
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
  // use super::*;

  // #[test]
  // fn test_suspect_encode_decode() {
  //   for i in 0..100 {
  //     // Generate and test 100 random instances
  //     let suspect = Suspect::generate(i);
  //     let mut buf = vec![0; suspect.encoded_len()];
  //     let encoded = suspect.encode(&mut buf).unwrap();
  //     assert_eq!(encoded, buf.len());
  //     let (read, decoded) = Suspect::<::smol_str::SmolStr>::decode(&buf).unwrap();
  //     assert_eq!(read, buf.len());
  //     assert_eq!(suspect.incarnation, decoded.incarnation);
  //     assert_eq!(suspect.node, decoded.node);
  //     assert_eq!(suspect.from, decoded.from);
  //   }
  // }

  // #[test]
  // fn test_dead_encode_decode() {
  //   for i in 0..100 {
  //     // Generate and test 100 random instances
  //     let dead = Dead::generate(i);
  //     let mut buf = vec![0; dead.encoded_len()];
  //     let encoded = dead.encode(&mut buf).unwrap();
  //     assert_eq!(encoded, buf.len());
  //     let (read, decoded) = Dead::<::smol_str::SmolStr>::decode(&buf).unwrap();
  //     assert_eq!(read, buf.len());
  //     assert_eq!(dead.incarnation, decoded.incarnation);
  //     assert_eq!(dead.node, decoded.node);
  //     assert_eq!(dead.from, decoded.from);
  //   }
  // }
}
