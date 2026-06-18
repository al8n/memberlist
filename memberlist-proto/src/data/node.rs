use crate::Node;

use super::{Data, DataRef, DecodeError, EncodeError};
use crate::wire_type::{merge, skip};

const _: () = {
  const NODE_ID_TAG: u8 = 1;
  const NODE_ADDR_TAG: u8 = 2;

  #[inline]
  const fn node_id_byte<I>() -> u8
  where
    I: Data,
  {
    merge(I::WIRE_TYPE, NODE_ID_TAG)
  }

  #[inline]
  const fn node_addr_byte<A>() -> u8
  where
    A: Data,
  {
    merge(A::WIRE_TYPE, NODE_ADDR_TAG)
  }

  impl<'a, I, A> DataRef<'a, Node<I, A>> for Node<I::Ref<'a>, A::Ref<'a>>
  where
    I: Data,
    A: Data,
  {
    fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
    where
      Self: Sized,
    {
      let mut offset = 0;
      let mut id = None;
      let mut address = None;

      while offset < src.len() {
        match src[offset] {
          b if b == node_id_byte::<I>() => {
            if id.is_some() {
              return Err(DecodeError::duplicate_field("Node", "id", NODE_ID_TAG));
            }

            offset += 1;
            let (bytes_read, value) = I::Ref::decode_length_delimited(&src[offset..])?;
            offset += bytes_read;
            id = Some(value);
          }
          b if b == node_addr_byte::<A>() => {
            if address.is_some() {
              return Err(DecodeError::duplicate_field(
                "Node",
                "address",
                NODE_ADDR_TAG,
              ));
            }

            offset += 1;
            let (bytes_read, value) = A::Ref::decode_length_delimited(&src[offset..])?;
            offset += bytes_read;
            address = Some(value);
          }
          _ => offset += skip("Node", &src[offset..])?,
        }
      }

      let id = id.ok_or_else(|| DecodeError::missing_field("Node", "id"))?;
      let address = address.ok_or_else(|| DecodeError::missing_field("Node", "address"))?;
      Ok((offset, Self::new(id, address)))
    }
  }

  impl<I, A> Data for Node<I, A>
  where
    I: Data,
    A: Data,
  {
    type Ref<'a> = Node<I::Ref<'a>, A::Ref<'a>>;

    fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
      let (id, address) = val.into_parts();
      I::from_ref(id).and_then(|id| A::from_ref(address).map(|address| Self::new(id, address)))
    }

    fn encoded_len(&self) -> usize {
      1 + self.id_ref().encoded_len_with_length_delimited()
        + 1
        + self.addr_ref().encoded_len_with_length_delimited()
    }

    fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
      let src_len = buf.len();
      if src_len == 0 {
        return Err(EncodeError::insufficient_buffer(
          self.encoded_len(),
          src_len,
        ));
      }

      let mut offset = 0;
      buf[offset] = node_id_byte::<I>();
      offset += 1;
      offset += self
        .id_ref()
        .encode_length_delimited(&mut buf[offset..])
        .map_err(|e| e.update(self.encoded_len(), src_len))?;

      if offset >= src_len {
        return Err(EncodeError::insufficient_buffer(
          self.encoded_len(),
          src_len,
        ));
      }
      buf[offset] = node_addr_byte::<A>();
      offset += 1;
      offset += self
        .addr_ref()
        .encode_length_delimited(&mut buf[offset..])
        .map_err(|e| e.update(self.encoded_len(), src_len))?;

      #[cfg(debug_assertions)]
      super::debug_assert_write_eq::<Self>(offset, self.encoded_len());
      Ok(offset)
    }
  }
};
