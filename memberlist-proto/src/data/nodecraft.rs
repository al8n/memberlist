use std::net::SocketAddr;

use either::Either;
use nodecraft::{Domain, DomainRef, HostAddr, HostAddrRef, Node, NodeId, NodeIdRef};

use super::{
  super::{WireType, merge, skip},
  Data, DataRef, DecodeError, EncodeError,
};

impl<'a> DataRef<'a, Domain> for DomainRef<'a> {
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    DomainRef::try_from(buf)
      .map(|domain| (buf.len(), domain))
      .map_err(|e| DecodeError::custom(e.as_str()))
  }
}

impl Data for Domain {
  type Ref<'a> = DomainRef<'a>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(val.to_owned())
  }

  fn encoded_len(&self) -> usize {
    self.fqdn_str().len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let val = self.fqdn_str();
    let len = val.len();
    if buf.len() < len {
      return Err(EncodeError::insufficient_buffer(len, buf.len()));
    }
    buf[..len].copy_from_slice(val.as_bytes());
    Ok(len)
  }
}

impl<'a, const N: usize> DataRef<'a, NodeId<N>> for NodeIdRef<'a, N> {
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    NodeIdRef::try_from(buf)
      .map(|node_id| (buf.len(), node_id))
      .map_err(|e| DecodeError::custom(e.to_string()))
  }
}

impl<const N: usize> Data for NodeId<N> {
  type Ref<'a> = NodeIdRef<'a, N>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(Self::new(val).expect("reference must be a valid node id"))
  }

  fn encoded_len(&self) -> usize {
    self.as_bytes().len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let val = self.as_bytes();
    let len = val.len();
    if buf.len() < len {
      return Err(EncodeError::insufficient_buffer(len, buf.len()));
    }
    buf[..len].copy_from_slice(val);
    Ok(len)
  }
}

const _: () = {
  const HOST_ADDR_SOCKET_TAG: u8 = 1;
  const HOST_ADDR_SOCKET_BYTE: u8 = merge(WireType::LengthDelimited, HOST_ADDR_SOCKET_TAG);
  const HOST_ADDR_DOMAIN_TAG: u8 = 2;
  const HOST_ADDR_DOMAIN_BYTE: u8 = merge(WireType::LengthDelimited, HOST_ADDR_DOMAIN_TAG);

  impl<'a> DataRef<'a, HostAddr> for HostAddrRef<'a> {
    fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError> {
      let mut offset = 0;
      let buf_len = buf.len();
      let mut value = None;

      while offset < buf_len {
        match buf[offset] {
          HOST_ADDR_SOCKET_BYTE => {
            if value.is_some() {
              return Err(DecodeError::duplicate_field(
                "HostAddr",
                "addr",
                HOST_ADDR_SOCKET_TAG,
              ));
            }
            offset += 1;
            let (bytes_read, addr) =
              <SocketAddr as DataRef<SocketAddr>>::decode_length_delimited(&buf[offset..])?;
            offset += bytes_read;
            value = Some(Self::from(addr));
          }
          HOST_ADDR_DOMAIN_BYTE => {
            if value.is_some() {
              return Err(DecodeError::duplicate_field(
                "HostAddr",
                "domain",
                HOST_ADDR_DOMAIN_TAG,
              ));
            }

            offset += 1;
            let (bytes_read, domain) =
              <DomainRef<'_> as DataRef<Domain>>::decode_length_delimited(&buf[offset..])?;
            let required = offset + bytes_read + 2;
            if required > buf_len {
              return Err(DecodeError::buffer_underflow());
            }
            let port = u16::from_be_bytes(buf[offset + bytes_read..required].try_into().unwrap());
            offset += bytes_read + 2;
            value = Some(Self::from((domain, port)));
          }
          _ => offset += skip("HostAddr", &buf[offset..])?,
        }
      }

      let value = value.ok_or_else(|| DecodeError::missing_field("HostAddr", "value"))?;
      Ok((offset, value))
    }
  }

  impl Data for HostAddr {
    type Ref<'a> = HostAddrRef<'a>;

    fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
    where
      Self: Sized,
    {
      Ok(val.to_owned())
    }

    fn encoded_len(&self) -> usize {
      match self.as_inner() {
        Either::Left(addr) => 1 + addr.encoded_len_with_length_delimited(),
        Either::Right((_, domain)) => 1 + 2 + domain.encoded_len_with_length_delimited(),
      }
    }

    fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
      let src_len = buf.len();

      match self.as_inner() {
        Either::Left(addr) => {
          if src_len < 1 {
            return Err(EncodeError::insufficient_buffer(1, src_len));
          }
          buf[0] = HOST_ADDR_SOCKET_BYTE;
          let offset = addr.encode_length_delimited(&mut buf[1..])?;
          Ok(1 + offset)
        }
        Either::Right((port, domain)) => {
          buf[0] = HOST_ADDR_DOMAIN_BYTE;
          let offset = domain.encode_length_delimited(&mut buf[1..])?;
          buf[1 + offset..1 + offset + 2].copy_from_slice(&port.to_be_bytes());
          Ok(1 + offset + 2)
        }
      }
    }
  }
};

const _: () = {
  const NODE_ID_TAG: u8 = 1;
  const NODE_ADDR_TAG: u8 = 2;

  #[inline]
  const fn node_id_byte<I: Data>() -> u8 {
    merge(I::WIRE_TYPE, NODE_ID_TAG)
  }

  #[inline]
  const fn node_addr_byte<A: Data>() -> u8 {
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
      let (id, address) = val.into_components();
      I::from_ref(id).and_then(|id| A::from_ref(address).map(|address| Self::new(id, address)))
    }

    fn encoded_len(&self) -> usize {
      1 + self.id().encoded_len_with_length_delimited()
        + 1
        + self.address().encoded_len_with_length_delimited()
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
        .id()
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
        .address()
        .encode_length_delimited(&mut buf[offset..])
        .map_err(|e| e.update(self.encoded_len(), src_len))?;

      #[cfg(debug_assertions)]
      super::super::debug_assert_write_eq::<Self>(offset, self.encoded_len());
      Ok(offset)
    }
  }
};
