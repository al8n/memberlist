use either::Either;
use nodecraft::{Domain, DomainRef, HostAddr, HostAddrRef, Node, NodeId, NodeIdRef};

use super::{
  super::{merge, skip, split, WireType},
  Data, DecodeError, EncodeError,
};

impl Data for Domain {
  type Ref<'a> = DomainRef<'a>;

  fn from_ref(val: Self::Ref<'_>) -> Self
  where
    Self: Sized,
  {
    val.to_owned()
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

  fn decode_ref(buf: &[u8]) -> Result<(usize, Self::Ref<'_>), DecodeError> {
    DomainRef::try_from(buf)
      .map(|domain| (buf.len(), domain))
      .map_err(|e| DecodeError::new(e.to_string()))
  }
}

impl<const N: usize> Data for NodeId<N> {
  type Ref<'a> = NodeIdRef<'a, N>;

  fn from_ref(val: Self::Ref<'_>) -> Self {
    Self::new(val).expect("reference must be a valid node id")
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

  fn decode_ref(buf: &[u8]) -> Result<(usize, Self::Ref<'_>), DecodeError> {
    NodeIdRef::try_from(buf)
      .map(|node_id| (buf.len(), node_id))
      .map_err(|e| DecodeError::new(e.to_string()))
  }
}

const _: () = {
  const HOST_ADDR_SOCKET_TAG: u8 = 1;
  const HOST_ADDR_SOCKET_BYTE: u8 = merge(WireType::LengthDelimited, HOST_ADDR_SOCKET_TAG);
  const HOST_ADDR_DOMAIN_TAG: u8 = 2;
  const HOST_ADDR_DOMAIN_BYTE: u8 = merge(WireType::LengthDelimited, HOST_ADDR_DOMAIN_TAG);

  impl Data for HostAddr {
    type Ref<'a> = HostAddrRef<'a>;

    fn from_ref(val: Self::Ref<'_>) -> Self
    where
      Self: Sized,
    {
      match val.into_inner() {
        Either::Left(addr) => Self::from(addr),
        Either::Right((port, domain)) => Self::from((domain, port)),
      }
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

    fn decode_ref(buf: &[u8]) -> Result<(usize, Self::Ref<'_>), DecodeError> {
      if buf.is_empty() {
        return Err(DecodeError::new("buffer underflow"));
      }

      let b = buf[0];
      match b {
        HOST_ADDR_SOCKET_BYTE => {
          let (bytes_read, addr) = core::net::SocketAddr::decode_length_delimited_ref(&buf[1..])?;
          Ok((bytes_read + 1, Self::Ref::from(addr)))
        }
        HOST_ADDR_DOMAIN_BYTE => {
          let (bytes_read, domain) = Domain::decode_length_delimited_ref(&buf[1..])?;
          let required = 1 + bytes_read + 2;
          if required > buf.len() {
            return Err(DecodeError::new("buffer underflow"));
          }
          let port = u16::from_be_bytes(buf[1 + bytes_read..required].try_into().unwrap());
          Ok((required, Self::Ref::from((domain, port))))
        }
        b => {
          let (wire_type, tag) = split(b);
          WireType::try_from(wire_type)
            .map_err(|_| DecodeError::new(format!("invalid wire type value {}", wire_type)))?;

          Err(DecodeError::new(format!("unknown tag: {tag}",)))
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

  impl<I, A> Data for Node<I, A>
  where
    I: Data,
    A: Data,
  {
    type Ref<'a> = Node<I::Ref<'a>, A::Ref<'a>>;

    fn from_ref(val: Self::Ref<'_>) -> Self {
      let (id, address) = val.into_components();
      Self::new(I::from_ref(id), A::from_ref(address))
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
      super::super::debug_assert_write_eq(offset, self.encoded_len());
      Ok(offset)
    }

    fn decode_ref(src: &[u8]) -> Result<(usize, Self::Ref<'_>), DecodeError>
    where
      Self: Sized,
    {
      let mut offset = 0;
      let mut id = None;
      let mut address = None;

      while offset < src.len() {
        let b = src[offset];
        offset += 1;

        match b {
          b if b == node_id_byte::<I>() => {
            let (bytes_read, value) = I::decode_length_delimited_ref(&src[offset..])?;
            offset += bytes_read;
            id = Some(value);
          }
          b if b == node_addr_byte::<A>() => {
            let (bytes_read, value) = A::decode_length_delimited_ref(&src[offset..])?;
            offset += bytes_read;
            address = Some(value);
          }
          _ => {
            let (wire_type, _) = split(b);
            let wire_type = WireType::try_from(wire_type)
              .map_err(|_| DecodeError::new(format!("invalid wire type value {}", wire_type)))?;
            offset += skip(wire_type, &src[offset..])?;
          }
        }
      }

      let id = id.ok_or_else(|| DecodeError::new("missing node id"))?;
      let address = address.ok_or_else(|| DecodeError::new("missing node address"))?;
      Ok((offset, Self::Ref::new(id, address)))
    }
  }
};
