use core::{
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
  time::Duration,
};

use const_varint::{Varint, decode_duration, encode_duration_to, encoded_duration_len};

use crate::{Data, DataRef, DecodeError, EncodeError, WireType};

const IPV4_ADDR_LEN: usize = 4;
const IPV6_ADDR_LEN: usize = 16;

impl<'a> DataRef<'a, Self> for Ipv4Addr {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    if src.len() < IPV4_ADDR_LEN {
      return Err(DecodeError::buffer_underflow());
    }

    let octets: [u8; IPV4_ADDR_LEN] = src[..IPV4_ADDR_LEN].try_into().unwrap();
    Ok((IPV4_ADDR_LEN, Self::from(octets)))
  }
}

impl Data for Ipv4Addr {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  #[inline]
  fn encoded_len(&self) -> usize {
    IPV4_ADDR_LEN
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.len() < IPV4_ADDR_LEN {
      return Err(EncodeError::insufficient_buffer(IPV4_ADDR_LEN, buf.len()));
    }
    buf[..IPV4_ADDR_LEN].copy_from_slice(&self.octets());
    Ok(IPV4_ADDR_LEN)
  }
}

impl<'a> DataRef<'a, Self> for Ipv6Addr {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    if src.len() < IPV6_ADDR_LEN {
      return Err(DecodeError::buffer_underflow());
    }

    let octets: [u8; IPV6_ADDR_LEN] = src[..IPV6_ADDR_LEN].try_into().unwrap();
    Ok((IPV6_ADDR_LEN, Self::from(octets)))
  }
}

impl Data for Ipv6Addr {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  #[inline]
  fn encoded_len(&self) -> usize {
    IPV6_ADDR_LEN
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.len() < IPV6_ADDR_LEN {
      return Err(EncodeError::insufficient_buffer(IPV6_ADDR_LEN, buf.len()));
    }
    buf[..IPV6_ADDR_LEN].copy_from_slice(&self.octets());
    Ok(IPV6_ADDR_LEN)
  }
}

impl<'a> DataRef<'a, Self> for IpAddr {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    let buf_len = src.len();

    if buf_len < 1 {
      return Err(DecodeError::buffer_underflow());
    }

    match src[0] {
      0 => {
        let (len, addr) = <Ipv4Addr as DataRef<Ipv4Addr>>::decode(&src[1..])?;
        Ok((len + 1, IpAddr::V4(addr)))
      }
      1 => {
        let (len, addr) = <Ipv6Addr as DataRef<Ipv6Addr>>::decode(&src[1..])?;
        Ok((len + 1, IpAddr::V6(addr)))
      }
      val => Err(DecodeError::unknown_tag("IpAddr", val)),
    }
  }
}

impl Data for IpAddr {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  #[inline]
  fn encoded_len(&self) -> usize {
    1 + match self {
      IpAddr::V4(addr) => addr.encoded_len(),
      IpAddr::V6(addr) => addr.encoded_len(),
    }
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    match self {
      Self::V4(addr) => {
        const V4_REQUIRED: usize = 1 + IPV4_ADDR_LEN;

        if buf.len() < V4_REQUIRED {
          return Err(EncodeError::insufficient_buffer(V4_REQUIRED, buf.len()));
        }

        buf[0] = 0;
        buf[1..V4_REQUIRED].copy_from_slice(&addr.octets());
        Ok(V4_REQUIRED)
      }
      Self::V6(addr) => {
        const V6_REQUIRED: usize = 1 + IPV6_ADDR_LEN;

        if buf.len() < V6_REQUIRED {
          return Err(EncodeError::insufficient_buffer(V6_REQUIRED, buf.len()));
        }

        buf[0] = 1;
        buf[1..V6_REQUIRED].copy_from_slice(&addr.octets());
        Ok(V6_REQUIRED)
      }
    }
  }
}

impl<'a> DataRef<'a, Self> for SocketAddrV4 {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    const V4_REQUIRED: usize = IPV4_ADDR_LEN + 2;

    let buf_len = src.len();

    if buf_len < V4_REQUIRED {
      return Err(DecodeError::buffer_underflow());
    }

    let (ip_len, ip) = <Ipv4Addr as DataRef<Ipv4Addr>>::decode(src)?;
    let port = u16::from_be_bytes(src[ip_len..ip_len + 2].try_into().unwrap());
    Ok((V4_REQUIRED, SocketAddrV4::new(ip, port)))
  }
}

impl Data for SocketAddrV4 {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  #[inline]
  fn encoded_len(&self) -> usize {
    IPV4_ADDR_LEN + 2
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    const V4_REQUIRED: usize = IPV4_ADDR_LEN + 2;

    if buf.len() < V4_REQUIRED {
      return Err(EncodeError::insufficient_buffer(V4_REQUIRED, buf.len()));
    }

    buf[..IPV4_ADDR_LEN].copy_from_slice(&self.ip().octets());
    buf[IPV4_ADDR_LEN..V4_REQUIRED].copy_from_slice(&self.port().to_be_bytes());
    Ok(V4_REQUIRED)
  }
}

impl<'a> DataRef<'a, Self> for SocketAddrV6 {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    const V6_REQUIRED: usize = IPV6_ADDR_LEN + 2;
    let buf_len = src.len();

    if buf_len < V6_REQUIRED {
      return Err(DecodeError::buffer_underflow());
    }

    let (ip_len, ip) = <Ipv6Addr as DataRef<Ipv6Addr>>::decode(src)?;
    let port = u16::from_be_bytes(src[ip_len..ip_len + 2].try_into().unwrap());
    Ok((V6_REQUIRED, SocketAddrV6::new(ip, port, 0, 0)))
  }
}

impl Data for SocketAddrV6 {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  #[inline]
  fn encoded_len(&self) -> usize {
    IPV6_ADDR_LEN + 2
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    const V6_REQUIRED: usize = IPV6_ADDR_LEN + 2;

    if buf.len() < V6_REQUIRED {
      return Err(EncodeError::insufficient_buffer(V6_REQUIRED, buf.len()));
    }

    buf[..IPV6_ADDR_LEN].copy_from_slice(&self.ip().octets());
    buf[IPV6_ADDR_LEN..V6_REQUIRED].copy_from_slice(&self.port().to_be_bytes());
    Ok(V6_REQUIRED)
  }
}

impl<'a> DataRef<'a, Self> for SocketAddr {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let buf_len = src.len();

    if buf_len < 1 {
      return Err(DecodeError::buffer_underflow());
    }

    match src[0] {
      0 => {
        let (len, addr) = <SocketAddrV4 as DataRef<SocketAddrV4>>::decode(&src[1..])?;
        Ok((len + 1, SocketAddr::V4(addr)))
      }
      1 => {
        let (len, addr) = <SocketAddrV6 as DataRef<SocketAddrV6>>::decode(&src[1..])?;
        Ok((len + 1, SocketAddr::V6(addr)))
      }
      val => Err(DecodeError::unknown_tag("SocketAddr", val)),
    }
  }
}

impl Data for SocketAddr {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  #[inline]
  fn encoded_len(&self) -> usize {
    1 + match self {
      SocketAddr::V4(addr) => addr.encoded_len(),
      SocketAddr::V6(addr) => addr.encoded_len(),
    }
  }

  #[inline]
  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    match self {
      Self::V4(addr) => {
        const V4_REQUIRED: usize = 1 + IPV4_ADDR_LEN + 2;

        if buf.len() < V4_REQUIRED {
          return Err(EncodeError::insufficient_buffer(V4_REQUIRED, buf.len()));
        }

        buf[0] = 0;
        let len = addr.encode(&mut buf[1..])?;
        Ok(len + 1)
      }
      Self::V6(addr) => {
        const V6_REQUIRED: usize = 1 + IPV6_ADDR_LEN + 2;

        if buf.len() < V6_REQUIRED {
          return Err(EncodeError::insufficient_buffer(V6_REQUIRED, buf.len()));
        }

        buf[0] = 1;
        let len = addr.encode(&mut buf[1..])?;
        Ok(len + 1)
      }
    }
  }
}

macro_rules! impl_primitives {
  (@integer $($ty:ident), +$(,)?) => {
    $(
      impl<'a> DataRef<'a, Self> for $ty {
        fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
          Varint::decode(src).map_err(Into::into)
        }
      }

      impl Data for $ty {
        const WIRE_TYPE: WireType = WireType::Varint;

        type Ref<'a> = Self;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
          Ok(val)
        }

        fn encoded_len(&self) -> usize {
          Varint::encoded_len(self)
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
          Varint::encode(self, buf).map_err(Into::into)
        }
      }
    )*
  };
  (@float $($ty:literal), +$(,)?) => {
    paste::paste! {
      $(
        impl<'a> DataRef<'a, Self> for [< f $ty >] {
          fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
            const SIZE: usize = core::mem::size_of::<[< f $ty >]>();

            if src.len() < SIZE {
              return Err(DecodeError::buffer_underflow());
            }

            let mut bytes = [0; SIZE];
            bytes.copy_from_slice(&src[..SIZE]);
            Ok((SIZE, Self::from_le_bytes(bytes)))
          }
        }

        impl Data for [< f $ty >] {
          const WIRE_TYPE: WireType = WireType:: [< Fixed $ty >];

          type Ref<'a> = Self;

          fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
            Ok(val)
          }

          fn encoded_len(&self) -> usize {
            core::mem::size_of::<[< f $ty >]>()
          }

          fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
            const SIZE: usize = core::mem::size_of::<[< f $ty >]>();

            if buf.len() < SIZE {
              return Err(EncodeError::insufficient_buffer(
                SIZE, buf.len(),
              ));
            }

            buf[..SIZE].copy_from_slice(&self.to_le_bytes());
            Ok(SIZE)
          }
        }
      )*
    }
  };
  (@as_u8 $($ty:ident), +$(,)?) => {
    $(
      impl<'a> DataRef<'a, Self> for $ty {
        fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
          <u8 as DataRef<u8>>::decode(src).map(|(bytes_read, value)| (bytes_read, value as $ty))
        }
      }

      impl Data for $ty {
        const WIRE_TYPE: WireType = WireType::Byte;

        type Ref<'a> = Self;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
          Ok(val)
        }

        fn encoded_len(&self) -> usize {
          <u8 as Data>::encoded_len(&(*self as u8))
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
          <u8 as Data>::encode(&(*self as u8), buf)
        }
      }
    )*
  };
  (@wrapper $($wrapper:ty), +$(,)?) => {
    $(
      impl<'a, T> DataRef<'a, $wrapper> for T::Ref<'a>
      where
        T: Data,
      {
        fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
          let (bytes_read, value) = T::Ref::decode(src)?;
          Ok((bytes_read, value))
        }
      }

      impl<T> Data for $wrapper
      where
        T: Data,
      {
        const WIRE_TYPE: WireType = T::WIRE_TYPE;

        type Ref<'a> = <T as Data>::Ref<'a>;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
          T::from_ref(val).map(From::from)
        }

        fn encoded_len(&self) -> usize {
          T::encoded_len(&**self)
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
          T::encode(&**self, buf)
        }
      }
    )*
  };
}

impl_primitives!(@integer u16, u32, u64, u128, i16, i32, i64, i128);
impl_primitives!(@float 32, 64);
impl_primitives!(@as_u8 i8);

#[cfg(any(feature = "std", feature = "alloc"))]
impl_primitives!(@wrapper std::sync::Arc<T>, std::boxed::Box<T>, triomphe::Arc<T>);

impl<'a> DataRef<'a, Self> for char {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    let (bytes_read, value) = Varint::decode(src)?;
    Ok((
      bytes_read,
      char::from_u32(value).ok_or_else(|| DecodeError::custom("invalid character value"))?,
    ))
  }
}

impl Data for char {
  const WIRE_TYPE: WireType = WireType::Varint;

  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    Varint::encoded_len(&(*self as u32))
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    Varint::encode(&(*self as u32), buf).map_err(Into::into)
  }
}

impl<'a> DataRef<'a, Self> for u8 {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    if src.is_empty() {
      return Err(DecodeError::buffer_underflow());
    }

    Ok((1, src[0]))
  }
}

impl Data for u8 {
  const WIRE_TYPE: WireType = WireType::Byte;

  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    1
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    if buf.is_empty() {
      return Err(EncodeError::insufficient_buffer(1, 0));
    }

    buf[0] = *self;
    Ok(1)
  }
}

impl<'a> DataRef<'a, Self> for bool {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    if src.is_empty() {
      return Err(DecodeError::buffer_underflow());
    }
    Ok((1, src[0] != 0))
  }
}

impl Data for bool {
  const WIRE_TYPE: WireType = WireType::Byte;

  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    1
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    <u8 as Data>::encode(&(*self as u8), buf)
  }
}

impl<'a> DataRef<'a, Self> for Duration {
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError> {
    decode_duration(src).map_err(|_| DecodeError::custom("invalid duration"))
  }
}

impl Data for Duration {
  const WIRE_TYPE: WireType = WireType::Varint;

  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    encoded_duration_len(self)
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    encode_duration_to(self, buf).map_err(Into::into)
  }
}
