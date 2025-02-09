use super::{super::check_encoded_message_size, Data, DataRef};

macro_rules! impl_bytes {
  ($($ty:ty => $from:ident),+$(,)?) => {
    $(
      impl<'a> DataRef<'a, $ty> for &'a [u8] {
        fn decode(buf: &'a [u8]) -> Result<(usize, Self), super::DecodeError> {
          Ok((buf.len(), buf))
        }
      }

      impl Data for $ty {
        type Ref<'a> = &'a [u8];

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, super::DecodeError> {
          Ok(Self::$from(val))
        }

        fn encoded_len(&self) -> usize {
          if self.is_empty() {
            return 0;
          }

          self.len()
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, super::EncodeError> {
          if self.is_empty() {
            return Ok(0);
          }

          let len = self.len();
          let buf_len = buf.len();
          if len > buf_len {
            return Err(super::EncodeError::insufficient_buffer(len, buf_len));
          }

          check_encoded_message_size(len)?;

          buf[..len].copy_from_slice(self);
          Ok(len)
        }
      }
    )*
  };
}

impl_bytes!(
  bytes::Bytes => copy_from_slice,
  Vec<u8> => from,
  Box<[u8]> => from,
  std::sync::Arc<[u8]> => from,
);

impl<'a, const N: usize> DataRef<'a, [u8; N]> for [u8; N] {
  fn decode(src: &'a [u8]) -> Result<(usize, [u8; N]), super::DecodeError> {
    if src.len() < N {
      return Err(super::DecodeError::new("buffer underflow"));
    }

    let mut bytes = [0; N];
    bytes.copy_from_slice(&src[..N]);
    Ok((N, bytes))
  }
}

impl<const N: usize> Data for [u8; N] {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, super::DecodeError> {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    N
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, super::EncodeError> {
    check_encoded_message_size(N)?;

    if N > buf.len() {
      return Err(super::EncodeError::insufficient_buffer(N, buf.len()));
    }

    buf[..N].copy_from_slice(self);
    Ok(N)
  }
}
