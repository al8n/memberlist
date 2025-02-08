use super::{super::check_encoded_message_size, Data};

macro_rules! impl_bytes {
  ($($ty:ty => $from:ident),+$(,)?) => {
    $(
      impl Data for $ty {
        type Ref<'a> = &'a [u8];

        fn from_ref(val: Self::Ref<'_>) -> Self {
          Self::$from(val)
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

        fn decode_ref(src: &[u8]) -> Result<(usize, Self::Ref<'_>), super::DecodeError>
        where
          Self: Sized,
        {
          Ok((src.len(), src))
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

impl<const N: usize> Data for [u8; N] {
  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Self {
    val
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

  fn decode_ref(src: &[u8]) -> Result<(usize, Self::Ref<'_>), super::DecodeError>
  where
    Self: Sized,
  {
    if src.len() < N {
      return Err(super::DecodeError::new("buffer underflow"));
    }

    let mut bytes = [0; N];
    bytes.copy_from_slice(&src[..N]);
    Ok((N, bytes))
  }
}
