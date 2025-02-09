use super::{Data, DataRef};

macro_rules! impl_str {
  ($($ty:ty => $from:ident),+$(,)?) => {
    $(
      impl<'a> DataRef<'a, $ty> for &'a str {
        fn decode(src: &'a [u8]) -> Result<(usize, Self), super::DecodeError> {
          match core::str::from_utf8(src) {
            Ok(value) => Ok((src.len(), value)),
            Err(e) => Err(super::DecodeError::new(e.to_string())),
          }
        }
      }

      impl Data for $ty {
        type Ref<'a> = &'a str;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, super::DecodeError> {
          Ok(Self::$from(val))
        }

        #[inline]
        fn encoded_len(&self) -> usize {
          let bytes = self.as_bytes();

          if bytes.is_empty() {
            return 0;
          }

          bytes.len()
        }

        #[inline]
        fn encode(&self, buf: &mut [u8]) -> Result<usize, super::EncodeError> {
          let bytes = self.as_bytes();
          if bytes.is_empty() {
            return Ok(0);
          }

          let len = bytes.len();
          let buf_len = buf.len();
          if len > buf_len {
            return Err(super::EncodeError::insufficient_buffer(len, buf_len));
          }

          super::super::check_encoded_message_size(len)?;

          buf[..len].copy_from_slice(bytes);
          Ok(len)
        }
      }
    )*
  };
}

impl_str!(
  std::string::String => from,
  smol_str::SmolStr => new,
  std::sync::Arc<str> => from,
  std::boxed::Box<str> => from,
  triomphe::Arc<str> => from,
);
