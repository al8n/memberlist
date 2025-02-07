use super::Data;

macro_rules! impl_str {
  ($($ty:ty => $from:ident),+$(,)?) => {
    $(
      impl Data for $ty {
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

        fn decode(src: &[u8]) -> Result<(usize, Self), super::DecodeError>
        where
          Self: Sized
        {
          match core::str::from_utf8(src) {
            Ok(value) => Ok((src.len(), Self::$from(value))),
            Err(e) => Err(super::DecodeError::new(e.to_string())),
          }
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
