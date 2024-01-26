use std::marker::PhantomData;

use transformable::Transformable;

use super::{MessageTransformError, Wire};

/// A length-prefixed encoding [`Wire`] implementation
pub struct Lpe<I, A>(PhantomData<(I, A)>);

impl<I, A> Wire for Lpe<I, A>
where
  I: super::Transformable + core::fmt::Debug,
  A: super::Transformable + core::fmt::Debug,
{
  type Error = MessageTransformError<I, A>;
  type Id = I;
  type Address = A;

  fn encoded_len(msg: &super::Message<I, A>) -> usize {
    msg.encoded_len()
  }

  fn encode_message(msg: super::Message<I, A>, dst: &mut [u8]) -> Result<usize, Self::Error> {
    msg.encode(dst)
  }

  fn decode_message(src: &[u8]) -> Result<(usize, super::Message<I, A>), Self::Error> {
    super::Message::decode(src)
  }

  async fn decode_message_from_reader(
    mut conn: impl futures::prelude::AsyncRead + Send + Unpin,
  ) -> std::io::Result<(usize, super::Message<I, A>)> {
    super::Message::decode_from_async_reader(&mut conn).await
  }
}
