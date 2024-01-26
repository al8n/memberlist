use std::marker::PhantomData;

use transformable::Transformable;

use super::{Message, MessageTransformError, Wire};

/// A length-prefixed encoding [`Wire`] implementation
pub struct Lpe<I, A>(PhantomData<(I, A)>);

impl<I, A> Default for Lpe<I, A> {
  #[inline(always)]
  fn default() -> Self {
    Self(PhantomData)
  }
}

impl<I, A> Lpe<I, A> {
  /// Create a new `Lpe` instance
  #[inline(always)]
  pub const fn new() -> Self {
    Self(PhantomData)
  }
}

impl<I, A> Clone for Lpe<I, A> {
  #[inline(always)]
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A> Copy for Lpe<I, A> {}

impl<I, A> core::fmt::Debug for Lpe<I, A> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("Lpe").finish()
  }
}

impl<I, A> core::fmt::Display for Lpe<I, A> {
  #[inline]
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.write_str("Lpe")
  }
}

impl<I, A> Wire for Lpe<I, A>
where
  I: Transformable + core::fmt::Debug,
  A: Transformable + core::fmt::Debug,
{
  type Error = MessageTransformError<I, A>;
  type Id = I;
  type Address = A;

  fn encoded_len(msg: &Message<I, A>) -> usize {
    msg.encoded_len()
  }

  fn encode_message(msg: Message<I, A>, dst: &mut [u8]) -> Result<usize, Self::Error> {
    msg.encode(dst)
  }

  fn decode_message(src: &[u8]) -> Result<(usize, Message<I, A>), Self::Error> {
    Message::decode(src)
  }

  async fn decode_message_from_reader(
    mut conn: impl futures::prelude::AsyncRead + Send + Unpin,
  ) -> std::io::Result<(usize, Message<I, A>)> {
    Message::decode_from_async_reader(&mut conn).await
  }
}
