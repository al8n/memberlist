use crate::message::{self, Data, DataRef, DecodeError, Message, MessageRef};
use core::marker::PhantomData;

/// A zero-copy decoder for [`Message`]s.
#[derive(Debug)]
pub struct MessagesDecoder<I, A, B> {
  buf: B,
  offset: usize,
  num_msgs: usize,
  _m: PhantomData<(I, A)>,
}

impl<I, A, B> MessagesDecoder<I, A, B>
where
  B: AsRef<[u8]>,
{
  /// Creates a new decoder.
  #[inline]
  pub fn new(buf: B) -> Result<Self, DecodeError> {
    let bytes = buf.as_ref();
    if bytes.is_empty() {
      return Ok(Self::zero(buf));
    }

    let mut offset = 0;
    let tag = bytes[offset];
    let num_msgs = match tag {
      message::COMPOOUND_MESSAGE_TAG => {
        offset += 1;
        if offset >= bytes.len() {
          return Err(DecodeError::buffer_underflow());
        }

        let num_msgs = bytes[offset] as usize;
        offset += 1;
        num_msgs
      }
      tag if message::is_plain_message_tag(tag) => 1,
      _ => return Err(DecodeError::unknown_tag("Message", tag)),
    };

    Ok(Self::new_in(buf, offset, num_msgs))
  }

  #[inline]
  const fn zero(buf: B) -> Self {
    Self {
      buf,
      offset: 0,
      num_msgs: 0,
      _m: PhantomData,
    }
  }

  #[inline]
  const fn new_in(buf: B, offset: usize, num_msgs: usize) -> Self {
    Self {
      buf,
      offset,
      num_msgs,
      _m: PhantomData,
    }
  }

  /// Returns the number of messages in the buffer.
  pub fn num_msgs(&self) -> usize {
    self.num_msgs
  }

  /// Returns the number of bytes read from the buffer.
  pub fn offset(&self) -> usize {
    self.offset
  }

  /// Returns the remaining bytes in the buffer.
  pub fn remaining(&self) -> &[u8] {
    if self.offset == 0 {
      self.buf.as_ref()
    } else {
      &self.buf.as_ref()[self.offset..]
    }
  }

  /// Returns the buffer.
  pub fn into_inner(self) -> B {
    self.buf
  }

  /// Returns an iterator to yield the messages.
  pub fn iter(&self) -> MessagesDecoderIter<'_, I, A, B> {
    MessagesDecoderIter {
      decoder: self,
      buf: self.remaining(),
      offset: 0,
      should_stop: false,
      msg_index: 0,
    }
  }
}

/// An iterator to yield the messages.
#[derive(Debug)]
pub struct MessagesDecoderIter<'a, I, A, B> {
  decoder: &'a MessagesDecoder<I, A, B>,
  buf: &'a [u8],
  offset: usize,
  msg_index: usize,
  should_stop: bool,
}

impl<'a, I, A, B> Iterator for MessagesDecoderIter<'a, I, A, B>
where
  I: Data,
  A: Data,
  B: AsRef<[u8]>,
{
  type Item = Result<MessageRef<'a, I::Ref<'a>, A::Ref<'a>>, DecodeError>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.should_stop {
      return None;
    }

    if self.msg_index >= self.decoder.num_msgs {
      return None;
    }

    if self.decoder.num_msgs == 1 {
      return Some(
        <<Message<I, A> as Data>::Ref<'a> as DataRef<Message<I, A>>>::decode(self.buf)
          .map(|(read, data)| {
            self.offset += read;
            self.msg_index += 1;
            data
          })
          .inspect_err(|_| {
            self.should_stop = true;
          }),
      );
    }

    Some(
      <<Message<I, A> as Data>::Ref<'a> as DataRef<Message<I, A>>>::decode_length_delimited(
        &self.buf[self.offset..],
      )
      .map(|(read, data)| {
        self.offset += read;
        self.msg_index += 1;
        data
      })
      .inspect_err(|_| {
        self.should_stop = true;
      }),
    )
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    let remaining_msgs = self.decoder.num_msgs - self.msg_index;
    (remaining_msgs, Some(remaining_msgs))
  }
}

impl<I, A, B> core::iter::ExactSizeIterator for MessagesDecoderIter<'_, I, A, B>
where
  I: Data,
  A: Data,
  B: AsRef<[u8]>,
{
  fn len(&self) -> usize {
    if self.should_stop {
      return 0;
    }

    self.decoder.num_msgs - self.msg_index
  }
}
