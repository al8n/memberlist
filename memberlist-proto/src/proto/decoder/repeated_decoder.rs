use core::marker::PhantomData;

use crate::{
  Data, DataRef, DecodeError, WireType,
  utils::{merge, skip},
};

/// An iterator with can yields the reference type of `D`
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RepeatedDecoder<'a> {
  id: u8,
  buf: &'a [u8],
  start_offset: usize,
  end_offset: usize,
  nums: usize,
}

impl<'a> RepeatedDecoder<'a> {
  /// Creates a new [`RepeatedDecoder`] with the given tag, source buffer
  pub fn new(tag: u8, wire_type: WireType, src: &'a [u8]) -> Self {
    Self {
      id: merge(wire_type, tag),
      buf: src,
      start_offset: 0,
      end_offset: 0,
      nums: 0,
    }
  }

  /// Sets the offsets for this decoder.
  pub const fn with_offsets(mut self, start: usize, end: usize) -> Self {
    self.start_offset = start;
    self.end_offset = end;
    self
  }

  /// Sets the number of elements in the buffer.
  pub const fn with_nums(mut self, nums: usize) -> Self {
    self.nums = nums;
    self
  }

  /// Returns the number of elements in the collection.
  #[inline]
  pub const fn len(&self) -> usize {
    self.nums
  }

  /// Returns `true` if the collection is empty.
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.nums == 0
  }

  /// Returns an iterator over the `D`s in the collection.
  pub fn iter<D>(&self) -> RepeatedDecoderIter<'a, D> {
    RepeatedDecoderIter {
      id: self.id,
      src: self.buf,
      current_offset: (self.nums > 0).then_some(self.start_offset),
      end_offset: self.end_offset,
      nums: self.nums,
      has_err: false,
      yields: 0,
      _phantom: PhantomData,
    }
  }
}

/// An iterator over the `RepeatedDecoder` in the collection.
pub struct RepeatedDecoderIter<'a, D> {
  src: &'a [u8],
  id: u8,
  current_offset: Option<usize>,
  end_offset: usize,
  nums: usize,
  yields: usize,
  has_err: bool,
  _phantom: PhantomData<D>,
}

impl<'a, D> Iterator for RepeatedDecoderIter<'a, D>
where
  D: Data,
{
  type Item = Result<D::Ref<'a>, DecodeError>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.has_err || self.yields == self.nums {
      return None;
    }

    let current_offset = self.current_offset.as_mut()?;

    while *current_offset < self.end_offset {
      if self.src[*current_offset] == self.id {
        *current_offset += 1;
        let (readed, value) =
          match <D::Ref<'_> as DataRef<D>>::decode_length_delimited(&self.src[*current_offset..]) {
            Ok((readed, value)) => (readed, value),
            Err(e) => return Some(Err(e)),
          };
        *current_offset += readed;
        self.yields += 1;
        return Some(Ok(value));
      } else {
        *current_offset += match skip(core::any::type_name::<Self>(), &self.src[*current_offset..])
        {
          Ok(offset) => offset,
          Err(e) => return Some(Err(e)),
        };
      }
    }
    None
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    (0, Some(self.nums - self.yields))
  }
}

impl<D: Data> core::iter::FusedIterator for RepeatedDecoderIter<'_, D> {}
