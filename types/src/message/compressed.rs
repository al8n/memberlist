use core::marker::PhantomData;
use std::borrow::Cow;

use bytes::Bytes;

use super::{merge, skip, split, Data, DataRef, DecodeError, EncodeError, WireType};

const ZSTD_RANGE: core::ops::RangeInclusive<i8> = -99..=22;
const BROTLI_TAG: i8 = 23;
const GZIP_TAG: i8 = 24;
const LZW_TAG: i8 = 25;
const LZ4_TAG: i8 = 26;
const SNAPPY_TAG: i8 = 27;
const ZLIB_TAG: i8 = 28;

/// The compressioned algorithm used to compression the message.
#[derive(
  Debug, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum CompressAlgorithm {
  /// Brotli
  #[display("brotli")]
  Brotli,
  /// Gzip
  #[display("gzip")]
  Gzip,
  /// LAW
  #[default]
  #[display("lzw")]
  Lzw,
  /// LZ4
  #[display("lz4")]
  Lz4,
  /// Snappy
  #[display("snappy")]
  Snappy,
  /// Zlib
  #[display("zlib")]
  Zlib,
  /// Zstd
  #[display("zstd({_0})")]
  Zstd(i8),
  /// Unknwon compressioned algorithm
  #[display("unknown({_0})")]
  Unknown(i8),
}

impl CompressAlgorithm {
  /// Returns the compressioned algorithm as a `u8`.
  #[inline]
  pub const fn as_i8(&self) -> i8 {
    match self {
      Self::Brotli => BROTLI_TAG,
      Self::Gzip => GZIP_TAG,
      Self::Lzw => LZW_TAG,
      Self::Lz4 => LZ4_TAG,
      Self::Snappy => SNAPPY_TAG,
      Self::Zlib => ZLIB_TAG,
      Self::Zstd(v) => *v,
      Self::Unknown(v) => *v,
    }
  }

  /// Returns the compressioned algorithm as a `&'static str`.
  #[inline]
  pub fn as_str(&self) -> Cow<'static, str> {
    let val = match self {
      Self::Brotli => "brotli",
      Self::Gzip => "gzip",
      Self::Lzw => "lzw",
      Self::Lz4 => "lz4",
      Self::Snappy => "snappy",
      Self::Zlib => "zlib",
      Self::Zstd(e) => return Cow::Owned(format!("zstd({})", e)),
      Self::Unknown(e) => return Cow::Owned(format!("unknown({})", e)),
    };
    Cow::Borrowed(val)
  }
}

impl From<i8> for CompressAlgorithm {
  fn from(value: i8) -> Self {
    match value {
      BROTLI_TAG => Self::Brotli,
      GZIP_TAG => Self::Gzip,
      LZW_TAG => Self::Lzw,
      LZ4_TAG => Self::Lz4,
      SNAPPY_TAG => Self::Snappy,
      ZLIB_TAG => Self::Zlib,
      val if ZSTD_RANGE.contains(&val) => Self::Zstd(value),
      _ => Self::Unknown(value),
    }
  }
}

impl From<CompressAlgorithm> for i8 {
  fn from(value: CompressAlgorithm) -> Self {
    value.as_i8()
  }
}

/// A message with a compression.
#[viewit::viewit(
  vis_all = "",
  setters(prefix = "with", style = "move"),
  getters(style = "move")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompressedMessage<I, A> {
  /// The algorithm used to compression the message.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the algorithm used to compression the message.", inline,)
    ),
    setter(attrs(doc = "Sets the algorithm used to compression the message.", inline,))
  )]
  algo: CompressAlgorithm,
  /// The message.
  #[viewit(
    getter(skip),
    setter(attrs(doc = "Sets the payload of the message.", inline,))
  )]
  payload: Bytes,
  #[viewit(getter(skip), setter(skip))]
  _m: PhantomData<(I, A)>,
}

const COMPRESS_ALGORITHM_TAG: u8 = 1;
const MESSAGE_TAG: u8 = 2;

impl<I, A> CompressedMessage<I, A> {
  /// Creates a new `CompressedMessage`.
  #[inline]
  pub const fn new(algo: CompressAlgorithm, payload: Bytes) -> Self {
    Self {
      algo,
      payload,
      _m: PhantomData,
    }
  }

  /// Returns the payload in bytes.
  #[inline]
  pub fn payload(&self) -> &[u8] {
    &self.payload
  }

  /// Returns the payload in `Bytes`.
  #[inline]
  pub const fn payload_bytes(&self) -> &Bytes {
    &self.payload
  }

  /// Consumes the `CompressedMessage` and returns the message.
  #[inline]
  pub fn into_payload(self) -> Bytes {
    self.payload
  }

  #[inline]
  const fn payload_byte() -> u8 {
    merge(WireType::LengthDelimited, MESSAGE_TAG)
  }

  #[inline]
  const fn algorithm_byte() -> u8 {
    merge(WireType::Byte, COMPRESS_ALGORITHM_TAG)
  }
}

impl<I, A> From<CompressedMessage<I, A>> for Bytes {
  #[inline]
  fn from(compressioned_message: CompressedMessage<I, A>) -> Self {
    compressioned_message.payload
  }
}

/// A reference type for `CompressedMessage`.
#[viewit::viewit(vis_all = "", setters(skip), getters(style = "move"))]
#[derive(Debug, Clone, Copy)]
pub struct CompressedMessageRef<'a, I, A> {
  /// The algorithm used to compression the message.
  #[viewit(getter(
    const,
    attrs(doc = "Returns the algorithm used to compression the message.", inline,)
  ))]
  algo: CompressAlgorithm,
  /// The message.
  #[viewit(getter(const, attrs(doc = "Returns the payload of the message.", inline,)))]
  payload: &'a [u8],
  #[viewit(getter(skip))]
  _m: PhantomData<(I, A)>,
}

impl<'a, I, A> CompressedMessageRef<'a, I, A> {
  /// Creates a new `CompressedMessageRef`.
  #[inline]
  pub(crate) const fn new(algo: CompressAlgorithm, payload: &'a [u8]) -> Self {
    Self {
      algo,
      payload,
      _m: PhantomData,
    }
  }
}

impl<I, A> Data for CompressedMessage<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = CompressedMessageRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(CompressedMessage::new(
      val.algo,
      Bytes::copy_from_slice(val.payload),
    ))
  }

  fn encoded_len(&self) -> usize {
    1 + 1 + 1 + self.payload.encoded_len_with_length_delimited()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let buf_len = buf.len();
    let mut offset = 0;
    if buf_len == 0 {
      return Err(EncodeError::insufficient_buffer(
        self.encoded_len(),
        buf_len,
      ));
    }

    buf[offset] = Self::algorithm_byte();
    offset += 1;
    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), offset));
    }
    buf[offset] = self.algo.as_i8() as u8;
    offset += 1;

    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), offset));
    }

    buf[offset] = Self::payload_byte();
    offset += 1;
    offset += self
      .payload
      .encode_length_delimited(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), buf_len))?;

    Ok(offset)
  }
}

impl<'a, I, A> DataRef<'a, CompressedMessage<I, A>>
  for CompressedMessageRef<'a, I::Ref<'a>, A::Ref<'a>>
where
  I: Data,
  A: Data,
{
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let buf_len = buf.len();

    if buf_len == 0 {
      return Err(DecodeError::buffer_underflow());
    }

    let mut message = None;
    let mut algo = None;

    while offset < buf_len {
      let b = buf[offset];
      offset += 1;

      match b {
        b if b == CompressedMessage::<I, A>::algorithm_byte() => {
          if algo.is_some() {
            return Err(DecodeError::duplicate_field(
              "CompressedMessage",
              "algorithm",
              COMPRESS_ALGORITHM_TAG,
            ));
          }
          let (bytes_read, a) = <u8 as DataRef<u8>>::decode(&buf[offset..])?;
          offset += bytes_read;
          algo = Some(CompressAlgorithm::from(a as i8));
        }
        b if b == CompressedMessage::<I, A>::payload_byte() => {
          if message.is_some() {
            return Err(DecodeError::duplicate_field(
              "CompressedMessage",
              "message",
              MESSAGE_TAG,
            ));
          }

          let (bytes_read, payload) =
            <&[u8] as DataRef<Bytes>>::decode_length_delimited(&buf[offset..])?;
          offset += bytes_read;
          message = Some(payload);
        }
        _ => {
          let (wire_type, _) = split(b);
          let wire_type = WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type)?;
          offset += skip(wire_type, &buf[offset..])?;
        }
      }
    }

    let message = message.ok_or(DecodeError::missing_field("CompressedMessage", "payload"))?;
    let algo = algo.ok_or(DecodeError::missing_field("CompressedMessage", "algorithm"))?;
    Ok((offset, CompressedMessageRef::new(algo, message)))
  }
}

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a> Arbitrary<'a> for CompressAlgorithm {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(Self::from(u.arbitrary::<i8>()?))
    }
  }

  impl<'a, I, A> Arbitrary<'a> for CompressedMessage<I, A>
  where
    I: Arbitrary<'a>,
    A: Arbitrary<'a>,
  {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
      Ok(Self::new(u.arbitrary()?, u.arbitrary::<Vec<u8>>()?.into()))
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl Arbitrary for CompressAlgorithm {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::from(i8::arbitrary(g))
    }
  }

  impl<I, A> Arbitrary for CompressedMessage<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::new(
        Arbitrary::arbitrary(g),
        <Vec<u8> as Arbitrary>::arbitrary(g).into(),
      )
    }
  }
};
