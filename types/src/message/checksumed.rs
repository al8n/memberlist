use core::marker::PhantomData;
use std::borrow::Cow;

use bytes::Bytes;

use super::{merge, skip, split, Data, DataRef, DecodeError, EncodeError, WireType};

const CRC32_TAG: u8 = 0;
const XXHASH32_TAG: u8 = 1;
const XXHASH64_TAG: u8 = 2;
const XXHASH3_TAG: u8 = 3;
const MURMUR3_TAG: u8 = 4;

/// The algorithm used to checksum the message.
#[derive(
  Debug, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display, derive_more::IsVariant,
)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
  /// CRC32 IEEE
  #[default]
  #[display("crc32")]
  Crc32,
  /// XXHash32
  #[display("xxhash32")]
  XxHash32,
  /// XXHash64
  #[display("xxhash64")]
  XxHash64,
  /// XXHash3
  #[display("xxhash3")]
  XxHash3,
  /// Murmur3
  #[display("murmur3")]
  Murmur3,
  /// Unknwon checksum algorithm
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl ChecksumAlgorithm {
  /// Returns the checksum algorithm as a `u8`.
  #[inline]
  pub const fn as_u8(&self) -> u8 {
    match self {
      Self::Crc32 => CRC32_TAG,
      Self::XxHash32 => XXHASH32_TAG,
      Self::XxHash64 => XXHASH64_TAG,
      Self::XxHash3 => XXHASH3_TAG,
      Self::Murmur3 => MURMUR3_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Returns the checksum algorithm as a `&'static str`.
  #[inline]
  pub fn as_str(&self) -> Cow<'static, str> {
    let val = match self {
      Self::Crc32 => "crc32",
      Self::XxHash32 => "xxhash32",
      Self::XxHash64 => "xxhash64",
      Self::XxHash3 => "xxhash3",
      Self::Murmur3 => "murmur3",
      Self::Unknown(e) => return Cow::Owned(format!("unknown({})", e)),
    };
    Cow::Borrowed(val)
  }
}

impl From<u8> for ChecksumAlgorithm {
  fn from(value: u8) -> Self {
    match value {
      CRC32_TAG => Self::Crc32,
      XXHASH32_TAG => Self::XxHash32,
      XXHASH64_TAG => Self::XxHash64,
      XXHASH3_TAG => Self::XxHash3,
      MURMUR3_TAG => Self::Murmur3,
      _ => Self::Unknown(value),
    }
  }
}

impl From<ChecksumAlgorithm> for u8 {
  fn from(value: ChecksumAlgorithm) -> Self {
    value.as_u8()
  }
}

/// A message with a checksum.
#[viewit::viewit(
  vis_all = "",
  setters(prefix = "with", style = "move"),
  getters(style = "move")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChecksumedMessage<I, A> {
  /// The algorithm used to checksum the message.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the algorithm used to checksum the message.", inline,)
    ),
    setter(attrs(doc = "Sets the algorithm used to checksum the message.", inline,))
  )]
  algo: ChecksumAlgorithm,
  /// The checksum.
  #[viewit(
    getter(const, attrs(doc = "Returns the checksum.", inline,)),
    setter(attrs(doc = "Sets the checksum.", inline,))
  )]
  checksum: u64,
  /// The message.
  #[viewit(
    getter(skip),
    setter(attrs(doc = "Sets the payload of the message.", inline,))
  )]
  payload: Bytes,
  #[viewit(getter(skip), setter(skip))]
  _m: PhantomData<(I, A)>,
}

const CHECKSUM_ALGORITHM_TAG: u8 = 1;
const CHECKSUM_TAG: u8 = 2;
const MESSAGE_TAG: u8 = 3;

impl<I, A> ChecksumedMessage<I, A> {
  /// Creates a new `ChecksumedMessage`.
  #[inline]
  pub const fn new(algo: ChecksumAlgorithm, checksum: u64, payload: Bytes) -> Self {
    Self {
      algo,
      checksum,
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

  /// Consumes the `ChecksumedMessage` and returns the message.
  #[inline]
  pub fn into_payload(self) -> Bytes {
    self.payload
  }

  #[inline]
  const fn checksum_byte() -> u8 {
    merge(WireType::Varint, CHECKSUM_TAG)
  }

  #[inline]
  const fn payload_byte() -> u8 {
    merge(WireType::LengthDelimited, MESSAGE_TAG)
  }

  #[inline]
  const fn algorithm_byte() -> u8 {
    merge(WireType::Byte, CHECKSUM_ALGORITHM_TAG)
  }
}

impl<I, A> From<ChecksumedMessage<I, A>> for Bytes {
  #[inline]
  fn from(checksumed_message: ChecksumedMessage<I, A>) -> Self {
    checksumed_message.payload
  }
}

/// A reference type for `ChecksumedMessage`.
#[viewit::viewit(vis_all = "", setters(skip), getters(style = "move"))]
#[derive(Debug, Clone, Copy)]
pub struct ChecksumedMessageRef<'a, I, A> {
  /// The algorithm used to checksum the message.
  #[viewit(getter(
    const,
    attrs(doc = "Returns the algorithm used to checksum the message.", inline,)
  ))]
  algo: ChecksumAlgorithm,
  /// The checksum.
  #[viewit(getter(const, attrs(doc = "Returns the checksum.", inline,)))]
  checksum: u64,
  /// The message.
  #[viewit(getter(const, attrs(doc = "Returns the payload of the message.", inline,)))]
  payload: &'a [u8],
  #[viewit(getter(skip))]
  _m: PhantomData<(I, A)>,
}

impl<'a, I, A> ChecksumedMessageRef<'a, I, A> {
  /// Creates a new `ChecksumedMessageRef`.
  #[inline]
  pub(crate) const fn new(algo: ChecksumAlgorithm, checksum: u64, payload: &'a [u8]) -> Self {
    Self {
      algo,
      checksum,
      payload,
      _m: PhantomData,
    }
  }
}

impl<I, A> Data for ChecksumedMessage<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = ChecksumedMessageRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(ChecksumedMessage::new(
      val.algo,
      val.checksum,
      Bytes::copy_from_slice(val.payload),
    ))
  }

  fn encoded_len(&self) -> usize {
    1 + 1 + 1 + self.checksum.encoded_len() + 1 + self.payload.encoded_len_with_length_delimited()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let buf_len = buf.len();
    let mut offset = 0;
    if buf_len == 0 {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), 0));
    }

    buf[offset] = Self::algorithm_byte();
    offset += 1;
    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), offset));
    }
    buf[offset] = self.algo.as_u8();
    offset += 1;

    if offset >= buf_len {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), offset));
    }
    buf[offset] = Self::checksum_byte();
    offset += 1;
    offset += self
      .checksum
      .encode(&mut buf[offset..])
      .map_err(|e| e.update(self.encoded_len(), buf_len))?;

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

impl<'a, I, A> DataRef<'a, ChecksumedMessage<I, A>>
  for ChecksumedMessageRef<'a, I::Ref<'a>, A::Ref<'a>>
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

    let mut checksum = None;
    let mut message = None;
    let mut algo = None;

    while offset < buf_len {
      let b = buf[offset];
      offset += 1;

      match b {
        b if b == ChecksumedMessage::<I, A>::algorithm_byte() => {
          if algo.is_some() {
            return Err(DecodeError::duplicate_field(
              "ChecksumedMessage",
              "algorithm",
              CHECKSUM_ALGORITHM_TAG,
            ));
          }
          let (bytes_read, a) = <u8 as DataRef<u8>>::decode(&buf[offset..])?;
          offset += bytes_read;
          algo = Some(ChecksumAlgorithm::from(a));
        }
        b if b == ChecksumedMessage::<I, A>::checksum_byte() => {
          if checksum.is_some() {
            return Err(DecodeError::duplicate_field(
              "ChecksumedMessage",
              "checksum",
              CHECKSUM_TAG,
            ));
          }
          let (bytes_read, cks) = <u64 as DataRef<u64>>::decode(&buf[offset..])?;
          offset += bytes_read;
          checksum = Some(cks);
        }
        b if b == ChecksumedMessage::<I, A>::payload_byte() => {
          if message.is_some() {
            return Err(DecodeError::duplicate_field(
              "ChecksumedMessage",
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

    let checksum = checksum.ok_or(DecodeError::missing_field("ChecksumedMessage", "checksum"))?;
    let message = message.ok_or(DecodeError::missing_field("ChecksumedMessage", "payload"))?;
    let algo = algo.ok_or(DecodeError::missing_field("ChecksumedMessage", "algorithm"))?;
    Ok((offset, ChecksumedMessageRef::new(algo, checksum, message)))
  }
}

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a> Arbitrary<'a> for ChecksumAlgorithm {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(Self::from(u.arbitrary::<u8>()?))
    }
  }

  impl<'a, I, A> Arbitrary<'a> for ChecksumedMessage<I, A>
  where
    I: Arbitrary<'a>,
    A: Arbitrary<'a>,
  {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
      Ok(Self::new(
        u.arbitrary()?,
        u.arbitrary()?,
        u.arbitrary::<Vec<u8>>()?.into(),
      ))
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl Arbitrary for ChecksumAlgorithm {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::from(u8::arbitrary(g))
    }
  }

  impl<I, A> Arbitrary for ChecksumedMessage<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::new(
        Arbitrary::arbitrary(g),
        Arbitrary::arbitrary(g),
        <Vec<u8> as Arbitrary>::arbitrary(g).into(),
      )
    }
  }
};
