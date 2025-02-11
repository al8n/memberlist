use bytes::Bytes;
use core::marker::PhantomData;
use std::borrow::Cow;

use super::{merge, skip, split, Data, DataRef, DecodeError, EncodeError, WireType};

const LZW_TAG: u8 = 0;
const ZSTD_TAG: u8 = 1;
const BROTLI_TAG: u8 = 2;
const DEFLATE_TAG: u8 = 3;
const GZIP_TAG: u8 = 4;
const LZ4_TAG: u8 = 5;
const SNAPPY_TAG: u8 = 6;
const ZLIB_TAG: u8 = 7;

macro_rules! num_to_enum {
  (
    $(#[$meta:meta])*
    $name:ident($inner:ident in [$min:expr, $max:literal]):$exp:literal:$short:literal {
      $($value:literal), +$(,)?
    }
  ) => {
    paste::paste! {
      $(#[$meta])*
      #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display, derive_more::IsVariant)]
      #[non_exhaustive]
      pub enum $name {
        $(
          [< $short:camel $value >] = $value,
        )*
      }

      impl $name {
        #[allow(unused_comparisons)]
        const fn from_u8(value: u8) -> Self {
          match value {
            $(
              $value => Self::[< $short:camel $value >],
            )*
            val if val > $max => Self::[< $short:camel $max >],
            val if val < $min => Self::[< $short:camel $min >],
            _ => Self::[< $short:camel $max >],
          }
        }
      }

      impl From<$inner> for $name {
        fn from(value: $inner) -> Self {
          Self::from_u8(value as u8)
        }
      }

      #[cfg(feature = "serde")]
      const _: () = {
        use serde::{Deserialize, Serialize};

        impl Serialize for $name {
          fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
          where
            S: serde::Serializer,
          {
            serializer.serialize_u8(*self as u8)
          }
        }

        impl<'de> Deserialize<'de> for $name {
          fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
          where
            D: serde::Deserializer<'de> {
            u8::deserialize(deserializer).map(Self::from_u8)
          }
        }
      };
    }
  };
}

num_to_enum! {
  /// The brotli quality
  #[repr(u8)]
  BrotliQuality(u8 in [0, 11]):"quality":"Q" {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
  }
}

num_to_enum! {
  /// The brotli window
  #[repr(u8)]
  BrotliWindow(u8 in [10, 24]):"window":"W" {
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
  }
}

/// The brotli algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
#[display("({quality}, {window})")]
pub struct BrotliAlgorithm {
  quality: BrotliQuality,
  window: BrotliWindow,
}

impl Default for BrotliAlgorithm {
  fn default() -> Self {
    Self::new()
  }
}

impl BrotliAlgorithm {
  /// Creates a new `BrotliAlgorithm` with the default quality and window.
  #[inline]
  pub const fn new() -> Self {
    Self {
      quality: BrotliQuality::Q11,
      window: BrotliWindow::W22,
    }
  }

  /// Creates a new `BrotliAlgorithm` with the quality and window.
  #[inline]
  pub const fn with_quality_and_window(quality: BrotliQuality, window: BrotliWindow) -> Self {
    Self { quality, window }
  }

  /// Returns the quality of the brotli algorithm.
  #[inline]
  pub const fn quality(&self) -> BrotliQuality {
    self.quality
  }

  /// Returns the window of the brotli algorithm.
  #[inline]
  pub const fn window(&self) -> BrotliWindow {
    self.window
  }

  /// Encodes the algorithm settings into a single byte.
  /// Quality (0-11) is stored in the high 4 bits.
  /// Window (10-24) is stored in the low 4 bits, mapped to 0-15.
  #[inline]
  pub(super) const fn encode(&self) -> u8 {
    // Quality goes in high 4 bits
    let quality_bits = (self.quality as u8) << 4;
    // Window needs to be mapped from 10-24 to 0-15
    let window_bits = (self.window as u8).saturating_sub(10);
    quality_bits | window_bits
  }

  /// Decodes a single byte into algorithm settings.
  /// Quality is extracted from high 4 bits.
  /// Window is extracted from low 4 bits and mapped back to 10-24 range.
  #[inline]
  pub(super) const fn decode(byte: u8) -> Self {
    // Extract quality from high 4 bits
    let quality = BrotliQuality::from_u8(byte >> 4);
    // Extract window from low 4 bits and map back to 10-24 range
    let window = BrotliWindow::from_u8(10 + (byte & 0x0F));
    Self { quality, window }
  }
}

/// The zstd algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, derive_more::Display)]
#[display("{}", level)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct ZstdAlgorithm {
  level: i8,
}

impl Default for ZstdAlgorithm {
  fn default() -> Self {
    Self::new()
  }
}

impl ZstdAlgorithm {
  /// Creates a new `ZstdAlgorithm` with the default level.
  #[inline]
  pub const fn new() -> Self {
    Self { level: 3 }
  }

  /// Creates a new `ZstdAlgorithm` with the level.
  #[inline]
  pub const fn with_level(level: i8) -> Self {
    Self { level }
  }

  /// Returns the level of the zstd algorithm.
  #[inline]
  pub const fn level(&self) -> i8 {
    self.level
  }
}

impl From<u8> for ZstdAlgorithm {
  fn from(value: u8) -> Self {
    if value > 22 {
      Self::with_level(22)
    } else {
      Self::with_level(value as i8)
    }
  }
}

impl From<i8> for ZstdAlgorithm {
  fn from(value: i8) -> Self {
    if value > 22 {
      Self::with_level(22)
    } else if value < -99 {
      Self::with_level(-99)
    } else {
      Self::with_level(value)
    }
  }
}

/// The compressioned algorithm used to compression the message.
#[derive(
  Debug, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum CompressAlgorithm {
  /// Brotli
  #[display("brotli{_0}")]
  Brotli(BrotliAlgorithm),
  /// Deflate
  #[display("deflate")]
  Deflate,
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
  Zstd(ZstdAlgorithm),
  /// Unknwon compressioned algorithm
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl CompressAlgorithm {
  /// Returns the string representation of the algorithm.
  #[inline]
  pub fn as_str(&self) -> Cow<'_, str> {
    match self {
      Self::Brotli(algo) => return Cow::Owned(format!("brotli{algo}")),
      Self::Deflate => "deflate",
      Self::Gzip => "gzip",
      Self::Lzw => "lzw",
      Self::Lz4 => "lz4",
      Self::Snappy => "snappy",
      Self::Zlib => "zlib",
      Self::Zstd(val) => return Cow::Owned(format!("zstd({})", val.level())),
      Self::Unknown(val) => return Cow::Owned(format!("unknown({val})")),
    }
    .into()
  }

  /// Encodes the algorithm into a u16.
  /// First byte is the algorithm tag.
  /// Second byte stores additional configuration if any.
  #[inline]
  pub(super) const fn encode_to_u16(&self) -> u16 {
    let (tag, extra) = match self {
      Self::Brotli(algo) => (BROTLI_TAG, algo.encode()),
      Self::Deflate => (DEFLATE_TAG, 0),
      Self::Gzip => (GZIP_TAG, 0),
      Self::Lzw => (LZW_TAG, 0),
      Self::Lz4 => (LZ4_TAG, 0),
      Self::Snappy => (SNAPPY_TAG, 0),
      Self::Zlib => (ZLIB_TAG, 0),
      Self::Zstd(algo) => (ZSTD_TAG, algo.level() as u8),
      Self::Unknown(v) => (*v, 0),
    };
    ((tag as u16) << 8) | (extra as u16)
  }

  /// Creates a CompressAlgorithm from a u16.
  /// First byte determines the algorithm type.
  /// Second byte contains additional configuration if any.
  #[inline]
  pub(super) const fn decode_from_u16(value: u16) -> Self {
    let tag = (value >> 8) as u8;
    let extra = value as u8;

    match tag {
      BROTLI_TAG => Self::Brotli(BrotliAlgorithm::decode(extra)),
      DEFLATE_TAG => Self::Deflate,
      GZIP_TAG => Self::Gzip,
      LZW_TAG => Self::Lzw,
      LZ4_TAG => Self::Lz4,
      SNAPPY_TAG => Self::Snappy,
      ZLIB_TAG => Self::Zlib,
      ZSTD_TAG => Self::Zstd(ZstdAlgorithm { level: extra as i8 }),
      v => Self::Unknown(v),
    }
  }
}

impl From<u16> for CompressAlgorithm {
  fn from(value: u16) -> Self {
    Self::decode_from_u16(value)
  }
}

impl<'a> DataRef<'a, Self> for CompressAlgorithm {
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    <u16 as DataRef<u16>>::decode(buf).map(|(bytes_read, value)| (bytes_read, Self::from(value)))
  }
}

impl Data for CompressAlgorithm {
  const WIRE_TYPE: WireType = WireType::Varint;

  type Ref<'a> = Self;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(val)
  }

  fn encoded_len(&self) -> usize {
    self.encode_to_u16().encoded_len()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    self.encode_to_u16().encode(buf)
  }
}

/// A message with a compression.
#[viewit::viewit(
  vis_all = "",
  setters(prefix = "with", style = "move"),
  getters(style = "move")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
  #[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary_impl::bytes))]
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
    1 + self.algo.encoded_len() + 1 + self.payload.encoded_len_with_length_delimited()
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
    offset += self.algo.encode(&mut buf[offset..])?;
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
          let (bytes_read, val) =
            <CompressAlgorithm as DataRef<CompressAlgorithm>>::decode(&buf[offset..])?;
          offset += bytes_read;
          algo = Some(val);
        }
        b if b == CompressedMessage::<I, A>::payload_byte() => {
          if message.is_some() {
            return Err(DecodeError::duplicate_field(
              "CompressedMessage",
              "payload",
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

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl Arbitrary for CompressAlgorithm {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::from(u16::arbitrary(g))
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
