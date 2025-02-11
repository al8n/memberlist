use core::marker::PhantomData;
use std::borrow::Cow;

use bytes::Bytes;

use super::{merge, skip, split, Data, DataRef, DecodeError, EncodeError, WireType};

const NOPADDING_TAG: u8 = 0;
const PKCS7_TAG: u8 = 1;

/// The encryption algorithm used to encrypt the message.
#[derive(
  Debug, Default, Clone, Copy, PartialEq, Eq, Hash, derive_more::IsVariant, derive_more::Display,
)]
#[non_exhaustive]
pub enum EncryptionAlgorithm {
  /// AES-GCM, using no padding
  #[default]
  #[display("aes-gcm-nopadding")]
  NoPadding,
  /// AES-GCM, using PKCS7 padding
  #[display("aes-gcm-pkcs7")]
  Pkcs7,
  /// Unknwon encryption version
  #[display("unknown({_0})")]
  Unknown(u8),
}

impl EncryptionAlgorithm {
  /// Returns the encryption version as a `u8`.
  #[inline]
  pub const fn as_u8(&self) -> u8 {
    match self {
      Self::NoPadding => NOPADDING_TAG,
      Self::Pkcs7 => PKCS7_TAG,
      Self::Unknown(v) => *v,
    }
  }

  /// Returns the encryption version as a `&'static str`.
  #[inline]
  pub fn as_str(&self) -> Cow<'static, str> {
    let val = match self {
      Self::NoPadding => "aes-gcm-nopadding",
      Self::Pkcs7 => "aes-gcm-pkcs7",
      Self::Unknown(e) => return Cow::Owned(format!("unknown({})", e)),
    };
    Cow::Borrowed(val)
  }
}

impl From<u8> for EncryptionAlgorithm {
  fn from(value: u8) -> Self {
    match value {
      NOPADDING_TAG => Self::NoPadding,
      PKCS7_TAG => Self::Pkcs7,
      e => Self::Unknown(e),
    }
  }
}

/// A message with a encryption.
#[viewit::viewit(
  vis_all = "",
  setters(prefix = "with", style = "move"),
  getters(style = "move")
)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct EncryptedMessage<I, A> {
  /// The algorithm used to encryption the message.
  #[viewit(
    getter(
      const,
      attrs(doc = "Returns the algorithm used to encryption the message.", inline,)
    ),
    setter(attrs(doc = "Sets the algorithm used to encryption the message.", inline,))
  )]
  algo: EncryptionAlgorithm,
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

const ENCRYPT_ALGORITHM_TAG: u8 = 1;
const MESSAGE_TAG: u8 = 2;

impl<I, A> EncryptedMessage<I, A> {
  /// Creates a new `EncryptedMessage`.
  #[inline]
  pub const fn new(algo: EncryptionAlgorithm, payload: Bytes) -> Self {
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

  /// Consumes the `EncryptedMessage` and returns the message.
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
    merge(WireType::Byte, ENCRYPT_ALGORITHM_TAG)
  }
}

impl<I, A> From<EncryptedMessage<I, A>> for Bytes {
  #[inline]
  fn from(encryptioned_message: EncryptedMessage<I, A>) -> Self {
    encryptioned_message.payload
  }
}

/// A reference type for `EncryptedMessage`.
#[viewit::viewit(vis_all = "", setters(skip), getters(style = "move"))]
#[derive(Debug, Clone, Copy)]
pub struct EncryptedMessageRef<'a, I, A> {
  /// The algorithm used to encryption the message.
  #[viewit(getter(
    const,
    attrs(doc = "Returns the algorithm used to encryption the message.", inline,)
  ))]
  algo: EncryptionAlgorithm,
  /// The message.
  #[viewit(getter(const, attrs(doc = "Returns the payload of the message.", inline,)))]
  payload: &'a [u8],
  #[viewit(getter(skip))]
  _m: PhantomData<(I, A)>,
}

impl<'a, I, A> EncryptedMessageRef<'a, I, A> {
  /// Creates a new `EncryptedMessageRef`.
  #[inline]
  pub(crate) const fn new(algo: EncryptionAlgorithm, payload: &'a [u8]) -> Self {
    Self {
      algo,
      payload,
      _m: PhantomData,
    }
  }
}

impl<I, A> Data for EncryptedMessage<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = EncryptedMessageRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(EncryptedMessage::new(
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
    buf[offset] = self.algo.as_u8();
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

impl<'a, I, A> DataRef<'a, EncryptedMessage<I, A>>
  for EncryptedMessageRef<'a, I::Ref<'a>, A::Ref<'a>>
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
        b if b == EncryptedMessage::<I, A>::algorithm_byte() => {
          if algo.is_some() {
            return Err(DecodeError::duplicate_field(
              "EncryptedMessage",
              "algorithm",
              ENCRYPT_ALGORITHM_TAG,
            ));
          }
          let (bytes_read, a) = <u8 as DataRef<u8>>::decode(&buf[offset..])?;
          offset += bytes_read;
          algo = Some(EncryptionAlgorithm::from(a));
        }
        b if b == EncryptedMessage::<I, A>::payload_byte() => {
          if message.is_some() {
            return Err(DecodeError::duplicate_field(
              "EncryptedMessage",
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

    let message = message.ok_or(DecodeError::missing_field("EncryptedMessage", "payload"))?;
    let algo = algo.ok_or(DecodeError::missing_field("EncryptedMessage", "algorithm"))?;
    Ok((offset, EncryptedMessageRef::new(algo, message)))
  }
}
