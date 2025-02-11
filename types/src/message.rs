use core::marker::PhantomData;

use bytes::Bytes;
use triomphe::Arc;

mod checksumed;
mod compressed;
mod encrypted;
mod label;

#[cfg(feature = "serde")]
mod serde_impl;

/// This mod impls `core::str::FromStr`, which is used for using with `clap` crate.
mod from_str;

pub use checksumed::*;
pub use compressed::*;
pub use encrypted::*;
pub use from_str::*;
pub use label::*;

use super::*;

macro_rules! enum_wrapper {
  (
    $(#[$outer:meta])*
    $vis:vis enum $name:ident $(<$($generic:tt),+>)? {
      $(
        $(#[$variant_meta:meta])*
        $variant:ident(
          $(#[$variant_ty_meta:meta])*
          $variant_ty: ident $(<$($variant_generic:tt),+>)? $(,)?
        ) = $variant_tag:expr
      ), +$(,)?
    }
  ) => {
    paste::paste! {
      #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::IsVariant)]
      #[repr(u8)]
      #[non_exhaustive]
      #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
      $vis enum [< $name Type >] {
        $(
          $(#[$variant_meta])*
          $variant = $variant_tag,
        )*
      }

      impl core::fmt::Display for [< $name Type >] {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
          write!(f, "{}", self.kind())
        }
      }

      impl TryFrom<u8> for [< $name Type >] {
        type Error = u8;

        fn try_from(value: u8) -> Result<Self, Self::Error> {
          match value {
            $(
              $variant_tag => Ok(Self::$variant),
            )*
            _ => Err(value),
          }
        }
      }

      impl [< $name Type >] {
        /// Returns the tag of this message type for encoding/decoding.
        #[inline]
        pub const fn tag(&self) -> u8 {
          match self {
            $(
              Self::$variant => $variant_tag,
            )*
          }
        }

        /// Returns the kind of this message.
        #[inline]
        pub const fn kind(&self) -> &'static str {
          match self {
            $(
              Self::$variant => stringify!([< $variant:camel >]),
            )*
          }
        }
      }

      #[cfg(feature = "quickcheck")]
      const _: () = {
        use quickcheck::{Arbitrary, Gen};

        impl [< $name Type >] {
          const POSSIBLE_VALUES: &'static [u8] = &[
            $(
              $variant_tag,
            )*
          ];
        }

        impl Arbitrary for [< $name Type >] {
          fn arbitrary(g: &mut Gen) -> Self {
            let val = g.choose(&Self::POSSIBLE_VALUES).unwrap();
            Self::try_from(*val).unwrap()
          }
        }
      };
    }

    $(#[$outer])*
    $vis enum $name $(< $($generic),+ >)? {
      $(
        $(#[$variant_meta])*
        $variant(
          $(#[$variant_ty_meta])*
          $variant_ty $(< $($variant_generic),+ >)?
        ),
      )*
    }

    impl $(< $($generic),+ >)? $name $(< $($generic),+ >)? {
      paste::paste! {
        $(
          #[doc = concat!("The tag of [`", stringify!($variant_ty), "`] message.")]
          pub const [< $variant: snake:upper _TAG >]: u8 = $variant_tag;
          const [< $variant:snake:upper _BYTE >]: u8 = super::merge(WireType::LengthDelimited, $variant_tag);
        )*

        /// Returns the tag of this message type for encoding/decoding.
        #[inline]
        pub const fn tag(&self) -> u8 {
          match self {
            $(
              Self::$variant(_) => Self::[< $variant: snake:upper _TAG >],
            )*
          }
        }

        const fn byte(&self) -> u8 {
          match self {
            $(
              Self::$variant(_) => Self::[< $variant: snake:upper _BYTE >],
            )*
          }
        }
      }

      /// Returns the kind of this message.
      #[inline]
      pub const fn kind(&self) -> &'static str {
        match self {
          $(
            Self::$variant(_) => stringify!($variant),
          )*
        }
      }

      $(
        paste::paste! {
          #[doc = concat!("Construct a [`", stringify!($name), "`] from [`", stringify!($variant_ty), "`].")]
          pub const fn [< $variant:snake >](val: $variant_ty $(< $($variant_generic),+ >)?) -> Self {
            Self::$variant(val)
          }
        }
      )*
    }
  };
}

enum_wrapper!(
  /// Request to be sent to the Raft node.
  #[derive(
    Debug,
    Clone,
    derive_more::From,
    derive_more::IsVariant,
    derive_more::Unwrap,
    derive_more::TryUnwrap,
    PartialEq,
    Eq,
    Hash,
  )]
  #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
  #[non_exhaustive]
  pub enum Message<I, A> {
    /// Compound message
    Compound(
      #[cfg_attr(feature = "arbitrary", arbitrary(with = super::arbitrary_triomphe_arc))]
      Arc<[Self]>,
    ) = 1,
    /// Ping message
    Ping(Ping<I, A>) = 2,
    /// Indirect ping message
    IndirectPing(IndirectPing<I, A>) = 3,
    /// Ack response message
    Ack(Ack) = 4,
    /// Suspect message
    Suspect(Suspect<I>) = 5,
    /// Alive message
    Alive(Alive<I, A>) = 6,
    /// Dead message
    Dead(Dead<I>) = 7,
    /// PushPull message
    PushPull(PushPull<I, A>) = 8,
    /// User mesg, not handled by us
    UserData(#[cfg_attr(feature = "arbitrary", arbitrary(with = super::arbitrary_bytes))] Bytes) =
      9,
    /// Nack response message
    Nack(Nack) = 10,
    /// Error response message
    ErrorResponse(ErrorResponse) = 11,
    /// Checksumed message
    Checksumed(ChecksumedMessage<I, A>) = 12,
    /// Encrypted message
    Encrypted(EncryptedMessage<I, A>) = 13,
    /// Compressed message
    Compressed(CompressedMessage<I, A>) = 14,
    /// Labeled message
    Labeled(LabeledMessage<I, A>) = Label::TAG,
  }
);

impl<I, A> Data for Message<I, A>
where
  I: Data,
  A: Data,
{
  type Ref<'a> = MessageRef<'a, I::Ref<'a>, A::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    Ok(match val {
      MessageRef::Compound(val) => val
        .iter::<I, A>()
        .map(|res| res.and_then(Message::from_ref))
        .collect::<Result<Arc<[_]>, DecodeError>>()
        .map(|msgs| Self::Compound(msgs))?,
      MessageRef::Ping(val) => Self::Ping(Ping::from_ref(val)?),
      MessageRef::IndirectPing(val) => Self::IndirectPing(IndirectPing::from_ref(val)?),
      MessageRef::Ack(val) => Self::Ack(Ack::from_ref(val)?),
      MessageRef::Suspect(val) => Self::Suspect(Suspect::from_ref(val)?),
      MessageRef::Alive(val) => Self::Alive(Alive::from_ref(val)?),
      MessageRef::Dead(val) => Self::Dead(Dead::from_ref(val)?),
      MessageRef::PushPull(val) => Self::PushPull(PushPull::from_ref(val)?),
      MessageRef::UserData(val) => Self::UserData(Bytes::from_ref(val)?),
      MessageRef::Nack(val) => Self::Nack(Nack::from_ref(val)?),
      MessageRef::ErrorResponse(val) => Self::ErrorResponse(ErrorResponse::from_ref(val)?),
      MessageRef::Checksumed(val) => Self::Checksumed(ChecksumedMessage::from_ref(val)?),
      MessageRef::Encrypted(val) => Self::Encrypted(EncryptedMessage::from_ref(val)?),
      MessageRef::Compressed(val) => Self::Compressed(CompressedMessage::from_ref(val)?),
      MessageRef::Labeled(val) => Self::Labeled(LabeledMessage::from_ref(val)?),
    })
  }

  fn encoded_len(&self) -> usize {
    1 + match self {
      Self::Compound(val) => encoded_messages_len(val),
      Self::Ping(val) => val.encoded_len_with_length_delimited(),
      Self::IndirectPing(val) => val.encoded_len_with_length_delimited(),
      Self::Ack(val) => val.encoded_len_with_length_delimited(),
      Self::Suspect(val) => val.encoded_len_with_length_delimited(),
      Self::Alive(val) => val.encoded_len_with_length_delimited(),
      Self::Dead(val) => val.encoded_len_with_length_delimited(),
      Self::PushPull(val) => val.encoded_len_with_length_delimited(),
      Self::UserData(val) => val.encoded_len_with_length_delimited(),
      Self::Nack(val) => val.encoded_len_with_length_delimited(),
      Self::ErrorResponse(val) => val.encoded_len_with_length_delimited(),
      Self::Checksumed(val) => val.encoded_len_with_length_delimited(),
      Self::Encrypted(val) => val.encoded_len_with_length_delimited(),
      Self::Compressed(val) => val.encoded_len_with_length_delimited(),
      Self::Labeled(val) => val.encoded_len_with_length_delimited(),
    }
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let len = buf.len();
    if len < 1 {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), len));
    }

    let mut offset = 0;
    buf[offset] = self.byte();
    offset += 1;

    match self {
      Self::Compound(val) => {
        offset += encode_messages_slice(val, &mut buf[offset..])?;
      }
      Self::Ping(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::IndirectPing(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Ack(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Suspect(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Alive(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Dead(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::PushPull(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::UserData(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Nack(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::ErrorResponse(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Checksumed(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Encrypted(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Compressed(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Labeled(val) => {
        offset += val.encode_length_delimited(&mut buf[offset..])?;
      }
    }

    Ok(offset)
  }
}

impl<'a, I, A> DataRef<'a, Message<I, A>> for MessageRef<'a, I::Ref<'a>, A::Ref<'a>>
where
  I: Data,
  A: Data,
{
  fn decode(src: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let len = src.len();
    if len < 1 {
      return Err(DecodeError::buffer_underflow());
    }

    let mut offset = 0;
    let b = src[offset];
    offset += 1;

    Ok(match b {
      Message::<I, A>::COMPOUND_BYTE => {
        let decoder = CompoundMessagesDecoder::new(&src[offset..]);
        (src.len(), Self::Compound(decoder))
      }
      Message::<I, A>::PING_BYTE => {
        let (bytes_read, decoded) =
          <Ping<I::Ref<'_>, A::Ref<'_>> as DataRef<Ping<I, A>>>::decode_length_delimited(
            &src[offset..],
          )?;
        offset += bytes_read;
        (offset, Self::Ping(decoded))
      }
      Message::<I, A>::INDIRECT_PING_BYTE => {
        let (bytes_read, decoded) = <IndirectPing<I::Ref<'_>, A::Ref<'_>> as DataRef<
          IndirectPing<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::IndirectPing(decoded))
      }
      Message::<I, A>::ACK_BYTE => {
        let (bytes_read, decoded) =
          <AckRef<'_> as DataRef<Ack>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Ack(decoded))
      }
      Message::<I, A>::SUSPECT_BYTE => {
        let (bytes_read, decoded) =
          <Suspect<I::Ref<'_>> as DataRef<Suspect<I>>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Suspect(decoded))
      }
      Message::<I, A>::ALIVE_BYTE => {
        let (bytes_read, decoded) = <AliveRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          Alive<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Alive(decoded))
      }
      Message::<I, A>::DEAD_BYTE => {
        let (bytes_read, decoded) =
          <Dead<I::Ref<'_>> as DataRef<Dead<I>>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Dead(decoded))
      }
      Message::<I, A>::PUSH_PULL_BYTE => {
        let (bytes_read, decoded) = <PushPullRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          PushPull<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::PushPull(decoded))
      }
      Message::<I, A>::USER_DATA_BYTE => {
        let (bytes_read, decoded) =
          <&[u8] as DataRef<Bytes>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::UserData(decoded))
      }
      Message::<I, A>::NACK_BYTE => {
        let (bytes_read, decoded) =
          <Nack as DataRef<Nack>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Nack(decoded))
      }
      Message::<I, A>::ERROR_RESPONSE_BYTE => {
        let (bytes_read, decoded) =
          <ErrorResponseRef<'_> as DataRef<ErrorResponse>>::decode_length_delimited(
            &src[offset..],
          )?;
        offset += bytes_read;
        (offset, Self::ErrorResponse(decoded))
      }
      Message::<I, A>::CHECKSUMED_BYTE => {
        let (bytes_read, decoded) = <ChecksumedMessageRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          ChecksumedMessage<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Checksumed(decoded))
      }
      Message::<I, A>::ENCRYPTED_BYTE => {
        let (bytes_read, decoded) = <EncryptedMessageRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          EncryptedMessage<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Encrypted(decoded))
      }
      Message::<I, A>::COMPRESSED_BYTE => {
        let (bytes_read, decoded) = <CompressedMessageRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          CompressedMessage<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Compressed(decoded))
      }
      Message::<I, A>::LABELED_BYTE => {
        let (bytes_read, decoded) = <LabeledMessageRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          LabeledMessage<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Labeled(decoded))
      }
      _ => {
        let (wt, tag) = super::split(b);
        WireType::try_from(wt).map_err(DecodeError::unknown_wire_type)?;
        return Err(DecodeError::unknown_tag("Message", tag));
      }
    })
  }
}

/// The reference type of the [`Message`] enum.
#[derive(
  Debug,
  Copy,
  Clone,
  derive_more::From,
  derive_more::IsVariant,
  derive_more::Unwrap,
  derive_more::TryUnwrap,
)]
pub enum MessageRef<'a, I, A> {
  /// Compound message
  Compound(CompoundMessagesDecoder<'a>),
  /// Ping message
  Ping(Ping<I, A>),
  /// Indirect ping message
  IndirectPing(IndirectPing<I, A>),
  /// Ack response message
  Ack(AckRef<'a>),
  /// Suspect message
  Suspect(Suspect<I>),
  /// Alive message
  Alive(AliveRef<'a, I, A>),
  /// Dead message
  Dead(Dead<I>),
  /// PushPull message
  PushPull(PushPullRef<'a, I, A>),
  /// User mesg, not handled by us
  UserData(&'a [u8]),
  /// Nack response message
  Nack(Nack),
  /// Error response message
  ErrorResponse(ErrorResponseRef<'a>),
  /// Checksumed message
  Checksumed(ChecksumedMessageRef<'a, I, A>),
  /// Encrypted message
  Encrypted(EncryptedMessageRef<'a, I, A>),
  /// Compressed message
  Compressed(CompressedMessageRef<'a, I, A>),
  /// Labeled message
  Labeled(LabeledMessageRef<'a, I, A>),
}

impl<I, A> MessageRef<'_, I, A> {
  /// Returns the type of the message.
  pub const fn ty(&self) -> MessageType {
    match self {
      Self::Compound(_) => MessageType::Compound,
      Self::Ping(_) => MessageType::Ping,
      Self::IndirectPing(_) => MessageType::IndirectPing,
      Self::Ack(_) => MessageType::Ack,
      Self::Suspect(_) => MessageType::Suspect,
      Self::Alive(_) => MessageType::Alive,
      Self::Dead(_) => MessageType::Dead,
      Self::PushPull(_) => MessageType::PushPull,
      Self::UserData(_) => MessageType::UserData,
      Self::Nack(_) => MessageType::Nack,
      Self::ErrorResponse(_) => MessageType::ErrorResponse,
      Self::Checksumed(_) => MessageType::Checksumed,
      Self::Encrypted(_) => MessageType::Encrypted,
      Self::Compressed(_) => MessageType::Compressed,
      Self::Labeled(_) => MessageType::Labeled,
    }
  }
}

/// A compound message encoder which can encode multiple messages into a buffer.
#[derive(Debug)]
pub struct CompoundMessagesEncoder<'a, I, A> {
  src: &'a [Message<I, A>],
}

impl<'a, I, A> CompoundMessagesEncoder<'a, I, A> {
  /// Creates a new compound message encoder.
  #[inline]
  pub const fn new(src: &'a [Message<I, A>]) -> Self {
    Self { src }
  }

  /// Encodes the messages into the buffer.
  #[inline]
  pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>
  where
    I: Data,
    A: Data,
  {
    encode_messages_slice(self.src, buf)
  }

  /// Encodes the messages into a [`Bytes`].
  #[inline]
  pub fn encode_to_bytes(&self) -> Result<Bytes, EncodeError>
  where
    I: Data,
    A: Data,
  {
    let len = self.encoded_len();
    let mut buf = vec![0; len];
    self.encode(&mut buf).map(|_| Bytes::from(buf))
  }

  /// Returns the total length of the encoded messages.
  #[inline]
  pub fn encoded_len(&self) -> usize
  where
    I: Data,
    A: Data,
  {
    encoded_messages_len(self.src)
  }
}

/// A message decoder which can yield messages from a buffer.
///
/// This decoder will not modify the source buffer and will only read from it.
#[derive(Debug, Clone, Copy)]
pub struct CompoundMessagesDecoder<'a> {
  src: &'a [u8],
}

impl<'a> CompoundMessagesDecoder<'a> {
  /// Creates a new message decoder.
  #[inline]
  pub const fn new(buf: &'a [u8]) -> Self {
    Self { src: buf }
  }

  /// Returns an iterator which can yields the reference to the messages in the buffer.
  #[inline]
  pub const fn iter<I, A>(&self) -> CompoundMessagesDecoderIter<'a, I, A> {
    CompoundMessagesDecoderIter {
      src: self.src,
      offset: 0,
      _phantom: PhantomData,
    }
  }
}

/// An iterator over the [`NodeStateRef`] in the collection.
pub struct CompoundMessagesDecoderIter<'a, I, A> {
  src: &'a [u8],
  offset: usize,
  _phantom: PhantomData<(I, A)>,
}

impl<'a, I, A> Iterator for CompoundMessagesDecoderIter<'a, I, A>
where
  I: Data,
  A: Data,
{
  type Item = Result<MessageRef<'a, I::Ref<'a>, A::Ref<'a>>, DecodeError>;

  fn next(&mut self) -> Option<Self::Item> {
    let src = self.src;
    while self.offset < src.len() {
      let b = src[self.offset];
      self.offset += 1;
      match b {
        b if b == Message::<I, A>::COMPOUND_BYTE => {
          let (bytes_read, msg) = match <<Message<I, A> as Data>::Ref<'a> as DataRef<
            Message<I, A>,
          >>::decode_length_delimited(&src[self.offset..])
          {
            Ok((bytes_read, msg)) => (bytes_read, msg),
            Err(e) => return Some(Err(e)),
          };
          self.offset += bytes_read;
          return Some(Ok(msg));
        }
        _ => {
          let (wire_type, _) = split(b);
          let wt = match WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type) {
            Ok(wt) => wt,
            Err(e) => return Some(Err(e)),
          };

          self.offset += match skip(wt, &src[self.offset..]) {
            Ok(bytes_read) => bytes_read,
            Err(e) => return Some(Err(e)),
          };
        }
      }
    }
    None
  }
}

impl<I, A> CompoundMessagesDecoderIter<'_, I, A> {
  /// Rewinds the decoder to the beginning of the source buffer.
  #[inline]
  pub fn rewind(&mut self) {
    self.offset = 0;
  }
}

#[inline]
fn encoded_messages_len<I, A>(msgs: &[Message<I, A>]) -> usize
where
  I: Data,
  A: Data,
{
  msgs
    .iter()
    .map(|msg| 1 + msg.encoded_len_with_length_delimited())
    .sum::<usize>()
}

fn encode_messages_slice<I, A>(msgs: &[Message<I, A>], buf: &mut [u8]) -> Result<usize, EncodeError>
where
  I: Data,
  A: Data,
{
  let len = buf.len();
  let mut offset = 0;
  macro_rules! bail {
    ($this:ident($offset:expr, $len:ident)) => {
      if $offset >= $len {
        return Err(EncodeError::insufficient_buffer($offset, $len).into());
      }
    };
  }

  for msg in msgs.iter() {
    bail!(self(offset, len));
    buf[offset] = Message::<I, A>::COMPOUND_BYTE;
    offset += 1;
    {
      offset += msg
        .encode_length_delimited(&mut buf[offset..])
        .map_err(|e| e.update(encoded_messages_len(msgs), len))?
    }
  }

  #[cfg(debug_assertions)]
  super::debug_assert_write_eq(offset, encoded_messages_len(msgs));

  Ok(offset)
}

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl<I, A> Message<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn quickcheck_arbitrary_helper(g: &mut Gen, ty: MessageType) -> Option<Self> {
      Some(match ty {
        MessageType::Compound => {
          return None;
        }
        MessageType::Ping => {
          let ping = Ping::<I, A>::arbitrary(g);
          Self::Ping(ping)
        }
        MessageType::IndirectPing => {
          let indirect_ping = IndirectPing::<I, A>::arbitrary(g);
          Self::IndirectPing(indirect_ping)
        }
        MessageType::Ack => {
          let ack = Ack::arbitrary(g);
          Self::Ack(ack)
        }
        MessageType::Suspect => {
          let suspect = Suspect::<I>::arbitrary(g);
          Self::Suspect(suspect)
        }
        MessageType::Alive => {
          let alive = Alive::<I, A>::arbitrary(g);
          Self::Alive(alive)
        }
        MessageType::Dead => {
          let dead = Dead::<I>::arbitrary(g);
          Self::Dead(dead)
        }
        MessageType::PushPull => {
          let push_pull = PushPull::<I, A>::arbitrary(g);
          Self::PushPull(push_pull)
        }
        MessageType::UserData => {
          let bytes = Vec::<u8>::arbitrary(g).into();
          Self::UserData(bytes)
        }
        MessageType::Nack => {
          let nack = Nack::arbitrary(g);
          Self::Nack(nack)
        }
        MessageType::ErrorResponse => {
          let error_response = ErrorResponse::arbitrary(g);
          Self::ErrorResponse(error_response)
        }
        MessageType::Checksumed => {
          let checksumed = ChecksumedMessage::<I, A>::arbitrary(g);
          Self::Checksumed(checksumed)
        }
        MessageType::Encrypted => {
          let encrypted = EncryptedMessage::<I, A>::arbitrary(g);
          Self::Encrypted(encrypted)
        }
        MessageType::Compressed => {
          let compressed = CompressedMessage::<I, A>::arbitrary(g);
          Self::Compressed(compressed)
        }
        MessageType::Labeled => {
          let labeled = LabeledMessage::<I, A>::arbitrary(g);
          Self::Labeled(labeled)
        }
      })
    }
  }

  impl<I, A> Arbitrary for Message<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      let ty = MessageType::arbitrary(g);
      match ty {
        MessageType::Compound => {
          let num = u8::arbitrary(g) as usize;
          let compound = (0..num)
            .filter_map(|_| {
              let ty = MessageType::arbitrary(g);
              Message::<I, A>::quickcheck_arbitrary_helper(g, ty)
            })
            .collect::<Arc<[_]>>();
          Self::Compound(compound)
        }
        _ => Self::quickcheck_arbitrary_helper(g, ty).unwrap(),
      }
    }
  }
};
