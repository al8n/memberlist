use bytes::Bytes;

use super::*;

pub use label::*;
pub use proto::*;

mod label;
mod proto;
#[cfg(feature = "serde")]
mod serde_impl;

const COMPOOUND_MESSAGE_TAG: u8 = 1;
const PING_MESSAGE_TAG: u8 = 2;
const INDIRECT_PING_MESSAGE_TAG: u8 = 3;
const ACK_MESSAGE_TAG: u8 = 4;
const SUSPECT_MESSAGE_TAG: u8 = 5;
const ALIVE_MESSAGE_TAG: u8 = 6;
const DEAD_MESSAGE_TAG: u8 = 7;
const PUSH_PULL_MESSAGE_TAG: u8 = 8;
const USER_DATA_MESSAGE_TAG: u8 = 9;
const NACK_MESSAGE_TAG: u8 = 10;
const ERROR_RESPONSE_MESSAGE_TAG: u8 = 11;
const CHECKSUMED_MESSAGE_TAG: u8 = 12;
const COMPRESSED_MESSAGE_TAG: u8 = 13;
const ENCRYPTED_MESSAGE_TAG: u8 = 14;
const LABELED_MESSAGE_TAG: u8 = Label::TAG;

#[inline]
const fn is_plain_message_tag(tag: u8) -> bool {
  matches!(
    tag,
    PING_MESSAGE_TAG
      | INDIRECT_PING_MESSAGE_TAG
      | ACK_MESSAGE_TAG
      | SUSPECT_MESSAGE_TAG
      | ALIVE_MESSAGE_TAG
      | DEAD_MESSAGE_TAG
      | PUSH_PULL_MESSAGE_TAG
      | USER_DATA_MESSAGE_TAG
      | NACK_MESSAGE_TAG
      | ERROR_RESPONSE_MESSAGE_TAG
  )
}

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
      #[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
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
        /// All possible values of this enum.
        pub const POSSIBLE_VALUES: &'static [Self] = &[
          $(
            Self::$variant,
          )*
        ];

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
        /// Returns the tag of this message type for encoding/decoding.
        #[inline]
        pub const fn tag(&self) -> u8 {
          match self {
            $(
              Self::$variant(_) => $variant_tag,
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
  #[cfg_attr(any(feature = "arbitrary", test), derive(arbitrary::Arbitrary))]
  #[non_exhaustive]
  pub enum Message<I, A> {
    /// Ping message
    Ping(Ping<I, A>) = PING_MESSAGE_TAG,
    /// Indirect ping message
    IndirectPing(IndirectPing<I, A>) = INDIRECT_PING_MESSAGE_TAG,
    /// Ack response message
    Ack(Ack) = ACK_MESSAGE_TAG,
    /// Suspect message
    Suspect(Suspect<I>) = SUSPECT_MESSAGE_TAG,
    /// Alive message
    Alive(Alive<I, A>) = ALIVE_MESSAGE_TAG,
    /// Dead message
    Dead(Dead<I>) = DEAD_MESSAGE_TAG,
    /// PushPull message
    PushPull(PushPull<I, A>) = PUSH_PULL_MESSAGE_TAG,
    /// User mesg, not handled by us
    UserData(
      #[cfg_attr(any(feature = "arbitrary", test), arbitrary(with = crate::arbitrary_impl::bytes))]
      Bytes,
    ) = USER_DATA_MESSAGE_TAG,
    /// Nack response message
    Nack(Nack) = NACK_MESSAGE_TAG,
    /// Error response message
    ErrorResponse(ErrorResponse) = ERROR_RESPONSE_MESSAGE_TAG,
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
      Self::Ref::Ping(val) => Self::Ping(Ping::from_ref(val)?),
      Self::Ref::IndirectPing(val) => Self::IndirectPing(IndirectPing::from_ref(val)?),
      Self::Ref::Ack(val) => Self::Ack(Ack::from_ref(val)?),
      Self::Ref::Suspect(val) => Self::Suspect(Suspect::from_ref(val)?),
      Self::Ref::Alive(val) => Self::Alive(Alive::from_ref(val)?),
      Self::Ref::Dead(val) => Self::Dead(Dead::from_ref(val)?),
      Self::Ref::PushPull(val) => Self::PushPull(PushPull::from_ref(val)?),
      Self::Ref::UserData(val) => Self::UserData(Bytes::from_ref(val)?),
      Self::Ref::Nack(val) => Self::Nack(Nack::from_ref(val)?),
      Self::Ref::ErrorResponse(val) => Self::ErrorResponse(ErrorResponse::from_ref(val)?),
    })
  }

  fn encoded_len(&self) -> usize {
    1 + match self {
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
    }
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let len = buf.len();
    if len < 1 {
      return Err(EncodeError::insufficient_buffer(self.encoded_len(), len));
    }

    let mut offset = 0;
    buf[offset] = self.tag();
    offset += 1;

    match self {
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
      PING_MESSAGE_TAG => {
        let (bytes_read, decoded) =
          <Ping<I::Ref<'_>, A::Ref<'_>> as DataRef<Ping<I, A>>>::decode_length_delimited(
            &src[offset..],
          )?;
        offset += bytes_read;
        (offset, Self::Ping(decoded))
      }
      INDIRECT_PING_MESSAGE_TAG => {
        let (bytes_read, decoded) = <IndirectPing<I::Ref<'_>, A::Ref<'_>> as DataRef<
          IndirectPing<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::IndirectPing(decoded))
      }
      ACK_MESSAGE_TAG => {
        let (bytes_read, decoded) =
          <AckRef<'_> as DataRef<Ack>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Ack(decoded))
      }
      SUSPECT_MESSAGE_TAG => {
        let (bytes_read, decoded) =
          <Suspect<I::Ref<'_>> as DataRef<Suspect<I>>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Suspect(decoded))
      }
      ALIVE_MESSAGE_TAG => {
        let (bytes_read, decoded) = <AliveRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          Alive<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Alive(decoded))
      }
      DEAD_MESSAGE_TAG => {
        let (bytes_read, decoded) =
          <Dead<I::Ref<'_>> as DataRef<Dead<I>>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Dead(decoded))
      }
      PUSH_PULL_MESSAGE_TAG => {
        let (bytes_read, decoded) = <PushPullRef<'_, I::Ref<'_>, A::Ref<'_>> as DataRef<
          PushPull<I, A>,
        >>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::PushPull(decoded))
      }
      USER_DATA_MESSAGE_TAG => {
        let (bytes_read, decoded) =
          <&[u8] as DataRef<Bytes>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::UserData(decoded))
      }
      NACK_MESSAGE_TAG => {
        let (bytes_read, decoded) =
          <Nack as DataRef<Nack>>::decode_length_delimited(&src[offset..])?;
        offset += bytes_read;
        (offset, Self::Nack(decoded))
      }
      ERROR_RESPONSE_MESSAGE_TAG => {
        let (bytes_read, decoded) =
          <ErrorResponseRef<'_> as DataRef<ErrorResponse>>::decode_length_delimited(
            &src[offset..],
          )?;
        offset += bytes_read;
        (offset, Self::ErrorResponse(decoded))
      }
      tag => {
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
}

impl<I, A> MessageRef<'_, I, A> {
  /// Returns the type of the message.
  pub const fn ty(&self) -> MessageType {
    match self {
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
    }
  }
}

// /// A compound message encoder which can encode multiple messages into a buffer.
// #[derive(Debug)]
// pub struct CompoundProtoEncoder<'a, I, A> {
//   src: &'a [Message<I, A>],
// }

// impl<'a, I, A> CompoundProtoEncoder<'a, I, A> {
//   /// Creates a new compound message encoder.
//   #[inline]
//   pub const fn new(src: &'a [Message<I, A>]) -> Self {
//     Self { src }
//   }

//   /// Encodes the messages into the buffer.
//   #[inline]
//   pub fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError>
//   where
//     I: Data,
//     A: Data,
//   {
//     encode_messages_slice(self.src, buf)
//   }

//   /// Encodes the messages into a [`Bytes`].
//   #[inline]
//   pub fn encode_to_bytes(&self) -> Result<Bytes, EncodeError>
//   where
//     I: Data,
//     A: Data,
//   {
//     let len = self.encoded_len();
//     let mut buf = vec![0; len];
//     self.encode(&mut buf).map(|_| Bytes::from(buf))
//   }

//   /// Returns the total length of the encoded messages.
//   #[inline]
//   pub fn encoded_len(&self) -> usize
//   where
//     I: Data,
//     A: Data,
//   {
//     encoded_messages_len(self.src)
//   }
// }

// /// A message decoder which can yield messages from a buffer.
// ///
// /// This decoder will not modify the source buffer and will only read from it.
// #[derive(Debug, Clone, Copy)]
// pub struct CompoundMessagesDecoder<'a> {
//   src: &'a [u8],
// }

// impl<'a> CompoundMessagesDecoder<'a> {
//   /// Creates a new message decoder.
//   #[inline]
//   pub const fn new(buf: &'a [u8]) -> Self {
//     Self { src: buf }
//   }

//   /// Returns an iterator which can yields the reference to the messages in the buffer.
//   #[inline]
//   pub const fn iter<I, A>(&self) -> CompoundMessagesDecoderIter<'a, I, A> {
//     CompoundMessagesDecoderIter {
//       src: self.src,
//       offset: 0,
//       _phantom: PhantomData,
//     }
//   }
// }

// /// An iterator over the [`NodeStateRef`] in the collection.
// pub struct CompoundMessagesDecoderIter<'a, I, A> {
//   src: &'a [u8],
//   offset: usize,
//   _phantom: PhantomData<(I, A)>,
// }

// impl<'a, I, A> Iterator for CompoundMessagesDecoderIter<'a, I, A>
// where
//   I: Data,
//   A: Data,
// {
//   type Item = Result<MessageRef<'a, I::Ref<'a>, A::Ref<'a>>, DecodeError>;

//   fn next(&mut self) -> Option<Self::Item> {
//     let src = self.src;
//     while self.offset < src.len() {
//       let b = src[self.offset];
//       self.offset += 1;
//       match b {
//         b if b == Message::<I, A>::COMPOUND_BYTE => {
//           let (bytes_read, msg) = match <<Message<I, A> as Data>::Ref<'a> as DataRef<
//             Message<I, A>,
//           >>::decode_length_delimited(&src[self.offset..])
//           {
//             Ok((bytes_read, msg)) => (bytes_read, msg),
//             Err(e) => return Some(Err(e)),
//           };
//           self.offset += bytes_read;
//           return Some(Ok(msg));
//         }
//         _ => {
//           let (wire_type, _) = split(b);
//           let wt = match WireType::try_from(wire_type).map_err(DecodeError::unknown_wire_type) {
//             Ok(wt) => wt,
//             Err(e) => return Some(Err(e)),
//           };

//           self.offset += match skip(wt, &src[self.offset..]) {
//             Ok(bytes_read) => bytes_read,
//             Err(e) => return Some(Err(e)),
//           };
//         }
//       }
//     }
//     None
//   }
// }

// impl<I, A> CompoundMessagesDecoderIter<'_, I, A> {
//   /// Rewinds the decoder to the beginning of the source buffer.
//   #[inline]
//   pub fn rewind(&mut self) {
//     self.offset = 0;
//   }
// }

// #[inline]
// fn encoded_messages_len<I, A>(msgs: &[Message<I, A>]) -> usize
// where
//   I: Data,
//   A: Data,
// {
//   msgs
//     .iter()
//     .map(|msg| 1 + msg.encoded_len_with_length_delimited())
//     .sum::<usize>()
// }

// fn encode_messages_slice<I, A>(_: &[Message<I, A>], _: &mut [u8]) -> Result<usize, EncodeError>
// where
//   I: Data,
//   A: Data,
// {
//   // let len = buf.len();
//   // let mut offset = 0;
//   // macro_rules! bail {
//   //   ($this:ident($offset:expr, $len:ident)) => {
//   //     if $offset >= $len {
//   //       return Err(EncodeError::insufficient_buffer($offset, $len).into());
//   //     }
//   //   };
//   // }

//   // for msg in msgs.iter() {
//   //   bail!(self(offset, len));
//   //   buf[offset] = Message::<I, A>::COMPOUND_BYTE;
//   //   offset += 1;
//   //   {
//   //     offset += msg
//   //       .encode_length_delimited(&mut buf[offset..])
//   //       .map_err(|e| e.update(encoded_messages_len(msgs), len))?
//   //   }
//   // }

//   // #[cfg(debug_assertions)]
//   // super::debug_assert_write_eq(offset, encoded_messages_len(msgs));

//   // Ok(offset)
//   todo!()
// }
