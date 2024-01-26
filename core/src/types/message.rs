use bytes::Bytes;
use futures::AsyncRead;
use smol_str::SmolStr;
use transformable::{
  BytesTransformError, NumberTransformError, StringTransformError, Transformable,
};

use super::*;

macro_rules! enum_wrapper {
  (
    $(#[$outer:meta])*
    $vis:vis enum $name:ident $(<$($generic:tt),+>)? {
      $(
        $(#[$variant_meta:meta])*
        $variant:ident($variant_ty: ident $(<$($variant_generic:tt),+>)?) = $variant_tag:literal
      ), +$(,)?
    }
  ) => {
    $(#[$outer])*
    $vis enum $name $(< $($generic),+ >)? {
      $(
        $(#[$variant_meta])*
        $variant($variant_ty $(< $($variant_generic),+ >)?),
      )*
    }

    impl $(< $($generic),+ >)? $name $(< $($generic),+ >)? {
      paste::paste! {
        $(
          #[doc = concat!("The tag of [`", stringify!($variant_ty), "`] message.")]
          pub const [< $variant: upper _TAG >]: u8 = $variant_tag;
        )*
      }

      /// Returns the tag of this message type for encoding/decoding.
      #[inline]
      pub const fn tag(&self) -> u8 {
        match self {
          $(
            Self::$variant(_) => $variant_tag,
          )*
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
          #[doc = concat!("Returns the contained [`", stringify!($variant_ty), "`] message, consuming the self value. Panics if the value is not [`", stringify!($variant_ty), "`].")]
          $vis fn [< unwrap_ $variant:snake>] (self) -> $variant_ty $(< $($variant_generic),+ >)? {
            if let Self::$variant(val) = self {
              val
            } else {
              panic!(concat!("expect ", stringify!($variant), ", buf got {}"), self.kind())
            }
          }

          #[doc = concat!("Returns the contained [`", stringify!($variant_ty), "`] message, consuming the self value. Returns `None` if the value is not [`", stringify!($variant_ty), "`].")]
          $vis fn [< try_unwrap_ $variant:snake>] (self) -> ::std::option::Option<$variant_ty $(< $($variant_generic),+ >)?> {
            if let Self::$variant(val) = self {
              ::std::option::Option::Some(val)
            } else {
              ::std::option::Option::None
            }
          }

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
  #[derive(Debug, Clone, derive_more::From, PartialEq, Eq, Hash)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
  #[non_exhaustive]
  pub enum Message<I, A> {
    // DO NOT REMOVE THE COMMENT BELOW!!!
    // tag 0 is reserved for compound message
    // Compound(OneOrMore<Message<I, A>>) = 0,
    /// Ping message
    Ping(Ping<I, A>) = 1,
    /// Indirect ping message
    IndirectPing(IndirectPing<I, A>) = 2,
    /// Ack response message
    Ack(Ack) = 3,
    /// Suspect message
    Suspect(Suspect<I>) = 4,
    /// Alive message
    Alive(Alive<I, A>) = 5,
    /// Dead message
    Dead(Dead<I>) = 6,
    /// PushPull message
    PushPull(PushPull<I, A>) = 7,
    /// User mesg, not handled by us
    UserData(Bytes) = 8,
    /// Nack response message
    Nack(Nack) = 9,
    /// Error response message
    ErrorResponse(ErrorResponse) = 10,
  }
);

impl<I, A> Message<I, A> {
  /// Defines the range of reserved tags for message types.
  ///
  /// This constant specifies a range of tag values that are reserved for internal use
  /// by the [`Message`] enum variants. When implementing custom
  /// with [`Wire`](crate::transport::Wire) or [`Transport`](crate::transport::Transport),
  /// it is important to ensure that any custom header added to the message bytes does not
  /// start with a tag value within this reserved range.
  ///
  /// The reserved range is `0..=128`, meaning that the first byte of any custom message
  /// must not fall within this range to avoid conflicts with predefined message types.
  ///
  /// # Note
  ///
  /// Adhering to this constraint is crucial for ensuring that custom messages
  /// are correctly distinguishable from the standard messages defined by the `Message` enum.
  /// Failing to do so may result in incorrect message parsing and handling.
  pub const RESERVED_TAG_RANGE: std::ops::RangeInclusive<u8> = (0..=128);

  /// Returns the tag of the compound message type for encoding/decoding.
  pub const COMPOUND_TAG: u8 = 0;
}

/// Transform error for [`Message`].
#[derive(Debug, thiserror::Error)]
pub enum MessageTransformError<I: Transformable, A: Transformable> {
  /// Returned when the buffer is too small to encode.
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// Returned when the buffer is too small to decode.
  #[error("not enough bytes to decode message")]
  NotEnoughBytes,
  /// Returned when the fail to transform ping message.
  #[error("{0}")]
  Ping(#[from] PingTransformError<I, A>),
  /// Returned when the fail to transform indirect ping message.
  #[error("{0}")]
  IndirectPing(#[from] IndirectPingTransformError<I, A>),
  /// Returned when the fail to transform ack message.
  #[error("{0}")]
  Ack(#[from] AckTransformError),
  /// Returned when the fail to transform suspect message.
  #[error("{0}")]
  Suspect(#[from] SuspectTransformError<I>),
  /// Returned when the fail to transform alive message.
  #[error("{0}")]
  Alive(#[from] AliveTransformError<I, A>),
  /// Returned when the fail to transform dead message.
  #[error("{0}")]
  Dead(#[from] DeadTransformError<I>),
  /// Returned when the fail to transform push pull message.
  #[error("{0}")]
  PushPull(#[from] PushPullTransformError<I, A>),
  /// Returned when the fail to transform user data message.
  #[error("{0}")]
  UserData(#[from] BytesTransformError),
  /// Returned when the fail to transform nack message.
  #[error("{0}")]
  Nack(#[from] NumberTransformError),
  /// Returned when the fail to transform error response message.
  #[error("{0}")]
  ErrorResponse(#[from] StringTransformError),
}

impl<I: Transformable + core::fmt::Debug, A: Transformable + core::fmt::Debug> Transformable
  for Message<I, A>
{
  type Error = MessageTransformError<I, A>;

  fn encode(&self, mut dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    dst[0] = self.tag();
    dst = &mut dst[1..];

    Ok(match self {
      Self::Ping(msg) => msg.encode(dst)?,
      Self::IndirectPing(msg) => msg.encode(dst)?,
      Self::Ack(msg) => msg.encode(dst)?,
      Self::Suspect(msg) => msg.encode(dst)?,
      Self::Alive(msg) => msg.encode(dst)?,
      Self::Dead(msg) => msg.encode(dst)?,
      Self::PushPull(msg) => msg.encode(dst)?,
      Self::UserData(msg) => msg.encode(dst)?,
      Self::Nack(msg) => msg.encode(dst)?,
      Self::ErrorResponse(msg) => msg.encode(dst)?,
    })
  }

  fn encoded_len(&self) -> usize {
    1 + match self {
      Self::Ping(msg) => msg.encoded_len(),
      Self::IndirectPing(msg) => msg.encoded_len(),
      Self::Ack(msg) => msg.encoded_len(),
      Self::Suspect(msg) => msg.encoded_len(),
      Self::Alive(msg) => msg.encoded_len(),
      Self::Dead(msg) => msg.encoded_len(),
      Self::PushPull(msg) => msg.encoded_len(),
      Self::UserData(msg) => msg.encoded_len(),
      Self::Nack(msg) => msg.encoded_len(),
      Self::ErrorResponse(msg) => msg.encoded_len(),
    }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.is_empty() {
      return Err(Self::Error::NotEnoughBytes);
    }

    let tag = src[0];
    let src = &src[1..];

    Ok(match tag {
      Self::PING_TAG => {
        let (len, msg) = Ping::decode(src)?;
        (len + 1, Self::Ping(msg))
      }
      Self::INDIRECTPING_TAG => {
        let (len, msg) = IndirectPing::decode(src)?;
        (len + 1, Self::IndirectPing(msg))
      }
      Self::ACK_TAG => {
        let (len, msg) = Ack::decode(src)?;
        (len + 1, Self::Ack(msg))
      }
      Self::SUSPECT_TAG => {
        let (len, msg) = Suspect::decode(src)?;
        (len + 1, Self::Suspect(msg))
      }
      Self::ALIVE_TAG => {
        let (len, msg) = Alive::decode(src)?;
        (len + 1, Self::Alive(msg))
      }
      Self::DEAD_TAG => {
        let (len, msg) = Dead::decode(src)?;
        (len + 1, Self::Dead(msg))
      }
      Self::PUSHPULL_TAG => {
        let (len, msg) = PushPull::decode(src)?;
        (len + 1, Self::PushPull(msg))
      }
      Self::USERDATA_TAG => {
        let (len, msg) = Bytes::decode(src)?;
        (len + 1, Self::UserData(msg))
      }
      Self::NACK_TAG => {
        let (len, msg) = u32::decode(src)?;
        (len + 1, Self::Nack(Nack { seq_no: msg }))
      }
      Self::ERRORRESPONSE_TAG => {
        let (len, msg) = <SmolStr as Transformable>::decode(src)?;
        (len + 1, Self::ErrorResponse(ErrorResponse { err: msg }))
      }
      _ => return Err(Self::Error::NotEnoughBytes),
    })
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut tag = [0u8; 1];
    reader.read_exact(&mut tag)?;
    let tag = tag[0];
    let (len, msg) = match tag {
      Self::PING_TAG => {
        let (len, msg) = Ping::decode_from_reader(reader)?;
        (len + 1, Self::Ping(msg))
      }
      Self::INDIRECTPING_TAG => {
        let (len, msg) = IndirectPing::decode_from_reader(reader)?;
        (len + 1, Self::IndirectPing(msg))
      }
      Self::ACK_TAG => {
        let (len, msg) = Ack::decode_from_reader(reader)?;
        (len + 1, Self::Ack(msg))
      }
      Self::SUSPECT_TAG => {
        let (len, msg) = Suspect::decode_from_reader(reader)?;
        (len + 1, Self::Suspect(msg))
      }
      Self::ALIVE_TAG => {
        let (len, msg) = Alive::decode_from_reader(reader)?;
        (len + 1, Self::Alive(msg))
      }
      Self::DEAD_TAG => {
        let (len, msg) = Dead::decode_from_reader(reader)?;
        (len + 1, Self::Dead(msg))
      }
      Self::PUSHPULL_TAG => {
        let (len, msg) = PushPull::decode_from_reader(reader)?;
        (len + 1, Self::PushPull(msg))
      }
      Self::USERDATA_TAG => {
        let (len, msg) = Bytes::decode_from_reader(reader)?;
        (len + 1, Self::UserData(msg))
      }
      Self::NACK_TAG => {
        let (len, msg) = u32::decode_from_reader(reader)?;
        (len + 1, Self::Nack(Nack { seq_no: msg }))
      }
      Self::ERRORRESPONSE_TAG => {
        let (len, msg) = <SmolStr as Transformable>::decode_from_reader(reader)?;
        (len + 1, Self::ErrorResponse(ErrorResponse { err: msg }))
      }
      _ => {
        return Err(std::io::Error::new(
          std::io::ErrorKind::InvalidData,
          "unknown message",
        ))
      }
    };
    Ok((len, msg))
  }

  async fn decode_from_async_reader<R: AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    use futures::io::AsyncReadExt;

    let mut tag = [0u8; 1];
    reader.read_exact(&mut tag).await?;
    let tag = tag[0];
    let (len, msg) = match tag {
      Self::PING_TAG => {
        let (len, msg) = Ping::decode_from_async_reader(reader).await?;
        (len + 1, Self::Ping(msg))
      }
      Self::INDIRECTPING_TAG => {
        let (len, msg) = IndirectPing::decode_from_async_reader(reader).await?;
        (len + 1, Self::IndirectPing(msg))
      }
      Self::ACK_TAG => {
        let (len, msg) = Ack::decode_from_async_reader(reader).await?;
        (len + 1, Self::Ack(msg))
      }
      Self::SUSPECT_TAG => {
        let (len, msg) = Suspect::decode_from_async_reader(reader).await?;
        (len + 1, Self::Suspect(msg))
      }
      Self::ALIVE_TAG => {
        let (len, msg) = Alive::decode_from_async_reader(reader).await?;
        (len + 1, Self::Alive(msg))
      }
      Self::DEAD_TAG => {
        let (len, msg) = Dead::decode_from_async_reader(reader).await?;
        (len + 1, Self::Dead(msg))
      }
      Self::PUSHPULL_TAG => {
        let (len, msg) = PushPull::decode_from_async_reader(reader).await?;
        (len + 1, Self::PushPull(msg))
      }
      Self::USERDATA_TAG => {
        let (len, msg) = Bytes::decode_from_async_reader(reader).await?;
        (len + 1, Self::UserData(msg))
      }
      Self::NACK_TAG => {
        let (len, msg) = u32::decode_from_async_reader(reader).await?;
        (len + 1, Self::Nack(Nack { seq_no: msg }))
      }
      Self::ERRORRESPONSE_TAG => {
        let (len, msg) = <SmolStr as Transformable>::decode_from_async_reader(reader).await?;
        (len + 1, Self::ErrorResponse(ErrorResponse { err: msg }))
      }
      _ => {
        return Err(std::io::Error::new(
          std::io::ErrorKind::InvalidData,
          "unknown message",
        ))
      }
    };
    Ok((len, msg))
  }
}
