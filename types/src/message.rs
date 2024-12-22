use byteorder::{ByteOrder, NetworkEndian};
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
  #[error(transparent)]
  Ping(#[from] PingTransformError<I, A>),
  /// Returned when the fail to transform indirect ping message.
  #[error(transparent)]
  IndirectPing(#[from] IndirectPingTransformError<I, A>),
  /// Returned when the fail to transform ack message.
  #[error(transparent)]
  Ack(#[from] AckTransformError),
  /// Returned when the fail to transform suspect message.
  #[error(transparent)]
  Suspect(#[from] SuspectTransformError<I>),
  /// Returned when the fail to transform alive message.
  #[error(transparent)]
  Alive(#[from] AliveTransformError<I, A>),
  /// Returned when the fail to transform dead message.
  #[error(transparent)]
  Dead(#[from] DeadTransformError<I>),
  /// Returned when the fail to transform push pull message.
  #[error(transparent)]
  PushPull(#[from] PushPullTransformError<I, A>),
  /// Returned when the fail to transform user data message.
  #[error(transparent)]
  UserData(#[from] BytesTransformError),
  /// Returned when the fail to transform nack message.
  #[error(transparent)]
  Nack(#[from] NumberTransformError),
  /// Returned when the fail to transform error response message.
  #[error(transparent)]
  ErrorResponse(#[from] StringTransformError),
}

const USER_DATA_LEN_SIZE: usize = core::mem::size_of::<u32>();
const INLINED_BYTES_SIZE: usize = 64;

impl<I, A> Transformable for Message<I, A>
where
  I: Transformable + core::fmt::Debug,
  A: Transformable + core::fmt::Debug,
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
      Self::Ping(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::IndirectPing(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::Ack(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::Suspect(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::Alive(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::Dead(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::PushPull(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::UserData(msg) => {
        let len = msg.len();
        NetworkEndian::write_u32(dst, len as u32);
        dst = &mut dst[USER_DATA_LEN_SIZE..];
        dst[..len].copy_from_slice(msg);
        1 + USER_DATA_LEN_SIZE + len
      }
      Self::Nack(msg) => msg.encode(dst).map(|w| w + 1)?,
      Self::ErrorResponse(msg) => msg.encode(dst).map(|w| w + 1)?,
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
      Self::UserData(msg) => USER_DATA_LEN_SIZE + msg.len(),
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
        let len = NetworkEndian::read_u32(src) as usize;
        let src = &src[USER_DATA_LEN_SIZE..];
        let msg = Bytes::copy_from_slice(&src[..len]);
        (len + 1 + USER_DATA_LEN_SIZE, Self::UserData(msg))
      }
      Self::NACK_TAG => {
        let (len, msg) = u32::decode(src)?;
        (len + 1, Self::Nack(Nack::new(msg)))
      }
      Self::ERRORRESPONSE_TAG => {
        let (len, msg) = <SmolStr as Transformable>::decode(src)?;
        (len + 1, Self::ErrorResponse(ErrorResponse::new(msg)))
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
        let mut len_buf = [0u8; USER_DATA_LEN_SIZE];
        reader.read_exact(&mut len_buf)?;
        let len = NetworkEndian::read_u32(&len_buf) as usize;
        if len <= INLINED_BYTES_SIZE {
          let mut buf = [0u8; INLINED_BYTES_SIZE];
          reader.read_exact(&mut buf[..len])?;
          (
            len + 1 + USER_DATA_LEN_SIZE,
            Self::UserData(Bytes::copy_from_slice(&buf[..len])),
          )
        } else {
          let mut buf = vec![0u8; len];
          reader.read_exact(&mut buf)?;
          (len + 1 + USER_DATA_LEN_SIZE, Self::UserData(buf.into()))
        }
      }
      Self::NACK_TAG => {
        let (len, msg) = Nack::decode_from_reader(reader)?;
        (len + 1, Self::Nack(msg))
      }
      Self::ERRORRESPONSE_TAG => {
        let (len, msg) = ErrorResponse::decode_from_reader(reader)?;
        (len + 1, Self::ErrorResponse(msg))
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
        let mut len_buf = [0u8; USER_DATA_LEN_SIZE];
        reader.read_exact(&mut len_buf).await?;
        let len = NetworkEndian::read_u32(&len_buf) as usize;
        if len <= INLINED_BYTES_SIZE {
          let mut buf = [0u8; INLINED_BYTES_SIZE];
          reader.read_exact(&mut buf[..len]).await?;
          (
            len + 1 + USER_DATA_LEN_SIZE,
            Self::UserData(Bytes::copy_from_slice(&buf[..len])),
          )
        } else {
          let mut buf = vec![0u8; len];
          reader.read_exact(&mut buf).await?;
          (len + 1 + USER_DATA_LEN_SIZE, Self::UserData(buf.into()))
        }
      }
      Self::NACK_TAG => {
        let (len, msg) = Nack::decode_from_async_reader(reader).await?;
        (len + 1, Self::Nack(msg))
      }
      Self::ERRORRESPONSE_TAG => {
        let (len, msg) = ErrorResponse::decode_from_async_reader(reader).await?;
        (len + 1, Self::ErrorResponse(msg))
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

#[cfg(test)]
mod test {
  use std::net::SocketAddr;

  use super::*;

  #[tokio::test]
  async fn test_ping_transformable_round_trip() {
    let msg = Message::Ping(Ping::generate(1));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_ack_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::Ack(Ack::random(10));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_indirect_ping_transformable_round_trip() {
    let msg = Message::IndirectPing(IndirectPing::generate(1));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_nack_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::Nack(Nack::new(10));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_suspect_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::Suspect(Suspect::generate(10));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_dead_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::Dead(Dead::generate(10));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_alive_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::Alive(Alive::random(128));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_push_pull_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::PushPull(PushPull::generate(10));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_user_data_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::UserData(Bytes::from_static(b"hello world"));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }

  #[tokio::test]
  async fn test_error_response_transformable_round_trip() {
    let msg = Message::<SmolStr, SocketAddr>::ErrorResponse(ErrorResponse::new("hello world"));
    let mut buf = vec![0u8; msg.encoded_len()];
    msg.encode(&mut buf).unwrap();
    let (len, decoded) = Message::decode(&buf).unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);

    let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
      .await
      .unwrap();
    assert_eq!(len, buf.len());
    assert_eq!(decoded, msg);
  }
}
