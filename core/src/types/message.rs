use bytes::Bytes;

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
    paste::paste! {
      #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
      #[repr(u8)]
      #[non_exhaustive]
      $vis enum [< $name Type >] {
        Compound = 0,
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
            0 => Ok(Self::Compound),
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
            Self::Compound => 0,
            $(
              Self::$variant => $variant_tag,
            )*
          }
        }

        /// Returns the kind of this message.
        #[inline]
        pub const fn kind(&self) -> &'static str {
          match self {
            Self::Compound => "Compound",
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
  /// with [`Wire`] or [`Transport`],
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
  ///
  /// [`Wire`]: https://docs.rs/memberlist/latest/memberlist/transport/trait.Wire.html
  /// [`Transport`]: https://docs.rs/memberlist/latest/memberlist/transport/trait.Transport.html
  pub const RESERVED_TAG_RANGE: std::ops::RangeInclusive<u8> = (0..=128);

  /// Returns the tag of the compound message type for encoding/decoding.
  pub const COMPOUND_TAG: u8 = 0;

  /// Returns the encoded length of the message
  pub fn encoded_len(&self) -> usize
  where
    I: Data,
    A: Data,
  {
    1 + match self {
      Self::Ping(ping) => ping.encoded_len(),
      Self::IndirectPing(indirect_ping) => indirect_ping.encoded_len(),
      Self::Ack(ack) => ack.encoded_len(),
      Self::Suspect(suspect) => suspect.encoded_len(),
      Self::Alive(alive) => alive.encoded_len(),
      Self::Dead(dead) => dead.encoded_len(),
      Self::PushPull(push_pull) => push_pull.encoded_len(),
      Self::UserData(bytes) => super::encoded_length_delimited_len(bytes.len()),
      Self::Nack(nack) => nack.encoded_len(),
      Self::ErrorResponse(error_response) => error_response.encoded_len(),
    }
  }
}

const USER_DATA_LEN_SIZE: usize = core::mem::size_of::<u32>();
const INLINED_BYTES_SIZE: usize = 64;

// #[cfg(test)]
// mod test {
//   use std::net::SocketAddr;

//   use super::*;

//   #[tokio::test]
//   async fn test_ping_transformable_round_trip() {
//     let msg = Message::Ping(Ping::generate(1));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_ack_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::Ack(Ack::random(10));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_indirect_ping_transformable_round_trip() {
//     let msg = Message::IndirectPing(IndirectPing::generate(1));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_nack_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::Nack(Nack::new(10));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_suspect_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::Suspect(Suspect::generate(10));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_dead_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::Dead(Dead::generate(10));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_alive_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::Alive(Alive::random(128));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_push_pull_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::PushPull(PushPull::generate(10));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_user_data_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::UserData(Bytes::from_static(b"hello world"));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }

//   #[tokio::test]
//   async fn test_error_response_transformable_round_trip() {
//     let msg = Message::<SmolStr, SocketAddr>::ErrorResponse(ErrorResponse::new("hello world"));
//     let mut buf = vec![0u8; msg.encoded_len()];
//     msg.encode(&mut buf).unwrap();
//     let (len, decoded) = Message::decode(&buf).unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();

//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);

//     let (len, decoded) = Message::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
//       .await
//       .unwrap();
//     assert_eq!(len, buf.len());
//     assert_eq!(decoded, msg);
//   }
// }
