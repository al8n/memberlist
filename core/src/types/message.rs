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
    $(#[$outer])*
    $vis enum $name $(< $($generic),+ >)? {
      $(
        $(#[$variant_meta])*
        $variant($variant_ty $(< $($variant_generic),+ >)?),
      )*
    }

    impl $(< $($generic),+ >)? $name $(< $($generic),+ >)? {
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
    /// Ping message
    Ping(Ping<I, A>) = 0,
    /// Indirect ping message
    IndirectPing(IndirectPing<I, A>) = 1,
    /// Ack response message
    Ack(Ack) = 2,
    /// Suspect message
    Suspect(Suspect<I>) = 3,
    /// Alive message
    Alive(Alive<I, A>) = 4,
    /// Dead message
    Dead(Dead<I>) = 5,
    /// PushPull message
    PushPull(PushPull<I, A>) = 6,
    /// User mesg, not handled by us
    UserData(Bytes) = 7,
    /// Nack response message
    Nack(Nack) = 8,
    /// Error response message
    ErrorResponse(ErrorResponse) = 9,
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
}
