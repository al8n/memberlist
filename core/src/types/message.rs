use bytes::Bytes;

use super::*;

/// A compound message.
pub type CompoundMessage<I, A> = Vec<Message<I, A>>;

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
    /// Compound message
    Compound(CompoundMessage<I, A>) = 7,
    /// User mesg, not handled by us
    UserData(Bytes) = 8,
    /// Nack response message
    Nack(Nack) = 9,
    /// Error response message
    ErrorResponse(ErrorResponse) = 10,
  }
);
