use bytes::Bytes;

use super::{super::*, *};

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

        /// Returns the wire type of this message.
        #[inline]
        pub const fn wire_type $(< $($generic),+ >)?(&self) -> WireType
        where
          $($($generic: Data),+)?
        {
          match self {
            $(
              Self::$variant => <$variant_ty $(< $($variant_generic),+ >)? as Data>::WIRE_TYPE,
            )*
          }
        }

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
      /// Returns the tag of this message type for encoding/decoding.
      #[inline]
      pub const fn tag(&self) -> u8 {
        match self {
          $(
            Self::$variant(_) => $variant_tag,
          )*
        }
      }

      paste::paste! {
        /// Returns the type of the message.
        #[inline]
        pub const fn ty(&self) -> [< $name Type >] {
          match self {
            $(
              Self::$variant(_) => [< $name Type >]::$variant,
            )*
          }
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

    paste::paste! {
      impl $(< $($generic),+ >)? [< $name Re f>]<'_, $( $($generic),+ )?> {
        /// Returns the type of the message.
        #[inline]
        pub const fn ty(&self) -> [< $name Type >] {
          match self {
            $(
              Self::$variant(_) => [< $name Type >]::$variant,
            )*
          }
        }
      }

      impl$(< $($generic),+ >)? Data for $name $(< $($generic),+ >)?
      $(
        where
          $($generic: Data),+
      )?
      {
        type Ref<'a> = [< $name Ref >]<'a, $($($generic::Ref<'a>),+)?>;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
        where
          Self: Sized,
        {
          Ok(match val {
            $(
              Self::Ref::$variant(val) => Self::$variant($variant_ty::from_ref(val)?),
            )*
          })
        }

        fn encoded_len(&self) -> usize {
          1 + match self {
            $(
              Self::$variant(val) => val.encoded_len_with_length_delimited(),
            )*
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
            $(
              Self::$variant(val) => {
                offset += val.encode_length_delimited(&mut buf[offset..])?;
              }
            )*
          }

          Ok(offset)
        }
      }
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
