use bytes::Bytes;
use either::Either;

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
        $variant($variant_ty $(< $($variant_generic),+ >)?),
      )*
    }

    paste::paste! {
      impl$(< $($generic),+ >)? Data for $name $(< $($generic),+ >)?
      $(where
        $(
          $generic: Data,
        )*
      )?
      {
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
          buf[offset] = self.byte();
          offset += 1;

          match self {
            $(
              Self::$variant(val) => {
                offset += val.encode_length_delimited(&mut buf[offset..])?;
              },
            )*
          }

          Ok(offset)
        }

        fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
        where
          Self: Sized,
        {
          let len = src.len();
          if len < 1 {
            return Err(DecodeError::new("buffer underflow"));
          }

          let mut offset = 0;
          let b = src[offset];
          offset += 1;

          Ok(match b {
            $(
              Self::[< $variant:snake:upper _BYTE >] => {
                let (bytes_read, decoded) = <$variant_ty $(< $($variant_generic),+ >)?>::decode_length_delimited(&src[offset..])?;
                offset += bytes_read;
                (offset, Self::$variant(decoded))
              },
            )*
            _ => {
              let (_, tag) = super::split(b);
              return Err(DecodeError::new(format!("unknown message tag: {tag}")));
            }
          })
        }
      }
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
}

/// A collection of messages.
#[allow(clippy::type_complexity)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Messages<'a, I, A>(Either<&'a [Message<I, A>], Vec<Message<I, A>>>);

impl<I, A> core::fmt::Debug for Messages<'_, I, A>
where
  I: core::fmt::Debug,
  A: core::fmt::Debug,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_list().entries(self.0.iter()).finish()
  }
}

impl<'a, I, A> From<&'a [Message<I, A>]> for Messages<'a, I, A> {
  fn from(messages: &'a [Message<I, A>]) -> Self {
    Self(Either::Left(messages))
  }
}

impl<I, A> From<Vec<Message<I, A>>> for Messages<'_, I, A> {
  fn from(messages: Vec<Message<I, A>>) -> Self {
    Self(Either::Right(messages))
  }
}
impl<I, A> Messages<'_, I, A> {
  const TAG: u8 = 1;
  const fn byte() -> u8 {
    merge(WireType::LengthDelimited, Self::TAG)
  }
}

impl<I, A> Data for Messages<'_, I, A>
where
  I: Data,
  A: Data,
{
  fn encoded_len(&self) -> usize {
    self
      .0
      .iter()
      .map(|msg| 1 + msg.encoded_len_with_length_delimited())
      .sum::<usize>()
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let len = buf.len();
    let mut offset = 0;
    macro_rules! bail {
      ($this:ident($offset:expr, $len:ident)) => {
        if $offset >= $len {
          return Err(EncodeError::insufficient_buffer($offset, $len).into());
        }
      };
    }

    let msgs = match &self.0 {
      Either::Left(msgs) => msgs.iter(),
      Either::Right(msgs) => msgs.iter(),
    };

    for msg in msgs {
      bail!(self(offset, len));
      buf[offset] = Self::byte();
      offset += 1;
      {
        offset += msg
          .encode_length_delimited(&mut buf[offset..])
          .map_err(|e| e.update(self.encoded_len(), len))?
      }
    }

    #[cfg(debug_assertions)]
    super::debug_assert_write_eq(offset, self.encoded_len());

    Ok(offset)
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let mut msgs = Vec::new();

    while offset < src.len() {
      let b = src[offset];
      offset += 1;
      match b {
        b if b == Self::byte() => {
          let (bytes_read, msg) = Message::decode_length_delimited(&src[offset..])?;
          offset += bytes_read;
          msgs.push(msg);
        }
        _ => {
          let (wire_type, _) = split(b);
          let wt = WireType::try_from(wire_type)
            .map_err(|_| DecodeError::new(format!("unknown wire type: {}", wire_type)))?;

          offset += skip(wt, &src[offset..])?;
        }
      }
    }

    Ok((offset, Self::from(msgs)))
  }
}

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a, I, A> Arbitrary<'a> for Message<I, A>
  where
    I: Arbitrary<'a>,
    A: Arbitrary<'a>,
  {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      let ty = u.arbitrary::<MessageType>()?;
      match ty {
        MessageType::Ping => {
          let ping = u.arbitrary::<Ping<I, A>>()?;
          Ok(Self::Ping(ping))
        }
        MessageType::IndirectPing => {
          let indirect_ping = u.arbitrary::<IndirectPing<I, A>>()?;
          Ok(Self::IndirectPing(indirect_ping))
        }
        MessageType::Ack => {
          let ack = u.arbitrary::<Ack>()?;
          Ok(Self::Ack(ack))
        }
        MessageType::Suspect => {
          let suspect = u.arbitrary::<Suspect<I>>()?;
          Ok(Self::Suspect(suspect))
        }
        MessageType::Alive => {
          let alive = u.arbitrary::<Alive<I, A>>()?;
          Ok(Self::Alive(alive))
        }
        MessageType::Dead => {
          let dead = u.arbitrary::<Dead<I>>()?;
          Ok(Self::Dead(dead))
        }
        MessageType::PushPull => {
          let push_pull = u.arbitrary::<PushPull<I, A>>()?;
          Ok(Self::PushPull(push_pull))
        }
        MessageType::UserData => {
          let bytes = u.arbitrary::<Vec<u8>>()?.into();
          Ok(Self::UserData(bytes))
        }
        MessageType::Nack => {
          let nack = u.arbitrary::<Nack>()?;
          Ok(Self::Nack(nack))
        }
        MessageType::ErrorResponse => {
          let error_response = u.arbitrary::<ErrorResponse>()?;
          Ok(Self::ErrorResponse(error_response))
        }
      }
    }
  }

  impl<'a, I, A> Arbitrary<'a> for Messages<'a, I, A>
  where
    I: Arbitrary<'a>,
    A: Arbitrary<'a>,
  {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(Self::from(u.arbitrary::<Vec<_>>()?))
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::{Arbitrary, Gen};

  impl<I, A> Arbitrary for Message<I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      let ty = MessageType::arbitrary(g);
      match ty {
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
      }
    }
  }

  impl<I, A> Arbitrary for Messages<'static, I, A>
  where
    I: Arbitrary,
    A: Arbitrary,
  {
    fn arbitrary(g: &mut Gen) -> Self {
      Self::from(Vec::<_>::arbitrary(g))
    }
  }
};
