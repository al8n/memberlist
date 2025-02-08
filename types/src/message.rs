// use bytes::Bytes;
// use triomphe::Arc;

// use super::*;

// macro_rules! enum_wrapper {
//   (
//     $(#[$outer:meta])*
//     $vis:vis enum $name:ident $(<$($generic:tt),+>)? {
//       $(
//         $(#[$variant_meta:meta])*
//         $variant:ident($variant_ty: ident $(<$($variant_generic:tt),+>)?) = $variant_tag:literal
//       ), +$(,)?
//     }
//   ) => {
//     paste::paste! {
//       #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::IsVariant)]
//       #[repr(u8)]
//       #[non_exhaustive]
//       #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
//       $vis enum [< $name Type >] {
//         $(
//           $(#[$variant_meta])*
//           $variant = $variant_tag,
//         )*
//       }

//       impl core::fmt::Display for [< $name Type >] {
//         fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//           write!(f, "{}", self.kind())
//         }
//       }

//       impl TryFrom<u8> for [< $name Type >] {
//         type Error = u8;

//         fn try_from(value: u8) -> Result<Self, Self::Error> {
//           match value {
//             $(
//               $variant_tag => Ok(Self::$variant),
//             )*
//             _ => Err(value),
//           }
//         }
//       }

//       impl [< $name Type >] {
//         /// Returns the tag of this message type for encoding/decoding.
//         #[inline]
//         pub const fn tag(&self) -> u8 {
//           match self {
//             $(
//               Self::$variant => $variant_tag,
//             )*
//           }
//         }

//         /// Returns the kind of this message.
//         #[inline]
//         pub const fn kind(&self) -> &'static str {
//           match self {
//             $(
//               Self::$variant => stringify!([< $variant:camel >]),
//             )*
//           }
//         }
//       }

//       #[cfg(feature = "quickcheck")]
//       const _: () = {
//         use quickcheck::{Arbitrary, Gen};

//         impl [< $name Type >] {
//           const POSSIBLE_VALUES: &'static [u8] = &[
//             $(
//               $variant_tag,
//             )*
//           ];
//         }

//         impl Arbitrary for [< $name Type >] {
//           fn arbitrary(g: &mut Gen) -> Self {
//             let val = g.choose(&Self::POSSIBLE_VALUES).unwrap();
//             Self::try_from(*val).unwrap()
//           }
//         }
//       };
//     }

//     $(#[$outer])*
//     $vis enum $name $(< $($generic),+ >)? {
//       $(
//         $(#[$variant_meta])*
//         $variant($variant_ty $(< $($variant_generic),+ >)?),
//       )*
//     }

//     paste::paste! {
//       impl$(< $($generic),+ >)? Data for $name $(< $($generic),+ >)?
//       $(where
//         $(
//           $generic: Data,
//         )*
//       )?
//       {
//         fn encoded_len(&self) -> usize {
//           1 + match self {
//             $(
//               Self::$variant(val) => val.encoded_len_with_length_delimited(),
//             )*
//           }
//         }

//         fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
//           let len = buf.len();
//           if len < 1 {
//             return Err(EncodeError::insufficient_buffer(self.encoded_len(), len));
//           }

//           let mut offset = 0;
//           buf[offset] = self.byte();
//           offset += 1;

//           match self {
//             $(
//               Self::$variant(val) => {
//                 offset += val.encode_length_delimited(&mut buf[offset..])?;
//               },
//             )*
//           }

//           Ok(offset)
//         }

//         fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
//         where
//           Self: Sized,
//         {
//           let len = src.len();
//           if len < 1 {
//             return Err(DecodeError::new("buffer underflow"));
//           }

//           let mut offset = 0;
//           let b = src[offset];
//           offset += 1;

//           Ok(match b {
//             $(
//               Self::[< $variant:snake:upper _BYTE >] => {
//                 let (bytes_read, decoded) = <$variant_ty $(< $($variant_generic),+ >)?>::decode_length_delimited(&src[offset..])?;
//                 offset += bytes_read;
//                 (offset, Self::$variant(decoded))
//               },
//             )*
//             _ => {
//               let (_, tag) = super::split(b);
//               return Err(DecodeError::new(format!("unknown message tag: {tag}")));
//             }
//           })
//         }
//       }
//     }

//     impl $(< $($generic),+ >)? $name $(< $($generic),+ >)? {
//       paste::paste! {
//         $(
//           #[doc = concat!("The tag of [`", stringify!($variant_ty), "`] message.")]
//           pub const [< $variant: snake:upper _TAG >]: u8 = $variant_tag;
//           const [< $variant:snake:upper _BYTE >]: u8 = super::merge(WireType::LengthDelimited, $variant_tag);
//         )*

//         /// Returns the tag of this message type for encoding/decoding.
//         #[inline]
//         pub const fn tag(&self) -> u8 {
//           match self {
//             $(
//               Self::$variant(_) => Self::[< $variant: snake:upper _TAG >],
//             )*
//           }
//         }

//         const fn byte(&self) -> u8 {
//           match self {
//             $(
//               Self::$variant(_) => Self::[< $variant: snake:upper _BYTE >],
//             )*
//           }
//         }
//       }

//       /// Returns the kind of this message.
//       #[inline]
//       pub const fn kind(&self) -> &'static str {
//         match self {
//           $(
//             Self::$variant(_) => stringify!($variant),
//           )*
//         }
//       }

//       $(
//         paste::paste! {
//           #[doc = concat!("Construct a [`", stringify!($name), "`] from [`", stringify!($variant_ty), "`].")]
//           pub const fn [< $variant:snake >](val: $variant_ty $(< $($variant_generic),+ >)?) -> Self {
//             Self::$variant(val)
//           }
//         }
//       )*
//     }
//   };
// }

// // type Cow<'a, I, A> = Either<&'a [Message<'a, I, A>], Vec<Message<'static, I, A>>>;

// enum_wrapper!(
//   /// Request to be sent to the Raft node.
//   #[derive(
//     Debug,
//     Clone,
//     derive_more::From,
//     derive_more::IsVariant,
//     derive_more::Unwrap,
//     derive_more::TryUnwrap,
//     PartialEq,
//     Eq,
//     Hash,
//   )]
//   #[non_exhaustive]
//   pub enum Message<I, A> {
//     /// Compound message
//     Compound(Arc<[Self]>) = 1,
//     /// Ping message
//     Ping(Ping<I, A>) = 2,
//     /// Indirect ping message
//     IndirectPing(IndirectPing<I, A>) = 3,
//     /// Ack response message
//     Ack(Ack) = 4,
//     /// Suspect message
//     Suspect(Suspect<I>) = 5,
//     /// Alive message
//     Alive(Alive<I, A>) = 6,
//     /// Dead message
//     Dead(Dead<I>) = 7,
//     /// PushPull message
//     PushPull(PushPull<I, A>) = 8,
//     /// User mesg, not handled by us
//     UserData(Bytes) = 9,
//     /// Nack response message
//     Nack(Nack) = 10,
//     /// Error response message
//     ErrorResponse(ErrorResponse) = 11,
//   }
// );

// impl<I, A> Message<I, A> {
//   /// Defines the range of reserved tags for message types.
//   ///
//   /// This constant specifies a range of tag values that are reserved for internal use
//   /// by the [`Message`] enum variants. When implementing custom
//   /// with [`Wire`] or [`Transport`],
//   /// it is important to ensure that any custom header added to the message bytes does not
//   /// start with a tag value within this reserved range.
//   ///
//   /// The reserved range is `0..=128`, meaning that the first byte of any custom message
//   /// must not fall within this range to avoid conflicts with predefined message types.
//   ///
//   /// # Note
//   ///
//   /// Adhering to this constraint is crucial for ensuring that custom messages
//   /// are correctly distinguishable from the standard messages defined by the `Message` enum.
//   /// Failing to do so may result in incorrect message parsing and handling.
//   ///
//   /// [`Wire`]: https://docs.rs/memberlist/latest/memberlist/transport/trait.Wire.html
//   /// [`Transport`]: https://docs.rs/memberlist/latest/memberlist/transport/trait.Transport.html
//   pub const RESERVED_TAG_RANGE: std::ops::RangeInclusive<u8> = (0..=128);
// }

// macro_rules! impl_data_for_collections {
//   ($($ty:ty), +$(,)?) => {
//     $(
//       impl<I, A> Data for $ty
//       where
//         I: Data,
//         A: Data,
//       {
//         fn encoded_len(&self) -> usize {
//           encoded_messages_len(self.as_ref())
//         }

//         fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
//           encode_messages_slice(self.as_ref(), buf)
//         }

//         fn decode(src: &[u8]) -> Result<(usize, Self), DecodeError>
//         where
//           Self: Sized,
//         {
//           let mut decoder = MessageDecoder::new(src);
//           (&mut decoder)
//             .collect::<Result<Self, _>>()
//             .map(|msgs| (decoder.offset, msgs))
//         }
//       }
//     )*
//   };
// }

// impl_data_for_collections!(
//   Vec<Message<I, A>>,
//   triomphe::Arc<[Message<I, A>]>,
//   std::sync::Arc<[Message<I, A>]>,
//   std::boxed::Box<[Message<I, A>]>,
//   smallvec_wrapper::OneOrMore<Message<I, A>>,
//   smallvec_wrapper::TinyVec<Message<I, A>>,
//   smallvec_wrapper::TriVec<Message<I, A>>,
//   smallvec_wrapper::SmallVec<Message<I, A>>,
//   smallvec_wrapper::MediumVec<Message<I, A>>,
// );

// /// A message decoder which can yield messages from a buffer.
// ///
// /// This decoder will not modify the source buffer and will only read from it.
// #[derive(Debug)]
// pub struct MessageDecoder<I, A, B> {
//   src: B,
//   offset: usize,
//   _m: core::marker::PhantomData<(I, A)>,
// }

// impl<I, A, B: Clone> Clone for MessageDecoder<I, A, B> {
//   fn clone(&self) -> Self {
//     Self {
//       src: self.src.clone(),
//       offset: self.offset,
//       _m: core::marker::PhantomData,
//     }
//   }
// }

// impl<I, A, B: Copy> Copy for MessageDecoder<I, A, B> {}

// impl<I, A, B> MessageDecoder<I, A, B> {
//   /// Creates a new message decoder.
//   #[inline]
//   pub const fn new(buf: B) -> Self {
//     Self {
//       src: buf,
//       offset: 0,
//       _m: core::marker::PhantomData,
//     }
//   }

//   /// Returns the current offset of the decoder.
//   #[inline]
//   pub const fn offset(&self) -> usize {
//     self.offset
//   }

//   /// Consumes the decoder and returns the source buffer.
//   #[inline]
//   pub fn into_components(self) -> (usize, B) {
//     (self.offset, self.src)
//   }
// }

// impl<I, A, B> Iterator for MessageDecoder<I, A, B>
// where
//   I: Data,
//   A: Data,
//   B: AsRef<[u8]>,
// {
//   type Item = Result<Message<I, A>, DecodeError>;

//   fn next(&mut self) -> Option<Self::Item> {
//     let src = self.src.as_ref();
//     while self.offset < src.len() {
//       let b = src[self.offset];
//       self.offset += 1;
//       match b {
//         b if b == Message::<I, A>::COMPOUND_BYTE => {
//           let (bytes_read, msg) = match Message::decode_length_delimited(&src[self.offset..]) {
//             Ok((bytes_read, msg)) => (bytes_read, msg),
//             Err(e) => return Some(Err(e)),
//           };
//           self.offset += bytes_read;
//           return Some(Ok(msg));
//         }
//         _ => {
//           let (wire_type, _) = split(b);
//           let wt = match WireType::try_from(wire_type)
//             .map_err(|_| DecodeError::new(format!("unknown wire type: {}", wire_type)))
//           {
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

// fn encode_messages_slice<I, A>(msgs: &[Message<I, A>], buf: &mut [u8]) -> Result<usize, EncodeError>
// where
//   I: Data,
//   A: Data,
// {
//   let len = buf.len();
//   let mut offset = 0;
//   macro_rules! bail {
//     ($this:ident($offset:expr, $len:ident)) => {
//       if $offset >= $len {
//         return Err(EncodeError::insufficient_buffer($offset, $len).into());
//       }
//     };
//   }

//   for msg in msgs.iter() {
//     bail!(self(offset, len));
//     buf[offset] = Message::<I, A>::COMPOUND_BYTE;
//     offset += 1;
//     {
//       offset += msg
//         .encode_length_delimited(&mut buf[offset..])
//         .map_err(|e| e.update(encoded_messages_len(msgs), len))?
//     }
//   }

//   #[cfg(debug_assertions)]
//   super::debug_assert_write_eq(offset, encoded_messages_len(msgs));

//   Ok(offset)
// }

// #[cfg(feature = "arbitrary")]
// const _: () = {
//   use arbitrary::{Arbitrary, Unstructured};

//   impl<'a, I, A> Message<I, A>
//   where
//     I: Arbitrary<'a>,
//     A: Arbitrary<'a>,
//   {
//     fn arbitrary_helper(
//       u: &mut Unstructured<'a>,
//       ty: MessageType,
//     ) -> arbitrary::Result<Option<Self>> {
//       Ok(Some(match ty {
//         MessageType::Compound => return Ok(None),
//         MessageType::Ping => {
//           let ping = u.arbitrary::<Ping<I, A>>()?;
//           Self::Ping(ping)
//         }
//         MessageType::IndirectPing => {
//           let indirect_ping = u.arbitrary::<IndirectPing<I, A>>()?;
//           Self::IndirectPing(indirect_ping)
//         }
//         MessageType::Ack => {
//           let ack = u.arbitrary::<Ack>()?;
//           Self::Ack(ack)
//         }
//         MessageType::Suspect => {
//           let suspect = u.arbitrary::<Suspect<I>>()?;
//           Self::Suspect(suspect)
//         }
//         MessageType::Alive => {
//           let alive = u.arbitrary::<Alive<I, A>>()?;
//           Self::Alive(alive)
//         }
//         MessageType::Dead => {
//           let dead = u.arbitrary::<Dead<I>>()?;
//           Self::Dead(dead)
//         }
//         MessageType::PushPull => {
//           let push_pull = u.arbitrary::<PushPull<I, A>>()?;
//           Self::PushPull(push_pull)
//         }
//         MessageType::UserData => {
//           let bytes = u.arbitrary::<Vec<u8>>()?.into();
//           Self::UserData(bytes)
//         }
//         MessageType::Nack => {
//           let nack = u.arbitrary::<Nack>()?;
//           Self::Nack(nack)
//         }
//         MessageType::ErrorResponse => {
//           let error_response = u.arbitrary::<ErrorResponse>()?;
//           Self::ErrorResponse(error_response)
//         }
//       }))
//     }
//   }

//   impl<'a, I, A> Arbitrary<'a> for Message<I, A>
//   where
//     I: Arbitrary<'a>,
//     A: Arbitrary<'a>,
//   {
//     fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
//       let ty = u.arbitrary::<MessageType>()?;
//       match ty {
//         MessageType::Compound => {
//           let num = u8::arbitrary(u)? as usize;
//           let compound = (0..num)
//             .filter_map(|_| {
//               MessageType::arbitrary(u)
//                 .and_then(|ty| Message::<I, A>::arbitrary_helper(u, ty))
//                 .transpose()
//             })
//             .collect::<Result<Arc<[_]>, _>>()?;
//           Ok(Self::Compound(compound))
//         }
//         _ => Self::arbitrary_helper(u, ty)?.ok_or(arbitrary::Error::IncorrectFormat),
//       }
//     }
//   }
// };

// #[cfg(feature = "quickcheck")]
// const _: () = {
//   use quickcheck::{Arbitrary, Gen};

//   impl<I, A> Message<I, A>
//   where
//     I: Arbitrary,
//     A: Arbitrary,
//   {
//     fn quickcheck_arbitrary_helper(g: &mut Gen, ty: MessageType) -> Option<Self> {
//       Some(match ty {
//         MessageType::Compound => {
//           return None;
//         }
//         MessageType::Ping => {
//           let ping = Ping::<I, A>::arbitrary(g);
//           Self::Ping(ping)
//         }
//         MessageType::IndirectPing => {
//           let indirect_ping = IndirectPing::<I, A>::arbitrary(g);
//           Self::IndirectPing(indirect_ping)
//         }
//         MessageType::Ack => {
//           let ack = Ack::arbitrary(g);
//           Self::Ack(ack)
//         }
//         MessageType::Suspect => {
//           let suspect = Suspect::<I>::arbitrary(g);
//           Self::Suspect(suspect)
//         }
//         MessageType::Alive => {
//           let alive = Alive::<I, A>::arbitrary(g);
//           Self::Alive(alive)
//         }
//         MessageType::Dead => {
//           let dead = Dead::<I>::arbitrary(g);
//           Self::Dead(dead)
//         }
//         MessageType::PushPull => {
//           let push_pull = PushPull::<I, A>::arbitrary(g);
//           Self::PushPull(push_pull)
//         }
//         MessageType::UserData => {
//           let bytes = Vec::<u8>::arbitrary(g).into();
//           Self::UserData(bytes)
//         }
//         MessageType::Nack => {
//           let nack = Nack::arbitrary(g);
//           Self::Nack(nack)
//         }
//         MessageType::ErrorResponse => {
//           let error_response = ErrorResponse::arbitrary(g);
//           Self::ErrorResponse(error_response)
//         }
//       })
//     }
//   }

//   impl<I, A> Arbitrary for Message<I, A>
//   where
//     I: Arbitrary,
//     A: Arbitrary,
//   {
//     fn arbitrary(g: &mut Gen) -> Self {
//       let ty = MessageType::arbitrary(g);
//       match ty {
//         MessageType::Compound => {
//           let num = u8::arbitrary(g) as usize;
//           let compound = (0..num)
//             .filter_map(|_| {
//               let ty = MessageType::arbitrary(g);
//               Message::<I, A>::quickcheck_arbitrary_helper(g, ty)
//             })
//             .collect::<Arc<[_]>>();
//           Self::Compound(compound)
//         }
//         _ => Self::quickcheck_arbitrary_helper(g, ty).unwrap(),
//       }
//     }
//   }
// };
