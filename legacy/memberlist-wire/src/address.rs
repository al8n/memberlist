use nodecraft::CheapClone;

use super::{
  Data, DataRef, DecodeError, EncodeError,
  utils::{merge, skip},
};

/// `MaybeResolvedAddress` is used to represent an address that may or may not be resolved.
#[derive(
  Clone,
  Copy,
  Debug,
  PartialEq,
  Eq,
  Hash,
  derive_more::Display,
  derive_more::TryUnwrap,
  derive_more::Unwrap,
  derive_more::IsVariant,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[unwrap(ref, ref_mut)]
#[try_unwrap(ref, ref_mut)]
pub enum MaybeResolvedAddress<A, R> {
  /// The resolved address, which means that can be directly used to communicate with the remote node.
  #[display("{_0}")]
  Resolved(R),
  /// The unresolved address, which means that need to be resolved before using it to communicate with the remote node.
  #[display("{_0}")]
  Unresolved(A),
}

impl<A: CheapClone, R: CheapClone> CheapClone for MaybeResolvedAddress<A, R> {
  fn cheap_clone(&self) -> Self {
    match self {
      Self::Resolved(addr) => Self::Resolved(addr.cheap_clone()),
      Self::Unresolved(addr) => Self::Unresolved(addr.cheap_clone()),
    }
  }
}

impl<A, R> MaybeResolvedAddress<A, R> {
  /// Creates a resolved address.
  #[inline]
  pub const fn resolved(addr: R) -> Self {
    Self::Resolved(addr)
  }

  /// Creates an unresolved address.
  #[inline]
  pub const fn unresolved(addr: A) -> Self {
    Self::Unresolved(addr)
  }

  /// Converts from `&MaybeResolvedAddress<A, R>` to `MaybeResolvedAddress<&A, &R>`.
  #[inline]
  pub const fn as_ref(&self) -> MaybeResolvedAddress<&A, &R> {
    match self {
      Self::Resolved(addr) => MaybeResolvedAddress::Resolved(addr),
      Self::Unresolved(addr) => MaybeResolvedAddress::Unresolved(addr),
    }
  }

  /// Converts from `&mut MaybeResolvedAddress<A, R>` to `MaybeResolvedAddress<&mut A, &mut R>`.
  #[inline]
  pub const fn as_mut(&mut self) -> MaybeResolvedAddress<&mut A, &mut R> {
    match self {
      Self::Resolved(addr) => MaybeResolvedAddress::Resolved(addr),
      Self::Unresolved(addr) => MaybeResolvedAddress::Unresolved(addr),
    }
  }
}

const RESOLVED_TAG: u8 = 1;
const UNRESOLVED_TAG: u8 = 2;

impl<A, R> MaybeResolvedAddress<A, R>
where
  A: Data,
  R: Data,
{
  #[inline]
  const fn resolved_byte() -> u8 {
    merge(R::WIRE_TYPE, RESOLVED_TAG)
  }

  #[inline]
  const fn unresolved_byte() -> u8 {
    merge(A::WIRE_TYPE, UNRESOLVED_TAG)
  }
}

impl<'a, A, R> DataRef<'a, MaybeResolvedAddress<A, R>>
  for MaybeResolvedAddress<A::Ref<'a>, R::Ref<'a>>
where
  A: Data,
  R: Data,
{
  fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError>
  where
    Self: Sized,
  {
    let mut offset = 0;
    let buf_len = buf.len();
    let mut addr = None;

    while offset < buf_len {
      match buf[offset] {
        byte if byte == MaybeResolvedAddress::<A, R>::resolved_byte() => {
          if addr.is_some() {
            return Err(DecodeError::duplicate_field(
              "MaybeResolvedAddress",
              "value",
              RESOLVED_TAG,
            ));
          }

          offset += 1;

          let (len, val) = <R::Ref<'_> as DataRef<'_, R>>::decode_length_delimited(&buf[offset..])?;
          offset += len;
          addr = Some(Self::Resolved(val));
        }
        byte if byte == MaybeResolvedAddress::<A, R>::unresolved_byte() => {
          if addr.is_some() {
            return Err(DecodeError::duplicate_field(
              "MaybeResolvedAddress",
              "value",
              UNRESOLVED_TAG,
            ));
          }
          offset += 1;

          let (len, val) = <A::Ref<'_> as DataRef<'_, A>>::decode_length_delimited(&buf[offset..])?;
          offset += len;
          addr = Some(Self::Unresolved(val));
        }
        _ => offset += skip("MaybeResolvedAddress", &buf[offset..])?,
      }
    }

    let addr = addr.ok_or_else(|| DecodeError::missing_field("MaybeResolvedAddress", "value"))?;
    Ok((offset, addr))
  }
}

impl<A, R> Data for MaybeResolvedAddress<A, R>
where
  A: Data,
  R: Data,
{
  type Ref<'a> = MaybeResolvedAddress<A::Ref<'a>, R::Ref<'a>>;

  fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError>
  where
    Self: Sized,
  {
    match val {
      MaybeResolvedAddress::Resolved(addr) => R::from_ref(addr).map(Self::Resolved),
      MaybeResolvedAddress::Unresolved(addr) => A::from_ref(addr).map(Self::Unresolved),
    }
  }

  fn encoded_len(&self) -> usize {
    1 + match self {
      Self::Resolved(addr) => addr.encoded_len_with_length_delimited(),
      Self::Unresolved(addr) => addr.encoded_len_with_length_delimited(),
    }
  }

  fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let mut offset = 0;
    let buf_len = buf.len();
    match self {
      Self::Resolved(addr) => {
        if buf_len < 1 {
          return Err(EncodeError::insufficient_buffer(
            self.encoded_len(),
            buf_len,
          ));
        }

        buf[offset] = Self::resolved_byte();
        offset += 1;
        offset += addr.encode_length_delimited(&mut buf[offset..])?;
      }
      Self::Unresolved(addr) => {
        if buf_len < 1 {
          return Err(EncodeError::insufficient_buffer(
            self.encoded_len(),
            buf_len,
          ));
        }

        buf[offset] = Self::unresolved_byte();
        offset += 1;
        offset += addr.encode_length_delimited(&mut buf[offset..])?;
      }
    }

    #[cfg(debug_assertions)]
    super::debug_assert_write_eq::<Self>(offset, self.encoded_len());

    Ok(offset)
  }
}
