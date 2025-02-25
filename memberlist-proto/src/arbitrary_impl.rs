use super::{Label, Meta};

use arbitrary::{Arbitrary, Result, Unstructured};

impl<'a> Arbitrary<'a> for Meta {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    let len = u.int_in_range(0..=Self::MAX_SIZE)?;
    let mut buf = Vec::with_capacity(len);
    for _ in 0..len {
      buf.push(u.arbitrary::<u8>()?);
    }
    Ok(Meta::try_from(buf).unwrap())
  }
}

#[cfg(any(
  feature = "crc32",
  feature = "xxhash64",
  feature = "xxhash32",
  feature = "xxhash3",
  feature = "murmur3",
))]
impl<'a> Arbitrary<'a> for super::ChecksumAlgorithm {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(Self::from(u.arbitrary::<u8>()?))
  }
}

#[cfg(any(
  feature = "zstd",
  feature = "snappy",
  feature = "lz4",
  feature = "brotli",
))]
impl<'a> Arbitrary<'a> for super::CompressAlgorithm {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(Self::from(u.arbitrary::<u16>()?))
  }
}

#[cfg(feature = "encryption")]
impl<'a> Arbitrary<'a> for super::EncryptionAlgorithm {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(Self::from(u.arbitrary::<u8>()?))
  }
}

#[cfg(feature = "encryption")]
impl<'a> Arbitrary<'a> for super::SecretKeys {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(u.arbitrary::<Vec<super::SecretKey>>()?.into())
  }
}

impl<'a> Arbitrary<'a> for Label {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    let mut s = String::new();
    while s.len() < 253 {
      let c = u.arbitrary::<char>()?;
      let char_len = c.len_utf8();

      if s.len() + char_len > 253 {
        break;
      }
      s.push(c);
    }

    Ok(Label(s.into()))
  }
}

pub(super) fn bytes(u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<bytes::Bytes> {
  u.arbitrary::<Vec<u8>>().map(Into::into)
}

pub(super) fn triomphe_arc<'a, T: arbitrary::Arbitrary<'a>>(
  u: &mut arbitrary::Unstructured<'a>,
) -> arbitrary::Result<triomphe::Arc<[T]>> {
  u.arbitrary::<Vec<T>>().map(Into::into)
}

pub(super) fn from_u8<T: From<u8>>(u: &mut arbitrary::Unstructured<'_>) -> arbitrary::Result<T> {
  Ok(T::from(u.arbitrary::<u8>()?))
}

impl<'a> Arbitrary<'a> for super::State {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    from_u8(u)
  }
}

impl<'a> Arbitrary<'a> for super::ProtocolVersion {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    from_u8(u)
  }
}

impl<'a> Arbitrary<'a> for super::DelegateVersion {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    from_u8(u)
  }
}
