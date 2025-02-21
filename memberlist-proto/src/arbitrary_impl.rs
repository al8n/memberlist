use super::{
  ChecksumAlgorithm, CompressAlgorithm, DelegateVersion, EncryptionAlgorithm, Label, Meta,
  ProtocolVersion, State,
};

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

impl<'a> Arbitrary<'a> for State {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(u.arbitrary::<u8>()?.into())
  }
}

impl<'a> Arbitrary<'a> for DelegateVersion {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(u.arbitrary::<u8>()?.into())
  }
}

impl<'a> Arbitrary<'a> for ProtocolVersion {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(u.arbitrary::<u8>()?.into())
  }
}

impl<'a> Arbitrary<'a> for ChecksumAlgorithm {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(Self::from(u.arbitrary::<u8>()?))
  }
}

impl<'a> Arbitrary<'a> for CompressAlgorithm {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(Self::from(u.arbitrary::<u16>()?))
  }
}

impl<'a> Arbitrary<'a> for EncryptionAlgorithm {
  fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
    Ok(Self::from(u.arbitrary::<u8>()?))
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
