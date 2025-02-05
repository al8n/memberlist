use length_delimited::InsufficientBuffer;

use super::Data;

/// Delegate version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[non_exhaustive]
pub enum DelegateVersion {
  /// Version 1
  #[default]
  V1,

  /// Unknown version (used for forwards and backwards compatibility)
  Unknown(u8),
}

impl core::fmt::Display for DelegateVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::V1 => write!(f, "V1"),
      Self::Unknown(val) => write!(f, "Unknown({})", val),
    }
  }
}

impl From<u8> for DelegateVersion {
  fn from(v: u8) -> Self {
    match v {
      1 => Self::V1,
      val => Self::Unknown(val),
    }
  }
}

impl From<DelegateVersion> for u8 {
  fn from(v: DelegateVersion) -> Self {
    match v {
      DelegateVersion::V1 => 1,
      DelegateVersion::Unknown(val) => val,
    }
  }
}

/// Protocol version
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
#[non_exhaustive]
pub enum ProtocolVersion {
  /// Version 1
  #[default]
  V1,
  /// Unknown version (used for forwards and backwards compatibility)
  Unknown(u8),
}

impl core::fmt::Display for ProtocolVersion {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::V1 => write!(f, "V1"),
      Self::Unknown(val) => write!(f, "Unknown({})", val),
    }
  }
}

impl From<u8> for ProtocolVersion {
  fn from(v: u8) -> Self {
    match v {
      1 => Self::V1,
      val => Self::Unknown(val),
    }
  }
}

impl From<ProtocolVersion> for u8 {
  fn from(v: ProtocolVersion) -> Self {
    match v {
      ProtocolVersion::V1 => 1,
      ProtocolVersion::Unknown(val) => val,
    }
  }
}

macro_rules! impl_data {
  ($($ty:ty),+$(,)?) => {
    $(
      impl super::Data for $ty {
        const WIRE_TYPE: super::WireType = super::WireType::Byte;

        #[inline]
        fn encoded_len(&self) -> usize {
          1
        }

        #[inline]
        fn encode(&self, buf: &mut [u8]) -> Result<usize, super::EncodeError> {
          if buf.is_empty() {
            return Err(super::EncodeError::InsufficientBuffer(
              InsufficientBuffer::with_information(1, 0),
            ));
          }

          buf[0] = u8::from(*self);
          Ok(1)
        }

        #[inline]
        fn decode(src: &[u8]) -> Result<(usize, Self), super::DecodeError>
        where
          Self: Sized,
        {
          if src.is_empty() {
            return Err(super::DecodeError::new("buffer underflow"));
          }

          Ok((1, Self::from(src[0])))
        }
      }
    )*
  };
}

impl_data!(DelegateVersion, ProtocolVersion);

#[cfg(feature = "arbitrary")]
const _: () = {
  use arbitrary::{Arbitrary, Unstructured};

  impl<'a> Arbitrary<'a> for DelegateVersion {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(u.arbitrary::<u8>()?.into())
    }
  }

  impl<'a> Arbitrary<'a> for ProtocolVersion {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
      Ok(u.arbitrary::<u8>()?.into())
    }
  }
};

#[cfg(feature = "quickcheck")]
const _: () = {
  use quickcheck::Arbitrary;

  impl Arbitrary for DelegateVersion {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
      u8::arbitrary(g).into()
    }
  }

  impl Arbitrary for ProtocolVersion {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
      u8::arbitrary(g).into()
    }
  }
};

#[cfg(test)]
mod tests {
  use super::*;

  use arbitrary::{Arbitrary, Unstructured};
  use quickcheck_macros::quickcheck;

  #[quickcheck]
  fn delegate_version_arbitrary(v: DelegateVersion) -> bool {
    let mut buf = [0; 1];
    let Ok(written) = v.encode(&mut buf) else {
      return false;
    };

    let Ok((read, decoded)) = DelegateVersion::decode(&buf) else {
      return false;
    };

    written == 1 && read == 1 && v == decoded
  }

  #[quickcheck]
  fn protocol_version_arbitrary(v: ProtocolVersion) -> bool {
    let mut buf = [0; 1];
    let Ok(written) = v.encode(&mut buf) else {
      return false;
    };

    let Ok((read, decoded)) = ProtocolVersion::decode(&buf) else {
      return false;
    };

    written == 1 && read == 1 && v == decoded
  }

  #[test]
  fn test_delegate_version() {
    let mut buf = [0; 64];
    rand::fill(&mut buf[..]);

    let mut data = Unstructured::new(&buf);
    let _ = DelegateVersion::arbitrary(&mut data).unwrap();

    assert_eq!(u8::from(DelegateVersion::V1), 1u8);
    assert_eq!(DelegateVersion::V1.to_string(), "V1");
    assert_eq!(DelegateVersion::Unknown(2).to_string(), "Unknown(2)");
    assert_eq!(DelegateVersion::from(1), DelegateVersion::V1);
    assert_eq!(DelegateVersion::from(2), DelegateVersion::Unknown(2));
  }

  #[test]
  fn test_protocol_version() {
    let mut buf = [0; 64];
    rand::fill(&mut buf[..]);

    let mut data = Unstructured::new(&buf);
    let _ = ProtocolVersion::arbitrary(&mut data).unwrap();
    assert_eq!(u8::from(ProtocolVersion::V1), 1);
    assert_eq!(ProtocolVersion::V1.to_string(), "V1");
    assert_eq!(ProtocolVersion::Unknown(2).to_string(), "Unknown(2)");
    assert_eq!(ProtocolVersion::from(1), ProtocolVersion::V1);
    assert_eq!(ProtocolVersion::from(2), ProtocolVersion::Unknown(2));
  }
}
