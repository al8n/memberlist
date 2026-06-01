use super::{BrotliAlgorithm, ZstdCompressionLevel};
use quickcheck::{Arbitrary, Gen};

impl Arbitrary for ZstdCompressionLevel {
  fn arbitrary(g: &mut Gen) -> Self {
    let level = i8::arbitrary(g);
    ZstdCompressionLevel::from(level)
  }
}

impl Arbitrary for BrotliAlgorithm {
  fn arbitrary(g: &mut Gen) -> Self {
    let val = u8::arbitrary(g);
    BrotliAlgorithm::decode(val)
  }
}
