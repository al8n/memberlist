use super::*;
use bytes::Bytes;

#[derive(
  Debug,
  Default,
  Copy,
  Clone,
  PartialEq,
  Eq,
  Hash,
  Archive,
  Serialize,
  Deserialize,
  serde::Serialize,
  serde::Deserialize,
)]
#[repr(u8)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug), repr(u8), non_exhaustive)]
#[non_exhaustive]
pub enum CompressionAlgo {
  #[default]
  Lzw = 0,
  None = 1,
}

impl CompressionAlgo {
  pub fn is_none(&self) -> bool {
    matches!(self, Self::None)
  }
}

impl ArchivedCompressionAlgo {
  pub fn is_none(&self) -> bool {
    match self {
      ArchivedCompressionAlgo::Lzw => false,
      ArchivedCompressionAlgo::None => true,
    }
  }
}

impl From<ArchivedCompressionAlgo> for CompressionAlgo {
  fn from(value: ArchivedCompressionAlgo) -> Self {
    match value {
      ArchivedCompressionAlgo::Lzw => Self::Lzw,
      ArchivedCompressionAlgo::None => Self::None,
    }
  }
}

#[viewit::viewit]
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[archive(compare(PartialEq), check_bytes)]
#[archive_attr(derive(Debug))]
pub(crate) struct Compress {
  algo: CompressionAlgo,
  buf: Bytes,
}

impl super::Type for Compress {
  const PREALLOCATE: usize = super::DEFAULT_ENCODE_PREALLOCATE_SIZE;

  fn encode<C: Checksumer>(&self, pv: ProtocolVersion, dv: DelegateVersion) -> Message {
    super::encode::<C, _, { Self::PREALLOCATE }>(MessageType::Compress, pv, dv, self)
  }
}

