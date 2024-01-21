pub(crate) const CHECKSUM_SIZE: usize = core::mem::size_of::<u32>();

/// The trait used to calculate the checksum of a packet.
pub trait Checksumer: Send + Sync + 'static {
  fn new() -> Self;

  fn update(&mut self, data: &[u8]);

  fn finalize(self) -> u32;
}

/// Checksumer based on crc32.
#[derive(Debug, Clone)]
pub struct Crc32(crc32fast::Hasher);

impl crate::checksum::Checksumer for Crc32 {
  fn new() -> Self {
    Self(crc32fast::Hasher::new())
  }

  fn update(&mut self, data: &[u8]) {
    self.0.update(data);
  }

  fn finalize(self) -> u32 {
    self.0.finalize()
  }
}
