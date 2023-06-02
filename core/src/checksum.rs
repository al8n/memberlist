pub trait Checksumer {
  fn new() -> Self;

  fn update(&mut self, data: &[u8]);

  fn finalize(self) -> u32;
}

impl Checksumer for crc32fast::Hasher {
  fn new() -> Self {
    crc32fast::Hasher::new()
  }

  fn update(&mut self, data: &[u8]) {
    self.update(data);
  }

  fn finalize(self) -> u32 {
    self.finalize()
  }
}
