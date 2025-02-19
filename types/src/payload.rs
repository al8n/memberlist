use bytes::BytesMut;


/// A payload can be sent over the transport.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Payload {
  buf: BytesMut,
  header_size: usize,
}

impl core::fmt::Debug for Payload {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Payload")
      .field("header", &self.header())
      .field("data", &self.data())
      .finish()
  }
}

impl Payload {
  /// Creates a new payload with the given buffer.
  pub fn new(header_size: usize, data_size: usize) -> Self {
    let buf = BytesMut::zeroed(header_size + data_size);
    Self { buf, header_size }
  }

  /// Returns the data section of the payload.
  pub fn data(&self) -> &[u8] {
    &self.buf[self.header_size..]
  }

  /// Returns the mutable data section of the payload.
  pub fn data_mut(&mut self) -> &mut [u8] {
    &mut self.buf[self.header_size..]
  }

  /// Returns the header section of the payload.
  pub fn header(&self) -> &[u8] {
    &self.buf[..self.header_size]
  }

  /// Returns the mutable header section of the payload.
  pub fn header_mut(&mut self) -> &mut [u8] {
    &mut self.buf[..self.header_size]
  }

  /// Truncate the payload to the given length.
  /// 
  /// This will truncate the data section of the payload,
  /// the header section will not be affected.
  pub fn truncate(&mut self, len: usize) {
    self.buf.truncate(self.header_size + len);
  }

  /// Split the payload into header and data.
  /// 
  /// The first element of the tuple is the header and the second element is the data.
  pub fn split_mut(&mut self) -> (&mut [u8], &mut [u8]) {
    self.buf.split_at_mut(self.header_size)
  }

  /// Split the payload into header and data.
  ///
  /// The first element of the tuple is the header and the second element is the data.
  pub fn split(mut self) -> (BytesMut, BytesMut) {
    let data = self.buf.split_off(self.header_size);
    (self.buf, data)
  }

  /// Returns the whole payload as a slice.
  pub fn as_slice(&self) -> &[u8] {
    &self.buf
  }
}
