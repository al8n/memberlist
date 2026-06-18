use bytes::Bytes;

/// Ack response is sent for a ping
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ack {
  /// The sequence number of the ack
  sequence_number: u32,
  /// The payload of the ack
  payload: Bytes,
}

impl Ack {
  /// Create a new ack response with the given sequence number and empty payload.
  #[inline(always)]
  pub const fn new(sequence_number: u32) -> Self {
    Self {
      sequence_number,
      payload: Bytes::new(),
    }
  }

  /// Returns the sequence number of the ack
  #[inline(always)]
  pub const fn sequence_number(&self) -> u32 {
    self.sequence_number
  }

  /// Returns the payload of the ack as a byte slice.
  #[inline(always)]
  pub fn payload(&self) -> &[u8] {
    self.payload.as_ref()
  }

  /// Cheap-clones the payload of the ack as a `Bytes` handle.
  #[inline(always)]
  pub fn payload_bytes(&self) -> Bytes {
    self.payload.clone()
  }

  /// Sets the sequence number of the ack
  #[inline(always)]
  pub const fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the sequence number of the ack (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_sequence_number(mut self, sequence_number: u32) -> Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the payload of the ack
  #[inline(always)]
  pub fn set_payload(&mut self, payload: Bytes) -> &mut Self {
    self.payload = payload;
    self
  }

  /// Sets the payload of the ack (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub fn with_payload(mut self, payload: Bytes) -> Self {
    self.payload = payload;
    self
  }

  /// Consumes the [`Ack`] and returns the sequence number and payload
  #[inline(always)]
  pub fn into_components(self) -> (u32, Bytes) {
    (self.sequence_number, self.payload)
  }
}

/// Nack response is sent for an indirect ping when the pinger doesn't hear from
/// the ping-ee within the configured timeout. This lets the original node know
/// that the indirect ping attempt happened but didn't succeed.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Nack {
  sequence_number: u32,
}

impl Nack {
  /// Create a new nack response with the given sequence number.
  #[inline(always)]
  pub const fn new(sequence_number: u32) -> Self {
    Self { sequence_number }
  }

  /// Returns the sequence number of the nack
  #[inline(always)]
  pub const fn sequence_number(&self) -> u32 {
    self.sequence_number
  }

  /// Sets the sequence number of the nack response
  #[inline(always)]
  pub const fn set_sequence_number(&mut self, sequence_number: u32) -> &mut Self {
    self.sequence_number = sequence_number;
    self
  }

  /// Sets the sequence number of the nack response (Builder pattern)
  #[must_use]
  #[inline(always)]
  pub const fn with_sequence_number(mut self, sequence_number: u32) -> Self {
    self.sequence_number = sequence_number;
    self
  }
}
