//! Inbound datagram first-byte classification.
//!
//! Collision-free by construction: every memberlist datagram's first byte is
//! a frame tag in `1..=15` (Compound=1 … ErrorResponse=11, Labeled=12,
//! Encrypted=13, Compressed=14, Checksumed=15), so bits 0x40/0x80 are clear;
//! every QUIC packet's first byte sets LONG_HEADER_FORM (0x80) or the
//! short-header FIXED_BIT (0x40), so it is >= 0x40.

/// Which protocol owns an inbound datagram. Field-less by nature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
  /// First byte >= 0x40: hand the datagram to `quinn_proto::Endpoint::handle`.
  Quic,
  /// First byte in 1..=15: hand the datagram to the memberlist codec.
  Memberlist,
  /// Empty datagram, or first byte in 0x10..=0x3F: belongs to neither.
  Reject,
}

/// Classify a datagram by its first byte.
pub(crate) const fn classify(datagram: &[u8]) -> Class {
  match datagram.first() {
    None => Class::Reject,
    Some(b) if *b & 0xC0 != 0 => Class::Quic,
    Some(b) if *b >= 1 && *b <= 15 => Class::Memberlist,
    Some(_) => Class::Reject,
  }
}

#[cfg(test)]
mod tests;
