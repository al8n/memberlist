use super::*;

#[test]
fn classify_is_collision_free_over_the_whole_byte_space() {
  assert_eq!(classify(&[]), Class::Reject);
  for b in 0u16..=255 {
    let b = b as u8;
    let got = classify(&[b, 0xAB]);
    let want = if b & 0xC0 != 0 {
      Class::Quic
    } else if (1..=15).contains(&b) {
      Class::Memberlist
    } else {
      Class::Reject
    };
    assert_eq!(got, want, "byte {b:#04x}");
  }
  // Spot-check the semantic boundaries.
  assert_eq!(classify(&[1]), Class::Memberlist); // Compound
  assert_eq!(classify(&[15]), Class::Memberlist); // Checksumed
  assert_eq!(classify(&[0x10]), Class::Reject); // gap floor
  assert_eq!(classify(&[0x3F]), Class::Reject); // gap ceiling
  assert_eq!(classify(&[0x40]), Class::Quic); // short-header FIXED_BIT
  assert_eq!(classify(&[0x80]), Class::Quic); // long-header form bit
  assert_eq!(classify(&[0xC3]), Class::Quic);
}
