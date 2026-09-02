use super::*;

#[test]
fn defaults_are_sane_and_overridable() {
  let c = Options::new();
  assert_eq!(c.port, 7946);
  assert!(!c.close_timeout.is_zero());
  let c = Options::new()
    .with_port(1234)
    .with_close_timeout(Duration::from_secs(3));
  assert_eq!(c.port, 1234);
  assert_eq!(c.close_timeout, Duration::from_secs(3));
}

/// The gossip work ceiling defaults to the documented constant and is overridable
/// through the builder, so `GOSSIP_READ_CAP` stays the value the docs name while
/// a caller can size the ceiling to its own driver.
#[test]
fn gossip_read_cap_defaults_to_the_constant_and_is_overridable() {
  assert_eq!(Options::new().gossip_read_cap, crate::GOSSIP_READ_CAP);
  assert_eq!(Options::new().with_gossip_read_cap(8).gossip_read_cap, 8);
}
