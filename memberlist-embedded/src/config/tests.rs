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
