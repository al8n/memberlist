use super::*;

#[test]
fn defaults_are_sane_and_overridable() {
  let c = Options::new();
  assert!(c.tcp_pool_size >= 1);
  assert!(c.udp_rx_payload_bytes > 0);
  assert_eq!(
    c.max_pending_seeds,
    memberlist_embedded::DEFAULT_MAX_PENDING_SEEDS
  );
  assert_eq!(
    c.max_pending_dials,
    memberlist_embedded::DEFAULT_MAX_PENDING_DIALS
  );
  let c = Options::new()
    .with_tcp_pool_size(8)
    .with_port(1234)
    .with_max_pending_seeds(4)
    .with_max_pending_dials(2);
  assert_eq!(c.tcp_pool_size, 8);
  assert_eq!(c.port, 1234);
  assert_eq!(c.max_pending_seeds, 4);
  assert_eq!(c.max_pending_dials, 2);
}
