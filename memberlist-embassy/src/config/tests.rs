use super::*;

#[test]
fn defaults_are_sane_and_overridable() {
  let c = Options::new();
  assert_eq!(c.port, 7946);
  assert!(c.tcp_socket_rx_bytes > 0);
  assert!(c.tcp_socket_tx_bytes > 0);
  assert!(!c.close_timeout.is_zero());
  assert!(
    c.socket_timeout > c.close_timeout,
    "socket timeout must exceed the close timeout"
  );
  assert_eq!(c.gossip_read_cap, memberlist_embedded::GOSSIP_READ_CAP);
  assert_eq!(
    c.max_pending_seeds,
    memberlist_embedded::DEFAULT_MAX_PENDING_SEEDS
  );
  assert_eq!(
    c.max_pending_dials,
    memberlist_embedded::DEFAULT_MAX_PENDING_DIALS
  );

  let c = Options::new()
    .with_port(1234)
    .with_tcp_socket_rx_bytes(8192)
    .with_tcp_socket_tx_bytes(2048)
    .with_close_timeout(Duration::from_secs(3))
    .with_socket_timeout(Duration::from_secs(20))
    .with_gossip_read_cap(12)
    .with_max_pending_seeds(4)
    .with_max_pending_dials(2);
  assert_eq!(c.port, 1234);
  assert_eq!(c.tcp_socket_rx_bytes, 8192);
  assert_eq!(c.tcp_socket_tx_bytes, 2048);
  assert_eq!(c.close_timeout, Duration::from_secs(3));
  assert_eq!(c.socket_timeout, Duration::from_secs(20));
  assert_eq!(c.gossip_read_cap, 12);
  assert_eq!(c.max_pending_seeds, 4);
  assert_eq!(c.max_pending_dials, 2);
}
