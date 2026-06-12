use super::*;

#[test]
fn defaults_are_sane_and_overridable() {
  let c = Options::new();
  assert!(c.tcp_pool_size >= 1);
  assert!(c.udp_rx_payload_bytes > 0);
  let c = Options::new().with_tcp_pool_size(8).with_port(1234);
  assert_eq!(c.tcp_pool_size, 8);
  assert_eq!(c.port, 1234);
}
