use super::*;

#[test]
fn ping() {
  run(test_handle_ping::<AsyncStdRuntime>());
}

#[test]
fn compound_ping() {
  run(test_handle_compound_ping::<AsyncStdRuntime>());
}

#[test]
fn ping_wrong_node() {
  run(test_handle_ping_wrong_node::<AsyncStdRuntime>());
}

#[test]
fn indirect_ping() {
  run(test_handle_indirect_ping::<AsyncStdRuntime>());
}

#[test]
fn tcp_ping() {
  run(test_tcp_ping::<AsyncStdRuntime>());
}
