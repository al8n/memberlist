use super::*;

#[test]
fn ping() {
  run(test_handle_ping::<TokioRuntime>());
}

#[test]
fn compound_ping() {
  run(test_handle_compound_ping::<TokioRuntime>());
}

#[test]
fn ping_wrong_node() {
  run(test_handle_ping_wrong_node::<TokioRuntime>());
}

#[test]
fn indirect_ping() {
  run(test_handle_indirect_ping::<TokioRuntime>());
}

#[test]
fn tcp_ping() {
  run(test_tcp_ping::<TokioRuntime>());
}
