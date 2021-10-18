use super::*;

#[test]
fn send_msg_piggyback() {
  run(test_send_msg_piggyback::<AsyncStdRuntime>())
}
