use agnostic::tokio::TokioRuntime;
use showbiz_core::tests::test_send_msg_piggyback;

use crate::run;

#[test]
fn send_msg_piggyback() {
  run(test_send_msg_piggyback::<TokioRuntime>())
}
