use agnostic::tokio::TokioRuntime;
use showbiz_core::tests::test_probe;

use crate::run;

#[test]
fn probe() {
  run(test_probe::<TokioRuntime>());
}
