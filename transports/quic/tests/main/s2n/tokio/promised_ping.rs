use super::*;
use crate::promised_ping_test_suites;

promised_ping_test_suites!("s2n": TokioRuntime::run({
  s2n_stream_layer::<TokioRuntime>().await
}));
