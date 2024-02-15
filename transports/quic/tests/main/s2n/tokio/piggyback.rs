use super::*;
use crate::handle_piggyback_test_suites;

handle_piggyback_test_suites!("s2n": TokioRuntime::run({
  s2n_stream_layer::<TokioRuntime>().await
}));
