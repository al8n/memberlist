use super::*;
use crate::handle_compound_ping_test_suites;

handle_compound_ping_test_suites!("s2n": TokioRuntime::run({
  s2n_stream_layer::<TokioRuntime>().await
}));