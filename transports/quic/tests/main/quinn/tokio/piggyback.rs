use super::*;
use crate::handle_piggyback_test_suites;

handle_piggyback_test_suites!("quinn": TokioRuntime::run({
  quinn_stream_layer::<TokioRuntime>().await
}));
