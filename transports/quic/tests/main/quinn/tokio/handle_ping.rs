use super::*;
use crate::handle_ping_test_suites;

handle_ping_test_suites!("quinn": TokioRuntime::run({
  quinn_stream_layer::<TokioRuntime>().await
}));