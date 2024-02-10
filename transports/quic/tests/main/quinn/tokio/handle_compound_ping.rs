use super::*;
use crate::handle_compound_ping_test_suites;

handle_compound_ping_test_suites!("quinn": TokioRuntime::run({
  quinn_stream_layer::<TokioRuntime>().await
}));
