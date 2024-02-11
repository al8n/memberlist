use super::*;
use crate::handle_join_test_suites;

handle_join_test_suites!("quinn": TokioRuntime::run({
  quinn_stream_layer::<TokioRuntime>().await
}));
