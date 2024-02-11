use super::*;
use crate::handle_send_test_suites;

handle_send_test_suites!("quinn": TokioRuntime::run({
  quinn_stream_layer::<TokioRuntime>().await
}));
