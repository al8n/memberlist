use super::*;
use crate::handle_ping_wrong_node_test_suites;

handle_ping_wrong_node_test_suites!("s2n": TokioRuntime::run({
  s2n_stream_layer::<TokioRuntime>().await
}));