use super::*;
use crate::handle_join_dead_node_test_suites;

handle_join_dead_node_test_suites!("s2n": TokioRuntime::run({
  s2n_stream_layer::<TokioRuntime>().await
}));