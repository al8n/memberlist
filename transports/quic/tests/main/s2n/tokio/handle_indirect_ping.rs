use super::*;
use crate::handle_indirect_ping_test_suites;

handle_indirect_ping_test_suites!("s2n": S2n<TokioRuntime>::run({
  s2n_stream_layer::<TokioRuntime>().await
}));
