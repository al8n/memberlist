use super::*;
use crate::promised_push_pull_test_suites;

promised_push_pull_test_suites!("s2n": S2n<TokioRuntime>::run({
  s2n_stream_layer::<TokioRuntime>().await
}));
