use super::*;
use crate::handle_join_test_suites;

handle_join_test_suites!("quinn_tokio": TokioRuntime::tokio_run({
  quinn_stream_layer::<TokioRuntime>().await
}));

handle_join_test_suites!("quinn_async_std": AsyncStdRuntime::async_std_run({
  quinn_stream_layer::<AsyncStdRuntime>().await
}));

handle_join_test_suites!("quinn_smol": SmolRuntime::smol_run({
  quinn_stream_layer::<SmolRuntime>().await
}));