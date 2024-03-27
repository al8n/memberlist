use super::*;
use crate::handle_join_test_suites;

#[cfg(feature = "tokio")]
handle_join_test_suites!("quinn_tokio": Quinn<TokioRuntime>::tokio_run({
  quinn_stream_layer::<TokioRuntime>().await
}));

#[cfg(feature = "async-std")]
handle_join_test_suites!("quinn_async_std": Quinn<AsyncStdRuntime>::async_std_run({
  quinn_stream_layer::<AsyncStdRuntime>().await
}));

#[cfg(feature = "smol")]
handle_join_test_suites!("quinn_smol": Quinn<SmolRuntime>::smol_run({
  quinn_stream_layer::<SmolRuntime>().await
}));
