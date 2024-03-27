use super::*;
use crate::promised_ping_test_suites;

#[cfg(feature = "tokio")]
promised_ping_test_suites!("quinn_tokio": Quinn<TokioRuntime>::tokio_run({
  quinn_stream_layer::<TokioRuntime>().await
}));

#[cfg(feature = "async-std")]
promised_ping_test_suites!("quinn_async_std": Quinn<AsyncStdRuntime>::async_std_run({
  quinn_stream_layer::<AsyncStdRuntime>().await
}));

#[cfg(feature = "smol")]
promised_ping_test_suites!("quinn_smol": Quinn<SmolRuntime>::smol_run({
  quinn_stream_layer::<SmolRuntime>().await
}));
