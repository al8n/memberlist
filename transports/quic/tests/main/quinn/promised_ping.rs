use super::*;
use crate::promised_ping_test_suites;

promised_ping_test_suites!("quinn_tokio": TokioRuntime::tokio_run({
  quinn_stream_layer::<TokioRuntime>().await
}));

promised_ping_test_suites!("quinn_async_std": AsyncStdRuntime::async_std_run({
  quinn_stream_layer::<AsyncStdRuntime>().await
}));

promised_ping_test_suites!("quinn_smol": SmolRuntime::smol_run({
  quinn_stream_layer::<SmolRuntime>().await
}));
