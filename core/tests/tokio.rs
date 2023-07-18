use agnostic::tokio::TokioRuntime;
use showbiz_core::{security::EncryptionAlgo, tests::*, CompressionAlgo};

fn run(fut: impl std::future::Future<Output = ()>) {
  initialize_tests_tracing();
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  runtime.block_on(fut);
}

/// Create related tests
#[path = "tokio/create.rs"]
mod create;

/// Join related tests
#[path = "tokio/join.rs"]
mod join;
