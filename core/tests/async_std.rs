// use agnostic::{async_std::AsyncStdRuntime, Runtime};
// use showbiz_core::{security::EncryptionAlgo, tests::*, CompressionAlgo};

// fn run(fut: impl std::future::Future<Output = ()>) {
//   initialize_tests_tracing();
//   AsyncStdRuntime::block_on(fut);
// }

// /// Create related tests
// #[path = "async_std/create.rs"]
// mod create;

// /// Join related tests
// #[path = "async_std/join.rs"]
// mod join;

// #[path = "async_std/delegate.rs"]
// mod delegate;

// #[path = "async_std/leave.rs"]
// mod leave;

// #[path = "async_std/probe.rs"]
// mod probe;

// #[path = "async_std/ping.rs"]
// mod ping;

// #[path = "async_std/net.rs"]
// mod net;
