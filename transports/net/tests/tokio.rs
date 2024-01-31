use agnostic::tokio::TokioRuntime;
use memberlist_core::{tests::{run as run_unit_test, next_socket_addr_v4, next_socket_addr_v6, get_memberlist}, unit_tests, unit_tests_with_expr};

fn run(fut: impl std::future::Future<Output = ()>) {
  let runtime = ::tokio::runtime::Runtime::new().unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[path = "tokio/tcp.rs"]
mod tcp;

// #[path = "tokio/tls.rs"]
// #[cfg(feature = "tls")]
// mod tls;

// #[path = "tokio/native_tls.rs"]
// #[cfg(feature = "native-tls")]
// mod native_tls;