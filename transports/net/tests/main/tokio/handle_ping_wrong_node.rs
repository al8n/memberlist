use crate::handle_ping_wrong_node_test_suites;

use super::*;

handle_ping_wrong_node_test_suites!("tcp": TokioRuntime::run({
  Tcp::<TokioRuntime>::new()
}));

#[cfg(feature = "tls")]
handle_ping_wrong_node_test_suites!("tls": TokioRuntime::run({
  memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
}));

#[cfg(feature = "native-tls")]
handle_ping_wrong_node_test_suites!("native_tls": TokioRuntime::run({
  memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
}));