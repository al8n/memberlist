use crate::handle_ping_wrong_node_test_suites;

use super::*;

handle_ping_wrong_node_test_suites!("tcp": SmolRuntime::run({
  Tcp::<SmolRuntime>::new()
}));

#[cfg(feature = "tls")]
handle_ping_wrong_node_test_suites!("tls": SmolRuntime::run({
  memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
}));

#[cfg(feature = "native-tls")]
handle_ping_wrong_node_test_suites!("native_tls": SmolRuntime::run({
  memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
}));
