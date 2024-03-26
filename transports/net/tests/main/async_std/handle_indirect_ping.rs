use super::*;
use crate::handle_indirect_ping_test_suites;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
handle_indirect_ping_test_suites!("tcp": Tcp<AsyncStdRuntime>::run({
  ()
}));

#[cfg(feature = "tls")]
handle_indirect_ping_test_suites!("tls": Tls<AsyncStdRuntime>::run({
  memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
}));

#[cfg(feature = "native-tls")]
handle_indirect_ping_test_suites!("native_tls": NativeTls<AsyncStdRuntime>::run({
  memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
}));
