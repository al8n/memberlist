use super::*;
use crate::handle_indirect_ping_test_suites;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
handle_indirect_ping_test_suites!("tcp": Tcp<SmolRuntime>::run({
  ()
}));

#[cfg(feature = "tls")]
handle_indirect_ping_test_suites!("tls": Tls<SmolRuntime>::run({
  memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
}));

#[cfg(feature = "native-tls")]
handle_indirect_ping_test_suites!("native_tls": NativeTls<SmolRuntime>::run({
  memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
}));
