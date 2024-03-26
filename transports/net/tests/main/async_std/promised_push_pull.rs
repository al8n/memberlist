use crate::promised_push_pull_test_suites;

use super::*;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
promised_push_pull_test_suites!("tcp": Tcp<AsyncStdRuntime>::run({
  ()
}));

#[cfg(feature = "tls")]
promised_push_pull_test_suites!("tls": Tls<AsyncStdRuntime>::run({
  memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
}));

#[cfg(feature = "native-tls")]
promised_push_pull_test_suites!("native_tls": NativeTls<AsyncStdRuntime>::run({
  memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
}));
