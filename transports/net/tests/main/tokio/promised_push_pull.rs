use crate::promised_push_pull_test_suites;

use super::*;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
promised_push_pull_test_suites!("tcp": Tcp<TokioRuntime>::run({
  ()
}));

#[cfg(feature = "tls")]
promised_push_pull_test_suites!("tls": Tls<TokioRuntime>::run({
  memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
}));

#[cfg(feature = "native-tls")]
promised_push_pull_test_suites!("native_tls": NativeTls<TokioRuntime>::run({
  memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
}));
