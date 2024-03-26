use super::*;
use crate::label_test_suites;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
label_test_suites!("tcp": Tcp<TokioRuntime>::run({
  ()
}));

#[cfg(feature = "tls")]
label_test_suites!("tls": Tls<TokioRuntime>::run({
  memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
}));

#[cfg(feature = "native-tls")]
label_test_suites!("native_tls": NativeTls<TokioRuntime>::run({
  memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
}));
