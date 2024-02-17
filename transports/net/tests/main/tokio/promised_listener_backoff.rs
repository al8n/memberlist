use super::*;
use crate::promised_listener_backoff_test_suites;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
promised_listener_backoff_test_suites! {
  "tcp": Tcp<TokioRuntime>::run({ Tcp::<TokioRuntime>::new() })
}

#[cfg(feature = "tls")]
use memberlist_net::stream_layer::tls::Tls;

#[cfg(feature = "tls")]
promised_listener_backoff_test_suites! {
  "tls": Tls<TokioRuntime>::run({
    memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
  })
}

#[cfg(feature = "native-tls")]
use memberlist_net::stream_layer::native_tls::NativeTls;

#[cfg(feature = "native-tls")]
promised_listener_backoff_test_suites! {
  "native_tls": NativeTls<TokioRuntime>::run({
    memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
  })
}
