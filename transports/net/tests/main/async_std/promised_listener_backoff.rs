use super::*;
use crate::promised_listener_backoff_test_suites;

#[cfg(not(any(feature = "tls", feature = "native-tls")))]
promised_listener_backoff_test_suites! {
  "tcp": Tcp<AsyncStdRuntime>::run({ Tcp::<AsyncStdRuntime>::new() })
}

#[cfg(feature = "tls")]
use memberlist_net::stream_layer::tls::Tls;

#[cfg(feature = "tls")]
promised_listener_backoff_test_suites! {
  "tls": Tls<AsyncStdRuntime>::run({
    memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
  })
}

#[cfg(feature = "native-tls")]
use memberlist_net::stream_layer::native_tls::NativeTls;

#[cfg(feature = "native-tls")]
promised_listener_backoff_test_suites! {
  "native_tls": NativeTls<AsyncStdRuntime>::run({
    memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
  })
}
