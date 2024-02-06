use crate::send_test_suites;

use super::*;

send_test_suites!("tcp": SmolRuntime::run({
  Tcp::<SmolRuntime>::new()
}));

#[cfg(feature = "tls")]
send_test_suites!("tls": SmolRuntime::run({
  memberlist_net::tests::tls_stream_layer::<SmolRuntime>().await
}));

#[cfg(feature = "native-tls")]
send_test_suites!("native_tls": SmolRuntime::run({
  memberlist_net::tests::native_tls_stream_layer::<SmolRuntime>().await
}));