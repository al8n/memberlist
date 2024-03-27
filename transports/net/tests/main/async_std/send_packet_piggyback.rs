use super::*;
use crate::send_packet_piggyback_test_suites;

#[cfg(any(
  not(any(feature = "tls", feature = "native-tls")),
  all(feature = "tls", feature = "native-tls")
))]
send_packet_piggyback_test_suites!("tcp": Tcp<AsyncStdRuntime>::run({
  ()
}));

#[cfg(feature = "tls")]
send_packet_piggyback_test_suites!("tls": Tls<AsyncStdRuntime>::run({
  memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
}));

#[cfg(feature = "native-tls")]
send_packet_piggyback_test_suites!("native_tls": NativeTls<AsyncStdRuntime>::run({
  memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
}));
