use super::*;
use crate::send_packet_piggyback_test_suites;

#[cfg(not(any(feature = "tls", feature = "native-tls")))]
send_packet_piggyback_test_suites!("tcp": AsyncStdRuntime::run({
  Tcp::<AsyncStdRuntime>::new()
}));

#[cfg(feature = "tls")]
send_packet_piggyback_test_suites!("tls": AsyncStdRuntime::run({
  memberlist_net::tests::tls_stream_layer::<AsyncStdRuntime>().await
}));

#[cfg(feature = "native-tls")]
send_packet_piggyback_test_suites!("native_tls": AsyncStdRuntime::run({
  memberlist_net::tests::native_tls_stream_layer::<AsyncStdRuntime>().await
}));
