use super::*;
use crate::send_packet_piggyback_test_suites;

#[cfg(not(any(feature = "tls", feature = "native-tls")))]
send_packet_piggyback_test_suites!("tcp": TokioRuntime::run({
  Tcp::<TokioRuntime>::new()
}));

#[cfg(feature = "tls")]
send_packet_piggyback_test_suites!("tls": TokioRuntime::run({
  memberlist_net::tests::tls_stream_layer::<TokioRuntime>().await
}));

#[cfg(feature = "native-tls")]
send_packet_piggyback_test_suites!("native_tls": TokioRuntime::run({
  memberlist_net::tests::native_tls_stream_layer::<TokioRuntime>().await
}));
