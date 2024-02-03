use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::tests::packet_piggyback::packet_piggyback;

unit_tests_with_expr!(run(
  v4_packet_piggyback({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = packet_piggyback::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  v6_packet_piggyback({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = packet_piggyback::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  })
));
