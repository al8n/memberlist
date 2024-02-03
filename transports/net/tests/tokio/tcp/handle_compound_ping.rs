use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::tests::handle_compound_ping::compound_ping;

unit_tests_with_expr!(run(
  v4_compound_ping({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = compound_ping::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  v6_compound_ping({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = compound_ping::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  })
));
