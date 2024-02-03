use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::tests::handle_ping_wrong_node::ping_wrong_node;

unit_tests_with_expr!(run(
  #[should_panic]
  v4_ping_wrong_node({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = ping_wrong_node::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  #[should_panic]
  v6_ping_wrong_node({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = ping_wrong_node::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  })
));
