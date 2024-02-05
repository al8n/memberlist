use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::tests::send::*;

unit_tests_with_expr!(run(
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v4_send({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    if let Err(e) = send::<_, _, TokioRuntime>(s, c, kind).await {
      panic!("{}", e);
    }
  }),
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v6_send({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    if let Err(e) = send::<_, _, TokioRuntime>(s, c, kind).await {
      panic!("{}", e);
    }
  })
));
