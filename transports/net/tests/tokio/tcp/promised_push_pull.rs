use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::{
  stream_layer::StreamLayer,
  tests::{promised_push_pull::*, NetTransporTestPromisedClient},
};

unit_tests_with_expr!(run(
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v4_promised_push_pull({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  }),
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v6_promised_push_pull({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  })
));

unit_tests_with_expr!(run(
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v4_promised_push_pull_no_label({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_no_label::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  }),
  #[cfg(all(feature = "encryption", feature = "compression"))]
  v6_promised_push_pull_no_label({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_no_label::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  })
));

unit_tests_with_expr!(run(
  v4_promised_push_pull_label_only({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_label_only::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  }),
  v6_promised_push_pull_label_only({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_label_only::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  })
));

unit_tests_with_expr!(run(
  #[cfg(feature = "compression")]
  v4_promised_push_pull_compression_only({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_compression_only::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  }),
  #[cfg(feature = "compression")]
  v6_promised_push_pull_compression_only({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_compression_only::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  })
));

unit_tests_with_expr!(run(
  #[cfg(feature = "encryption")]
  v4_promised_push_pull_encryption_only({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_encryption_only::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  }),
  #[cfg(feature = "encryption")]
  v6_promised_push_pull_encryption_only({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) = promised_push_pull_encryption_only::<_, TokioRuntime>(s, client, kind).await {
      panic!("{}", e);
    }
  })
));

unit_tests_with_expr!(run(
  #[cfg(feature = "encryption")]
  v4_promised_push_pull_label_and_encryption({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) =
      promised_push_pull_label_and_encryption::<_, TokioRuntime>(s, client, kind).await
    {
      panic!("{}", e);
    }
  }),
  #[cfg(feature = "encryption")]
  v6_promised_push_pull_label_and_encryption({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) =
      promised_push_pull_label_and_encryption::<_, TokioRuntime>(s, client, kind).await
    {
      panic!("{}", e);
    }
  })
));

unit_tests_with_expr!(run(
  #[cfg(feature = "compression")]
  v4_promised_push_pull_label_and_compression({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) =
      promised_push_pull_label_and_compression::<_, TokioRuntime>(s, client, kind).await
    {
      panic!("{}", e);
    }
  }),
  #[cfg(feature = "compression")]
  v6_promised_push_pull_label_and_compression({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) =
      promised_push_pull_label_and_compression::<_, TokioRuntime>(s, client, kind).await
    {
      panic!("{}", e);
    }
  })
));

unit_tests_with_expr!(run(
  v4_promised_push_pull_no_label_no_compression_no_encryption({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V4;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) =
      promised_push_pull_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, client, kind)
        .await
    {
      panic!("{}", e);
    }
  }),
  v6_promised_push_pull_no_label_no_compression_no_encryption({
    let s = Tcp::<TokioRuntime>::new();
    let kind = AddressKind::V6;
    let c = Tcp::<TokioRuntime>::new();
    let client_addr = kind.next();
    let ln = c.bind(client_addr).await.unwrap();
    let client = NetTransporTestPromisedClient::new(client_addr, c, ln);
    if let Err(e) =
      promised_push_pull_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, client, kind)
        .await
    {
      panic!("{}", e);
    }
  })
));
