use super::*;

use memberlist_net::tests::handle_ping::no_label_with_compression_with_encryption::*;

unit_tests_with_expr!(run(
  handle_v4_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  handle_v6_ping_server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_with_compression_with_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  }),
  handle_v4_ping_server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  handle_v6_ping_server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_no_compression_no_encryption_client_no_label_with_compression_with_encryption::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  }),
  handle_v4_ping_server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  handle_v6_ping_server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_with_compression_with_encryption_client_no_label_with_compression_with_encryption::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  }),
));
