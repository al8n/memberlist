use super::*;

use memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::*;

unit_tests_with_expr!(run(
  handle_v4_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V4).await {
      panic!("{}", e);
    }
  }),
  handle_v6_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V6).await {
      panic!("{}", e);
    }
  }),
  handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V4, true).await {
      panic!("{}", e);
    }
  }),
  handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V6, true).await {
      panic!("{}", e);
    }
  }),
  handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V4, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),
  handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V6, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),
));

// All of below are fail tests, because if the server is not has label, and the client has label
// then
unit_tests_with_expr!(run(
  handle_v4_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V4, true).await {
      panic!("{e}");
    }
  }),
  handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V6, true).await {
      panic!("{e}");
    }
  }),
  handle_v4_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V4, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),
  handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, AddressKind::V6, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),
));
