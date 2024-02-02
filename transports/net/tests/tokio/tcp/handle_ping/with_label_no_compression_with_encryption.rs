use super::*;

use memberlist_net::tests::handle_ping::with_label_no_compression_no_encryption::*;

unit_tests_with_expr!(run(
  handle_v4_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v4_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s).await {
      panic!("{}", e);
    }
  }),
  handle_v6_ping_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v6_server_with_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s).await {
      panic!("{}", e);
    }
  }),
  handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v4_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, true).await {
      panic!("{}", e);
    }
  }),
  handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v6_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, true).await {
      panic!("{}", e);
    }
  }),
  handle_v4_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v4_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),
  handle_v6_ping_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v6_server_with_label_no_compression_no_encryption_client_no_label_no_compression_no_encryption::<_, TokioRuntime>(s, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),
));

unit_tests_with_expr!(run(
  handle_v4_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v4_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, true).await {
      panic!("{e}");
    }
  }),

  handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v4_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, true).await {
      panic!("{e}");
    }
  }),
  handle_v4_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v4_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),

  handle_v6_ping_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption_panic ({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = v4_server_no_label_no_compression_no_encryption_client_with_label_no_compression_no_encryption::<_, TokioRuntime>(s, false).await {
      assert!(e.to_string().contains("timeout"));
    }
  }),
));
