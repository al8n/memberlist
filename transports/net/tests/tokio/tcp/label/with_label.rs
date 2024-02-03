use super::*;

use memberlist_net::tests::label::with_label::*;

unit_tests_with_expr!(run(
  v4_server_with_label_client_with_label({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_client_with_label::<_, TokioRuntime>(s, AddressKind::V4).await
    {
      panic!("{}", e);
    }
  }),
  v6_server_with_label_client_with_label({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_client_with_label::<_, TokioRuntime>(s, AddressKind::V6).await
    {
      panic!("{}", e);
    }
  }),
));

unit_tests_with_expr!(run(
  v4_server_with_label_client_no_label({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_with_label_client_no_label::<_, TokioRuntime>(s, AddressKind::V4, true).await
    {
      panic!("{}", e);
    }
  }),
  v6_server_with_label_client_no_label({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_with_label_client_no_label::<_, TokioRuntime>(s, AddressKind::V6, true).await
    {
      panic!("{}", e);
    }
  }),
  #[should_panic]
  v4_server_with_label_client_no_label_panic({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_with_label_client_no_label::<_, TokioRuntime>(s, AddressKind::V4, false).await
    {
      panic!("{}", e);
    }
  }),
  #[should_panic]
  v6_server_with_label_client_no_label_panic({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_with_label_client_no_label::<_, TokioRuntime>(s, AddressKind::V6, false).await
    {
      panic!("{}", e);
    }
  }),
));

unit_tests_with_expr!(run(
  v4_server_no_label_client_with_label({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_no_label_client_with_label::<_, TokioRuntime>(s, AddressKind::V4, true).await
    {
      panic!("{}", e);
    }
  }),
  v6_server_no_label_client_with_label({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_no_label_client_with_label::<_, TokioRuntime>(s, AddressKind::V6, true).await
    {
      panic!("{}", e);
    }
  }),
  #[should_panic]
  v4_server_no_label_client_with_label_panic({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_no_label_client_with_label::<_, TokioRuntime>(s, AddressKind::V4, false).await
    {
      panic!("{}", e);
    }
  }),
  #[should_panic]
  v6_server_no_label_client_with_label_panic({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      server_no_label_client_with_label::<_, TokioRuntime>(s, AddressKind::V6, false).await
    {
      panic!("{}", e);
    }
  }),
));
