use super::*;

use memberlist_net::tests::label::with_label_and_encryption::*;

unit_tests_with_expr!(run(
  v4_server_with_label_with_encryption_client_with_label_with_encryption({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_with_encryption_client_with_label_with_encryption::<
      _,
      TokioRuntime,
    >(s, AddressKind::V4)
    .await
    {
      panic!("{}", e);
    }
  }),
  v6_server_with_label_with_encryption_client_with_label_with_encryption({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_with_encryption_client_with_label_with_encryption::<
      _,
      TokioRuntime,
    >(s, AddressKind::V6)
    .await
    {
      panic!("{}", e);
    }
  }),
));

unit_tests_with_expr!(run(
  v4_server_with_label_with_encryption_client_with_label_no_encryption({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_with_encryption_client_with_label_no_encryption::<
      _,
      TokioRuntime,
    >(s, AddressKind::V4)
    .await
    {
      panic!("{}", e);
    }
  }),
  v6_server_with_label_client_no_label_and_encryption({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_with_encryption_client_with_label_no_encryption::<
      _,
      TokioRuntime,
    >(s, AddressKind::V6)
    .await
    {
      panic!("{}", e);
    }
  }),
));

unit_tests_with_expr!(run(
  v4_server_with_label_no_encryption_client_with_label_with_encryption({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_encryption_client_with_label_with_encryption::<
      _,
      TokioRuntime,
    >(s, AddressKind::V4)
    .await
    {
      panic!("{}", e);
    }
  }),
  v6_server_with_label_no_encryption_client_with_label_with_encryption({
    let s = Tcp::<TokioRuntime>::new();
    if let Err(e) = server_with_label_no_encryption_client_with_label_with_encryption::<
      _,
      TokioRuntime,
    >(s, AddressKind::V6)
    .await
    {
      panic!("{}", e);
    }
  }),
));
