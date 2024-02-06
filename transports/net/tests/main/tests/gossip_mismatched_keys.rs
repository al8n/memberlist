use super::*;
use memberlist_core::transport::tests::AddressKind;
use memberlist_net::stream_layer::tcp::Tcp;

use memberlist_net::tests::gossip_mismatch_keys::*;

unit_tests_with_expr!(run(
  #[cfg(feature = "encryption")]
  // #[should_panic]
  v4_promised_ping({
    let s = Tcp::<TokioRuntime>::new();
    let c = Tcp::<TokioRuntime>::new();
    if let Err(e) = gossip_mismatched_keys::<_, TokioRuntime>(s, c, AddressKind::V4).await {
      assert!(e
        .to_string()
        .contains("security: no installed keys could decrypt the message"))
    }
  }),
  #[cfg(feature = "encryption")]
  v6_promised_ping({
    let s = Tcp::<TokioRuntime>::new();
    let c = Tcp::<TokioRuntime>::new();
    if let Err(e) = gossip_mismatched_keys::<_, TokioRuntime>(s, c, AddressKind::V6).await {
      assert!(e
        .to_string()
        .contains("security: no installed keys could decrypt the message"))
    }
  })
));

unit_tests_with_expr!(run(
  #[cfg(feature = "encryption")] 
  v4_promised_ping_with_label({
    let s = Tcp::<TokioRuntime>::new();
    let c = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      gossip_mismatched_keys_with_label::<_, TokioRuntime>(s, c, AddressKind::V4).await
    {
      assert!(e
        .to_string()
        .contains("security: no installed keys could decrypt the message"))
    }
  }),
  #[cfg(feature = "encryption")]
  v6_promised_ping_with_label({
    let s = Tcp::<TokioRuntime>::new();
    let c = Tcp::<TokioRuntime>::new();
    if let Err(e) =
      gossip_mismatched_keys_with_label::<_, TokioRuntime>(s, c, AddressKind::V6).await
    {
      assert!(e
        .to_string()
        .contains("security: no installed keys could decrypt the message"))
    }
  })
));
