use super::*;

#[test]
fn frame_roundtrips() {
  let m: Message<SmolStr, SocketAddr> = Message::Alive(Alive::new(
    7,
    Node::new(SmolStr::new("x"), "127.0.0.1:9000".parse().unwrap()),
  ));
  let b = encode_frame(&m).expect("alive encodes");
  let (n, back) = decode_frame(&b).expect("decodes");
  assert_eq!(n, b.len());
  assert!(matches!(back, Message::Alive(_)));
}

#[test]
fn tls_configs_construct() {
  // Both bundles build a usable rustls connection pair.
  let _trusted = sim_tls_config();
  let _mtls = sim_tls_config_mtls_required();
}
