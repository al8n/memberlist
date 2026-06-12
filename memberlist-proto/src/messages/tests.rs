use super::memberlist::v1::{Alive, Node};
use buffa::Message as _;

#[test]
fn alive_roundtrip() {
  let original = Alive {
    incarnation: Some(42),
    meta: bytes::Bytes::new(),
    node: Some(Node {
      id: Some(bytes::Bytes::from_static(b"node-a")),
      addr: Some(bytes::Bytes::from_static(b"127.0.0.1:7000")),
      ..Default::default()
    })
    .into(),
    protocol_version: Some(1),
    delegate_version: Some(1),
    ..Default::default()
  };

  let encoded = original.encode_to_vec();
  let decoded = Alive::decode_from_slice(encoded.as_slice()).unwrap();

  assert_eq!(original.incarnation, decoded.incarnation);
  assert_eq!(original.protocol_version, decoded.protocol_version);
  assert_eq!(original.delegate_version, decoded.delegate_version);
}
