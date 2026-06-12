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
fn tcp_cluster_compression_mode_configures_nodes() {
  use memberlist_proto::CompressAlgorithm;
  let a = "127.0.0.1:9301".parse().unwrap();
  let b = "127.0.0.1:9302".parse().unwrap();
  let c = TcpCluster::two_node_join_compressed(a, b, CompressAlgorithm::Lz4);
  // Every node carries the configured algorithm.
  for node in c.nodes_for_test() {
    assert_eq!(node.compression().algorithm(), Some(CompressAlgorithm::Lz4));
  }
}
