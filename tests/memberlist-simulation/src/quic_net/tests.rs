use super::*;

#[test]
fn frame_roundtrips_and_is_memberlist_classified() {
  let m: Message<SmolStr, SocketAddr> = Message::Alive(Alive::new(
    7,
    Node::new(SmolStr::new("x"), "127.0.0.1:9000".parse().unwrap()),
  ));
  let b = encode_frame(&m).expect("alive encodes");
  assert!(
    matches!(b.first(), Some(t) if (1..=15).contains(t)),
    "encoded memberlist frame must be demux-classified Memberlist (first byte 1..=15)"
  );
  let (n, back) = decode_frame(&b).expect("decodes");
  assert_eq!(n, b.len());
  assert!(matches!(back, Message::Alive(_)));
}
