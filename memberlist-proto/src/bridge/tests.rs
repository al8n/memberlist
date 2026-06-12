use core::net::SocketAddr;

use bytes::Bytes;
use smol_str::SmolStr;

use super::*;
use crate::typed::{self, DelegateVersion, Node, ProtocolVersion, State};

type I = SmolStr;
type A = SocketAddr;

fn sample_node() -> Node<I, A> {
  Node::new(
    SmolStr::new("node-a"),
    "127.0.0.1:7946".parse::<SocketAddr>().unwrap(),
  )
}

// ── Alive ──────────────────────────────────────────────────────────────────

#[test]
fn alive_roundtrip() {
  let alive = typed::Alive::new(42, sample_node())
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1);

  let pb = alive_to_buffa::<I, A>(&alive).unwrap();
  assert_eq!(pb.incarnation, Some(42));
  assert!(pb.node.is_set());

  let back = alive_from_buffa::<I, A>(&pb).unwrap();
  assert_eq!(back.incarnation(), alive.incarnation());
  assert_eq!(back.node_ref().id_ref(), alive.node_ref().id_ref());
  assert_eq!(back.node_ref().addr_ref(), alive.node_ref().addr_ref());
  assert_eq!(back.protocol_version(), alive.protocol_version());
  assert_eq!(back.delegate_version(), alive.delegate_version());
}

#[test]
fn meta_from_wire_detaches_from_the_inbound_datagram() {
  // Model the zero-copy decode: a small `meta` aliasing a large inbound
  // datagram (a remote can pad the datagram with protobuf unknown fields).
  let datagram = Bytes::from(vec![7u8; 4096]);
  let aliased_meta = datagram.slice(16..32); // 16-byte meta sharing the 4 KiB buffer

  let detached = meta_from_wire(&aliased_meta).unwrap();

  // Value preserved, but the typed meta is a fresh small allocation outside the
  // datagram, so retaining it in membership cannot pin the whole buffer.
  assert_eq!(detached.as_bytes(), &datagram[16..32]);
  let meta_addr = detached.as_bytes().as_ptr() as usize;
  let dg_start = datagram.as_ptr() as usize;
  let dg_end = dg_start + datagram.len();
  assert!(
    meta_addr < dg_start || meta_addr >= dg_end,
    "decoded meta must be copied, not aliasing the inbound datagram",
  );
}

#[test]
fn ack_payload_is_detached_from_the_inbound_datagram() {
  use crate::messages::memberlist::v1 as pb;
  // A small ack payload aliasing a large inbound datagram (a remote can pad the
  // datagram with unknown fields). handle_ack retains the payload past decode.
  let datagram = Bytes::from(vec![9u8; 4096]);
  let aliased_payload = datagram.slice(8..24);
  let b = pb::Ack {
    sequence_number: 7,
    payload: aliased_payload,
    ..Default::default()
  };

  let ack = ack_from_buffa(&b);

  assert_eq!(ack.payload(), &datagram[8..24]); // value preserved
  let payload_addr = ack.payload().as_ptr() as usize;
  let dg_start = datagram.as_ptr() as usize;
  let dg_end = dg_start + datagram.len();
  assert!(
    payload_addr < dg_start || payload_addr >= dg_end,
    "ack payload must be copied, not aliasing the inbound datagram",
  );
}

#[test]
fn app_field_copies_small_payloads_and_aliases_large_ones() {
  // UserData / PushPull user_data go through `app_field`.
  let datagram = Bytes::from(vec![3u8; 8192]);
  let dg_start = datagram.as_ptr() as usize;
  let dg_end = dg_start + datagram.len();

  // Small (< ZEROCOPY_MIN_LEN): copied, so unknown-field padding cannot pin the
  // whole frame behind a tiny app payload queued in a bounded event.
  let small = datagram.slice(0..16);
  let small_out = app_field(&small);
  assert_eq!(small_out.as_ref(), &datagram[0..16]);
  let p = small_out.as_ptr() as usize;
  assert!(
    p < dg_start || p >= dg_end,
    "a small app payload must be copied, not aliasing the datagram",
  );

  // Large (>= ZEROCOPY_MIN_LEN): kept zero-copy — still aliases the frame.
  let large = datagram.slice(0..ZEROCOPY_MIN_LEN);
  let large_out = app_field(&large);
  let q = large_out.as_ptr() as usize;
  assert!(
    q >= dg_start && q < dg_end,
    "a large app payload stays zero-copy (aliases the datagram)",
  );
}

// ── Suspect ────────────────────────────────────────────────────────────────

#[test]
fn suspect_roundtrip() {
  let suspect = typed::Suspect::new(7, SmolStr::new("node-b"), SmolStr::new("node-a"));

  let pb = suspect_to_buffa::<I>(&suspect).unwrap();
  assert_eq!(pb.incarnation, Some(7));

  let back = suspect_from_buffa::<I>(&pb).unwrap();
  assert_eq!(back.incarnation(), suspect.incarnation());
  assert_eq!(back.node_ref(), suspect.node_ref());
  assert_eq!(back.from_ref(), suspect.from_ref());
}

// ── Dead ───────────────────────────────────────────────────────────────────

#[test]
fn dead_roundtrip() {
  let dead = typed::Dead::new(3, SmolStr::new("node-c"), SmolStr::new("node-a"));

  let pb = dead_to_buffa::<I>(&dead).unwrap();
  let back = dead_from_buffa::<I>(&pb).unwrap();
  assert_eq!(back.incarnation(), dead.incarnation());
  assert_eq!(back.node_ref(), dead.node_ref());
  assert_eq!(back.from_ref(), dead.from_ref());
}

// ── Ping ───────────────────────────────────────────────────────────────────

#[test]
fn ping_roundtrip() {
  let src = sample_node();
  let tgt = Node::new(
    SmolStr::new("node-b"),
    "127.0.0.2:7946".parse::<SocketAddr>().unwrap(),
  );
  let ping = typed::Ping::new(99, src, tgt);

  let pb = ping_to_buffa::<I, A>(&ping).unwrap();
  assert_eq!(pb.sequence_number, Some(99));

  let back = ping_from_buffa::<I, A>(&pb).unwrap();
  assert_eq!(back.sequence_number(), ping.sequence_number());
  assert_eq!(back.source_ref().id_ref(), ping.source_ref().id_ref());
  assert_eq!(back.target_ref().id_ref(), ping.target_ref().id_ref());
}

// ── IndirectPing ───────────────────────────────────────────────────────────

#[test]
fn indirect_ping_roundtrip() {
  let src = sample_node();
  let tgt = Node::new(
    SmolStr::new("node-b"),
    "127.0.0.2:7946".parse::<SocketAddr>().unwrap(),
  );
  let iping = typed::IndirectPing::new(55, src, tgt);

  let pb = indirect_ping_to_buffa::<I, A>(&iping).unwrap();
  let back = indirect_ping_from_buffa::<I, A>(&pb).unwrap();
  assert_eq!(back.sequence_number(), iping.sequence_number());
  assert_eq!(back.source_ref().id_ref(), iping.source_ref().id_ref());
  assert_eq!(back.target_ref().id_ref(), iping.target_ref().id_ref());
}

// ── Ack / Nack ─────────────────────────────────────────────────────────────

#[test]
fn ack_roundtrip() {
  let ack = typed::Ack::new(11).with_payload(Bytes::from_static(b"hello"));

  let pb = ack_to_buffa(&ack);
  assert_eq!(pb.sequence_number, 11);

  let back = ack_from_buffa(&pb);
  assert_eq!(back.sequence_number(), ack.sequence_number());
  assert_eq!(back.payload(), ack.payload());
}

#[test]
fn nack_roundtrip() {
  let nack = typed::Nack::new(22);
  let pb = nack_to_buffa(&nack);
  let back = nack_from_buffa(&pb);
  assert_eq!(back.sequence_number(), nack.sequence_number());
}

// ── PushPull ───────────────────────────────────────────────────────────────

#[test]
fn push_pull_roundtrip() {
  let pns1 = typed::PushNodeState::new(
    1,
    SmolStr::new("node-a"),
    "127.0.0.1:7946".parse::<SocketAddr>().unwrap(),
    State::Alive,
  );
  let pns2 = typed::PushNodeState::new(
    2,
    SmolStr::new("node-b"),
    "127.0.0.2:7946".parse::<SocketAddr>().unwrap(),
    State::Suspect,
  );
  let pp = typed::PushPull::new(true, [pns1, pns2].into_iter())
    .with_user_data(Bytes::from_static(b"userdata"));

  let pb = push_pull_to_buffa::<I, A>(&pp).unwrap();
  assert!(pb.join);
  assert_eq!(pb.states.len(), 2);
  assert_eq!(&pb.user_data[..], b"userdata");

  let back = push_pull_from_buffa::<I, A>(&pb).unwrap();
  assert_eq!(back.join(), pp.join());
  assert_eq!(back.states_slice().len(), pp.states_slice().len());
  assert_eq!(back.user_data(), pp.user_data());
  assert_eq!(
    back.states_slice()[0].id_ref(),
    pp.states_slice()[0].id_ref()
  );
  assert_eq!(back.states_slice()[1].state(), pp.states_slice()[1].state());
}

// ── UserData ───────────────────────────────────────────────────────────────

#[test]
fn user_data_roundtrip() {
  let data = Bytes::from_static(b"app-gossip");
  let pb = user_data_to_buffa(&data);
  let back = user_data_from_buffa(&pb);
  assert_eq!(back, data);
}

// ── ErrorResponse ──────────────────────────────────────────────────────────

#[test]
fn error_response_roundtrip() {
  let er = typed::ErrorResponse::new("something went wrong");
  let pb = error_response_to_buffa(&er);
  let back = error_response_from_buffa(&pb);
  assert_eq!(back.message(), er.message());
}

// ── message_to_any / message_from_any: Alive ───────────────────────────────

#[test]
fn message_roundtrip_alive() {
  let alive = typed::Alive::new(100, sample_node());
  let msg: typed::Message<I, A> = typed::Message::Alive(alive.clone());

  let any = message_to_any(&msg).unwrap();
  assert!(matches!(any, AnyMessage::Alive(_)));

  let back: typed::Message<I, A> = message_from_any(&any).unwrap();
  match back {
    typed::Message::Alive(a) => {
      assert_eq!(a.incarnation(), alive.incarnation());
      assert_eq!(a.node_ref().id_ref(), alive.node_ref().id_ref());
    }
    _ => panic!("expected Alive variant"),
  }
}

// ── message_to_any / message_from_any: PushPull ────────────────────────────

#[test]
fn message_roundtrip_push_pull() {
  let pns = typed::PushNodeState::new(
    5,
    SmolStr::new("node-x"),
    "10.0.0.1:7946".parse::<SocketAddr>().unwrap(),
    State::Dead,
  );
  let pp = typed::PushPull::new(false, [pns].into_iter()).with_user_data(Bytes::from_static(b"ud"));
  let msg: typed::Message<I, A> = typed::Message::PushPull(pp.clone());

  let any = message_to_any(&msg).unwrap();
  assert!(matches!(any, AnyMessage::PushPull(_)));

  let back: typed::Message<I, A> = message_from_any(&any).unwrap();
  match back {
    typed::Message::PushPull(p) => {
      assert_eq!(p.join(), pp.join());
      assert_eq!(p.states_slice().len(), pp.states_slice().len());
      assert_eq!(p.user_data(), pp.user_data());
    }
    _ => panic!("expected PushPull variant"),
  }
}

// ── alive_from_buffa: missing node → MissingField error ──────────────────

#[test]
fn alive_missing_node_returns_error() {
  use crate::messages::memberlist::v1 as pb;
  let pb_alive = pb::Alive {
    incarnation: Some(1),
    node: buffa::MessageField::none(),
    ..Default::default()
  };
  let err = alive_from_buffa::<I, A>(&pb_alive).unwrap_err();
  assert!(
    matches!(err, BridgeError::MissingField("Alive.node")),
    "got: {err:?}"
  );
}

// ── State mapping ─────────────────────────────────────────────────────────

#[test]
fn state_roundtrip_all_variants_including_unknown() {
  // Unknown(n) is a forward-compat byte and MUST round-trip, not
  // collapse to Alive.
  let variants = [
    State::Alive,
    State::Suspect,
    State::Dead,
    State::Left,
    State::Unknown(4),
    State::Unknown(99),
    State::Unknown(255),
  ];
  for &s in &variants {
    let pb_state = state_to_buffa(s);
    let back = state_from_buffa(&pb_state).unwrap();
    assert_eq!(back, s, "roundtrip failed for {s:?}");
  }
}

#[test]
fn state_from_buffa_rejects_out_of_byte_range_discriminant() {
  // A wire State is a single byte; a raw EnumValue outside 0..=255 is
  // malformed and must be rejected, not truncated into a valid state.
  for raw in [256i32, 300, -1, i32::MAX] {
    let ev = buffa::EnumValue::<crate::messages::memberlist::v1::State>::from(raw);
    assert!(
      matches!(state_from_buffa(&ev), Err(BridgeError::Decode(_))),
      "raw discriminant {raw} must be rejected"
    );
  }
}

#[test]
fn version_fields_round_trip_unknown_and_reject_overflow() {
  // Protocol/delegate versions are single wire bytes. Unknown(n) must
  // round-trip; an inbound uint32 > 255 must be REJECTED, not truncated
  // (`257 as u8 == 1` would forge V1).
  let ns = typed::NodeState::new(
    sample_node().id_ref().clone(),
    *sample_node().addr_ref(),
    State::Alive,
  )
  .with_protocol_version(ProtocolVersion::Unknown(7))
  .with_delegate_version(DelegateVersion::Unknown(200));
  let pb = node_state_to_buffa::<I, A>(&ns).unwrap();
  let back = node_state_from_buffa::<I, A>(&pb).unwrap();
  assert_eq!(back.protocol_version(), ProtocolVersion::Unknown(7));
  assert_eq!(back.delegate_version(), DelegateVersion::Unknown(200));

  let mut bad = pb.clone();
  bad.protocol_version = Some(257);
  assert!(
    matches!(
      node_state_from_buffa::<I, A>(&bad),
      Err(BridgeError::Decode(_))
    ),
    "protocol_version 257 must be rejected, not truncated to 1"
  );
}

#[test]
fn p2_omitted_version_defaults_v1_distinct_from_explicit_zero() {
  use crate::messages::memberlist::v1 as pb;

  let good_node = || pb::Node {
    id: Some(Bytes::from_static(b"n")),
    addr: Some(data_to_bytes(&"127.0.0.1:1".parse::<SocketAddr>().unwrap()).unwrap()),
    ..Default::default()
  };

  // Omitted version fields ⇒ default V1 (frozen `unwrap_or_default`),
  // NOT Unknown(0).
  let omitted = pb::Alive {
    incarnation: Some(1),
    node: buffa::MessageField::some(good_node()),
    protocol_version: None,
    delegate_version: None,
    ..Default::default()
  };
  let a = alive_from_buffa::<I, A>(&omitted).unwrap();
  assert_eq!(a.protocol_version(), ProtocolVersion::V1);
  assert_eq!(a.delegate_version(), DelegateVersion::V1);

  // Explicit 0 ⇒ Unknown(0) — must remain distinct from omitted.
  let explicit_zero = pb::Alive {
    incarnation: Some(1),
    node: buffa::MessageField::some(good_node()),
    protocol_version: Some(0),
    delegate_version: Some(0),
    ..Default::default()
  };
  let z = alive_from_buffa::<I, A>(&explicit_zero).unwrap();
  assert_eq!(z.protocol_version(), ProtocolVersion::Unknown(0));
  assert_eq!(z.delegate_version(), DelegateVersion::Unknown(0));

  // Explicit 1 ⇒ V1; explicit >255 ⇒ rejected.
  let one = pb::Alive {
    incarnation: Some(1),
    node: buffa::MessageField::some(good_node()),
    protocol_version: Some(1),
    delegate_version: Some(1),
    ..Default::default()
  };
  let o = alive_from_buffa::<I, A>(&one).unwrap();
  assert_eq!(o.protocol_version(), ProtocolVersion::V1);

  let over = pb::Alive {
    incarnation: Some(1),
    node: buffa::MessageField::some(good_node()),
    protocol_version: Some(300),
    ..Default::default()
  };
  assert!(matches!(
    alive_from_buffa::<I, A>(&over),
    Err(BridgeError::Decode(_))
  ));
}

#[test]
fn data_from_bytes_rejects_trailing_garbage() {
  // A fixed-width field (SocketAddr) with trailing bytes must be
  // rejected at the bridge in release builds, not silently
  // canonicalized.
  let addr: A = "127.0.0.1:7946".parse().unwrap();
  let mut raw = data_to_bytes(&addr).unwrap().to_vec();
  raw.push(0xff); // trailing garbage
  let buf = Bytes::from(raw);
  assert!(
    matches!(data_from_bytes::<A>(&buf), Err(BridgeError::Decode(_))),
    "trailing bytes after a valid SocketAddr must be rejected"
  );
  // The clean encoding still decodes.
  let clean = data_to_bytes(&addr).unwrap();
  assert_eq!(data_from_bytes::<A>(&clean).unwrap(), addr);
}

#[test]
fn scoped_ipv6_address_is_rejected_not_silently_canonicalized() {
  // The compact wire layout cannot carry flowinfo/scope_id. A scoped
  // link-local address (scope_id matters for dialing) must be REJECTED
  // on encode rather than silently flattened to scope 0 (which would
  // gossip an undialable peer). Tested through the real node address
  // path.
  use core::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

  let scoped = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1),
    7946,
    0x1234, // flowinfo
    7,      // scope_id (interface zone)
  ));
  let node: Node<I, A> = Node::new(SmolStr::new("ll"), scoped);
  assert!(
    node_to_buffa::<I, A>(&node).is_err(),
    "a scoped IPv6 node address must be rejected, not silently zeroed"
  );

  // An unscoped IPv6 address still round-trips cleanly.
  let unscoped = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
    7946,
    0,
    0,
  ));
  let n2: Node<I, A> = Node::new(SmolStr::new("g"), unscoped);
  let pb = node_to_buffa::<I, A>(&n2).expect("unscoped IPv6 must encode");
  let back = node_from_buffa::<I, A>(&pb).expect("unscoped IPv6 must decode");
  assert_eq!(*back.addr_ref(), unscoped);
}

// ── Omitted legacy-required fields must be rejected ──────────────────────

#[test]
fn p1_missing_required_fields_are_rejected() {
  use crate::messages::memberlist::v1 as pb;

  // A valid Node to satisfy message-typed presence where needed.
  let good_node = || pb::Node {
    id: Some(Bytes::from_static(b"n")),
    addr: Some(data_to_bytes(&"127.0.0.1:1".parse::<SocketAddr>().unwrap()).unwrap()),
    ..Default::default()
  };

  // Alive: incarnation omitted (proto3 would default it to 0).
  let a = pb::Alive {
    incarnation: None,
    node: buffa::MessageField::some(good_node()),
    ..Default::default()
  };
  assert!(
    matches!(
      alive_from_buffa::<I, A>(&a),
      Err(BridgeError::MissingField("Alive.incarnation"))
    ),
    "omitted Alive.incarnation must be rejected"
  );

  // Suspect: node bytes omitted.
  let s = pb::Suspect {
    incarnation: Some(1),
    node: None,
    from: Some(Bytes::from_static(b"x")),
    ..Default::default()
  };
  assert!(matches!(
    suspect_from_buffa::<I>(&s),
    Err(BridgeError::MissingField("Suspect.node"))
  ));

  // Ping: sequence_number omitted.
  let p = pb::Ping {
    sequence_number: None,
    source: buffa::MessageField::some(good_node()),
    target: buffa::MessageField::some(good_node()),
    ..Default::default()
  };
  assert!(matches!(
    ping_from_buffa::<I, A>(&p),
    Err(BridgeError::MissingField("Ping.sequence_number"))
  ));

  // Node: id omitted.
  let n = pb::Node {
    id: None,
    addr: Some(Bytes::from_static(b"a")),
    ..Default::default()
  };
  assert!(matches!(
    node_from_buffa::<I, A>(&n),
    Err(BridgeError::MissingField("Node.id"))
  ));

  // PushNodeState: state omitted.
  let pns = pb::PushNodeState {
    id: Some(Bytes::from_static(b"n")),
    addr: Some(data_to_bytes(&"127.0.0.1:1".parse::<SocketAddr>().unwrap()).unwrap()),
    incarnation: Some(1),
    state: None,
    ..Default::default()
  };
  assert!(matches!(
    push_node_state_from_buffa::<I, A>(&pns),
    Err(BridgeError::MissingField("PushNodeState.state"))
  ));

  // Sanity: a fully-populated Alive still round-trips (present values
  // are wire-identical under proto3 `optional`).
  let ok = typed::Alive::new(5, sample_node())
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1);
  let pb_ok = alive_to_buffa::<I, A>(&ok).unwrap();
  let back = alive_from_buffa::<I, A>(&pb_ok).unwrap();
  assert_eq!(back.incarnation(), 5);
}
