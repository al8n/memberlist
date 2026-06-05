//! Unit tests for the SWIM wire-domain types in [`crate::typed`].
//!
//! These exercise the pure data layer: constructors, builders (`with_*`),
//! mutators (`set_*`/`update_*`), accessors, `Display`/`Default`, the
//! `derive_more` variant helpers, and the numeric/`TryFrom` conversions.

use core::str::FromStr;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use bytes::{Bytes, BytesMut};
use smol_str::SmolStr;
use triomphe::Arc;

use crate::{
  CheapClone, Node,
  typed::{
    Ack, Alive, Dead, DelegateVersion, ErrorResponse, IndirectPing, LargeMeta, Message, Meta, Nack,
    NodeState, Ping, ProtocolVersion, PushNodeState, PushPull, State, Suspect, message_tags,
  },
};

// ─── helpers ─────────────────────────────────────────────────────────────────

fn addr(port: u16) -> SocketAddr {
  SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port))
}

fn smol_node(id: &str, port: u16) -> Node<SmolStr, SocketAddr> {
  Node::new(SmolStr::new(id), addr(port))
}

fn u64_node(id: u64, port: u16) -> Node<u64, SocketAddr> {
  Node::new(id, addr(port))
}

fn meta(bytes: &[u8]) -> Meta {
  Meta::try_from(bytes).expect("test meta within MAX_SIZE")
}

// ─── Meta ────────────────────────────────────────────────────────────────────

#[test]
fn meta_empty_and_default() {
  let e = Meta::empty();
  assert!(e.is_empty());
  assert_eq!(e.len(), 0);
  assert_eq!(e.as_bytes(), b"");
  // Default delegates to empty().
  assert_eq!(Meta::default(), e);
}

#[test]
fn meta_accessors_non_empty() {
  let m = meta(b"hello");
  assert!(!m.is_empty());
  assert_eq!(m.len(), 5);
  assert_eq!(m.as_bytes(), b"hello");
  // AsRef and Deref both reach the slice.
  assert_eq!(m.as_ref(), b"hello".as_slice());
  assert_eq!(&*m, b"hello".as_slice());
}

#[test]
fn meta_partial_eq_against_slices_and_containers() {
  let m = meta(b"abc");
  let slice: &[u8] = b"abc";
  assert_eq!(m, *slice);
  assert_eq!(m, slice);
  assert_eq!(m, Bytes::from_static(b"abc"));
  assert_eq!(m, b"abc".to_vec());
  // Inequality path.
  assert_ne!(m, *b"abd".as_slice());
}

#[test]
fn meta_ordering_and_hash_derives() {
  let a = meta(b"aaa");
  let b = meta(b"aab");
  assert!(a < b);
  assert!(b > a);
  assert_eq!(a.clone(), a);
  // CheapClone is a marker impl; cheap_clone yields an equal value.
  assert_eq!(a.cheap_clone(), a);
}

#[test]
fn meta_from_static_accept() {
  let m = Meta::from_static(b"static").expect("within cap");
  assert_eq!(m.as_bytes(), b"static");
  let s = Meta::from_static_str("statstr").expect("within cap");
  assert_eq!(s.as_bytes(), b"statstr");
}

#[test]
fn meta_try_from_accept_variants() {
  // &str
  assert_eq!(Meta::try_from("x").expect("ok").as_bytes(), b"x");
  // String
  assert_eq!(
    Meta::try_from(String::from("ys")).expect("ok").as_bytes(),
    b"ys"
  );
  // Bytes
  assert_eq!(
    Meta::try_from(Bytes::from_static(b"zz")).expect("ok").len(),
    2
  );
  // &Bytes
  let owned = Bytes::from_static(b"ref");
  assert_eq!(Meta::try_from(&owned).expect("ok").as_bytes(), b"ref");
  // Vec<u8>
  assert_eq!(Meta::try_from(vec![1u8, 2, 3]).expect("ok").len(), 3);
  // &[u8]
  let slice: &[u8] = b"sl";
  assert_eq!(Meta::try_from(slice).expect("ok").as_bytes(), b"sl");
  // BytesMut
  let mut bm = BytesMut::new();
  bm.extend_from_slice(b"bm");
  assert_eq!(Meta::try_from(bm).expect("ok").as_bytes(), b"bm");
  // FromStr
  assert_eq!(Meta::from_str("fs").expect("ok").as_bytes(), b"fs");
}

#[test]
fn meta_try_from_oversize_rejects_each_path() {
  let big = vec![0u8; Meta::MAX_SIZE + 1];
  let big_str = String::from_utf8(vec![b'a'; Meta::MAX_SIZE + 1]).expect("ascii");

  // &[u8]
  let slice: &[u8] = &big;
  assert!(Meta::try_from(slice).is_err());
  // Vec<u8>
  assert!(Meta::try_from(big.clone()).is_err());
  // Bytes
  let big_bytes = Bytes::from(big.clone());
  assert!(Meta::try_from(big_bytes.clone()).is_err());
  // &Bytes
  assert!(Meta::try_from(&big_bytes).is_err());
  // BytesMut
  let mut bm = BytesMut::new();
  bm.extend_from_slice(&big);
  assert!(Meta::try_from(bm).is_err());
  // &str / String / FromStr
  assert!(Meta::try_from(big_str.as_str()).is_err());
  assert!(Meta::try_from(big_str.clone()).is_err());
  assert!(Meta::from_str(big_str.as_str()).is_err());

  // The const constructors also reject oversize. A `&'static [u8]` of the
  // oversize length is impractical to leak; the runtime paths above cover the
  // reject branch shared by `from_static`.
  assert_eq!(Meta::MAX_SIZE, u16::MAX as usize);

  // The error carries the offending size and renders it.
  let err: LargeMeta = Meta::try_from(slice).unwrap_err();
  let rendered = err.to_string();
  assert!(rendered.contains(&(Meta::MAX_SIZE + 1).to_string()));
  assert!(rendered.contains("exceeds"));
}

#[test]
fn meta_at_exact_cap_accepts() {
  // Boundary: exactly MAX_SIZE is allowed (the check is strictly `>`).
  let exact = vec![7u8; Meta::MAX_SIZE];
  let m = Meta::try_from(exact).expect("exact cap accepted");
  assert_eq!(m.len(), Meta::MAX_SIZE);
}

// ─── DelegateVersion / ProtocolVersion ───────────────────────────────────────

#[test]
fn delegate_version_default_display_variants_roundtrip() {
  assert_eq!(DelegateVersion::default(), DelegateVersion::V1);
  assert_eq!(DelegateVersion::V1.to_string(), "v1");
  assert_eq!(DelegateVersion::Unknown(9).to_string(), "unknown(9)");

  assert!(DelegateVersion::V1.is_v_1());
  assert!(!DelegateVersion::V1.is_unknown());
  assert!(DelegateVersion::Unknown(2).is_unknown());

  // u8 round-trips both ways.
  assert_eq!(DelegateVersion::from(1u8), DelegateVersion::V1);
  assert_eq!(DelegateVersion::from(42u8), DelegateVersion::Unknown(42));
  assert_eq!(u8::from(DelegateVersion::V1), 1);
  assert_eq!(u8::from(DelegateVersion::Unknown(42)), 42);
  // Note: 0 is not V1; it round-trips through Unknown(0).
  assert_eq!(DelegateVersion::from(0u8), DelegateVersion::Unknown(0));
  assert_eq!(u8::from(DelegateVersion::Unknown(0)), 0);

  // Copy + equality.
  let v = DelegateVersion::Unknown(5);
  let copy = v;
  assert_eq!(v, copy);
}

#[test]
fn protocol_version_default_display_variants_roundtrip() {
  assert_eq!(ProtocolVersion::default(), ProtocolVersion::V1);
  assert_eq!(ProtocolVersion::V1.to_string(), "v1");
  assert_eq!(ProtocolVersion::Unknown(7).to_string(), "unknown(7)");

  assert!(ProtocolVersion::V1.is_v_1());
  assert!(ProtocolVersion::Unknown(3).is_unknown());

  assert_eq!(ProtocolVersion::from(1u8), ProtocolVersion::V1);
  assert_eq!(ProtocolVersion::from(200u8), ProtocolVersion::Unknown(200));
  assert_eq!(u8::from(ProtocolVersion::V1), 1);
  assert_eq!(u8::from(ProtocolVersion::Unknown(200)), 200);
}

// ─── State ─────────────────────────────────────────────────────────────────

#[test]
fn state_default_and_display() {
  assert_eq!(State::default(), State::Alive);
  assert_eq!(State::Alive.to_string(), "alive");
  assert_eq!(State::Suspect.to_string(), "suspect");
  assert_eq!(State::Dead.to_string(), "dead");
  assert_eq!(State::Left.to_string(), "left");
  assert_eq!(State::Unknown(13).to_string(), "unknown(13)");
}

#[test]
fn state_as_str_borrowed_and_owned() {
  assert_eq!(State::Alive.as_str(), "alive");
  assert_eq!(State::Suspect.as_str(), "suspect");
  assert_eq!(State::Dead.as_str(), "dead");
  assert_eq!(State::Left.as_str(), "left");
  assert_eq!(State::Unknown(255).as_str(), "unknown(255)");
}

#[test]
fn state_is_variant_helpers() {
  assert!(State::Alive.is_alive());
  assert!(State::Suspect.is_suspect());
  assert!(State::Dead.is_dead());
  assert!(State::Left.is_left());
  assert!(State::Unknown(1).is_unknown());
  assert!(!State::Alive.is_dead());
}

#[test]
fn state_u8_roundtrip_all_named_variants() {
  for (v, n) in [
    (State::Alive, 0u8),
    (State::Suspect, 1),
    (State::Dead, 2),
    (State::Left, 3),
  ] {
    assert_eq!(u8::from(v), n);
    assert_eq!(State::from(n), v);
  }
  // Unknown round-trips for out-of-range bytes.
  assert_eq!(State::from(99u8), State::Unknown(99));
  assert_eq!(u8::from(State::Unknown(99)), 99);
}

// ─── NodeState<I,A> ──────────────────────────────────────────────────────────

#[test]
fn node_state_new_defaults_and_accessors() {
  let ns = NodeState::new(SmolStr::new("n1"), addr(7000), State::Suspect);
  assert_eq!(ns.id_ref(), &SmolStr::new("n1"));
  assert_eq!(ns.address_ref(), &addr(7000));
  assert_eq!(ns.meta_ref(), &Meta::empty());
  assert_eq!(ns.state(), State::Suspect);
  assert_eq!(ns.protocol_version(), ProtocolVersion::V1);
  assert_eq!(ns.delegate_version(), DelegateVersion::V1);
}

#[test]
fn node_state_with_builders_roundtrip() {
  let m = meta(b"meta-bytes");
  let ns = NodeState::new(7u64, addr(1), State::Alive)
    .with_id(99)
    .with_address(addr(2))
    .with_meta(m.clone())
    .with_state(State::Dead)
    .with_protocol_version(ProtocolVersion::Unknown(4))
    .with_delegate_version(DelegateVersion::Unknown(5));

  assert_eq!(ns.id_ref(), &99u64);
  assert_eq!(ns.address_ref(), &addr(2));
  assert_eq!(ns.meta_ref(), &m);
  assert_eq!(ns.state(), State::Dead);
  assert_eq!(ns.protocol_version(), ProtocolVersion::Unknown(4));
  assert_eq!(ns.delegate_version(), DelegateVersion::Unknown(5));
}

#[test]
fn node_state_set_mutators_roundtrip() {
  let m = meta(b"zz");
  let mut ns = NodeState::new(SmolStr::new("a"), addr(10), State::Alive);
  ns.set_id(SmolStr::new("b"))
    .set_address(addr(11))
    .set_meta(m.clone())
    .set_state(State::Left)
    .set_protocol_version(ProtocolVersion::Unknown(8))
    .set_delegate_version(DelegateVersion::Unknown(9));

  assert_eq!(ns.id_ref(), &SmolStr::new("b"));
  assert_eq!(ns.address_ref(), &addr(11));
  assert_eq!(ns.meta_ref(), &m);
  assert_eq!(ns.state(), State::Left);
  assert_eq!(ns.protocol_version(), ProtocolVersion::Unknown(8));
  assert_eq!(ns.delegate_version(), DelegateVersion::Unknown(9));
}

#[test]
fn node_state_node_cheap_clone_display_eq() {
  let ns = NodeState::new(SmolStr::new("xyz"), addr(5000), State::Alive);
  let node = ns.node();
  assert_eq!(node.id_ref(), &SmolStr::new("xyz"));
  assert_eq!(node.addr_ref(), &addr(5000));

  // CheapClone yields an equal value.
  assert_eq!(ns.cheap_clone(), ns);
  // Clone + equality derive.
  assert_eq!(ns.clone(), ns);
  // Display is `id(addr)`.
  assert_eq!(ns.to_string(), format!("xyz({})", addr(5000)));
}

#[test]
fn node_state_from_alive() {
  let m = meta(b"m");
  let alive = Alive::new(3, smol_node("from-alive", 6000))
    .with_meta(m.clone())
    .with_protocol_version(ProtocolVersion::Unknown(2))
    .with_delegate_version(DelegateVersion::Unknown(3));
  let ns: NodeState<SmolStr, SocketAddr> = NodeState::from(alive);

  assert_eq!(ns.id_ref(), &SmolStr::new("from-alive"));
  assert_eq!(ns.address_ref(), &addr(6000));
  assert_eq!(ns.meta_ref(), &m);
  // Conversion from Alive forces the Alive state.
  assert_eq!(ns.state(), State::Alive);
  assert_eq!(ns.protocol_version(), ProtocolVersion::Unknown(2));
  assert_eq!(ns.delegate_version(), DelegateVersion::Unknown(3));
}

// ─── Alive<I,A> ──────────────────────────────────────────────────────────────

#[test]
fn alive_new_defaults_and_accessors() {
  let a = Alive::new(11, smol_node("alive-1", 7100));
  assert_eq!(a.incarnation(), 11);
  assert_eq!(a.meta_ref(), &Meta::empty());
  assert_eq!(a.node_ref(), &smol_node("alive-1", 7100));
  assert_eq!(a.protocol_version(), ProtocolVersion::V1);
  assert_eq!(a.delegate_version(), DelegateVersion::V1);
}

#[test]
fn alive_with_builders_roundtrip() {
  let m = meta(b"alivemeta");
  let a = Alive::new(1, smol_node("orig", 1))
    .with_incarnation(42)
    .with_meta(m.clone())
    .with_node(smol_node("new", 2))
    .with_protocol_version(ProtocolVersion::Unknown(6))
    .with_delegate_version(DelegateVersion::Unknown(7));

  assert_eq!(a.incarnation(), 42);
  assert_eq!(a.meta_ref(), &m);
  assert_eq!(a.node_ref(), &smol_node("new", 2));
  assert_eq!(a.protocol_version(), ProtocolVersion::Unknown(6));
  assert_eq!(a.delegate_version(), DelegateVersion::Unknown(7));
}

#[test]
fn alive_set_mutators_and_cheap_clone() {
  let m = meta(b"q");
  let mut a = Alive::new(0, u64_node(1, 1));
  a.set_incarnation(5)
    .set_meta(m.clone())
    .set_node(u64_node(2, 2))
    .set_protocol_version(ProtocolVersion::Unknown(1))
    .set_delegate_version(DelegateVersion::Unknown(2));

  assert_eq!(a.incarnation(), 5);
  assert_eq!(a.meta_ref(), &m);
  assert_eq!(a.node_ref(), &u64_node(2, 2));
  assert_eq!(a.protocol_version(), ProtocolVersion::Unknown(1));
  assert_eq!(a.delegate_version(), DelegateVersion::Unknown(2));

  assert_eq!(a.cheap_clone(), a);
  assert_eq!(a.clone(), a);
}

// ─── Suspect<I> / Dead<I> (macro-generated, same shape) ──────────────────────

#[test]
fn suspect_new_accessors_builders_mutators() {
  let s = Suspect::new(3, SmolStr::new("victim"), SmolStr::new("accuser"));
  assert_eq!(s.incarnation(), 3);
  assert_eq!(s.node_ref(), &SmolStr::new("victim"));
  assert_eq!(s.from_ref(), &SmolStr::new("accuser"));

  let s2 = s
    .with_incarnation(10)
    .with_node(SmolStr::new("victim2"))
    .with_from(SmolStr::new("accuser2"));
  assert_eq!(s2.incarnation(), 10);
  assert_eq!(s2.node_ref(), &SmolStr::new("victim2"));
  assert_eq!(s2.from_ref(), &SmolStr::new("accuser2"));

  let mut s3 = s2.clone();
  s3.set_incarnation(11)
    .set_node(SmolStr::new("v3"))
    .set_from(SmolStr::new("a3"));
  assert_eq!(s3.incarnation(), 11);
  assert_eq!(s3.node_ref(), &SmolStr::new("v3"));
  assert_eq!(s3.from_ref(), &SmolStr::new("a3"));

  // Copy + equality (Suspect<I> is Copy when I is). incarnation is u32.
  let s4 = Suspect::new(1u32, 2u64, 3u64);
  let copy = s4;
  assert_eq!(s4, copy);
}

#[test]
fn dead_new_accessors_builders_mutators() {
  let d = Dead::new(4, SmolStr::new("dead-node"), SmolStr::new("reporter"));
  assert_eq!(d.incarnation(), 4);
  assert_eq!(d.node_ref(), &SmolStr::new("dead-node"));
  assert_eq!(d.from_ref(), &SmolStr::new("reporter"));

  let mut d2 = d
    .with_incarnation(8)
    .with_node(SmolStr::new("n2"))
    .with_from(SmolStr::new("f2"));
  assert_eq!(d2.incarnation(), 8);
  d2.set_incarnation(9)
    .set_node(SmolStr::new("n3"))
    .set_from(SmolStr::new("f3"));
  assert_eq!(d2.incarnation(), 9);
  assert_eq!(d2.node_ref(), &SmolStr::new("n3"));
  assert_eq!(d2.from_ref(), &SmolStr::new("f3"));
  assert_eq!(d2.clone(), d2);
}

// ─── Ping<I,A> / IndirectPing<I,A> (macro-generated) ─────────────────────────

#[test]
fn ping_new_accessors_builders_mutators_cheap_clone() {
  let p = Ping::new(7, smol_node("src", 100), smol_node("tgt", 200));
  assert_eq!(p.sequence_number(), 7);
  assert_eq!(p.source_ref(), &smol_node("src", 100));
  assert_eq!(p.target_ref(), &smol_node("tgt", 200));

  let p2 = p
    .with_sequence_number(8)
    .with_source(smol_node("src2", 101))
    .with_target(smol_node("tgt2", 201));
  assert_eq!(p2.sequence_number(), 8);
  assert_eq!(p2.source_ref(), &smol_node("src2", 101));
  assert_eq!(p2.target_ref(), &smol_node("tgt2", 201));

  let mut p3 = p2.clone();
  p3.set_sequence_number(9)
    .set_source(smol_node("src3", 102))
    .set_target(smol_node("tgt3", 202));
  assert_eq!(p3.sequence_number(), 9);
  assert_eq!(p3.source_ref(), &smol_node("src3", 102));
  assert_eq!(p3.target_ref(), &smol_node("tgt3", 202));

  assert_eq!(p3.cheap_clone(), p3);
}

#[test]
fn indirect_ping_accessors_and_into_ping() {
  let ip = IndirectPing::new(5, u64_node(1, 300), u64_node(2, 400));
  assert_eq!(ip.sequence_number(), 5);
  assert_eq!(ip.source_ref(), &u64_node(1, 300));
  assert_eq!(ip.target_ref(), &u64_node(2, 400));
  assert_eq!(ip.cheap_clone(), ip);

  // IndirectPing -> Ping conversion preserves all fields.
  let p: Ping<u64, SocketAddr> = Ping::from(ip);
  assert_eq!(p.sequence_number(), 5);
  assert_eq!(p.source_ref(), &u64_node(1, 300));
  assert_eq!(p.target_ref(), &u64_node(2, 400));
}

// ─── Ack / Nack ──────────────────────────────────────────────────────────────

#[test]
fn ack_new_defaults_builders_mutators_components() {
  let a = Ack::new(123);
  assert_eq!(a.sequence_number(), 123);
  assert_eq!(a.payload(), b"");
  assert!(a.payload_bytes().is_empty());

  let payload = Bytes::from_static(b"ack-payload");
  let a2 = a.with_sequence_number(124).with_payload(payload.clone());
  assert_eq!(a2.sequence_number(), 124);
  assert_eq!(a2.payload(), b"ack-payload");
  assert_eq!(a2.payload_bytes(), payload);

  let mut a3 = a2.clone();
  a3.set_sequence_number(125)
    .set_payload(Bytes::from_static(b"x"));
  assert_eq!(a3.sequence_number(), 125);
  assert_eq!(a3.payload(), b"x");

  assert_eq!(a3.clone(), a3);

  // into_components yields the (seq, payload) pair.
  let (seq, pl) = a2.into_components();
  assert_eq!(seq, 124);
  assert_eq!(pl, payload);
}

#[test]
fn nack_new_accessors_builders_mutators() {
  let n = Nack::new(55);
  assert_eq!(n.sequence_number(), 55);

  let n2 = n.with_sequence_number(56);
  assert_eq!(n2.sequence_number(), 56);

  let mut n3 = n2;
  n3.set_sequence_number(57);
  assert_eq!(n3.sequence_number(), 57);

  // Copy + equality.
  let copy = n3;
  assert_eq!(n3, copy);
}

// ─── ErrorResponse ───────────────────────────────────────────────────────────

#[test]
fn error_response_new_accessors_builders_display_conversions() {
  let e = ErrorResponse::new("boom");
  assert_eq!(e.message(), "boom");
  assert_eq!(e.to_string(), "boom");

  let e2 = e.with_message("kaboom");
  assert_eq!(e2.message(), "kaboom");

  let mut e3 = e2.clone();
  e3.set_message(SmolStr::new("reset"));
  assert_eq!(e3.message(), "reset");
  assert_eq!(e3.clone(), e3);

  // From<SmolStr> and Into<SmolStr>.
  let from_smol: ErrorResponse = SmolStr::new("via-from").into();
  assert_eq!(from_smol.message(), "via-from");
  let back: SmolStr = from_smol.into();
  assert_eq!(back, SmolStr::new("via-from"));

  // core::error::Error is implemented (object-safe).
  let dyn_err: &dyn core::error::Error = &e3;
  assert_eq!(dyn_err.to_string(), "reset");
}

// ─── PushNodeState<I,A> ──────────────────────────────────────────────────────

#[test]
fn push_node_state_new_defaults_and_accessors() {
  let pns = PushNodeState::new(17, SmolStr::new("p1"), addr(8000), State::Dead);
  assert_eq!(pns.id_ref(), &SmolStr::new("p1"));
  assert_eq!(pns.address_ref(), &addr(8000));
  assert_eq!(pns.meta_ref(), &Meta::empty());
  assert_eq!(pns.incarnation(), 17);
  assert_eq!(pns.state(), State::Dead);
  assert_eq!(pns.protocol_version(), ProtocolVersion::V1);
  assert_eq!(pns.delegate_version(), DelegateVersion::V1);
}

#[test]
fn push_node_state_with_builders_roundtrip() {
  let m = meta(b"pmeta");
  let pns = PushNodeState::new(1, 100u64, addr(1), State::Alive)
    .with_id(200)
    .with_address(addr(2))
    .with_meta(m.clone())
    .with_incarnation(33)
    .with_state(State::Left)
    .with_protocol_version(ProtocolVersion::Unknown(11))
    .with_delegate_version(DelegateVersion::Unknown(12));

  assert_eq!(pns.id_ref(), &200u64);
  assert_eq!(pns.address_ref(), &addr(2));
  assert_eq!(pns.meta_ref(), &m);
  assert_eq!(pns.incarnation(), 33);
  assert_eq!(pns.state(), State::Left);
  assert_eq!(pns.protocol_version(), ProtocolVersion::Unknown(11));
  assert_eq!(pns.delegate_version(), DelegateVersion::Unknown(12));
}

#[test]
fn push_node_state_set_mutators_node_cheap_clone() {
  let m = meta(b"k");
  let mut pns = PushNodeState::new(0, SmolStr::new("a"), addr(10), State::Alive);
  pns
    .set_id(SmolStr::new("b"))
    .set_address(addr(11))
    .set_meta(m.clone())
    .set_incarnation(7)
    .set_state(State::Suspect)
    .set_protocol_version(ProtocolVersion::Unknown(1))
    .set_delegate_version(DelegateVersion::Unknown(2));

  assert_eq!(pns.id_ref(), &SmolStr::new("b"));
  assert_eq!(pns.address_ref(), &addr(11));
  assert_eq!(pns.meta_ref(), &m);
  assert_eq!(pns.incarnation(), 7);
  assert_eq!(pns.state(), State::Suspect);
  assert_eq!(pns.protocol_version(), ProtocolVersion::Unknown(1));
  assert_eq!(pns.delegate_version(), DelegateVersion::Unknown(2));

  let node = pns.node();
  assert_eq!(node.id_ref(), &SmolStr::new("b"));
  assert_eq!(node.addr_ref(), &addr(11));

  assert_eq!(pns.cheap_clone(), pns);
  assert_eq!(pns.clone(), pns);
}

// ─── PushPull<I,A> ───────────────────────────────────────────────────────────

fn sample_states() -> Vec<PushNodeState<SmolStr, SocketAddr>> {
  vec![
    PushNodeState::new(1, SmolStr::new("a"), addr(1), State::Alive),
    PushNodeState::new(2, SmolStr::new("b"), addr(2), State::Suspect),
  ]
}

#[test]
fn push_pull_new_accessors_and_defaults() {
  let pp = PushPull::new(true, sample_states().into_iter());
  assert!(pp.join());
  assert_eq!(pp.states_slice().len(), 2);
  assert_eq!(pp.states_slice()[0].id_ref(), &SmolStr::new("a"));
  assert_eq!(pp.user_data(), b"");
  assert!(pp.user_data_bytes().is_empty());

  // Clone + CheapClone share the same Arc-backed contents.
  assert_eq!(pp.clone(), pp);
  assert_eq!(pp.cheap_clone(), pp);
}

#[test]
fn push_pull_join_flag_helpers() {
  // with_join() forces true.
  let pp = PushPull::new(false, sample_states().into_iter()).with_join();
  assert!(pp.join());

  // maybe_join(bool) assigns the raw flag.
  let pp_false = pp.maybe_join(false);
  assert!(!pp_false.join());
  let pp_true = pp_false.maybe_join(true);
  assert!(pp_true.join());

  // set_join() / clear_join() / update_join(bool) mutators.
  let mut m = PushPull::new(false, sample_states().into_iter());
  m.set_join();
  assert!(m.join());
  m.clear_join();
  assert!(!m.join());
  m.update_join(true);
  assert!(m.join());
  m.update_join(false);
  assert!(!m.join());
}

#[test]
fn push_pull_with_states_and_user_data_builders() {
  let states: Arc<[PushNodeState<SmolStr, SocketAddr>]> = Arc::from_iter(sample_states());
  let data = Bytes::from_static(b"user-data");

  let pp = PushPull::new(false, core::iter::empty())
    .with_states(states.clone())
    .with_user_data(data.clone());
  assert_eq!(pp.states_slice().len(), 2);
  assert_eq!(pp.user_data(), b"user-data");
  assert_eq!(pp.user_data_bytes(), data);

  // set_* mutator equivalents.
  let mut pp2 = PushPull::new(true, core::iter::empty());
  let other_states: Arc<[PushNodeState<SmolStr, SocketAddr>]> =
    Arc::from_iter(sample_states().into_iter().take(1));
  pp2
    .set_states(other_states)
    .set_user_data(Bytes::from_static(b"d2"));
  assert_eq!(pp2.states_slice().len(), 1);
  assert_eq!(pp2.user_data(), b"d2");
}

#[test]
fn push_pull_into_components() {
  let data = Bytes::from_static(b"payload");
  let pp = PushPull::new(true, sample_states().into_iter()).with_user_data(data.clone());
  let (join, user_data, states) = pp.into_components();
  assert!(join);
  assert_eq!(user_data, data);
  assert_eq!(states.len(), 2);
}

// ─── Message<I,A> enum ───────────────────────────────────────────────────────

#[test]
fn message_tag_bytes_match_constants() {
  let ping: Message<SmolStr, SocketAddr> =
    Message::ping(Ping::new(1, smol_node("s", 1), smol_node("t", 2)));
  let iping: Message<SmolStr, SocketAddr> =
    Message::indirect_ping(IndirectPing::new(1, smol_node("s", 1), smol_node("t", 2)));
  let ack: Message<SmolStr, SocketAddr> = Message::ack(Ack::new(1));
  let suspect: Message<SmolStr, SocketAddr> =
    Message::suspect(Suspect::new(1, SmolStr::new("n"), SmolStr::new("f")));
  let alive: Message<SmolStr, SocketAddr> = Message::alive(Alive::new(1, smol_node("a", 1)));
  let dead: Message<SmolStr, SocketAddr> =
    Message::dead(Dead::new(1, SmolStr::new("n"), SmolStr::new("f")));
  let pushpull: Message<SmolStr, SocketAddr> =
    Message::push_pull(PushPull::new(true, sample_states().into_iter()));
  let userdata: Message<SmolStr, SocketAddr> = Message::user_data(Bytes::from_static(b"u"));
  let nack: Message<SmolStr, SocketAddr> = Message::nack(Nack::new(1));
  let err: Message<SmolStr, SocketAddr> = Message::error_response(ErrorResponse::new("e"));

  assert_eq!(ping.tag(), message_tags::PING);
  assert_eq!(iping.tag(), message_tags::INDIRECT_PING);
  assert_eq!(ack.tag(), message_tags::ACK);
  assert_eq!(suspect.tag(), message_tags::SUSPECT);
  assert_eq!(alive.tag(), message_tags::ALIVE);
  assert_eq!(dead.tag(), message_tags::DEAD);
  assert_eq!(pushpull.tag(), message_tags::PUSH_PULL);
  assert_eq!(userdata.tag(), message_tags::USER_DATA);
  assert_eq!(nack.tag(), message_tags::NACK);
  assert_eq!(err.tag(), message_tags::ERROR_RESPONSE);

  // Tag constant values are stable.
  assert_eq!(message_tags::PING, 2);
  assert_eq!(message_tags::INDIRECT_PING, 3);
  assert_eq!(message_tags::ACK, 4);
  assert_eq!(message_tags::SUSPECT, 5);
  assert_eq!(message_tags::ALIVE, 6);
  assert_eq!(message_tags::DEAD, 7);
  assert_eq!(message_tags::PUSH_PULL, 8);
  assert_eq!(message_tags::USER_DATA, 9);
  assert_eq!(message_tags::NACK, 10);
  assert_eq!(message_tags::ERROR_RESPONSE, 11);
}

#[test]
fn message_is_variant_helpers() {
  let alive: Message<SmolStr, SocketAddr> = Message::alive(Alive::new(1, smol_node("a", 1)));
  assert!(alive.is_alive());
  assert!(!alive.is_ping());

  let ping: Message<SmolStr, SocketAddr> =
    Message::ping(Ping::new(1, smol_node("s", 1), smol_node("t", 2)));
  assert!(ping.is_ping());

  let iping: Message<SmolStr, SocketAddr> =
    Message::indirect_ping(IndirectPing::new(1, smol_node("s", 1), smol_node("t", 2)));
  assert!(iping.is_indirect_ping());

  assert!(Message::<SmolStr, SocketAddr>::ack(Ack::new(1)).is_ack());
  assert!(
    Message::<SmolStr, SocketAddr>::suspect(Suspect::new(1, SmolStr::new("n"), SmolStr::new("f")))
      .is_suspect()
  );
  assert!(
    Message::<SmolStr, SocketAddr>::dead(Dead::new(1, SmolStr::new("n"), SmolStr::new("f")))
      .is_dead()
  );
  assert!(
    Message::<SmolStr, SocketAddr>::push_pull(PushPull::new(true, sample_states().into_iter()))
      .is_push_pull()
  );
  assert!(Message::<SmolStr, SocketAddr>::user_data(Bytes::from_static(b"u")).is_user_data());
  assert!(Message::<SmolStr, SocketAddr>::nack(Nack::new(1)).is_nack());
  assert!(
    Message::<SmolStr, SocketAddr>::error_response(ErrorResponse::new("e")).is_error_response()
  );
}

#[test]
fn message_from_impls_construct_correct_variant() {
  // derive_more::From builds the matching variant from the inner type.
  let m: Message<SmolStr, SocketAddr> = Ack::new(1).into();
  assert!(m.is_ack());
  let m: Message<SmolStr, SocketAddr> = Nack::new(1).into();
  assert!(m.is_nack());
  let m: Message<SmolStr, SocketAddr> = ErrorResponse::new("e").into();
  assert!(m.is_error_response());
  let m: Message<SmolStr, SocketAddr> = Alive::new(1, smol_node("a", 1)).into();
  assert!(m.is_alive());
  let m: Message<SmolStr, SocketAddr> = Ping::new(1, smol_node("s", 1), smol_node("t", 2)).into();
  assert!(m.is_ping());
}

#[test]
fn message_try_unwrap_ok_and_err() {
  // try_unwrap_* returns Ok for the matching variant...
  let ack_msg: Message<SmolStr, SocketAddr> = Message::ack(Ack::new(7));
  let ack = ack_msg.try_unwrap_ack().expect("is ack");
  assert_eq!(ack.sequence_number(), 7);

  // ...and Err (giving the original message back) for a mismatch.
  let alive_msg: Message<SmolStr, SocketAddr> = Message::alive(Alive::new(2, smol_node("a", 1)));
  let recovered = alive_msg.try_unwrap_ack();
  assert!(recovered.is_err());
  // TryUnwrapError exposes the original value in `.input`.
  let original = recovered.unwrap_err();
  assert!(original.input.is_alive());
}

#[test]
fn message_unwrap_matching_variant() {
  // unwrap_* on the matching variant returns the inner value without panicking.
  let nack_msg: Message<SmolStr, SocketAddr> = Message::nack(Nack::new(99));
  let nack = nack_msg.unwrap_nack();
  assert_eq!(nack.sequence_number(), 99);

  let ud_msg: Message<SmolStr, SocketAddr> = Message::user_data(Bytes::from_static(b"hi"));
  let ud = ud_msg.unwrap_user_data();
  assert_eq!(ud, Bytes::from_static(b"hi"));
}

#[test]
fn message_clone_eq_and_hash_derives() {
  let m1: Message<SmolStr, SocketAddr> = Message::ack(Ack::new(1));
  let m2 = m1.clone();
  assert_eq!(m1, m2);

  let other: Message<SmolStr, SocketAddr> = Message::nack(Nack::new(1));
  assert_ne!(m1, other);

  // Exercise Hash via a HashSet round-trip.
  let mut set = std::collections::HashSet::new();
  set.insert(m1.clone());
  assert!(set.contains(&m1));
  assert!(!set.contains(&other));
}
