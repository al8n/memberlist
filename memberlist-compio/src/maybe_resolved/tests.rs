use super::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[test]
fn resolved_constructor_and_accessors() {
  let sa: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
  let m: MaybeResolved<String, SocketAddr> = MaybeResolved::resolved(sa);
  assert!(m.is_resolved());
  assert!(!m.is_unresolved());
  match m.as_ref() {
    MaybeResolved::Resolved(r) => assert_eq!(r, &sa),
    MaybeResolved::Unresolved(_) => panic!("expected Resolved"),
  }
}

#[test]
fn unresolved_constructor_and_accessors() {
  let m: MaybeResolved<String, SocketAddr> = MaybeResolved::unresolved("host:7946".to_string());
  assert!(!m.is_resolved());
  assert!(m.is_unresolved());
  match m.as_ref() {
    MaybeResolved::Unresolved(a) => assert_eq!(a.as_str(), "host:7946"),
    MaybeResolved::Resolved(_) => panic!("expected Unresolved"),
  }
}

#[test]
fn cheap_clone_resolved() {
  let sa: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
  let m: MaybeResolved<smol_str::SmolStr, SocketAddr> = MaybeResolved::resolved(sa);
  use memberlist_proto::CheapClone;
  let m2 = m.cheap_clone();
  assert!(m2.is_resolved());
}

#[test]
fn cheap_clone_unresolved() {
  let m: MaybeResolved<smol_str::SmolStr, SocketAddr> =
    MaybeResolved::unresolved(smol_str::SmolStr::new("host:7946"));
  use memberlist_proto::CheapClone;
  let m2 = m.cheap_clone();
  assert!(m2.is_unresolved());
}
