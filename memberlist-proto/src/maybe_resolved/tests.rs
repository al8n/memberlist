use super::*;
use core::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::string::String;

#[test]
fn resolved_constructor_and_accessors() {
  let sa = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7946);
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
  let m: MaybeResolved<String, SocketAddr> = MaybeResolved::unresolved(String::from("host:7946"));
  assert!(m.is_unresolved());
  assert!(!m.is_resolved());
  match m.as_ref() {
    MaybeResolved::Unresolved(a) => assert_eq!(a.as_str(), "host:7946"),
    MaybeResolved::Resolved(_) => panic!("expected Unresolved"),
  }
}

#[test]
fn default_resolved_type_is_socket_addr() {
  // `MaybeResolved<A>` defaults the resolved type to SocketAddr.
  let sa = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
  let m: MaybeResolved<String> = MaybeResolved::Resolved(sa);
  assert!(m.is_resolved());
}

#[test]
fn cheap_clone_round_trips() {
  use crate::CheapClone;
  let sa = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7946);
  let r: MaybeResolved<SocketAddr, SocketAddr> = MaybeResolved::resolved(sa);
  assert!(r.cheap_clone().is_resolved());
  let u: MaybeResolved<SocketAddr> = MaybeResolved::unresolved(sa);
  assert!(u.cheap_clone().is_unresolved());
}

#[test]
fn eq_hash_debug_derives() {
  use std::collections::HashSet;
  let a = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
  let resolved: MaybeResolved<SocketAddr> = MaybeResolved::Resolved(a);
  let unresolved: MaybeResolved<SocketAddr> = MaybeResolved::Unresolved(a);
  // Resolved and Unresolved at the same address are distinct.
  assert_ne!(resolved, unresolved);
  assert_eq!(resolved.clone(), MaybeResolved::Resolved(a));
  // Hash + Debug are derived.
  let mut set: HashSet<MaybeResolved<SocketAddr>> = HashSet::new();
  set.insert(MaybeResolved::Resolved(a));
  assert!(set.contains(&MaybeResolved::Resolved(a)));
  assert!(!format!("{resolved:?}").is_empty());
}
