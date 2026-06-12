use super::*;
use smol_str::SmolStr;
use std::net::SocketAddr;

#[test]
fn void_delegate_satisfies_observation_composite() {
  fn assert_delegate<D: Delegate<Id = SmolStr, Address = SocketAddr>>(_d: &D) {}
  let v: VoidDelegate<SmolStr, SocketAddr> = VoidDelegate::default();
  assert_delegate(&v);
}
