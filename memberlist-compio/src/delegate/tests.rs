use super::*;
use smol_str::SmolStr;
use std::net::SocketAddr;

#[test]
fn void_delegate_satisfies_observation_composite() {
  fn assert_delegate<D>(_d: &D)
  where
    D: Delegate<Id = SmolStr, Address = SocketAddr>,
  {
  }
  let v: VoidDelegate<SmolStr, SocketAddr> = VoidDelegate::default();
  assert_delegate(&v);
}
