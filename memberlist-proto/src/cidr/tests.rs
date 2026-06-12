use super::*;

#[test]
fn tri_state_default_is_allow_all() {
  let policy = CidrPolicy::default();
  assert!(policy.is_allow_all());
  assert!(!policy.is_block_all());
  assert!(policy.is_allowed(&"203.0.113.7".parse().unwrap()));
}

#[test]
fn block_all_rejects_everything() {
  let policy = CidrPolicy::block_all();
  assert!(policy.is_block_all());
  assert!(!policy.is_allow_all());
  assert!(policy.is_blocked(&"127.0.0.1".parse().unwrap()));
}

#[test]
fn removing_every_entry_leaves_block_all() {
  let mut policy = CidrPolicy::default();
  let net0: IpNet = "127.0.1.1/16".parse().unwrap();
  let net1: IpNet = "127.0.1.1/24".parse().unwrap();
  let net2: IpNet = "128.0.0.2/16".parse().unwrap();
  policy.add(net0);
  policy.add(net1);
  policy.add(net2);

  assert!(policy.is_allowed(&net0.addr()));
  policy.remove(&net0);
  assert!(!policy.is_allowed_net(&net0));
  // 127.0.1.1 is still covered by net1 (/24).
  assert!(policy.is_allowed(&"127.0.1.1".parse().unwrap()));

  policy.remove_by_ip(&net1.addr());
  assert!(!policy.is_allowed(&"127.0.1.1".parse().unwrap()));

  // Removing the last allowed net leaves a block-all policy, NOT allow-all:
  // an explicitly emptied allow-list stays fail-closed.
  policy.remove_by_ip(&"128.0.0.2".parse().unwrap());
  assert!(policy.is_block_all());
  assert!(policy.is_blocked(&"128.0.0.2".parse().unwrap()));
}

#[test]
fn empty_collection_inputs_are_block_all_not_allow_all() {
  // FromIterator
  let from_iter: CidrPolicy = core::iter::empty::<IpNet>().collect();
  assert!(from_iter.is_block_all());
  // TryFrom<&[&str]>
  let empty: &[&str] = &[];
  assert!(
    CidrPolicy::try_from(empty)
      .expect("an empty slice parses")
      .is_block_all()
  );
  // From<std HashSet> (std builds only)
  assert!(CidrPolicy::from(std::collections::HashSet::new()).is_block_all());
}

#[test]
fn try_from_cidr_strings_round_trips() {
  let policy = CidrPolicy::try_from(["10.0.0.0/8", " 192.168.0.0/16 "].as_slice())
    .expect("valid CIDR strings parse");
  assert!(policy.is_allowed(&"10.1.2.3".parse().unwrap()));
  assert!(policy.is_allowed(&"192.168.5.5".parse().unwrap()));
  assert!(policy.is_blocked(&"172.16.0.1".parse().unwrap()));
  assert!(CidrPolicy::try_from(["not-a-cidr"].as_slice()).is_err());
}

#[test]
fn notify_alive_decides_on_the_self_advertised_address() {
  use crate::typed::State;

  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");

  // Admission reads the self-advertised address carried in the Alive, NOT the
  // transport source: a node presenting an in-policy address from ANY origin
  // is admitted. This is membership admission, not a spoof-proof origin
  // boundary (origin filtering belongs at the transport/driver layer).
  let advertises_allowed: NodeState<&str, SocketAddr> =
    NodeState::new("any-origin", "10.9.9.9:7000".parse().unwrap(), State::Alive);
  assert!(
    policy.notify_alive(&advertises_allowed),
    "an in-policy advertised address is admitted"
  );

  let advertises_blocked: NodeState<&str, SocketAddr> = NodeState::new(
    "outsider",
    "192.168.1.1:7000".parse().unwrap(),
    State::Alive,
  );
  assert!(
    !policy.notify_alive(&advertises_blocked),
    "an out-of-policy advertised address is ignored"
  );
}
