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

// An IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) is matched in its canonical
// IPv4 family: it shares a verdict with its plain IPv4 spelling. A policy over
// `10.0.0.0/8` admits `::ffff:10.1.2.3` and blocks `::ffff:11.0.0.1`, exactly
// as it does the IPv4 forms — so a host reachable under both spellings can never
// be admitted under one and denied under the other.
#[test]
fn mapped_v6_checked_as_v4() {
  let policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");

  let mapped_in: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
  let mapped_out: IpAddr = "::ffff:11.0.0.1".parse().unwrap();
  assert!(
    policy.is_allowed(&mapped_in),
    "a mapped in-policy address is admitted as its IPv4 form"
  );
  assert!(
    policy.is_blocked(&mapped_out),
    "a mapped out-of-policy address is blocked as its IPv4 form"
  );

  // The invariant: both spellings of a host give the SAME verdict.
  for probe in [
    "10.1.2.3",
    "11.0.0.1",
    "10.255.255.255",
    "9.255.255.255",
    "::ffff:10.1.2.3",
    "::ffff:11.0.0.1",
    "::ffff:10.255.255.255",
    "::ffff:9.255.255.255",
  ] {
    let ip: IpAddr = probe.parse().unwrap();
    assert_eq!(
      policy.is_allowed(&ip),
      policy.is_allowed(&ip.to_canonical()),
      "is_allowed({ip}) must equal is_allowed(canonical)"
    );
  }
}

// Canonicalization is a NO-OP for a genuine (non-mapped) IPv6 address: an
// `fd00::/8` policy admits `fd00::1` and blocks `2001:db8::1`, and both keep
// their IPv6 family through the check (a genuine V6 has no IPv4 canonical form).
#[test]
fn genuine_v6_canonicalization_noop() {
  let policy = CidrPolicy::try_from(["fd00::/8"].as_slice()).expect("valid cidr");

  let inside: IpAddr = "fd00::1".parse().unwrap();
  let outside: IpAddr = "2001:db8::1".parse().unwrap();
  assert!(
    policy.is_allowed(&inside),
    "an in-policy genuine V6 is admitted"
  );
  assert!(
    policy.is_blocked(&outside),
    "an out-of-policy genuine V6 is blocked"
  );

  // A genuine V6 canonicalizes to itself, so the invariant holds trivially.
  for probe in ["fd00::1", "2001:db8::1", "fdff:ffff::ffff"] {
    let ip: IpAddr = probe.parse().unwrap();
    assert_eq!(
      ip,
      ip.to_canonical(),
      "a genuine V6 is its own canonical form"
    );
    assert_eq!(
      policy.is_allowed(&ip),
      policy.is_allowed(&ip.to_canonical())
    );
  }
}

// Regression control: for same-family (V4-net/V4-ip and V6-net/V6-ip) matching,
// canonicalization changes NOTHING — the verdict truth table is byte-identical
// to the pre-change behavior (`to_canonical` is a no-op for a non-mapped
// address). This is the proof the fix does not perturb the common case.
#[test]
fn same_family_control() {
  // IPv4 net, IPv4 probes.
  let v4 = CidrPolicy::try_from(["192.168.0.0/16", "10.0.0.0/8"].as_slice()).expect("valid cidr");
  let v4_truth = [
    ("192.168.1.1", true),
    ("192.168.255.255", true),
    ("10.9.9.9", true),
    ("10.255.255.255", true),
    ("192.167.0.1", false),
    ("11.0.0.1", false),
    ("172.16.0.1", false),
    ("203.0.113.7", false),
  ];
  for (probe, expected) in v4_truth {
    let ip: IpAddr = probe.parse().unwrap();
    assert_eq!(v4.is_allowed(&ip), expected, "v4/v4 verdict for {probe}");
  }

  // IPv6 net, IPv6 probes (genuine, non-mapped).
  let v6 = CidrPolicy::try_from(["fd00::/8", "2001:db8::/32"].as_slice()).expect("valid cidr");
  let v6_truth = [
    ("fd00::1", true),
    ("fdff:ffff::1", true),
    ("2001:db8::1", true),
    ("2001:db8:ffff::ffff", true),
    ("fc00::1", false),
    ("2001:db9::1", false),
    ("::1", false),
    ("2606:4700::1", false),
  ];
  for (probe, expected) in v6_truth {
    let ip: IpAddr = probe.parse().unwrap();
    assert_eq!(v6.is_allowed(&ip), expected, "v6/v6 verdict for {probe}");
  }
}

// A policy net written INSIDE the mapped range `::ffff:0:0/96` is dead config:
// every checked address is canonicalized before the containment test, so no
// address — neither the IPv4 spelling nor the mapped IPv6 spelling — ever
// reaches it. Operators must write nets in the canonical family (`10.0.0.0/8`,
// not its mapped form).
#[test]
fn mapped_range_v6_net_is_dead_config() {
  let policy = CidrPolicy::try_from(["::ffff:10.0.0.0/104"].as_slice()).expect("valid cidr");

  // The IPv4 spelling canonicalizes to IPv4 and never enters the V6 mapped net.
  let v4: IpAddr = "10.1.2.3".parse().unwrap();
  assert!(
    policy.is_blocked(&v4),
    "the mapped-range net matches no IPv4 address"
  );

  // The mapped IPv6 spelling ALSO canonicalizes to IPv4 before the test, so it
  // never matches the V6 net either — the net is unreachable both ways.
  let mapped: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
  assert!(
    policy.is_blocked(&mapped),
    "a mapped address is canonicalized to IPv4, so it never reaches the mapped-range V6 net"
  );
}

// `remove_by_ip` canonicalizes its probe the same way `is_allowed` does: the
// mapped IPv6 spelling of a host removes the same IPv4 net its plain spelling
// would.
#[test]
fn remove_by_ip_uses_canonical() {
  let mut policy = CidrPolicy::try_from(["10.0.0.0/8"].as_slice()).expect("valid cidr");
  assert!(policy.is_allowed(&"10.1.2.3".parse().unwrap()));

  // Remove by the MAPPED spelling; it must strip the covering IPv4 net.
  policy.remove_by_ip(&"::ffff:10.1.2.3".parse().unwrap());
  assert!(
    policy.is_blocked(&"10.1.2.3".parse().unwrap()),
    "removing by the mapped spelling strips the covering IPv4 net"
  );
  assert!(
    policy.is_block_all(),
    "the last net removed leaves a block-all policy"
  );
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
