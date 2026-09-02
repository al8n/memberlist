use super::*;
use crate::{HardwareAddress, InterfaceOptions, IpCidr};
use core::net::{IpAddr, Ipv4Addr, SocketAddr};
use smol_str::SmolStr;

fn addr(p: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), p)
}

fn ip_iface() -> InterfaceOptions {
  InterfaceOptions::new(HardwareAddress::Ip).with_ip_addr(IpCidr::new(
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)).into(),
    24,
  ))
}

#[test]
fn gossip_seed_domain_separates_and_diverges_per_node() {
  let iface_seed = 0x1234_5678_9abc_def0u64;
  let a: SocketAddr = "10.0.0.1:7946".parse().unwrap();
  let b: SocketAddr = "10.0.0.2:7946".parse().unwrap();

  // Domain separation: the gossip seed never equals the interface seed it is
  // derived from, so an observer who infers the interface seed from the TCP
  // stack does not thereby learn the gossip schedule.
  assert_ne!(gossip_seed_from(iface_seed, &a), iface_seed);

  // Per-node divergence: two nodes handed the SAME interface seed get distinct
  // gossip seeds because their advertise addresses differ.
  assert_ne!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &b)
  );

  // A one-port difference at the same IP also diverges.
  let a2: SocketAddr = "10.0.0.1:7947".parse().unwrap();
  assert_ne!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &a2)
  );

  // IPv6 nodes on the same port likewise diverge, and an IPv6 address does not
  // alias the IPv4 node that shares its port.
  let v6a: SocketAddr = "[fe80::1]:7946".parse().unwrap();
  let v6b: SocketAddr = "[fe80::2]:7946".parse().unwrap();
  assert_ne!(
    gossip_seed_from(iface_seed, &v6a),
    gossip_seed_from(iface_seed, &v6b)
  );
  assert_ne!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &v6a)
  );

  // Deterministic: identical inputs reproduce the same gossip seed.
  assert_eq!(
    gossip_seed_from(iface_seed, &a),
    gossip_seed_from(iface_seed, &a)
  );
}

#[test]
fn new_node_is_sole_member() {
  let cfg = crate::Options::new();
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    cfg,
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &crate::SocketAddrResolver,
    &mut dev,
    now,
  );
  assert_eq!(m.num_members(), 1);
}

#[test]
fn poll_emits_initial_gossip_and_a_deadline() {
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));
  let mut m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    crate::Options::new(),
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &crate::SocketAddrResolver,
    &mut dev,
    now,
  );
  m.start(now);
  let next = m.poll(now, &mut dev);
  assert!(next.is_some(), "scheduler must arm a deadline");
}

#[test]
fn endpoint_is_routable_matches_smoltcp_unicast() {
  use core::net::Ipv6Addr;

  // Unicast IPv4 with a non-zero port is the only routable case here.
  assert!(endpoint_is_routable(&addr(7946)));
  // Unicast IPv6 is routable too.
  assert!(endpoint_is_routable(&SocketAddr::new(
    IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
    7946
  )));

  // Unspecified (0.0.0.0 / ::) is not unicast.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    7946
  )));
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    7946
  )));
  // Multicast (224.0.0.1) is not unicast.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
    7946
  )));
  // Limited broadcast (255.255.255.255) is not unicast.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)),
    7946
  )));
  // Port 0 is rejected even with a unicast IP: no socket can address it.
  assert!(!endpoint_is_routable(&SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
    0
  )));
}

// Resolvers exercising the `join` resolution boundary. `Address = SocketAddr`
// matches a node built with the [`SocketAddrResolver`](crate::SocketAddrResolver).

use core::{convert::Infallible, time::Duration};
use memberlist_embedded::ResolvedAddrs;
use smoltcp::phy::{Loopback, Medium};

/// Must never be invoked — the lifecycle guard rejects a left node's `join`
/// before any seed is resolved.
struct UnreachableResolver;

impl crate::Resolver for UnreachableResolver {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    unreachable!("a left node must not resolve seeds");
  }
}

/// Resolves every address to no candidates.
struct EmptyResolver;

impl crate::Resolver for EmptyResolver {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    Ok(ResolvedAddrs::new())
  }
}

/// Resolves every address to a FULL bounded result (the per-seed cap's worth of
/// the same wire address). The `ResolvedAddrs` type bounds the count, so even a
/// resolver that tries to emit "as many as possible" stays capped.
struct FullResolver;

impl crate::Resolver for FullResolver {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    let mut addrs = ResolvedAddrs::new();
    // Fill to capacity; `push` past `MAX_RESOLVED_ADDRS_PER_SEED` returns the
    // item, so the loop simply stops at the type's bound.
    while addrs.push(*address).is_ok() {}
    Ok(addrs)
  }
}

/// Panics if `resolve` is ever called — used to prove a construction error fires
/// BEFORE any address resolution.
struct PanicOnResolve;

impl crate::Resolver for PanicOnResolve {
  type Address = SocketAddr;
  type Error = Infallible;
  fn resolve(&self, _address: &SocketAddr) -> Result<ResolvedAddrs, Self::Error> {
    panic!("the config preflight must reject the node before any address is resolved");
  }
}

fn started_node(
  dev: &mut Loopback,
  now: memberlist_proto::Instant,
) -> Memberlist<SmolStr, SocketAddr, Loopback> {
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));
  let mut m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    crate::Options::new(),
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &crate::SocketAddrResolver,
    dev,
    now,
  );
  m.start(now);
  m
}

#[test]
fn join_after_leave_rejects_without_resolving() {
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let mut m = started_node(&mut dev, now);
  m.leave(now).expect("leave a running node");

  // A left node rejects join immediately — and the resolver is never called
  // (`UnreachableResolver` would panic otherwise).
  let err = m
    .join(
      &UnreachableResolver,
      &[crate::MaybeResolved::Unresolved(addr(7947))],
    )
    .expect_err("a left node rejects join");
  assert!(
    err.is_control(),
    "expected Control(NotRunning), got {err:?}"
  );
}

#[test]
fn join_with_all_seeds_unresolvable_is_no_addresses() {
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let mut m = started_node(&mut dev, now);

  // A non-empty seed set that resolves to nothing is a discovery failure.
  let err = m
    .join(
      &EmptyResolver,
      &[crate::MaybeResolved::Unresolved(addr(7947))],
    )
    .expect_err("all-empty resolution fails");
  assert!(err.is_no_addresses(), "expected NoAddresses, got {err:?}");
}

#[test]
fn join_accepts_a_full_bounded_resolution() {
  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let mut m = started_node(&mut dev, now);

  // A resolver that fills the bounded result to capacity still joins. The
  // `ResolvedAddrs` type caps the count at `MAX_RESOLVED_ADDRS_PER_SEED`, so the
  // driver never needs a post-hoc `.take`; a resolver simply cannot hand back an
  // oversized result for the driver to allocate.
  m.join(
    &FullResolver,
    &[crate::MaybeResolved::Unresolved(addr(7947))],
  )
  .expect("a full bounded resolution joins");
}

#[test]
fn invalid_config_is_rejected_before_resolution() {
  use Duration;

  let mut dev = Loopback::new(Medium::Ip);
  let now = memberlist_proto::Instant::from_origin(Duration::from_secs(1));
  let ep_cfg = memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946));

  // A zero close timeout is an advertise-independent misconfiguration. The
  // construction preflight must reject it BEFORE the advertise address is
  // resolved, so `PanicOnResolve` is never called. (`Memberlist` is not `Debug`,
  // so match the result rather than `expect_err`.)
  let cfg = crate::Options::new().with_close_timeout(Duration::ZERO);
  let res = Memberlist::<SmolStr, SocketAddr, _>::try_new(
    cfg,
    ip_iface(),
    TransformOptions::default(),
    ep_cfg,
    &PanicOnResolve,
    &mut dev,
    now,
  );
  match res {
    Err(InitError::ZeroCloseTimeout) => {}
    Err(other) => panic!("expected ZeroCloseTimeout from the preflight, got {other:?}"),
    Ok(_) => panic!("a zero close timeout must be rejected at construction"),
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ML-LIFE-01 integration: `poll` re-arms a listener the socket inactivity timeout
// reaped. A raw peer drives a real unauthenticated half-open (a SYN with the final
// ACK withheld) against the driver's single internal listener over a paired
// `Medium::Ip` link — something the self-looping `Loopback` device the other
// full-driver tests use cannot source. After the timeout reaps the half-open to
// `Closed`, `poll` must re-`listen` the slot in place so the node keeps accepting
// inbound reliable streams; without that re-arm the listener stays `Closed` and the
// DoS is permanent rather than bounded to one timeout window.

use smoltcp::{
  phy::{ChecksumCapabilities, DeviceCapabilities, RxToken, TxToken},
  time::Instant as SmolInstant,
};
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

/// A shared in-memory IP-frame FIFO (one direction of a paired link).
#[derive(Clone)]
struct IpWire(Rc<RefCell<VecDeque<Vec<u8>>>>);

/// One end of a paired `Medium::Ip` link: reads from `rx`, writes to `tx`.
struct PairedIp {
  rx: IpWire,
  tx: IpWire,
}

/// Cross-wire two ends so each side's TX is the other's RX.
fn ip_link() -> (PairedIp, PairedIp) {
  let a2b = IpWire(Rc::new(RefCell::new(VecDeque::new())));
  let b2a = IpWire(Rc::new(RefCell::new(VecDeque::new())));
  (
    PairedIp {
      rx: b2a.clone(),
      tx: a2b.clone(),
    },
    PairedIp { rx: a2b, tx: b2a },
  )
}

struct IpRx(Vec<u8>);
struct IpTx(IpWire);

impl RxToken for IpRx {
  fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
    f(&self.0)
  }
}

impl TxToken for IpTx {
  fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
    let mut buf = vec![0u8; len];
    let r = f(&mut buf);
    (self.0).0.borrow_mut().push_back(buf);
    r
  }
}

impl Device for PairedIp {
  type RxToken<'a> = IpRx;
  type TxToken<'a> = IpTx;

  fn receive(&mut self, _t: SmolInstant) -> Option<(IpRx, IpTx)> {
    let frame = self.rx.0.borrow_mut().pop_front()?;
    Some((IpRx(frame), IpTx(self.tx.clone())))
  }

  fn transmit(&mut self, _t: SmolInstant) -> Option<IpTx> {
    Some(IpTx(self.tx.clone()))
  }

  fn capabilities(&self) -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.medium = Medium::Ip;
    caps.max_transmission_unit = 1500;
    caps.checksum = ChecksumCapabilities::ignored();
    caps
  }
}

/// A raw smoltcp peer at `10.0.0.2/24` on the other end of the link.
fn peer_iface(dev: &mut PairedIp) -> Interface {
  let mut cfg = IfConfig::new(HardwareAddress::Ip);
  cfg.random_seed = 0x50a2_1166;
  let mut iface = Interface::new(cfg, dev, SmolInstant::from_millis(0));
  iface.update_ip_addrs(|addrs| {
    addrs
      .push(IpCidr::new(crate::IpAddress::v4(10, 0, 0, 2), 24))
      .expect("push peer ip");
  });
  iface
}

#[test]
fn poll_re_arms_a_listener_reaped_by_the_inactivity_timeout() {
  // A short close_timeout keeps the reap prompt; it is the value installed as the
  // per-socket inactivity timeout.
  const CLOSE_MS: u64 = 300;
  let (mut dev_m, mut dev_peer) = ip_link();

  // M: a full driver on 10.0.0.1, listening on 7946.
  let t0 = memberlist_proto::Instant::from_origin(Duration::from_millis(1_000));
  let mut m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    crate::Options::new().with_close_timeout(Duration::from_millis(CLOSE_MS)),
    ip_iface(),
    TransformOptions::default(),
    memberlist_proto::EndpointOptions::new(SmolStr::new("m"), addr(7946)),
    &crate::SocketAddrResolver,
    &mut dev_m,
    t0,
  );
  m.start(t0);
  m.poll(t0, &mut dev_m);
  assert!(
    m.listener_present() && m.listener_is_listening(),
    "the listener is armed and listening at start"
  );

  // The peer dials M, emitting a SYN. We deliver that SYN to M but NEVER poll the
  // peer again, so it never ACKs M's SYN-ACK: M's listener parks half-open.
  let mut peer_if = peer_iface(&mut dev_peer);
  let mut peer_set = SocketSet::new(Vec::new());
  let hp = peer_set.add(tcp::Socket::new(
    tcp::SocketBuffer::new(vec![0u8; 4096]),
    tcp::SocketBuffer::new(vec![0u8; 4096]),
  ));
  let m_listener = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7946);
  peer_set
    .get_mut::<tcp::Socket>(hp)
    .connect(peer_if.context(), to_endpoint(m_listener), 49_000u16)
    .expect("peer connect");
  peer_if.poll(
    SmolInstant::from_millis(1_000),
    &mut dev_peer,
    &mut peer_set,
  );

  let t1 = memberlist_proto::Instant::from_origin(Duration::from_millis(1_001));
  m.poll(t1, &mut dev_m);
  assert!(
    m.listener_present() && !m.listener_is_listening(),
    "M's listener consumed the SYN and is now half-open (SynReceived, not Listen)"
  );

  // Advance M past the inactivity timeout (remote_last_ts ~1_001 + CLOSE_MS). One
  // poll reaps the half-open to Closed (step 1 stack tick) and re-arms it (step 2b).
  let t2 = memberlist_proto::Instant::from_origin(Duration::from_millis(1_001 + CLOSE_MS + 200));
  m.poll(t2, &mut dev_m);
  assert!(
    m.listener_present() && m.listener_is_listening(),
    "poll must re-arm the listener the inactivity timeout reaped, so the node \
     accepts inbound reliable streams again (mutation anchor: without poll's \
     re-arm the listener stays Closed and this is false)"
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Bounded stack ingress. smoltcp's `Interface::poll` drains the device until it
// stops yielding, so a device that keeps producing packets keeps the call inside
// smoltcp: on a caller-driven super-loop with no preemption that starves every
// SWIM timer, the event drain, and whatever else shares the loop. `poll` feeds a
// bounded number of packets instead and always reaches the engine, signalling the
// unread remainder with an already-due deadline.

use core::cell::Cell;

/// A device with a scripted receive supply, counting what the stack takes and
/// emits.
///
/// `remaining == None` is an inexhaustible backlog — the hostile/overloaded case,
/// which the unbounded entry point would never finish draining. The frames are
/// 20 zero bytes: a non-empty frame the stack accounts for as processed, whose IP
/// version matches neither v4 nor v6, so it is dropped without touching a socket
/// or emitting a reply.
struct FloodDevice {
  remaining: Option<usize>,
  receives: Rc<Cell<usize>>,
  emitted: Rc<Cell<usize>>,
}

impl FloodDevice {
  /// A device with nothing to deliver.
  fn quiet() -> Self {
    Self {
      remaining: Some(0),
      receives: Rc::new(Cell::new(0)),
      emitted: Rc::new(Cell::new(0)),
    }
  }

  /// Supply `n` more frames, or an inexhaustible backlog when `n` is `None`.
  fn supply(&mut self, n: Option<usize>) {
    self.remaining = n;
  }

  /// Frames the stack has taken from the device (one per ingress iteration).
  fn receives(&self) -> usize {
    self.receives.get()
  }

  /// Frames the stack has actually put on the wire.
  fn emitted(&self) -> usize {
    self.emitted.get()
  }

  fn reset_counters(&mut self) {
    self.receives.set(0);
    self.emitted.set(0);
  }
}

/// A TX token that counts the frames the stack emits.
struct CountingTx(Rc<Cell<usize>>);

impl TxToken for CountingTx {
  fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
    let mut buf = std::vec![0u8; len];
    let r = f(&mut buf);
    self.0.set(self.0.get() + 1);
    r
  }
}

impl Device for FloodDevice {
  type RxToken<'a> = IpRx;
  type TxToken<'a> = CountingTx;

  fn receive(&mut self, _t: SmolInstant) -> Option<(IpRx, CountingTx)> {
    match self.remaining {
      Some(0) => return None,
      Some(ref mut n) => *n -= 1,
      None => {}
    }
    self.receives.set(self.receives.get() + 1);
    Some((IpRx(std::vec![0u8; 20]), CountingTx(self.emitted.clone())))
  }

  fn transmit(&mut self, _t: SmolInstant) -> Option<CountingTx> {
    Some(CountingTx(self.emitted.clone()))
  }

  fn capabilities(&self) -> DeviceCapabilities {
    let mut caps = DeviceCapabilities::default();
    caps.medium = Medium::Ip;
    caps.max_transmission_unit = 1500;
    caps.checksum = ChecksumCapabilities::ignored();
    caps
  }
}

/// Build a started node with one statically-known peer, quiesced over `dev`.
///
/// The peer gives the SWIM schedulers a destination, so a due probe or gossip
/// deadline produces an observable datagram on the wire.
fn node_with_peer(
  cfg: crate::Options,
  dev: &mut FloodDevice,
  now: memberlist_proto::Instant,
) -> Memberlist<SmolStr, SocketAddr, FloodDevice> {
  let mut m: Memberlist<SmolStr, SocketAddr, _> = Memberlist::new(
    cfg,
    ip_iface(),
    TransformOptions::default(),
    memberlist_proto::EndpointOptions::new(SmolStr::new("a"), addr(7946)),
    &crate::SocketAddrResolver,
    dev,
    now,
  );
  m.start(now);
  m.inject_alive(
    SmolStr::new("peer"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 7946),
    now,
  );
  // Drain the start-up burst so the counters below see only the poll under test.
  m.poll(now, dev);
  m
}

/// One `poll` over a device with an inexhaustible backlog RETURNS, having taken
/// exactly the configured budget of packets, run the engine — an already-due
/// timer fires and its datagram reaches the wire — and asked to be polled again
/// at once.
///
/// This is the starvation regression. With the unbounded entry point the same
/// device never lets the call reach the engine at all, so no SWIM deadline is
/// ever enforced and the caller's loop never regains control.
#[test]
fn poll_bounds_ingress_fires_due_timers_and_asks_to_be_polled_again() {
  for budget in [1usize, 4, crate::config::DEFAULT_INGRESS_PACKETS_PER_POLL] {
    let t0 = memberlist_proto::Instant::from_origin(Duration::from_secs(86_400));
    let mut dev = FloodDevice::quiet();
    let mut m = node_with_peer(
      crate::Options::new().with_ingress_packets_per_poll(budget),
      &mut dev,
      t0,
    );

    // Far enough past every periodic deadline armed at `start` that at least one
    // is due, whatever the randomized schedule picked.
    let t1 = t0 + Duration::from_secs(60);
    dev.supply(None);
    dev.reset_counters();
    let next = m.poll(t1, &mut dev);

    assert_eq!(
      dev.receives(),
      budget,
      "budget {budget}: the ingress phase takes exactly the configured number of packets \
       from a device that never runs dry"
    );
    assert!(
      dev.emitted() > 0,
      "budget {budget}: an already-due engine timer must fire inside the flooded poll and its \
       datagram must reach the wire (mutation anchor: an unbounded ingress phase never \
       reaches the engine)"
    );
    assert_eq!(
      next,
      Some(t1),
      "budget {budget}: an exhausted budget means device backlog remains, so the caller is \
       asked to poll again at once rather than sleep on a timer"
    );
  }
}

/// A device that runs dry inside the budget yields the stack's and the engine's
/// real deadline — the already-due instant is reserved for an actual backlog, not
/// folded on every poll.
#[test]
fn poll_returns_a_real_deadline_when_the_device_runs_dry_inside_the_budget() {
  const BUDGET: usize = 8;
  let t0 = memberlist_proto::Instant::from_origin(Duration::from_secs(86_400));
  let mut dev = FloodDevice::quiet();
  let mut m = node_with_peer(
    crate::Options::new().with_ingress_packets_per_poll(BUDGET),
    &mut dev,
    t0,
  );

  let t1 = t0 + Duration::from_secs(60);
  dev.supply(Some(BUDGET - 1));
  dev.reset_counters();
  let next = m.poll(t1, &mut dev);

  assert_eq!(
    dev.receives(),
    BUDGET - 1,
    "the loop stops as soon as the device reports nothing left, without spending the budget"
  );
  assert!(
    next > Some(t1),
    "a drained device leaves no backlog, so the returned deadline is the real next wake, \
     not an already-due re-poll: got {next:?}"
  );
}

/// Negative control for the bound: smoltcp's own `Interface::poll` — the entry
/// point this driver no longer calls — drains the device to exhaustion whatever
/// the driver's budget says, taking every one of the many-times-the-budget frames
/// offered here plus the one that reports empty. Given the inexhaustible device
/// of the regression above it would not return at all, which is precisely why the
/// driver runs the bounded phases instead.
#[test]
fn smoltcp_interface_poll_ignores_the_drivers_ingress_budget() {
  const OFFERED: usize = 4 * crate::config::DEFAULT_INGRESS_PACKETS_PER_POLL;
  let mut dev = FloodDevice::quiet();
  let mut cfg = IfConfig::new(HardwareAddress::Ip);
  cfg.random_seed = 0x51a2_1166;
  let mut iface = Interface::new(cfg, &mut dev, SmolInstant::from_millis(0));
  iface.update_ip_addrs(|addrs| {
    addrs
      .push(IpCidr::new(crate::IpAddress::v4(10, 0, 0, 1), 24))
      .expect("push ip");
  });
  let mut sockets = SocketSet::new(Vec::new());

  dev.supply(Some(OFFERED));
  dev.reset_counters();
  iface.poll(SmolInstant::from_millis(1_000), &mut dev, &mut sockets);

  assert_eq!(
    dev.receives(),
    OFFERED,
    "the unbounded entry point drains the device to exhaustion, so no configured budget \
     bounds it"
  );
}
