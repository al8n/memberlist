use super::*;
use core::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use smol_str::SmolStr;

fn cfg() -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  )
}

/// Test helper: process an Alive with no [`AliveDelegate`] installed (every
/// alive is admitted). Resulting `NodeJoined` / `NodeUpdated` events stay
/// queued for `poll_event`.
fn process_alive_auto<I, A>(
  e: &mut Endpoint<I, A>,
  alive: Alive<I, A>,
  bootstrap: bool,
  now: Instant,
) where
  I: crate::Id,
  A: crate::CheapClone
    + Data
    + Eq
    + core::hash::Hash
    + fmt::Debug
    + fmt::Display
    + Send
    + Sync
    + 'static,
{
  e.process_alive(alive, bootstrap, now);
}

#[test]
fn new_endpoint_inserts_local_at_incarnation_1() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert_eq!(e.local_id_ref(), &SmolStr::new("local"));
  assert_eq!(e.num_members(), 1);
  let local = e.member(&SmolStr::new("local")).expect("local present");
  assert_eq!(local.id_ref(), &SmolStr::new("local"));
  assert_eq!(local.state(), State::Alive);
}

#[test]
fn new_endpoint_emits_self_join_as_first_event() {
  // Mirrors Go memberlist's `NotifyJoin(localNode)` on Create: the first event
  // a fresh endpoint surfaces is the local node's own join, carrying the local
  // id and advertise address. Event-driven consumers (the driver EventStream,
  // serf) depend on this self-join to learn about the local node.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let ev = e
    .poll_event()
    .expect("a fresh endpoint emits the local self-join");
  match ev {
    Event::NodeJoined(n) => {
      assert_eq!(n.id_ref(), &SmolStr::new("local"));
      assert_eq!(
        n.address_ref(),
        &SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000)
      );
      assert_eq!(n.state(), State::Alive);
    }
    other => panic!("expected NodeJoined(self), got {other:?}"),
  }
  assert!(
    e.poll_event().is_none(),
    "construction emits exactly one event (the self-join)"
  );
}

#[test]
fn new_endpoint_is_not_leaving_or_left() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert!(!e.is_left());
  assert!(!e.is_leaving());
}

#[test]
fn new_endpoint_health_score_is_zero() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert_eq!(e.health_score(), 0);
}

#[test]
fn health_score_reflects_awareness() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert_eq!(e.health_score(), 0usize);
  e.degrade_health(3);
  assert_eq!(e.health_score(), 3usize);
  e.improve_health();
  assert_eq!(e.health_score(), 2usize);
}

#[test]
fn new_at_stamps_local_member_at_driver_clock() {
  // A std driver with a virtual clock whose instants sit far below
  // `Instant::now()`. The local member must be stamped at the driver's clock,
  // not the wall clock — otherwise the machine reads a clock the driver does
  // not own and carries a local `state_change` in the driver's own future,
  // where later `duration_since` could saturate or panic.
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at_seeded(cfg(), t0);
  assert_eq!(
    e.node_state_change(&SmolStr::new("local")),
    Some(t0),
    "local member must be stamped at the driver clock, not Instant::now()"
  );

  // Driving entirely with driver instants at/after `t0` (all far below the
  // wall clock) must not panic.
  e.handle_timeout(t0 + Duration::from_millis(500));
}

#[test]
fn try_new_at_with_seed_is_infallible() {
  // A seeded config never touches platform entropy, so the fallible
  // constructor always succeeds — the fully Sans-I/O / deterministic path.
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let e = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(cfg(), t0)
    .expect("seeded construction never fails");
  assert_eq!(e.num_members(), 1);
}

#[test]
fn scheduler_deadlines_saturate_at_extreme_now() {
  // A pathological near-maximum clock must not panic when the scheduler builds
  // probe/gossip/push-pull deadlines as `now + interval`; the forward
  // arithmetic saturates instead.
  let near_max = Instant::from_origin(Duration::MAX - Duration::from_secs(1));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at_seeded(cfg(), near_max);
  e.start_scheduling(near_max);
  e.handle_timeout(near_max);
}

use crate::typed::{DelegateVersion, ProtocolVersion};
use Alive;
use Dead;
use Meta;
use Node;
use Suspect;

fn alive(node_id: &str, port: u16, inc: u32) -> Alive<SmolStr, SocketAddr> {
  Alive::new(
    inc,
    Node::new(
      SmolStr::new(node_id),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
    ),
  )
  .with_meta(Meta::empty())
  .with_protocol_version(ProtocolVersion::V1)
  .with_delegate_version(DelegateVersion::V1)
}

#[test]
fn alive_inserts_new_node_with_join_event() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // Drain the initial events from `new()` first.
  while e.poll_event().is_some() {}
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  // Bob is in members now.
  assert_eq!(e.num_members(), 2);
  assert!(e.member(&SmolStr::new("bob")).is_some());
  // First event drained should be NodeJoined for bob.
  let ev = e.poll_event().expect("expected NodeJoined");
  match ev {
    Event::NodeJoined(node) => assert_eq!(node.id_ref(), &SmolStr::new("bob")),
    other => panic!("expected NodeJoined, got {other:?}"),
  }
}

#[test]
fn alive_existing_with_old_incarnation_is_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 5), false, Instant::now());
  while e.poll_event().is_some() {}
  // Older incarnation: ignored.
  process_alive_auto(&mut e, alive("bob", 7001, 3), false, Instant::now());
  // Incarnation should still be 5.
  let bob_member = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob_member.state_ref().incarnation(), 5);
  assert!(
    e.poll_event().is_none(),
    "no event expected on ignored alive"
  );
}

#[test]
fn alive_self_with_higher_incarnation_refutes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  // Someone is claiming to be us at incarnation 5; refute by bumping past.
  process_alive_auto(&mut e, alive("local", 7000, 5), false, Instant::now());
  let local_member = e.members.get(&SmolStr::new("local")).unwrap();
  assert!(
    local_member.state_ref().incarnation() > 5,
    "should bump past 5"
  );
  // Health score should have ticked up.
  assert_eq!(e.health_score(), 1);
}

/// Incarnation arithmetic must WRAP at u32::MAX. A u32::MAX
/// self-accusation is unrefutable (it wraps to 0, which peers reject as
/// `0 < MAX`); since the u32 incarnation is part of the wire protocol,
/// this degenerate behavior is faithfully preserved rather than diverging
/// with a saturating clamp or a widened type. This test pins the wrap so
/// a future change does not silently re-introduce a saturating clamp or a
/// u32::MAX guard.
#[test]
fn incarnation_wraps_at_u32_max() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  // A peer accuses us at u32::MAX. refute(): next_incarnation 1→2, then
  // skip_incarnation_past(MAX) ⇒ MAX.wrapping_add(1) == 0 (NOT MAX, which
  // a saturating impl would produce).
  process_alive_auto(
    &mut e,
    alive("local", 7000, u32::MAX),
    false,
    Instant::now(),
  );
  let local = e.members.get(&SmolStr::new("local")).unwrap();
  assert_eq!(
    local.state_ref().incarnation(),
    0,
    "incarnation must wrap to 0 at u32::MAX, not saturate"
  );
}

#[test]
fn alive_address_change_alive_node_emits_conflict() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  // Same id, different port → conflict.
  process_alive_auto(&mut e, alive("bob", 7777, 2), false, Instant::now());
  let ev = e.poll_event().expect("expected NodeConflict");
  match ev {
    Event::NodeConflict(p) => {
      assert_eq!(p.existing_ref().address_ref().port(), 7001);
      assert_eq!(p.other_ref().address_ref().port(), 7777);
    }
    other => panic!("expected NodeConflict, got {other:?}"),
  }
  // Bob's tracked address should still be 7001.
  assert_eq!(
    e.member(&SmolStr::new("bob")).unwrap().address_ref().port(),
    7001
  );
}

fn suspect(target: &str, from: &str, inc: u32) -> Suspect<SmolStr> {
  Suspect::new(inc, SmolStr::new(target), SmolStr::new(from))
}

fn dead(target: &str, from: &str, inc: u32) -> Dead<SmolStr> {
  Dead::new(inc, SmolStr::new(target), SmolStr::new(from))
}

#[test]
fn suspect_alive_node_starts_timer() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  e.process_suspect(suspect("bob", "carol", 1), Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Suspect);
  assert!(bob.suspicion().is_some());
}

#[test]
fn suspect_self_refutes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let starting_inc = e
    .members
    .get(&SmolStr::new("local"))
    .unwrap()
    .state_ref()
    .incarnation();
  e.process_suspect(suspect("local", "carol", starting_inc + 5), Instant::now());
  let local = e.members.get(&SmolStr::new("local")).unwrap();
  assert!(local.state_ref().incarnation() > starting_inc + 5);
  assert_eq!(local.state_ref().state(), State::Alive);
}

#[test]
fn suspect_old_incarnation_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 5), false, Instant::now());
  e.process_suspect(suspect("bob", "carol", 1), Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Alive);
  assert!(bob.suspicion().is_none());
}

#[test]
fn dead_alive_node_marks_dead_and_emits_left() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  e.process_dead(dead("bob", "carol", 1), Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Dead);
  let ev = e.poll_event().expect("expected NodeLeft");
  match ev {
    Event::NodeLeft(node) => assert_eq!(node.id_ref(), &SmolStr::new("bob")),
    other => panic!("expected NodeLeft, got {other:?}"),
  }
}

#[test]
fn reset_nodes_with_now_before_state_change_does_not_panic() {
  // A Dead member stamped at a late instant, then reclaim runs with an
  // earlier `now` (a stale timer tick, a virtual-clock domain, or a direct
  // `reset_nodes` call). The elapsed-since-state_change must saturate to zero
  // rather than panic, and the member must not be reclaimed yet.
  let t_late = Instant::from_origin(Duration::from_secs(100));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at_seeded(cfg(), t_late);
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t_late);
  while e.poll_event().is_some() {}
  e.process_dead(dead("bob", "carol", 1), t_late);
  assert_eq!(
    e.members
      .get(&SmolStr::new("bob"))
      .unwrap()
      .state_ref()
      .state(),
    State::Dead
  );

  let t_early = Instant::from_origin(Duration::from_secs(1));
  e.reset_nodes(t_early); // must not panic
  assert!(
    e.members.get(&SmolStr::new("bob")).is_some(),
    "a Dead member must not be reclaimed when now precedes its state_change"
  );
}

#[test]
fn dead_self_when_not_leaving_refutes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let starting_inc = e
    .members
    .get(&SmolStr::new("local"))
    .unwrap()
    .state_ref()
    .incarnation();
  e.process_dead(dead("local", "carol", starting_inc + 5), Instant::now());
  let local = e.members.get(&SmolStr::new("local")).unwrap();
  assert!(local.state_ref().incarnation() > starting_inc + 5);
  assert_eq!(local.state_ref().state(), State::Alive);
}

#[test]
fn dead_self_marked_message_treats_as_left() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  // node==from convention: bob is announcing his own departure.
  e.process_dead(dead("bob", "bob", 1), Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Left);
}

use PushNodeState;

fn pns(node_id: &str, port: u16, inc: u32, st: State) -> PushNodeState<SmolStr, SocketAddr> {
  PushNodeState::new(
    inc,
    SmolStr::new(node_id),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
    st,
  )
  .with_meta(Meta::empty())
  .with_protocol_version(ProtocolVersion::V1)
  .with_delegate_version(DelegateVersion::V1)
}

#[test]
fn merge_alive_inserts_as_alive() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let remote = vec![
    pns("bob", 7001, 1, State::Alive),
    pns("carol", 7002, 1, State::Alive),
  ];
  e.merge_state(&remote, Instant::now());
  // merge_state admits alives synchronously (no AliveDelegate installed).
  assert_eq!(e.num_members(), 3);
  assert_eq!(
    e.member(&SmolStr::new("bob")).unwrap().state(),
    State::Alive
  );
}

#[test]
fn merge_dead_treats_as_suspect() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  // Remote thinks bob is dead — we suspect rather than mark dead.
  e.merge_state(&[pns("bob", 7001, 2, State::Dead)], Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Suspect);
}

#[test]
fn merge_remote_left_marks_dead_not_left() {
  // A remote node learned as Left via anti-entropy must become State::Dead
  // (reclaim-protected), NOT State::Left (immediately address-reclaimable).
  // State::Left is reserved for the genuine self-leave sentinel where the
  // accuser == the node itself.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  e.merge_state(&[pns("bob", 7001, 2, State::Left)], Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Dead);
}

#[test]
fn handle_timeout_fires_expired_suspicion() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_interval(Duration::from_millis(10))
      .with_suspicion_mult(1)
      .with_suspicion_max_timeout_mult(1),
  );
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "carol", 1), t0);
  // Suspicion now active; verify deadline is set.
  let deadline = e.poll_timeout().expect("suspicion deadline expected");
  assert!(deadline > t0);
  // Advance past the deadline and fire timeouts.
  let later = deadline + Duration::from_millis(10);
  e.handle_timeout(later);
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Dead);
}

#[test]
fn poll_timeout_returns_none_with_no_timers() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert!(e.poll_timeout().is_none());
}

#[test]
fn poll_timeout_returns_min_across_suspicion_probe_and_forward() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);

  let dl = e.poll_timeout().expect("deadline expected");
  let probe_dl = t0 + Duration::from_millis(50);
  assert!(dl >= probe_dl.checked_sub(Duration::from_millis(10)).unwrap());
  assert!(dl <= probe_dl + Duration::from_millis(10));
}

#[test]
fn push_pull_scale_below_threshold() {
  let sec = Duration::from_secs(1);
  for n in 0..=32 {
    assert_eq!(push_pull_scale(sec, n), sec, "n={n}");
  }
}

#[test]
fn push_pull_scale_above_threshold() {
  let sec = Duration::from_secs(1);
  // n=33: log2(33)≈5.044, log2(32)=5.0, diff=0.044, ceil=1, +1=2
  assert_eq!(push_pull_scale(sec, 33), Duration::from_secs(2));
  // n=64: log2(64)=6.0, log2(32)=5.0, diff=1.0, ceil=1, +1=2
  assert_eq!(push_pull_scale(sec, 64), Duration::from_secs(2));
  // n=65: log2(65)≈6.022, diff=1.022, ceil=2, +1=3
  assert_eq!(push_pull_scale(sec, 65), Duration::from_secs(3));
}

/// The event-emitting `start_push_pull` and the descriptor-returning
/// `start_push_pull_direct` are the SAME surface: from an identical fresh endpoint
/// and identical args, the old method's `DialRequested` and the direct method's
/// `DialIntent` carry the same id / peer / deadline — but the direct method
/// enqueues NO event.
///
/// Mutation-verify: make the direct method also push the `DialRequested` (or the
/// old wrapper drop it) — the "no DialRequested from the direct path" assertion (or
/// the old-surface `expect`) then fails.
#[test]
fn start_push_pull_direct_matches_event_emitting_surface() {
  use crate::event::ExchangeKind;
  let t0 = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // OLD method: enqueues a DialRequested carrying (id, peer, deadline).
  let mut e_old: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at_seeded(cfg(), t0);
  let id_old = e_old.start_push_pull(peer, PushPullKind::Refresh, t0);
  let dial = core::iter::from_fn(|| e_old.poll_event())
    .find_map(|ev| match ev {
      Event::DialRequested(p) => Some(p),
      _ => None,
    })
    .expect("the event-emitting start_push_pull enqueues a DialRequested");
  assert_eq!(dial.id(), id_old);
  assert_eq!(dial.peer_ref(), &peer);
  let deadline = dial.deadline();

  // DIRECT method: same id / peer / deadline, and NO event.
  let mut e_new: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at_seeded(cfg(), t0);
  let (id_new, intent) = e_new.start_push_pull_direct(peer, PushPullKind::Refresh, t0);
  let intent = intent.expect("a running endpoint returns Some(DialIntent)");
  assert_eq!(
    id_new, id_old,
    "same id from the same fresh endpoint + args"
  );
  assert_eq!(intent.id(), id_old);
  assert_eq!(intent.peer_ref(), &peer);
  assert_eq!(intent.deadline(), deadline);
  assert_eq!(intent.kind(), ExchangeKind::PushPull);
  assert!(
    !core::iter::from_fn(|| e_new.poll_event()).any(|ev| matches!(ev, Event::DialRequested(..))),
    "start_push_pull_direct must enqueue NO DialRequested"
  );
}

#[test]
fn update_meta_emits_node_updated_and_increments_incarnation() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  let inc_before = e
    .members
    .get(&SmolStr::new("local"))
    .unwrap()
    .state_ref()
    .incarnation();
  let new_meta = Meta::try_from(Bytes::from_static(b"v2")).unwrap();
  e.update_meta(new_meta).expect("ok");
  let inc_after = e
    .members
    .get(&SmolStr::new("local"))
    .unwrap()
    .state_ref()
    .incarnation();
  assert!(inc_after > inc_before);
  let ev = e.poll_event().expect("expected NodeUpdated");
  assert!(matches!(ev, Event::NodeUpdated(_)));
}

#[test]
fn update_meta_at_limit_is_accepted() {
  // Meta::MAX_SIZE == META_MAX_SIZE == 512, so a meta at the limit should
  // be accepted. Since Meta enforces the bound at construction, we verify
  // the boundary value here.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let at_limit = Meta::try_from(Bytes::from(vec![0u8; META_MAX_SIZE])).unwrap();
  let r = e.update_meta(at_limit);
  assert!(r.is_ok(), "meta at the limit should be accepted");
}

#[test]
fn leave_with_no_live_peers_emits_left_cluster_immediately() {
  // No live peers ⇒ nothing to flush ⇒ legacy `if any_alive` gate is
  // false ⇒ leave completes immediately (LeftCluster synchronous).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  e.leave(Instant::now()).expect("ok");
  assert!(e.is_left());
  let events: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(events.iter().any(|ev| matches!(ev, Event::NodeLeft(_))));
  assert!(events.iter().any(|ev| matches!(ev, Event::LeftCluster)));
}

/// With a live peer, `LeftCluster` is the *completion* signal — it must
/// NOT fire at the state transition while the dead-self notice is still
/// queued. A driver that shut down on the `leave()` return / NodeLeft
/// would drop the leave notice and peers would see a failure instead of
/// an intentional leave. It fires only once `poll_transmit` has drained
/// the queued dead-self.
#[test]
fn leave_defers_left_cluster_until_dead_self_drained() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  e.leave(t0).expect("ok");
  // Local node transitioned synchronously (matches legacy has_left()).
  assert!(e.is_left());
  // ...but the leave is NOT complete: LeftCluster must be withheld while
  // the dead-self notice is still only queued.
  let pre: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(
    pre.iter().any(|ev| matches!(ev, Event::NodeLeft(_))),
    "NodeLeft is the immediate state-change notification"
  );
  assert!(
    !pre.iter().any(|ev| matches!(ev, Event::LeftCluster)),
    "LeftCluster must NOT fire before the dead-self is drained"
  );

  // The dead-self notice is queued for the live peer.
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let mut saw_dead_self = false;
  while let Some(tx) = e.poll_transmit() {
    let Transmit::Packet(p) = tx else {
      panic!("unexpected Compound transmit");
    };
    let (to, message) = p.into_parts();
    if to == peer_addr && matches!(message, Message::Dead(_)) {
      saw_dead_self = true;
    }
  }
  assert!(saw_dead_self, "dead-self must be queued to the live peer");

  // Now that poll_transmit drained it, LeftCluster fires.
  let post: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(
    post.iter().any(|ev| matches!(ev, Event::LeftCluster)),
    "LeftCluster must fire once the dead-self has been handed to the I/O layer"
  );
}

/// A zero-live-peer leave completes immediately AND drops any gossip-plane
/// packet queued before it: a departing node emits only its leave notice (here
/// absent — no live peers), never a stale Ack.
#[test]
fn leave_no_live_peers_drops_stale_transmit_and_completes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  // Queue an unrelated packet (Ack reply) WITHOUT adding any member.
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
  e.handle_ping(
    from,
    ping_to("local", 7000, "alice", 8001, 1),
    Instant::now(),
  );
  assert_eq!(e.num_members(), 1, "no live peers — only local");

  e.leave(Instant::now()).expect("ok");
  assert!(e.is_left());
  // LeftCluster fires immediately (no live peers).
  let events: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(
    events.iter().any(|ev| matches!(ev, Event::LeftCluster)),
    "zero-live-peer leave must complete immediately"
  );
  // The stale Ack was dropped by leave() — a left node holds no buffered
  // gossip that later produces I/O.
  assert!(
    e.poll_transmit().is_none(),
    "leave drops gossip queued before it; nothing remains to send"
  );
}

/// With a live peer, leave() drops any stale prefix and queues only the
/// dead-self, so the completion boundary is exactly that one notice: LeftCluster
/// fires when the dead-self drains, and a packet enqueued after leave never
/// re-triggers it.
#[test]
fn leave_left_cluster_boundary_is_exactly_the_dead_self() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Stale packet queued BEFORE leave — leave() must drop it.
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
  e.handle_ping(from, ping_to("local", 7000, "alice", 8001, 1), t0);

  e.leave(t0).expect("ok"); // drops the stale Ack, queues only the dead-self
  assert!(e.is_left());
  let pre: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(pre.iter().any(|ev| matches!(ev, Event::NodeLeft(_))));
  assert!(
    !pre.iter().any(|ev| matches!(ev, Event::LeftCluster)),
    "LeftCluster must not fire before the dead-self drains"
  );

  // Pop #1 is the dead-self itself — the stale prefix was dropped — and draining
  // it reaches the completion boundary.
  {
    let tx = e.poll_transmit();
    assert!(
      matches!(&tx, Some(Transmit::Packet(p)) if matches!(p.message_ref(), Message::Dead(_))),
      "the only queued packet is the dead-self; the stale Ack was dropped"
    );
  }
  assert!(
    core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "LeftCluster fires exactly when the dead-self is handed off"
  );

  // A packet enqueued after leave sits behind the already-passed boundary and
  // must not re-trigger completion.
  e.handle_ping(from, ping_to("local", 7000, "alice", 8001, 2), t0);
  {
    let tx = e.poll_transmit();
    assert!(
      matches!(&tx, Some(Transmit::Packet(p)) if matches!(p.message_ref(), Message::Ack(_))),
      "expected the post-leave Ack"
    );
  }
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "post-leave traffic must not re-trigger LeftCluster"
  );
}

#[test]
fn leave_is_idempotent() {
  // A repeated leave is a harmless no-op, not an error: once already
  // left/shutdown, `leave()` is idempotent and must not re-broadcast.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  e.leave(Instant::now()).expect("first leave ok");
  assert!(e.is_left());
  // Repeated leave: Ok, idempotent, and must not re-fan-out the dead-self.
  while e.poll_transmit().is_some() {}
  e.leave(Instant::now())
    .expect("repeated leave is Ok (idempotent)");
  assert!(
    e.poll_transmit().is_none(),
    "repeated leave must not re-enqueue/re-fan-out the dead-self"
  );
}

// ───────── leave() farewell: queued user broadcasts ride the leave notice ──

/// The encoded self-`Dead` `leave()` reserves before packing user payloads, for
/// a freshly-constructed endpoint (local incarnation 1, target == from ==
/// local id).
fn farewell_self_dead_len(e: &Endpoint<SmolStr, SocketAddr>) -> usize {
  let local = e.local_id_ref().clone();
  crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::Dead(Dead::new(
    1,
    local.clone(),
    local,
  )))
  .map(|b| b.len())
  .expect("a locally-built Dead always encodes")
}

/// The farewell drain budget `leave()` computes: the gossip MTU minus the
/// compound header and the reserved `Dead` part.
fn farewell_budget(e: &Endpoint<SmolStr, SocketAddr>) -> usize {
  e.gossip_mtu()
    - (crate::wire::COMPOUND_TAG_LEN + crate::wire::COMPOUND_MAX_COUNT_PREFIX_LEN)
    - (farewell_self_dead_len(e) + crate::wire::COMPOUND_MAX_PART_PREFIX_LEN)
}

/// With user broadcasts queued, the dead-self fan-out to a live peer is a
/// Compound whose parts are the user payloads followed by the `Dead` — user
/// parts FIRST, the membership death LAST.
#[test]
fn leave_farewell_packs_user_broadcasts_then_dead() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  e.queue_user_broadcast(Bytes::from_static(b"intent-1"))
    .unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"intent-2"))
    .unwrap();
  while e.poll_event().is_some() {}

  e.leave(t0).expect("ok");
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let parts = match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => {
      assert_eq!(c.to_ref(), &peer_addr);
      c.into_parts().1
    }
    Transmit::Packet(p) => panic!(
      "queued user data ⇒ farewell must be a compound, got a bare {:?}",
      p.message_ref()
    ),
  };
  let n = parts.len();
  assert!(n >= 2, "at least one user part plus the Dead");
  assert!(
    matches!(parts.last(), Some(Message::Dead(_))),
    "the Dead part must be LAST"
  );
  let user: Vec<Bytes> = parts[..n - 1]
    .iter()
    .map(|m| match m {
      Message::UserData(b) => b.clone(),
      other => panic!("every part before the Dead must be UserData, got {other:?}"),
    })
    .collect();
  assert!(user.contains(&Bytes::from_static(b"intent-1")));
  assert!(user.contains(&Bytes::from_static(b"intent-2")));
}

/// Strict priority across tiers: a rank-0 payload queued AFTER a lower-priority
/// (rank-1) payload is selected FIRST, so it precedes the rank-1 part in the
/// farewell compound (the `Dead` still last).
#[test]
fn leave_farewell_packs_higher_priority_tier_first() {
  let opts = cfg().with_user_broadcast_tiers(core::num::NonZeroU8::new(2).unwrap());
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(opts);
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  // Lower priority queued first, higher priority second.
  e.queue_user_broadcast_ranked(1, Bytes::from_static(b"low"))
    .unwrap();
  e.queue_user_broadcast_ranked(0, Bytes::from_static(b"high"))
    .unwrap();
  while e.poll_event().is_some() {}

  e.leave(t0).expect("ok");
  let parts = match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => c.into_parts().1,
    Transmit::Packet(p) => panic!("expected a compound, got a bare {:?}", p.message_ref()),
  };
  assert!(
    matches!(&parts[0], Message::UserData(b) if b.as_ref() == b"high"),
    "the rank-0 payload rides first even though it was queued last"
  );
  assert!(
    matches!(&parts[1], Message::UserData(b) if b.as_ref() == b"low"),
    "the rank-1 payload follows the rank-0"
  );
  assert!(matches!(parts.last(), Some(Message::Dead(_))), "Dead last");
}

/// Under same-tier crowding the within-tier order (larger encoded length first,
/// then freshest id) picks the rider. With the earliest rank-0 payload sized
/// largest and a budget that admits only one user part, that earliest payload
/// is the sole rider — the later, smaller same-tier payloads are crowded out,
/// not the priority winner.
#[test]
fn leave_farewell_same_tier_crowding_rides_priority_winner() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  let budget = farewell_budget(&e);
  // `big` alone consumes enough of the farewell budget that no second user part
  // fits; it is both the earliest and the largest same-tier payload.
  let big = budget * 3 / 5;
  let small = big - 8;
  e.queue_user_broadcast(Bytes::from(vec![0xAA_u8; big]))
    .unwrap();
  e.queue_user_broadcast(Bytes::from(vec![0xBB_u8; small]))
    .unwrap();
  e.queue_user_broadcast(Bytes::from(vec![0xCC_u8; small]))
    .unwrap();
  while e.poll_event().is_some() {}

  e.leave(t0).expect("ok");
  let parts = match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => c.into_parts().1,
    Transmit::Packet(p) => panic!("expected a compound, got a bare {:?}", p.message_ref()),
  };
  assert_eq!(
    parts.len(),
    2,
    "only the largest rank-0 rides, then the Dead"
  );
  assert!(
    matches!(&parts[0], Message::UserData(b) if b.len() == big),
    "the earliest, largest rank-0 payload is the sole rider under crowding"
  );
  assert!(matches!(parts.last(), Some(Message::Dead(_))), "Dead last");
}

/// An explicit farewell payload is reserved ahead of the drain: an older, larger
/// same-tier payload that would win the fewest-transmissions-then-largest
/// ordering cannot crowd it out. The reserved part rides FIRST in every fan-out
/// compound; the queued crowd rides only in whatever budget remains.
#[test]
fn leave_with_reserves_the_farewell_ahead_of_larger_queued_payloads() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  let overhead = Endpoint::<SmolStr, SocketAddr>::USER_PART_OVERHEAD;
  // The queued payload exactly fills the farewell budget on its own — a plain
  // leave() would carry it — so whether it still rides after the reservation is
  // decided purely by the explicit farewell's charge.
  let big = farewell_budget(&e) - overhead;
  e.queue_user_broadcast(Bytes::from(vec![0xAA_u8; big]))
    .unwrap();
  while e.poll_event().is_some() {}

  let intent = Bytes::from_static(b"leave-intent");
  e.leave_with(t0, Some(intent.clone())).expect("ok");
  let parts = match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => c.into_parts().1,
    Transmit::Packet(p) => panic!("expected a compound, got a bare {:?}", p.message_ref()),
  };
  assert!(
    matches!(&parts[0], Message::UserData(b) if b == &intent),
    "the explicit farewell must ride FIRST, immune to the drain ordering"
  );
  assert!(matches!(parts.last(), Some(Message::Dead(_))), "Dead last");
  // The big queued payload no longer fits the residual budget after the
  // reservation — it is dropped, not the farewell.
  assert_eq!(
    parts.len(),
    2,
    "reserved farewell plus the Dead; the crowding payload lost the residual"
  );
}

/// A residual budget large enough for both carries the reserved farewell FIRST
/// and the drained payload after it.
#[test]
fn leave_with_reserved_farewell_leads_the_drained_payloads() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  e.queue_user_broadcast(Bytes::from_static(b"queued-cargo"))
    .unwrap();
  while e.poll_event().is_some() {}

  let intent = Bytes::from_static(b"leave-intent");
  e.leave_with(t0, Some(intent.clone())).expect("ok");
  let parts = match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => c.into_parts().1,
    Transmit::Packet(p) => panic!("expected a compound, got a bare {:?}", p.message_ref()),
  };
  assert_eq!(parts.len(), 3, "farewell, the drained payload, the Dead");
  assert!(
    matches!(&parts[0], Message::UserData(b) if b == &intent),
    "the explicit farewell leads"
  );
  assert!(
    matches!(&parts[1], Message::UserData(b) if b.as_ref() == b"queued-cargo"),
    "the drained payload follows the reserved farewell"
  );
  assert!(matches!(parts.last(), Some(Message::Dead(_))), "Dead last");
}

/// `farewell_capacity` is consistent with `leave_with`'s admission: a payload
/// at the advertised capacity rides; anything above it is the caller's to
/// refuse before leaving.
#[test]
fn farewell_capacity_matches_leave_with_admission() {
  let t0 = Instant::now();
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  let cap = e.farewell_capacity().expect("a small id leaves room");
  e.leave_with(t0, Some(Bytes::from(vec![0xAD_u8; cap])))
    .expect("ok");
  let parts = match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => c.into_parts().1,
    Transmit::Packet(p) => panic!(
      "a capacity-sized farewell must ride, got a bare {:?}",
      p.message_ref()
    ),
  };
  assert!(
    matches!(&parts[0], Message::UserData(b) if b.len() == cap),
    "the capacity-sized farewell rides first"
  );
  assert!(matches!(parts.last(), Some(Message::Dead(_))), "Dead last");
}

/// An explicit farewell with an empty queue rides as `[farewell, Dead]`; and an
/// over-budget farewell is dropped, degrading to the bare `Dead` fan-out.
#[test]
fn leave_with_farewell_shapes_at_the_edges() {
  // Empty queue: [farewell, Dead].
  let t0 = Instant::now();
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  e.leave_with(t0, Some(Bytes::from_static(b"leave-intent")))
    .expect("ok");
  let parts = match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => c.into_parts().1,
    Transmit::Packet(p) => panic!("expected a compound, got a bare {:?}", p.message_ref()),
  };
  assert_eq!(parts.len(), 2, "the farewell plus the Dead");

  // Over-budget farewell: dropped, bare Dead preserved.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  let oversize = farewell_budget(&e) * 2;
  e.leave_with(t0, Some(Bytes::from(vec![0xEE_u8; oversize])))
    .expect("ok");
  assert!(
    matches!(
      e.poll_transmit().expect("farewell transmit"),
      Transmit::Packet(p) if matches!(p.message_ref(), Message::Dead(_))
    ),
    "an over-budget farewell degrades to the bare Dead"
  );
}

/// Budget boundary: a payload whose assembled charge is exactly the farewell
/// budget rides; one byte over does not (it is still enqueuable, since the
/// enqueue gate reserves less than the farewell budget — which additionally
/// reserves the `Dead` part — so the fan-out degrades to the bare `Dead`).
#[test]
fn leave_farewell_budget_boundary_rides_at_limit_not_one_over() {
  let overhead = Endpoint::<SmolStr, SocketAddr>::USER_PART_OVERHEAD;
  let t0 = Instant::now();

  // At the budget: rides as a compound.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  let max_payload = farewell_budget(&e) - overhead;
  e.queue_user_broadcast(Bytes::from(vec![0xAB_u8; max_payload]))
    .unwrap();
  while e.poll_event().is_some() {}
  e.leave(t0).expect("ok");
  match e.poll_transmit().expect("farewell transmit") {
    Transmit::Compound(c) => {
      let parts = c.into_parts().1;
      assert_eq!(parts.len(), 2, "the at-budget payload plus the Dead");
      assert!(matches!(&parts[0], Message::UserData(b) if b.len() == max_payload));
      assert!(matches!(parts.last(), Some(Message::Dead(_))));
    }
    Transmit::Packet(p) => panic!(
      "a payload at the budget must ride a compound, got a bare {:?}",
      p.message_ref()
    ),
  }

  // One byte over: does not ride; the fan-out is the bare Dead.
  let mut e2: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e2, alive("peer", 7001, 1), false, t0);
  let over = farewell_budget(&e2) - overhead + 1;
  e2.queue_user_broadcast(Bytes::from(vec![0xCD_u8; over]))
    .expect("one-over-budget payload is still enqueuable");
  while e2.poll_event().is_some() {}
  e2.leave(t0).expect("ok");
  match e2.poll_transmit().expect("farewell transmit") {
    Transmit::Packet(p) => assert!(
      matches!(p.message_ref(), Message::Dead(_)),
      "over-budget payload is dropped; the bare Dead rides"
    ),
    Transmit::Compound(c) => panic!(
      "an over-budget payload must not ride; expected a bare Dead, got a compound of {} parts",
      c.messages_slice().len()
    ),
  }
}

/// Degenerate budget: with the gossip MTU consumed entirely by the encoded
/// dead-self notice, queued user broadcasts get no farewell ride,
/// `farewell_capacity` reports `None`, and the fan-out degrades to the bare
/// `Packet(Dead)` rather than underflowing or panicking. The identity-aware
/// construction floor sizes the MTU against a local-node `Ping` that always
/// out-measures the Dead plus the compound overhead, so this state is
/// unreachable through the validated constructor — the drain arm is the
/// machine's own backstop, and the saturated MTU is forced directly to pin it.
#[test]
fn leave_farewell_huge_dead_leaves_no_farewell_room() {
  let t0 = Instant::now();
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  e.queue_user_broadcast(Bytes::from_static(b"stranded"))
    .expect("a small user broadcast is enqueuable");
  while e.poll_event().is_some() {}
  e.cfg = cfg().with_gossip_mtu(farewell_self_dead_len(&e));
  assert_eq!(
    e.farewell_capacity(),
    None,
    "no admissible farewell size when the Dead fills the MTU"
  );
  e.leave(t0).expect("ok");
  match e.poll_transmit().expect("fan-out transmit") {
    Transmit::Packet(p) => assert!(
      matches!(p.message_ref(), Message::Dead(_)),
      "the queued broadcast gets no ride; the bare Dead fans out"
    ),
    Transmit::Compound(c) => panic!(
      "the Dead alone fills the MTU; expected a bare Dead, got a compound of {} parts",
      c.messages_slice().len()
    ),
  }
}

/// Empty queue: with no user broadcasts queued, each fan-out is the
/// byte-identical bare `Packet(Dead)` — unchanged from before the farewell
/// flush existed.
#[test]
fn leave_with_no_queued_broadcasts_fans_out_bare_dead() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  e.leave(t0).expect("ok");
  match e.poll_transmit().expect("farewell transmit") {
    Transmit::Packet(p) => {
      let (to, msg) = p.into_parts();
      assert_eq!(
        to,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001)
      );
      assert!(
        matches!(msg, Message::Dead(_)),
        "no queued user data ⇒ a byte-identical bare Dead"
      );
    }
    Transmit::Compound(c) => panic!("empty queue must stay a bare Dead packet, got {c:?}"),
  }
}

/// A farewell Compound is ONE `pending_transmits` entry, so a single
/// `poll_transmit` pop reaches the leave-completion boundary and emits
/// `LeftCluster` — the one-pop-per-peer fence is unchanged by compounds.
#[test]
fn leave_farewell_compound_is_one_pop_for_left_cluster() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  e.queue_user_broadcast(Bytes::from_static(b"bye")).unwrap();
  while e.poll_event().is_some() {}

  e.leave(t0).expect("ok");
  // NodeLeft is immediate; LeftCluster is withheld until the farewell drains.
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "LeftCluster must not fire before the farewell drains"
  );
  let tx = e.poll_transmit().expect("farewell transmit");
  assert!(
    matches!(tx, Transmit::Compound(_)),
    "a queued user broadcast makes the farewell a compound"
  );
  assert!(
    core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "one pop of the compound reaches the completion boundary"
  );
}

/// Zero live peers with a queued broadcast: leave still completes immediately
/// (the drain is skipped when there is no fan-out), and the queued payload is
/// dropped by the reset rather than emitted — there is no recipient.
#[test]
fn leave_with_queued_broadcast_but_no_live_peers_completes_immediately() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  e.queue_user_broadcast(Bytes::from_static(b"bye")).unwrap();
  while e.poll_event().is_some() {}

  e.leave(Instant::now()).expect("ok");
  assert!(e.is_left());
  assert!(
    core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "a zero-live-peer leave completes immediately"
  );
  assert!(
    e.poll_transmit().is_none(),
    "no live recipient ⇒ the queued broadcast is dropped, not sent"
  );
}

/// The farewell compound survives the codec round-trip (and at least one wire
/// transform) with its parts in order and the `Dead` still last — the in-order
/// consumption a layered protocol relies on to process its departure payload
/// before the membership death.
#[test]
fn leave_farewell_compound_roundtrips_through_codec_in_order() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  e.queue_user_broadcast(Bytes::from_static(b"depart-a"))
    .unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"depart-b"))
    .unwrap();
  while e.poll_event().is_some() {}

  e.leave(t0).expect("ok");
  let parts: Vec<Message<SmolStr, SocketAddr>> = match e.poll_transmit().expect("farewell transmit")
  {
    Transmit::Compound(c) => c.into_parts().1.iter().cloned().collect(),
    Transmit::Packet(p) => panic!("expected a compound, got a bare {:?}", p.message_ref()),
  };

  // Plain codec round-trip preserves wire order (Dead last).
  let encoded =
    crate::codec::encode_outgoing_compound(&parts, &crate::codec::EncodeOptions::default())
      .unwrap();
  let inner =
    crate::codec::decode_incoming(encoded, &crate::codec::DecodeOptions::default()).unwrap();
  let decoded: Vec<Message<SmolStr, SocketAddr>> = crate::codec::parse_messages(inner).unwrap();
  assert_eq!(decoded.len(), parts.len());
  assert!(
    matches!(decoded.last(), Some(Message::Dead(_))),
    "Dead stays last after the round-trip"
  );
  for m in &decoded[..decoded.len() - 1] {
    assert!(
      matches!(m, Message::UserData(_)),
      "user parts stay ahead of the Dead"
    );
  }

  // The same order survives at least one wire transform (crc32 checksum wrap).
  #[cfg(feature = "crc32")]
  {
    let plain =
      crate::codec::encode_outgoing_compound(&parts, &crate::codec::EncodeOptions::default())
        .unwrap();
    let wrapped =
      crate::checksum::encode_checksummed_frame(crate::checksum::ChecksumAlgorithm::Crc32, &plain)
        .unwrap();
    let stripped = crate::checksum::decode_checksummed_frame(&wrapped).unwrap();
    let inner2 = crate::codec::decode_incoming(
      Bytes::copy_from_slice(stripped),
      &crate::codec::DecodeOptions::default(),
    )
    .unwrap();
    let decoded2: Vec<Message<SmolStr, SocketAddr>> = crate::codec::parse_messages(inner2).unwrap();
    assert!(
      matches!(decoded2.last(), Some(Message::Dead(_))),
      "Dead stays last through the checksum transform"
    );
  }
}

#[test]
fn user_data_emits_user_packet_event() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
  e.handle_user_data(from, Bytes::from_static(b"hello"), Reliability::Unreliable);
  let ev = e.poll_event().expect("UserPacket expected");
  match ev {
    Event::UserPacket(p) => {
      assert_eq!(*p.from_ref(), from);
      assert_eq!(p.data_ref().as_ref(), b"hello");
      assert!(!p.reliability().is_reliable());
    }
    other => panic!("expected UserPacket, got {other:?}"),
  }
}

#[test]
fn broadcast_queue_grows_on_alive_and_suspect() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let initial_len = e.broadcast_queue_len();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  assert!(e.broadcast_queue_len() > initial_len);
}

// ─────────────── Synchronous AliveDelegate admission ─────────────────────

/// An [`AliveDelegate`] that vetoes every inbound alive.
struct RejectAllAlive;
impl crate::delegate::AliveDelegate<SmolStr, SocketAddr> for RejectAllAlive {
  fn notify_alive(&self, _peer: &NodeState<SmolStr, SocketAddr>) -> bool {
    false
  }
}

#[test]
fn alive_delegate_reject_drops_the_message() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  e.set_alive_delegate(RejectAllAlive);
  // Drain the bootstrap event(s).
  while e.poll_event().is_some() {}
  // The delegate vetoes inline — bob must NOT enter members and no
  // NodeJoined is emitted.
  e.process_alive(alive("bob", 7001, 1), false, Instant::now());
  assert!(e.member(&SmolStr::new("bob")).is_none());
  assert!(e.poll_event().is_none());
}

#[test]
fn alive_no_delegate_applies_the_message() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  // No AliveDelegate installed → admitted synchronously.
  e.process_alive(alive("bob", 7001, 1), false, Instant::now());
  assert!(e.member(&SmolStr::new("bob")).is_some());
  let ev = e.poll_event().expect("expected NodeJoined");
  assert!(matches!(ev, Event::NodeJoined(_)));
}

/// A [`MergeDelegate`] that cancels every join merge. Records the peer view
/// it was handed so we can assert the application sees the remote states
/// (the boundary-enforcement contract).
struct RejectAllMerge {
  seen: std::sync::Mutex<Vec<SmolStr>>,
}
impl MergeDelegate<SmolStr, SocketAddr> for RejectAllMerge {
  fn notify_merge(&self, peers: crate::MaybeOwned<'_, [NodeState<SmolStr, SocketAddr>]>) -> bool {
    *self.seen.lock().unwrap() = peers.iter().map(|p| p.id_ref().clone()).collect();
    false
  }
}

/// A join push/pull merge is gated by the `MergeDelegate`, which is
/// handed the full remote peer view; returning `false` cancels the merge
/// and closes the stream.
#[test]
fn merge_delegate_vetoes_join_push_pull() {
  use EndpointEvent;
  use PushPullKind;
  use StreamCommand;
  use bytes::Bytes;
  use std::sync::{Arc, Mutex};

  use PushPullRequestReceived;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  let d = Arc::new(RejectAllMerge {
    seen: Mutex::new(Vec::new()),
  });
  e.set_merge_delegate(ArcMerge(d.clone()));

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let ev = EndpointEvent::PushPullRequestReceived(PushPullRequestReceived::new_with_stream_id(
    peer,
    vec![pns("carol", 7003, 1, State::Alive)],
    Bytes::new(),
    PushPullKind::Join,
    StreamId::from_raw(1),
  ));
  let cmd = e.handle_stream_event(ev, Instant::now());

  assert!(
    matches!(cmd, Some(StreamCommand::Close)),
    "vetoed join merge must close the stream, got {cmd:?}"
  );
  assert!(
    e.member(&SmolStr::new("carol")).is_none(),
    "vetoed join merge must not apply remote state"
  );
  assert_eq!(
    *d.seen.lock().unwrap(),
    vec![SmolStr::new("carol")],
    "MergeDelegate must be handed the remote peer view"
  );
}

/// Symmetric to [`merge_delegate_vetoes_join_push_pull`] on the OUTBOUND
/// reply arm: when WE initiated the join and the peer replied, a rejecting
/// `MergeDelegate` must close the stream so the exchange terminalizes as
/// failed (the synchronous join then counts the seed as NOT contacted). The
/// remote state must not be applied.
#[test]
fn merge_delegate_vetoes_outbound_push_pull_reply() {
  use EndpointEvent;
  use PushPullKind;
  use StreamCommand;
  use bytes::Bytes;
  use std::sync::{Arc, Mutex};

  use PushPullReplyReceived;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  let d = Arc::new(RejectAllMerge {
    seen: Mutex::new(Vec::new()),
  });
  e.set_merge_delegate(ArcMerge(d.clone()));

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let ev = EndpointEvent::PushPullReplyReceived(PushPullReplyReceived::new_with_stream_id(
    peer,
    vec![pns("carol", 7003, 1, State::Alive)],
    Bytes::new(),
    PushPullKind::Join,
    StreamId::from_raw(1),
  ));
  let cmd = e.handle_stream_event(ev, Instant::now());

  assert!(
    matches!(cmd, Some(StreamCommand::Close)),
    "vetoed outbound join merge must close the stream (fail the exchange), got {cmd:?}"
  );
  assert!(
    e.member(&SmolStr::new("carol")).is_none(),
    "vetoed outbound join merge must not apply remote state"
  );
  assert_eq!(
    *d.seen.lock().unwrap(),
    vec![SmolStr::new("carol")],
    "MergeDelegate must be handed the remote peer view on the reply arm"
  );
}

/// The `MergeDelegate` gates EVERY push/pull, a periodic Refresh included: a
/// reject-all delegate vetoes the refresh merge, closing the stream and
/// applying nothing. This deliberately tightens Go memberlist, which consults
/// `NotifyMerge` on join only and so merges a rejected peer's refresh state.
#[test]
fn merge_delegate_vetoes_refresh_push_pull() {
  use EndpointEvent;
  use PushPullKind;
  use StreamCommand;
  use bytes::Bytes;
  use std::sync::{Arc, Mutex};

  use PushPullRequestReceived;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  let d = Arc::new(RejectAllMerge {
    seen: Mutex::new(Vec::new()),
  });
  e.set_merge_delegate(ArcMerge(d.clone()));

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let ev = EndpointEvent::PushPullRequestReceived(PushPullRequestReceived::new_with_stream_id(
    peer,
    vec![pns("carol", 7003, 1, State::Alive)],
    Bytes::new(),
    PushPullKind::Refresh,
    StreamId::from_raw(1),
  ));
  let cmd = e.handle_stream_event(ev, Instant::now());

  assert!(
    matches!(cmd, Some(StreamCommand::Close)),
    "a vetoed refresh must close the stream, not respond, got {cmd:?}"
  );
  assert!(
    e.member(&SmolStr::new("carol")).is_none(),
    "a vetoed refresh merge must apply nothing"
  );
  assert!(
    !d.seen.lock().unwrap().is_empty(),
    "the MergeDelegate must be consulted for a Refresh push/pull"
  );
}

/// Newtype so an `Arc<RejectAllMerge>` can be installed as the boxed
/// `MergeDelegate` while the test retains a handle to inspect `seen`.
struct ArcMerge(std::sync::Arc<RejectAllMerge>);
impl MergeDelegate<SmolStr, SocketAddr> for ArcMerge {
  fn notify_merge(&self, peers: crate::MaybeOwned<'_, [NodeState<SmolStr, SocketAddr>]>) -> bool {
    self.0.notify_merge(peers)
  }
}

#[test]
fn set_ack_payload_round_trips() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert!(e.ack_payload().is_empty());
  e.set_ack_payload(Bytes::from_static(b"hello"))
    .expect("5-byte ack payload fits the gossip budget");
  assert_eq!(e.ack_payload(), b"hello");
}

/// An ack payload whose framed Ack exceeds the gossip packet budget is
/// rejected (and NOT stored): such an Ack is emitted as a single UDP
/// datagram that always fails to send, so every probe reply would silently
/// drop and peers would falsely suspect this node. Mirrors the fail-fast
/// `update_meta` cap. `gossip_mtu` defaults to 1400; a 4096-byte payload
/// frames to > 1400 with certainty.
#[test]
fn set_ack_payload_oversized_is_rejected() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let budget = cfg().gossip_mtu();
  let res = e.set_ack_payload(Bytes::from(vec![0xab_u8; 4096]));
  match res {
    Err(Error::AckPayloadExceedsMtu(info)) => {
      let (encoded, reported_budget) = (info.size(), info.limit());
      assert!(
        encoded > reported_budget,
        "rejected ack frame ({encoded}) must exceed the reported budget ({reported_budget})"
      );
      assert_eq!(
        reported_budget, budget,
        "budget must be the configured gossip_mtu"
      );
    }
    other => panic!("expected AckPayloadExceedsMtu, got {other:?}"),
  }
  // The rejected payload must NOT be stored — a later probe must still ack
  // with the prior (empty) payload, not the unsendable one.
  assert!(
    e.ack_payload().is_empty(),
    "a rejected oversized payload must not be stored"
  );
}

#[test]
fn disable_and_enable_reliable_ping_round_trips() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let bob = SmolStr::new("bob");
  assert!(e.is_reliable_ping_enabled(&bob));
  e.disable_reliable_ping(bob.cheap_clone());
  assert!(!e.is_reliable_ping_enabled(&bob));
  e.enable_reliable_ping(&bob);
  assert!(e.is_reliable_ping_enabled(&bob));
}

// ─────────────── Suspect confirm / dead-reclaim / idempotence ────────────

#[test]
fn suspect_with_existing_timer_confirm_pulls_deadline_in() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_interval(Duration::from_millis(10))
      .with_suspicion_mult(4)
      .with_suspicion_max_timeout_mult(6),
  );
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  // First suspect installs a timer.
  e.process_suspect(suspect("bob", "carol", 1), t0);
  let deadline_before = e.poll_timeout().expect("deadline expected");
  // Second suspect from a different peer should call confirm and pull
  // the deadline inward.
  e.process_suspect(suspect("bob", "dave", 1), t0);
  let deadline_after = e.poll_timeout().expect("deadline expected");
  assert!(
    deadline_after <= deadline_before,
    "deadline should pull inward"
  );
}

#[test]
fn alive_address_change_dead_node_reclaim_adopts_new_address() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_dead_node_reclaim_time(Duration::from_millis(50)));
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  // Mark bob dead.
  e.process_dead(dead("bob", "carol", 1), t0);
  // After the reclaim window, a new alive at a different address should
  // be adopted (no NodeConflict). Use process_alive_decided directly so
  // we control the `now` value rather than relying on wall-clock elapsed time.
  let later = t0 + Duration::from_millis(100);
  e.process_alive_decided(alive("bob", 7777, 2), false, later);
  assert_eq!(
    e.member(&SmolStr::new("bob")).unwrap().address_ref().port(),
    7777
  );
}

#[test]
fn dead_double_message_is_idempotent() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_dead(dead("bob", "carol", 1), t0);
  let inc1 = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .state_ref()
    .incarnation();
  // Second dead — same incarnation, already-dead node — must be ignored.
  e.process_dead(dead("bob", "evan", 1), t0);
  let inc2 = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .state_ref()
    .incarnation();
  assert_eq!(inc1, inc2);
  // Use members.get() to inspect the local liveness state (not the server Arc's state).
  assert_eq!(
    e.members
      .get(&SmolStr::new("bob"))
      .unwrap()
      .state_ref()
      .state(),
    State::Dead
  );
}

#[test]
fn process_alive_stamps_state_change_with_supplied_now() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  let receive_time = Instant::now();
  // Admission is synchronous: state_change must be the `now` handed to
  // process_alive, never a later wall-clock read.
  e.process_alive(alive("bob", 7001, 1), false, receive_time);
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(
    bob.state_ref().state_change(),
    receive_time,
    "process_alive must stamp state_change with the supplied receive-time"
  );
}

#[test]
fn set_local_state_snapshot_round_trips() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert!(e.local_state_snapshot().is_empty());
  e.set_local_state_snapshot(Bytes::from_static(b"snapshot-v1"))
    .expect("a tiny snapshot fits the frame budget");
  assert_eq!(e.local_state_snapshot(), b"snapshot-v1");
  e.set_local_state_snapshot(Bytes::from_static(b"snapshot-v2"))
    .expect("a tiny snapshot fits the frame budget");
  assert_eq!(e.local_state_snapshot(), b"snapshot-v2");
}

#[test]
fn set_local_state_snapshot_rejects_oversized() {
  // A snapshot whose framed PushPull exceeds `max_stream_frame_size` minus the
  // membership-state reserve is deterministically untransmittable: every
  // push/pull carrying it would be rejected at the receiver's frame-length
  // gate, so the application state would never reach a peer. The setter must
  // reject it (NOT store it) and leave the prior snapshot in place.
  let small_cap = crate::endpoint::LOCAL_STATE_FRAME_BUDGET + 4096;
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_max_stream_frame_size(small_cap));
  // Budget after the reserve is ~4096; an 8 KiB snapshot's framed PushPull is
  // well over it.
  let oversized = Bytes::from(vec![0u8; 8192]);
  let res = e.set_local_state_snapshot(oversized);
  assert!(
    matches!(res, Err(Error::LocalStateExceedsFrame(_))),
    "oversized snapshot must be rejected with LocalStateExceedsFrame, got {res:?}"
  );
  // Rejected, not stored: the snapshot is still empty.
  assert!(
    e.local_state_snapshot().is_empty(),
    "a rejected snapshot must not be stored"
  );
  // A snapshot that fits the post-reserve budget is accepted.
  e.set_local_state_snapshot(Bytes::from_static(b"fits"))
    .expect("a tiny snapshot fits even the reduced budget");
  assert_eq!(e.local_state_snapshot(), b"fits");
}

#[test]
fn queue_user_broadcast_counts_queued() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert_eq!(e.user_broadcast_queue_len(), 0);
  e.queue_user_broadcast(Bytes::from_static(b"hello"))
    .unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"world"))
    .unwrap();
  // User broadcasts carry no id and never invalidate one another, so both
  // coexist in the queue.
  assert_eq!(e.user_broadcast_queue_len(), 2);
}

#[test]
fn drain_user_broadcasts_respects_limit_and_retains_for_retransmit() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  e.queue_user_broadcast(Bytes::from_static(b"aaaa")).unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"bbbb")).unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"cccc")).unwrap();
  // Each 4 B payload is charged its assembled compound-part size:
  // 4 + USER_PART_OVERHEAD (= COMPOUND_MAX_PART_PREFIX_LEN 5 + UserData tag
  // 1 + plain-frame body-len varint 5 = 11) = 15 B. Limit 30 => pulls two
  // (15+15=30), the third (would be 45) does not fit. Within one tier the
  // queue prioritizes freshly-queued broadcasts (newest insertion id first),
  // so the drain yields cccc then bbbb, leaving aaaa for the next round.
  let (drained, used) = e.drain_user_broadcasts(1, 30);
  assert_eq!(drained.len(), 2);
  assert_eq!(drained[0].as_ref(), b"cccc");
  assert_eq!(drained[1].as_ref(), b"bbbb");
  assert_eq!(used, 30);
  // Unlike the old send-once queue, drained payloads are retransmit-counted:
  // they stay queued (bumped to transmits 1) for the next round, so all three
  // remain.
  assert_eq!(e.user_broadcast_queue_len(), 3);
}

#[test]
fn drain_user_broadcasts_zero_limit_returns_empty() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  e.queue_user_broadcast(Bytes::from_static(b"data")).unwrap();
  let (drained, used) = e.drain_user_broadcasts(1, 0);
  assert!(drained.is_empty());
  assert_eq!(used, 0);
  assert_eq!(e.user_broadcast_queue_len(), 1);
}

// ─────────────── handle_ping (responder side) ────────────────────────────

use Node as PNode;
use Ping;

fn ping_to(
  target_id: &str,
  target_port: u16,
  source_id: &str,
  source_port: u16,
  seq: u32,
) -> Ping<SmolStr, SocketAddr> {
  Ping::new(
    seq,
    PNode::new(
      SmolStr::new(source_id),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), source_port),
    ),
    PNode::new(
      SmolStr::new(target_id),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), target_port),
    ),
  )
}

#[test]
fn handle_ping_for_local_replies_with_ack() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
  let p = ping_to("local", 7000, "alice", 8001, 42);
  e.handle_ping(from, p, Instant::now());
  let t = e.poll_transmit().expect("Ack expected");
  match t {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      assert_eq!(to, from);
      match message {
        Message::Ack(ack) => assert_eq!(ack.sequence_number(), 42),
        other => panic!("expected Ack, got {other:?}"),
      }
    }
    Transmit::Compound(c) => panic!("unexpected Compound transmit: {c:?}"),
  }
}

#[test]
fn handle_ping_for_other_node_is_dropped() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
  let p = ping_to("alice", 8001, "bob", 8002, 5);
  e.handle_ping(from, p, Instant::now());
  assert!(
    e.poll_transmit().is_none(),
    "no Ack expected for misrouted Ping"
  );
}

#[test]
fn handle_ping_uses_current_ack_payload() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  e.set_ack_payload(Bytes::from_static(b"app-data"))
    .expect("8-byte ack payload fits the gossip budget");
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
  let p = ping_to("local", 7000, "alice", 8001, 1);
  e.handle_ping(from, p, Instant::now());
  let t = e.poll_transmit().expect("Ack expected");
  if let Transmit::Packet(p) = t {
    if let Message::Ack(ack) = p.into_parts().1 {
      assert_eq!(ack.payload(), b"app-data");
    } else {
      panic!("expected Ack message");
    }
  } else {
    panic!("expected Ack");
  }
}

// ─────────────── start_probe ─────────────────────────────────────────────

#[test]
fn start_probe_returns_false_when_only_local() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert!(!e.start_probe(Instant::now()), "no eligible target");
  assert!(e.poll_transmit().is_none());
}

#[test]
fn start_probe_emits_ping_to_alive_peer() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  assert!(e.start_probe(Instant::now()));
  let t = e.poll_transmit().expect("Ping expected");
  match t {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      assert_eq!(to.port(), 7001);
      match message {
        Message::Ping(p) => {
          assert_eq!(p.target_ref().id_ref(), &SmolStr::new("bob"));
          assert!(p.sequence_number() > 0);
        }
        other => panic!("expected Ping, got {other:?}"),
      }
    }
    Transmit::Compound(c) => panic!("unexpected Compound transmit: {c:?}"),
  }
}

#[test]
fn start_probe_skips_dead_peers() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  e.process_dead(dead("bob", "carol", 1), Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  assert!(!e.start_probe(Instant::now()), "bob is dead");
}

#[test]
fn start_probe_round_robins_across_alive_peers() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let mut targets = std::collections::HashSet::new();
  for _ in 0..4 {
    assert!(e.start_probe(Instant::now()));
    if let Some(Transmit::Packet(p)) = e.poll_transmit() {
      targets.insert(p.to_ref().port());
    }
  }
  assert!(targets.contains(&7001));
  assert!(targets.contains(&7002));
  assert!(targets.contains(&7003));
}

// ─────────────── handle_ack ──────────────────────────────────────────────

#[test]
fn handle_ack_completes_direct_probe_and_ticks_awareness() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  // Apply some awareness so the -1 is observable.
  e.awareness.record_failure(2);
  let initial_score = e.health_score();
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = if let Some(Transmit::Packet(p)) = e.poll_transmit() {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.sequence_number()
    } else {
      panic!("expected Ping message");
    }
  } else {
    panic!("expected Ping");
  };

  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.handle_ack(from, Ack::new(seq), t0 + Duration::from_millis(10));

  assert!(!e.probes.contains_key(&seq));
  assert!(e.health_score() < initial_score);
}

/// A direct Ack that arrives AFTER the direct sub-timeout but BEFORE the
/// cumulative deadline (and before the late handle_timeout fires) must
/// SUCCEED the probe — a direct Ack is delivered any time before the
/// overall deadline. It must NOT be suspected based on packet-vs-timer
/// ordering. The cumulative-deadline authority is still enforced: an Ack
/// past the cumulative deadline does terminate as failure.
#[test]
fn direct_ack_after_direct_timeout_within_cumulative_succeeds() {
  let pt = Duration::from_millis(50);
  // 4-node cluster: a timer-driven escalation WOULD fan out to indirect
  // peers — proving the outcome must not depend on packet-vs-timer order.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_probe_timeout(pt));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let (seq, target_addr, target_id) = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        (
          ping.sequence_number(),
          to,
          ping.target_ref().id_ref().cheap_clone(),
        )
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Direct deadline is t0+50ms; cumulative is t0+100ms. The Ack lands at
  // t0+70ms — past the direct sub-timeout, within the cumulative window —
  // and NO handle_timeout has fired yet (packet processed first).
  e.handle_ack(target_addr, Ack::new(seq), t0 + Duration::from_millis(70));

  // The node answered within the overall window ⇒ success: probe gone,
  // target still Alive, and no escalation traffic was emitted.
  assert!(
    !e.probes.contains_key(&seq),
    "probe must complete on the late-but-in-window Ack"
  );
  assert_eq!(
    e.member_liveness(&target_id),
    Some(State::Alive),
    "a direct Ack within the cumulative window must NOT suspect the target"
  );
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::DialRequested(..))),
    "an Ack proves liveness — no reliable-fallback escalation"
  );
  assert!(
    !core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::IndirectPing(_))
    )),
    "an Ack proves liveness — no indirect fan-out"
  );

  // A fresh probe whose Ack arrives PAST the cumulative deadline is
  // terminal failure (target suspected), not a rescue.
  let t1 = t0 + Duration::from_secs(10);
  e.start_probe(t1);
  // Capture the SECOND probe's own target address — start_probe
  // round-robins, so seq2 targets a different node than seq. The Ack
  // must come from seq2's actual target or responder validation (correctly)
  // drops it before the cutoff is even consulted.
  let (seq2, target2_addr) = match e.poll_transmit().expect("direct Ping 2") {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        (ping.sequence_number(), to)
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}
  // Way past t1 + 2*pt.
  e.handle_ack(target2_addr, Ack::new(seq2), t1 + Duration::from_secs(5));
  assert!(
    !e.probes.contains_key(&seq2),
    "probe past the cumulative deadline is terminated, not rescued"
  );
}

/// A periodic Detection probe must emit `Event::PingCompleted` on a DIRECT
/// Ack from the probe target even when the Ack arrives LATE — after the
/// direct sub-timeout (so `handle_timeout` has already moved the probe to
/// `AwaitingIndirect` and fanned out indirect pings), but still before the
/// cumulative failure deadline. Completion is keyed on the Ack SOURCE
/// (`ack.from == probe.target.address`), not the probe phase, so a direct
/// target Ack notifies completion in any phase. The contrast cases —
/// an indirect-relayed Ack (different source) and a reliable-fallback
/// success — do not emit `PingCompleted`.
#[test]
fn detection_late_direct_ack_in_awaiting_indirect_emits_ping_completed() {
  let pt = Duration::from_millis(50);
  // 4-node cluster so the direct-timeout escalation has real indirect peers
  // to fan out to — the probe genuinely enters `AwaitingIndirect`.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_probe_timeout(pt));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Trigger the periodic Detection probe and capture its seq + the target it
  // round-robined onto from the emitted direct Ping.
  let t0 = Instant::now();
  e.start_probe(t0);
  let (seq, target_addr, target_id) = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        (
          ping.sequence_number(),
          to,
          ping.target_ref().id_ref().cheap_clone(),
        )
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Direct sub-timeout is t0+50ms; the cumulative deadline is t0+1s
  // (sent + scaled probe_interval, default 1s, health 0 ⇒ identity). Fire
  // handle_timeout at t0+60ms: past the direct sub-timeout, far short of the
  // cumulative deadline → the probe transitions to AwaitingIndirect and fans
  // out IndirectPings.
  e.handle_timeout(t0 + Duration::from_millis(60));

  // Confirm the probe is genuinely in AwaitingIndirect (NOT AwaitingDirectAck)
  // when the Ack lands.
  match &e.probes.get(&seq).expect("probe still in flight").phase {
    ProbePhase::AwaitingIndirect(_) => {}
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
  // And that escalation traffic (indirect fan-out) was actually emitted.
  assert!(
    core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::IndirectPing(_))
    )),
    "the direct timeout must fan out IndirectPings",
  );
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // The target itself answers LATE (t0+70ms): a DIRECT Ack (its own source
  // address) while the probe sits in AwaitingIndirect, still inside the
  // cumulative window.
  e.handle_ack(
    target_addr,
    Ack::new(seq).with_payload(Bytes::from_static(b"pong")),
    t0 + Duration::from_millis(70),
  );

  // The probe completes, the target stays Alive, and — because the Ack came
  // direct from the target — PingCompleted IS emitted carrying that target,
  // a positive RTT (~70ms since sent_at), and the ack payload. A phase-keyed
  // success check (AwaitingDirectAck only) would suppress this event.
  assert!(
    !e.probes.contains_key(&seq),
    "the late direct Ack completes the probe"
  );
  assert_eq!(
    e.member_liveness(&target_id),
    Some(State::Alive),
    "a direct Ack within the cumulative window must NOT suspect the target",
  );
  let completed = core::iter::from_fn(|| e.poll_event())
    .find_map(|ev| match ev {
      Event::PingCompleted(c) => Some(c),
      _ => None,
    })
    .expect("a late direct target Ack in AwaitingIndirect must emit PingCompleted");
  assert_eq!(
    completed.node_ref().id_ref(),
    &target_id,
    "PingCompleted carries the probe target",
  );
  assert_eq!(completed.ping_id(), PingId::new(seq));
  assert!(
    completed.rtt() >= Duration::from_millis(60),
    "RTT is measured from the probe's sent_at",
  );
  assert_eq!(completed.payload_ref().as_ref(), b"pong");
}

#[test]
fn handle_ack_for_unknown_seq_is_noop() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.handle_ack(from, Ack::new(99999), Instant::now());
  assert!(e.poll_transmit().is_none());
}

/// ProbeKind::Ping is direct-only and its sole timeout is `probe_timeout`.
/// Its success cutoff in complete_probe_success must be
/// `sent_at + probe_timeout`, NOT the Detection cumulative
/// `sent_at + 2*probe_timeout`. A Ping Ack arriving after probe_timeout
/// but before handle_timeout must NOT emit PingCompleted (the caller
/// already saw the timeout); a Detection direct Ack at the same relative
/// time still succeeds (cumulative window).
#[test]
fn app_ping_ack_after_probe_timeout_does_not_complete() {
  let pt = Duration::from_millis(50);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_probe_timeout(pt));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let bob_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.ping(Node::new(SmolStr::new("bob"), bob_addr), t0)
    .expect("issued while running");
  let seq = match e.poll_transmit().expect("Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Ack lands at t0 + pt + 10ms — past the app-ping (direct) deadline,
  // and BEFORE any handle_timeout. For a Ping the cutoff is sent_at+pt,
  // so this is too late: probe_terminate_failure is called, emitting
  // PingFailed. No PingCompleted must be emitted.
  let ping_id = PingId::new(seq);
  e.handle_ack(bob_addr, Ack::new(seq), t0 + pt + Duration::from_millis(10));
  assert!(!e.probes.contains_key(&seq), "ping probe must be gone");
  let events: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(
    !events
      .iter()
      .any(|ev| matches!(ev, Event::PingCompleted(..))),
    "an app ping Ack after probe_timeout must NOT emit a late PingCompleted"
  );
  assert!(
    events
      .iter()
      .any(|ev| matches!(ev, Event::PingFailed(f) if f.ping_id() == ping_id)),
    "a late-Ack past the cutoff must emit PingFailed (probe_terminate_failure)"
  );

  // Contrast: a Detection probe direct Ack at the same relative offset is
  // still within its cumulative window → success.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  let t1 = t0 + Duration::from_secs(10);
  e.start_probe(t1);
  let dseq = match e.poll_transmit().expect("Detection Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}
  e.handle_ack(
    bob_addr,
    Ack::new(dseq),
    t1 + pt + Duration::from_millis(10),
  );
  assert!(!e.probes.contains_key(&dseq), "detection probe completes");
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Alive),
    "a Detection direct Ack within the cumulative window succeeds"
  );
}

// ─── Nack accounting — deadline-bound, allowlisted, deduped ─────────────
//
// A Nack carries only the sequence number; the responder identity is its
// transport source address. A duplicate / unsolicited / late Nack must NOT
// inflate `nacks_seen`: blindly incrementing on every matching seq lets
// `expected_nacks.saturating_sub(seen)` underflow to 0, silently
// suppressing the Lifeguard health penalty for a genuinely-unanswered
// indirect probe.

fn nacked_by_addrs(e: &Endpoint<SmolStr, SocketAddr>, seq: u32) -> SmallVec<SocketAddr> {
  match &e.probes.get(&seq).expect("probe present").phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect { nacked_by, .. }) => nacked_by.clone(),
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
}

fn indirect_probe_at(
  e: &mut Endpoint<SmolStr, SocketAddr>,
  seq: u32,
  sent_at: Instant,
  failure_deadline: Instant,
  indirect_peers: SmallVec<SocketAddr>,
) {
  let (target, target_incarnation, target_generation) = {
    let m = e.members.get(&SmolStr::new("bob")).unwrap();
    (
      m.state_ref().server_arc(),
      m.state_ref().incarnation(),
      m.generation(),
    )
  };
  let expected_nacks = indirect_peers.len();
  e.probes.insert(
    seq,
    Probe {
      target,
      target_incarnation,
      target_generation,
      sent_at,
      kind: ProbeKind::Detection,
      // A probe reaching AwaitingIndirect has already dispatched its direct Ping.
      dispatched: true,
      // For Detection in AwaitingIndirect the failure deadline IS the
      // phase (cumulative) deadline, by the single-source invariant.
      failure_deadline,
      phase: ProbePhase::AwaitingIndirect(AwaitingIndirect {
        expected_nacks,
        indirect_peers,
        nacked_by: SmallVec::new(),
        reliable_stream_id: None,
        deadline: failure_deadline,
      }),
    },
  );
}

#[test]
fn handle_nack_counts_distinct_indirect_responders() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let p2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7102);
  let t0 = Instant::now();
  indirect_probe_at(
    &mut e,
    7,
    t0,
    t0 + Duration::from_secs(1),
    SmallVec::from_iter([p1, p2]),
  );

  e.handle_nack(p1, Nack::new(7), t0 + Duration::from_millis(10));
  e.handle_nack(p2, Nack::new(7), t0 + Duration::from_millis(20));

  let seen = nacked_by_addrs(&e, 7);
  assert_eq!(seen.len(), 2, "two distinct indirect peers nacked");
  assert!(seen.contains(&p1) && seen.contains(&p2));
}

#[test]
fn handle_nack_dedupes_repeated_nack_from_same_responder() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let p2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7102);
  let t0 = Instant::now();
  indirect_probe_at(
    &mut e,
    7,
    t0,
    t0 + Duration::from_secs(1),
    SmallVec::from_iter([p1, p2]),
  );

  for _ in 0..5 {
    e.handle_nack(p1, Nack::new(7), t0 + Duration::from_millis(10));
  }

  let seen = nacked_by_addrs(&e, 7);
  assert_eq!(seen.len(), 1, "a Nack flood from one peer counts ONCE");
  assert_eq!(seen.as_slice(), &[p1]);
}

#[test]
fn handle_nack_rejects_nack_from_unsolicited_peer() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let off_path = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7999);
  let t0 = Instant::now();
  indirect_probe_at(
    &mut e,
    7,
    t0,
    t0 + Duration::from_secs(1),
    SmallVec::from_iter([p1]),
  );

  // A node we never IndirectPing'd guesses the seq and Nacks.
  e.handle_nack(off_path, Nack::new(7), t0 + Duration::from_millis(10));

  assert!(
    nacked_by_addrs(&e, 7).is_empty(),
    "a Nack from a peer not in the IndirectPing allowlist is ignored"
  );
}

#[test]
fn handle_nack_rejects_late_nack_after_failure_deadline() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let t0 = Instant::now();
  let deadline = t0 + Duration::from_secs(1);
  indirect_probe_at(&mut e, 7, t0, deadline, SmallVec::from_iter([p1]));

  // Exactly at the failure deadline is already too late (symmetric to the
  // ack cutoff): the probe terminates at this instant.
  e.handle_nack(p1, Nack::new(7), deadline);
  assert!(
    nacked_by_addrs(&e, 7).is_empty(),
    "a Nack at/after failure_deadline cannot influence the outcome"
  );

  // And strictly after.
  e.handle_nack(p1, Nack::new(7), deadline + Duration::from_millis(1));
  assert!(nacked_by_addrs(&e, 7).is_empty());
}

#[test]
fn nack_flood_from_one_peer_does_not_suppress_awareness_penalty() {
  // expected_nacks = 2 (two indirect peers). Only p1 ever responds, and it
  // floods 5 Nacks. Deduped seen = 1, so the probe is still
  // under-answered: severity = expected(2) - seen(1) = 1. The OLD code
  // would have seen = 5 → 2.saturating_sub(5) = 0 → NO awareness penalty,
  // i.e. a single misbehaving/duplicating peer could mask a real failure.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let p2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7102);
  let t0 = Instant::now();
  let deadline = t0 + Duration::from_secs(1);
  indirect_probe_at(&mut e, 7, t0, deadline, SmallVec::from_iter([p1, p2]));
  let initial_score = e.health_score();

  for _ in 0..5 {
    e.handle_nack(p1, Nack::new(7), t0 + Duration::from_millis(10));
  }

  // Cumulative deadline elapses → probe_terminate_failure (Detection).
  e.handle_timeout(deadline + Duration::from_millis(1));
  assert!(
    !e.probes.contains_key(&7),
    "probe terminated at the deadline"
  );
  assert_eq!(
    e.health_score(),
    initial_score + 1,
    "deduped severity = expected(2) - seen(1) = +1; a one-peer Nack \
     flood must NOT zero out the Lifeguard health penalty"
  );
}

// ─── Indirect fan-out samples DISTINCT transport addresses ────────────────
//
// memberlist-core `kRandomNodes` (util.go) picks up to `indirect_checks`
// helpers, excluding self, the target, and non-Alive members, and dedups the
// sample by node Name — its unique key, which upstream is one-per-address. This
// Sans-I/O port admits distinct ids at one address, so helper selection samples
// DISTINCT ADDRESSES: the IndirectPing body is `local + target`
// (helper-id-independent) and the relay / Nack accounting keys responders by
// address. Sampling by address preserves the configured fan-out width and keeps
// an id that merely aliases the local / target ADDRESS out of the pool.

/// Read `(expected_nacks, indirect_peers)` from a probe now in `AwaitingIndirect`.
fn awaiting_indirect(e: &Endpoint<SmolStr, SocketAddr>, seq: u32) -> (usize, SmallVec<SocketAddr>) {
  match &e.probes.get(&seq).expect("probe present").phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      indirect_peers,
      ..
    }) => (*expected_nacks, indirect_peers.clone()),
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
}

/// Insert an `AwaitingDirectAck` probe targeting an existing Alive member, so a
/// direct `probe_fan_out_indirect` call exercises helper selection against the
/// current membership. The phase is overwritten by the fan-out; `AwaitingDirectAck`
/// is simply the realistic pre-escalation phase.
fn insert_direct_probe(
  e: &mut Endpoint<SmolStr, SocketAddr>,
  seq: u32,
  target_id: &str,
  sent_at: Instant,
  failure_deadline: Instant,
) {
  let (target, target_incarnation, target_generation) = {
    let m = e
      .members
      .get(&SmolStr::new(target_id))
      .expect("target must be an existing member");
    (
      m.state_ref().server_arc(),
      m.state_ref().incarnation(),
      m.generation(),
    )
  };
  e.probes.insert(
    seq,
    Probe {
      target,
      target_incarnation,
      target_generation,
      sent_at,
      kind: ProbeKind::Detection,
      // Simulate a normally-dispatched probe (its direct Ping was sent), so the
      // failure path exercises suspicion rather than the no-dispatch abort.
      dispatched: true,
      failure_deadline,
      phase: ProbePhase::AwaitingDirectAck(AwaitingDirectAck { deadline: sent_at }),
    },
  );
}

/// Drain the transmit queue, returning the destination address of every queued
/// `IndirectPing`.
fn indirect_ping_dests(e: &mut Endpoint<SmolStr, SocketAddr>) -> Vec<SocketAddr> {
  let mut dests = Vec::new();
  while let Some(tx) = e.poll_transmit() {
    if let Transmit::Packet(p) = tx {
      if matches!(p.message_ref(), Message::IndirectPing(_)) {
        dests.push(*p.to_ref());
      }
    }
  }
  dests
}

/// Construct an endpoint with a fixed-seed gossip RNG, so helper sampling is
/// reproducible.
fn seeded_endpoint(
  cfg: EndpointOptions<SmolStr, SocketAddr>,
  seed: u64,
) -> Endpoint<SmolStr, SocketAddr> {
  use rand::SeedableRng;
  Endpoint::new(cfg, rand::rngs::SmallRng::seed_from_u64(seed))
}

#[test]
fn indirect_fan_out_samples_distinct_addresses_preserving_width() {
  // h1 and h2 share address A; h3 is at a distinct address B. With
  // indirect_checks = 2 the fan-out must reach the two DISTINCT addresses
  // {A, B} — width 2 — not collapse to a single relay because two sampled ids
  // happened to share A. (Sampling by id first samples 2 of {h1,h2,h3}; drawing
  // {h1,h2} then deduped to {A}, silently a 1-helper fan-out.)
  let a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5001);
  let b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5002);
  // Seed pinned so the sample is reproducible; the ID-first predecessor draws
  // both same-address ids here (collapsing to width 1), while address-distinct
  // sampling always reaches {A, B}.
  let mut e = seeded_endpoint(cfg().with_indirect_checks(2), 1);
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("h1", 5001, 1), false, t0); // @A
  process_alive_auto(&mut e, alive("h2", 5001, 1), false, t0); // @A (aliases h1)
  process_alive_auto(&mut e, alive("h3", 5002, 1), false, t0); // @B
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0); // target @T (distinct)
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Drop the concurrent reliable fallback so the only escalation traffic is the
  // IndirectPings under test.
  e.disable_reliable_ping(SmolStr::new("bob"));
  insert_direct_probe(&mut e, 9, "bob", t0, t0 + Duration::from_secs(3600));
  e.probe_fan_out_indirect(9, t0 + Duration::from_millis(1));

  let (expected_nacks, indirect_peers) = awaiting_indirect(&e, 9);
  assert_eq!(
    indirect_peers.len(),
    2,
    "the fan-out width must be the two DISTINCT addresses, not collapsed to one"
  );
  assert!(
    indirect_peers.contains(&a) && indirect_peers.contains(&b),
    "both distinct helper addresses A and B must be reached"
  );
  assert_eq!(
    expected_nacks, 2,
    "expected_nacks == the number of distinct helper addresses reached"
  );

  let dests = indirect_ping_dests(&mut e);
  assert_eq!(
    dests.len(),
    2,
    "exactly one IndirectPing per distinct address"
  );
  assert!(
    dests.contains(&a) && dests.contains(&b),
    "one IndirectPing queued to each of A and B"
  );
}

#[test]
fn indirect_fan_out_never_selects_local_or_target_address_alias() {
  // Two aliases with DISTINCT ids that a by-id exclusion would let through:
  // `talias` advertises the TARGET's address T, `lalias` advertises the LOCAL
  // advertise address L. Neither may be a helper — an IndirectPing to T is a
  // self-relay to the probed endpoint (whose address-keyed Nack would be counted
  // for a peer that by definition cannot answer), and one to L is a relay to
  // ourselves. Only the honest helper at H is eligible.
  //
  // indirect_checks is high enough to sample the ENTIRE eligible pool, so a
  // by-id predecessor (excluding only the local/target IDS) would select both
  // aliases; the address exclusion is what keeps T and L out.
  let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000);
  let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let helper = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7003);
  let mut e = seeded_endpoint(cfg().with_indirect_checks(5), 0x0BAD_A11A_5C0F_FEE2);
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0); // target @T
  process_alive_auto(&mut e, alive("talias", 7001, 1), false, t0); // distinct id @T
  process_alive_auto(&mut e, alive("lalias", 7000, 1), false, t0); // distinct id @L (== local)
  process_alive_auto(&mut e, alive("helper", 7003, 1), false, t0); // honest helper @H
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  e.disable_reliable_ping(SmolStr::new("bob"));
  insert_direct_probe(&mut e, 11, "bob", t0, t0 + Duration::from_secs(3600));
  e.probe_fan_out_indirect(11, t0 + Duration::from_millis(1));

  let (_expected_nacks, indirect_peers) = awaiting_indirect(&e, 11);
  assert!(
    !indirect_peers.contains(&target),
    "an id aliasing the TARGET's address must never be a helper"
  );
  assert!(
    !indirect_peers.contains(&local),
    "an id aliasing the LOCAL advertise address must never be a helper"
  );
  assert!(
    indirect_peers.contains(&helper),
    "the honest helper at a distinct address IS selected"
  );

  let dests = indirect_ping_dests(&mut e);
  assert!(
    !dests.contains(&target) && !dests.contains(&local),
    "no IndirectPing is ever queued to the target or local address"
  );
  assert!(
    dests.contains(&helper),
    "the honest helper is sent an IndirectPing"
  );
}

#[test]
fn indirect_fan_out_selects_bounded_distinct_helpers_at_scale() {
  // A large membership must still yield a bounded, DISTINCT indirect-helper
  // fan-out. The candidate addresses are scanned once and deduplicated into an
  // `FxHashSet<A>` (O(n)); up to `indirect_checks` DISTINCT addresses are then
  // sampled uniformly (O(distinct)). Exercised here with N = 2000 distinct
  // addresses to confirm the selection stays exactly `indirect_checks` wide and
  // free of duplicates regardless of membership size.
  const N: usize = 2000;
  const K: usize = 3;
  let mut e = seeded_endpoint(cfg().with_indirect_checks(K as u32), 0x51C0_FFEE);
  let t0 = Instant::now();
  // N Alive peers at DISTINCT transport addresses (ports 10000..10000+N), none
  // aliasing the local (7000) or the target (7001) address.
  for i in 0..N {
    let port = 10_000u16 + i as u16;
    process_alive_auto(&mut e, alive(&format!("h{i}"), port, 1), false, t0);
  }
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Drop the concurrent reliable fallback so the phase is deterministically
  // AwaitingIndirect with one expected Nack per selected helper address.
  e.disable_reliable_ping(SmolStr::new("bob"));
  insert_direct_probe(&mut e, 42, "bob", t0, t0 + Duration::from_secs(3600));

  e.probe_fan_out_indirect(42, t0 + Duration::from_millis(1));

  // Fan-out width is exactly `indirect_checks` DISTINCT addresses.
  let (expected_nacks, indirect_peers) = awaiting_indirect(&e, 42);
  assert_eq!(
    indirect_peers.len(),
    K,
    "the fan-out selects exactly indirect_checks distinct helper addresses"
  );
  assert_eq!(
    expected_nacks, K,
    "one expected Nack per distinct helper address"
  );
  for (i, addr) in indirect_peers.iter().enumerate() {
    assert!(
      !indirect_peers[..i].contains(addr),
      "selected helper addresses must be distinct"
    );
  }
}

// ─────────────── handle_indirect_ping ────────────────────────────────────

use crate::{
  delegate::MergeDelegate,
  error::{EndpointInitError, Error, StreamError},
  event::{PushPullKind, PushPullReplyReceived, PushPullRequestReceived, StreamEvent},
  probe::{AwaitingDirectAck, AwaitingIndirect, Probe, ProbeKind, ProbePhase},
  stream::StreamPhase,
  typed::NodeState,
};
use IndirectPing;
use core::{fmt, time::Duration};

fn ind_ping(
  target_id: &str,
  target_port: u16,
  source_id: &str,
  source_port: u16,
  seq: u32,
) -> IndirectPing<SmolStr, SocketAddr> {
  IndirectPing::new(
    seq,
    PNode::new(
      SmolStr::new(source_id),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), source_port),
    ),
    PNode::new(
      SmolStr::new(target_id),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), target_port),
    ),
  )
}

#[test]
fn handle_indirect_ping_forwards_to_target() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // The transport source MUST match the embedded requester address.
  // `ind_ping` builds the source at 127.0.0.1:<source_port>.
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let ind = ind_ping("bob", 7001, "carol", 7002, 5);
  e.handle_indirect_ping(from, ind, Instant::now());
  let t = e.poll_transmit().expect("forwarded Ping expected");
  match t {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      assert_eq!(to.port(), 7001);
      if let Message::Ping(ping) = message {
        assert_eq!(ping.target_ref().id_ref(), &SmolStr::new("bob"));
      } else {
        panic!("expected Ping");
      }
    }
    Transmit::Compound(c) => panic!("unexpected Compound transmit: {c:?}"),
  }
}

#[test]
fn handle_indirect_ping_followed_by_target_ack_relays_ack() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // Transport source must match the embedded requester address.
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let ind = ind_ping("bob", 7001, "carol", 7002, 42);
  e.handle_indirect_ping(from, ind, Instant::now());
  // Capture the forwarded Ping's seq.
  let our_seq = if let Some(Transmit::Packet(p)) = e.poll_transmit() {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.sequence_number()
    } else {
      panic!("expected Ping message");
    }
  } else {
    panic!("expected forwarded Ping");
  };

  // Target acks our forwarded Ping.
  let bob_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.handle_ack(bob_addr, Ack::new(our_seq), Instant::now());

  // We should now have emitted an Ack-relay to carol (the original requester).
  let t = e.poll_transmit().expect("Ack-relay expected");
  match t {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      assert_eq!(to.port(), 7002);
      if let Message::Ack(ack) = message {
        assert_eq!(ack.sequence_number(), 42);
      } else {
        panic!("expected Ack");
      }
    }
    Transmit::Compound(c) => panic!("unexpected Compound transmit: {c:?}"),
  }
}

/// An IndirectPing whose transport source does not match its embedded
/// `source` address is forged (the relay-oracle / reflection vector) and
/// must be dropped entirely — no forwarded Ping, no AckRegistry/forward
/// registration (so no Nack on expiry either).
#[test]
fn forged_indirect_ping_source_is_rejected() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  let t0 = Instant::now();

  // Attacker sends from its own address but claims source = carol@7002.
  let attacker = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 6666);
  let ind = ind_ping("bob", 7001, "carol", 7002, 42);
  e.handle_indirect_ping(attacker, ind, t0);

  assert!(
    e.poll_transmit().is_none(),
    "a forged-source IndirectPing must NOT produce a forwarded Ping"
  );
  // And no forward state was registered → no Nack when the (non-existent)
  // forward's deadline would have elapsed.
  e.handle_timeout(t0 + Duration::from_millis(60));
  assert!(
    !core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::Nack(_))
    )),
    "a forged IndirectPing must not register a forward (no Nack on expiry)"
  );

  // Sanity: the honest case (transport source == embedded source) still
  // forwards a Ping to the target.
  let honest_from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  e.handle_indirect_ping(honest_from, ind_ping("bob", 7001, "carol", 7002, 43), t0);
  assert!(
    core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::Ping(_))
    )),
    "an honest IndirectPing (from == source) still forwards"
  );
}

// ─────────────── handle_timeout probe FSM + forwards ─────────────────────

#[test]
fn probe_direct_timeout_fans_out_to_indirect() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let direct = e.poll_transmit().expect("direct Ping expected");
  let _seq = if let Transmit::Packet(p) = direct {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.sequence_number()
    } else {
      panic!("Ping message expected")
    }
  } else {
    panic!("Ping expected")
  };
  while e.poll_transmit().is_some() {}

  // Fire timeout after probe_timeout elapses.
  e.handle_timeout(t0 + Duration::from_millis(60));

  // We should now see IndirectPings to up to 3 indirect peers.
  let mut indirect_count = 0;
  while let Some(tx) = e.poll_transmit() {
    if let Transmit::Packet(p) = tx {
      if matches!(p.message_ref(), Message::IndirectPing(_)) {
        indirect_count += 1;
      }
    }
  }
  assert!(indirect_count > 0, "expected IndirectPings on timeout");
}

/// On direct-ack timeout the reliable fallback is opened CONCURRENTLY
/// with the indirect fan-out (same transition, same single cumulative
/// deadline) — not as a later serialized phase with its own extra stream
/// timeout.
#[test]
fn probe_arms_reliable_fallback_concurrently_with_indirect() {
  // probe_interval (80ms) is deliberately NOT 2*probe_timeout (100ms) so
  // the test pins the awareness-scaled probe_interval as the cumulative
  // deadline, not a probe_timeout coincidence.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80)),
  );
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Direct deadline elapses in a SINGLE handle_timeout call.
  e.handle_timeout(t0 + Duration::from_millis(60));

  // The very same transition both armed the concurrent reliable fallback
  // and entered AwaitingIndirect with one cumulative deadline.
  let probe = e.probes.get(&seq).expect("probe still in flight");
  match &probe.phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      reliable_stream_id,
      deadline,
      ..
    }) => {
      assert!(
        reliable_stream_id.is_some(),
        "reliable fallback must be opened concurrently at indirect fan-out"
      );
      // The cumulative deadline is `sent + awareness.scale_timeout(
      // probe_interval)`, snapshotted at probe start (t0, health 0 →
      // scale is identity), NOT 2*probe_timeout and NOT the t0+60ms
      // handle_timeout callback.
      assert_eq!(
        *deadline,
        t0 + e.cfg.probe_interval(),
        "cumulative deadline = sent + scaled probe_interval, anchored at start"
      );
    }
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
  // A DialRequested for that reliable-ping stream was emitted in the same step.
  assert!(
    core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::DialRequested(..))),
    "concurrent reliable ping must emit DialRequested at fan-out time"
  );
}

/// A LATE `handle_timeout` (overloaded runtime firing well past the
/// anchored cumulative deadline) must NOT hand the probe a fresh window.
/// The deadline is anchored to the probe start (sent_at + 2*probe_timeout);
/// when it is already past at fan-out time the probe is terminated
/// IMMEDIATELY in the same `handle_timeout` call — no AwaitingIndirect
/// phase, no indirect pings, no reliable fallback DialRequested — so no
/// late relayed/fallback Ack can rescue it past its failure deadline.
#[test]
fn late_handle_timeout_does_not_extend_probe_window() {
  let pt = Duration::from_millis(50);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_probe_timeout(pt));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Anchored cumulative deadline is t0 + 2*pt = t0+100ms; the runtime is
  // overloaded and only calls handle_timeout at t0+5s — 50x past it.
  let late = t0 + Duration::from_secs(5);
  e.handle_timeout(late);

  // The probe is terminated in THIS call — not parked in AwaitingIndirect
  // for a later tick — so there is no window for a queued indirect/
  // fallback Ack to rescue it past the failure deadline.
  assert!(
    !e.probes.contains_key(&seq),
    "a late handle_timeout must terminate the expired probe immediately"
  );
  // No reliable-fallback DialRequested and no IndirectPing were emitted.
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::DialRequested(..))),
    "an already-expired probe must NOT open the reliable fallback"
  );
  assert!(
    !core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::IndirectPing(_))
    )),
    "an already-expired probe must NOT fan out indirect pings"
  );
  // Target was suspected promptly despite the overloaded runtime.
  assert!(
    matches!(
      e.member_liveness(&SmolStr::new("bob")),
      Some(State::Suspect | State::Dead)
    ) || matches!(
      e.member_liveness(&SmolStr::new("carol")),
      Some(State::Suspect | State::Dead)
    ) || matches!(
      e.member_liveness(&SmolStr::new("dave")),
      Some(State::Suspect | State::Dead)
    ),
    "the probe target must be suspected immediately, not rescued later"
  );
}

#[test]
fn probe_indirect_timeout_marks_target_suspect() {
  // Cumulative failure deadline = sent + scaled probe_interval (80ms here,
  // != 2*probe_timeout). Direct sub-window = probe_timeout (50ms).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80))
      .with_stream_timeout(Duration::from_millis(50))
      .with_indirect_checks(2),
  );
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);

  // Capture the actual probe target from the emitted Ping.
  let target_id = if let Some(Transmit::Packet(p)) = e.poll_transmit() {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.target_ref().id_ref().cheap_clone()
    } else {
      panic!("expected Ping message");
    }
  } else {
    panic!("expected Ping");
  };
  while e.poll_transmit().is_some() {}

  // Direct deadline (probe_timeout=50ms) elapses → AwaitingIndirect with
  // the reliable fallback opened CONCURRENTLY (single cumulative deadline
  // = sent + scaled probe_interval = t0+80ms).
  e.handle_timeout(t0 + Duration::from_millis(60));
  while e.poll_event().is_some() {} // drain DialRequested (concurrent reliable ping)
  while e.poll_transmit().is_some() {} // drain IndirectPings
  // The single cumulative AwaitingIndirect deadline (t0+80ms) elapses →
  // probe_terminate_failure → target Suspect. No further phase.
  e.handle_timeout(t0 + Duration::from_millis(185));

  // The captured target — and ONLY that target — should now be Suspect (or Dead
  // if the suspicion timer's min < 60ms; in this test the default suspicion
  // mult * probe_interval * log(N+1) is comfortably more than 60ms).
  let target_member = e.members.get(&target_id).expect("target present");
  assert!(
    matches!(
      target_member.state_ref().state(),
      State::Suspect | State::Dead
    ),
    "expected target {:?} to be Suspect or Dead, got {:?}",
    target_id,
    target_member.state_ref().state()
  );
}

#[test]
fn indirect_forward_timeout_emits_nack() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let t0 = Instant::now();
  e.handle_indirect_ping(from, ind_ping("bob", 7001, "carol", 7002, 42), t0);
  while e.poll_transmit().is_some() {}

  // bob never acks; advance past the forward deadline.
  e.handle_timeout(t0 + Duration::from_millis(60));

  let t = e.poll_transmit().expect("Nack expected");
  if let Transmit::Packet(p) = t {
    let (to, message) = p.into_parts();
    if let Message::Nack(n) = message {
      assert_eq!(to.port(), 7002);
      assert_eq!(n.sequence_number(), 42);
    } else {
      panic!("expected Nack message");
    }
  } else {
    panic!("expected Nack to carol with seq=42");
  }
}

/// The indirect-forward timeout Nack must reach the requester even when
/// its id is NOT in our local membership (asymmetric membership). Resolving
/// the requester id via `members.get` and silently dropping the Nack when
/// absent corrupts the requester's `expected_nacks - seen` Lifeguard
/// accounting. The Nack goes to the validated transport source address
/// (same as the relay-Ack path).
#[test]
fn forward_timeout_nack_reaches_requester_absent_from_membership() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  // "carol" (the requester) is deliberately NOT added to members.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Honest IndirectPing: transport source == embedded source (validated).
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let t0 = Instant::now();
  e.handle_indirect_ping(from, ind_ping("bob", 7001, "carol", 7002, 42), t0);
  while e.poll_transmit().is_some() {}

  e.handle_timeout(t0 + Duration::from_millis(60)); // bob never acks
  let t = e
    .poll_transmit()
    .expect("Nack must be emitted even though the requester is unknown");
  match t {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Nack(n) = message {
        assert_eq!(to, from, "Nack goes to the validated transport source");
        assert_eq!(n.sequence_number(), 42);
      } else {
        panic!("expected Nack message");
      }
    }
    _ => panic!("expected Nack to the validated source seq=42"),
  }
}

/// A STALE local-membership address for the requester id must NOT misroute
/// the timeout Nack. It goes to the validated transport source from the
/// IndirectPing, not the stale members address.
#[test]
fn forward_timeout_nack_ignores_stale_membership_address() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  // "carol" is in members but at a STALE address (port 9999).
  process_alive_auto(&mut e, alive("carol", 9999, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // carol's CURRENT (validated) source is port 7002, != the stale 9999.
  let validated = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let stale = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9999);
  let t0 = Instant::now();
  e.handle_indirect_ping(validated, ind_ping("bob", 7001, "carol", 7002, 42), t0);
  while e.poll_transmit().is_some() {}

  e.handle_timeout(t0 + Duration::from_millis(60));
  let t = e.poll_transmit().expect("Nack expected");
  match t {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      assert!(matches!(message, Message::Nack(_)), "expected Nack message");
      assert_eq!(
        to, validated,
        "Nack goes to the validated source, not the stale members addr"
      );
      assert_ne!(to, stale, "must NOT use the stale local-membership address");
    }
    _ => panic!("expected Nack"),
  }
}

/// A Forward Ack arriving at/after the indirect-forward deadline must NOT
/// be relayed (drivers process packets before `handle_timeout`, so a
/// too-late Ack could otherwise suppress the Nack `fire_expired_forwards`
/// is about to send and falsely complete the requester's indirect probe of
/// a dead target). The forward record is left intact so the subsequent
/// timeout still Nacks normally.
#[test]
fn forward_ack_after_deadline_is_not_relayed_and_nack_still_fires() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let carol = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let bob = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  // Forward deadline = t0 + probe_timeout = t0 + 50ms.
  e.handle_indirect_ping(carol, ind_ping("bob", 7001, "carol", 7002, 42), t0);
  let our_seq = if let Some(Transmit::Packet(p)) = e.poll_transmit() {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.sequence_number()
    } else {
      panic!("expected Ping message");
    }
  } else {
    panic!("expected forwarded Ping");
  };
  while e.poll_transmit().is_some() {}

  // bob's Ack arrives EXACTLY at the forward deadline, processed BEFORE
  // handle_timeout (the packet-vs-timer order the live drivers use).
  let deadline = t0 + Duration::from_millis(50);
  e.handle_ack(bob, Ack::new(our_seq), deadline);
  assert!(
    !core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::Ack(_))
    )),
    "a Forward Ack at/after the forward deadline must NOT be relayed"
  );

  // The forward record was left intact → the timer path still Nacks.
  e.handle_timeout(deadline);
  let t = e.poll_transmit().expect("Nack expected after the deadline");
  match t {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Nack(n) = message {
        assert_eq!(to.port(), 7002, "Nack goes to the original requester");
        assert_eq!(n.sequence_number(), 42, "Nack carries the requester seq");
      } else {
        panic!("expected Nack message");
      }
    }
    _ => panic!("expected Nack to carol seq=42"),
  }
}

/// Sibling sanity: a Forward Ack strictly BEFORE the deadline still
/// relays (the deadline is a cutoff, not a clamp on the happy path).
#[test]
fn forward_ack_before_deadline_still_relays() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  let carol = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let bob = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  e.handle_indirect_ping(carol, ind_ping("bob", 7001, "carol", 7002, 42), t0);
  let our_seq = if let Some(Transmit::Packet(p)) = e.poll_transmit() {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.sequence_number()
    } else {
      panic!("expected Ping message");
    }
  } else {
    panic!("expected forwarded Ping");
  };
  while e.poll_transmit().is_some() {}

  // 1ms before the 50ms forward deadline.
  e.handle_ack(bob, Ack::new(our_seq), t0 + Duration::from_millis(49));
  let relayed = core::iter::from_fn(|| e.poll_transmit())
    .find(|tx| matches!(tx, Transmit::Packet(p) if matches!(p.message_ref(), Message::Ack(_))))
    .expect("a within-deadline Forward Ack must still relay");
  match relayed {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Ack(a) = message {
        assert_eq!(to.port(), 7002);
        assert_eq!(a.sequence_number(), 42);
      } else {
        panic!("expected Ack message");
      }
    }
    _ => unreachable!(),
  }
}

// ─────────────── Application-level ping ──────────────────────────────────

#[test]
fn ping_emits_ping_and_records_app_ping_state() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let bob_node = Node::new(
    SmolStr::new("bob"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
  );
  e.ping(bob_node, t0).expect("issued while running");
  let t = e.poll_transmit().expect("Ping expected");
  let seq = if let Transmit::Packet(p) = t {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.sequence_number()
    } else {
      panic!("Ping message expected")
    }
  } else {
    panic!("Ping expected");
  };
  assert!(matches!(
    e.probes.get(&seq).map(|p| p.kind),
    Some(ProbeKind::Ping)
  ));
}

#[test]
fn ping_completes_on_ack_with_pingcompleted_event() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let bob_node = Node::new(
    SmolStr::new("bob"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
  );
  e.ping(bob_node, t0).expect("issued while running");
  let seq = if let Some(Transmit::Packet(p)) = e.poll_transmit() {
    let (_, message) = p.into_parts();
    if let Message::Ping(ping) = message {
      ping.sequence_number()
    } else {
      panic!("Ping message expected")
    }
  } else {
    panic!("Ping expected");
  };

  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.handle_ack(
    from,
    Ack::new(seq).with_payload(Bytes::from_static(b"pong")),
    t0 + Duration::from_millis(20),
  );

  let ev = e.poll_event().expect("PingCompleted expected");
  match ev {
    Event::PingCompleted(p) => {
      assert_eq!(p.node_ref().id_ref(), &SmolStr::new("bob"));
      assert!(p.rtt() >= Duration::from_millis(15));
      assert_eq!(p.payload_ref().as_ref(), b"pong");
    }
    other => panic!("expected PingCompleted, got {other:?}"),
  }
}

/// An application `ping` is direct-only: on timeout it emits `Event::PingFailed`
/// with no indirect fan-out, no reliable-fallback DialRequested, and no
/// `PingCompleted`. The target is not suspected.
#[test]
fn app_ping_timeout_does_not_escalate_to_indirect_or_fallback() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  // Four-node cluster: indirect peers (carol, dave) DO exist — a
  // detection probe would fan out to them; an app ping must not.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let bob_node = Node::new(
    SmolStr::new("bob"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
  );
  let ping_id = e.ping(bob_node, t0).expect("issued while running");
  // Drain the single direct Ping.
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Direct deadline elapses with no Ack.
  e.handle_timeout(t0 + Duration::from_millis(60));

  // The probe is gone — probe_terminate_failure emits PingFailed, NOT escalated.
  assert!(
    !e.probes.contains_key(&seq),
    "an app ping must terminate on direct timeout, not enter AwaitingIndirect"
  );
  let events: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(
    events
      .iter()
      .any(|ev| matches!(ev, Event::PingFailed(f) if f.ping_id() == ping_id)),
    "handle_timeout on app ping must emit PingFailed with the original PingId"
  );
  assert!(
    !events
      .iter()
      .any(|ev| matches!(ev, Event::DialRequested(..) | Event::PingCompleted(..))),
    "no reliable-fallback DialRequested and no late PingCompleted"
  );
  assert!(
    !core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::IndirectPing(_))
    )),
    "an app ping must NOT leak IndirectPing traffic into failure detection"
  );
  // No spurious suspicion of the (merely unresponsive-to-app-ping) target.
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Alive),
    "an app ping timeout must not suspect the target"
  );
}

/// A two-node / zero-indirect-peer probe must STILL attempt the reliable
/// (TCP) fallback before suspecting — it must not short-circuit to failure
/// on the direct timeout. The reliable-fallback ping is spawned even when
/// `expected_nacks == 0`. The +1 Lifeguard awareness penalty for a
/// no-indirect failure still applies, but only once the single cumulative
/// deadline elapses without the fallback making contact.
#[test]
fn probe_failure_with_no_indirect_peers_bumps_awareness_by_one() {
  // Cumulative deadline = sent + scaled probe_interval (80ms, decoupled
  // from 2*probe_timeout=100ms).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80)),
  );
  // Two-node cluster: local + bob. No indirect peers available. Reliable
  // ping is enabled by default.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let initial_score = e.health_score();
  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}

  // Direct ping times out. With zero indirect peers but reliable ping
  // enabled the probe does NOT fail here — it opens the reliable fallback
  // concurrently and races the single cumulative deadline.
  e.handle_timeout(t0 + Duration::from_millis(60));
  assert!(
    core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::DialRequested(..))),
    "a 2-node probe must still attempt the reliable fallback"
  );
  assert_eq!(
    e.health_score(),
    initial_score,
    "no suspicion / awareness penalty until the fallback also fails"
  );
  match &e.probes.get(&seq).expect("probe still racing").phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      reliable_stream_id,
      ..
    }) => {
      assert_eq!(*expected_nacks, 0, "no indirect peers");
      assert!(
        reliable_stream_id.is_some(),
        "fallback armed despite 0 indirect"
      );
    }
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }

  // The single cumulative deadline elapses with no ack / no fallback
  // contact → terminate failure, +1 awareness (Lifeguard no-indirect rule).
  e.handle_timeout(t0 + Duration::from_millis(170));
  assert!(
    !e.probes.contains_key(&seq),
    "probe terminated at the deadline"
  );
  assert_eq!(
    e.health_score(),
    initial_score + 1,
    "expected +1 awareness once the no-indirect probe finally fails"
  );
}

/// The concurrent reliable fallback is bounded by the probe's single
/// cumulative deadline (its DialRequested carries that, not
/// `now + stream_timeout`), and `probe_terminate_failure` drops the
/// fallback's still-pending dial intent so a late `dial_succeeded` cannot
/// promote an orphan stream after the probe has failed.
#[test]
fn reliable_fallback_bounded_by_probe_deadline_and_cleaned_on_failure() {
  // probe_interval (80ms) != 2*probe_timeout (100ms): the fallback must be
  // bounded by the scaled probe_interval, not a probe_timeout multiple.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80)),
  );
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Direct timeout → concurrent reliable fallback armed; its DialRequested
  // deadline must be the probe's cumulative deadline = sent + scaled
  // probe_interval (t0 + probe_interval at health 0), anchored at start —
  // NOT now+stream_timeout, NOT 2*probe_timeout, NOT (late-callback)+x.
  e.handle_timeout(t0 + Duration::from_millis(60));
  let cumulative = t0 + e.cfg.probe_interval();
  let (rid, dl) = core::iter::from_fn(|| e.poll_event())
    .find_map(|ev| match ev {
      Event::DialRequested(p) => Some((p.id(), p.deadline())),
      _ => None,
    })
    .expect("reliable-fallback DialRequested");
  assert_eq!(
    dl, cumulative,
    "fallback DialRequested must carry the probe's cumulative deadline"
  );

  // Cumulative deadline elapses → probe_terminate_failure → the pending
  // reliable-ping intent is dropped, so a late dial cannot orphan a stream.
  e.handle_timeout(t0 + Duration::from_millis(170));
  assert!(!e.probes.contains_key(&seq), "probe terminated");
  assert!(
    e.dial_succeeded(rid, t0 + Duration::from_millis(180))
      .is_none(),
    "a late dial for the retired fallback must NOT create an orphan stream"
  );
}

#[test]
fn probe_failure_with_no_nacks_received_bumps_awareness_by_expected() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80))
      .with_stream_timeout(Duration::from_millis(50))
      .with_indirect_checks(2),
  );
  // Four-node cluster: local + bob + carol + dave. Probe bob; carol+dave
  // are eligible indirect peers (expected_nacks=2). Neither nacks → +2.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let initial_score = e.health_score();
  let t0 = Instant::now();
  e.start_probe(t0);
  while e.poll_transmit().is_some() {}

  // 60ms: direct deadline (50ms) elapsed → AwaitingIndirect (fan-out
  // IndirectPings + concurrent reliable fallback; single cumulative
  // deadline = sent + scaled probe_interval = t0+80ms).
  e.handle_timeout(t0 + Duration::from_millis(60));
  while e.poll_event().is_some() {} // drain DialRequested (concurrent reliable ping)
  while e.poll_transmit().is_some() {}
  // 185ms: the single cumulative AwaitingIndirect deadline (t0+80ms) has
  // elapsed → probe_terminate_failure (+2 awareness: expected_nacks=2, seen=0).
  e.handle_timeout(t0 + Duration::from_millis(185));

  // expected_nacks=2, seen_nacks=0 → delta = 2 - 0 = +2.
  assert_eq!(
    e.health_score(),
    initial_score + 2,
    "expected +2 awareness on full-nack-miss"
  );
}

#[test]
fn probe_with_suspect_target_piggybacks_suspect_message() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  // Mark bob as Suspect.
  e.process_suspect(suspect("bob", "carol", 1), Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Force the next probe to target bob by rotating probe_index.
  // (Tests are inherently order-dependent on probe_index; just probe until we hit bob.)
  // A Suspect-target probe co-sends Ping + buddy Suspect as ONE compound
  // datagram (>= 2 rule), so inspect the flattened messages of whichever
  // transmit shape is addressed to bob.
  let mut found_bob_pair = false;
  for _ in 0..5 {
    e.start_probe(Instant::now());
    let mut saw_ping_to_bob = false;
    let mut saw_suspect_about_bob = false;
    while let Some(tx) = e.poll_transmit() {
      let (to, msgs): (SocketAddr, TinyVec<Message<SmolStr, SocketAddr>>) = match tx {
        Transmit::Packet(p) => {
          let (to, message) = p.into_parts();
          (to, TinyVec::from_iter([message]))
        }
        Transmit::Compound(cmp) => cmp.into_parts(),
      };
      if to.port() == 7001 {
        for message in &msgs {
          match message {
            Message::Ping(p) if p.target_ref().id_ref() == &SmolStr::new("bob") => {
              saw_ping_to_bob = true;
            }
            Message::Suspect(s) if s.node_ref() == &SmolStr::new("bob") => {
              saw_suspect_about_bob = true;
            }
            _ => {}
          }
        }
      }
    }
    if saw_ping_to_bob && saw_suspect_about_bob {
      found_bob_pair = true;
      break;
    }
  }
  assert!(
    found_bob_pair,
    "expected Ping+Suspect pair to bob within 5 probe rotations"
  );
}

#[test]
fn stream_scaffolding_construction_smoke() {
  use crate::{
    event::PushPullKind,
    stream::{OutboundKind, Stream, StreamPhase},
  };
  use bytes::BytesMut;
  use core::net::{IpAddr, Ipv4Addr, SocketAddr};
  use std::collections::VecDeque;

  let id = StreamId::from_raw(1);
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);

  let s: Stream<SmolStr, SocketAddr> = Stream {
    id,
    peer,
    local_id: SmolStr::new("local"),
    max_frame_size: 64 * 1024 * 1024,
    phase: StreamPhase::InboundAwaitingFirstMessage,
    input_buf: BytesMut::new(),
    output_buf: VecDeque::new(),
    deadline: None,
    endpoint_events: VecDeque::new(),
    stream_events: VecDeque::new(),
  };

  assert_eq!(s.id().as_u64(), 1);
  assert!(!s.is_done());
  assert!(s.is_failed().is_none());
  assert!(s.poll_timeout().is_none());
  // Suppresses the unused-import warning on PushPullKind.
  let _ = OutboundKind::PushPull(PushPullKind::Join);
}

#[test]
fn stream_id_allocator_increments() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let id1 = e.allocate_stream_id();
  let id2 = e.allocate_stream_id();
  assert_eq!(id2.as_u64(), id1.as_u64() + 1);
}

#[test]
fn start_push_pull_emits_dial_requested_and_dial_succeeded_returns_stream() {
  use Event;
  use PushPullKind;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Join, t0);

  // poll_event should yield DialRequested.
  let ev = e.poll_event().expect("DialRequested expected");
  match ev {
    Event::DialRequested(p) => {
      assert_eq!(p.id(), id);
      assert_eq!(*p.peer_ref(), peer);
    }
    other => panic!("expected DialRequested, got {other:?}"),
  }

  // Simulate dial success.
  let mut stream = e.dial_succeeded(id, t0).expect("stream expected");
  assert_eq!(stream.id(), id);
  assert!(!stream.is_done());

  // Stream's output_buf should contain encoded PushPull bytes.
  let mut buf = Vec::new();
  let n = stream
    .poll_transmit_vec(t0, &mut buf)
    .expect("bytes expected");
  assert!(n > 0, "expected non-empty encoded PushPull");

  // First byte is PUSH_PULL_MESSAGE_TAG = 8.
  assert_eq!(buf[0], 8u8, "first byte must be PUSH_PULL_MESSAGE_TAG");
}

#[test]
fn dial_failed_removes_intent() {
  use crate::{error::StreamError, event::PushPullKind};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);

  e.dial_failed(id, StreamError::DialFailed("refused".into()), t0);
  // Dialing the same id again returns None.
  assert!(e.dial_succeeded(id, t0).is_none());
}

#[test]
fn accept_stream_returns_inbound_stream() {
  use StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let now = Instant::now();
  let s = e.accept_stream(from, now).expect("node is running");
  assert!(!s.is_done());
  assert!(matches!(s.phase, StreamPhase::InboundAwaitingFirstMessage));
  assert!(s.poll_timeout().is_some());
}

#[test]
fn outbound_push_pull_decode_and_merge() {
  use PushNodeState;
  use PushPull;
  use PushPullKind;
  use State;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("alice", 7000, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer_addr, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}

  let mut stream = e.dial_succeeded(id, t0).expect("stream");
  let mut _req_buf = Vec::new();
  // Draining the request via the public API auto-advances the phase to
  // OutboundAwaitingResponse.
  stream.poll_transmit_vec(t0, &mut _req_buf);

  // Simulate peer's PushPull reply — contains "carol" as a member.
  let carol_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7003);
  let carol_state = PushNodeState::new(1, SmolStr::new("carol"), carol_addr, State::Alive);
  let reply_pp = PushPull::new(false, core::iter::once(carol_state)).with_user_data(Bytes::new());
  let reply_msg = Message::<SmolStr, SocketAddr>::PushPull(reply_pp);
  let reply_bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&reply_msg).expect("encode");

  stream
    .handle_data(&reply_bytes, t0)
    .expect("handle_data ok");

  // Stream should be done.
  assert!(stream.is_done());

  // EndpointEvent::PushPullReplyReceived should be queued.
  let ep_ev = stream.poll_endpoint_event().expect("endpoint event");
  match ep_ev {
    EndpointEvent::PushPullReplyReceived(ref p) => {
      assert_eq!(p.states_slice().len(), 1);
      assert_eq!(p.states_slice()[0].id_ref(), &SmolStr::new("carol"));
    }
    ref other => panic!("expected PushPullReplyReceived, got {other:?}"),
  }

  // Route event through Endpoint. This is a Refresh reply; with no
  // MergeDelegate installed the merge is admitted and applies synchronously
  // and inline.
  let cmd = e.handle_stream_event(ep_ev, t0);
  // Outbound reply: no StreamCommand expected.
  assert!(cmd.is_none(), "outbound reply must return None");

  // "carol" lands in members immediately — no decision round-trip.
  assert!(
    e.member(&SmolStr::new("carol")).is_some(),
    "carol must be in members after the synchronous merge"
  );
}

#[test]
fn inbound_push_pull_decode_and_response_bytes() {
  use EndpointEvent;
  use PushNodeState;
  use PushPull;
  use PushPullKind;
  use State;
  use StreamCommand;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("local-node", 7000, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Peer dials us.
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer_addr, t0).expect("node is running");

  // Peer's inbound PushPull: join=true, one peer "dave".
  let dave_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7004);
  let dave_state = PushNodeState::new(1, SmolStr::new("dave"), dave_addr, State::Alive);
  let inbound_pp = PushPull::new(true, core::iter::once(dave_state)).with_user_data(Bytes::new());
  let inbound_msg = Message::<SmolStr, SocketAddr>::PushPull(inbound_pp);
  let inbound_bytes =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&inbound_msg).expect("encode");

  stream.handle_data(&inbound_bytes, t0).expect("handle_data");

  // Stream should now be in InboundSendingResponse.
  assert!(!stream.is_done());

  // EndpointEvent::PushPullRequestReceived from peer.
  let ep_ev = stream.poll_endpoint_event().expect("endpoint event");
  match &ep_ev {
    EndpointEvent::PushPullRequestReceived(p) => {
      assert_eq!(p.states_slice().len(), 1);
      assert_eq!(p.states_slice()[0].id_ref(), &SmolStr::new("dave"));
      assert_eq!(p.kind(), PushPullKind::Join);
    }
    other => panic!("expected PushPullRequestReceived, got {other:?}"),
  }

  // Driver routes through Endpoint → gets StreamCommand::SendPushPullResponse.
  let cmd = e
    .handle_stream_event(ep_ev, t0)
    .expect("expected StreamCommand for inbound stream");

  match cmd {
    StreamCommand::SendPushPullResponse(resp) => {
      // The response is the endpoint's pre-encoded, cached membership frame.
      let encoded = resp.into_encoded();
      assert!(!encoded.is_empty(), "encoded response must be non-empty");
      assert_eq!(encoded[0], 8u8, "first byte must be PUSH_PULL_MESSAGE_TAG");

      Endpoint::<SmolStr, SocketAddr>::stream_load_response(
        &mut stream,
        encoded,
        t0 + Duration::from_secs(5),
      );
      let mut out = Vec::new();
      let n = stream.poll_transmit_vec(t0, &mut out).expect("bytes");
      assert!(n > 0);
    }
    StreamCommand::Close => panic!("expected SendPushPullResponse, got Close"),
  }
}

#[test]
fn post_leave_inbound_alive_is_not_admitted() {
  // The graceful-leave drain must not grow membership. An inbound Alive — from
  // gossip or routed in from a push/pull merge — is dropped once the node has
  // left, so no new peer is admitted while draining.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  let before = e.num_members();
  process_alive_auto(&mut e, alive("intruder", 7009, 1), false, t0);
  assert_eq!(
    e.num_members(),
    before,
    "a left node must not admit a new Alive"
  );
  assert!(e.member(&SmolStr::new("intruder")).is_none());
}

#[test]
fn post_leave_data_setters_reject_with_not_running() {
  use bytes::Bytes;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  assert!(matches!(
    e.set_ack_payload(Bytes::from_static(b"x")),
    Err(Error::NotRunning)
  ));
  assert!(matches!(
    e.set_local_state_snapshot(Bytes::from_static(b"x")),
    Err(Error::NotRunning)
  ));
  assert!(matches!(
    e.queue_user_broadcast(Bytes::from_static(b"x")),
    Err(Error::NotRunning)
  ));
}

#[test]
fn post_leave_push_pull_reply_does_not_merge() {
  use PushNodeState;
  use PushPull;
  use PushPullKind;
  use State;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("alice", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // A join push/pull is initiated while Running...
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let id = e.start_push_pull(peer_addr, PushPullKind::Join, t0);
  while e.poll_event().is_some() {}
  let mut stream = e.dial_succeeded(id, t0).expect("stream");
  let mut req_buf = Vec::new();
  stream.poll_transmit_vec(t0, &mut req_buf);

  // ...then the node leaves before the peer's reply lands. The in-flight reply
  // must not re-establish membership during the drain.
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  let before = e.num_members();

  let carol_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7003);
  let carol_state = PushNodeState::new(1, SmolStr::new("carol"), carol_addr, State::Alive);
  let reply_pp = PushPull::new(true, core::iter::once(carol_state)).with_user_data(Bytes::new());
  let reply_msg = Message::<SmolStr, SocketAddr>::PushPull(reply_pp);
  let reply_bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&reply_msg).expect("encode");
  stream
    .handle_data(&reply_bytes, t0)
    .expect("handle_data ok");
  let ep_ev = stream.poll_endpoint_event().expect("endpoint event");

  let cmd = e.handle_stream_event(ep_ev, t0);
  assert!(cmd.is_none(), "outbound reply still returns None");
  assert_eq!(
    e.num_members(),
    before,
    "a left node must not merge a push/pull reply"
  );
  assert!(
    e.member(&SmolStr::new("carol")).is_none(),
    "carol must not be merged after leave"
  );
}

#[test]
fn post_leave_push_pull_request_closes_without_replying() {
  use PushNodeState;
  use PushPull;
  use State;
  use StreamCommand;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // An inbound push/pull stream is accepted while Running...
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer_addr, t0).expect("node is running");

  // ...then the node leaves before the peer's request is routed.
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  let before = e.num_members();

  let dave_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7004);
  let dave_state = PushNodeState::new(1, SmolStr::new("dave"), dave_addr, State::Alive);
  let inbound_pp = PushPull::new(true, core::iter::once(dave_state)).with_user_data(Bytes::new());
  let inbound_msg = Message::<SmolStr, SocketAddr>::PushPull(inbound_pp);
  let inbound_bytes =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&inbound_msg).expect("encode");
  stream.handle_data(&inbound_bytes, t0).expect("handle_data");
  let ep_ev = stream.poll_endpoint_event().expect("endpoint event");

  // A leaving node closes the inbound push/pull rather than replying — a reply
  // would start new reliable I/O and leak our local-state snapshot during drain.
  let cmd = e
    .handle_stream_event(ep_ev, t0)
    .expect("a leaving node closes the inbound push/pull");
  assert!(
    matches!(cmd, StreamCommand::Close),
    "a left node closes rather than replying"
  );
  // It also merges none of the peer's state.
  assert_eq!(
    e.num_members(),
    before,
    "a left node must not merge an inbound push/pull"
  );
  assert!(
    e.member(&SmolStr::new("dave")).is_none(),
    "dave must not be merged after leave"
  );
}

#[test]
fn post_leave_pending_push_pull_dial_is_refused() {
  use PushPullKind;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // A join push/pull is initiated while Running...
  let id = e.start_push_pull(peer_addr, PushPullKind::Join, t0);
  // ...then the node leaves before the dial completes. The pending dial must
  // not promote: a left node must not hand the seed its pre-leave Alive state.
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  assert!(
    e.dial_succeeded(id, t0).is_none(),
    "a left node must refuse to promote a pending push/pull dial"
  );
}

#[test]
fn post_leave_detection_probe_does_not_fan_out() {
  use Duration;

  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // A detection probe is in flight (direct Ping sent)...
  e.start_probe(t0);
  while e.poll_transmit().is_some() {}

  // ...then the node leaves. Firing the timeout in the fan-out window must NOT
  // escalate to IndirectPings (no new failure-detection I/O during the drain),
  // and the probe target stays Alive (no suspect).
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  e.handle_timeout(t0 + Duration::from_millis(60));

  let mut saw_indirect = false;
  while let Some(tx) = e.poll_transmit() {
    if let Transmit::Packet(p) = tx {
      if matches!(p.message_ref(), Message::IndirectPing(_)) {
        saw_indirect = true;
      }
    }
  }
  assert!(!saw_indirect, "a left node must not fan out IndirectPings");
  assert_eq!(
    e.member(&SmolStr::new("bob")).map(|m| m.state()),
    Some(State::Alive),
    "a left node must not suspect the probe target"
  );
}

#[test]
fn post_leave_remote_suspect_and_dead_are_inert() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("p1", 7001, 1), false, t0);
  process_alive_auto(&mut e, alive("p2", 7002, 1), false, t0);
  while e.poll_event().is_some() {}
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  while e.poll_event().is_some() {}

  let from_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);

  // A remote Suspect about p1 must not mark it Suspect during the drain.
  e.handle_suspect(
    from_addr,
    Suspect::new(1, SmolStr::new("p1"), SmolStr::new("p2")),
    t0,
  );
  assert_eq!(
    e.member(&SmolStr::new("p1")).map(|m| m.state()),
    Some(State::Alive),
    "a left node must not suspect a peer"
  );

  // A remote Dead about p2 must not mark it Dead or emit NodeLeft.
  e.handle_dead(from_addr, dead("p2", "p1", 1), t0);
  assert_eq!(
    e.member(&SmolStr::new("p2")).map(|m| m.state()),
    Some(State::Alive),
    "a left node must not mark a peer Dead"
  );
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::NodeLeft(_))),
    "a left node must not emit NodeLeft for a remote Dead"
  );
}

#[test]
fn post_leave_inbound_ping_emits_no_ack() {
  use Ping;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  while e.poll_transmit().is_some() {}

  // An inbound Ping during the drain must not produce an Ack: replying would
  // advertise liveness against our own leave. handle_packet drops all
  // gossip-plane inbound once not Running.
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000);
  e.handle_packet(
    from,
    Message::Ping(Ping::new(
      99,
      Node::new(SmolStr::new("prober"), from),
      Node::new(SmolStr::new("local"), local),
    )),
    t0,
  );
  assert!(
    e.poll_transmit().is_none(),
    "a left node must not Ack an inbound Ping"
  );
}

#[test]
fn post_leave_directed_io_rejects() {
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  assert!(matches!(
    e.send_user_packet(peer, Bytes::from_static(b"x")),
    Err(Error::NotRunning)
  ));
  assert!(matches!(
    e.send_user_packets(peer, &[Bytes::from_static(b"x")]),
    Err(Error::NotRunning)
  ));
  assert!(matches!(
    e.ping(Node::new(SmolStr::new("bob"), peer), t0),
    Err(Error::NotRunning)
  ));
  assert!(matches!(
    e.start_user_message(peer, Bytes::from_static(b"x"), t0),
    Err(Error::NotRunning)
  ));
}

#[test]
fn post_leave_poll_timeout_is_none_so_no_spin() {
  use Duration;

  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  while e.poll_event().is_some() {}

  // A probe is in flight, so a SWIM deadline is pending.
  e.start_probe(t0);
  assert!(
    e.poll_timeout().is_some(),
    "a probe deadline is pending while running"
  );

  // After leave, poll_timeout reports no SWIM deadline, so the driver does not
  // spin on the now-stale probe deadline during the drain.
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  assert!(
    e.poll_timeout().is_none(),
    "a left node reports no SWIM deadline (no busy-loop)"
  );
}

#[test]
fn post_leave_initiators_are_inert() {
  use Duration;
  use PushPullKind;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // A left node starts no failure-detection probe (no direct Ping I/O)...
  assert!(!e.start_probe(t0), "a left node starts no probe");
  // ...no push/pull dial (which would advertise our pre-leave Alive)...
  // Ignoring the returned id: the post-leave no-op produces an inert StreamId.
  let _ = e.start_push_pull(peer, PushPullKind::Join, t0);
  // ...and no reliable-ping fallback dial.
  // Ignoring the returned id: the post-leave no-op produces an inert StreamId.
  let _ = e.start_reliable_ping(SmolStr::new("bob"), peer, 7, t0 + Duration::from_secs(5));
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::DialRequested(_))),
    "a left node emits no DialRequested"
  );
  assert!(
    e.poll_transmit().is_none(),
    "a left node queues no probe or push/pull transmit"
  );
}

#[test]
fn post_leave_reliable_user_data_is_not_delivered() {
  use crate::event::UserDataReceived;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  while e.poll_event().is_some() {}

  // A reliable UserData arriving during the drain must not surface a UserPacket
  // to the departing application.
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let cmd = e.handle_stream_event(
    EndpointEvent::UserDataReceived(UserDataReceived::new(peer, Bytes::from_static(b"late"))),
    t0,
  );
  assert!(cmd.is_none());
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::UserPacket(_))),
    "a left node delivers no reliable user data to the application"
  );
}

#[test]
fn post_leave_no_stale_dial_requested_event() {
  use PushPullKind;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // start_push_pull queues a DialRequested event while Running...
  let _sid = e.start_push_pull(peer, PushPullKind::Join, t0);
  // ...then the node leaves before the driver polls events. A raw Endpoint
  // driver must not still be told to dial after leave.
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::DialRequested(_))),
    "a left node surfaces no stale DialRequested event"
  );
}

#[test]
fn post_leave_requeue_event_drops_dial_requested() {
  use PushPullKind;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // A raw driver polls a DialRequested while Running and holds it.
  let _sid = e.start_push_pull(peer, PushPullKind::Join, t0);
  let dial = core::iter::from_fn(|| e.poll_event())
    .find_map(|ev| match ev {
      Event::DialRequested(d) => Some(d),
      _ => None,
    })
    .expect("a DialRequested was emitted");

  // ...then the node leaves and the driver requeues the held event. It must be
  // dropped, not re-surfaced — a requeued dial would restart transport I/O.
  e.leave(t0).expect("leave ok");
  e.requeue_event(Event::DialRequested(dial));
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::DialRequested(_))),
    "a left node drops a requeued DialRequested"
  );
}

#[test]
fn post_leave_accept_stream_returns_none() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // Running: the reliable-inbound door mints an inbound stream.
  assert!(
    e.accept_stream(peer, t0).is_some(),
    "a running node admits inbound reliable streams"
  );

  e.leave(t0).expect("leave ok");
  // Left: the chokepoint refuses — no stream is minted and the driver is
  // expected to drop the transport connection.
  assert!(
    e.accept_stream(peer, t0).is_none(),
    "a left node admits no new inbound reliable stream"
  );
}

// ─────────────── Reliable ping / probe FSM tests ─────────────────────────

#[test]
fn start_reliable_ping_emits_dial_requested() {
  use Event;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer_id = SmolStr::new("bob");
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  while e.poll_event().is_some() {} // drain new() events

  // 4th arg is the exchange deadline; use a future instant so the dial at
  // t0 is within the window (a dial at/after the
  // deadline is rejected).
  let id = e.start_reliable_ping(peer_id, peer_addr, 42, t0 + Duration::from_secs(5));

  let ev = e.poll_event().expect("DialRequested event expected");
  match ev {
    Event::DialRequested(p) => {
      assert_eq!(p.id(), id);
      assert_eq!(*p.peer_ref(), peer_addr);
    }
    other => panic!("expected DialRequested, got {other:?}"),
  }

  // Dial succeeds — stream output_buf must contain an encoded Ping.
  let mut stream = e.dial_succeeded(id, t0).expect("stream expected");
  let mut buf = Vec::new();
  let n = stream
    .poll_transmit_vec(t0, &mut buf)
    .expect("bytes expected");
  assert!(n > 0, "expected non-empty encoded Ping");
  // First byte is PING_MESSAGE_TAG = 2.
  assert_eq!(buf[0], 2u8, "first byte must be PING_MESSAGE_TAG");
}

/// A dial that completes at/after the exchange deadline must NOT be
/// promoted into a sending stream — the write side is
/// deadline-authoritative just like the read side, so a stale request
/// never reaches the wire. A reliable-ping late dial retires the fallback
/// without failing the (still racing) probe.
#[test]
fn dial_succeeded_after_deadline_emits_no_stream() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  while e.poll_event().is_some() {}

  // PushPull-style outbound: a reliable ping with a deadline at t0+50ms.
  let id = e.start_reliable_ping(
    SmolStr::new("peer"),
    peer,
    7,
    t0 + Duration::from_millis(50),
  );
  while e.poll_event().is_some() {}

  // Dial completes AFTER the deadline → no stream, nothing to transmit.
  assert!(
    e.dial_succeeded(id, t0 + Duration::from_millis(51))
      .is_none(),
    "a dial completing past the exchange deadline must not create a stream"
  );
  // The intent is consumed (a second dial_succeeded is also None).
  assert!(
    e.dial_succeeded(id, t0).is_none(),
    "the pending intent must have been dropped"
  );

  // Sanity: a dial WITHIN the deadline still yields a sending stream.
  let id2 = e.start_reliable_ping(
    SmolStr::new("peer"),
    peer,
    8,
    t0 + Duration::from_millis(50),
  );
  while e.poll_event().is_some() {}
  assert!(
    e.dial_succeeded(id2, t0 + Duration::from_millis(10))
      .is_some(),
    "a dial within the deadline must still create the stream"
  );
}

/// With `probe_interval < probe_timeout` the scaled `failure_deadline`
/// precedes the direct deadline. The correct semantic is that
/// `failure_deadline` is the authoritative end of the probe in EVERY
/// phase, so the probe is suspected at `failure_deadline` (t0+20ms) —
/// NOT after the longer, pointless direct sub-window (t0+50ms) — and
/// `poll_timeout` schedules that earlier instant.
#[test]
fn tiny_probe_interval_suspects_at_failure_deadline_not_direct() {
  // probe_interval(20ms) < probe_timeout(50ms): failure_deadline (t0+20ms)
  // < direct_deadline (t0+50ms).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(20)),
  );
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // poll_timeout schedules the EARLIER authoritative failure_deadline
  // (t0+20ms), not the longer direct sub-window (t0+50ms).
  assert_eq!(
    e.poll_timeout(),
    Some(t0 + Duration::from_millis(20)),
    "next wake = the authoritative failure_deadline (earlier than direct)"
  );

  // Before failure_deadline: still AwaitingDirectAck, target Alive.
  e.handle_timeout(t0 + Duration::from_millis(10));
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Alive),
    "not suspected before the authoritative failure_deadline"
  );

  // At failure_deadline (t0+20ms, well before the t0+50ms direct
  // deadline): the probe terminates immediately — it does NOT wait out
  // the longer, pointless direct sub-window.
  e.handle_timeout(t0 + Duration::from_millis(21));
  assert!(
    !e.probes.contains_key(&seq),
    "probe terminated at failure_deadline, not the later direct deadline"
  );
  assert!(
    matches!(
      e.member_liveness(&SmolStr::new("bob")),
      Some(State::Suspect | State::Dead)
    ),
    "suspected at the authoritative deadline (t0+20ms)"
  );
}

#[test]
fn inbound_reliable_ping_encodes_ack() {
  use Node;
  use Ping;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer, t0).expect("node is running");

  // Construct a Ping from peer, correctly addressed to the local node
  // ("local" / 127.0.0.1:7000 per cfg()). A reliable Ping whose target is
  // not us is now rejected (see inbound-ping target validation).
  let src_node = Node::new(SmolStr::new("peer"), peer);
  let tgt_node = Node::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  );
  let ping = Ping::new(99, src_node, tgt_node);
  let ping_msg = Message::<SmolStr, SocketAddr>::Ping(ping);
  let ping_bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&ping_msg).expect("encode");

  stream.handle_data(&ping_bytes, t0).expect("handle_data");

  // Stream should have Ack in output_buf.
  let mut out = Vec::new();
  let n = stream
    .poll_transmit_vec(t0, &mut out)
    .expect("ack bytes expected");
  assert!(n > 0, "expected non-empty Ack response");
  // First byte is ACK_MESSAGE_TAG = 4.
  assert_eq!(out[0], 4u8, "first byte must be ACK_MESSAGE_TAG");
}

/// The public driver contract is "drain poll_transmit until None".
/// Draining must itself advance the write phase: the driver lives in
/// another crate and can only use public APIs.
#[test]
fn poll_transmit_advances_outbound_and_inbound_phases() {
  use PushNodeState;
  use PushPull;
  use State;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();

  // ── Outbound: dial → drain request ⇒ OutboundAwaitingResponse ──────────
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s_out = e.dial_succeeded(id, t0).expect("stream");
  let mut buf = Vec::new();
  assert!(
    s_out.poll_transmit_vec(t0, &mut buf).is_some(),
    "request bytes"
  );
  assert!(
    matches!(s_out.phase, StreamPhase::OutboundAwaitingResponse(_)),
    "drain must advance OutboundSendingRequest → OutboundAwaitingResponse"
  );

  // ── Inbound: accept → req → load response → drain ⇒ Done + Closed ──────
  let mut s_in = e.accept_stream(peer, t0).expect("node is running");
  let pp = PushPull::new(
    true,
    core::iter::once(PushNodeState::new(
      1,
      SmolStr::new("peer"),
      peer,
      State::Alive,
    )),
  )
  .with_user_data(Bytes::new());
  let req = crate::wire::encode_message::<SmolStr, SocketAddr>(
    &Message::<SmolStr, SocketAddr>::PushPull(pp),
  )
  .expect("encode");
  s_in.handle_data(&req, t0).expect("handle_data");
  let ep_ev = s_in.poll_endpoint_event().expect("PushPullRequestReceived");
  let cmd = e.handle_stream_event(ep_ev, t0).expect("StreamCommand");
  match cmd {
    StreamCommand::SendPushPullResponse(resp) => {
      Endpoint::<SmolStr, SocketAddr>::stream_load_response(
        &mut s_in,
        resp.into_encoded(),
        t0 + Duration::from_secs(5),
      );
    }
    StreamCommand::Close => panic!("expected SendPushPullResponse"),
  }
  buf.clear();
  assert!(
    s_in.poll_transmit_vec(t0, &mut buf).is_some(),
    "response bytes"
  );
  assert!(
    s_in.is_done(),
    "draining the response must advance InboundSendingResponse → Done"
  );
  assert!(
    s_in
      .poll_event()
      .is_some_and(|ev| matches!(ev, StreamEvent::Closed)),
    "Done must emit StreamEvent::Closed"
  );
}

/// An outbound reliable ping must reject an Ack whose sequence number does
/// not match the probe — otherwise a stale/relayed/spoofed Ack falsely
/// marks a dead target healthy.
#[test]
fn outbound_reliable_ping_rejects_wrong_ack_seq() {
  use Ack;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let id = e.start_reliable_ping(SmolStr::new("peer"), peer, 42, t0 + Duration::from_secs(5));
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");
  let mut buf = Vec::new();
  s.poll_transmit_vec(t0, &mut buf); // drain ping ⇒ OutboundAwaitingResponse

  let wrong = crate::wire::encode_message::<SmolStr, SocketAddr>(
    &Message::<SmolStr, SocketAddr>::Ack(Ack::new(9999)),
  )
  .expect("encode");
  let r = s.handle_data(&wrong, t0);
  assert!(r.is_err(), "mismatched Ack seq must be rejected");
  assert!(
    s.poll_endpoint_event().is_none(),
    "no ReliablePingAcked for a wrong-seq Ack"
  );
}

/// An inbound reliable Ping whose target is not the local node must NOT be
/// Acked (mirrors Endpoint::handle_ping).
#[test]
fn inbound_reliable_ping_rejects_wrong_target() {
  use Node;
  use Ping;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let mut s = e.accept_stream(peer, t0).expect("node is running");
  let ping = Ping::new(
    7,
    Node::new(SmolStr::new("peer"), peer),
    Node::new(SmolStr::new("someone-else"), peer),
  );
  let bytes =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::<SmolStr, SocketAddr>::Ping(ping))
      .expect("encode");
  assert!(
    s.handle_data(&bytes, t0).is_err(),
    "ping for a non-local target must be rejected"
  );
  let mut out = Vec::new();
  assert!(
    s.poll_transmit_vec(t0, &mut out).is_none(),
    "no Ack must be queued for a misrouted ping"
  );
}

/// A peer that declares a frame larger than `max_stream_frame_size` (or a
/// length varint that overflows u32) is rejected the instant the varint
/// decodes — the body is never buffered, so a slow-drip huge-length frame
/// cannot exhaust memory.
#[test]
fn oversize_or_overflowing_stream_frame_is_rejected_without_buffering() {
  use Node;
  use Ping;
  let cap = 1024usize;
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_max_stream_frame_size(cap));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();

  // Header only: tag=8 (push/pull) + varint(body_len = 2048). total =
  // 1 + 2 + 2048 = 2051 > cap(1024). The body is NEVER sent.
  let mut s = e.accept_stream(peer, t0).expect("node is running");
  let header = [8u8, 0x80, 0x10]; // varint LE base-128 for 2048
  let r = s.handle_data(&header, t0);
  assert!(
    r.is_err(),
    "an oversize declared frame must be rejected on the header alone"
  );
  assert!(
    s.input_buf.len() <= header.len(),
    "no body must be buffered — memory stays bounded (len={})",
    s.input_buf.len()
  );

  // A never-terminating length varint overflows u32 within 5 bytes and is
  // rejected, not buffered indefinitely.
  let mut s2 = e.accept_stream(peer, t0).expect("node is running");
  let overflow = [8u8, 0x80, 0x80, 0x80, 0x80, 0x80];
  assert!(
    s2.handle_data(&overflow, t0).is_err(),
    "a never-terminating varint must be rejected"
  );

  // A terminal overflowing 5th byte: `80 80 80 80 10` encodes 2^32 in
  // base-128 LE. Under u32 accumulation this wrapped to 0 and was accepted
  // as a complete 6-byte empty PushPull, bypassing the size guard. Decoding
  // via u64 keeps the true value (2^32) → total far exceeds cap → Err.
  let mut s2b = e.accept_stream(peer, t0).expect("node is running");
  let terminal_overflow = [8u8, 0x80, 0x80, 0x80, 0x80, 0x10];
  let r = s2b.handle_data(&terminal_overflow, t0);
  assert!(
    r.is_err(),
    "a terminal-overflowing length varint must be rejected, not wrap to 0"
  );
  assert!(
    !s2b.is_done(),
    "the crafted frame must NOT be accepted as a complete tiny frame"
  );

  // A within-cap frame still decodes normally (no false positive): a real
  // small Ping addressed to local is accepted.
  let mut s3 = e.accept_stream(peer, t0).expect("node is running");
  let ping = Ping::new(
    1,
    Node::new(SmolStr::new("peer"), peer),
    Node::new(
      SmolStr::new("local"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ),
  );
  let bytes =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::<SmolStr, SocketAddr>::Ping(ping))
      .expect("encode");
  assert!(
    bytes.len() <= cap,
    "test precondition: a Ping frame fits under the cap"
  );
  assert!(
    s3.handle_data(&bytes, t0).is_ok(),
    "a within-cap frame must still be accepted"
  );
}

/// The wire framing length is a u32 by protocol, so a declared body length
/// beyond u32::MAX is malformed REGARDLESS of `max_stream_frame_size`. Even at
/// the largest cap the machine accepts (u32::MAX — a larger one is rejected at
/// construction as an unreachable receive gate), a terminal-overflowing varint
/// must be rejected at decode — not cast to u32 where it would wrap to a small
/// in-cap length and desync framing.
#[test]
fn over_u32_frame_length_rejected_even_with_max_cap() {
  // Cap at the u32 ceiling (the largest the machine accepts). A ~2^32 declared
  // length overflows the u32 wire decode and must be rejected there before any
  // cast — a wrap to 0 would otherwise pass the cap and desync framing.
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_max_stream_frame_size(u32::MAX as usize));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");
  // tag=8 + varint for 0x10<<28 == 2^32 == u32::MAX+1 (> u32 wire limit).
  let terminal_over_u32 = [8u8, 0x80, 0x80, 0x80, 0x80, 0x10];
  let r = s.handle_data(&terminal_over_u32, t0);
  assert!(
    r.is_err(),
    "a length beyond the u32 wire limit must be rejected regardless of cap"
  );
  assert!(
    !s.is_done(),
    "the crafted over-u32 frame must NOT be accepted"
  );
  assert!(
    s.input_buf.len() <= terminal_over_u32.len(),
    "no multi-GB body buffered (len={})",
    s.input_buf.len()
  );
}

/// `max_stream_frame_size` is a HARD bound on `input_buf`, enforced BEFORE
/// the append — a single `handle_data` delivering an oversize header plus a
/// big body, or a valid frame followed by large trailing bytes, must be
/// rejected without ever allocating past the cap.
#[test]
fn handle_data_bounds_input_buf_before_append() {
  use Node;
  use Ping;
  let cap = 256usize;

  // (a) Oversize declared frame + a large body in ONE handle_data call.
  // tag=8 + varint(4096) + 4096 body bytes, all delivered together.
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_max_stream_frame_size(cap).with_meta_max_size(16));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");
  let mut frame = vec![8u8, 0x80, 0x20]; // varint LE base-128 for 4096
  frame.extend(core::iter::repeat_n(0u8, 4096));
  assert!(
    s.handle_data(&frame, t0).is_err(),
    "single-call oversize frame must be rejected"
  );
  assert!(
    s.input_buf.len() <= cap,
    "input_buf must never exceed the cap (len={}, cap={})",
    s.input_buf.len(),
    cap
  );

  // (b) A valid small frame followed by a large trailing blob in the SAME
  // call. The append-time bound rejects before buffering the trailing.
  let mut e2: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_max_stream_frame_size(cap).with_meta_max_size(16));
  let mut s2 = e2.accept_stream(peer, t0).expect("node is running");
  let ping = Ping::new(
    1,
    Node::new(SmolStr::new("peer"), peer),
    Node::new(
      SmolStr::new("local"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ),
  );
  let mut delivery =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::<SmolStr, SocketAddr>::Ping(ping))
      .expect("encode");
  assert!(delivery.len() < cap, "precondition: ping frame under cap");
  delivery.extend(core::iter::repeat_n(0u8, cap)); // huge trailing
  assert!(
    s2.handle_data(&delivery, t0).is_err(),
    "valid frame + oversize trailing in one call must be rejected pre-append"
  );
  assert!(
    s2.input_buf.len() <= cap,
    "input_buf must never exceed the cap (len={}, cap={})",
    s2.input_buf.len(),
    cap
  );

  // Sanity: a within-cap delivery still works.
  let mut e3: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_max_stream_frame_size(cap).with_meta_max_size(16));
  let mut s3 = e3.accept_stream(peer, t0).expect("node is running");
  let ping3 = Ping::new(
    2,
    Node::new(SmolStr::new("peer"), peer),
    Node::new(
      SmolStr::new("local"),
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
    ),
  );
  let ok = crate::wire::encode_message::<SmolStr, SocketAddr>(
    &Message::<SmolStr, SocketAddr>::Ping(ping3),
  )
  .expect("encode");
  assert!(
    s3.handle_data(&ok, t0).is_ok(),
    "a within-cap frame must still be accepted"
  );
}

/// The stream deadline is authoritative regardless of packet-vs-timer
/// ordering. A push/pull request whose bytes are fed to handle_data at or
/// after the stream deadline (before handle_timeout fires) must fail the
/// stream — NOT decode and emit PushPullRequestReceived past stream_timeout.
#[test]
fn handle_data_after_stream_deadline_fails_without_decoding() {
  use EndpointEvent;
  use PushNodeState;
  use PushPull;
  use State;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_stream_timeout(Duration::from_millis(100)));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let req =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::<SmolStr, SocketAddr>::PushPull(
      PushPull::new(
        true,
        core::iter::once(PushNodeState::new(1, SmolStr::new("p"), peer, State::Alive)),
      )
      .with_user_data(Bytes::new()),
    ))
    .expect("encode");

  // Exactly at the deadline (now == deadline): authoritative → fail.
  let mut s = e.accept_stream(peer, t0).expect("node is running"); // deadline = t0 + 100ms
  let at = s.handle_data(&req, t0 + Duration::from_millis(100));
  assert!(
    at.is_err(),
    "data exactly at the deadline must fail the stream"
  );
  assert!(s.is_failed().is_some(), "stream must be Failed");
  assert!(
    s.poll_endpoint_event().is_none(),
    "no PushPullRequestReceived may be emitted past the deadline"
  );

  // After the deadline, before any handle_timeout: same.
  let mut s2 = e.accept_stream(peer, t0).expect("node is running");
  let after = s2.handle_data(&req, t0 + Duration::from_millis(101));
  assert!(
    after.is_err(),
    "data after the deadline must fail the stream"
  );
  assert!(
    s2.poll_endpoint_event().is_none(),
    "no event past the deadline"
  );

  // Sanity: just before the deadline still decodes normally.
  let mut s3 = e.accept_stream(peer, t0).expect("node is running");
  assert!(s3.handle_data(&req, t0 + Duration::from_millis(99)).is_ok());
  assert!(
    matches!(
      s3.poll_endpoint_event(),
      Some(EndpointEvent::PushPullRequestReceived(..))
    ),
    "within the deadline the request decodes"
  );
}

/// The Detection probe's cumulative failure deadline is
/// `sent + Awareness::scale_timeout(probe_interval)` — derived from the
/// independent probe_interval knob (NOT 2*probe_timeout) AND scaled by the
/// local Lifeguard health score (degraded node waits proportionally longer
/// before suspecting). Snapshotted at probe start.
#[test]
fn detection_failure_deadline_is_scaled_probe_interval() {
  // Decoupled: probe_interval(70ms) != 2*probe_timeout(100ms).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(70)),
  );
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Degrade local health by 2 → scale_timeout multiplies by (2+1)=3.
  e.degrade_health(2);
  assert_eq!(e.health_score(), 2);

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };

  let probe = e.probes.get(&seq).expect("probe present");
  // Snapshotted at construction: sent_at + probe_interval*(health+1)
  // = t0 + 70ms*3 = t0 + 210ms. NOT t0+2*50ms and NOT t0+70ms.
  assert_eq!(
    probe.failure_deadline(),
    t0 + Duration::from_millis(70) * 3,
    "Detection failure deadline = sent + scaled probe_interval"
  );
  // The direct sub-window stays the UNSCALED probe_timeout.
  assert_eq!(
    probe.direct_deadline(e.cfg.probe_timeout()),
    t0 + Duration::from_millis(50),
    "direct sub-window = unscaled probe_timeout"
  );
}

#[test]
fn reliable_ping_ack_drives_probe_success() {
  use EndpointEvent;

  use crate::event::ReliablePingAcked;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let probe_seq = 55u32;
  let bob = e.member(&SmolStr::new("bob")).unwrap().clone();
  let bob_incarnation = e.node_incarnation(&SmolStr::new("bob")).unwrap();
  let bob_generation = e.members.get(&SmolStr::new("bob")).unwrap().generation();
  let now = Instant::now();

  // Probe in AwaitingIndirect with the reliable fallback armed
  // concurrently — a ReliablePingAcked must win the race and succeed the
  // probe even though the indirect deadline has not elapsed.
  let stream_id = StreamId::from_raw(999);
  e.probes.insert(
    probe_seq,
    Probe {
      target: bob,
      target_incarnation: bob_incarnation,
      target_generation: bob_generation,
      sent_at: now,
      kind: ProbeKind::Detection,
      dispatched: true,
      failure_deadline: now + Duration::from_secs(5),
      phase: ProbePhase::AwaitingIndirect(AwaitingIndirect {
        expected_nacks: 2,
        indirect_peers: SmallVec::new(),
        nacked_by: SmallVec::new(),
        reliable_stream_id: Some(stream_id),
        deadline: now + Duration::from_secs(5),
      }),
    },
  );

  let initial_score = e.health_score();
  // Route the ReliablePingAcked event through handle_stream_event.
  e.handle_stream_event(
    EndpointEvent::ReliablePingAcked(ReliablePingAcked::new(probe_seq, now)),
    now,
  );
  // Probe must be removed and awareness score must not increase (success ticks down).
  assert!(
    !e.probes.contains_key(&probe_seq),
    "probe must be removed after ack"
  );
  assert!(
    e.health_score() <= initial_score,
    "health score must not increase on probe success"
  );
}

// ─────────────── Reliable user messages ──────────────────────────────────

#[test]
fn start_user_message_encodes_user_data() {
  use Event;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // Drain the construction-time self-join so the DialRequested below is first.
  while e.poll_event().is_some() {}
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e
    .start_user_message(peer, Bytes::from_static(b"hello"), t0)
    .expect("issued while running");

  let ev = e.poll_event().expect("DialRequested");
  match ev {
    Event::DialRequested(p) => assert_eq!(p.id(), id),
    other => panic!("expected DialRequested, got {other:?}"),
  }

  let mut stream = e.dial_succeeded(id, t0).expect("stream");
  let mut buf = Vec::new();
  let n = stream.poll_transmit_vec(t0, &mut buf).expect("bytes");
  assert!(n > 0);
  // First byte is USER_DATA_MESSAGE_TAG = 9.
  assert_eq!(buf[0], 9u8);
  // A reliable user message is one-way — draining the send must terminate
  // the stream, NOT park it in OutboundAwaitingResponse waiting for a
  // reply that never arrives (which would time out under load).
  assert!(
    stream.is_done(),
    "one-way reliable user message must be Done after send"
  );
  assert!(
    stream
      .poll_event()
      .is_some_and(|ev| matches!(ev, StreamEvent::Closed)),
    "user-message completion must emit StreamEvent::Closed"
  );
}

#[test]
fn inbound_user_data_emits_user_packet_event() {
  use EndpointEvent;
  use Event;
  use Reliability;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // Drain the construction-time self-join so the UserPacket below is first.
  while e.poll_event().is_some() {}
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer, t0).expect("node is running");

  let msg = Message::<SmolStr, SocketAddr>::UserData(Bytes::from_static(b"world"));
  let msg_bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&msg).expect("encode");
  stream.handle_data(&msg_bytes, t0).expect("handle_data");
  assert!(stream.is_done());

  let ep_ev = stream.poll_endpoint_event().expect("endpoint event");
  match &ep_ev {
    EndpointEvent::UserDataReceived(p) => {
      assert_eq!(p.data_ref().as_ref(), b"world");
    }
    other => panic!("expected UserDataReceived, got {other:?}"),
  }

  e.handle_stream_event(ep_ev, t0);

  let app_ev = e.poll_event().expect("UserPacket");
  match app_ev {
    Event::UserPacket(p) => {
      assert_eq!(p.data_ref().as_ref(), b"world");
      assert_eq!(p.reliability(), Reliability::Reliable);
    }
    other => panic!("expected UserPacket, got {other:?}"),
  }
}

// ─────────────── dial_failed + stream timeouts ───────────────────────────

/// `Stream::handle_timeout` transitions the stream to `Failed(Timeout)` when
/// `now >= deadline`. Before the deadline the phase is unchanged.
#[test]
fn stream_timeout_transitions_to_failed() {
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer, t0).expect("node is running");

  // Stream starts in InboundAwaitingFirstMessage with a deadline.
  assert!(matches!(
    stream.phase,
    StreamPhase::InboundAwaitingFirstMessage
  ));
  let deadline = stream.poll_timeout().expect("deadline expected");

  // Firing handle_timeout before the deadline must NOT fail the stream.
  stream.handle_timeout(t0);
  assert!(
    matches!(stream.phase, StreamPhase::InboundAwaitingFirstMessage),
    "stream must not time out before deadline"
  );

  // Firing handle_timeout AT the deadline must transition to Failed(Timeout).
  stream.handle_timeout(deadline);
  assert!(
    matches!(stream.phase, StreamPhase::Failed(StreamError::Timeout)),
    "stream must be Failed(Timeout) at deadline, got {:?}",
    stream.phase
  );
}

/// A stream whose deadline elapsed (moved to `Failed` by `handle_timeout`)
/// must NOT have its queued request/reply bytes drained by a later
/// `poll_transmit` — otherwise a timed-out push/pull or reliable user
/// message reaches the wire AFTER the exchange deadline.
/// `enter_failed` also clears `output_buf`.
#[test]
fn timed_out_stream_never_transmits_queued_bytes() {
  use crate::{
    error::StreamError,
    event::{PushPullKind, StreamEvent},
    stream::StreamPhase,
  };

  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_stream_timeout(Duration::from_millis(100)));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Join, t0);
  while e.poll_event().is_some() {} // drain DialRequested
  let mut stream = e.dial_succeeded(id, t0).expect("stream expected");

  // The outbound PushPull request is queued but NOT yet drained.
  assert!(!stream.output_buf.is_empty(), "request bytes queued");
  let deadline = stream.poll_timeout().expect("deadline expected");

  // Deadline elapses → Failed(Timeout).
  stream.handle_timeout(deadline);
  assert!(
    matches!(stream.phase, StreamPhase::Failed(StreamError::Timeout)),
    "stream must be Failed at the deadline, got {:?}",
    stream.phase
  );

  // The queued request must NOT go out post-deadline, and the buffer is
  // cleared (`poll_transmit` must not drain it after the stream fails).
  let mut buf = Vec::new();
  assert!(
    stream.poll_transmit_vec(deadline, &mut buf).is_none(),
    "a timed-out stream must transmit nothing"
  );
  assert!(
    buf.is_empty(),
    "no bytes may be written for a failed stream"
  );
  assert!(
    stream.output_buf.is_empty(),
    "enter_failed must clear the output buffer"
  );

  // The failure is observable so the driver reaps it.
  assert!(
    core::iter::from_fn(|| stream.poll_event()).any(|ev| matches!(ev, StreamEvent::Failed(_))),
    "a Failed event must be emitted"
  );
}

/// A fatal `handle_data` error (here an oversize declared frame exceeding
/// `max_stream_frame_size`) must TERMINALIZE the FSM — `is_failed()` set,
/// `poll_timeout()` None, a `Failed` event emitted, further data a no-op —
/// not merely return `Err` while the phase stays non-terminal (which would
/// leave the driver believing the stream is still live until an unrelated
/// timeout).
#[test]
fn fatal_frame_error_terminalizes_stream_fsm() {
  use StreamEvent;

  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_max_stream_frame_size(1024));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");
  assert!(
    s.poll_timeout().is_some(),
    "an active stream pulls a deadline"
  );

  // tag=8 + varint(2048) → declared total 2051 > cap 1024 → fatal.
  let header = [8u8, 0x80, 0x10];
  assert!(
    s.handle_data(&header, t0).is_err(),
    "oversize frame is fatal"
  );

  assert!(
    s.is_failed().is_some(),
    "a fatal frame error must move the FSM to Failed"
  );
  assert!(
    s.poll_timeout().is_none(),
    "a terminal stream must not keep pulling a deadline"
  );
  assert!(
    core::iter::from_fn(|| s.poll_event()).any(|ev| matches!(ev, StreamEvent::Failed(_))),
    "a Failed event must be emitted so the driver reaps the stream"
  );
  // Idempotent: data after terminalization is a silent no-op (no second
  // Failed event, no panic).
  assert!(
    s.handle_data(&header, t0).is_ok(),
    "post-failure data is ignored"
  );
  assert!(s.poll_event().is_none(), "no duplicate Failed event");
}

/// A reliable-ping dial failure must NOT fail the probe. The fallback runs
/// concurrently with the indirect pings; a fallback failure is only
/// `did_contact = false`. `dial_failed` retires the fallback (clears
/// `reliable_stream_id`) but the probe stays alive in `AwaitingIndirect`,
/// still racing its single cumulative deadline.
#[test]
fn dial_failed_for_reliable_ping_retires_fallback_not_probe() {
  use StreamError;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let probe_seq = 77u32;
  let bob = e.member(&SmolStr::new("bob")).unwrap().clone();
  let bob_incarnation = e.node_incarnation(&SmolStr::new("bob")).unwrap();
  let bob_generation = e.members.get(&SmolStr::new("bob")).unwrap().generation();
  let t0 = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // Dial a reliable ping — registers the intent with OutboundKind::ReliablePing.
  let stream_id = e.start_reliable_ping(SmolStr::new("bob"), peer, probe_seq, t0);
  while e.poll_event().is_some() {} // drain DialRequested

  let deadline = t0 + Duration::from_secs(5);
  e.probes.insert(
    probe_seq,
    Probe {
      target: bob,
      target_incarnation: bob_incarnation,
      target_generation: bob_generation,
      sent_at: t0,
      kind: ProbeKind::Detection,
      dispatched: true,
      failure_deadline: deadline,
      phase: ProbePhase::AwaitingIndirect(AwaitingIndirect {
        expected_nacks: 2,
        indirect_peers: SmallVec::new(),
        nacked_by: SmallVec::new(),
        reliable_stream_id: Some(stream_id),
        deadline,
      }),
    },
  );

  e.dial_failed(stream_id, StreamError::DialFailed("refused".into()), t0);

  // Probe still alive — the indirect path keeps racing the deadline.
  let probe = e.probes.get(&probe_seq).expect("probe must NOT be removed");
  match &probe.phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      reliable_stream_id,
      deadline: d,
      ..
    }) => {
      assert!(
        reliable_stream_id.is_none(),
        "the failed fallback stream must be retired"
      );
      assert_eq!(*d, deadline, "the cumulative deadline is unchanged");
    }
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
}

// ─────────── Ack responder validation ────────────────────────────────────
//
// Naïvely keying ack handlers purely by the monotonic u32 seq and
// accepting an Ack from ANY source means a peer that observes/guesses
// the seq could forge probe success / PingCompleted, relay-forge a
// forward, and — worst — evict the in-flight slot so the genuine Ack is
// then lost. The endpoint validates the source against the expected
// responder BEFORE consuming the slot.

#[test]
fn forged_probe_ack_from_wrong_source_is_rejected() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let (seq, target_addr) = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        (ping.sequence_number(), to)
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}

  // An off-path node guesses the seq and Acks from the wrong address.
  let evil = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9999);
  e.handle_ack(evil, Ack::new(seq), t0 + Duration::from_millis(10));
  assert!(
    e.probes.contains_key(&seq),
    "a forged Ack from a non-target source must NOT complete the probe"
  );

  // The slot was preserved, so the genuine target Ack still resolves it.
  e.handle_ack(target_addr, Ack::new(seq), t0 + Duration::from_millis(20));
  assert!(
    !e.probes.contains_key(&seq),
    "the genuine target Ack still completes the probe (slot not evicted)"
  );
}

#[test]
fn indirect_relayed_ack_is_accepted_only_from_a_chosen_peer() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_timeout(Duration::from_millis(50)));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, Instant::now());
  process_alive_auto(&mut e, alive("dave", 7003, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Direct sub-timeout (50ms) elapses → fan out IndirectPing to chosen peers.
  e.handle_timeout(t0 + Duration::from_millis(60));
  let mut relay_from = None;
  while let Some(tx) = e.poll_transmit() {
    if let Transmit::Packet(p) = tx {
      let (to, message) = p.into_parts();
      if matches!(message, Message::IndirectPing(_)) {
        relay_from = Some(to);
      }
    }
  }
  let chosen = relay_from.expect("an IndirectPing was fanned out to a chosen peer");

  // A relayed Ack carrying the chosen indirect peer's source address
  // (NOT the target's) within the cumulative window completes the probe.
  e.handle_ack(chosen, Ack::new(seq), t0 + Duration::from_millis(70));
  assert!(
    !e.probes.contains_key(&seq),
    "a relayed Ack from a chosen indirect peer completes the probe"
  );
}

#[test]
fn forged_forward_ack_does_not_relay() {
  use IndirectPing;
  use Node;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  let requester = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7100);
  let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7200);

  let ind = IndirectPing::new(
    42,
    Node::new(SmolStr::new("req"), requester),
    Node::new(SmolStr::new("tgt"), target),
  );
  e.handle_indirect_ping(requester, ind, t0);

  // We forwarded a Ping to `target` under our own allocated seq.
  let (our_seq, fwd_to) = match e.poll_transmit().expect("forwarded Ping") {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        (ping.sequence_number(), to)
      } else {
        panic!("Ping message expected")
      }
    }
    _ => panic!("forwarded Ping expected"),
  };
  assert_eq!(fwd_to, target, "the forwarded Ping goes to the target");

  // A forged Ack from a non-target node must NOT produce a relay Ack and
  // must NOT consume the forward state.
  let evil = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9999);
  e.handle_ack(evil, Ack::new(our_seq), t0 + Duration::from_millis(5));
  assert!(
    !core::iter::from_fn(|| e.poll_transmit()).any(|tx| matches!(
      &tx,
      Transmit::Packet(p) if matches!(p.message_ref(), Message::Ack(_))
    )),
    "a forged forward Ack must not be relayed to the requester"
  );

  // The genuine target Ack relays an Ack back to the original requester.
  e.handle_ack(target, Ack::new(our_seq), t0 + Duration::from_millis(10));
  let relayed = core::iter::from_fn(|| e.poll_transmit())
    .find(|tx| matches!(tx, Transmit::Packet(p) if matches!(p.message_ref(), Message::Ack(_))))
    .expect("the genuine target Ack is relayed");
  match relayed {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      if let Message::Ack(a) = message {
        assert_eq!(to, requester, "relay goes to the original requester");
        assert_eq!(a.sequence_number(), 42, "relay carries the requester's seq");
      } else {
        panic!("expected Ack message");
      }
    }
    _ => unreachable!(),
  }
}

// ─────────── poll_transmit is deadline-authoritative ─────────────────────

/// A dial that succeeds just before the deadline, followed by the driver
/// polling transmit at/after the deadline before it gets to
/// `handle_timeout`: the stream must transmit nothing and move to
/// `Failed(Timeout)`. Without this guard, the non-terminal stream drained
/// its queued bytes (and for a one-way `UserMessage` the same call marked
/// it `Done`, making the post-deadline send unrevocable).
#[test]
fn dial_before_deadline_then_transmit_after_deadline_emits_nothing() {
  use crate::{error::StreamError, event::PushPullKind};

  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_stream_timeout(Duration::from_millis(100)));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Join, t0);
  while e.poll_event().is_some() {}

  // Dial succeeds 1ms before the t0+100ms exchange deadline.
  let mut s = e
    .dial_succeeded(id, t0 + Duration::from_millis(99))
    .expect("stream built before the deadline");
  assert!(!s.output_buf.is_empty(), "request bytes queued");

  // Driver polls transmit AT the deadline, before any handle_timeout.
  let mut buf = Vec::new();
  assert!(
    s.poll_transmit_vec(t0 + Duration::from_millis(100), &mut buf)
      .is_none(),
    "a stream past its deadline must transmit nothing"
  );
  assert!(buf.is_empty(), "no bytes may reach the wire post-deadline");
  assert!(
    matches!(s.phase, StreamPhase::Failed(StreamError::Timeout)),
    "poll_transmit past the deadline fails the stream, got {:?}",
    s.phase
  );
  assert!(
    s.output_buf.is_empty(),
    "the queued request is dropped, not merely withheld"
  );
  // Idempotent.
  assert!(
    s.poll_transmit_vec(t0 + Duration::from_millis(200), &mut buf)
      .is_none()
  );
}

// ─────────── peer EOF terminalizes (PeerClosed) ──────────────────────────

/// The live drivers signal peer EOF as an empty `handle_data` slice
/// (driver_net `Some(Ok(0))`, driver_quic `eof`). An empty slice in
/// `OutboundAwaitingResponse` must terminalize the stream as
/// `Failed(PeerClosed)` — not no-op it, which would leave the FSM live
/// until the deadline.
#[test]
fn eof_before_response_fails_peer_closed() {
  use crate::{
    error::StreamError,
    event::{PushPullKind, StreamEvent},
    stream::StreamPhase,
  };

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");
  let mut buf = Vec::new();
  assert!(
    s.poll_transmit_vec(t0, &mut buf).is_some(),
    "request drains ⇒ OutboundAwaitingResponse"
  );

  // Peer hangs up before the response (driver EOF marker).
  let r = s.handle_data(&[], t0);
  assert!(
    matches!(r, Err(StreamError::PeerClosed)),
    "EOF while awaiting a response ⇒ PeerClosed, got {r:?}"
  );
  assert!(
    matches!(s.phase, StreamPhase::Failed(StreamError::PeerClosed)),
    "the FSM is terminal, got {:?}",
    s.phase
  );
  assert!(
    s.poll_timeout().is_none(),
    "a terminal stream pulls no deadline"
  );
  assert!(
    core::iter::from_fn(|| s.poll_event()).any(|ev| matches!(ev, StreamEvent::Failed(_))),
    "a Failed event must be emitted so the driver reaps it"
  );
  // Idempotent: another EOF is a silent no-op.
  assert!(
    s.handle_data(&[], t0).is_ok(),
    "post-failure EOF is ignored"
  );
  assert!(s.poll_event().is_none(), "no duplicate Failed event");
}

#[test]
fn eof_with_incomplete_buffered_frame_fails_peer_closed() {
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // A single byte: a partial frame header, not yet decodable.
  assert!(s.handle_data(&[8u8], t0).is_ok(), "partial frame buffered");
  assert!(
    s.is_failed().is_none(),
    "still awaiting the rest of the frame"
  );

  // EOF with an incomplete buffered frame ⇒ truncated message ⇒ PeerClosed.
  let r = s.handle_data(&[], t0);
  assert!(
    matches!(r, Err(StreamError::PeerClosed)),
    "EOF mid-frame ⇒ PeerClosed, got {r:?}"
  );
  assert!(matches!(
    s.phase,
    StreamPhase::Failed(StreamError::PeerClosed)
  ));
}

#[test]
fn eof_on_terminal_stream_is_a_noop() {
  use StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // Time it out first.
  let deadline = s.poll_timeout().expect("deadline");
  s.handle_timeout(deadline);
  assert!(
    matches!(s.phase, StreamPhase::Failed(_)),
    "stream is terminal"
  );
  while s.poll_event().is_some() {} // drain the timeout Failed event

  // An EOF arriving on an already-terminal stream must NOT re-fail it or
  // emit a second event (the terminal guard short-circuits before the
  // empty-slice ⇒ PeerClosed path).
  assert!(
    s.handle_data(&[], deadline).is_ok(),
    "EOF on a terminal stream is ignored"
  );
  assert!(
    s.poll_event().is_none(),
    "no second Failed event from a post-terminal EOF"
  );
}

#[test]
fn eof_in_outbound_sending_request_push_pull_is_premature() {
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");

  // The freshly-dialed stream starts in `OutboundSendingRequest` —
  // bytes are buffered in `output_buf` waiting for `poll_transmit` to
  // drain them. Phase has NOT yet advanced to
  // `OutboundAwaitingResponse`.
  assert!(matches!(s.phase, StreamPhase::OutboundSendingRequest(_)));

  // Peer FINs before we even finish writing. For a response-bearing
  // exchange (PushPull) this is premature — the response will never
  // arrive — so EOF here MUST fail.
  let r = s.handle_data(&[], t0);
  assert!(
    matches!(r, Err(StreamError::PeerClosed)),
    "PushPull EOF in OutboundSendingRequest ⇒ PeerClosed, got {r:?}"
  );
  assert!(matches!(
    s.phase,
    StreamPhase::Failed(StreamError::PeerClosed)
  ));
}

#[test]
fn eof_in_outbound_sending_request_reliable_ping_is_premature() {
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  // Need a probe sequence first — start_reliable_ping is keyed on the
  // probe_seq from an originating UDP probe. Use a synthetic seq.
  let id = e.start_reliable_ping(SmolStr::from("peer"), peer, 42, t0 + Duration::from_secs(1));
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");

  assert!(matches!(s.phase, StreamPhase::OutboundSendingRequest(_)));

  // ReliablePing also expects a response (the ack). EOF in
  // OutboundSendingRequest is premature.
  let r = s.handle_data(&[], t0);
  assert!(
    matches!(r, Err(StreamError::PeerClosed)),
    "ReliablePing EOF in OutboundSendingRequest ⇒ PeerClosed, got {r:?}"
  );
  assert!(matches!(
    s.phase,
    StreamPhase::Failed(StreamError::PeerClosed)
  ));
}

#[test]
fn eof_in_outbound_sending_request_user_message_is_ok() {
  use StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e
    .start_user_message(peer, bytes::Bytes::from_static(b"payload"), t0)
    .expect("issued while running");
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");

  assert!(matches!(s.phase, StreamPhase::OutboundSendingRequest(_)));

  // UserMessage is one-way — we don't expect any inbound bytes from
  // the peer. Peer FIN'ing their (empty) send side before we finish
  // writing is benign; the FSM must accept it without failing.
  assert!(
    s.handle_data(&[], t0).is_ok(),
    "UserMessage EOF in OutboundSendingRequest ⇒ Ok (one-way exchange \
     doesn't expect inbound bytes)"
  );
  assert!(
    !matches!(s.phase, StreamPhase::Failed(_)),
    "phase MUST NOT be Failed after a benign one-way EOF (got {:?})",
    s.phase
  );
}

/// Build a push/pull request frame the FSM can decode in
/// `InboundAwaitingFirstMessage`. Mirrors the encoding used by the
/// existing `inbound_push_pull_decode_and_response_bytes` test.
fn build_push_pull_request_bytes() -> Vec<u8> {
  use crate::typed::{Message, PushNodeState, PushPull, State};
  let dave_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7004);
  let dave_state =
    PushNodeState::<SmolStr, SocketAddr>::new(1, SmolStr::new("dave"), dave_addr, State::Alive);
  let inbound_pp = PushPull::<SmolStr, SocketAddr>::new(true, core::iter::once(dave_state))
    .with_user_data(bytes::Bytes::new());
  let inbound_msg = Message::<SmolStr, SocketAddr>::PushPull(inbound_pp);
  crate::wire::encode_message::<SmolStr, SocketAddr>(&inbound_msg).expect("encode")
}

#[test]
fn eof_in_inbound_sending_response_empty_buf_is_ok() {
  use StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // Feed a complete push/pull request frame so the FSM transitions
  // from `InboundAwaitingFirstMessage` → `InboundSendingResponse`.
  let request_bytes = build_push_pull_request_bytes();
  s.handle_data(&request_bytes, t0)
    .expect("complete request frame is decoded");
  assert!(
    matches!(s.phase, StreamPhase::InboundSendingResponse),
    "FSM did not reach InboundSendingResponse — phase = {:?}",
    s.phase
  );

  // Peer FINs after sending the full request — natural close of
  // their send half. EOF here MUST be Ok.
  assert!(
    s.handle_data(&[], t0).is_ok(),
    "EOF in InboundSendingResponse with empty input_buf ⇒ Ok"
  );
  assert!(
    !matches!(s.phase, StreamPhase::Failed(_)),
    "phase MUST NOT be Failed after natural FIN"
  );
}

#[test]
fn trailing_bytes_after_outbound_done_fails_decode() {
  use crate::{
    error::StreamError,
    event::PushPullKind,
    stream::StreamPhase,
    typed::{Message, PushNodeState, PushPull, State},
  };
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("alice", 7000, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");
  let mut _req_buf = Vec::new();
  s.poll_transmit_vec(t0, &mut _req_buf); // advance to OutboundAwaitingResponse
  assert!(matches!(s.phase, StreamPhase::OutboundAwaitingResponse(_)));

  // Build a legitimate reply followed by junk bytes in a SINGLE
  // handle_data delivery — what an adversarial peer could send before
  // FIN'ing.
  let carol_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7003);
  let carol_state =
    PushNodeState::<SmolStr, SocketAddr>::new(1, SmolStr::new("carol"), carol_addr, State::Alive);
  let reply_pp = PushPull::<SmolStr, SocketAddr>::new(false, core::iter::once(carol_state))
    .with_user_data(Bytes::new());
  let reply_msg = Message::<SmolStr, SocketAddr>::PushPull(reply_pp);
  let mut bytes_with_trailing =
    crate::wire::encode_message::<SmolStr, SocketAddr>(&reply_msg).expect("encode");
  bytes_with_trailing.extend_from_slice(&[0x99u8, 0xAAu8, 0xBBu8]);

  let r = s.handle_data(&bytes_with_trailing, t0);
  assert!(
    matches!(r, Err(StreamError::Decode(_))),
    "trailing bytes after a Done-completing frame ⇒ Decode error, \
     got {r:?}"
  );
  assert!(
    matches!(s.phase, StreamPhase::Failed(StreamError::Decode(_))),
    "FSM phase ⇒ Failed(Decode), got {:?}",
    s.phase
  );
}

#[test]
fn late_failure_emits_only_failed_lifecycle_not_closed() {
  // `dispatch_message` queues `StreamEvent::Closed` to `stream_events`
  // BEFORE the post-dispatch trailing-bytes guard runs. `enter_failed`
  // must clear `stream_events` (alongside `endpoint_events`) before
  // pushing `Failed`; otherwise a `poll_event` drain would deliver
  // `Closed` BEFORE `Failed`, contradicting the dispatch/validation
  // atomicity at the Sans-I/O boundary. This test asserts the public
  // observable: after a single-delivery [request][second_frame] failure,
  // the FSM emits ONLY `Failed`, never `Closed`.
  use crate::{error::StreamError, event::StreamEvent, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  let req = build_push_pull_request_bytes();
  let second = build_push_pull_request_bytes();
  let mut combined = req.clone();
  combined.extend_from_slice(&second);

  let r = s.handle_data(&combined, t0);
  assert!(
    matches!(r, Err(StreamError::Decode(_))),
    "single-delivery [request][second_frame] ⇒ Decode, got {r:?}"
  );
  assert!(matches!(
    s.phase,
    StreamPhase::Failed(StreamError::Decode(_))
  ));

  // Public observable: poll_event only yields Failed; no spurious
  // Closed from the dispatched-but-rejected frame.
  let events: Vec<StreamEvent> = core::iter::from_fn(|| s.poll_event()).collect();
  assert!(
    !events.iter().any(|ev| matches!(ev, StreamEvent::Closed)),
    "no `Closed` lifecycle event must survive a late-failure dispatch \
     — got {events:?}"
  );
  assert!(
    events.iter().any(|ev| matches!(ev, StreamEvent::Failed(_))),
    "the `Failed` lifecycle event MUST be present — got {events:?}"
  );
}

#[test]
fn split_delivery_done_then_trailing_bytes_fails_decode() {
  // `handle_data`'s terminal-Done guard must distinguish trailing data
  // from EOF: a follow-up `handle_data(non-empty)` call after the FSM
  // reached Done (e.g. the QUIC bridge's per-chunk feed delivering
  // chunk1=valid frame moving FSM to Done, chunk2=trailing bytes in the
  // next `Chunks::next` iteration) MUST fail Decode rather than silently
  // ignore the trailing bytes. Failed remains a no-op. Done + empty
  // data (EOF) remains a no-op (clean close).
  use crate::{
    error::StreamError,
    event::PushPullKind,
    stream::StreamPhase,
    typed::{Message, PushNodeState, PushPull, State},
  };
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("alice", 7000, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");
  let mut _req_buf = Vec::new();
  s.poll_transmit_vec(t0, &mut _req_buf);
  assert!(matches!(s.phase, StreamPhase::OutboundAwaitingResponse(_)));

  // Chunk 1: the legitimate reply (FSM → Done).
  let carol_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7003);
  let carol_state =
    PushNodeState::<SmolStr, SocketAddr>::new(1, SmolStr::new("carol"), carol_addr, State::Alive);
  let reply_pp = PushPull::<SmolStr, SocketAddr>::new(false, core::iter::once(carol_state))
    .with_user_data(Bytes::new());
  let reply_msg = Message::<SmolStr, SocketAddr>::PushPull(reply_pp);
  let reply = crate::wire::encode_message::<SmolStr, SocketAddr>(&reply_msg).expect("encode");
  s.handle_data(&reply, t0).expect("decode reply");
  assert!(matches!(s.phase, StreamPhase::Done));

  // Chunk 2: trailing junk arrives after the FSM is already Done.
  // The terminal-Done-with-data guard MUST fail Decode here; a permissive
  // guard would silently return Ok and the bridge would clean-reap.
  let r = s.handle_data(&[0x99u8, 0xAAu8], t0);
  assert!(
    matches!(r, Err(StreamError::Decode(_))),
    "Done + non-empty data ⇒ Decode (split-delivery), got {r:?}"
  );
  assert!(matches!(
    s.phase,
    StreamPhase::Failed(StreamError::Decode(_))
  ));

  // Combined trailing-bytes + lifecycle-clear invariant: only Failed
  // surfaces, no Closed.
  let events: Vec<_> = core::iter::from_fn(|| s.poll_event()).collect();
  assert!(
    !events.iter().any(|ev| matches!(ev, StreamEvent::Closed)),
    "no Closed lifecycle event after split-delivery failure — got {events:?}"
  );
}

#[test]
fn done_phase_empty_eof_is_still_a_noop() {
  // Done + EMPTY data (the EOF marker) MUST remain a no-op — this is
  // the clean-close path the QUIC bridge uses to signal peer FIN after
  // a successful exchange (companion to the Done+data fail-Decode case).
  use StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // Drive the FSM to Done by feeding a one-way UserData frame.
  use crate::typed::Message;
  let msg = Message::<SmolStr, SocketAddr>::UserData(bytes::Bytes::from_static(b"hello"));
  let bytes = crate::wire::encode_message::<SmolStr, SocketAddr>(&msg).expect("encode");
  s.handle_data(&bytes, t0).expect("decode user data");
  assert!(matches!(s.phase, StreamPhase::Done));

  // EOF (empty data) on Done is the clean-close marker — Ok.
  assert!(
    s.handle_data(&[], t0).is_ok(),
    "Done + empty data (EOF) MUST remain a no-op clean-close path"
  );
  assert!(
    matches!(s.phase, StreamPhase::Done),
    "phase MUST stay Done after EOF (not flipped to Failed)"
  );
}

#[test]
fn dispatched_frame_endpoint_events_discarded_on_late_failure() {
  // When `dispatch_message` queues an endpoint event (e.g.
  // `PushPullRequestReceived`) and a subsequent guard fails the stream
  // — like the trailing-bytes rejection — the queued event MUST NOT
  // survive the failure. Otherwise the bridge's `drain_then_reap`
  // (or `drain_payload_only`) would route the event into `Endpoint::
  // handle_stream_event`, merging state / encoding a response / etc.
  // for a stream that was just declared invalid.
  //
  // This test asserts the FSM-level invariant: after a single-delivery
  // [request][second_frame] failure, `poll_endpoint_event` returns
  // `None` — `enter_failed` clears the queue alongside the input/
  // output buffers.
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // Single delivery: legitimate request + a second complete frame.
  let req = build_push_pull_request_bytes();
  let second = build_push_pull_request_bytes();
  let mut combined = req.clone();
  combined.extend_from_slice(&second);

  let r = s.handle_data(&combined, t0);
  assert!(
    matches!(r, Err(StreamError::Decode(_))),
    "single-delivery [request][second_frame] ⇒ Decode error, got {r:?}"
  );
  assert!(matches!(
    s.phase,
    StreamPhase::Failed(StreamError::Decode(_))
  ));

  // Public observable: NO endpoint event survived the failure. Without
  // the `endpoint_events.clear()` line in `enter_failed`, the
  // `PushPullRequestReceived` queued by `dispatch_message` would
  // still be drainable here.
  assert!(
    s.poll_endpoint_event().is_none(),
    "FSM endpoint_events MUST be empty after a post-dispatch \
     failure — the bridge would otherwise route the queued event \
     into Endpoint::handle_stream_event for a failed stream"
  );
}

#[test]
fn single_delivery_request_plus_second_frame_fails_decode() {
  // Catches the SINGLE-DELIVERY case where the bridge's `pump_in` hands
  // `[request_bytes][second_frame_bytes]` to `handle_data` in one call.
  // `try_decode_frame` consumes the legitimate first frame, transitions
  // the FSM to `InboundSendingResponse`, and the trailing second frame
  // would otherwise sit in `input_buf` until the NEXT `handle_data` call
  // dispatched it through `PhaseKind::Ignore` (the SPLIT-delivery case).
  // But before that next call fires, the bridge's `drain_payload_only`
  // would route the first frame's endpoint events into `Endpoint` —
  // merging state, encoding a response, etc. — i.e. the protocol
  // violation is observed too late.
  //
  // The `try_decode_frame` post-dispatch trailing-bytes guard catches
  // the violation BEFORE the endpoint side-effects fire.
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // Single delivery: request bytes + a second complete frame.
  let req = build_push_pull_request_bytes();
  let second = build_push_pull_request_bytes();
  let mut combined = req.clone();
  combined.extend_from_slice(&second);

  let r = s.handle_data(&combined, t0);
  assert!(
    matches!(r, Err(StreamError::Decode(_))),
    "single-delivery [request][second_frame] ⇒ Decode error on \
     trailing bytes, got {r:?}"
  );
  assert!(
    matches!(s.phase, StreamPhase::Failed(StreamError::Decode(_))),
    "FSM phase ⇒ Failed(Decode), got {:?}",
    s.phase
  );
}

#[test]
fn second_frame_in_inbound_sending_response_fails_unexpected() {
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // Feed a complete request frame so the FSM moves to
  // InboundSendingResponse.
  let req = build_push_pull_request_bytes();
  s.handle_data(&req, t0).expect("decode request");
  assert!(matches!(s.phase, StreamPhase::InboundSendingResponse));

  // Adversarial peer sends a SECOND complete frame while we're still
  // sending the response. The FSM's `PhaseKind::Ignore` arm MUST fail
  // with UnexpectedMessage rather than silently consume the frame.
  let second = build_push_pull_request_bytes();
  let r = s.handle_data(&second, t0);
  assert!(
    matches!(r, Err(StreamError::UnexpectedMessage(_))),
    "second frame in InboundSendingResponse ⇒ UnexpectedMessage, \
     got {r:?}"
  );
  assert!(
    matches!(
      s.phase,
      StreamPhase::Failed(StreamError::UnexpectedMessage(_))
    ),
    "FSM phase ⇒ Failed(UnexpectedMessage), got {:?}",
    s.phase
  );
}

#[test]
fn eof_in_inbound_sending_response_partial_trailing_fails() {
  use crate::{error::StreamError, stream::StreamPhase};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0).expect("node is running");

  // Feed a complete request frame (advances to InboundSendingResponse).
  let request_bytes = build_push_pull_request_bytes();
  s.handle_data(&request_bytes, t0).expect("decode request");
  assert!(matches!(s.phase, StreamPhase::InboundSendingResponse));

  // Feed a single byte of a SECOND frame (partial trailing).
  s.handle_data(&[7u8], t0)
    .expect("partial trailing byte buffered");

  // EOF with non-empty input_buf — the trailing byte will never be
  // decoded into a complete frame. PeerClosed.
  let r = s.handle_data(&[], t0);
  assert!(
    matches!(r, Err(StreamError::PeerClosed)),
    "EOF in InboundSendingResponse with non-empty input_buf ⇒ \
     PeerClosed (truncated trailing frame), got {r:?}"
  );
  assert!(matches!(
    s.phase,
    StreamPhase::Failed(StreamError::PeerClosed)
  ));
}

// ─────────── End-to-end push/pull integration test ───────────────────────

/// Simulate a complete push/pull exchange between two Endpoints.
///
/// A initiates (start_push_pull → dial_succeeded → poll_transmit → feed to B).
/// B accepts (accept_stream → handle_data → emit PushPullRequestReceived →
/// encode response → load into stream → poll_transmit → feed back to A).
/// A handles response (handle_data → emit PushPullReplyReceived → inline
/// synchronous merge). Both endpoints should see each other's members after
/// the exchange.
#[test]
fn end_to_end_push_pull_membership_convergence() {
  use EndpointEvent;
  use Event;
  use PushPullKind;
  use StreamCommand;

  fn make_addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
  }

  // ── Set up A: knows alice (self) + bob ────────────────────────────────
  let cfg_a = EndpointOptions::<SmolStr, SocketAddr>::new(SmolStr::new("alice"), make_addr(7000));
  let mut a: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg_a);
  // Drain new() startup events.
  while a.poll_event().is_some() {}
  while a.poll_transmit().is_some() {}
  // Add bob to A's membership.
  let t0 = Instant::now();
  process_alive_auto(&mut a, alive("bob", 7001, 1), false, t0);
  while a.poll_event().is_some() {}
  while a.poll_transmit().is_some() {}

  // ── Set up B: knows charlie (self) ────────────────────────────────────
  let cfg_b = EndpointOptions::<SmolStr, SocketAddr>::new(SmolStr::new("charlie"), make_addr(7002));
  let mut b: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg_b);
  while b.poll_event().is_some() {}
  while b.poll_transmit().is_some() {}

  // ── Step 1: A initiates push/pull toward B ────────────────────────────
  let id_a = a.start_push_pull(make_addr(7002), PushPullKind::Join, t0);

  // Consume the DialRequested event.
  match a.poll_event().expect("DialRequested") {
    Event::DialRequested(p) => assert_eq!(p.id(), id_a),
    other => panic!("expected DialRequested, got {other:?}"),
  }

  // A's dial succeeds; stream has request bytes queued.
  let mut stream_a = a.dial_succeeded(id_a, t0).expect("A stream");
  let mut a_request = Vec::new();
  stream_a
    .poll_transmit_vec(t0, &mut a_request)
    .expect("A request bytes");
  assert!(!a_request.is_empty(), "request must be non-empty");
  assert_eq!(a_request[0], 8u8, "must start with PUSH_PULL_MESSAGE_TAG");
  // Draining the request via poll_transmit auto-advances A to
  // OutboundAwaitingResponse — the real driver contract, no manual phase
  // mutation and no removed write-completion helper.
  assert!(
    matches!(stream_a.phase, StreamPhase::OutboundAwaitingResponse(_)),
    "poll_transmit must advance the outbound phase"
  );

  // ── Step 2: B accepts A's stream and handles the request ─────────────
  let mut stream_b = b
    .accept_stream(make_addr(7000), t0)
    .expect("node is running");
  stream_b
    .handle_data(&a_request, t0)
    .expect("B handle_data ok");

  // B's stream should have queued PushPullRequestReceived.
  let ep_ev_b = stream_b.poll_endpoint_event().expect("B endpoint event");
  assert!(
    matches!(ep_ev_b, EndpointEvent::PushPullRequestReceived { .. }),
    "expected PushPullRequestReceived, got {ep_ev_b:?}"
  );

  // Route the event through B's Endpoint; it should return a StreamCommand.
  // The join merge is admitted (no MergeDelegate) and applied synchronously
  // and inline — no decision round-trip.
  let cmd_b = b
    .handle_stream_event(ep_ev_b, t0)
    .expect("expected StreamCommand from B");

  // ── Step 3: B encodes its response and loads it into stream_b ─────────
  let encoded_b = match cmd_b {
    StreamCommand::SendPushPullResponse(resp) => resp.into_encoded(),
    StreamCommand::Close => panic!("expected SendPushPullResponse, got Close"),
  };
  assert!(!encoded_b.is_empty(), "B response must be non-empty");
  assert_eq!(
    encoded_b[0], 8u8,
    "response must start with PUSH_PULL_MESSAGE_TAG"
  );

  Endpoint::<SmolStr, SocketAddr>::stream_load_response(
    &mut stream_b,
    encoded_b,
    t0 + Duration::from_secs(5),
  );

  // Drain B's response bytes (simulating the driver sending them to A).
  let mut b_response = Vec::new();
  stream_b
    .poll_transmit_vec(t0, &mut b_response)
    .expect("B response bytes");
  assert!(!b_response.is_empty(), "B response bytes must be non-empty");

  // ── Step 4: A handles B's response ───────────────────────────────────
  stream_a
    .handle_data(&b_response, t0)
    .expect("A handle_data ok");
  assert!(
    stream_a.is_done(),
    "A stream should be done after decoding response"
  );

  // A's stream should have queued PushPullReplyReceived.
  let ep_ev_a = stream_a.poll_endpoint_event().expect("A endpoint event");
  assert!(
    matches!(ep_ev_a, EndpointEvent::PushPullReplyReceived { .. }),
    "expected PushPullReplyReceived, got {ep_ev_a:?}"
  );

  // Route through A's Endpoint; outbound reply returns no StreamCommand.
  // B's states are merged synchronously and inline.
  let cmd_a = a.handle_stream_event(ep_ev_a, t0);
  assert!(cmd_a.is_none(), "outbound reply must return None");

  // ── Verify convergence ────────────────────────────────────────────────
  // A's view: alice (self) + bob (initial) + charlie (from B)
  assert!(
    a.member(&SmolStr::new("alice")).is_some(),
    "A should know alice (self)"
  );
  assert!(
    a.member(&SmolStr::new("bob")).is_some(),
    "A should know bob (initial)"
  );
  assert!(
    a.member(&SmolStr::new("charlie")).is_some(),
    "A should know charlie from B"
  );

  // B's view: charlie (self) + alice (from A) + bob (from A)
  assert!(
    b.member(&SmolStr::new("charlie")).is_some(),
    "B should know charlie (self)"
  );
  assert!(
    b.member(&SmolStr::new("alice")).is_some(),
    "B should know alice from A"
  );
  assert!(
    b.member(&SmolStr::new("bob")).is_some(),
    "B should know bob from A"
  );
}

#[test]
fn start_scheduling_sets_finite_deadlines() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  );
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  // Before start_scheduling: no scheduler deadlines → poll_timeout None.
  assert!(e.poll_timeout().is_none());

  e.start_scheduling(t0);
  // After start_scheduling: at least one deadline is set (probe_interval > 0 by default).
  let pt = e.poll_timeout().expect("scheduler deadlines should be set");
  // The staggered deadline must be at or after t0 and at or before
  // t0 + probe_interval (probe_interval = 1s by default).
  assert!(pt >= t0, "deadline should be in the future");
  assert!(
    pt <= t0 + Duration::from_secs(1),
    "stagger must be within probe_interval"
  );
}

#[test]
fn start_scheduling_no_op_when_leaving() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  );
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  e.leave(t0).unwrap();
  e.start_scheduling(t0 + Duration::from_secs(1));
  // Scheduler should not have been armed.
  assert!(e.next_probe.is_none());
  assert!(e.next_gossip.is_none());
  assert!(e.next_pushpull.is_none());
}

#[test]
fn probe_scheduler_fires_when_deadline_elapses() {
  // Two-node cluster so start_probe has a valid target.
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::from_millis(100))
  .with_gossip_interval(Duration::ZERO)
  .with_push_pull_interval(Duration::ZERO);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);

  // Add a live peer so start_probe has a target.
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7947, 1), false, t0);
  // Drain any pending events and transmits from alive processing.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Manually arm next_probe to t0 so it fires immediately.
  e.next_probe = Some(t0);

  // Before deadline: no probe transmit.
  e.handle_timeout(t0 - Duration::from_millis(1));
  assert!(
    e.poll_transmit().is_none(),
    "should not fire before deadline"
  );

  // At deadline: start_probe fires → a Ping transmit should appear.
  e.handle_timeout(t0);
  let tx = e
    .poll_transmit()
    .expect("probe scheduler should emit a Ping");
  let Transmit::Packet(p) = tx else {
    panic!("unexpected Compound transmit");
  };
  let (_, message) = p.into_parts();
  assert!(
    matches!(message, Message::Ping(_)),
    "expected Ping, got {message:?}"
  );

  // next_probe should be rescheduled to t0 + 100ms.
  assert_eq!(
    e.next_probe,
    Some(t0 + Duration::from_millis(100)),
    "next_probe should advance by probe_interval"
  );
}

#[test]
fn probe_scheduler_does_not_fire_after_leave() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::from_millis(50))
  .with_gossip_interval(Duration::ZERO)
  .with_push_pull_interval(Duration::ZERO);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  e.next_probe = Some(t0);
  e.leave(t0).unwrap();
  // Drain leave-related events/transmits.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  e.handle_timeout(t0 + Duration::from_millis(200));
  // No probe transmit should appear.
  assert!(
    e.poll_transmit().is_none(),
    "scheduler must not fire after leave"
  );
}

#[test]
fn zero_interval_disables_scheduler() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_gossip_interval(Duration::ZERO)
  .with_push_pull_interval(Duration::ZERO);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  e.start_scheduling(Instant::now());
  // gossip and push/pull should remain None; probe should be set.
  assert!(
    e.next_gossip.is_none(),
    "zero gossip_interval → no gossip scheduler"
  );
  assert!(
    e.next_pushpull.is_none(),
    "zero push_pull_interval → no pushpull scheduler"
  );
  assert!(
    e.next_probe.is_some(),
    "probe_interval > 0 → probe scheduler armed"
  );
}

// ─────────────── Gossip scheduler tests ──────────────────────────────────

#[test]
fn gossip_scheduler_emits_transmits_to_peers() {
  // Two live peers, gossip_nodes=2 → both should receive the user broadcast.
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::ZERO)
  .with_gossip_interval(Duration::from_millis(50))
  .with_gossip_nodes(2)
  .with_push_pull_interval(Duration::ZERO);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);

  let t0 = Instant::now();

  // Add two live peers.
  for (name, port) in [("alice", 7947u16), ("bob", 7948u16)] {
    process_alive_auto(&mut e, alive(name, port, 1), false, t0);
  }
  // Drain initial broadcasts generated by alive processing.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Queue a user broadcast — fire_gossip_scheduler drains this as
  // Message::UserData and includes it in the gossip payload.
  e.queue_user_broadcast(bytes::Bytes::from_static(b"hello gossip"))
    .unwrap();

  // Arm gossip scheduler and fire it at t0.
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  // Collect all transmits produced by the gossip fire.
  let mut transmits = Vec::new();
  while let Some(tx) = e.poll_transmit() {
    transmits.push(tx);
  }

  // Adding two peers also queued membership Alive broadcasts (for alice, bob,
  // and the local node) into self.broadcast. Those plus our 1 user broadcast
  // = >= 2 messages per peer, so by the >= 2 rule each of the 2 targets gets
  // exactly ONE Transmit::Compound carrying the whole gossip payload.
  let messages_of = |tx: &Transmit<SmolStr, SocketAddr>| -> Vec<Message<SmolStr, SocketAddr>> {
    match tx {
      Transmit::Packet(p) => vec![p.message_ref().clone()],
      Transmit::Compound(cmp) => cmp.messages_slice().to_vec(),
    }
  };

  assert_eq!(
    transmits.len(),
    2,
    "exactly one gossip datagram per peer (gossip_nodes=2); got {transmits:?}"
  );

  // Every per-peer datagram must carry our UserData payload exactly once
  // and at least one membership Alive broadcast.
  for tx in &transmits {
    let msgs = messages_of(tx);
    let user: Vec<_> = msgs
      .iter()
      .filter_map(|m| match m {
        Message::UserData(b) => Some(b.clone()),
        _ => None,
      })
      .collect();
    assert_eq!(
      user.len(),
      1,
      "each peer's datagram must carry exactly one UserData, got {msgs:?}"
    );
    assert_eq!(
      user[0].as_ref(),
      b"hello gossip",
      "unexpected payload in UserData"
    );
    assert!(
      msgs.iter().any(|m| matches!(m, Message::Alive(_))),
      "membership Alive broadcasts should also be in the datagram, got {msgs:?}"
    );
  }

  // Deadline must advance by gossip_interval regardless.
  assert_eq!(
    e.next_gossip,
    Some(t0 + Duration::from_millis(50)),
    "next_gossip must advance by gossip_interval"
  );
}

#[test]
fn gossip_scheduler_skips_emit_when_no_broadcasts() {
  // No peers, no user broadcasts → no transmits, but deadline still advances.
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::ZERO)
  .with_gossip_interval(Duration::from_millis(50))
  .with_gossip_nodes(3)
  .with_push_pull_interval(Duration::ZERO);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);
  // Nothing was queued → no transmits.
  assert!(
    e.poll_transmit().is_none(),
    "no broadcasts queued → no transmit expected"
  );
  // Deadline still advances.
  assert_eq!(
    e.next_gossip,
    Some(t0 + Duration::from_millis(50)),
    "next_gossip must advance even when nothing was emitted"
  );
}

// ── Push/pull scheduler tests ─────────────────────────────────────────────

#[test]
fn pushpull_scheduler_emits_dial_requested() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::ZERO)
  .with_gossip_interval(Duration::ZERO)
  .with_push_pull_interval(Duration::from_secs(30));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);

  let t0 = Instant::now();

  // Add one live peer (admitted synchronously — no AliveDelegate).
  process_alive_auto(&mut e, alive("peer", 7947, 1), false, t0);
  // Drain any residual events and transmits from alive processing.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Arm push/pull scheduler at t0.
  e.next_pushpull = Some(t0);
  e.handle_timeout(t0);

  // Expect a DialRequested event (emitted by start_push_pull).
  let mut found_dial = false;
  while let Some(ev) = e.poll_event() {
    if matches!(ev, Event::DialRequested(..)) {
      found_dial = true;
    }
  }
  assert!(found_dial, "push/pull scheduler must emit DialRequested");

  // 1 live peer ≤ 32 → no scaling, interval stays 30s.
  assert_eq!(
    e.next_pushpull,
    Some(t0 + Duration::from_secs(30)),
    "next_pushpull must advance by push_pull_scale(interval, num_live)"
  );
}

#[test]
fn pushpull_scheduler_scales_interval_for_large_cluster() {
  // Verify push_pull_scale integration: 33 live nodes → interval * 2.
  let base = Duration::from_secs(30);
  assert_eq!(push_pull_scale(base, 33), Duration::from_secs(60));
  assert_eq!(push_pull_scale(base, 65), Duration::from_secs(90));
  assert_eq!(push_pull_scale(base, 32), Duration::from_secs(30));
}

#[test]
fn pushpull_scheduler_no_op_with_no_peers() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::ZERO)
  .with_gossip_interval(Duration::ZERO)
  .with_push_pull_interval(Duration::from_secs(30));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  e.next_pushpull = Some(t0);
  e.handle_timeout(t0);
  // No peers → no DialRequested.
  while let Some(ev) = e.poll_event() {
    assert!(
      !matches!(ev, Event::DialRequested(..)),
      "should not dial when no peers available"
    );
  }
  // Deadline still advances (num_live=0 → no scaling, stays 30s).
  assert_eq!(e.next_pushpull, Some(t0 + Duration::from_secs(30)));
}

// ─────────────── Lifecycle gating ────────────────────────────────────────

#[test]
fn leave_clears_scheduler_fields() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::from_millis(100))
  .with_gossip_interval(Duration::from_millis(50))
  .with_push_pull_interval(Duration::from_secs(30));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  e.start_scheduling(t0);
  assert!(e.next_probe.is_some());
  assert!(e.next_gossip.is_some());
  assert!(e.next_pushpull.is_some());

  e.leave(t0).unwrap();
  assert!(
    e.next_probe.is_none(),
    "probe scheduler must be cleared on leave"
  );
  assert!(
    e.next_gossip.is_none(),
    "gossip scheduler must be cleared on leave"
  );
  assert!(
    e.next_pushpull.is_none(),
    "pushpull scheduler must be cleared on leave"
  );
}

#[test]
fn poll_timeout_returns_none_after_leave_with_no_other_timers() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::from_millis(100))
  .with_gossip_interval(Duration::from_millis(50))
  .with_push_pull_interval(Duration::from_secs(30));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  e.start_scheduling(t0);
  e.leave(t0).unwrap();
  // Drain leave-related events/transmits.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  // No suspicion/probe/forward timers active → None.
  assert!(
    e.poll_timeout().is_none(),
    "poll_timeout must be None after leave when no other timers active"
  );
}

#[test]
fn handle_timeout_no_op_scheduler_after_leave() {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::from_millis(50))
  .with_gossip_interval(Duration::from_millis(50))
  .with_push_pull_interval(Duration::from_secs(30));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  // Manually set deadlines in the past and then leave.
  e.next_probe = Some(t0);
  e.next_gossip = Some(t0);
  e.next_pushpull = Some(t0);
  e.leave(t0).unwrap();
  // Drain leave outputs.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Advance time well past all deadlines.
  e.handle_timeout(t0 + Duration::from_secs(60));
  assert!(
    e.poll_transmit().is_none(),
    "no probe/gossip/pushpull transmit after leave"
  );
  let mut found_dial = false;
  while let Some(ev) = e.poll_event() {
    if matches!(ev, Event::DialRequested(..)) {
      found_dial = true;
    }
  }
  assert!(!found_dial, "no DialRequested after leave");
}

#[test]
fn handle_packet_dispatches_ping_to_ack() {
  use Instant;
  use Message;
  use Node;
  use Ping;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let from: SocketAddr = "127.0.0.1:7001".parse().unwrap();
  let local: SocketAddr = "127.0.0.1:7000".parse().unwrap();
  let local_id = SmolStr::new("local");

  let ping = Ping::new(
    42,
    Node::new(SmolStr::new("peer"), from),
    Node::new(local_id, local),
  );
  e.handle_packet(from, Message::Ping(ping), Instant::now());

  let tx = e.poll_transmit().expect("Ack transmit expected");
  let Transmit::Packet(p) = tx else {
    panic!("unexpected Compound transmit");
  };
  let (to, message) = p.into_parts();
  assert_eq!(to, from);
  assert!(matches!(message, Message::Ack(_)));
}

#[test]
fn handle_packet_ignores_push_pull() {
  use Instant;
  use Message;
  use PushPull;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let from: SocketAddr = "127.0.0.1:7001".parse().unwrap();
  let pp = PushPull::new(false, core::iter::empty());
  e.handle_packet(from, Message::PushPull(pp), Instant::now());
  assert!(e.poll_transmit().is_none());
}

// ── Push/pull liveness + graceful-leave observability ───────────────────────

/// Push/pull must advertise the live `LocalNodeState` liveness, not the
/// embedded `NodeState` snapshot (frozen at the last Alive, since
/// `set_server` only runs on Alive). A Suspect member must not be serialized
/// as Alive — doing so resurrects it on the peer via `merge_state`.
#[test]
fn push_pull_serializes_live_liveness_not_frozen_alive() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  // Suspect updates LocalNodeState only; the embedded NodeState stays Alive.
  e.process_suspect(suspect("bob", "carol", 1), Instant::now());
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect)
  );

  let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, Instant::now());
  let encoded = e
    .pending_stream_intents
    .get(&id)
    .expect("push/pull intent present")
    .encoded
    .clone();
  let (_, msg) =
    crate::wire::decode_message::<SmolStr, SocketAddr>(&encoded).expect("decode push/pull");
  let Message::PushPull(pp) = msg else {
    panic!("expected PushPull message");
  };
  let (_, _, states) = pp.into_components();
  let bob = states
    .iter()
    .find(|s| s.id_ref() == &SmolStr::new("bob"))
    .expect("bob present in push/pull states");
  assert_eq!(
    bob.state(),
    State::Suspect,
    "push/pull must serialize live liveness, not the frozen Alive snapshot"
  );
}

/// Graceful leave must be observable through `poll_transmit` (the path
/// real drivers drain). The gossip broadcast queue alone is insufficient:
/// it stops being drained once `lifecycle == Left`.
#[test]
fn leave_emits_dead_self_via_poll_transmit() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  e.leave(Instant::now()).expect("leave ok");

  let bob_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
  let local_id = SmolStr::new("local");
  let mut saw_dead_self_to_bob = false;
  while let Some(tx) = e.poll_transmit() {
    if let Transmit::Packet(p) = tx {
      let (to, message) = p.into_parts();
      if to == bob_addr {
        if let Message::Dead(d) = message {
          // Intentional-leave sentinel: node == from == local id.
          assert_eq!(d.node_ref(), &local_id);
          assert_eq!(d.from_ref(), &local_id);
          saw_dead_self_to_bob = true;
        }
      }
    }
  }
  assert!(
    saw_dead_self_to_bob,
    "leave() must fan the dead-self out to live peers via poll_transmit"
  );
}

/// A remote `State::Left` learned via push/pull must become `State::Dead`:
/// `State::Left` is reserved for the genuine `node == from` self-leave
/// sentinel. Because the resulting state is Dead — not Left — a
/// different-address Alive with the default `dead_node_reclaim_time == 0`
/// must NOT hijack the node id (it conflicts).
#[test]
fn merge_remote_left_marks_dead_and_blocks_reclaim() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());

  e.merge_state(&[pns("bob", 7001, 2, State::Left)], Instant::now());
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Dead),
    "remote Left via anti-entropy must become Dead, not Left"
  );

  // Dead + default dead_node_reclaim_time (ZERO) ⇒ a different-address
  // Alive must NOT reclaim the id; the old address is kept (conflict).
  // State::Left would have allowed an immediate hijack — the
  // reclaim-bypass this fix closes.
  process_alive_auto(&mut e, alive("bob", 7777, 3), false, Instant::now());
  assert_eq!(
    e.member(&SmolStr::new("bob")).unwrap().address_ref().port(),
    7001,
    "Dead node id must not be reclaimable at a new address with reclaim_time=0"
  );
}

// ── Post-leave self-resurrection guards ──────────────────────────────────────

/// `update_meta` after leave must return `NotRunning`. Otherwise it bumps
/// the incarnation and broadcasts a higher-incarnation Alive that
/// resurrects the left node.
#[test]
fn update_meta_after_leave_returns_not_running() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  e.leave(Instant::now()).expect("leave ok");
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  let meta = Meta::try_from(Bytes::from_static(b"v2")).unwrap();
  assert!(matches!(e.update_meta(meta), Err(Error::NotRunning)));
  // No resurrecting Alive may have been produced.
  assert!(
    e.poll_transmit().is_none(),
    "update_meta after leave must not broadcast an Alive"
  );
  assert_eq!(e.member_liveness(&SmolStr::new("local")), Some(State::Left));
}

/// A self-Alive received after the local node has Left (e.g. a peer
/// echoing our pre-leave state) must be ignored — `process_alive`
/// suppresses self-handling for both Leaving and Left, not just Leaving.
#[test]
fn self_alive_after_left_is_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  e.leave(Instant::now()).expect("leave ok");
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Inbound self-Alive at a much higher incarnation.
  e.process_alive(alive("local", 7000, 99), false, Instant::now());
  assert!(
    e.poll_event().is_none(),
    "self-Alive after Left must not emit any event"
  );
  assert_eq!(
    e.member_liveness(&SmolStr::new("local")),
    Some(State::Left),
    "self-Alive after Left must not resurrect the local node"
  );
}

/// A Suspect about the local node after Left must not resurrect it
/// (refute is a no-op once not Running).
#[test]
fn suspect_about_self_after_left_does_not_resurrect() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  e.leave(Instant::now()).expect("leave ok");
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  e.process_suspect(suspect("local", "carol", 50), Instant::now());
  assert!(
    e.poll_transmit().is_none(),
    "Suspect about self after Left must not trigger a refute Alive"
  );
  assert_eq!(e.member_liveness(&SmolStr::new("local")), Some(State::Left));
}

/// Long-dead members must be pruned by the probe-cycle `reset_nodes`
/// sweep (production drivers only call `handle_timeout`). The sweep is
/// triggered when the probe index wraps a full round-robin pass.
#[test]
fn probe_cycle_prunes_long_dead_nodes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_interval(Duration::from_millis(10))
      .with_gossip_to_the_dead_time(Duration::from_millis(1)),
  );
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_dead(dead("bob", "carol", 2), t0);
  assert_eq!(e.member_liveness(&SmolStr::new("bob")), Some(State::Dead));

  e.start_scheduling(t0);
  // Several probe ticks well past the gossip-to-the-dead window: a full
  // round-robin pass elapses and reset_nodes prunes bob.
  for i in 1..=6u64 {
    e.handle_timeout(t0 + Duration::from_secs(i));
  }
  assert!(
    e.member(&SmolStr::new("bob")).is_none(),
    "long-dead member must be pruned by the probe-cycle reset (no external GC)"
  );
}

// ── "Gossip to the dead" window + queue preservation ─────────────────────────

/// A recently-Dead peer (within `gossip_to_the_dead_time`) must still be a
/// gossip target — the SWIM "gossip to the dead" path that lets a falsely-
/// dead node hear the accusation and refute before GC.
#[test]
fn gossip_targets_recently_dead_peer_within_window() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_dead(dead("bob", "carol", 2), t0); // bob Dead, state_change = t0
  assert_eq!(e.member_liveness(&SmolStr::new("bob")), Some(State::Dead));
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  e.broadcast_message(SmolStr::new("zz"), Message::Alive(alive("zz", 7099, 1)));
  // Fire gossip immediately (now == bob.state_change, well within the
  // default 30 s gossip_to_the_dead_time).
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let bob_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
  let mut gossiped_to_bob = false;
  while let Some(tx) = e.poll_transmit() {
    let to = match &tx {
      Transmit::Packet(p) => p.to_ref(),
      Transmit::Compound(cmp) => cmp.to_ref(),
    };
    if *to == bob_addr {
      gossiped_to_bob = true;
    }
  }
  assert!(
    gossiped_to_bob,
    "recently-dead peer must receive gossip so it can refute a false death"
  );
}

/// An aged-Dead peer (beyond `gossip_to_the_dead_time`) is NOT a gossip
/// target; and when no eligible target exists the broadcast queue must
/// be left intact (retransmit counters not advanced with zero packets
/// emitted) — broadcasts are fetched per selected target.
#[test]
fn gossip_skips_aged_dead_and_preserves_queue_when_no_targets() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_dead(dead("bob", "carol", 2), t0); // bob Dead at t0
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  e.broadcast_message(SmolStr::new("zz"), Message::Alive(alive("zz", 7099, 1)));
  let queued_before = e.broadcast_queue_len();
  assert!(queued_before > 0, "precondition: a broadcast is queued");

  // Fire gossip 31 s later — beyond the default 30 s window. bob is the
  // only non-local member and is now aged-dead ⇒ no eligible target.
  let later = t0 + Duration::from_secs(31);
  e.next_gossip = Some(later);
  e.handle_timeout(later);

  let bob_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
  while let Some(tx) = e.poll_transmit() {
    let to = match &tx {
      Transmit::Packet(p) => p.to_ref(),
      Transmit::Compound(cmp) => cmp.to_ref(),
    };
    assert_ne!(*to, bob_addr, "aged-dead peer must not receive gossip");
  }
  assert_eq!(
    e.broadcast_queue_len(),
    queued_before,
    "broadcast queue must be untouched when there is no eligible target"
  );
}

// ── Synchronous-admission ordering invariants ────────────────────────────────
//
// A deferred-admission design (Alive sitting in a pending-decision buffer
// while a Dead/Suspect races in before the id is in `members`) would drop
// the failure signal and then resurrect the node on Alive acceptance. The
// architectural choice here is synchronous, inline admission — the race
// window does not exist. These tests prove that a Dead/Suspect following
// an admitted Alive is never lost (no resurrection). Rejection is an
// `AliveDelegate` veto — see `alive_delegate_reject_drops_the_message`.

/// A Dead arriving right after an (admitted) Alive must win — no resurrection.
#[test]
fn alive_then_dead_marks_dead() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  e.process_alive(alive("bob", 7001, 1), false, t0);
  assert!(
    e.member(&SmolStr::new("bob")).is_some(),
    "bob admitted synchronously (no AliveDelegate)"
  );
  e.process_dead(dead("bob", "carol", 2), t0);
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Dead),
    "a Dead following an admitted Alive must not be lost"
  );
}

// ─────────────── Piggyback producers + MTU byte-budget ────────────────────
//
// The gossip scheduler and the probe path are the only two producers that
// co-send >= 2 messages to one peer. By the >= 2 rule a single queued
// message stays a byte-identical plain `Transmit::Packet`; two or more ride
// ONE `Transmit::Compound` datagram (legacy `makeCompoundMessage`). The MTU
// regression assembles the emitted datagram via the real wire path and
// bounds it <= 1400 B.

/// Build a one-peer, gossip_nodes=1 endpoint armed to fire gossip at `t0`
/// with the broadcast/user queues drained empty. Returns `(endpoint, t0)`.
fn gossip_harness_one_target() -> (Endpoint<SmolStr, SocketAddr>, Instant) {
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::ZERO)
  .with_gossip_interval(Duration::from_millis(50))
  .with_gossip_nodes(1)
  .with_push_pull_interval(Duration::ZERO);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  // One Alive peer so a gossip target exists.
  process_alive_auto(&mut e, alive("alice", 7947, 1), false, t0);
  // process_alive + new() queue membership Alive broadcasts and events;
  // clear them so the queues start empty and the test controls the count.
  // `drain_broadcasts` only bumps the retransmit counter (a broadcast is
  // re-queued until its ceiling), so reset the queue outright.
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  e.broadcast.reset();
  assert!(
    e.broadcast.is_empty(),
    "broadcast queue must start empty for the producer tests"
  );
  (e, t0)
}

/// Drain every transmit the gossip fire produced.
fn collect_transmits(e: &mut Endpoint<SmolStr, SocketAddr>) -> Vec<Transmit<SmolStr, SocketAddr>> {
  let mut v = Vec::new();
  while let Some(tx) = e.poll_transmit() {
    v.push(tx);
  }
  v
}

/// Assemble the on-wire datagram for an emitted gossip transmit via the
/// real `memberlist-wire` path (the same hops `crate::wire` composes).
fn assembled_datagram_len(tx: &Transmit<SmolStr, SocketAddr>) -> usize {
  match tx {
    Transmit::Compound(cmp) => {
      let anys: Vec<_> = cmp
        .messages_slice()
        .iter()
        .map(|m| crate::message_to_any(m).expect("bridge"))
        .collect();
      crate::framing::encode_compound(&anys)
        .expect("encode compound")
        .len()
    }
    Transmit::Packet(p) => {
      let any = crate::message_to_any(p.message_ref()).expect("bridge");
      crate::framing::encode_message(&any)
        .expect("encode message")
        .len()
    }
  }
}

/// (a) gossip with 0 queued broadcasts ⇒ NO transmit emitted.
#[test]
fn gossip_zero_broadcasts_emits_no_transmit() {
  let (mut e, t0) = gossip_harness_one_target();
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);
  assert!(
    e.poll_transmit().is_none(),
    "empty broadcast/user queue ⇒ no gossip transmit"
  );
  assert_eq!(
    e.next_gossip,
    Some(t0 + Duration::from_millis(50)),
    "deadline still advances"
  );
}

/// (b) gossip with exactly 1 queued broadcast ⇒ Transmit::Packet (NOT
///     Compound) — a single message stays a byte-identical plain frame.
#[test]
fn gossip_single_broadcast_emits_packet_not_compound() {
  let (mut e, t0) = gossip_harness_one_target();
  e.broadcast_message(SmolStr::new("bc0"), Message::Alive(alive("bc0", 8000, 1)));
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let transmits = collect_transmits(&mut e);
  assert_eq!(
    transmits.len(),
    1,
    "exactly one transmit to the single gossip target, got {transmits:?}"
  );
  match &transmits[0] {
    Transmit::Packet(p) => {
      let message = p.message_ref();
      assert!(
        matches!(message, Message::Alive(_)),
        "single broadcast must be a plain Packet(Alive), got {message:?}"
      );
    }
    Transmit::Compound(c) => {
      panic!("single broadcast must NOT be wrapped in a Compound: {c:?}")
    }
  }
}

/// (c) gossip with >=2 queued broadcasts, ONE target ⇒ exactly one
///     Transmit::Compound whose .messages.len() == number queued.
#[test]
fn gossip_two_plus_broadcasts_emit_one_compound_per_target() {
  let (mut e, t0) = gossip_harness_one_target();
  let n = 4usize;
  for i in 0..n {
    let id: &'static str = Box::leak(format!("bc{i}").into_boxed_str());
    e.broadcast_message(
      SmolStr::new(id),
      Message::Alive(alive(id, 8000 + i as u16, 1)),
    );
  }
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let transmits = collect_transmits(&mut e);
  assert_eq!(
    transmits.len(),
    1,
    "exactly ONE transmit (compound) for the single target, got {transmits:?}"
  );
  match &transmits[0] {
    Transmit::Compound(cmp) => {
      let messages = cmp.messages_slice();
      assert_eq!(
        messages.len(),
        n,
        "all {n} queued broadcasts must ride one compound datagram"
      );
      assert!(
        messages.iter().all(|m| matches!(m, Message::Alive(_))),
        "every compounded message is one of the queued Alive broadcasts"
      );
    }
    Transmit::Packet(p) => {
      panic!(">=2 broadcasts must be a single Compound, got Packet: {p:?}")
    }
  }
}

/// (d) probe of an Alive peer (no buddy) ⇒ Transmit::Packet(Ping).
#[test]
fn probe_without_buddy_emits_packet() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  assert!(e.start_probe(Instant::now()));
  let tx = e.poll_transmit().expect("probe transmit expected");
  match tx {
    Transmit::Packet(p) => {
      assert_eq!(p.to_ref().port(), 7001);
      let message = p.message_ref();
      assert!(
        matches!(message, Message::Ping(_)),
        "probe of an Alive peer is a plain Packet(Ping), got {message:?}"
      );
    }
    Transmit::Compound(c) => panic!("Alive-peer probe must not be Compound: {c:?}"),
  }
  assert!(
    e.poll_transmit().is_none(),
    "exactly one transmit for an Alive (no-buddy) probe"
  );
}

/// (e) probe of a Suspect peer ⇒ exactly one Transmit::Compound with
///     messages == [Ping, Suspect] in THAT order (ping first).
#[test]
fn probe_with_buddy_suspect_emits_one_compound_ping_then_suspect() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  // Single peer so the round-robin probe target is deterministically bob.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "carol", 1), t0);
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect)
  );
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  assert!(e.start_probe(t0));
  let transmits = collect_transmits(&mut e);
  assert_eq!(
    transmits.len(),
    1,
    "Ping + buddy Suspect must be co-sent as ONE compound datagram, got {transmits:?}"
  );
  match &transmits[0] {
    Transmit::Compound(cmp) => {
      assert_eq!(
        cmp.to_ref().port(),
        7001,
        "compound addressed to the Suspect target"
      );
      let messages = cmp.messages_slice();
      assert_eq!(messages.len(), 2, "exactly [Ping, Suspect]");
      assert!(
        matches!(messages[0], Message::Ping(_)),
        "ping MUST be first, got {:?}",
        messages[0]
      );
      match &messages[1] {
        Message::Suspect(s) => {
          assert_eq!(
            s.node_ref(),
            &SmolStr::new("bob"),
            "buddy Suspect must be about the probe target"
          );
        }
        other => panic!("second message must be Suspect, got {other:?}"),
      }
    }
    Transmit::Packet(p) => {
      panic!("Suspect-target probe must be one Compound(Ping,Suspect), got Packet: {p:?}")
    }
  }
}

/// The probe Ping+buddy-Suspect compound must be MTU-size-gated. Node id
/// `I` is unbounded, so a large id makes each of Ping / Suspect a valid
/// lone datagram (<= 1400) while their compound exceeds 1400. Without the
/// gate the producer emits one unsplittable over-MTU `Compound`
/// (fragmented / dropped ⇒ buddy refutation lost). With it: two
/// `Packet`s, Ping then Suspect — separate <= max_packet_size datagrams
/// rather than one oversize.
#[test]
fn probe_buddy_compound_over_mtu_splits_into_two_packets() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  // 900-byte id: lone Ping ≈ lone Suspect ≈ ~930 B (<= 1400 each), but
  // their compound ≈ ~1.8 KB (> 1400). Comfortable margins both sides.
  let big = "b".repeat(900);
  process_alive_auto(&mut e, alive(big.as_str(), 7001, 1), false, t0);
  e.process_suspect(suspect(big.as_str(), "carol", 1), t0);
  assert_eq!(
    e.member_liveness(&SmolStr::new(big.as_str())),
    Some(State::Suspect)
  );
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  assert!(e.start_probe(t0));
  let txs = collect_transmits(&mut e);
  assert_eq!(
    txs.len(),
    2,
    "over-MTU Ping+Suspect must split into TWO Packets, got {txs:?}"
  );
  match (&txs[0], &txs[1]) {
    (Transmit::Packet(p0), Transmit::Packet(p1)) => {
      let (to0, message0) = (p0.to_ref(), p0.message_ref());
      let (to1, message1) = (p1.to_ref(), p1.message_ref());
      assert!(
        matches!(message0, Message::Ping(_)),
        "first packet must be Ping, got {message0:?}"
      );
      let s = match message1 {
        Message::Suspect(s) => s,
        other => panic!("second packet must be Suspect, got {other:?}"),
      };
      assert_eq!(to0.port(), 7001, "Ping to the probe target");
      assert_eq!(to1.port(), 7001, "Suspect to the probe target");
      assert_eq!(
        s.node_ref(),
        &SmolStr::new(big.as_str()),
        "buddy Suspect must be about the probe target"
      );
    }
    other => {
      panic!("expected [Packet(Ping), Packet(Suspect)] in order, got {other:?}")
    }
  }
  // Precondition: each lone datagram IS individually sendable (<= 1400) —
  // the genuine splittable case — yet together they exceed one datagram,
  // so the split was necessary (not gratuitous).
  let l0 = assembled_datagram_len(&txs[0]);
  let l1 = assembled_datagram_len(&txs[1]);
  assert!(
    l0 <= 1400 && l1 <= 1400,
    "each lone Packet must be <= MTU (l0={l0}, l1={l1})"
  );
  assert!(
    l0 + l1 > 1400,
    "precondition: combined would exceed one datagram (l0+l1={})",
    l0 + l1
  );
}

/// (f) LOAD-BEARING: flood the broadcast queue with many large broadcasts,
///     fire gossip, take the emitted transmit, ASSEMBLE its encoded
///     datagram via the real wire path, assert encoded_len <= 1400. An
///     under-counted compound overhead would break this.
#[test]
fn gossip_compound_datagram_never_exceeds_mtu() {
  let (mut e, t0) = gossip_harness_one_target();
  // Many broadcasts, each large (a 500 B node-meta blob, just under
  // Meta::MAX_SIZE=512) so naive packing would blow well past the 1400
  // sub-MTU ceiling if the compound header / per-part length prefixes were
  // not charged against the budget.
  for i in 0..64usize {
    let id: &'static str = Box::leak(format!("big-broadcast-node-{i:04}").into_boxed_str());
    let big_meta = Meta::try_from(bytes::Bytes::from(vec![0x5a_u8; 500]))
      .expect("500 B meta is within Meta::MAX_SIZE (512)");
    let av = Alive::new(
      1,
      Node::new(
        SmolStr::new(id),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000 + i as u16),
      ),
    )
    .with_meta(big_meta)
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1);
    e.broadcast_message(SmolStr::new(id), Message::Alive(av));
  }
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let transmits = collect_transmits(&mut e);
  assert_eq!(
    transmits.len(),
    1,
    "single gossip target ⇒ exactly one transmit (Packet or Compound), got {}",
    transmits.len()
  );
  let assembled_len = assembled_datagram_len(&transmits[0]);
  assert!(
    assembled_len <= 1400,
    "assembled gossip datagram {assembled_len} B exceeds 1400 MTU budget"
  );
  // Load-bearing: this regression only guards the MTU risk if it actually
  // exercises a real multi-part compound. A degraded 1-part Packet would
  // silently pass `assembled_len <= 1400` while no longer testing anything.
  match &transmits[0] {
    Transmit::Compound(cmp) => {
      let n = cmp.messages_slice().len();
      assert!(
        n >= 2,
        "MTU regression must exercise a real multi-part compound; got {} parts",
        n
      );
    }
    Transmit::Packet(_) => panic!(
      "MTU regression degraded to a 1-part Packet — no longer load-bearing; \
       increase the broadcast flood so >= 2 parts are selected"
    ),
  }
}

/// (g) CRITICAL regression: the combined membership + user-broadcast path.
///     `drain_user_broadcasts` MUST share the SAME compound_budget the
///     membership drain consumed and be charged its assembled part size;
///     a fresh 1400-byte budget that ignored already-consumed membership
///     bytes AND charged only raw payload bytes would let the single
///     emitted `Transmit::Compound` (membership ++ user, never MTU-split)
///     blow to ~2600-2800 B (~2x MTU). The whole assembled datagram is
///     provably <= 1400.
#[test]
fn gossip_membership_plus_user_compound_within_mtu() {
  let (mut e, t0) = gossip_harness_one_target();
  // Many membership broadcasts, each large, so the membership compound is
  // packed near its 1394 budget (same construction as the MTU regression).
  for i in 0..64usize {
    let id: &'static str = Box::leak(format!("mp-broadcast-node-{i:04}").into_boxed_str());
    let big_meta = Meta::try_from(bytes::Bytes::from(vec![0x5a_u8; 500]))
      .expect("500 B meta is within Meta::MAX_SIZE (512)");
    let av = Alive::new(
      1,
      Node::new(
        SmolStr::new(id),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000 + i as u16),
      ),
    )
    .with_meta(big_meta)
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1);
    e.broadcast_message(SmolStr::new(id), Message::Alive(av));
  }
  // AND several large user broadcasts. A separate fresh 1400-byte budget
  // (raw-byte charged) concatenated onto the already-near-1394 membership
  // compound would blow the single datagram to ~2600-2800 B — exactly
  // what sharing one compound_budget prevents.
  for i in 0..6usize {
    let payload = vec![0xa5_u8; 400 + i * 100]; // ~400..900 B each
    e.queue_user_broadcast(bytes::Bytes::from(payload)).unwrap();
  }
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let transmits = collect_transmits(&mut e);
  assert_eq!(
    transmits.len(),
    1,
    "single gossip target ⇒ exactly one transmit (membership ++ user), got {}",
    transmits.len()
  );
  // Assemble via the SAME real wire path the other MTU tests use.
  let assembled_len = assembled_datagram_len(&transmits[0]);
  assert!(
    assembled_len <= 1400,
    "combined membership+user gossip datagram {assembled_len} B exceeds the \
     1400 MTU budget (a double-budget bug would exceed this)"
  );
  // Non-vacuous: the combined path must actually produce a real multi-part
  // compound, not a 1-part Packet that would pass the bound trivially.
  match &transmits[0] {
    Transmit::Compound(cmp) => {
      let n = cmp.messages_slice().len();
      assert!(
        n >= 2,
        "combined membership+user path must exercise a real multi-part \
         compound; got {} parts",
        n
      );
    }
    Transmit::Packet(_) => panic!(
      "combined membership+user regression degraded to a 1-part Packet — \
       not exercising the multi-part compound it must guard"
    ),
  }
}

/// A user payload over the gossip compound-part budget is rejected at
/// `queue_user_broadcast` rather than stored: the gossip drain selects user
/// payloads only through the compound-part tier walk, so an over-budget payload
/// is never selected, and storing it would falsely report success and leave it
/// queued forever. (The 2000-byte payload here also exceeds `gossip_mtu`; the
/// gate rejects on the compound-part budget regardless of lone-datagram fit.)
/// A valid payload queued after the rejected one is unaffected and still goes out.
#[test]
fn gossip_oversized_user_broadcast_rejected_at_enqueue() {
  let (mut e, t0) = gossip_harness_one_target();
  // 2000 B exceeds the compound-part budget (default 1383), so no gossip tick
  // could ever select it; it is rejected without being stored.
  assert!(matches!(
    e.queue_user_broadcast(bytes::Bytes::from(vec![0x5a_u8; 2000])),
    Err(Error::UserBroadcastExceedsMtu(_))
  ));
  assert_eq!(
    e.user_broadcast_queue_len(),
    0,
    "a rejected oversized payload must not be stored"
  );
  // A small, valid payload is accepted and gossiped normally.
  e.queue_user_broadcast(bytes::Bytes::from(vec![0xc3_u8; 64]))
    .unwrap();
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let txs = collect_transmits(&mut e);
  assert_eq!(txs.len(), 1, "the valid payload must still be sent");
  match &txs[0] {
    Transmit::Packet(p) => match p.message_ref() {
      Message::UserData(b) => {
        assert_eq!(b.len(), 64, "the valid small payload must be emitted")
      }
      other => panic!("expected Packet(UserData(64)), got Packet({other:?})"),
    },
    other => panic!("expected Packet(UserData(64)), got {other:?}"),
  }
  assert_eq!(
    e.user_broadcast_queue_len(),
    1,
    "the emitted payload is retransmit-counted, so it stays queued for the next round"
  );
}

/// A near-MTU MEMBERSHIP broadcast whose real plain-frame length is in
/// (1389, 1400] is stranded by the compound-reduced drain
/// (`fit = compound_budget 1394 - per-part overhead 5 = 1389`) yet is a
/// valid lone `Packet` (<= 1400). The lone-Packet rescue must cover
/// membership too (node id `I: Data` is unbounded, so a large id reaches
/// this range even with `Meta` capped at 512). Without this, the rescue
/// only scanned user broadcasts and the Alive was never emitted.
#[test]
fn gossip_lone_near_mtu_membership_broadcast_emits_packet() {
  let (mut e, t0) = gossip_harness_one_target();
  let meta = Meta::try_from(bytes::Bytes::from(vec![0x5a_u8; 500]))
    .expect("500 B within META_MAX_SIZE (512)");
  // Size an Alive so its real wire frame lands in (1389, 1400]. Node id
  // is unbounded; grow/shrink it toward the window (encoded ≈ linear in
  // id length). Deterministic, bounded.
  let mut id_len = 875usize;
  let (id, av, frame_len) = loop {
    let id = SmolStr::new("n".repeat(id_len));
    let av = Alive::new(
      7,
      Node::new(
        id.clone(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9100),
      ),
    )
    .with_meta(meta.clone())
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1);
    let any = crate::message_to_any(&Message::Alive(av.clone())).expect("bridge");
    let len = crate::framing::encode_message(&any).expect("encode").len();
    if (1390..=1400).contains(&len) {
      break (id, av, len);
    }
    if len > 1400 {
      id_len -= 1;
    } else {
      id_len += 1;
    }
    assert!(
      (1..4000).contains(&id_len),
      "failed to size an Alive into (1389, 1400]"
    );
  };
  assert!(
    (1390..=1400).contains(&frame_len),
    "test must exercise the stranded near-MTU range, got {frame_len}"
  );
  e.broadcast_message(id, Message::Alive(av));
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let txs = collect_transmits(&mut e);
  assert_eq!(
    txs.len(),
    1,
    "a near-MTU membership broadcast must be sent as exactly one transmit, got {}",
    txs.len()
  );
  match &txs[0] {
    Transmit::Packet(p) => assert!(
      matches!(p.message_ref(), Message::Alive(_)),
      "expected lone Packet(Alive), got Packet({:?})",
      p.message_ref()
    ),
    other => panic!("expected lone Packet(Alive), got {other:?}"),
  }
  let assembled = assembled_datagram_len(&txs[0]);
  assert!(
    (1390..=1400).contains(&assembled),
    "the emitted lone Packet must be the stranded near-MTU frame, got {assembled}"
  );
}

/// The lone-Packet rescue must be SWIM-priority — a stranded near-MTU
/// membership broadcast must NOT be skipped just because a small user
/// broadcast is queued. The rescue must be gated BEFORE draining user
/// data, not after; a small user payload must not make `all_broadcasts`
/// non-empty and skip the membership rescue (continuous user traffic would
/// starve the membership update indefinitely). Correct regime: rescue a
/// stranded membership broadcast first; the user payload waits for the
/// next tick (best-effort, deferred not dropped).
#[test]
fn gossip_near_mtu_membership_outranks_user_broadcast() {
  let (mut e, t0) = gossip_harness_one_target();
  let meta = Meta::try_from(bytes::Bytes::from(vec![0x5a_u8; 500]))
    .expect("500 B within META_MAX_SIZE (512)");
  // Size an Alive so its real wire frame lands in (1389, 1400] — stranded
  // by the compound-reduced drain but a valid lone Packet.
  let mut id_len = 875usize;
  let (id, av) = loop {
    let id = SmolStr::new("n".repeat(id_len));
    let av = Alive::new(
      7,
      Node::new(
        id.clone(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9100),
      ),
    )
    .with_meta(meta.clone())
    .with_protocol_version(ProtocolVersion::V1)
    .with_delegate_version(DelegateVersion::V1);
    let any = crate::message_to_any(&Message::Alive(av.clone())).expect("bridge");
    let len = crate::framing::encode_message(&any).expect("encode").len();
    if (1390..=1400).contains(&len) {
      break (id, av);
    }
    if len > 1400 {
      id_len -= 1;
    } else {
      id_len += 1;
    }
    assert!(
      (1..4000).contains(&id_len),
      "failed to size an Alive into (1389, 1400]"
    );
  };
  e.broadcast_message(id, Message::Alive(av));
  // A small user broadcast that DOES fit the user budget — without the
  // SWIM-priority gate this made all_broadcasts non-empty and skipped
  // the membership rescue.
  e.queue_user_broadcast(bytes::Bytes::from(vec![0x11_u8; 32]))
    .unwrap();
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let txs = collect_transmits(&mut e);
  assert_eq!(
    txs.len(),
    1,
    "exactly one transmit — the SWIM-priority membership Packet, got {}",
    txs.len()
  );
  match &txs[0] {
    Transmit::Packet(p) => assert!(
      matches!(p.message_ref(), Message::Alive(_)),
      "SWIM priority: membership must outrank best-effort user data; got Packet({:?})",
      p.message_ref()
    ),
    other => panic!("SWIM priority: membership must outrank best-effort user data; got {other:?}"),
  }
  assert_eq!(
    e.user_broadcast_queue_len(),
    1,
    "the user broadcast must remain queued (deferred behind the membership update)"
  );
}

/// Same, for Suspect.
#[test]
fn alive_then_suspect_marks_suspect() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  e.process_alive(alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "carol", 2), t0);
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect),
    "a Suspect following an admitted Alive must not be lost"
  );
}

/// `EndpointOptions::new` seeds `gossip_mtu` to [`DEFAULT_GOSSIP_MTU`] (1400),
/// matching the legacy memberlist sub-MTU ceiling. Operators that bypass the
/// builder still get the safe LAN default.
#[test]
fn endpoint_config_gossip_mtu_defaults_to_1400() {
  use crate::config::DEFAULT_GOSSIP_MTU;
  let c: EndpointOptions<SmolStr, SocketAddr> = EndpointOptions::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  );
  assert_eq!(
    c.gossip_mtu(),
    DEFAULT_GOSSIP_MTU,
    "EndpointOptions::new must seed gossip_mtu to DEFAULT_GOSSIP_MTU so a \
     bypassed builder still gets the safe LAN default",
  );
  assert_eq!(
    DEFAULT_GOSSIP_MTU, 1400,
    "DEFAULT_GOSSIP_MTU must remain 1400 — operators tune from this anchor",
  );
}

// ─────────────── Application ping: PingId token + PingFailed event ──────────────────

#[test]
fn app_ping_returns_token_carried_on_completion() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let bob_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let ping_id = e
    .ping(Node::new(SmolStr::new("bob"), bob_addr), t0)
    .expect("issued while running");
  // Drain the Ping transmit and extract the sequence number.
  let seq = match e.poll_transmit().expect("Ping transmit") {
    Transmit::Packet(p) => {
      let (_, message) = p.into_parts();
      if let Message::Ping(ping) = message {
        ping.sequence_number()
      } else {
        panic!("expected Ping message")
      }
    }
    other => panic!("expected Packet transmit, got {other:?}"),
  };
  while e.poll_transmit().is_some() {}

  // Feed back an Ack for the same sequence number.
  e.handle_ack(bob_addr, Ack::new(seq), t0 + Duration::from_millis(10));

  // The resulting PingCompleted must carry the same PingId.
  let completed = core::iter::from_fn(|| e.poll_event())
    .find_map(|ev| {
      if let Event::PingCompleted(c) = ev {
        Some(c)
      } else {
        None
      }
    })
    .expect("PingCompleted event");
  assert_eq!(completed.ping_id(), ping_id);
}

#[test]
fn app_ping_timeout_emits_ping_failed_with_token() {
  let pt = Duration::from_millis(50);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_probe_timeout(pt));
  let bob_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let ping_id = e
    .ping(Node::new(SmolStr::new("bob"), bob_addr), t0)
    .expect("issued while running");
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Advance past probe_timeout without delivering an Ack.
  e.handle_timeout(t0 + pt + Duration::from_millis(1));

  let failed = core::iter::from_fn(|| e.poll_event())
    .find_map(|ev| {
      if let Event::PingFailed(f) = ev {
        Some(f)
      } else {
        None
      }
    })
    .expect("PingFailed event");
  assert_eq!(failed.ping_id(), ping_id);
}

// ─────────────── Directed unreliable user messages ───────────

#[test]
fn send_user_packet_enqueues_one_transmit() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let dest = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  e.send_user_packet(dest, bytes::Bytes::from_static(b"hi"))
    .unwrap();
  match e.poll_transmit() {
    Some(Transmit::Packet(p)) => {
      assert_eq!(p.to_ref(), &dest);
      assert!(matches!(p.message_ref(), Message::UserData(_)));
    }
    other => panic!("expected one Packet transmit, got {other:?}"),
  }
  assert!(e.poll_transmit().is_none());
}

#[test]
fn send_user_packet_rejects_oversize() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let dest = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  // A payload larger than gossip_mtu is always oversize after framing.
  let huge = bytes::Bytes::from(vec![0u8; e.gossip_mtu() + 1]);
  assert!(e.send_user_packet(dest, huge).is_err());
  assert!(e.poll_transmit().is_none());
}

#[test]
fn send_user_packets_compounds_when_multiple() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let dest = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  e.send_user_packets(
    dest,
    &[
      bytes::Bytes::from_static(b"a"),
      bytes::Bytes::from_static(b"b"),
    ],
  )
  .unwrap();
  match e.poll_transmit() {
    Some(Transmit::Compound(c)) => {
      assert_eq!(c.to_ref(), &dest);
      assert_eq!(c.messages_slice().len(), 2);
      assert!(
        c.messages_slice()
          .iter()
          .all(|m| matches!(m, Message::UserData(_)))
      );
    }
    other => panic!("expected a Compound transmit, got {other:?}"),
  }
  assert!(e.poll_transmit().is_none());
}

/// Two messages each larger than `gossip_mtu / 2` cannot be compounded into
/// one datagram once compound framing overhead is included.  `send_user_packets`
/// must reject the call and leave the transmit queue empty.
#[test]
fn send_user_packets_rejects_compound_oversize() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let mtu = e.gossip_mtu();
  // Each individual payload just over half the MTU — two of them will
  // overflow after compound tag + count-prefix + two part-prefixes are added.
  let half_plus = bytes::Bytes::from(vec![0u8; mtu / 2 + 1]);
  let result = e.send_user_packets(
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7003),
    &[half_plus.clone(), half_plus],
  );
  assert!(
    result.is_err(),
    "expected UserPacketExceedsMtu error for compound oversized pair"
  );
  assert!(
    e.poll_transmit().is_none(),
    "transmit queue must remain empty after rejected send_user_packets"
  );
}

/// `with_gossip_mtu` propagates through to `Endpoint::<SmolStr, SocketAddr>::gossip_mtu()`, which is
/// what the composed stream-transport coordinators read for the FSM's
/// plaintext gossip budget. A non-default configured value must reach the
/// inner endpoint without being clamped or overridden by the legacy
/// constant.
#[test]
fn endpoint_config_gossip_mtu_is_propagated_to_endpoint() {
  let c: EndpointOptions<SmolStr, SocketAddr> = EndpointOptions::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  )
  .with_gossip_mtu(1200);
  assert_eq!(c.gossip_mtu(), 1200);
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(c);
  assert_eq!(
    e.gossip_mtu(),
    1200,
    "Endpoint::<SmolStr, SocketAddr>::gossip_mtu() must return the configured value (not the \
     hard-coded legacy 1400) so composed coordinators read the operator's \
     budget",
  );
}

#[test]
fn advertise_ref_returns_configured_address() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert_eq!(
    e.advertise_ref(),
    &SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000)
  );
}

#[test]
fn node_incarnation_tracks_known_member_and_is_none_for_unknown() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert!(
    e.node_incarnation(&SmolStr::new("peer")).is_none(),
    "unknown peer has no incarnation"
  );
  process_alive_auto(&mut e, alive("peer", 7100, 5), false, Instant::now());
  assert_eq!(e.node_incarnation(&SmolStr::new("peer")), Some(5));
  // The local node's incarnation is also reachable through the same accessor.
  assert_eq!(
    e.node_incarnation(&SmolStr::new("local")),
    Some(e.local_incarnation())
  );
}

#[test]
fn requeue_event_pushes_to_the_back_of_the_pending_buffer() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // Drain the construction-time self-join so the only queued event below is the
  // peer join we generate.
  while e.poll_event().is_some() {}
  // Generate one real event (a join), drain it, then re-enqueue it: it must
  // come back out of poll_event unchanged (FIFO push_back).
  process_alive_auto(&mut e, alive("peer", 7100, 1), false, Instant::now());
  let ev = e.poll_event().expect("a NodeJoined event is queued");
  assert!(matches!(ev, Event::NodeJoined(_)));
  e.requeue_event(ev);
  assert!(
    matches!(e.poll_event(), Some(Event::NodeJoined(_))),
    "requeued event must be observable again"
  );
  assert!(e.poll_event().is_none(), "only the one requeued event");
}

#[test]
fn ack_payload_bytes_mirrors_ack_payload_slice() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // Default is empty.
  assert!(e.ack_payload().is_empty());
  assert!(e.ack_payload_bytes().is_empty());
  e.set_ack_payload(Bytes::from_static(b"pong-data"))
    .expect("small payload fits the gossip MTU");
  assert_eq!(e.ack_payload(), b"pong-data");
  assert_eq!(e.ack_payload_bytes().as_ref(), b"pong-data");
}

#[test]
fn local_state_snapshot_bytes_mirrors_slice() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert!(e.local_state_snapshot().is_empty());
  assert!(e.local_state_snapshot_bytes().is_empty());
  e.set_local_state_snapshot(Bytes::from_static(b"snap"))
    .expect("small snapshot fits the stream frame size");
  assert_eq!(e.local_state_snapshot(), b"snap");
  assert_eq!(e.local_state_snapshot_bytes().as_ref(), b"snap");
}

#[test]
fn age_member_rolls_state_change_back_and_is_noop_for_unknown() {
  let now = Instant::now();
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at_seeded(cfg(), now);
  process_alive_auto(&mut e, alive("peer", 7100, 1), false, now);
  let before = e
    .node_state_change(&SmolStr::new("peer"))
    .expect("peer present");
  e.age_member(&SmolStr::new("peer"), Duration::from_secs(5));
  let after = e
    .node_state_change(&SmolStr::new("peer"))
    .expect("peer present");
  assert_eq!(
    before - Duration::from_secs(5),
    after,
    "state_change must roll back by exactly the delta"
  );
  // Aging an unknown member is a silent no-op (no panic, no state created).
  e.age_member(&SmolStr::new("ghost"), Duration::from_secs(1));
  assert!(e.node_incarnation(&SmolStr::new("ghost")).is_none());
}

// ─── lifecycle / accessor edges ──────────────────────────────────────────────

#[test]
fn lifecycle_accessor_reports_running_then_left() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  assert_eq!(e.lifecycle(), Lifecycle::Running);
  // No live peers ⇒ leave completes immediately to Left.
  e.leave(Instant::now()).expect("leave ok");
  assert_eq!(e.lifecycle(), Lifecycle::Left);
}

#[test]
fn members_iterator_yields_every_known_member() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let ids: std::collections::BTreeSet<SmolStr> =
    e.members().map(|m| m.id_ref().cheap_clone()).collect();
  assert!(ids.contains(&SmolStr::new("local")));
  assert!(ids.contains(&SmolStr::new("bob")));
  assert_eq!(ids.len(), 2);
}

#[test]
fn try_new_seedless_on_std_succeeds() {
  // A config WITHOUT an explicit rng seed exercises the platform-entropy
  // construction branch (`getrandom::fill`) — which always succeeds on std —
  // and the std `try_new` convenience that stamps the local node at the wall
  // clock.
  let seedless = EndpointOptions::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  );
  let e =
    Endpoint::<SmolStr, SocketAddr>::try_new_seeded(seedless).expect("seedless std construction");
  assert_eq!(e.num_members(), 1);
}

#[test]
fn drain_broadcasts_returns_queued_messages() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  // `new()` enqueues the local Alive; queue another via an inbound alive.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let drained = e.drain_broadcasts();
  assert!(
    !drained.is_empty(),
    "queued Alive broadcasts must be drainable"
  );
}

#[test]
fn handle_packet_dispatches_each_message_variant() {
  // `handle_packet` is the datagram demux; route one of each membership /
  // probe message through it so every match arm is exercised. The exact
  // side effects are covered by the per-handler tests; here we only assert
  // the dispatch does not panic and membership reacts where expected.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.handle_packet(from, Message::Alive(alive("bob", 7001, 1)), now);
  assert!(e.member(&SmolStr::new("bob")).is_some(), "Alive dispatched");
  e.handle_packet(from, Message::Suspect(suspect("bob", "carol", 1)), now);
  e.handle_packet(from, Message::Dead(dead("bob", "carol", 2)), now);
  // Probe-family messages: untracked seq / unknown target are safe no-ops.
  e.handle_packet(from, Message::Ack(Ack::new(999)), now);
  e.handle_packet(from, Message::Nack(Nack::new(999)), now);
  e.handle_packet(
    from,
    Message::IndirectPing(crate::typed::IndirectPing::new(
      1,
      Node::new(
        SmolStr::new("alice"),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002),
      ),
      Node::new(
        SmolStr::new("bob"),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
      ),
    )),
    now,
  );
}

// ─── alive: self lower incarnation + revival ─────────────────────────────────

#[test]
fn self_alive_with_strictly_lower_incarnation_is_ignored() {
  // An inbound Alive for the local id whose incarnation is strictly below
  // our own is dropped (the self-specific strict-less-than guard), with no
  // refute and no broadcast.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let before = e.local_incarnation();
  assert!(before >= 1);
  // local started at incarnation 1; send a self-Alive at incarnation 0.
  process_alive_auto(&mut e, alive("local", 7000, 0), false, Instant::now());
  assert_eq!(
    e.local_incarnation(),
    before,
    "a lower-incarnation self-Alive must not bump our incarnation"
  );
}

#[test]
fn dead_then_higher_alive_revives_with_node_joined_event() {
  // A peer marked Dead, then a higher-incarnation Alive, transitions back to
  // Alive and emits NodeJoined (the `old_state == Dead` revival arm), and the
  // member's liveness reflects Alive again.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, now);
  e.process_dead(dead("bob", "carol", 2), now);
  assert_eq!(e.member_liveness(&SmolStr::new("bob")), Some(State::Dead));
  while e.poll_event().is_some() {}
  // Higher incarnation Alive revives bob.
  process_alive_auto(&mut e, alive("bob", 7001, 3), false, now);
  assert_eq!(e.member_liveness(&SmolStr::new("bob")), Some(State::Alive));
  let joined = core::iter::from_fn(|| e.poll_event())
    .any(|ev| matches!(ev, Event::NodeJoined(n) if n.id_ref() == &SmolStr::new("bob")));
  assert!(joined, "reviving a Dead node must emit NodeJoined");
}

#[test]
fn alive_with_changed_meta_emits_node_updated() {
  // A higher-incarnation Alive for a still-Alive peer that changes its meta
  // emits NodeUpdated (the `old_meta != new_meta` arm).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, now);
  while e.poll_event().is_some() {}
  let updated_alive = alive("bob", 7001, 2).with_meta(Meta::try_from(&b"meta-v2"[..]).unwrap());
  process_alive_auto(&mut e, updated_alive, false, now);
  let updated = core::iter::from_fn(|| e.poll_event())
    .any(|ev| matches!(ev, Event::NodeUpdated(n) if n.id_ref() == &SmolStr::new("bob")));
  assert!(updated, "a meta change must emit NodeUpdated");
}

// ─── refute while leaving + skip-past ─────────────────────────────────────────

#[test]
fn refute_is_suppressed_once_leaving() {
  // After leave() the lifecycle is no longer Running; a higher-incarnation
  // self-Alive would normally refute (bumping our incarnation and broadcasting
  // a higher-incarnation Alive), but refute must short-circuit once leaving so
  // it cannot resurrect the leaving node.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  // A live peer so leave() goes Leaving (not immediately Left).
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, now);
  e.leave(now).expect("leave ok");
  assert!(e.is_leaving() || e.is_left());
  let inc_before = e.local_incarnation();
  // A higher-incarnation self-Alive reaches the refute path, which is then
  // suppressed because the lifecycle is no longer Running.
  process_alive_auto(&mut e, alive("local", 7000, inc_before + 5), false, now);
  assert_eq!(
    e.local_incarnation(),
    inc_before,
    "refute must be suppressed once leaving/left"
  );
}

#[test]
fn self_suspect_with_high_incarnation_refutes_past_accusation() {
  // A self-Suspect whose accused incarnation is at/above ours forces refute to
  // skip its incarnation PAST the accusation (the `skip_incarnation_past`
  // branch), so the refuting Alive out-ranks the accusation.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  let accused = e.local_incarnation() + 10;
  e.process_suspect(suspect("local", "bob", accused), now);
  assert!(
    e.local_incarnation() > accused,
    "refute must skip our incarnation past the accusation, got {} <= {accused}",
    e.local_incarnation()
  );
}

// ─── suspect / dead unknown + re-confirmation ────────────────────────────────

#[test]
fn suspect_for_unknown_node_is_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let n = e.num_members();
  e.process_suspect(suspect("ghost", "bob", 1), Instant::now());
  assert_eq!(e.num_members(), n, "an unknown-node Suspect is a no-op");
}

#[test]
fn dead_for_unknown_node_is_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let n = e.num_members();
  e.process_dead(dead("ghost", "bob", 1), Instant::now());
  assert_eq!(e.num_members(), n, "an unknown-node Dead is a no-op");
}

#[test]
fn dead_with_older_incarnation_is_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 5), false, now);
  // Dead at incarnation 3 < bob's 5 ⇒ ignored; bob stays Alive.
  e.process_dead(dead("bob", "carol", 3), now);
  assert_eq!(e.member_liveness(&SmolStr::new("bob")), Some(State::Alive));
}

#[test]
fn second_suspect_from_distinct_source_rebroadcasts_on_confirmation() {
  // A peer already Suspect, then a Suspect from a DIFFERENT source confirms the
  // suspicion (incrementing its confirmation count), which re-broadcasts the
  // Suspect (the `Confirmation::Accepted` arm).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg().with_suspicion_mult(4), // k > 0 so confirmations are tracked
  );
  let now = Instant::now();
  // Add several peers so the cluster is large enough for k > 0.
  for (i, name) in ["bob", "carol", "dave", "erin"].iter().enumerate() {
    process_alive_auto(&mut e, alive(name, 7001 + i as u16, 1), false, now);
  }
  // First Suspect starts the timer.
  e.process_suspect(suspect("bob", "carol", 1), now);
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect)
  );
  e.drain_broadcasts();
  // Second Suspect from a distinct source confirms ⇒ rebroadcast.
  e.process_suspect(suspect("bob", "dave", 1), now);
  let rebroadcast = e
    .drain_broadcasts()
    .into_iter()
    .any(|m| matches!(m, Message::Suspect(s) if s.node_ref() == &SmolStr::new("bob")));
  assert!(
    rebroadcast,
    "a confirming Suspect from a new source must rebroadcast"
  );
}

// ─── merge: unknown remote state ──────────────────────────────────────────────

#[test]
fn merge_skips_unknown_remote_state() {
  // A PushNodeState carrying an Unknown wire state is silently skipped by
  // merge_state (the `State::Unknown(_)` arm), leaving membership unchanged.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let n = e.num_members();
  let remote = vec![pns("weird", 7009, 1, State::Unknown(99))];
  e.merge_state(&remote, Instant::now());
  assert_eq!(e.num_members(), n, "an Unknown-state entry must be skipped");
}

// ─── handle_ack / handle_nack edges ──────────────────────────────────────────

#[test]
fn handle_nack_for_untracked_seq_is_noop() {
  // A Nack for a sequence with no pending probe is dropped silently.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  // Must not panic / must be a clean no-op.
  e.handle_nack(from, Nack::new(424242), Instant::now());
}

// ─── probe fan-out: reliable disabled + zero indirect → terminate ────────────

#[test]
fn probe_fan_out_with_reliable_disabled_and_no_indirect_suspects_immediately() {
  // A two-node cluster (no indirect peers) with reliable ping DISABLED for the
  // target: when the direct ping times out, fan-out has nothing to try and
  // suspects the target immediately (the `expected_nacks == 0 &&
  // reliable_stream_id.is_none()` terminate arm).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80)),
  );
  let now = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, now);
  e.disable_reliable_ping(SmolStr::new("bob"));
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}
  // Past the direct deadline but before the cumulative failure deadline.
  e.handle_timeout(t0 + Duration::from_millis(60));
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect),
    "no indirect + no reliable fallback must suspect the target on direct timeout"
  );
}

// ─── reset_nodes / user packets / meta cap ───────────────────────────────────

#[test]
fn reset_nodes_keeps_live_members_and_reaps_only_expired_dead() {
  // reset_nodes scans all members; a live (Alive) remote member is retained
  // (the `state != Dead && state != Left` filter arm), while a long-expired
  // Dead member is reaped.
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_gossip_to_the_dead_time(Duration::from_millis(10)));
  let now = Instant::now();
  process_alive_auto(&mut e, alive("live", 7001, 1), false, now);
  process_alive_auto(&mut e, alive("gone", 7002, 1), false, now);
  e.process_dead(dead("gone", "x", 2), now);
  // Far in the future so "gone" is past the dead window; "live" is retained.
  e.reset_nodes(now + Duration::from_secs(60));
  assert!(
    e.member(&SmolStr::new("live")).is_some(),
    "live member retained"
  );
  assert!(
    e.member(&SmolStr::new("gone")).is_none(),
    "expired dead reaped"
  );
}

#[test]
fn send_user_packets_empty_slice_is_ok_noop() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let to = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.send_user_packets(to, &[]).expect("empty slice is Ok");
  assert!(
    e.poll_transmit().is_none(),
    "an empty payload slice queues nothing"
  );
}

#[test]
fn update_meta_rejects_oversized_meta() {
  // A meta larger than the configured cap is rejected with MetaExceedsCap and
  // not applied.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_meta_max_size(4));
  let big = Meta::try_from(&b"way-too-long"[..]).unwrap();
  let err = e
    .update_meta(big)
    .expect_err("oversized meta must be rejected");
  assert!(matches!(err, Error::MetaExceedsCap(_)), "got {err:?}");
}

// ─── stream events: RemoteStateReceived + reliable-ping failure ──────────────

#[test]
fn push_pull_reply_with_user_data_emits_remote_state_received() {
  use PushPullKind;
  use bytes::Bytes;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let states = vec![pns("carol", 7003, 1, State::Alive)];
  // A non-empty user_data payload must surface as RemoteStateReceived after
  // the merge is admitted.
  let ev = EndpointEvent::PushPullReplyReceived(PushPullReplyReceived::new_with_stream_id(
    peer,
    states,
    Bytes::from_static(b"peer-app-state"),
    PushPullKind::Refresh,
    StreamId::from_raw(4242),
  ));
  let cmd = e.handle_stream_event(ev, now);
  assert!(cmd.is_none(), "an outbound reply returns no command");
  let got = core::iter::from_fn(|| e.poll_event()).any(|ev| {
    matches!(ev, Event::RemoteStateReceived(r) if r.user_data_ref().as_ref() == b"peer-app-state")
  });
  assert!(
    got,
    "non-empty reply user_data must emit RemoteStateReceived"
  );
}

#[test]
fn push_pull_request_with_user_data_emits_remote_state_received_and_response() {
  use PushPullKind;
  use bytes::Bytes;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let states = vec![pns("carol", 7003, 1, State::Alive)];
  let ev = EndpointEvent::PushPullRequestReceived(PushPullRequestReceived::new_with_stream_id(
    peer,
    states,
    Bytes::from_static(b"inbound-app-state"),
    PushPullKind::Refresh,
    StreamId::from_raw(4343),
  ));
  let cmd = e.handle_stream_event(ev, now);
  assert!(
    matches!(cmd, Some(StreamCommand::SendPushPullResponse(_))),
    "an inbound request returns a SendPushPullResponse command"
  );
  let got = core::iter::from_fn(|| e.poll_event()).any(|ev| {
    matches!(ev, Event::RemoteStateReceived(r) if r.user_data_ref().as_ref() == b"inbound-app-state")
  });
  assert!(
    got,
    "non-empty request user_data must emit RemoteStateReceived"
  );
}

/// `RemoteStateReceived::originating_stream_id()` for an outbound join push/pull
/// equals the `StreamId` fed into the producing `PushPullReplyReceived` — the
/// same id that `start_push_pull` returns for that exchange. A driver uses it to
/// correlate the merge to the exact exchange it started, independently of the
/// `ExchangeCompleted` terminal token (which on the `StreamEndpoint` backend is a
/// separately-allocated coordinator id and does NOT match here by design).
#[test]
fn remote_state_received_carries_originating_stream_id() {
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  // The id `start_push_pull` would have returned for this outbound join.
  let stream_id = StreamId::from_raw(9182);

  let ev = EndpointEvent::PushPullReplyReceived(PushPullReplyReceived::new_with_stream_id(
    peer,
    vec![pns("carol", 7003, 1, State::Alive)],
    Bytes::from_static(b"peer-app-state"),
    PushPullKind::Join,
    stream_id,
  ));
  e.handle_stream_event(ev, now);

  let rs = core::iter::from_fn(|| e.poll_event())
    .find_map(|ev| match ev {
      Event::RemoteStateReceived(r) => Some(r),
      _ => None,
    })
    .expect("a non-empty join reply must emit RemoteStateReceived");

  assert_eq!(
    rs.originating_stream_id(),
    stream_id,
    "the merge carries the producing stream's StreamId directly"
  );
}

#[test]
fn reliable_ping_failed_event_retires_fallback_without_failing_probe() {
  // Drive a probe to AwaitingIndirect with the reliable fallback armed, then
  // route a ReliablePingFailed back through handle_stream_event: the fallback
  // is retired but the probe keeps running (failure is decided only by the
  // cumulative deadline).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(200)),
  );
  let now = Instant::now();
  // Three peers so an indirect peer exists ⇒ AwaitingIndirect (not immediate
  // terminate), and the reliable fallback is armed concurrently.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, now);
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, now);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  e.start_probe(t0);
  let seq = match e.poll_transmit().expect("direct Ping") {
    Transmit::Packet(p) => match p.into_parts().1 {
      Message::Ping(ping) => ping.sequence_number(),
      _ => panic!("Ping expected"),
    },
    _ => panic!("Ping expected"),
  };
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}
  // Direct timeout (before the 200ms cumulative deadline) ⇒ fan out to indirect
  // + arm the reliable fallback.
  e.handle_timeout(t0 + Duration::from_millis(60));
  assert!(e.probes.contains_key(&seq), "probe is in AwaitingIndirect");
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The reliable fallback fails: routed through the stream-event path.
  let cmd = e.handle_stream_event(
    EndpointEvent::ReliablePingFailed(crate::event::ReliablePingFailed::new(seq)),
    t0 + Duration::from_millis(70),
  );
  assert!(
    cmd.is_none(),
    "ReliablePingFailed returns no stream command"
  );
  assert!(
    e.probes.contains_key(&seq),
    "a fallback failure must NOT fail the probe — the indirect path keeps racing"
  );
}

#[test]
fn dial_failed_for_unknown_stream_is_noop() {
  // dial_failed for a StreamId with no pending intent is a clean no-op.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  e.dial_failed(
    StreamId::from_raw(999),
    StreamError::PeerClosed,
    Instant::now(),
  );
}

#[test]
fn ping_untracked_node_synthesizes_target_state() {
  // Pinging a node not in membership synthesizes a minimal Alive NodeState so a
  // later PingCompleted can still carry the target (the `None` arm of the
  // member lookup). A direct Ack then completes it.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let now = Instant::now();
  let target_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7099);
  let node = Node::new(SmolStr::new("stranger"), target_addr);
  let ping_id = e.ping(node, now).expect("issued while running");
  // The probe was registered; drain the outbound direct Ping and its seq.
  let seq = core::iter::from_fn(|| e.poll_transmit())
    .find_map(|tx| match tx {
      Transmit::Packet(p) => match p.into_parts().1 {
        Message::Ping(ping) => Some(ping.sequence_number()),
        _ => None,
      },
      _ => None,
    })
    .expect("a direct Ping must be emitted");
  // The stranger Acks: PingCompleted carries the synthesized target.
  e.handle_ack(target_addr, Ack::new(seq), now);
  let completed = core::iter::from_fn(|| e.poll_event())
    .any(|ev| matches!(ev, Event::PingCompleted(c) if c.ping_id() == ping_id));
  assert!(
    completed,
    "an Ack from the untracked target must complete the ping"
  );
}

#[test]
fn gossip_scheduler_skips_left_members_as_candidates() {
  // A Left member is not a gossip candidate (the `_ => false` filter arm). With
  // only a Left peer present, gossip selects no target and emits nothing, yet
  // does not panic and reschedules.
  let cfg = EndpointOptions::<SmolStr, SocketAddr>::new(
    SmolStr::new("local"),
    "127.0.0.1:7946".parse().unwrap(),
  )
  .with_probe_interval(Duration::ZERO)
  .with_gossip_interval(Duration::from_millis(50))
  .with_gossip_nodes(2)
  .with_push_pull_interval(Duration::ZERO);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("gone", 7947, 1), false, t0);
  // Mark "gone" as Left (self-marked dead sentinel: node == from).
  e.process_dead(dead("gone", "gone", 2), t0);
  assert_eq!(e.member_liveness(&SmolStr::new("gone")), Some(State::Left));
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  e.queue_user_broadcast(bytes::Bytes::from_static(b"x"))
    .unwrap();
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);
  // The only peer is Left ⇒ no gossip target ⇒ nothing transmitted.
  assert!(
    e.poll_transmit().is_none(),
    "a Left-only membership must yield no gossip target"
  );
}

#[test]
fn try_new_at_rejects_meta_larger_than_meta_max_size() {
  let meta = Meta::try_from(Bytes::from(vec![0u8; 64])).expect("within wire ceiling");
  let c = cfg().with_initial_meta(meta).with_meta_max_size(10);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::MetaTooLarge(_))),
    "initial_meta over meta_max_size must be rejected"
  );
}

#[test]
fn try_new_at_rejects_zero_awareness_multiplier() {
  let c = cfg().with_awareness_max_multiplier(0);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::AwarenessMultiplierZero)),
    "a zero awareness multiplier must be rejected, not panic"
  );
}

#[test]
fn try_new_at_rejects_gossip_mtu_below_floor() {
  let c = cfg().with_gossip_mtu(1);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::GossipMtuTooSmall(_))),
    "a gossip_mtu below the mandatory-control-packet floor must be rejected"
  );
}

#[test]
fn try_new_at_rejects_gossip_mtu_above_udp_ceiling() {
  let c = cfg().with_gossip_mtu(crate::config::MAX_GOSSIP_MTU + 1);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::GossipMtuTooLarge(_))),
    "a gossip_mtu above the UDP datagram ceiling must be rejected"
  );
}

#[test]
fn try_new_at_rejects_zero_max_stream_frame_size() {
  let c = cfg().with_max_stream_frame_size(0);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::InvalidMaxStreamFrameSize(0))),
    "a zero max_stream_frame_size must be rejected"
  );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn try_new_at_rejects_max_stream_frame_size_above_u32() {
  // `u32::MAX as usize + 1` only exists as a distinct value on 64-bit; on a
  // 32-bit target `usize::MAX == u32::MAX`, so the addition overflows at
  // compile time and the above-u32 case is unreachable there.
  let c = cfg().with_max_stream_frame_size(u32::MAX as usize + 1);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::InvalidMaxStreamFrameSize(_))),
    "a max_stream_frame_size above the u32 wire limit must be rejected"
  );
}

#[test]
fn try_new_at_accepts_default_gossip_and_frame() {
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(cfg(), t0);
  assert!(
    res.is_ok(),
    "the default gossip_mtu and frame ceiling must construct cleanly"
  );
}

#[test]
fn try_new_at_rejects_gossip_mtu_below_identity_floor() {
  // A long local_id inflates the mandatory control packets past the configured
  // gossip_mtu even though that value clears the constant 512 floor.
  let long_id = SmolStr::new("n".repeat(1500));
  let c = EndpointOptions::new(
    long_id,
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  )
  .with_gossip_mtu(1400);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::GossipMtuTooSmall(_))),
    "a gossip_mtu too small for a long local_id's control packets must be rejected"
  );
}

#[test]
fn try_new_at_rejects_oversized_initial_local_state() {
  // A frame cap just above the 1 MiB reserve leaves a tiny budget; a snapshot
  // whose framed PushPull exceeds it must be rejected at construction, matching
  // the runtime set_local_state_snapshot setter.
  let snapshot = Bytes::from(vec![0u8; 8 * 1024]);
  let c = cfg()
    .with_max_stream_frame_size(LOCAL_STATE_FRAME_BUDGET + 1024)
    .with_initial_local_state(snapshot);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::LocalStateExceedsFrame(_))),
    "an initial_local_state too large for the frame budget must be rejected at construction"
  );
}

#[test]
fn try_new_at_rejects_unencodable_local_advertise_addr() {
  // A scoped IPv6 advertise address (nonzero scope_id) is not representable on
  // the compact wire layout, so the node could not broadcast its own membership.
  let scoped = SocketAddr::V6(core::net::SocketAddrV6::new(
    core::net::Ipv6Addr::LOCALHOST,
    7000,
    0,
    7,
  ));
  let c = EndpointOptions::new(SmolStr::new("local"), scoped);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::UnencodableLocalIdentity)),
    "a scoped IPv6 advertise address must be rejected at construction"
  );
}

#[test]
fn try_new_at_rejects_tiny_max_stream_frame_size() {
  // A frame cap of 1 passes the 0 / u32 range check but cannot carry the local
  // node's minimal push/pull frame, so membership exchange could never complete.
  let c = cfg().with_max_stream_frame_size(1);
  let t0 = Instant::from_origin(Duration::from_secs(1));
  let res = Endpoint::<SmolStr, SocketAddr>::try_new_at_seeded(c, t0);
  assert!(
    matches!(res, Err(EndpointInitError::MaxStreamFrameSizeTooSmall(_))),
    "a max_stream_frame_size too small for the minimal push/pull frame must be rejected"
  );
}

#[test]
fn max_members_rejects_new_id_at_cap() {
  // Cap total membership at 2 (local + one peer).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_max_members(Some(2)));
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer-1", 7001, 1), false, t0);
  assert_eq!(e.num_members(), 2, "first peer admitted (local + peer-1)");
  process_alive_auto(&mut e, alive("peer-2", 7002, 1), false, t0);
  assert_eq!(e.num_members(), 2, "second peer rejected at the cap");
  // A state update for an already-known id is never rejected by the cap.
  process_alive_auto(&mut e, alive("peer-1", 7001, 2), false, t0);
  assert_eq!(e.num_members(), 2, "known-id update is not gated");
}

#[test]
fn max_members_none_admits_unlimited() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer-1", 7001, 1), false, t0);
  process_alive_auto(&mut e, alive("peer-2", 7002, 1), false, t0);
  assert_eq!(e.num_members(), 3, "default None admits both peers");
}

#[test]
fn metrics_count_membership_load_shed() {
  // Cap at 2 (local + one peer); the over-cap admission is shed and counted.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_max_members(Some(2)));
  let t0 = Instant::now();
  assert_eq!(
    e.metrics().members_rejected,
    0,
    "no load shed at construction"
  );
  process_alive_auto(&mut e, alive("peer-1", 7001, 1), false, t0);
  process_alive_auto(&mut e, alive("peer-2", 7002, 1), false, t0);
  assert_eq!(
    e.metrics().members_rejected,
    1,
    "the over-cap admission is counted"
  );
  // A known-id update is never gated, so the counter does not advance.
  process_alive_auto(&mut e, alive("peer-1", 7001, 2), false, t0);
  assert_eq!(
    e.metrics().members_rejected,
    1,
    "a known-id update is not shed"
  );
}

#[test]
fn unknown_alive_with_zero_incarnation_admits_visibly_and_bumps() {
  // A first Alive carrying incarnation 0 (a peer that starts at incarnation 0, or
  // a forged id) for an unknown member must admit a VISIBLE Alive — not insert a
  // Dead placeholder that is invisible to snapshot readers yet still consumes a
  // max_members slot, which would let zero-incarnation Alives silently fill the
  // cap and reject later legitimate members.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_max_members(Some(3)));
  let t0 = Instant::now();
  let v0 = e.snapshot_version();

  process_alive_auto(&mut e, alive("peer-0", 7000, 0), false, t0);

  assert_eq!(
    e.member_liveness(&SmolStr::new("peer-0")),
    Some(State::Alive),
    "Alive(incarnation=0) for an unknown id must admit a visible Alive member"
  );
  assert_eq!(
    e.num_members(),
    2,
    "the zero-incarnation peer counts as a member (local + peer-0)"
  );
  assert!(
    e.snapshot_version() != v0,
    "admitting the new member must bump the snapshot version"
  );
}

#[test]
fn snapshot_version_bumps_on_membership_and_health_changes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  let v0 = e.snapshot_version();

  // New member (NodeJoined) bumps.
  process_alive_auto(&mut e, alive("peer-1", 7001, 1), false, t0);
  let v1 = e.snapshot_version();
  assert!(v1 != v0, "a new member must bump the snapshot version");

  // A stale (older-incarnation) alive for the same node is ignored — no bump.
  process_alive_auto(&mut e, alive("peer-1", 7001, 1), false, t0);
  assert_eq!(
    e.snapshot_version(),
    v1,
    "an ignored (no-op) alive must not bump the version"
  );

  // A health change bumps.
  e.degrade_health(2);
  assert!(
    e.snapshot_version() != v1,
    "a health change must bump the snapshot version"
  );
}

#[test]
fn snapshot_version_bumps_on_incarnation_only_alive_update() {
  // A newer-incarnation Alive for an already-Alive member with the SAME meta
  // and address emits NO event (not a resurrection, not a meta change), but the
  // member's incarnation — and, for a Suspect->Alive refutation, its FSM state —
  // changes, so the published snapshot must still be republished.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer-1", 7001, 1), false, t0);
  let v1 = e.snapshot_version();
  process_alive_auto(&mut e, alive("peer-1", 7001, 5), false, t0);
  assert!(
    e.snapshot_version() != v1,
    "an incarnation-only (no event) alive update must still bump the snapshot version"
  );
}

// ───────── probe / ping / suspicion failure-path edge regressions ─────────

/// A periodic probe `Ping` whose target carries an over-MTU id must never be
/// emitted as an over-MTU datagram. Node ids are unbounded, so a lone Ping
/// (the target is Alive ⇒ no buddy Suspect ⇒ the `len == 1` path) carrying a
/// large already-admitted target id can exceed `gossip_mtu` even though the
/// local self-Ping fit at construction. A single message has no compound split
/// point, so it must be dropped rather than sent as a fragmentable datagram —
/// the same ceiling the compound branch already enforces.
#[test]
fn probe_ping_with_oversize_peer_id_is_not_emitted_over_mtu() {
  let gossip_mtu = 1400usize;
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_gossip_mtu(gossip_mtu));
  let t0 = Instant::now();
  // A peer whose id alone dwarfs the gossip MTU. The local id ("local") is
  // small, so the construction-time self-Ping floor passed; only the remote
  // target's id pushes the probe Ping past one datagram.
  let big_id = "b".repeat(gossip_mtu * 2);
  process_alive_auto(&mut e, alive(&big_id, 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  assert!(
    e.start_probe(t0),
    "a probe against the only (Alive) peer must be initiated"
  );
  // The FSM entry is registered regardless — dropping the datagram must not
  // drop the probe (it then fails on its deadline like any lost UDP Ping).
  assert!(
    !e.probes.is_empty(),
    "the probe FSM entry must be registered even when its Ping is too large to send"
  );

  // No emitted datagram may exceed gossip_mtu.
  while let Some(tx) = e.poll_transmit() {
    match tx {
      Transmit::Packet(p) => {
        let len = crate::wire::encode_message::<SmolStr, SocketAddr>(p.message_ref())
          .expect("a probe message must encode")
          .len();
        assert!(
          len <= gossip_mtu,
          "start_probe emitted a {len}-byte lone Packet exceeding gossip_mtu {gossip_mtu}"
        );
      }
      Transmit::Compound(c) => panic!(
        "an Alive target yields no buddy Suspect, so no compound is expected; got {:?}",
        c.into_parts().1
      ),
    }
  }
}

/// Public `ping(X @ B)` must bind the probe to the caller-supplied address B,
/// not the address A at which membership happens to store X. Otherwise the Ping
/// is sent to B while `ack_source_is_valid` expects a responder at A, so the
/// genuine Ack from B is rejected and the terminal event misreports X @ A.
#[test]
fn public_ping_binds_probe_to_caller_supplied_address() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  // Membership stores bob at address A (127.0.0.1:7001)...
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // ...but the caller pings bob at a DIFFERENT address B (127.0.0.1:7009).
  let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7009);
  let pid = e
    .ping(Node::new(SmolStr::new("bob"), addr_b), t0)
    .expect("ping is accepted while Running");

  // The Ping is transmitted to B.
  let (seq, to) = match e.poll_transmit().expect("a Ping is emitted") {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      match message {
        Message::Ping(ping) => (ping.sequence_number(), to),
        other => panic!("expected a Ping, got {other:?}"),
      }
    }
    other => panic!("expected a lone Packet Ping, got {other:?}"),
  };
  assert_eq!(
    to, addr_b,
    "the Ping must be sent to the caller-supplied address B"
  );
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // An Ack from B (where the Ping actually went) must COMPLETE the ping: the
  // probe target is bound to B, so that responder is accepted.
  e.handle_ack(addr_b, Ack::new(seq), t0 + Duration::from_millis(10));

  let completed = core::iter::from_fn(|| e.poll_event()).find_map(|ev| match ev {
    Event::PingCompleted(pc) => Some(pc),
    _ => None,
  });
  let pc = completed.expect(
    "an Ack from the pinged address B must complete the ping (the probe target \
     must bind to B, not bob's stored address A)",
  );
  assert_eq!(
    pc.ping_id(),
    pid,
    "the completion carries the returned PingId"
  );
  assert_eq!(
    pc.node_ref().address_ref(),
    &addr_b,
    "PingCompleted must report the pinged address B, not the stored address A"
  );
}

/// An expired Detection probe must suspect the generation it PROBED, not
/// whatever incarnation the target holds when the probe finally fails. If the
/// target refuted (advanced its incarnation) while the probe was in flight, the
/// stale failure must NOT suspect it — matching Go memberlist, which suspects
/// the snapshot `node.Incarnation` and drops it in `suspectNode` once the live
/// incarnation has moved on.
#[test]
fn expired_probe_does_not_suspect_target_that_refuted_to_newer_incarnation() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  // bob joins at incarnation 1; the probe snapshots that generation.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  assert!(e.start_probe(t0), "probe bob at incarnation 1");
  while e.poll_transmit().is_some() {}

  // bob refutes mid-probe, advancing to incarnation 5 at the same address.
  process_alive_auto(&mut e, alive("bob", 7001, 5), false, t0);
  assert_eq!(
    e.node_incarnation(&SmolStr::new("bob")),
    Some(5),
    "bob advanced to incarnation 5 while the probe was in flight"
  );
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The probe expires long after bob advanced. It suspects generation 1, which
  // process_suspect drops (1 < 5), so bob stays Alive.
  e.handle_timeout(t0 + Duration::from_secs(30));
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Alive),
    "a probe failure against incarnation 1 must not suspect bob after it \
     refuted to incarnation 5"
  );
}

/// A sub-millisecond `probe_interval` must not arm a suspicion whose deadline
/// equals `now`. `probe_interval.as_millis()` truncates to 0, so the old code
/// produced `min == max == 0` and `Suspicion::new` set `deadline == now` — an
/// immediate false death the instant a node is suspected.
#[test]
fn submillisecond_probe_interval_does_not_arm_immediate_death_suspect() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_interval(Duration::from_micros(500)));
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  while e.poll_event().is_some() {}

  // A gossiped Suspect for bob at its current incarnation transitions bob to
  // Suspect and arms a suspicion timer.
  e.process_suspect(
    Suspect::new(1, SmolStr::new("bob"), SmolStr::new("carol")),
    t0,
  );
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect),
    "bob transitions to Suspect"
  );

  // The armed deadline must be strictly AFTER now — not an already-expired timer.
  let deadline = e
    .members
    .get(&SmolStr::new("bob"))
    .and_then(|m| m.suspicion().map(|s| s.deadline()))
    .expect("bob has an armed suspicion");
  assert!(
    deadline > t0,
    "a sub-ms probe_interval must not arm a suspicion already at its deadline"
  );

  // Firing the timer at t0 must NOT kill bob (the deadline has not elapsed).
  e.handle_timeout(t0);
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect),
    "bob must remain Suspect, not die immediately, under a sub-ms probe_interval"
  );
}

/// A sub-millisecond `probe_interval` is floored to a 1ms minimum BEFORE it is
/// scaled by `suspicion_mult` and the node-count scale — the mult and scale are
/// preserved, not discarded. For a 500µs interval, default mult 4, and two
/// nodes (node_scale = max(1.0, log10(2)) = 1.0), the min timeout is
/// 1ms * 4 * 1.0 = 4ms and the max is 4ms * 6 = 24ms. Flooring the final product
/// instead would collapse this to 1ms, dropping both the mult and the scale.
#[test]
fn submillisecond_probe_interval_scales_suspicion_timeout_by_mult() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_probe_interval(Duration::from_micros(500)));
  // Two members: local + bob → node_scale = max(1.0, log10(2)) = 1.0 exactly
  // (the `.max(1.0)` floor makes it independent of log10 precision).
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  assert_eq!(e.num_members(), 2, "local + bob");

  let (min, max) = e.suspicion_timeouts();
  assert_eq!(
    min,
    Duration::from_millis(4),
    "500µs floored to 1ms, then * mult(4) * node_scale(1.0) = 4ms — the mult must survive the floor"
  );
  assert_eq!(
    max,
    Duration::from_millis(24),
    "max = min(4ms) * suspicion_max_timeout_mult(6) = 24ms"
  );
}

/// `suspicion_mult == 0` (a publicly accepted value) must yield a ZERO suspicion
/// timeout for a >= 1ms interval — unchanged from before the sub-ms fix and
/// matching Go `suspicionTimeout` (mult 0 ⇒ 0). Flooring the sub-ms interval
/// must NOT force this >= 1ms / zero-mult path to a 1ms minimum: with the floor
/// applied to the interval (not the product) the zero mult keeps the product 0.
#[test]
fn zero_suspicion_mult_yields_zero_suspicion_timeout() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_interval(Duration::from_secs(1))
      .with_suspicion_mult(0),
  );
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());

  let (min, max) = e.suspicion_timeouts();
  assert_eq!(
    min,
    Duration::ZERO,
    "suspicion_mult == 0 with a >= 1ms interval yields a zero min timeout (unchanged, matches Go)"
  );
  assert_eq!(max, Duration::ZERO, "a zero min scales to a zero max");
}

/// When the direct probe escalates to the indirect fan-out, an `IndirectPing`
/// carrying an over-MTU target id must be dropped (never emitted as an
/// unsendable datagram), and `expected_nacks` / `indirect_peers` must count
/// ONLY the peers whose IndirectPing was actually queued. Otherwise the FSM
/// waits on a Nack from a helper that never received the ping, and an over-MTU
/// datagram is emitted. bob's id alone dwarfs the MTU, so every IndirectPing is
/// over-MTU; carol is an eligible helper. With reliable ping enabled (default)
/// the probe stays in AwaitingIndirect racing the (unbounded) TCP fallback,
/// with `expected_nacks == 0`.
#[test]
fn indirect_fan_out_drops_over_mtu_indirect_ping_and_counts_only_queued() {
  let gossip_mtu = 1400usize;
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_gossip_mtu(gossip_mtu));
  let t0 = Instant::now();
  // bob's id alone dwarfs the gossip MTU, so every Ping/IndirectPing carrying
  // bob as its target exceeds one datagram. carol is a small, Alive indirect
  // helper (not local, not the target).
  let big_id = "b".repeat(gossip_mtu * 2);
  process_alive_auto(&mut e, alive(&big_id, 7001, 1), false, t0);
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Insert a Detection probe against bob directly in AwaitingDirectAck (the
  // direct Ping MTU drop is covered elsewhere). The failure deadline sits far
  // ahead, so the elapsed direct sub-window escalates to the indirect fan-out
  // rather than terminating the probe outright.
  let seq = 91u32;
  let (target, target_incarnation, target_generation) = {
    let m = e
      .members
      .get(&SmolStr::new(big_id.as_str()))
      .expect("bob admitted");
    (
      m.state_ref().server_arc(),
      m.state_ref().incarnation(),
      m.generation(),
    )
  };
  e.probes.insert(
    seq,
    Probe::new_direct(
      target,
      target_incarnation,
      target_generation,
      t0,
      ProbeKind::Detection,
      Duration::from_millis(500),
      t0 + Duration::from_secs(100),
    ),
  );

  // Advance past the direct sub-window (cfg probe_timeout = 500ms) but well
  // before the failure deadline → the indirect fan-out fires.
  e.handle_timeout(t0 + Duration::from_secs(1));

  // Every emitted datagram must be within gossip_mtu, and NO IndirectPing may
  // be queued: bob's oversized id makes each one over-MTU.
  let mut queued_indirect = 0usize;
  while let Some(tx) = e.poll_transmit() {
    match tx {
      Transmit::Packet(p) => {
        let len = crate::wire::encode_message::<SmolStr, SocketAddr>(p.message_ref())
          .expect("a probe message must encode")
          .len();
        assert!(
          len <= gossip_mtu,
          "the indirect fan-out emitted a {len}-byte Packet exceeding gossip_mtu {gossip_mtu}"
        );
        if matches!(p.message_ref(), Message::IndirectPing(_)) {
          queued_indirect += 1;
        }
      }
      Transmit::Compound(c) => panic!(
        "the fan-out emits lone IndirectPing Packets, never a compound; got {:?}",
        c.into_parts().1
      ),
    }
  }
  assert_eq!(
    queued_indirect, 0,
    "bob's oversized id makes every IndirectPing over-MTU, so none may be queued"
  );

  // The outstanding-nack accounting must equal the number actually queued (0):
  // the probe races only the reliable-ping fallback and must NOT wait on a Nack
  // from carol, which never received an IndirectPing.
  match &e
    .probes
    .get(&seq)
    .expect("probe remains, racing the reliable-ping fallback")
    .phase
  {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      indirect_peers,
      ..
    }) => {
      assert_eq!(
        *expected_nacks, queued_indirect,
        "expected_nacks must equal the count of IndirectPings actually queued"
      );
      assert_eq!(*expected_nacks, 0, "no indirect ping was queued");
      assert!(
        indirect_peers.is_empty(),
        "no peer received an IndirectPing, so the Nack allowlist must be empty"
      );
    }
    other => panic!("probe must be AwaitingIndirect (racing the reliable fallback), got {other:?}"),
  }
}

/// A caller-supplied `ping` target whose framed `Ping` exceeds `gossip_mtu`
/// (an unbounded id dwarfing the single-datagram budget) is rejected up front
/// with `PingExceedsMtu`: it registers NO probe/ack state and queues NO Packet,
/// so a probe that could never be delivered on the unreliable plane is never
/// begun. A within-budget target still queues exactly one Ping and returns Ok.
#[test]
fn ping_over_mtu_target_errs_without_registering_probe_or_ack() {
  let gossip_mtu = 1400usize;
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_gossip_mtu(gossip_mtu));
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  let t0 = Instant::now();

  // An id alone dwarfing the datagram budget makes the framed Ping over-MTU.
  let big_id = "b".repeat(gossip_mtu * 2);
  let over = Node::new(
    SmolStr::new(big_id.as_str()),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
  );
  let probes_before = e.probes.len();
  let acks_before = e.ack_registry.len();
  match e.ping(over, t0) {
    Err(Error::PingExceedsMtu(info)) => {
      assert!(
        info.size() > gossip_mtu,
        "the reported framed size must exceed the budget"
      );
      assert_eq!(
        info.limit(),
        gossip_mtu,
        "the reported budget is gossip_mtu"
      );
    }
    other => panic!("expected PingExceedsMtu, got {other:?}"),
  }
  assert!(
    e.poll_transmit().is_none(),
    "an over-MTU ping queues no Packet"
  );
  assert_eq!(
    e.probes.len(),
    probes_before,
    "an over-MTU ping registers no probe (no dangling probe slot)"
  );
  assert_eq!(
    e.ack_registry.len(),
    acks_before,
    "an over-MTU ping registers no ack entry (no dangling ack)"
  );

  // A within-budget target still pings: exactly one Ping Packet, Ok.
  let within = Node::new(
    SmolStr::new("bob"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002),
  );
  e.ping(within, t0).expect("a within-MTU ping issues");
  let mut pings = 0usize;
  while let Some(tx) = e.poll_transmit() {
    match tx {
      Transmit::Packet(p) => {
        assert!(
          matches!(p.message_ref(), Message::Ping(_)),
          "a direct ping queues only a Ping"
        );
        pings += 1;
      }
      Transmit::Compound(c) => {
        panic!("a direct ping emits a lone Packet, not a compound: {c:?}")
      }
    }
  }
  assert_eq!(pings, 1, "a within-MTU ping queues exactly one Ping");
}

/// A caller-supplied `ping` target whose address cannot be encoded on the
/// compact wire layout — here a scoped IPv6 `SocketAddr` (nonzero `scope_id`),
/// which the compact encoder rejects — is rejected up front with
/// `UnencodablePingTarget` rather than PANICKING on the encode: it registers NO
/// probe/ack state and queues NO Packet. A wire-decoded address always decodes
/// `scope_id` / `flowinfo` to 0, so only a caller-supplied target can hit this;
/// a `Result`-returning API must reject it, not abort the process.
#[test]
fn ping_unencodable_target_errs_without_registering_probe_or_ack() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  let t0 = Instant::now();

  // A scoped IPv6 address (nonzero scope_id) the compact encoder rejects, so
  // the framed Ping to it is un-encodable.
  let scoped = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 7003, 0, 1));
  let target = Node::new(SmolStr::new("scoped-peer"), scoped);
  let probes_before = e.probes.len();
  let acks_before = e.ack_registry.len();
  match e.ping(target, t0) {
    Err(Error::UnencodablePingTarget) => {}
    other => panic!("expected UnencodablePingTarget, got {other:?}"),
  }
  assert!(
    e.poll_transmit().is_none(),
    "an unencodable-target ping queues no Packet"
  );
  assert_eq!(
    e.probes.len(),
    probes_before,
    "an unencodable-target ping registers no probe (no dangling probe slot)"
  );
  assert_eq!(
    e.ack_registry.len(),
    acks_before,
    "an unencodable-target ping registers no ack entry (no dangling ack)"
  );
}

/// A relayed IndirectPing whose forwarded `Ping` would exceed `gossip_mtu`
/// under DIFFERENTIAL MTU — this relay's long local id, prepended to a `target`
/// that the requester's shorter inbound IndirectPing (`source + target`) still
/// fit under the same budget — is NOT dropped silently. The requester already
/// counted this helper in its `expected_nacks`, so a missing Nack would make it
/// read a responsive helper as unresponsive, inflating `expected_nacks - seen`
/// and degrading its Lifeguard health. The relay instead sends an immediate
/// Nack — echoing the requester's own seq, to the validated requester address —
/// and registers NO relay/ack state, bumping the dedicated
/// `indirect_forwards_oversized` counter (NOT the flood-cap
/// `indirect_forwards_dropped`).
#[test]
fn handle_indirect_ping_over_mtu_forward_nacks_requester_and_counts_oversized() {
  let gossip_mtu = 1400usize;
  // A long relay local id: its self-Ping (`Ping(local, local)`) must still fit
  // gossip_mtu (the construction floor), but `local + target` for a long target
  // must exceed it.
  let long_local = "L".repeat(500);
  let relay_cfg = EndpointOptions::new(
    SmolStr::new(long_local.as_str()),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  )
  .with_gossip_mtu(gossip_mtu);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(relay_cfg);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  let t0 = Instant::now();

  // A short-id requester and a target sized so the inbound IndirectPing
  // (`source + target`) fits gossip_mtu but the relay-built forwarded Ping
  // (`long local + target`) exceeds it: the realistic differential-MTU trigger.
  let requester_seq = 42u32;
  let short_source = "s";
  let long_target = "t".repeat(1100);
  let requester_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);

  // The inbound IndirectPing itself fits the datagram budget, so the forward is
  // over-MTU only because of the relay's longer local id — not an oversized
  // inbound that a driver's receive buffer would already truncate.
  let inbound_len = crate::wire::encode_message::<SmolStr, SocketAddr>(&Message::IndirectPing(
    ind_ping(&long_target, 7001, short_source, 7002, requester_seq),
  ))
  .expect("the inbound IndirectPing encodes")
  .len();
  assert!(
    inbound_len <= gossip_mtu,
    "the inbound IndirectPing must fit gossip_mtu ({inbound_len} <= {gossip_mtu})"
  );

  let acks_before = e.ack_registry.len();
  e.handle_indirect_ping(
    requester_addr,
    ind_ping(&long_target, 7001, short_source, 7002, requester_seq),
    t0,
  );

  assert!(
    e.indirect_forwards.is_empty(),
    "an over-MTU forward registers no relay state"
  );
  assert_eq!(
    e.ack_registry.len(),
    acks_before,
    "an over-MTU forward registers no ack entry"
  );
  assert_eq!(
    e.metrics().indirect_forwards_oversized,
    1,
    "the oversized forward bumps its dedicated counter"
  );
  assert_eq!(
    e.metrics().indirect_forwards_dropped,
    0,
    "an oversized forward is NOT a flood-cap drop"
  );

  // Exactly one transmit: a Nack echoing the requester's own seq, to the
  // validated requester address — and no forwarded Ping.
  match e
    .poll_transmit()
    .expect("an over-MTU forward Nacks the requester")
  {
    Transmit::Packet(p) => {
      let (to, message) = p.into_parts();
      assert_eq!(
        to, requester_addr,
        "the Nack goes to the validated requester address"
      );
      match message {
        Message::Nack(nack) => assert_eq!(
          nack.sequence_number(),
          requester_seq,
          "the Nack echoes the requester's own probe seq, not our forward seq"
        ),
        other => panic!("expected a Nack, got {other:?}"),
      }
    }
    Transmit::Compound(c) => panic!("expected a lone Nack Packet, not a compound: {c:?}"),
  }
  assert!(
    e.poll_transmit().is_none(),
    "the over-MTU forward emits exactly one transmit (the Nack), no forwarded Ping"
  );

  // The oversized-Nack path left the relay functional: a within-MTU indirect
  // ping still forwards exactly one Ping and registers one relay entry.
  e.handle_indirect_ping(
    requester_addr,
    ind_ping("bob", 7001, short_source, 7002, 43),
    t0,
  );
  match e
    .poll_transmit()
    .expect("a within-MTU indirect ping forwards a Ping")
  {
    Transmit::Packet(p) => assert!(
      matches!(p.message_ref(), Message::Ping(_)),
      "the forward is a lone Ping"
    ),
    Transmit::Compound(c) => {
      panic!("the relay forwards a lone Ping Packet, not a compound: {c:?}")
    }
  }
  assert_eq!(
    e.indirect_forwards.len(),
    1,
    "a within-MTU forward registers exactly one relay entry"
  );
}

/// Two distinct member ids advertising the SAME transport address, both chosen
/// as indirect helpers, contribute exactly ONE IndirectPing to that address.
/// The IndirectPing body (`local + target`) is independent of the helper id,
/// the relay dedups identical forwards, and both `handle_nack` and
/// `ack_source_is_valid` key responders by address — so a second copy could
/// never yield a second Nack. `expected_nacks` must therefore be 1 (not 2), and
/// a single Nack from that address drives `expected_nacks - nacked_by.len()` to
/// 0, so the one responsive helper never leaves a residual awareness penalty.
#[test]
fn indirect_fan_out_dedups_helpers_by_shared_address() {
  // Two eligible helpers (h1, h2) at the SAME address; bob is the probe target
  // at a distinct address. indirect_checks=2 selects both helpers (pick_random
  // returns min(k, candidates)).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg().with_indirect_checks(2));
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  process_alive_auto(&mut e, alive("h1", 7002, 1), false, t0);
  process_alive_auto(&mut e, alive("h2", 7002, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let shared_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);

  // Insert a Detection probe against bob in AwaitingDirectAck with a far-ahead
  // failure deadline, so the elapsed direct sub-window escalates to the indirect
  // fan-out rather than terminating outright.
  let seq = 77u32;
  let (target, target_incarnation, target_generation) = {
    let m = e.members.get(&SmolStr::new("bob")).expect("bob admitted");
    (
      m.state_ref().server_arc(),
      m.state_ref().incarnation(),
      m.generation(),
    )
  };
  e.probes.insert(
    seq,
    Probe::new_direct(
      target,
      target_incarnation,
      target_generation,
      t0,
      ProbeKind::Detection,
      Duration::from_millis(500),
      t0 + Duration::from_secs(100),
    ),
  );

  // Escalate: past the 500ms direct sub-window, well before the failure deadline.
  e.handle_timeout(t0 + Duration::from_secs(1));

  // Exactly ONE IndirectPing may be queued, addressed to the shared helper
  // address (other transmits — a scheduled direct Ping, gossip — are not
  // IndirectPings and are filtered out).
  let mut indirect_to_shared = 0usize;
  while let Some(tx) = e.poll_transmit() {
    if let Transmit::Packet(p) = tx {
      if matches!(p.message_ref(), Message::IndirectPing(_)) {
        assert_eq!(
          p.to_ref(),
          &shared_addr,
          "the only IndirectPing must target the shared helper address"
        );
        indirect_to_shared += 1;
      }
    }
  }
  assert_eq!(
    indirect_to_shared, 1,
    "two ids sharing one address must yield exactly one IndirectPing, not two"
  );

  // `expected_nacks` counts the unique helper address once, and the Nack
  // allowlist holds it exactly once.
  match &e.probes.get(&seq).expect("probe remains").phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      indirect_peers,
      ..
    }) => {
      assert_eq!(
        *expected_nacks, 1,
        "expected_nacks must count the unique helper address once, not per id"
      );
      assert_eq!(
        indirect_peers.as_slice(),
        &[shared_addr],
        "the Nack allowlist must hold the shared address exactly once"
      );
    }
    other => panic!("probe must be AwaitingIndirect, got {other:?}"),
  }

  // One Nack from the shared address answers the single distinct helper:
  // expected_nacks - nacked_by.len() == 0, so no false awareness penalty.
  e.handle_nack(shared_addr, Nack::new(seq), t0 + Duration::from_secs(1));
  match &e.probes.get(&seq).expect("probe remains").phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      nacked_by,
      ..
    }) => {
      assert_eq!(*expected_nacks, 1);
      assert_eq!(
        nacked_by.len(),
        1,
        "the one distinct helper's Nack is recorded"
      );
      assert_eq!(
        expected_nacks.saturating_sub(nacked_by.len()),
        0,
        "the single distinct helper responded → no residual Lifeguard severity"
      );
    }
    other => panic!("probe must be AwaitingIndirect, got {other:?}"),
  }
}

/// A probe of X@A whose target then leaves and is re-admitted at a DIFFERENT
/// address (X@B) before the probe expires must NOT suspect the replacement: X@B
/// is a different membership instance. The `inc < local_inc` guard in
/// `process_suspect` does not catch this, because the address-adoption branch of
/// `process_alive_decided` re-admits the id at an incarnation the guard accepts
/// (here below the probe snapshot). The generation gate in
/// `probe_terminate_failure` protects the replacement: the reclaim draws a fresh
/// generation token, so the probe's snapshot generation no longer matches the
/// live member's.
#[test]
fn probe_expiry_does_not_suspect_a_readdressed_replacement() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80)),
  );
  let t0 = Instant::now();
  // bob@7001 admitted Alive at incarnation 10.
  process_alive_auto(&mut e, alive("bob", 7001, 10), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Start a Detection probe of bob@7001; snapshot target=bob@7001, inc=10.
  let seq = 55u32;
  let (target, target_incarnation, target_generation) = {
    let m = e.members.get(&SmolStr::new("bob")).expect("bob admitted");
    (
      m.state_ref().server_arc(),
      m.state_ref().incarnation(),
      m.generation(),
    )
  };
  assert_eq!(
    target_incarnation, 10,
    "probe snapshots bob@7001 at incarnation 10"
  );
  // Model a normally-dispatched probe (direct Ping sent) so its failure reaches
  // the generation gate rather than the no-dispatch abort.
  let mut probe = Probe::new_direct(
    target,
    target_incarnation,
    target_generation,
    t0,
    ProbeKind::Detection,
    Duration::from_millis(50),
    t0 + Duration::from_millis(80),
  );
  probe.dispatched = true;
  e.probes.insert(seq, probe);

  // bob gracefully leaves (self-marked Dead ⇒ State::Left, immediately
  // address-reclaimable), then a NEW bob instance is admitted at a DIFFERENT
  // address (7777) with a LOWER incarnation (2) than the probe snapshot (10).
  // The address-adoption path bypasses the incarnation guard, so bob@7777 is
  // admitted at incarnation 2.
  e.process_dead(dead("bob", "bob", 10), t0 + Duration::from_millis(10));
  e.process_alive_decided(alive("bob", 7777, 2), false, t0 + Duration::from_millis(20));
  {
    let m = e
      .members
      .get(&SmolStr::new("bob"))
      .expect("bob@7777 present");
    assert_eq!(
      m.state_ref().address_ref().port(),
      7777,
      "replacement adopted 7777"
    );
    assert_eq!(
      m.state_ref().incarnation(),
      2,
      "replacement at incarnation 2"
    );
    assert_eq!(m.state_ref().state(), State::Alive, "replacement is Alive");
  }
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The probe of bob@7001 expires at its failure deadline (t0+80ms).
  e.handle_timeout(t0 + Duration::from_millis(90));
  assert!(!e.probes.contains_key(&seq), "the probe terminated");

  // The replacement bob@7777 must be UNTOUCHED: still Alive, still incarnation
  // 2, no suspicion timer. A probe of bob@7001 says nothing about bob@7777.
  let m = e
    .members
    .get(&SmolStr::new("bob"))
    .expect("bob@7777 present");
  assert_eq!(
    m.state_ref().state(),
    State::Alive,
    "a probe of bob@7001 must not suspect the re-addressed bob@7777"
  );
  assert_eq!(
    m.state_ref().incarnation(),
    2,
    "the replacement's incarnation must be unchanged"
  );
  assert_eq!(m.state_ref().address_ref().port(), 7777);
  assert!(
    m.suspicion().is_none(),
    "no suspicion timer may be armed on the replacement"
  );
}

/// A probe of X@A whose target is removed by `reset_nodes` and then re-admitted
/// at the SAME id and SAME transport address with a LOWER incarnation before the
/// probe expires must NOT suspect the replacement. The rejoin takes the
/// new-member branch of `process_alive_decided` and starts at the arriving
/// incarnation (2), so the probe snapshot (inc 10) is >= the replacement's and
/// `process_suspect`'s `inc < local_inc` guard does NOT reject it. The generation
/// gate in `probe_terminate_failure` protects the fresh instance: the re-admit
/// draws a NEW generation token, so the probe's snapshot generation no longer
/// matches — neither the address (unchanged) nor the incarnation (regressed)
/// could distinguish the instances on its own.
#[test]
fn probe_expiry_does_not_suspect_a_same_address_lower_incarnation_replacement() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80))
      // Short window so reset_nodes reclaims the left node quickly.
      .with_gossip_to_the_dead_time(Duration::from_millis(5)),
  );
  let t0 = Instant::now();
  // bob@7001 admitted Alive at incarnation 10; the probe snapshots that.
  process_alive_auto(&mut e, alive("bob", 7001, 10), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let seq = 56u32;
  let (target, target_incarnation, target_generation) = {
    let m = e.members.get(&SmolStr::new("bob")).expect("bob admitted");
    (
      m.state_ref().server_arc(),
      m.state_ref().incarnation(),
      m.generation(),
    )
  };
  assert_eq!(
    target_incarnation, 10,
    "probe snapshots bob@7001 at incarnation 10"
  );
  // Model a normally-dispatched probe (direct Ping sent) so its failure reaches
  // the generation gate rather than the no-dispatch abort.
  let mut probe = Probe::new_direct(
    target,
    target_incarnation,
    target_generation,
    t0,
    ProbeKind::Detection,
    Duration::from_millis(50),
    t0 + Duration::from_millis(80),
  );
  probe.dispatched = true;
  e.probes.insert(seq, probe);

  // bob gracefully leaves (self-marked Dead ⇒ State::Left), is reclaimed by
  // reset_nodes once past gossip_to_the_dead_time, then a NEW bob is admitted at
  // the SAME address 7001 with a LOWER incarnation (2). Because the id was
  // removed first, this hits the new-member branch, which sets incarnation to 2
  // (a same-address Alive would otherwise be rejected as `inc <= local_inc`).
  e.process_dead(dead("bob", "bob", 10), t0 + Duration::from_millis(10));
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Left),
    "bob self-marked Left"
  );
  e.reset_nodes(t0 + Duration::from_millis(20));
  assert!(
    e.members.get(&SmolStr::new("bob")).is_none(),
    "reset_nodes reclaimed the left bob"
  );
  e.process_alive_decided(alive("bob", 7001, 2), false, t0 + Duration::from_millis(30));
  {
    let m = e
      .members
      .get(&SmolStr::new("bob"))
      .expect("bob@7001 re-admitted");
    assert_eq!(
      m.state_ref().address_ref().port(),
      7001,
      "replacement at the SAME address"
    );
    assert_eq!(
      m.state_ref().incarnation(),
      2,
      "replacement at incarnation 2"
    );
    assert_eq!(m.state_ref().state(), State::Alive, "replacement is Alive");
  }
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The probe of bob@7001/inc10 expires at its failure deadline (t0+80ms).
  e.handle_timeout(t0 + Duration::from_millis(90));
  assert!(!e.probes.contains_key(&seq), "the probe terminated");

  // The replacement bob@7001/inc2 must be UNTOUCHED: still Alive, still
  // incarnation 2, no suspicion timer. A probe of bob@7001/inc10 says nothing
  // about the fresh bob@7001/inc2.
  let m = e
    .members
    .get(&SmolStr::new("bob"))
    .expect("bob@7001 present");
  assert_eq!(
    m.state_ref().state(),
    State::Alive,
    "a probe of bob@7001/inc10 must not suspect the reset+re-admitted bob@7001/inc2"
  );
  assert_eq!(
    m.state_ref().incarnation(),
    2,
    "the replacement's incarnation must be unchanged"
  );
  assert!(
    m.suspicion().is_none(),
    "no suspicion timer may be armed on the replacement"
  );
}

/// F1 core case: a probe of X@A/inc1 whose target is removed by `reset_nodes` and
/// re-admitted at the SAME id, SAME address, and the SAME incarnation before the
/// probe expires must NOT suspect the fresh instance. `(id, address, incarnation)`
/// are all identical to the probed instance, so an `(address, incarnation)` check
/// cannot tell them apart — only the generation token can. The rejoin takes the
/// new-member branch of `process_alive_decided`, which draws a fresh generation,
/// so the probe's snapshot generation no longer matches and no Suspect is emitted.
///
/// Mutation check: reverting the generation gate in `probe_terminate_failure` to
/// `address == probed && incarnation >= snapshot` makes the equal-incarnation
/// fresh instance satisfy that predicate (same address, `1 >= 1`) and be falsely
/// suspected — this test then fails.
#[test]
fn probe_expiry_does_not_suspect_an_equal_incarnation_reset_readmit_replacement() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(80))
      // Short window so reset_nodes reclaims the left node quickly.
      .with_gossip_to_the_dead_time(Duration::from_millis(5)),
  );
  let t0 = Instant::now();
  // bob@7001 admitted Alive at incarnation 1.
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  let original_generation = e.members.get(&SmolStr::new("bob")).unwrap().generation();
  assert_ne!(
    original_generation, 0,
    "a tracked member has a nonzero generation"
  );

  // Start a Detection probe of bob@7001/inc1; `insert_direct_probe` snapshots the
  // original generation and marks the probe dispatched (its direct Ping was
  // sent), so failure reaches the generation gate rather than the no-dispatch
  // abort.
  let seq = 57u32;
  insert_direct_probe(&mut e, seq, "bob", t0, t0 + Duration::from_millis(80));
  let probe_generation = e.probes.get(&seq).unwrap().target_generation;
  assert_eq!(
    probe_generation, original_generation,
    "the probe snapshots the original instance's generation"
  );

  // bob gracefully leaves (State::Left), reset_nodes reclaims it, then a FRESH
  // bob is admitted at the SAME address 7001 and the SAME incarnation 1. Because
  // the id was removed first, this hits the new-member branch, which draws a NEW
  // generation — so (id, address, incarnation) all match the probed instance yet
  // the generation differs.
  e.process_dead(dead("bob", "bob", 1), t0 + Duration::from_millis(10));
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Left),
    "bob self-marked Left"
  );
  e.reset_nodes(t0 + Duration::from_millis(20));
  assert!(
    e.members.get(&SmolStr::new("bob")).is_none(),
    "reset_nodes reclaimed the left bob"
  );
  e.process_alive_decided(alive("bob", 7001, 1), false, t0 + Duration::from_millis(30));

  let fresh_generation = e.members.get(&SmolStr::new("bob")).unwrap().generation();
  assert_ne!(
    fresh_generation, probe_generation,
    "a reset + EQUAL-incarnation re-admit is a NEW instance with a distinct generation"
  );
  assert_ne!(
    fresh_generation, 0,
    "the fresh instance has a nonzero generation"
  );
  {
    let m = e.members.get(&SmolStr::new("bob")).unwrap();
    assert_eq!(m.state_ref().address_ref().port(), 7001, "SAME address");
    assert_eq!(m.state_ref().incarnation(), 1, "SAME incarnation 1");
    assert_eq!(m.state_ref().state(), State::Alive);
  }
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The probe of the departed instance expires at its failure deadline (t0+80ms).
  // The fresh bob@7001/inc1 must be UNTOUCHED — Alive, incarnation 1, no
  // suspicion — because the probe's snapshot generation no longer matches.
  e.handle_timeout(t0 + Duration::from_millis(90));
  assert!(!e.probes.contains_key(&seq), "the probe terminated");
  let m = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(
    m.state_ref().state(),
    State::Alive,
    "the fresh equal-incarnation instance must NOT be suspected"
  );
  assert_eq!(m.state_ref().incarnation(), 1);
  assert!(
    m.suspicion().is_none(),
    "no suspicion timer may be armed on the fresh instance"
  );
}

/// F2: a probe that dispatched NO datagram — its direct Ping is over-MTU, its
/// IndirectPings are over-MTU (the same oversized target id), and reliable ping
/// is DISABLED — reflects a purely LOCAL MTU limit, not peer loss. It must abort
/// cleanly: no Suspect, no awareness penalty, no leaked probe/ack state.
///
/// Mutation check: reverting the abort (always suspecting + penalizing on
/// failure) makes the unprobeable peer suspected and raises the health score, so
/// this test fails.
#[test]
fn unprobeable_over_mtu_peer_aborts_without_suspicion_or_penalty() {
  let gossip_mtu = 1400usize;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_gossip_mtu(gossip_mtu)
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(200)),
  );
  let t0 = Instant::now();
  // A peer whose id alone dwarfs the MTU: its direct Ping AND every IndirectPing
  // (which carries this id as the target) exceed one datagram and are dropped.
  let big_id = "z".repeat(gossip_mtu * 2);
  process_alive_auto(&mut e, alive(&big_id, 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  // Reliable ping disabled → NOTHING can be dispatched for this probe.
  e.disable_reliable_ping(SmolStr::new(big_id.as_str()));
  assert_eq!(e.health_score(), 0, "healthy before the probe");

  // start_probe builds the direct Ping; it is over-MTU and not queued, so the
  // probe records dispatched == false through the real send path.
  assert!(e.start_probe(t0), "a probe is started for the only peer");
  let seq = *e.probes.keys().next().expect("one probe registered");
  assert!(
    !e.probes.get(&seq).unwrap().dispatched,
    "the over-MTU direct Ping was NOT dispatched"
  );
  while e.poll_transmit().is_some() {}

  // Escalate past the direct sub-window (50ms) but before the failure deadline
  // (200ms): the fan-out finds every IndirectPing over-MTU (expected_nacks == 0)
  // and reliable disabled, so it terminates the probe — which aborts.
  e.handle_timeout(t0 + Duration::from_millis(60));

  assert!(
    e.probes.is_empty(),
    "the unprobeable probe aborted, leaving no probe state"
  );
  let m = e.members.get(&SmolStr::new(big_id.as_str())).unwrap();
  assert_eq!(
    m.state_ref().state(),
    State::Alive,
    "an over-MTU peer we never sent any datagram to must NOT be suspected"
  );
  assert!(m.suspicion().is_none(), "no suspicion timer may be armed");
  assert_eq!(
    e.health_score(),
    0,
    "a purely local MTU limit must not charge an awareness failure"
  );
}

/// F2 sibling: the SAME over-MTU peer, but with reliable ping ENABLED. The fan-out
/// opens the reliable-ping fallback (a dial IS a dispatch), so the probe stays
/// alive racing the deadline; when that fallback never acks, the timeout IS a
/// genuine peer failure and DOES suspect + penalize. Only the truly-unprobeable
/// (nothing dispatched) case aborts.
#[test]
fn over_mtu_peer_with_reliable_fallback_still_suspects_on_failure() {
  let gossip_mtu = 1400usize;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_gossip_mtu(gossip_mtu)
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(200)),
  );
  let t0 = Instant::now();
  let big_id = "z".repeat(gossip_mtu * 2);
  process_alive_auto(&mut e, alive(&big_id, 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  // Reliable ping left ENABLED (the default).
  assert!(e.start_probe(t0), "a probe is started for the only peer");
  let seq = *e.probes.keys().next().expect("one probe registered");
  assert!(
    !e.probes.get(&seq).unwrap().dispatched,
    "the over-MTU direct Ping was NOT dispatched"
  );
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Escalate: the fan-out opens the reliable fallback even with zero indirect
  // peers, so the probe does NOT abort — it stays AwaitingIndirect racing the
  // deadline with the reliable stream armed.
  e.handle_timeout(t0 + Duration::from_millis(60));
  match &e
    .probes
    .get(&seq)
    .expect("probe still racing the reliable fallback")
    .phase
  {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      reliable_stream_id,
      ..
    }) => {
      assert_eq!(*expected_nacks, 0, "every IndirectPing is over-MTU");
      assert!(
        reliable_stream_id.is_some(),
        "the reliable fallback was dispatched (a dial counts as a dispatch)"
      );
    }
    other => panic!("expected AwaitingIndirect racing the reliable fallback, got {other:?}"),
  }

  // The reliable fallback never acks; the probe expires at its failure deadline
  // (t0+200ms). A datagram WAS dispatched (the reliable dial), so this is a
  // genuine failure: suspect the peer and charge the awareness penalty.
  e.handle_timeout(t0 + Duration::from_millis(260));
  assert!(e.probes.is_empty(), "the probe terminated");
  let m = e.members.get(&SmolStr::new(big_id.as_str())).unwrap();
  assert_eq!(
    m.state_ref().state(),
    State::Suspect,
    "a reliable-fallback failure IS peer loss and must suspect the target"
  );
  assert!(
    e.health_score() > 0,
    "a dispatched-then-failed probe charges an awareness penalty"
  );
}

/// F2 (monotonic dispatch): when the direct Ping and every IndirectPing are
/// over-MTU, the reliable fallback is the ONLY dispatched attempt. If its DIAL
/// then fails before the probe expires (`dial_failed` clears
/// `reliable_stream_id` while the probe stays live), the probe must STILL
/// suspect on expiry — a failed attempt is peer loss, not "nothing dispatched".
/// Inferring dispatch from the mutable `reliable_stream_id` at terminate time
/// (the pre-fix behavior) misreads the retired fallback as no-attempt and lets
/// the unreachable peer evade suspicion forever; the monotonic
/// `Probe::dispatched` flag, set when the fallback opened, prevents it.
///
/// Mutation check: reverting the abort gate to infer dispatch from
/// `reliable_stream_id.is_some()` makes this peer abort (no Suspect, no penalty).
#[test]
fn over_mtu_reliable_fallback_dial_failed_before_expiry_still_suspects() {
  let gossip_mtu = 1400usize;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_gossip_mtu(gossip_mtu)
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(200)),
  );
  let t0 = Instant::now();
  let big_id = "z".repeat(gossip_mtu * 2);
  process_alive_auto(&mut e, alive(&big_id, 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  assert!(e.start_probe(t0), "a probe is started for the only peer");
  let seq = *e.probes.keys().next().expect("one probe registered");
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  // Escalate: over-MTU direct + all-over-MTU indirect ⇒ the reliable fallback is
  // the sole dispatch. The fan-out records that dispatch monotonically.
  e.handle_timeout(t0 + Duration::from_millis(60));
  let rid = match &e.probes.get(&seq).expect("probe still racing").phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      reliable_stream_id,
      ..
    }) => {
      assert_eq!(*expected_nacks, 0, "every IndirectPing is over-MTU");
      reliable_stream_id.expect("the reliable fallback was dispatched")
    }
    other => panic!("expected AwaitingIndirect racing the reliable fallback, got {other:?}"),
  };
  assert!(
    e.probes.get(&seq).unwrap().dispatched,
    "the reliable dial set the monotonic dispatched flag"
  );
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The reliable fallback's DIAL fails before the cumulative deadline — this
  // retires the fallback (clears reliable_stream_id) but leaves the probe alive.
  e.dial_failed(
    rid,
    StreamError::DialFailed("refused".into()),
    t0 + Duration::from_millis(70),
  );
  match &e
    .probes
    .get(&seq)
    .expect("probe not removed by fallback retirement")
    .phase
  {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      reliable_stream_id, ..
    }) => assert!(
      reliable_stream_id.is_none(),
      "the failed fallback stream is retired"
    ),
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
  assert!(
    e.probes.get(&seq).unwrap().dispatched,
    "retiring the fallback must NOT clear the monotonic dispatched flag"
  );

  // Expire: a dispatched-then-failed probe is genuine peer loss ⇒ suspect + penalty.
  e.handle_timeout(t0 + Duration::from_millis(260));
  assert!(e.probes.is_empty(), "the probe terminated");
  assert_eq!(
    e.members
      .get(&SmolStr::new(big_id.as_str()))
      .unwrap()
      .state_ref()
      .state(),
    State::Suspect,
    "a failed reliable-only fallback IS peer loss and must suspect the target"
  );
  assert!(
    e.health_score() > 0,
    "a dispatched-then-failed probe charges an awareness penalty"
  );
}

/// F2 (monotonic dispatch): same as the `dial_failed` sibling, but the reliable
/// fallback fails via a `ReliablePingFailed` stream event (the connection
/// established, then the reliable ping failed). It likewise retires the fallback
/// (clears `reliable_stream_id`) while the probe stays live, and the probe must
/// STILL suspect on expiry.
#[test]
fn over_mtu_reliable_fallback_ping_failed_before_expiry_still_suspects() {
  let gossip_mtu = 1400usize;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_gossip_mtu(gossip_mtu)
      .with_probe_timeout(Duration::from_millis(50))
      .with_probe_interval(Duration::from_millis(200)),
  );
  let t0 = Instant::now();
  let big_id = "z".repeat(gossip_mtu * 2);
  process_alive_auto(&mut e, alive(&big_id, 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  assert!(e.start_probe(t0), "a probe is started");
  let seq = *e.probes.keys().next().expect("one probe registered");
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  e.handle_timeout(t0 + Duration::from_millis(60));
  match &e.probes.get(&seq).expect("probe still racing").phase {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      expected_nacks,
      reliable_stream_id,
      ..
    }) => {
      assert_eq!(*expected_nacks, 0, "every IndirectPing is over-MTU");
      assert!(
        reliable_stream_id.is_some(),
        "the reliable fallback was dispatched"
      );
    }
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
  assert!(
    e.probes.get(&seq).unwrap().dispatched,
    "the reliable dial set the monotonic dispatched flag"
  );
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // The reliable ping fails via the stream-event path before the deadline.
  let cmd = e.handle_stream_event(
    EndpointEvent::ReliablePingFailed(crate::event::ReliablePingFailed::new(seq)),
    t0 + Duration::from_millis(70),
  );
  assert!(
    cmd.is_none(),
    "ReliablePingFailed returns no stream command"
  );
  match &e
    .probes
    .get(&seq)
    .expect("probe not removed by fallback retirement")
    .phase
  {
    ProbePhase::AwaitingIndirect(AwaitingIndirect {
      reliable_stream_id, ..
    }) => assert!(
      reliable_stream_id.is_none(),
      "the failed fallback is retired"
    ),
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
  assert!(
    e.probes.get(&seq).unwrap().dispatched,
    "ReliablePingFailed must NOT clear the monotonic dispatched flag"
  );

  e.handle_timeout(t0 + Duration::from_millis(260));
  assert!(e.probes.is_empty(), "the probe terminated");
  assert_eq!(
    e.members
      .get(&SmolStr::new(big_id.as_str()))
      .unwrap()
      .state_ref()
      .state(),
    State::Suspect,
    "a failed reliable-only fallback IS peer loss and must suspect the target"
  );
  assert!(
    e.health_score() > 0,
    "a dispatched-then-failed probe charges an awareness penalty"
  );
}

/// F3: indirect-helper selection must be UNIFORM over DISTINCT addresses, not
/// biased by how many alias ids advertise an address. Address A is advertised by
/// `M` alias ids and address B by a single id; with `indirect_checks == 1`, over
/// many independent seeds each distinct address must be chosen ~half the time —
/// NOT `M:1` in A's favor. Sampling by shuffling the raw, duplicate-laden
/// candidate pool gives A ~`M`× the selection probability; deduplicating to
/// distinct addresses BEFORE sampling removes the bias.
///
/// Mutation check: replacing the `FxHashSet` dedup + uniform sample with a
/// shuffle over the duplicate-laden pool makes A win ≈`M/(M+1)` of the trials,
/// breaking the tolerance.
#[test]
fn indirect_helper_selection_is_uniform_over_distinct_addresses() {
  const M: usize = 5; // alias ids all sharing address A
  const TRIALS: u64 = 3000;
  let addr_a = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6001);
  let addr_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6002);

  let mut a_selected = 0u64;
  let mut b_selected = 0u64;
  for seed in 0..TRIALS {
    // Fresh endpoint per trial with a distinct seed, so the aggregate reflects
    // the sampler's distribution rather than one PRNG path.
    let mut e = seeded_endpoint(cfg().with_indirect_checks(1), seed);
    let t0 = Instant::now();
    // M alias ids at address A (6001), one id at address B (6002), and the probe
    // target at a third address (7001). Both A and B are eligible helpers.
    for i in 0..M {
      process_alive_auto(&mut e, alive(&format!("a{i}"), 6001, 1), false, t0);
    }
    process_alive_auto(&mut e, alive("bonly", 6002, 1), false, t0);
    process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
    while e.poll_event().is_some() {}
    while e.poll_transmit().is_some() {}

    // Isolate the IndirectPing selection: drop the reliable fallback.
    e.disable_reliable_ping(SmolStr::new("bob"));
    insert_direct_probe(&mut e, 9, "bob", t0, t0 + Duration::from_secs(3600));
    e.probe_fan_out_indirect(9, t0 + Duration::from_millis(1));

    let (_n, peers) = awaiting_indirect(&e, 9);
    assert_eq!(
      peers.len(),
      1,
      "indirect_checks == 1 selects exactly one helper address"
    );
    match peers[0] {
      p if p == addr_a => a_selected += 1,
      p if p == addr_b => b_selected += 1,
      other => panic!("selected an unexpected helper address {other:?}"),
    }
  }

  assert_eq!(
    a_selected + b_selected,
    TRIALS,
    "every trial selected A or B"
  );
  // Uniform ⇒ each ≈ 0.5. The window is many standard deviations wide (so it
  // never flakes) yet far below the biased ≈M/(M+1) ratio (~0.83) the
  // entry-shuffle predecessor produced.
  let a_frac = a_selected as f64 / TRIALS as f64;
  assert!(
    (0.43..=0.57).contains(&a_frac),
    "address A (advertised by {M} alias ids) was selected {a_selected}/{TRIALS} \
     ({a_frac:.3}); uniform-over-distinct expects ~0.5, an alias-multiplicity bias \
     would push it toward {:.3}",
    M as f64 / (M as f64 + 1.0)
  );
}

// ─── incremental suspicion-deadline index (oracle-checked) ───────────────────
//
// Each test asserts EXACT equality `poll_timeout() == suspicion_deadline_bruteforce()`
// (the old member-list fold), never `<=`: a stale index returns the wrong
// instant, which an inequality assertion would silently admit. No scheduler is
// started and no probe / forward / intent is in flight in these tests, so the
// suspicion minimum is the whole of `poll_timeout` and the two must match
// exactly. `earliest() == fold` is additionally asserted directly — it holds in
// every lifecycle state, unlike `poll_timeout`, which is gated to `None` when
// the endpoint is not Running.

/// Count of members currently carrying a live suspicion — the size the index
/// must have, with no tombstones.
fn live_suspicion_count(e: &Endpoint<SmolStr, SocketAddr>) -> usize {
  e.members.iter().filter(|m| m.suspicion().is_some()).count()
}

/// The always-valid half of the invariant: the index's earliest equals the
/// member-list fold, and its storage carries exactly the live suspicions (no
/// tombstones). Holds in every lifecycle state.
fn assert_index_consistent(e: &Endpoint<SmolStr, SocketAddr>) {
  assert_eq!(
    e.suspicion_deadlines.earliest(),
    e.suspicion_deadline_bruteforce(),
    "index earliest must equal the member-list fold",
  );
  let live = live_suspicion_count(e);
  assert_eq!(
    e.suspicion_deadlines.entry_count(),
    live,
    "ordered-map storage must equal the live-suspicion count (no tombstones)",
  );
  assert_eq!(
    e.suspicion_deadlines.live_key_count(),
    live,
    "authority-map size must equal the live-suspicion count",
  );
}

/// The Running-state half: with no probe / forward / intent / scheduler timer
/// armed, `poll_timeout` surfaces exactly the suspicion minimum.
fn assert_poll_is_suspicion_min(e: &Endpoint<SmolStr, SocketAddr>) {
  assert_eq!(
    e.poll_timeout(),
    e.suspicion_deadline_bruteforce(),
    "poll_timeout must equal the suspicion fold when no other timer is armed",
  );
}

/// An installed suspicion becomes the new earliest, through BOTH ingress paths
/// — a direct `process_suspect` and a `merge_state` Suspect.
#[test]
fn suspicion_index_install_becomes_new_earliest_via_both_ingress_paths() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t_early = Instant::from_origin(Duration::from_secs(10));
  let t_late = Instant::from_origin(Duration::from_secs(20));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t_early);
  process_alive_auto(&mut e, alive("carol", 7002, 1), false, t_early);
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e); // no suspicion yet ⇒ None

  // Direct ingress: suspect carol at the LATER instant — the only suspicion so
  // far, hence the minimum.
  e.process_suspect(suspect("carol", "x", 1), t_late);
  let carol_dl = e
    .members
    .get(&SmolStr::new("carol"))
    .unwrap()
    .suspicion()
    .unwrap()
    .deadline();
  assert_eq!(e.suspicion_deadlines.earliest(), Some(carol_dl));
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e);

  // Merge ingress: a Suspect for bob arriving through `merge_state`, stamped at
  // the EARLIER instant, installs a smaller deadline and becomes the new
  // earliest.
  e.merge_state(&[pns("bob", 7001, 1, State::Suspect)], t_early);
  let bob_dl = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .suspicion()
    .unwrap()
    .deadline();
  assert!(
    bob_dl < carol_dl,
    "bob suspected earlier ⇒ smaller deadline"
  );
  assert_eq!(
    e.suspicion_deadlines.earliest(),
    Some(bob_dl),
    "the merge-path install is the new earliest",
  );
  assert!(e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  assert!(e.suspicion_deadlines.contains(&SmolStr::new("carol")));
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e);
}

/// A confirmation accelerating a suspicion moves the poll minimum strictly
/// inward — the mutant an existing `<=` assertion admits. The threshold
/// `k` (`suspicion_mult - 2`) needs `n >= suspicion_mult`, so five members give
/// `k = 2` and the timer starts at max.
#[test]
fn suspicion_index_confirm_accelerate_moves_the_min() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_suspicion_mult(4)
      .with_suspicion_max_timeout_mult(6),
  );
  let t0 = Instant::from_origin(Duration::from_secs(10));
  for (i, name) in ["bob", "carol", "dave", "erin"].iter().enumerate() {
    process_alive_auto(&mut e, alive(name, 7001 + i as u16, 1), false, t0);
  }
  // A (bob) and B (erin) both suspected at t0 ⇒ both start at max ⇒ equal
  // deadlines, so the minimum is t0 + max.
  e.process_suspect(suspect("bob", "s1", 1), t0);
  e.process_suspect(suspect("erin", "s2", 1), t0);
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e);
  let before = e.poll_timeout().expect("a suspicion deadline is armed");

  // A distinct suspector confirms A, pulling its deadline strictly below B's.
  e.process_suspect(suspect("bob", "s3", 1), t0);
  let bob_dl = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .suspicion()
    .unwrap()
    .deadline();
  let after = e
    .poll_timeout()
    .expect("a suspicion deadline is still armed");
  assert!(
    after < before,
    "the confirmation must pull the minimum strictly inward: {after:?} !< {before:?}",
  );
  assert_eq!(after, bob_dl, "the new minimum is A's accelerated deadline");
  assert_eq!(e.suspicion_deadlines.earliest(), Some(bob_dl));
  assert_poll_is_suspicion_min(&e); // == oracle
  // The in-place confirm update left no tombstone.
  assert_index_consistent(&e);
  assert_eq!(e.suspicion_deadlines.entry_count(), 2);
}

/// A superseding Alive removes the suspicion (falls back to the next term); a
/// non-superseding Alive leaves it — the negative half.
#[test]
fn suspicion_index_clear_via_alive_superseding_removes_non_superseding_leaves() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::from_origin(Duration::from_secs(10));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "x", 1), t0);
  let bob_dl = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .suspicion()
    .unwrap()
    .deadline();
  assert_eq!(e.suspicion_deadlines.earliest(), Some(bob_dl));

  // NEGATIVE: a non-superseding Alive (incarnation <= local) returns before the
  // clear, so the suspicion and its index entry survive.
  e.process_alive_decided(alive("bob", 7001, 1), false, t0);
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Suspect),
    "a non-superseding Alive must not clear the suspicion",
  );
  assert!(e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  assert_eq!(e.suspicion_deadlines.earliest(), Some(bob_dl));
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e);

  // POSITIVE: a superseding Alive (incarnation > local) refutes the suspicion
  // and removes its index entry.
  e.process_alive_decided(alive("bob", 7001, 2), false, t0);
  assert_eq!(
    e.member_liveness(&SmolStr::new("bob")),
    Some(State::Alive),
    "a superseding Alive clears the suspicion",
  );
  assert!(!e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  assert_eq!(e.suspicion_deadlines.earliest(), None);
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e);
}

/// A Dead clears the suspicion and removes its index entry.
#[test]
fn suspicion_index_clear_via_dead_removes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::from_origin(Duration::from_secs(10));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "x", 1), t0);
  assert!(e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  assert_poll_is_suspicion_min(&e);

  e.process_dead(dead("bob", "carol", 1), t0);
  assert_eq!(e.member_liveness(&SmolStr::new("bob")), Some(State::Dead));
  assert!(!e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  assert_eq!(e.suspicion_deadlines.earliest(), None);
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e); // == None
}

/// An expired suspicion (fired via `handle_timeout`) is popped, so
/// `poll_timeout` stops returning the now-past deadline — the driver-busy-loop
/// mutant killer.
#[test]
fn suspicion_index_expire_stops_returning_the_past_instant() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::from_origin(Duration::from_secs(10));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "x", 1), t0);
  let bob_dl = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .suspicion()
    .unwrap()
    .deadline();
  assert_eq!(e.poll_timeout(), Some(bob_dl));

  // Fire the timer well past the deadline: the suspicion expires (→ Dead) and
  // its index entry is popped (the remote-clear path, via the Dead synthesized
  // by `fire_expired_suspicions`).
  e.handle_timeout(bob_dl + Duration::from_secs(1));
  assert_eq!(e.member_liveness(&SmolStr::new("bob")), Some(State::Dead));
  assert!(!e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  assert_ne!(
    e.poll_timeout(),
    Some(bob_dl),
    "a fired suspicion must not remain the poll deadline (busy-loop mutant)",
  );
  assert_eq!(e.poll_timeout(), None);
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e);
}

/// Reclaiming a long-dead member via `reset_nodes` leaves no index entry (the
/// suspicion was already cleared by the preceding Dead — the removal theorem).
#[test]
fn suspicion_index_reset_nodes_removal_leaves_no_entry() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new_seeded(cfg().with_gossip_to_the_dead_time(Duration::from_millis(1)));
  let t0 = Instant::from_origin(Duration::from_secs(10));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "x", 1), t0);
  assert!(e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  // Dead clears the suspicion — the entry is gone BEFORE reclaim, so
  // `reset_nodes`' debug_assert (entry already absent) holds.
  e.process_dead(dead("bob", "carol", 1), t0);
  assert!(!e.suspicion_deadlines.contains(&SmolStr::new("bob")));

  // Reclaim the long-dead member; removal must not resurrect an index entry.
  e.reset_nodes(t0 + Duration::from_secs(1));
  assert!(
    e.member(&SmolStr::new("bob")).is_none(),
    "the long-dead member is reclaimed",
  );
  assert!(!e.suspicion_deadlines.contains(&SmolStr::new("bob")));
  assert_index_consistent(&e);
  assert_poll_is_suspicion_min(&e); // == None
}

/// A leaving endpoint gates `poll_timeout` to `None`, but leave mutates no peer
/// suspicion, so the index still tracks it underneath and still equals the fold.
#[test]
fn suspicion_index_leave_drain_gates_poll_but_index_tracks_underneath() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::from_origin(Duration::from_secs(10));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, t0);
  e.process_suspect(suspect("bob", "x", 1), t0);
  let bob_dl = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .suspicion()
    .unwrap()
    .deadline();
  assert_eq!(e.poll_timeout(), Some(bob_dl));

  e.leave(t0).expect("leave ok");
  assert_eq!(
    e.poll_timeout(),
    None,
    "a leaving endpoint reports no deadline"
  );
  assert!(
    e.suspicion_deadlines.contains(&SmolStr::new("bob")),
    "the entry lingers consistently during the drain",
  );
  // earliest() == fold underneath (both Some(bob_dl)); the oracle debug_assert
  // inside poll_timeout, run above before the gate, would already have fired
  // otherwise.
  assert_index_consistent(&e);
  assert_eq!(e.suspicion_deadlines.earliest(), Some(bob_dl));
}

/// Through install → a confirmation flood → clear → a second install → clear,
/// storage stays exactly equal to the live-suspicion count — the no-tombstone
/// space proof at the FSM level.
#[test]
fn suspicion_index_structural_churn_keeps_storage_at_live_suspicions() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(
    cfg()
      .with_suspicion_mult(4)
      .with_suspicion_max_timeout_mult(6),
  );
  let t0 = Instant::from_origin(Duration::from_secs(10));
  for (i, name) in ["bob", "carol", "dave", "erin"].iter().enumerate() {
    process_alive_auto(&mut e, alive(name, 7001 + i as u16, 1), false, t0);
  }
  assert_index_consistent(&e); // 0 live

  // Install bob (1 live), then a flood of confirmations from distinct sources.
  // Each accepted confirmation is an in-place deadline update; none may accrete
  // a tombstone.
  e.process_suspect(suspect("bob", "s0", 1), t0);
  assert_index_consistent(&e);
  for s in ["s1", "s2", "s3", "s4", "s5"] {
    e.process_suspect(suspect("bob", s, 1), t0);
    assert_index_consistent(&e); // entry_count == live_key_count == 1 throughout
  }
  assert_eq!(
    e.suspicion_deadlines.entry_count(),
    1,
    "a confirmation flood must not grow storage",
  );

  // Install carol (2 live).
  e.process_suspect(suspect("carol", "s0", 1), t0);
  assert_index_consistent(&e);
  assert_eq!(e.suspicion_deadlines.entry_count(), 2);

  // Clear bob via a superseding Alive (1 live).
  e.process_alive_decided(alive("bob", 7001, 9), false, t0);
  assert_index_consistent(&e);
  assert_eq!(e.suspicion_deadlines.entry_count(), 1);

  // Clear carol via Dead (0 live).
  e.process_dead(dead("carol", "z", 1), t0);
  assert_index_consistent(&e);
  assert!(e.suspicion_deadlines.is_empty());
  assert_eq!(e.suspicion_deadlines.entry_count(), 0);
  assert_poll_is_suspicion_min(&e);
}

// ─── incremental pending-dial-intent index (oracle-checked) ──────────────────
//
// The intent-plane twin of the suspicion-index section above. Every mutation of
// `pending_stream_intents` must mirror into `intent_deadlines` through the
// `insert_intent` / `remove_intent` chokepoints (and `leave` clears both), so
// `poll_timeout`'s intent term is an O(log n) index read rather than an
// O(intents) fold an attacker could inflate by parking many dials.

/// The always-valid half of the invariant: the intent index's earliest equals
/// the pending-intent fold, and its storage carries exactly the parked intents
/// (no tombstones). Reads via `earliest_uncounted` so it never perturbs the
/// scan-count measurement.
fn assert_intent_index_consistent(e: &Endpoint<SmolStr, SocketAddr>) {
  assert_eq!(
    e.intent_deadlines.earliest_uncounted(),
    e.pending_stream_intents.values().map(|i| i.deadline).min(),
    "intent index earliest must equal the pending-intent fold",
  );
  let live = e.pending_stream_intents.len();
  assert_eq!(
    e.intent_deadlines.entry_count(),
    live,
    "ordered-map storage must equal the pending-intent count (no tombstones)",
  );
  assert_eq!(
    e.intent_deadlines.live_key_count(),
    live,
    "authority-map size must equal the pending-intent count",
  );
}

/// A datagram flood that inflates the parked-dial-intent count must not turn
/// `poll_timeout` into an O(intents) scan. Parks many pending stream intents
/// through the public dial API, then asserts a single `poll_timeout` examines a
/// small constant number of intent-index entries that does NOT scale with the
/// intent count — the property that denies an attacker O(intents) work per
/// driver re-poll.
///
/// Mutation-verify: reverting the intent term of `poll_timeout` to the old
/// `pending_stream_intents.values().map(|i| i.deadline).min()` fold never calls
/// `DeadlineIndex::earliest`, so `entities_scanned` stays `0` and the `>= 1`
/// assertion fails — the fold's O(intents) work per poll is exactly what this
/// guards against.
#[test]
fn poll_timeout_intent_term_scans_o1_regardless_of_intent_count() {
  use crate::event::PushPullKind;

  fn scanned_for(n: usize) -> (u64, usize) {
    let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
    let t0 = Instant::now();
    while e.poll_event().is_some() {}
    // Park `n` pending dial intents: each `start_push_pull` to a distinct peer
    // queues an intent that stays parked (no dial_succeeded / dial_failed), so
    // `pending_stream_intents` — and the mirrored `intent_deadlines` index —
    // grows to `n`.
    for i in 0..n {
      let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 20000 + i as u16);
      let _ = e.start_push_pull(peer, PushPullKind::Join, t0);
    }
    assert_eq!(e.pending_stream_intents.len(), n, "all intents parked");
    // Settle, then measure exactly one poll in isolation.
    let _ = e.poll_timeout();
    e.intent_deadlines.reset_entities_scanned();
    let _ = e.poll_timeout();
    (
      e.intent_deadlines.entities_scanned(),
      e.pending_stream_intents.len(),
    )
  }

  let (small, small_n) = scanned_for(8);
  let (large, large_n) = scanned_for(512);
  assert_eq!(small_n, 8);
  assert_eq!(large_n, 512);
  assert!(
    small >= 1,
    "poll_timeout must consult the intent index — reverting it to the O(intents) \
     fold never calls earliest(), leaving this 0"
  );
  assert!(
    small <= 2,
    "one poll must examine O(1) intent-index entries, not scale with the parked \
     intents; examined {small}"
  );
  assert_eq!(
    large, small,
    "intent-term examination count must not grow with the {large_n} parked intents \
     (was {large}, single-baseline {small})"
  );
}

/// Every intent maintenance chokepoint keeps `intent_deadlines` in lockstep with
/// `pending_stream_intents`: the three inserts (`start_push_pull` /
/// `start_reliable_ping` / `start_user_message`) and the removes via
/// `dial_succeeded` and `dial_failed`. The consistency oracle is asserted after
/// each step.
#[test]
fn intent_index_tracks_pending_intents_through_dial_lifecycle() {
  use crate::{error::StreamError, event::PushPullKind};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  assert_intent_index_consistent(&e); // 0 intents

  let p = |port: u16| SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);

  // Insert via all three start_* chokepoints, each with a distinct deadline so
  // the surfaced minimum is unambiguous.
  let id_pp = e.start_push_pull(p(7101), PushPullKind::Join, t0);
  assert_intent_index_consistent(&e);
  let id_rp = e.start_reliable_ping(SmolStr::new("bob"), p(7102), 7, t0 + Duration::from_secs(5));
  assert_intent_index_consistent(&e);
  let id_um = e
    .start_user_message(p(7103), bytes::Bytes::from_static(b"hi"), t0)
    .expect("running node accepts a user message");
  assert_intent_index_consistent(&e);
  assert_eq!(e.pending_stream_intents.len(), 3);
  assert_eq!(e.intent_deadlines.entry_count(), 3);

  // Remove one via dial_succeeded (promoted to a live stream)...
  let _stream = e.dial_succeeded(id_pp, t0).expect("dial within deadline");
  assert!(!e.pending_stream_intents.contains_key(&id_pp));
  assert_intent_index_consistent(&e);

  // ...one via dial_failed...
  e.dial_failed(id_um, StreamError::DialFailed("refused".into()), t0);
  assert!(!e.pending_stream_intents.contains_key(&id_um));
  assert_intent_index_consistent(&e);

  // ...leaving only the reliable-ping intent parked.
  assert!(e.pending_stream_intents.contains_key(&id_rp));
  assert_eq!(e.pending_stream_intents.len(), 1);
  assert_eq!(e.intent_deadlines.entry_count(), 1);
  assert_intent_index_consistent(&e);
}

/// `leave()` drops all parked intents and must clear the mirrored index in
/// lockstep, leaving no stale earliest-intent deadline behind. The oracle in
/// `poll_timeout` runs before the lifecycle gate, so it also checks the index
/// against the (now empty) fold in the Leaving state.
#[test]
fn intent_index_cleared_on_leave() {
  use crate::event::PushPullKind;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}

  for i in 0..4u16 {
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7201 + i);
    let _ = e.start_push_pull(peer, PushPullKind::Join, t0);
  }
  assert_eq!(e.intent_deadlines.entry_count(), 4);
  assert_intent_index_consistent(&e);

  e.leave(t0).expect("leave ok");

  assert!(
    e.pending_stream_intents.is_empty(),
    "leave drops all intents"
  );
  assert!(
    e.intent_deadlines.is_empty(),
    "leave clears the intent index"
  );
  assert_eq!(e.intent_deadlines.entry_count(), 0);
  assert_eq!(e.intent_deadlines.live_key_count(), 0);
  // Oracle still holds in the Leaving state (both empty).
  assert_intent_index_consistent(&e);
  let _ = e.poll_timeout();
}

// ── Inbound push/pull response cache: O(1) per-request servicing ─────────────

fn sock(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

fn inbound_pushpull_request(
  peer: SocketAddr,
  states: Vec<PushNodeState<SmolStr, SocketAddr>>,
  stream_id: u64,
) -> EndpointEvent<SmolStr, SocketAddr> {
  EndpointEvent::PushPullRequestReceived(PushPullRequestReceived::new_with_stream_id(
    peer,
    states,
    Bytes::new(),
    PushPullKind::Join,
    StreamId::from_raw(stream_id),
  ))
}

fn pushpull_response_bytes(cmd: Option<StreamCommand<SmolStr, SocketAddr>>) -> Bytes {
  match cmd {
    Some(StreamCommand::SendPushPullResponse(resp)) => resp.into_encoded(),
    other => panic!("expected SendPushPullResponse, got {other:?}"),
  }
}

fn decode_pushpull(bytes: &Bytes) -> crate::typed::PushPull<SmolStr, SocketAddr> {
  let (_, msg) = crate::wire::decode_message::<SmolStr, SocketAddr>(bytes)
    .expect("cached push/pull response must decode");
  match msg {
    crate::typed::Message::PushPull(pp) => {
      assert!(
        !pp.join(),
        "a push/pull response always clears the join flag"
      );
      pp
    }
    other => panic!("expected a PushPull response frame, got {other:?}"),
  }
}

fn decode_pushpull_response(
  cmd: Option<StreamCommand<SmolStr, SocketAddr>>,
) -> crate::typed::PushPull<SmolStr, SocketAddr> {
  decode_pushpull(&pushpull_response_bytes(cmd))
}

fn reply_member_ids(cmd: Option<StreamCommand<SmolStr, SocketAddr>>) -> Vec<SmolStr> {
  decode_pushpull_response(cmd)
    .states_slice()
    .iter()
    .map(|s| s.id_ref().cheap_clone())
    .collect()
}

/// The completed inbound push/pull path serves a cached, pre-encoded response:
/// the O(members) fold+encode runs once per membership-changing tick, never per
/// request. Drives many requests against a static member table and asserts the
/// build counter stays flat, then grows the table and asserts a single tick
/// rebuilds exactly once — the counter scales with neither request count nor
/// member count.
#[test]
fn pushpull_response_cache_built_once_per_membership_change_not_per_request() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}

  // Grow a large member table.
  const N: u16 = 60;
  for i in 0..N {
    process_alive_auto(&mut e, alive(&format!("m{i}"), 9000 + i, 1), false, t0);
  }
  // Publish the current membership into the response cache (the schedule-fired
  // tick is the only place the O(members) response is (re)built).
  e.handle_timeout(t0);
  assert_eq!(e.num_members(), N as usize + 1);
  let builds_before = e.pushpull_response_builds();

  // Drive many inbound requests against STATIC membership. Each pushes a state
  // we already hold at the same incarnation (a no-op merge → no version bump),
  // so the cache is served, never rebuilt.
  let peer = sock(9500);
  // The served frame is an O(1) clone of the cache: every request shares the
  // SAME underlying buffer. A rebuild — extracted or inlined — allocates a fresh
  // frame at a different address, so a stable pointer proves no per-request fold
  // ran (complementing the build counter below).
  let mut cache_ptr: Option<*const u8> = None;
  for req_i in 0..200u64 {
    let bytes = pushpull_response_bytes(e.handle_stream_event(
      inbound_pushpull_request(peer, vec![pns("m0", 9000, 1, State::Alive)], req_i + 1),
      t0,
    ));
    match cache_ptr {
      None => cache_ptr = Some(bytes.as_ptr()),
      Some(p) => assert_eq!(
        bytes.as_ptr(),
        p,
        "each response must be an O(1) clone of the same cached buffer, not a rebuild"
      ),
    }
    assert_eq!(
      decode_pushpull(&bytes).states_slice().len(),
      e.num_members(),
      "every response is complete (carries all members)"
    );
  }
  assert_eq!(
    e.pushpull_response_builds(),
    builds_before,
    "the per-request path must never rebuild the response (O(1) clone per request)"
  );

  // Grow again + a single tick → exactly one more build, regardless of the 200
  // requests already served or the member count.
  for i in N..(2 * N) {
    process_alive_auto(&mut e, alive(&format!("m{i}"), 9000 + i, 1), false, t0);
  }
  e.handle_timeout(t0);
  assert_eq!(
    e.pushpull_response_builds(),
    builds_before + 1,
    "one membership-changing tick rebuilds exactly once"
  );

  // Still O(1) at the larger size: a fresh cache buffer (from the grow tick),
  // again shared across every request.
  let builds_after_grow = e.pushpull_response_builds();
  let mut cache_ptr2: Option<*const u8> = None;
  for req_i in 0..200u64 {
    let bytes = pushpull_response_bytes(e.handle_stream_event(
      inbound_pushpull_request(peer, vec![pns("m0", 9000, 1, State::Alive)], 1000 + req_i),
      t0,
    ));
    match cache_ptr2 {
      None => cache_ptr2 = Some(bytes.as_ptr()),
      Some(p) => assert_eq!(bytes.as_ptr(), p),
    }
    assert_eq!(
      decode_pushpull(&bytes).states_slice().len(),
      e.num_members()
    );
  }
  assert_eq!(
    e.pushpull_response_builds(),
    builds_after_grow,
    "requests remain O(1) after the table grows"
  );
}

/// The cached response reflects membership as of the last tick: a change applied
/// between ticks is at most one tick stale (absent until the next refresh), and
/// present afterward. Proves the staleness bound is exactly one tick.
#[test]
fn pushpull_response_reflects_membership_change_only_after_the_next_tick() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  process_alive_auto(&mut e, alive("known", 9001, 1), false, t0);
  e.handle_timeout(t0); // cache: [local, known]

  // Add "late" WITHOUT an intervening tick.
  process_alive_auto(&mut e, alive("late", 9002, 1), false, t0);
  assert!(
    e.member(&SmolStr::new("late")).is_some(),
    "late is a member immediately (the merge/insert is not tick-gated)"
  );

  // A request BEFORE the next tick serves the pre-change cache: local + known,
  // but NOT late.
  let peer = sock(9500);
  let ids = reply_member_ids(e.handle_stream_event(inbound_pushpull_request(peer, vec![], 1), t0));
  assert!(
    ids.iter().any(|id| id == &SmolStr::new("known")),
    "the reply carries the last-tick view of known, got {ids:?}"
  );
  assert!(
    !ids.iter().any(|id| id == &SmolStr::new("late")),
    "late (added since the last tick) is at most one tick stale, not yet in the reply: {ids:?}"
  );

  // After a tick, a fresh request reflects late.
  e.handle_timeout(t0);
  let ids2 = reply_member_ids(e.handle_stream_event(inbound_pushpull_request(peer, vec![], 2), t0));
  assert!(
    ids2.iter().any(|id| id == &SmolStr::new("late")),
    "after a tick the reply reflects late: {ids2:?}"
  );
}

/// A Leaving/Left node closes an inbound push/pull rather than serving a stale
/// live snapshot: the lifecycle gate precedes the cached-response path.
#[test]
fn pushpull_response_left_node_closes_and_never_serves_the_cache() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  e.leave(t0).expect("leave ok");
  assert!(e.is_left());

  let peer = sock(9500);
  let cmd = e.handle_stream_event(
    inbound_pushpull_request(peer, vec![pns("carol", 9003, 1, State::Alive)], 1),
    t0,
  );
  assert!(
    matches!(cmd, Some(StreamCommand::Close)),
    "a left node must Close an inbound push/pull, never serve a live snapshot, got {cmd:?}"
  );
}

/// `set_local_state_snapshot` mutates the response body (the local application
/// state) WITHOUT moving `snapshot_version`, so it marks the cache dirty; the
/// next tick then rebuilds the response with the new snapshot.
#[test]
fn pushpull_response_cache_dirtied_by_set_local_state_snapshot() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg());
  let t0 = Instant::now();
  while e.poll_event().is_some() {}
  e.handle_timeout(t0); // cache built with the initial (empty) snapshot

  e.set_local_state_snapshot(Bytes::from_static(b"app-state-v2"))
    .expect("set_local_state_snapshot ok");

  // A request BEFORE a tick still serves the old cache (empty snapshot): the
  // dirty flag defers the rebuild to the tick.
  let peer = sock(9500);
  let before =
    decode_pushpull_response(e.handle_stream_event(inbound_pushpull_request(peer, vec![], 1), t0))
      .user_data_bytes();
  assert!(
    before.is_empty(),
    "pre-tick reply carries the old (empty) snapshot: {before:?}"
  );

  // After a tick, the dirty flag forces a rebuild reflecting the new snapshot,
  // even though `snapshot_version` never moved.
  e.handle_timeout(t0);
  let after =
    decode_pushpull_response(e.handle_stream_event(inbound_pushpull_request(peer, vec![], 2), t0))
      .user_data_bytes();
  assert_eq!(
    after.as_ref(),
    b"app-state-v2",
    "the dirty flag makes the next tick rebuild the cache with the new snapshot"
  );
}
