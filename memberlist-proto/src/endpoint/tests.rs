use super::*;
use core::net::{IpAddr, Ipv4Addr, SocketAddr};
use smol_str::SmolStr;

fn cfg() -> EndpointOptions<SmolStr, SocketAddr> {
  EndpointOptions::new(
    SmolStr::new("local"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000),
  )
  .with_rng_seed(0xdeadbeef)
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
  I: crate::Id
    + Data
    + crate::CheapClone
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
  A: crate::CheapClone
    + Data
    + Eq
    + core::hash::Hash
    + core::fmt::Debug
    + core::fmt::Display
    + Send
    + Sync
    + 'static,
{
  e.process_alive(alive, bootstrap, now);
}

#[test]
fn new_endpoint_inserts_local_at_incarnation_1() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert_eq!(e.local_id_ref(), &SmolStr::new("local"));
  assert_eq!(e.num_members(), 1);
  let local = e.member(&SmolStr::new("local")).expect("local present");
  assert_eq!(local.id_ref(), &SmolStr::new("local"));
  assert_eq!(local.state(), State::Alive);
}

#[test]
fn new_endpoint_is_not_leaving_or_left() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert!(!e.is_left());
  assert!(!e.is_leaving());
}

#[test]
fn new_endpoint_health_score_is_zero() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert_eq!(e.health_score(), 0);
}

#[test]
fn health_score_reflects_awareness() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let t0 = Instant::from_origin(core::time::Duration::from_secs(1));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at(cfg(), t0);
  assert_eq!(
    e.node_state_change(&SmolStr::new("local")),
    Some(t0),
    "local member must be stamped at the driver clock, not Instant::now()"
  );

  // Driving entirely with driver instants at/after `t0` (all far below the
  // wall clock) must not panic.
  e.handle_timeout(t0 + core::time::Duration::from_millis(500));
}

#[test]
fn try_new_at_with_seed_is_infallible() {
  // A seeded config never touches platform entropy, so the fallible
  // constructor always succeeds — the fully Sans-I/O / deterministic path.
  let t0 = Instant::from_origin(core::time::Duration::from_secs(1));
  let e = Endpoint::<SmolStr, SocketAddr>::try_new_at(cfg(), t0)
    .expect("seeded construction never fails");
  assert_eq!(e.num_members(), 1);
}

#[test]
fn scheduler_deadlines_saturate_at_extreme_now() {
  // A pathological near-maximum clock must not panic when the scheduler builds
  // probe/gossip/push-pull deadlines as `now + interval`; the forward
  // arithmetic saturates instead.
  let near_max =
    Instant::from_origin(core::time::Duration::MAX - core::time::Duration::from_secs(1));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at(cfg(), near_max);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  e.process_suspect(suspect("bob", "carol", 1), Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Suspect);
  assert!(bob.suspicion().is_some());
}

#[test]
fn suspect_self_refutes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 5), false, Instant::now());
  e.process_suspect(suspect("bob", "carol", 1), Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Alive);
  assert!(bob.suspicion().is_none());
}

#[test]
fn dead_alive_node_marks_dead_and_emits_left() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let t_late = Instant::from_origin(core::time::Duration::from_secs(100));
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at(cfg(), t_late);
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

  let t_early = Instant::from_origin(core::time::Duration::from_secs(1));
  e.reset_nodes(t_early); // must not panic
  assert!(
    e.members.get(&SmolStr::new("bob")).is_some(),
    "a Dead member must not be reclaimed when now precedes its state_change"
  );
}

#[test]
fn dead_self_when_not_leaving_refutes() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  e.merge_state(&[pns("bob", 7001, 2, State::Left)], Instant::now());
  let bob = e.members.get(&SmolStr::new("bob")).unwrap();
  assert_eq!(bob.state_ref().state(), State::Dead);
}

#[test]
fn handle_timeout_fires_expired_suspicion() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert!(e.poll_timeout().is_none());
}

#[test]
fn poll_timeout_returns_min_across_suspicion_probe_and_forward() {
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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

#[test]
fn update_meta_emits_node_updated_and_increments_incarnation() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let at_limit = Meta::try_from(Bytes::from(vec![0u8; META_MAX_SIZE])).unwrap();
  let r = e.update_meta(at_limit);
  assert!(r.is_ok(), "meta at the limit should be accepted");
}

#[test]
fn leave_with_no_live_peers_emits_left_cluster_immediately() {
  // No live peers ⇒ nothing to flush ⇒ legacy `if any_alive` gate is
  // false ⇒ leave completes immediately (LeftCluster synchronous).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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

/// A zero-live-peer leave must complete immediately even when unrelated
/// traffic (a stale ping Ack here) is already queued. A `pending_transmits.
/// is_empty()` boundary would wrongly defer this.
#[test]
fn leave_no_live_peers_not_delayed_by_stale_transmit() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  // LeftCluster must be emitted immediately (no live peers ⇒ legacy
  // `if any_alive` is false), NOT held behind the stale Ack.
  let events: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(
    events.iter().any(|ev| matches!(ev, Event::LeftCluster)),
    "zero-live-peer leave must complete immediately despite stale transmit"
  );
  // The stale Ack is still queued (it is not the leave notice).
  {
    let tx = e.poll_transmit();
    let is_ack_packet =
      matches!(&tx, Some(Transmit::Packet(p)) if matches!(p.message_ref(), Message::Ack(_)));
    assert!(is_ack_packet, "stale Ack packet expected");
  }
}

/// With a live peer, the completion boundary is exactly the queued
/// dead-self (plus any stale prefix), never trailing post-leave traffic.
/// LeftCluster fires when the dead-self is drained and is neither delayed
/// by, nor spuriously emitted for, unrelated packets.
#[test]
fn leave_left_cluster_boundary_is_the_dead_self_not_other_traffic() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let t0 = Instant::now();
  process_alive_auto(&mut e, alive("peer", 7001, 1), false, t0);
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Stale packet queued BEFORE leave (a prefix ahead of the dead-self).
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
  e.handle_ping(from, ping_to("local", 7000, "alice", 8001, 1), t0);

  e.leave(t0).expect("ok"); // queues dead-self ⇒ [stale Ack, dead-self]
  assert!(e.is_left());
  let pre: Vec<_> = core::iter::from_fn(|| e.poll_event()).collect();
  assert!(pre.iter().any(|ev| matches!(ev, Event::NodeLeft(_))));
  assert!(
    !pre.iter().any(|ev| matches!(ev, Event::LeftCluster)),
    "LeftCluster must not fire before the dead-self drains"
  );

  // Pop #1: the stale prefix Ack — boundary not reached yet.
  {
    let tx = e.poll_transmit();
    assert!(
      matches!(&tx, Some(Transmit::Packet(p)) if matches!(p.message_ref(), Message::Ack(_))),
      "expected stale Ack packet"
    );
  }
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "stale prefix packet must not trigger LeftCluster"
  );

  // Enqueue a TRAILING packet (post-leave) — sits behind the dead-self.
  e.handle_ping(from, ping_to("local", 7000, "alice", 8001, 2), t0);

  // Pop #2: the dead-self ⇒ boundary reached ⇒ LeftCluster now.
  {
    let tx = e.poll_transmit();
    assert!(
      matches!(&tx, Some(Transmit::Packet(p)) if matches!(p.message_ref(), Message::Dead(_))),
      "expected dead-self packet"
    );
  }
  assert!(
    core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "LeftCluster fires exactly when the dead-self is handed off"
  );

  // Pop #3: the trailing Ack — must NOT emit a second LeftCluster.
  {
    let tx = e.poll_transmit();
    assert!(
      matches!(&tx, Some(Transmit::Packet(p)) if matches!(p.message_ref(), Message::Ack(_))),
      "expected trailing Ack packet"
    );
  }
  assert!(
    !core::iter::from_fn(|| e.poll_event()).any(|ev| matches!(ev, Event::LeftCluster)),
    "trailing post-leave traffic must not re-trigger LeftCluster"
  );
}

#[test]
fn leave_is_idempotent() {
  // A repeated leave is a harmless no-op, not an error: once already
  // left/shutdown, `leave()` is idempotent and must not re-broadcast.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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

#[test]
fn user_data_emits_user_packet_event() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let initial_len = e.broadcast_queue_len();
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  assert!(e.broadcast_queue_len() > initial_len);
}

// ─────────────── Synchronous AliveDelegate admission ─────────────────────

/// An [`AliveDelegate`] that vetoes every inbound alive.
struct RejectAllAlive;
impl crate::delegate::AliveDelegate<SmolStr, SocketAddr> for RejectAllAlive {
  fn notify_alive(&self, _peer: &crate::typed::NodeState<SmolStr, SocketAddr>) -> bool {
    false
  }
}

#[test]
fn alive_delegate_reject_drops_the_message() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
impl crate::delegate::MergeDelegate<SmolStr, SocketAddr> for RejectAllMerge {
  fn notify_merge(&self, peers: &[crate::typed::NodeState<SmolStr, SocketAddr>]) -> bool {
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

  use crate::event::PushPullRequestReceived;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  while e.poll_event().is_some() {}
  let d = Arc::new(RejectAllMerge {
    seen: Mutex::new(Vec::new()),
  });
  e.set_merge_delegate(ArcMerge(d.clone()));

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let ev = EndpointEvent::PushPullRequestReceived(PushPullRequestReceived::new(
    peer,
    vec![pns("carol", 7003, 1, State::Alive)],
    Bytes::new(),
    PushPullKind::Join,
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

  use crate::event::PushPullReplyReceived;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  while e.poll_event().is_some() {}
  let d = Arc::new(RejectAllMerge {
    seen: Mutex::new(Vec::new()),
  });
  e.set_merge_delegate(ArcMerge(d.clone()));

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let ev = EndpointEvent::PushPullReplyReceived(PushPullReplyReceived::new(
    peer,
    vec![pns("carol", 7003, 1, State::Alive)],
    Bytes::new(),
    PushPullKind::Join,
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

/// The `MergeDelegate` is join-only: a periodic Refresh push/pull is never
/// gated, so it merges even with a reject-all delegate installed.
#[test]
fn merge_delegate_not_consulted_for_refresh() {
  use EndpointEvent;
  use PushPullKind;
  use StreamCommand;
  use bytes::Bytes;
  use std::sync::{Arc, Mutex};

  use crate::event::PushPullRequestReceived;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  while e.poll_event().is_some() {}
  let d = Arc::new(RejectAllMerge {
    seen: Mutex::new(Vec::new()),
  });
  e.set_merge_delegate(ArcMerge(d.clone()));

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let ev = EndpointEvent::PushPullRequestReceived(PushPullRequestReceived::new(
    peer,
    vec![pns("carol", 7003, 1, State::Alive)],
    Bytes::new(),
    PushPullKind::Refresh,
  ));
  let cmd = e.handle_stream_event(ev, Instant::now());

  assert!(
    matches!(cmd, Some(StreamCommand::SendPushPullResponse(_))),
    "refresh must respond, not close, got {cmd:?}"
  );
  assert!(
    e.member(&SmolStr::new("carol")).is_some(),
    "refresh merge must apply despite a reject-all MergeDelegate"
  );
  assert!(
    d.seen.lock().unwrap().is_empty(),
    "MergeDelegate must not be consulted for a Refresh push/pull"
  );
}

/// Newtype so an `Arc<RejectAllMerge>` can be installed as the boxed
/// `MergeDelegate` while the test retains a handle to inspect `seen`.
struct ArcMerge(std::sync::Arc<RejectAllMerge>);
impl crate::delegate::MergeDelegate<SmolStr, SocketAddr> for ArcMerge {
  fn notify_merge(&self, peers: &[crate::typed::NodeState<SmolStr, SocketAddr>]) -> bool {
    self.0.notify_merge(peers)
  }
}

#[test]
fn set_ack_payload_round_trips() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let budget = cfg().gossip_mtu();
  let res = e.set_ack_payload(Bytes::from(vec![0xab_u8; 4096]));
  match res {
    Err(crate::error::Error::AckPayloadExceedsMtu(encoded, reported_budget)) => {
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
    Endpoint::new(cfg().with_dead_node_reclaim_time(Duration::from_millis(50)));
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
    Endpoint::new(cfg().with_max_stream_frame_size(small_cap));
  // Budget after the reserve is ~4096; an 8 KiB snapshot's framed PushPull is
  // well over it.
  let oversized = Bytes::from(vec![0u8; 8192]);
  let res = e.set_local_state_snapshot(oversized);
  assert!(
    matches!(res, Err(crate::error::Error::LocalStateExceedsFrame(_, _))),
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
fn queue_user_broadcast_appends_fifo() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert_eq!(e.user_broadcast_queue_len(), 0);
  e.queue_user_broadcast(Bytes::from_static(b"hello"))
    .unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"world"))
    .unwrap();
  assert_eq!(e.user_broadcast_queue_len(), 2);
}

#[test]
fn drain_user_broadcasts_respects_limit() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  e.queue_user_broadcast(Bytes::from_static(b"aaaa")).unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"bbbb")).unwrap();
  e.queue_user_broadcast(Bytes::from_static(b"cccc")).unwrap();
  // Each 4 B payload is charged its assembled compound-part size:
  // 4 + USER_PART_OVERHEAD (= COMPOUND_MAX_PART_PREFIX_LEN 5 + UserData tag
  // 1 + plain-frame body-len varint 5 = 11) = 15 B. Limit 30 => pulls the
  // first two (15+15=30), the third (would be 45) does not fit.
  let drained = e.drain_user_broadcasts(30);
  assert_eq!(drained.len(), 2);
  assert_eq!(drained[0].as_ref(), b"aaaa");
  assert_eq!(drained[1].as_ref(), b"bbbb");
  assert_eq!(e.user_broadcast_queue_len(), 1);
}

#[test]
fn drain_user_broadcasts_zero_limit_returns_empty() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  e.queue_user_broadcast(Bytes::from_static(b"data")).unwrap();
  let drained = e.drain_user_broadcasts(0);
  assert!(drained.is_empty());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert!(!e.start_probe(Instant::now()), "no eligible target");
  assert!(e.poll_transmit().is_none());
}

#[test]
fn start_probe_emits_ping_to_alive_peer() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  e.process_dead(dead("bob", "carol", 1), Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}
  assert!(!e.start_probe(Instant::now()), "bob is dead");
}

#[test]
fn start_probe_round_robins_across_alive_peers() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_probe_timeout(pt));
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

#[test]
fn handle_ack_for_unknown_seq_is_noop() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_probe_timeout(pt));
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let bob_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  e.ping(Node::new(SmolStr::new("bob"), bob_addr), t0);
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

fn nacked_by_addrs(
  e: &Endpoint<SmolStr, SocketAddr>,
  seq: u32,
) -> smallvec::SmallVec<[SocketAddr; 4]> {
  match &e.probes.get(&seq).expect("probe present").phase {
    crate::probe::ProbePhase::AwaitingIndirect(crate::probe::AwaitingIndirect {
      nacked_by,
      ..
    }) => nacked_by.clone(),
    other => panic!("expected AwaitingIndirect, got {other:?}"),
  }
}

fn indirect_probe_at(
  e: &mut Endpoint<SmolStr, SocketAddr>,
  seq: u32,
  sent_at: Instant,
  failure_deadline: Instant,
  indirect_peers: smallvec::SmallVec<[SocketAddr; 4]>,
) {
  let target = e
    .members
    .get(&SmolStr::new("bob"))
    .unwrap()
    .state_ref()
    .server_arc();
  let expected_nacks = indirect_peers.len();
  e.probes.insert(
    seq,
    crate::probe::Probe {
      target,
      sent_at,
      kind: crate::probe::ProbeKind::Detection,
      // For Detection in AwaitingIndirect the failure deadline IS the
      // phase (cumulative) deadline, by the single-source invariant.
      failure_deadline,
      phase: crate::probe::ProbePhase::AwaitingIndirect(crate::probe::AwaitingIndirect {
        expected_nacks,
        indirect_peers,
        nacked_by: smallvec::SmallVec::new(),
        reliable_stream_id: None,
        deadline: failure_deadline,
      }),
    },
  );
}

#[test]
fn handle_nack_counts_distinct_indirect_responders() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let p2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7102);
  let t0 = Instant::now();
  indirect_probe_at(
    &mut e,
    7,
    t0,
    t0 + Duration::from_secs(1),
    smallvec::SmallVec::from_iter([p1, p2]),
  );

  e.handle_nack(p1, Nack::new(7), t0 + Duration::from_millis(10));
  e.handle_nack(p2, Nack::new(7), t0 + Duration::from_millis(20));

  let seen = nacked_by_addrs(&e, 7);
  assert_eq!(seen.len(), 2, "two distinct indirect peers nacked");
  assert!(seen.contains(&p1) && seen.contains(&p2));
}

#[test]
fn handle_nack_dedupes_repeated_nack_from_same_responder() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let p2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7102);
  let t0 = Instant::now();
  indirect_probe_at(
    &mut e,
    7,
    t0,
    t0 + Duration::from_secs(1),
    smallvec::SmallVec::from_iter([p1, p2]),
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let off_path = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7999);
  let t0 = Instant::now();
  indirect_probe_at(
    &mut e,
    7,
    t0,
    t0 + Duration::from_secs(1),
    smallvec::SmallVec::from_iter([p1]),
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let t0 = Instant::now();
  let deadline = t0 + Duration::from_secs(1);
  indirect_probe_at(&mut e, 7, t0, deadline, smallvec::SmallVec::from_iter([p1]));

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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  let p1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7101);
  let p2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7102);
  let t0 = Instant::now();
  let deadline = t0 + Duration::from_secs(1);
  indirect_probe_at(
    &mut e,
    7,
    t0,
    deadline,
    smallvec::SmallVec::from_iter([p1, p2]),
  );
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

// ─────────────── handle_indirect_ping ────────────────────────────────────

use IndirectPing;

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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
    crate::probe::ProbePhase::AwaitingIndirect(crate::probe::AwaitingIndirect {
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_probe_timeout(pt));
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let bob_node = Node::new(
    SmolStr::new("bob"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
  );
  e.ping(bob_node, t0);
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
    Some(crate::probe::ProbeKind::Ping)
  ));
}

#[test]
fn ping_completes_on_ack_with_pingcompleted_event() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let bob_node = Node::new(
    SmolStr::new("bob"),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001),
  );
  e.ping(bob_node, t0);
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
  let ping_id = e.ping(bob_node, t0);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
    crate::probe::ProbePhase::AwaitingIndirect(crate::probe::AwaitingIndirect {
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
      let (to, msgs): (SocketAddr, Vec<Message<SmolStr, SocketAddr>>) = match tx {
        Transmit::Packet(p) => {
          let (to, message) = p.into_parts();
          (to, vec![message])
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let id1 = e.allocate_stream_id();
  let id2 = e.allocate_stream_id();
  assert_eq!(id2.as_u64(), id1.as_u64() + 1);
}

#[test]
fn start_push_pull_emits_dial_requested_and_dial_succeeded_returns_stream() {
  use Event;
  use PushPullKind;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let n = stream.poll_transmit(t0, &mut buf).expect("bytes expected");
  assert!(n > 0, "expected non-empty encoded PushPull");

  // First byte is PUSH_PULL_MESSAGE_TAG = 8.
  assert_eq!(buf[0], 8u8, "first byte must be PUSH_PULL_MESSAGE_TAG");
}

#[test]
fn dial_failed_removes_intent() {
  use crate::{error::StreamError, event::PushPullKind};

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);

  e.dial_failed(id, StreamError::DialFailed("refused".into()), t0);
  // Dialing the same id again returns None.
  assert!(e.dial_succeeded(id, t0).is_none());
}

#[test]
fn accept_stream_returns_inbound_stream() {
  use crate::stream::StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  let now = Instant::now();
  let s = e.accept_stream(from, now);
  assert!(!s.is_done());
  assert!(matches!(s.phase, StreamPhase::InboundAwaitingFirstMessage));
  assert!(s.poll_timeout().is_some());
}

#[test]
fn outbound_push_pull_decode_and_merge() {
  use crate::event::PushPullKind;
  use PushNodeState;
  use PushPull;
  use State;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  stream.poll_transmit(t0, &mut _req_buf);

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

  // Route event through Endpoint. This is a Refresh reply, so the
  // MergeDelegate gate is not consulted (join-only) and the merge applies
  // synchronously and inline.
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("local-node", 7000, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  // Peer dials us.
  let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer_addr, t0);

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
      let (local_states, user_data) = resp.into_parts();
      // local_states is already in wire-format PushNodeState (with the
      // local node's tracked incarnation), so we hand it straight to the
      // encoder.
      let encoded = Endpoint::encode_push_pull_response(&local_states, user_data, false);
      assert!(!encoded.is_empty(), "encoded response must be non-empty");
      assert_eq!(encoded[0], 8u8, "first byte must be PUSH_PULL_MESSAGE_TAG");

      Endpoint::stream_load_response(&mut stream, encoded, t0 + Duration::from_secs(5));
      let mut out = Vec::new();
      let n = stream.poll_transmit(t0, &mut out).expect("bytes");
      assert!(n > 0);
    }
    StreamCommand::Close => panic!("expected SendPushPullResponse, got Close"),
  }
}

// ─────────────── Reliable ping / probe FSM tests ─────────────────────────

#[test]
fn start_reliable_ping_emits_dial_requested() {
  use Event;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let n = stream.poll_transmit(t0, &mut buf).expect("bytes expected");
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer, t0);

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
    .poll_transmit(t0, &mut out)
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let t0 = Instant::now();

  // ── Outbound: dial → drain request ⇒ OutboundAwaitingResponse ──────────
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s_out = e.dial_succeeded(id, t0).expect("stream");
  let mut buf = Vec::new();
  assert!(s_out.poll_transmit(t0, &mut buf).is_some(), "request bytes");
  assert!(
    matches!(
      s_out.phase,
      crate::stream::StreamPhase::OutboundAwaitingResponse(_)
    ),
    "drain must advance OutboundSendingRequest → OutboundAwaitingResponse"
  );

  // ── Inbound: accept → req → load response → drain ⇒ Done + Closed ──────
  let mut s_in = e.accept_stream(peer, t0);
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
      let (local_states, user_data) = resp.into_parts();
      let enc =
        Endpoint::<SmolStr, SocketAddr>::encode_push_pull_response(&local_states, user_data, false);
      Endpoint::<SmolStr, SocketAddr>::stream_load_response(
        &mut s_in,
        enc,
        t0 + Duration::from_secs(5),
      );
    }
    StreamCommand::Close => panic!("expected SendPushPullResponse"),
  }
  buf.clear();
  assert!(s_in.poll_transmit(t0, &mut buf).is_some(), "response bytes");
  assert!(
    s_in.is_done(),
    "draining the response must advance InboundSendingResponse → Done"
  );
  assert!(
    s_in
      .poll_event()
      .is_some_and(|ev| matches!(ev, crate::event::StreamEvent::Closed)),
    "Done must emit StreamEvent::Closed"
  );
}

/// An outbound reliable ping must reject an Ack whose sequence number does
/// not match the probe — otherwise a stale/relayed/spoofed Ack falsely
/// marks a dead target healthy.
#[test]
fn outbound_reliable_ping_rejects_wrong_ack_seq() {
  use Ack;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let t0 = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let id = e.start_reliable_ping(SmolStr::new("peer"), peer, 42, t0 + Duration::from_secs(5));
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");
  let mut buf = Vec::new();
  s.poll_transmit(t0, &mut buf); // drain ping ⇒ OutboundAwaitingResponse

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let t0 = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let mut s = e.accept_stream(peer, t0);
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
    s.poll_transmit(t0, &mut out).is_none(),
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_max_stream_frame_size(cap));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();

  // Header only: tag=8 (push/pull) + varint(body_len = 2048). total =
  // 1 + 2 + 2048 = 2051 > cap(1024). The body is NEVER sent.
  let mut s = e.accept_stream(peer, t0);
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
  let mut s2 = e.accept_stream(peer, t0);
  let overflow = [8u8, 0x80, 0x80, 0x80, 0x80, 0x80];
  assert!(
    s2.handle_data(&overflow, t0).is_err(),
    "a never-terminating varint must be rejected"
  );

  // A terminal overflowing 5th byte: `80 80 80 80 10` encodes 2^32 in
  // base-128 LE. Under u32 accumulation this wrapped to 0 and was accepted
  // as a complete 6-byte empty PushPull, bypassing the size guard. Decoding
  // via u64 keeps the true value (2^32) → total far exceeds cap → Err.
  let mut s2b = e.accept_stream(peer, t0);
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
  let mut s3 = e.accept_stream(peer, t0);
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
/// beyond u32::MAX is malformed REGARDLESS of `max_stream_frame_size`. Even
/// with the cap configured absurdly high (above 4 GiB), a
/// terminal-overflowing varint must be rejected here — not passed through to
/// the u32 wire decoder where it would wrap and desync framing.
#[test]
fn over_u32_frame_length_rejected_even_with_huge_cap() {
  // Cap well above u32::MAX so a cap-only check would not catch a ~2^32
  // declared length — the u32 wire limit must be enforced independently.
  let mut e: Endpoint<SmolStr, SocketAddr> =
    Endpoint::new(cfg().with_max_stream_frame_size(usize::MAX));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_max_stream_frame_size(cap));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);
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
  let mut e2: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_max_stream_frame_size(cap));
  let mut s2 = e2.accept_stream(peer, t0);
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
  let mut e3: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_max_stream_frame_size(cap));
  let mut s3 = e3.accept_stream(peer, t0);
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
    Endpoint::new(cfg().with_stream_timeout(Duration::from_millis(100)));
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
  let mut s = e.accept_stream(peer, t0); // deadline = t0 + 100ms
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
  let mut s2 = e.accept_stream(peer, t0);
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
  let mut s3 = e.accept_stream(peer, t0);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let probe_seq = 55u32;
  let bob = e.member(&SmolStr::new("bob")).unwrap().clone();
  let now = Instant::now();

  // Probe in AwaitingIndirect with the reliable fallback armed
  // concurrently — a ReliablePingAcked must win the race and succeed the
  // probe even though the indirect deadline has not elapsed.
  let stream_id = StreamId::from_raw(999);
  e.probes.insert(
    probe_seq,
    crate::probe::Probe {
      target: bob,
      sent_at: now,
      kind: crate::probe::ProbeKind::Detection,
      failure_deadline: now + Duration::from_secs(5),
      phase: crate::probe::ProbePhase::AwaitingIndirect(crate::probe::AwaitingIndirect {
        expected_nacks: 2,
        indirect_peers: smallvec::SmallVec::new(),
        nacked_by: smallvec::SmallVec::new(),
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_user_message(peer, Bytes::from_static(b"hello"), t0);

  let ev = e.poll_event().expect("DialRequested");
  match ev {
    Event::DialRequested(p) => assert_eq!(p.id(), id),
    other => panic!("expected DialRequested, got {other:?}"),
  }

  let mut stream = e.dial_succeeded(id, t0).expect("stream");
  let mut buf = Vec::new();
  let n = stream.poll_transmit(t0, &mut buf).expect("bytes");
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
      .is_some_and(|ev| matches!(ev, crate::event::StreamEvent::Closed)),
    "user-message completion must emit StreamEvent::Closed"
  );
}

#[test]
fn inbound_user_data_emits_user_packet_event() {
  use EndpointEvent;
  use Event;
  use Reliability;
  use bytes::Bytes;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer, t0);

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut stream = e.accept_stream(peer, t0);

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
    Endpoint::new(cfg().with_stream_timeout(Duration::from_millis(100)));
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
    stream.poll_transmit(deadline, &mut buf).is_none(),
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
  use crate::event::StreamEvent;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_max_stream_frame_size(1024));
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);
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
  use crate::error::StreamError;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let probe_seq = 77u32;
  let bob = e.member(&SmolStr::new("bob")).unwrap().clone();
  let t0 = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);

  // Dial a reliable ping — registers the intent with OutboundKind::ReliablePing.
  let stream_id = e.start_reliable_ping(SmolStr::new("bob"), peer, probe_seq, t0);
  while e.poll_event().is_some() {} // drain DialRequested

  let deadline = t0 + Duration::from_secs(5);
  e.probes.insert(
    probe_seq,
    crate::probe::Probe {
      target: bob,
      sent_at: t0,
      kind: crate::probe::ProbeKind::Detection,
      failure_deadline: deadline,
      phase: crate::probe::ProbePhase::AwaitingIndirect(crate::probe::AwaitingIndirect {
        expected_nacks: 2,
        indirect_peers: smallvec::SmallVec::new(),
        nacked_by: smallvec::SmallVec::new(),
        reliable_stream_id: Some(stream_id),
        deadline,
      }),
    },
  );

  e.dial_failed(stream_id, StreamError::DialFailed("refused".into()), t0);

  // Probe still alive — the indirect path keeps racing the deadline.
  let probe = e.probes.get(&probe_seq).expect("probe must NOT be removed");
  match &probe.phase {
    crate::probe::ProbePhase::AwaitingIndirect(crate::probe::AwaitingIndirect {
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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
    Endpoint::new(cfg().with_probe_timeout(Duration::from_millis(50)));
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
    Endpoint::new(cfg().with_stream_timeout(Duration::from_millis(100)));
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
    s.poll_transmit(t0 + Duration::from_millis(100), &mut buf)
      .is_none(),
    "a stream past its deadline must transmit nothing"
  );
  assert!(buf.is_empty(), "no bytes may reach the wire post-deadline");
  assert!(
    matches!(
      s.phase,
      crate::stream::StreamPhase::Failed(StreamError::Timeout)
    ),
    "poll_transmit past the deadline fails the stream, got {:?}",
    s.phase
  );
  assert!(
    s.output_buf.is_empty(),
    "the queued request is dropped, not merely withheld"
  );
  // Idempotent.
  assert!(
    s.poll_transmit(t0 + Duration::from_millis(200), &mut buf)
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
    s.poll_transmit(t0, &mut buf).is_some(),
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

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
  use crate::stream::StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  // Need a probe sequence first — start_reliable_ping is keyed on the
  // probe_seq from an originating UDP probe. Use a synthetic seq.
  let id = e.start_reliable_ping(
    SmolStr::from("peer"),
    peer,
    42,
    t0 + core::time::Duration::from_secs(1),
  );
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
  use crate::stream::StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_user_message(peer, bytes::Bytes::from_static(b"payload"), t0);
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
  use crate::stream::StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

  // Feed a complete push/pull request frame so the FSM transitions
  // from `InboundAwaitingFirstMessage` → `InboundSendingResponse`.
  let request_bytes = build_push_pull_request_bytes();
  s.handle_data(&request_bytes, t0)
    .expect("complete request frame is decoded");
  assert!(
    matches!(s.phase, StreamPhase::InboundSendingResponse(_)),
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("alice", 7000, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");
  let mut _req_buf = Vec::new();
  s.poll_transmit(t0, &mut _req_buf); // advance to OutboundAwaitingResponse
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  process_alive_auto(&mut e, alive("alice", 7000, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let id = e.start_push_pull(peer, PushPullKind::Refresh, t0);
  while e.poll_event().is_some() {}
  let mut s = e.dial_succeeded(id, t0).expect("stream");
  let mut _req_buf = Vec::new();
  s.poll_transmit(t0, &mut _req_buf);
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
    !events
      .iter()
      .any(|ev| matches!(ev, crate::event::StreamEvent::Closed)),
    "no Closed lifecycle event after split-delivery failure — got {events:?}"
  );
}

#[test]
fn done_phase_empty_eof_is_still_a_noop() {
  // Done + EMPTY data (the EOF marker) MUST remain a no-op — this is
  // the clean-close path the QUIC bridge uses to signal peer FIN after
  // a successful exchange (companion to the Done+data fail-Decode case).
  use crate::stream::StreamPhase;

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

  // Feed a complete request frame so the FSM moves to
  // InboundSendingResponse.
  let req = build_push_pull_request_bytes();
  s.handle_data(&req, t0).expect("decode request");
  assert!(matches!(s.phase, StreamPhase::InboundSendingResponse(_)));

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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let t0 = Instant::now();
  let mut s = e.accept_stream(peer, t0);

  // Feed a complete request frame (advances to InboundSendingResponse).
  let request_bytes = build_push_pull_request_bytes();
  s.handle_data(&request_bytes, t0).expect("decode request");
  assert!(matches!(s.phase, StreamPhase::InboundSendingResponse(_)));

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
  let mut a: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_a);
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
  let mut b: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg_b);
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
    .poll_transmit(t0, &mut a_request)
    .expect("A request bytes");
  assert!(!a_request.is_empty(), "request must be non-empty");
  assert_eq!(a_request[0], 8u8, "must start with PUSH_PULL_MESSAGE_TAG");
  // Draining the request via poll_transmit auto-advances A to
  // OutboundAwaitingResponse — the real driver contract, no manual phase
  // mutation and no removed write-completion helper.
  assert!(
    matches!(
      stream_a.phase,
      crate::stream::StreamPhase::OutboundAwaitingResponse(_)
    ),
    "poll_transmit must advance the outbound phase"
  );

  // ── Step 2: B accepts A's stream and handles the request ─────────────
  let mut stream_b = b.accept_stream(make_addr(7000), t0);
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
    StreamCommand::SendPushPullResponse(resp) => {
      let (local_states, user_data) = resp.into_parts();
      Endpoint::encode_push_pull_response(&local_states, user_data, false)
    }
    StreamCommand::Close => panic!("expected SendPushPullResponse, got Close"),
  };
  assert!(!encoded_b.is_empty(), "B response must be non-empty");
  assert_eq!(
    encoded_b[0], 8u8,
    "response must start with PUSH_PULL_MESSAGE_TAG"
  );

  Endpoint::stream_load_response(&mut stream_b, encoded_b, t0 + Duration::from_secs(5));

  // Drain B's response bytes (simulating the driver sending them to A).
  let mut b_response = Vec::new();
  stream_b
    .poll_transmit(t0, &mut b_response)
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
  )
  .with_rng_seed(42);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  .with_push_pull_interval(Duration::ZERO)
  .with_rng_seed(0);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);

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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  .with_push_pull_interval(Duration::ZERO)
  .with_rng_seed(1);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);

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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  .with_push_pull_interval(Duration::from_secs(30))
  .with_rng_seed(7);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);

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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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

  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  while e.poll_event().is_some() {}
  e.leave(Instant::now()).expect("leave ok");
  while e.poll_transmit().is_some() {}
  while e.poll_event().is_some() {}

  let meta = Meta::try_from(Bytes::from_static(b"v2")).unwrap();
  assert!(matches!(
    e.update_meta(meta),
    Err(crate::error::Error::NotRunning)
  ));
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  .with_push_pull_interval(Duration::ZERO)
  .with_rng_seed(1);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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

/// A lone near-MTU user broadcast that is a valid standalone plain-frame
/// datagram must be emitted as a single `Packet` — NOT permanently wedged
/// because `drain_user_broadcasts` charges it the compound per-part
/// overhead against the compound-reduced residual budget. 1384 B: as a
/// compound part it is charged 1384 + 11 = 1395 > the 1394 residual
/// (would never be sent, blocks the FIFO forever); as a lone `Packet` its
/// wire frame is ~1390 B <= 1400 (must be sent).
#[test]
fn gossip_lone_near_mtu_user_broadcast_emits_packet() {
  let (mut e, t0) = gossip_harness_one_target();
  e.queue_user_broadcast(bytes::Bytes::from(vec![0xab_u8; 1384]))
    .unwrap();
  e.next_gossip = Some(t0);
  e.handle_timeout(t0);

  let txs = collect_transmits(&mut e);
  assert_eq!(
    txs.len(),
    1,
    "a lone sendable user broadcast must produce exactly one transmit, got {}",
    txs.len()
  );
  match &txs[0] {
    Transmit::Packet(p) => match p.message_ref() {
      Message::UserData(b) => assert_eq!(
        b.len(),
        1384,
        "the near-MTU user payload must ride a lone Packet"
      ),
      other => panic!("expected Packet(UserData(1384)), got Packet({other:?})"),
    },
    other => panic!("expected Packet(UserData(1384)), got {other:?}"),
  }
  assert_eq!(
    e.user_broadcast_queue_len(),
    0,
    "the emitted user broadcast must be drained, not wedged at the FIFO front"
  );
}

/// An un-gossipable user payload (larger than ANY single UDP datagram) is
/// rejected at `queue_user_broadcast` rather than stored: it could never be
/// gossiped even alone, so accepting it would falsely report success and
/// leave it queued until a gossip tick discarded it. A valid payload queued
/// after the rejected one is unaffected and still goes out.
#[test]
fn gossip_oversized_user_broadcast_rejected_at_enqueue() {
  let (mut e, t0) = gossip_harness_one_target();
  // Its lone framed `UserData` packet exceeds `gossip_mtu`, so it is
  // deterministically untransmittable and rejected without being stored.
  assert!(matches!(
    e.queue_user_broadcast(bytes::Bytes::from(vec![0x5a_u8; 2000])),
    Err(crate::error::Error::UserBroadcastExceedsMtu(_, _))
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
    0,
    "the emitted payload must leave the FIFO"
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
/// next tick (best-effort, FIFO intact).
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let bob_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let ping_id = e.ping(Node::new(SmolStr::new("bob"), bob_addr), t0);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_probe_timeout(pt));
  let bob_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  process_alive_auto(&mut e, alive("bob", 7001, 1), false, Instant::now());
  while e.poll_event().is_some() {}
  while e.poll_transmit().is_some() {}

  let t0 = Instant::now();
  let ping_id = e.ping(Node::new(SmolStr::new("bob"), bob_addr), t0);
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let dest = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7002);
  // A payload larger than gossip_mtu is always oversize after framing.
  let huge = bytes::Bytes::from(vec![0u8; e.gossip_mtu() + 1]);
  assert!(e.send_user_packet(dest, huge).is_err());
  assert!(e.poll_transmit().is_none());
}

#[test]
fn send_user_packets_compounds_when_multiple() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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

/// `with_gossip_mtu` propagates through to `Endpoint::gossip_mtu()`, which is
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
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(c);
  assert_eq!(
    e.gossip_mtu(),
    1200,
    "Endpoint::gossip_mtu() must return the configured value (not the \
     hard-coded legacy 1400) so composed coordinators read the operator's \
     budget",
  );
}

#[test]
fn advertise_ref_returns_configured_address() {
  let e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert_eq!(
    e.advertise_ref(),
    &SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7000)
  );
}

#[test]
fn node_incarnation_tracks_known_member_and_is_none_for_unknown() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new_at(cfg(), now);
  process_alive_auto(&mut e, alive("peer", 7100, 1), false, now);
  let before = e
    .node_state_change(&SmolStr::new("peer"))
    .expect("peer present");
  e.age_member(&SmolStr::new("peer"), core::time::Duration::from_secs(5));
  let after = e
    .node_state_change(&SmolStr::new("peer"))
    .expect("peer present");
  assert_eq!(
    before - core::time::Duration::from_secs(5),
    after,
    "state_change must roll back by exactly the delta"
  );
  // Aging an unknown member is a silent no-op (no panic, no state created).
  e.age_member(&SmolStr::new("ghost"), core::time::Duration::from_secs(1));
  assert!(e.node_incarnation(&SmolStr::new("ghost")).is_none());
}

// ─── lifecycle / accessor edges ──────────────────────────────────────────────

#[test]
fn lifecycle_accessor_reports_running_then_left() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  assert_eq!(e.lifecycle(), Lifecycle::Running);
  // No live peers ⇒ leave completes immediately to Left.
  e.leave(Instant::now()).expect("leave ok");
  assert_eq!(e.lifecycle(), Lifecycle::Left);
}

#[test]
fn members_iterator_yields_every_known_member() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let e = Endpoint::<SmolStr, SocketAddr>::try_new(seedless).expect("seedless std construction");
  assert_eq!(e.num_members(), 1);
}

#[test]
fn drain_broadcasts_returns_queued_messages() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let n = e.num_members();
  e.process_suspect(suspect("ghost", "bob", 1), Instant::now());
  assert_eq!(e.num_members(), n, "an unknown-node Suspect is a no-op");
}

#[test]
fn dead_for_unknown_node_is_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let n = e.num_members();
  e.process_dead(dead("ghost", "bob", 1), Instant::now());
  assert_eq!(e.num_members(), n, "an unknown-node Dead is a no-op");
}

#[test]
fn dead_with_older_incarnation_is_ignored() {
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let n = e.num_members();
  let remote = vec![pns("weird", 7009, 1, State::Unknown(99))];
  e.merge_state(&remote, Instant::now());
  assert_eq!(e.num_members(), n, "an Unknown-state entry must be skipped");
}

// ─── handle_ack / handle_nack edges ──────────────────────────────────────────

#[test]
fn handle_nack_for_untracked_seq_is_noop() {
  // A Nack for a sequence with no pending probe is dropped silently.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
    Endpoint::new(cfg().with_gossip_to_the_dead_time(Duration::from_millis(10)));
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg().with_meta_max_size(4));
  let big = Meta::try_from(&b"way-too-long"[..]).unwrap();
  let err = e
    .update_meta(big)
    .expect_err("oversized meta must be rejected");
  assert!(
    matches!(err, crate::error::Error::MetaExceedsCap(_, _)),
    "got {err:?}"
  );
}

// ─── stream events: RemoteStateReceived + reliable-ping failure ──────────────

#[test]
fn push_pull_reply_with_user_data_emits_remote_state_received() {
  use crate::event::PushPullKind;
  use bytes::Bytes;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let now = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let states = vec![pns("carol", 7003, 1, State::Alive)];
  // A non-empty user_data payload must surface as RemoteStateReceived after
  // the merge is admitted.
  let ev = EndpointEvent::PushPullReplyReceived(crate::event::PushPullReplyReceived::new(
    peer,
    states,
    Bytes::from_static(b"peer-app-state"),
    PushPullKind::Refresh,
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
  use crate::event::PushPullKind;
  use bytes::Bytes;
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let now = Instant::now();
  let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7001);
  let states = vec![pns("carol", 7003, 1, State::Alive)];
  let ev = EndpointEvent::PushPullRequestReceived(crate::event::PushPullRequestReceived::new(
    peer,
    states,
    Bytes::from_static(b"inbound-app-state"),
    PushPullKind::Refresh,
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

#[test]
fn reliable_ping_failed_event_retires_fallback_without_failing_probe() {
  // Drive a probe to AwaitingIndirect with the reliable fallback armed, then
  // route a ReliablePingFailed back through handle_stream_event: the fallback
  // is retired but the probe keeps running (failure is decided only by the
  // cumulative deadline).
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(
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
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  e.dial_failed(
    StreamId::from_raw(999),
    crate::error::StreamError::PeerClosed,
    Instant::now(),
  );
}

#[test]
fn ping_untracked_node_synthesizes_target_state() {
  // Pinging a node not in membership synthesizes a minimal Alive NodeState so a
  // later PingCompleted can still carry the target (the `None` arm of the
  // member lookup). A direct Ack then completes it.
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg());
  let now = Instant::now();
  let target_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7099);
  let node = Node::new(SmolStr::new("stranger"), target_addr);
  let ping_id = e.ping(node, now);
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
  .with_push_pull_interval(Duration::ZERO)
  .with_rng_seed(1);
  let mut e: Endpoint<SmolStr, SocketAddr> = Endpoint::new(cfg);
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
