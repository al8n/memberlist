//! Delegate-policy scenarios ported from `memberlist-core/src/base/tests.rs`.
//!
//! These four tests cover the four legacy scenarios that require configurable
//! per-host acceptance policies (analogues to `MergeDelegate`, `AliveDelegate`,
//! `ConflictDelegate`, and `PingDelegate`):
//!
//! | Test | Legacy scenario (line) |
//! |---|---|
//! | `conflict_detected_when_two_nodes_share_id` | `memberlist_conflict_delegate` (977) |
//! | `ping_delegate_payload_roundtrip` | `memberlist_ping_delegate` (1065) |
//! | `join_cancel_when_merge_rejected` | `memberlist_join_cancel` (363) |
//! | `join_cancel_passive_when_alive_rejected` | `memberlist_join_cancel_passive` (457) |

use bytes::Bytes;
use memberlist_simulation::{Alive, Cluster, Event, Node, State};
use smol_str::SmolStr;
use std::net::SocketAddr;

fn addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().unwrap()
}

// ── 1. `memberlist_conflict_delegate` (line 977) ──────────────────────────────

/// Two endpoints share the same id but different addresses. When m1 joins m2,
/// m2 receives an Alive about id "node-x" at addr_1 (different from its own
/// addr_2) and must emit `Event::NodeConflict`.
///
/// This uses the real join path (push/pull) rather than `inject_alive`, so it
/// exercises a different code path than `alive_node_conflict` in
/// `tests/legacy_alive.rs` (Task C).
///
/// Ported from `memberlist-core/src/base/tests.rs:977 memberlist_conflict_delegate`.
#[test]
fn conflict_detected_when_two_nodes_share_id() {
  let mut c = Cluster::new();
  let addr_1 = addr(34001);
  let addr_2 = addr(34002);
  // Both nodes claim the same id — the classic conflict scenario.
  let a1 = c.add_node(SmolStr::new("node-x"), addr_1);
  let a2 = c.add_node(SmolStr::new("node-x"), addr_2);

  // m1 joins m2 via push/pull. m2 receives an Alive about "node-x" at addr_1
  // while m2's own local entry for "node-x" is at addr_2 → NodeConflict.
  c.join(a1, a2);
  for _ in 0..200 {
    c.step();
  }

  // m2 should have observed at least one Event::NodeConflict.
  let mut saw_conflict = false;
  while let Some(ev) = c.poll_event(a2) {
    if matches!(ev, Event::NodeConflict { .. }) {
      saw_conflict = true;
    }
  }
  assert!(saw_conflict, "m2 should have emitted Event::NodeConflict");
}

// ── 2. `memberlist_ping_delegate` (line 1065) ─────────────────────────────────

/// m1 sets `set_ack_payload(b"whatever")`. m2 pings m1. The resulting
/// `Event::PingCompleted` on m2 carries `payload == b"whatever"`.
///
/// Ported from `memberlist-core/src/base/tests.rs:1065 memberlist_ping_delegate`.
#[test]
fn ping_delegate_payload_roundtrip() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34101));
  let a2 = c.add_node(SmolStr::new("m2"), addr(34102));

  // Bootstrap: m2 knows m1 so trigger_ping can find it.
  c.alive_node(a2, Alive::new(1, Node::new(SmolStr::new("m1"), a1)), false);

  // m1 will attach this payload to every Ack it sends.
  c.set_ack_payload(a1, Bytes::from_static(b"whatever"));

  // m2 pings m1.
  c.trigger_ping(a2, SmolStr::new("m1"), a1);
  for _ in 0..200 {
    c.step();
  }

  // m2 should have received a PingCompleted with the configured payload.
  let mut got_payload: Option<Bytes> = None;
  while let Some(ev) = c.poll_event(a2) {
    if let Event::PingCompleted(p) = ev {
      got_payload = Some(p.payload_ref().clone());
      break;
    }
  }
  assert!(
    got_payload.is_some(),
    "m2 should have received PingCompleted"
  );
  assert_eq!(
    got_payload.as_deref(),
    Some(b"whatever".as_ref()),
    "PingCompleted payload should be b\"whatever\""
  );
}

// ── 3. `memberlist_join_cancel` (line 363) ────────────────────────────────────

/// m2 is configured to reject all `PendingMerge` *and* `PendingAlive` tokens,
/// simulating a `MergeDelegate` that cancels the join entirely.
///
/// The legacy `MergeDelegate::notify_merge` callback blocks both the push/pull
/// state merge *and* any Alive gossip that arrives as part of the exchange.  In
/// our simulation those two gate points are `PendingMerge` (for the push/pull
/// bulk state) and `PendingAlive` (for per-peer Alive gossip packets).  Both
/// must be rejected to reproduce the full "join cancel" behaviour where m2 ends
/// up with only itself in its membership view.
///
/// m1 only rejects merges of *m2*'s push/pull state (via the `PendingMerge`
/// path, because m2 initiates). m1 still accepts the `PendingAlive` gossip
/// from m2 that arrives via UDP broadcast after the push/pull, so m1 will
/// eventually learn about m2 through the gossip path.
///
/// Ported from `memberlist-core/src/base/tests.rs:363 memberlist_join_cancel`.
#[test]
fn join_cancel_when_merge_rejected() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34201));
  let a2 = c.add_node(SmolStr::new("m2"), addr(34202));

  // m2 rejects every PendingMerge (push/pull bulk state) AND every
  // PendingAlive (per-peer gossip) it receives during step(). Together these
  // replicate the full legacy MergeDelegate "cancel" semantics.
  c.reject_merges(a2);
  c.reject_alives(a2);

  // m2 initiates a join push/pull with m1.
  c.join(a2, a1);
  for _ in 0..200 {
    c.step();
  }

  // m2 rejected all merge and alive decisions → m1 must not appear in m2's view.
  assert_eq!(
    c.num_members(a2),
    1,
    "m2 rejected all PendingMerge and PendingAlive — should still see only itself"
  );

  // m1 is unaffected; it remains alive.
  assert_eq!(
    c.get_node_state(a1, &SmolStr::new("m1")),
    Some(State::Alive),
    "m1 should still be alive"
  );
}

// ── 4. `memberlist_join_cancel_passive` (line 457) ────────────────────────────

/// m2 is configured to reject all `PendingAlive` tokens. After m2 joins m1,
/// every Alive about m1 that m2 receives is vetoed → m1 is never added to m2's
/// membership view.
///
/// Note: `inject_alive` / `alive_node` bypass the policy map (they are test
/// setup back-doors that auto-accept). Only Alives arriving via the network
/// during `step()` are subject to the per-host policy.
///
/// Ported from `memberlist-core/src/base/tests.rs:457 memberlist_join_cancel_passive`.
#[test]
fn join_cancel_passive_when_alive_rejected() {
  let mut c = Cluster::new();
  let a1 = c.add_node(SmolStr::new("m1"), addr(34301));
  let a2 = c.add_node(SmolStr::new("m2"), addr(34302));

  // m2 will reject every PendingAlive it receives during step().
  c.reject_alives(a2);

  // m2 initiates a join push/pull with m1. The push/pull causes m1 to send
  // an Alive about itself to m2, but m2 rejects it.
  c.join(a2, a1);
  for _ in 0..200 {
    c.step();
  }

  // m2 rejected all alives → m1 must not appear in m2's view.
  assert_eq!(
    c.get_node_state(a2, &SmolStr::new("m1")),
    None,
    "m2 rejected all PendingAlive tokens — m1 must not be in m2's view"
  );
}
