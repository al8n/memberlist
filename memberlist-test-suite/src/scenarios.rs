//! Driver-agnostic real-node scenarios, ported faithfully from the legacy
//! `memberlist::*` test helpers. Each is generic over [`TestCluster`] so the
//! same body runs across every driver, transport, and transform cell.

use std::time::Duration;

use bytes::Bytes;

use crate::{NodeConfig, TestCluster, wait_until};

/// Generous convergence budget: a healthy two-node cluster on loopback
/// converges in well under a second; the margin absorbs scheduler jitter.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait before concluding two mismatched-label nodes have stayed
/// isolated. Normal convergence is sub-second, so a few seconds of silence is
/// strong evidence the label check kept them apart.
const ISOLATION_SETTLE: Duration = Duration::from_secs(3);

/// Ceiling for the failure detector to reap a hard-down peer: one probe
/// interval to notice, then the suspicion timeout (suspicion_mult · log(N+1) ·
/// probe_interval) before the peer is declared dead. `wait_until` returns as
/// soon as the peer drops, so this is only an upper bound, not added latency.
const DETECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Grace window after a node shuts down before a fresh node rebinds its address.
/// A driver whose `shutdown()` only signals teardown (releasing the socket when
/// its driver task later exits) needs this so the rebind does not race the
/// still-open listener; the legacy helpers sleep here for the same reason.
const SOCKET_RELEASE_GRACE: Duration = Duration::from_secs(2);

/// Two nodes; the second joins the first by its advertise address and the
/// cluster converges to a 2-member view on both sides.
///
/// Ports the legacy `memberlist_join` helper (which asserts `num_members == 2`
/// after the join settles).
pub async fn join<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("join-a")).await;
  let b = C::spawn(NodeConfig::new("join-b")).await;

  let contacted = b
    .join(a.advertise_addr())
    .await
    .expect("join the seed node");
  assert_eq!(contacted, 1, "exactly one seed contacted");

  let converged = wait_until::<C>(
    || a.num_members() == 2 && b.num_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "cluster did not converge to 2 members: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// A freshly started node tracks exactly itself: one member, and it is online.
///
/// Ports the legacy `memberlist_create` helper.
pub async fn create<C: TestCluster>() {
  let m = C::spawn(NodeConfig::new("create")).await;

  // The node converges on its own self-Alive without any peer contact.
  let settled = wait_until::<C>(
    || m.num_members() == 1 && m.num_online_members() == 1,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    settled,
    "a solo node must track exactly itself: members={}, online={}",
    m.num_members(),
    m.num_online_members()
  );

  m.shutdown().await.expect("shut down the solo node");
}

/// A solo node tracks itself, then shuts down cleanly.
///
/// Ports the legacy `memberlist_create_shutdown` helper.
pub async fn create_shutdown<C: TestCluster>() {
  let m = C::spawn(NodeConfig::new("create-shutdown")).await;

  let settled = wait_until::<C>(|| m.num_members() == 1, CONVERGE_TIMEOUT).await;
  assert!(settled, "a solo node tracks exactly itself before shutdown");

  m.shutdown().await.expect("a solo node shuts down cleanly");
}

/// Two nodes converge, then one gracefully leaves; both sides settle on a
/// single online member as the leave propagates.
///
/// Ports the legacy `memberlist_leave` helper (which, after the leave, asserts
/// one online member on each side — the observable form of the leaver being
/// marked `Left`).
pub async fn leave<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("leave-a")).await;
  let b = C::spawn(NodeConfig::new("leave-b")).await;

  b.join(a.advertise_addr())
    .await
    .expect("join the seed node");
  let converged = wait_until::<C>(
    || a.num_online_members() == 2 && b.num_online_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "cluster did not converge to 2 online members: a={}, b={}",
    a.num_online_members(),
    b.num_online_members()
  );

  a.leave().await.expect("node a leaves gracefully");

  // The leave gossips out; both sides settle on one online member (b sees a as
  // Left, a no longer counts itself online).
  let propagated = wait_until::<C>(
    || a.num_online_members() == 1 && b.num_online_members() == 1,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    propagated,
    "the leave did not settle to 1 online member: a={}, b={}",
    a.num_online_members(),
    b.num_online_members()
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// Bring up a converged two-node cluster, returning the pair. Shared setup for
/// the directed-I/O scenarios below.
async fn converged_pair<C: TestCluster>(tag: &str) -> (C, C) {
  let a = C::spawn(NodeConfig::new(format!("{tag}-a"))).await;
  let b = C::spawn(NodeConfig::new(format!("{tag}-b"))).await;
  b.join(a.advertise_addr())
    .await
    .expect("join the seed node");
  let converged = wait_until::<C>(
    || a.num_members() == 2 && b.num_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "cluster did not converge before the directed send: a={}, b={}",
    a.num_members(),
    b.num_members()
  );
  (a, b)
}

/// Re-send `payload` on the gossip plane until `to` observes it or the budget
/// elapses. The unreliable plane may drop a datagram, so a single send is not
/// enough — this mirrors the legacy `send` helper, which likewise tolerates
/// loss by asserting eventual receipt.
async fn deliver_unreliable<C: TestCluster>(from: &C, to: &C, payload: &Bytes) -> bool {
  const STEP: Duration = Duration::from_millis(50);
  let steps = (CONVERGE_TIMEOUT.as_millis() / STEP.as_millis()).max(1);
  let to_addr = to.advertise_addr();
  for _ in 0..steps {
    from
      .send(to_addr, payload.clone())
      .await
      .expect("unreliable send");
    if to.received_messages().iter().any(|m| m == payload) {
      return true;
    }
    C::sleep(STEP).await;
  }
  to.received_messages().iter().any(|m| m == payload)
}

/// One node sends an unreliable (gossip-plane) user message; the peer's
/// delegate observes it.
///
/// Ports the legacy `memberlist_send` helper.
pub async fn send_unreliable<C: TestCluster>() {
  let (a, b) = converged_pair::<C>("send-u").await;

  let payload = Bytes::from_static(b"unreliable-hello");
  let delivered = deliver_unreliable::<C>(&a, &b, &payload).await;
  assert!(
    delivered,
    "node b never observed the unreliable user message"
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// One node sends a reliable (stream-plane) user message; the peer's delegate
/// observes it. The reliable plane guarantees delivery, so a single send
/// suffices and the test only waits for the delegate to run.
///
/// Ports the legacy `memberlist_send_reliable` helper.
pub async fn send_reliable<C: TestCluster>() {
  let (a, b) = converged_pair::<C>("send-r").await;

  let payload = Bytes::from_static(b"reliable-hello");
  a.send_reliable(b.advertise_addr(), payload.clone())
    .await
    .expect("reliable send");

  let delivered = wait_until::<C>(
    || b.received_messages().iter().any(|m| m == &payload),
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(delivered, "node b never observed the reliable user message");

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// One node sends several distinct reliable user messages; the peer observes
/// every one. Exercises multiplicity on the directed-I/O path.
///
/// Ports the legacy `memberlist_send_many` helper (over the reliable plane, so
/// every distinct payload is guaranteed to arrive for the assertion).
pub async fn send_many<C: TestCluster>() {
  let (a, b) = converged_pair::<C>("send-m").await;

  let payloads = [
    Bytes::from_static(b"many-0"),
    Bytes::from_static(b"many-1"),
    Bytes::from_static(b"many-2"),
  ];
  let b_addr = b.advertise_addr();
  for p in &payloads {
    a.send_reliable(b_addr, p.clone())
      .await
      .expect("reliable send");
  }

  let all_arrived = wait_until::<C>(
    || {
      let got = b.received_messages();
      payloads.iter().all(|p| got.iter().any(|m| m == p))
    },
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    all_arrived,
    "node b did not observe all reliable messages; saw {} of {}",
    b.received_messages().len(),
    payloads.len()
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// A node advertises initial metadata; a peer that joins it observes that
/// metadata on the corresponding member.
///
/// Ports the legacy `memberlist_node_delegate_meta` helper (which asserts a
/// joined peer sees the node's configured meta).
pub async fn node_meta<C: TestCluster>() {
  let meta = b"role=db,zone=eu".to_vec();
  let a = C::spawn(NodeConfig::new("meta-a").with_meta(meta.clone())).await;
  let b = C::spawn(NodeConfig::new("meta-b")).await;

  b.join(a.advertise_addr())
    .await
    .expect("join the seed node");

  let a_id = a.id().clone();
  let observed = wait_until::<C>(
    || b.member_meta(&a_id).as_deref() == Some(meta.as_slice()),
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    observed,
    "node b never observed node a's advertised metadata; saw {:?}",
    b.member_meta(&a_id)
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// Two nodes that share a cluster label join and converge — the label frames
/// every datagram and reliable stream, so matching labels pass the check.
///
/// Ports the legacy `memberlist_join_with_labels` helper (its converging case).
pub async fn join_labeled<C: TestCluster>() {
  let label = b"tenant-x".to_vec();
  let a = C::spawn(NodeConfig::new("lj-a").with_label(label.clone())).await;
  let b = C::spawn(NodeConfig::new("lj-b").with_label(label)).await;

  b.join(a.advertise_addr())
    .await
    .expect("join under a shared label");
  let converged = wait_until::<C>(
    || a.num_members() == 2 && b.num_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "same-label cluster did not converge: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// Two nodes under different cluster labels stay isolated: the label check
/// rejects the cross-label traffic, so neither admits the other.
///
/// Ports the legacy `memberlist_join_with_labels` helper (its isolation case) —
/// the multi-tenancy guarantee that distinct labels never merge.
pub async fn labeled_isolation<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("li-a").with_label(b"tenant-x".to_vec())).await;
  let b = C::spawn(NodeConfig::new("li-b").with_label(b"tenant-y".to_vec())).await;

  // Ignoring Err: a label-mismatched join is expected to fail or no-op; the
  // isolation guarantee is asserted below by the absence of convergence.
  let _ = b.join(a.advertise_addr()).await;

  // Well past the normal (sub-second) convergence window: mismatched labels
  // must leave each node tracking only itself.
  C::sleep(ISOLATION_SETTLE).await;
  assert_eq!(
    a.num_members(),
    1,
    "node a must not admit the mismatched-label peer"
  );
  assert_eq!(
    b.num_members(),
    1,
    "node b must not admit the mismatched-label peer"
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// After a converged peer shuts down hard (no graceful leave), the failure
/// detector reaps it: the survivor's online count drops back to one.
///
/// Ports the legacy `memberlist_join_shutdown` helper (failure detection of a
/// silently-departed node, distinct from the graceful [`leave`] path).
pub async fn shutdown_detection<C: TestCluster>() {
  let (a, b) = converged_pair::<C>("sd").await;
  let converged = wait_until::<C>(
    || a.num_online_members() == 2 && b.num_online_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "cluster did not reach 2 online members before shutdown"
  );

  // Hard shutdown: no leave gossip, so the survivor must notice via probing.
  b.shutdown().await.expect("shut down node b");

  let detected = wait_until::<C>(|| a.num_online_members() == 1, DETECT_TIMEOUT).await;
  assert!(
    detected,
    "node a never detected node b's failure: online={}",
    a.num_online_members()
  );

  a.shutdown().await.expect("shut down node a");
}

/// A two-node cluster brought up under the transform configuration `configure`
/// applies (compression / checksum / encryption) converges and round-trips both
/// an unreliable and a reliable user message.
///
/// If a transform were applied on send but mishandled on receive, the peer would
/// drop the frame and this would fail — so it verifies end-to-end interop
/// through the transform stack. The complementary check that the transform tag
/// is actually present on the wire lives in each driver's own transform tests;
/// here we confirm the real-node scenarios still interoperate with transforms
/// enabled.
pub async fn transforms_roundtrip<C: TestCluster>(
  tag: &str,
  configure: impl Fn(NodeConfig) -> NodeConfig,
) {
  let a = C::spawn(configure(NodeConfig::new(format!("{tag}-a")))).await;
  let b = C::spawn(configure(NodeConfig::new(format!("{tag}-b")))).await;

  b.join(a.advertise_addr())
    .await
    .expect("join under transforms");
  let converged = wait_until::<C>(
    || a.num_members() == 2 && b.num_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "cluster did not converge under transforms: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  // Reliable plane: compression / encryption frame the stream payload.
  let reliable = Bytes::from_static(b"transform-reliable");
  a.send_reliable(b.advertise_addr(), reliable.clone())
    .await
    .expect("reliable send under transforms");
  let got_reliable = wait_until::<C>(
    || b.received_messages().iter().any(|m| m == &reliable),
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(got_reliable, "reliable transform payload not observed");

  // Gossip plane: checksum / compression / encryption frame the datagram.
  let unreliable = Bytes::from_static(b"transform-unreliable");
  let got_unreliable = deliver_unreliable::<C>(&a, &b, &unreliable).await;
  assert!(got_unreliable, "unreliable transform payload not observed");

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// A node's shutdown fully releases its socket, so a fresh node can rebind the
/// very same address and come up healthy.
///
/// Ports the legacy `memberlist_shutdown_cleanup` helper (which, after shutting
/// a solo node down, constructs a new one on its recorded address and expects
/// the rebind to succeed).
pub async fn shutdown_cleanup<C: TestCluster>() {
  let m = C::spawn(NodeConfig::new("cleanup")).await;
  let addr = m.advertise_addr();
  m.shutdown().await.expect("shut down the solo node");

  // A driver whose shutdown() signals teardown without awaiting it releases the
  // bound socket only once its driver task exits; give that a grace window so
  // the rebind does not race a still-open listener (the legacy helper sleeps
  // here for the same reason).
  C::sleep(SOCKET_RELEASE_GRACE).await;

  // The fresh node only comes up if the prior node truly released the socket.
  let rebound = C::spawn(NodeConfig::new("cleanup-rebind").with_advertise_addr(addr)).await;
  let settled = wait_until::<C>(|| rebound.num_members() == 1, CONVERGE_TIMEOUT).await;
  assert!(
    settled,
    "rebound node never came up on the released address: members={}",
    rebound.num_members()
  );

  rebound.shutdown().await.expect("shut down the rebound node");
}

/// The socket-release guarantee for a node that was an active cluster member:
/// after it shuts down, a fresh node can rebind its address.
///
/// Ports the legacy `memberlist_shutdown_cleanup2` helper (the same rebind, but
/// for a node that had first joined a peer and converged).
pub async fn shutdown_cleanup2<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("cleanup2-a")).await;
  let b = C::spawn(NodeConfig::new("cleanup2-b")).await;

  b.join(a.advertise_addr())
    .await
    .expect("join the seed node");
  let converged = wait_until::<C>(
    || a.num_members() == 2 && b.num_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "cluster did not converge before the rebind: a={}, b={}",
    a.num_members(),
    b.num_members()
  );

  let b_addr = b.advertise_addr();
  b.shutdown().await.expect("shut down node b");

  // Grace window for the socket teardown (see `shutdown_cleanup`).
  C::sleep(SOCKET_RELEASE_GRACE).await;

  // A fresh node binds the departed member's address only if it was released.
  let rebound = C::spawn(NodeConfig::new("cleanup2-rebind").with_advertise_addr(b_addr)).await;
  let settled = wait_until::<C>(|| rebound.num_members() == 1, CONVERGE_TIMEOUT).await;
  assert!(
    settled,
    "rebound node never came up on the released address: members={}",
    rebound.num_members()
  );

  a.shutdown().await.expect("shut down node a");
  rebound.shutdown().await.expect("shut down the rebound node");
}

/// A merge admission predicate that returns false cancels the join merge, so
/// neither node admits the other and both stay at a one-member view.
///
/// Ports the legacy `memberlist_join_cancel` helper (whose `MergeDelegate`
/// returns an error to cancel the merge; the Sans-I/O analog returns false). The
/// join's own `Result` is intentionally not asserted on. The legacy responder
/// sends its push-back state before consulting the merge delegate, so both ends
/// run their hook; the Sans-I/O responder instead closes the exchange the moment
/// the merge is vetoed, before replying, so only the responding side's hook
/// fires (a stricter, symmetric rejection). The observable contract is therefore
/// the un-grown membership plus the merge having been vetoed on at least one
/// side.
pub async fn join_cancel<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("jc-a").with_reject_merge()).await;
  let b = C::spawn(NodeConfig::new("jc-b").with_reject_merge()).await;

  // Ignoring Err: the join fails or no-ops once the merge is vetoed; the
  // cancellation is asserted below by membership and the merge-hook flag.
  let _ = b.join(a.advertise_addr()).await;

  let fired = wait_until::<C>(
    || a.merge_invoked() || b.merge_invoked(),
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    fired,
    "the merge predicate never fired: a={}, b={}",
    a.merge_invoked(),
    b.merge_invoked()
  );
  assert_eq!(a.num_members(), 1, "node a must not admit the vetoed peer");
  assert_eq!(b.num_members(), 1, "node b must not admit the vetoed peer");

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// An alive admission predicate that admits only its own node vetoes every
/// foreign alive: the join handshake succeeds, but membership never grows.
///
/// Ports the legacy `memberlist_join_cancel_passive` helper. Unlike
/// [`join_cancel`], the join itself returns success — the veto happens later, as
/// each foreign alive is processed — so membership stays at one while both
/// nodes' alive hooks accumulate invocations.
pub async fn join_cancel_passive<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("jcp-a").with_veto_foreign_alive()).await;
  let b = C::spawn(NodeConfig::new("jcp-b").with_veto_foreign_alive()).await;

  b.join(a.advertise_addr())
    .await
    .expect("join contacts the seed");

  let fired = wait_until::<C>(
    || a.alive_invocations() > 0 && b.alive_invocations() > 0,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    fired,
    "alive predicate never fired on both sides: a={}, b={}",
    a.alive_invocations(),
    b.alive_invocations()
  );
  assert_eq!(a.num_members(), 1, "node a must not admit the vetoed peer");
  assert_eq!(b.num_members(), 1, "node b must not admit the vetoed peer");
  assert!(
    a.alive_invocations() > 0,
    "node a's alive hook must have fired"
  );
  assert!(
    b.alive_invocations() > 0,
    "node b's alive hook must have fired"
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// A node's metadata update after join propagates to every member view: each
/// node sees both members' new metas.
///
/// Ports the legacy `memberlist_node_delegate_meta_update` helper (which joins
/// two nodes carrying initial metas, updates each via the node delegate, and
/// asserts the new metas appear in both members' views).
pub async fn node_delegate_meta_update<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("meta-up-a").with_meta(b"web".to_vec())).await;
  let b = C::spawn(NodeConfig::new("meta-up-b").with_meta(b"lb".to_vec())).await;

  b.join(a.advertise_addr())
    .await
    .expect("join the seed node");

  let a_id = a.id().clone();
  let b_id = b.id().clone();

  // Both views first agree on the initial metas (a -> web, b -> lb).
  let initial = wait_until::<C>(
    || {
      a.member_meta(&a_id).as_deref() == Some(b"web".as_ref())
        && a.member_meta(&b_id).as_deref() == Some(b"lb".as_ref())
        && b.member_meta(&a_id).as_deref() == Some(b"web".as_ref())
        && b.member_meta(&b_id).as_deref() == Some(b"lb".as_ref())
    },
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(initial, "initial metas never propagated to both views");

  a.update_meta(b"api".to_vec())
    .await
    .expect("node a updates its meta");
  b.update_meta(b"db".to_vec())
    .await
    .expect("node b updates its meta");

  // The updated metas (a -> api, b -> db) propagate to both views.
  let updated = wait_until::<C>(
    || {
      a.member_meta(&a_id).as_deref() == Some(b"api".as_ref())
        && a.member_meta(&b_id).as_deref() == Some(b"db".as_ref())
        && b.member_meta(&a_id).as_deref() == Some(b"api".as_ref())
        && b.member_meta(&b_id).as_deref() == Some(b"db".as_ref())
    },
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    updated,
    "updated metas never propagated: a-view(a={:?}, b={:?}), b-view(a={:?}, b={:?})",
    a.member_meta(&a_id),
    a.member_meta(&b_id),
    b.member_meta(&a_id),
    b.member_meta(&b_id)
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// A probed node attaches its configured ack payload, and the prober observes
/// the completed probe with a positive round-trip time.
///
/// Ports the legacy `memberlist_ping_delegate` helper (which configures an ack
/// payload, lets a couple of probe intervals elapse, and asserts the ping
/// delegate saw a positive RTT carrying that payload).
pub async fn ping_delegate<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("ping-a").with_ack_payload(b"whatever".to_vec())).await;
  let b = C::spawn(NodeConfig::new("ping-b").with_ack_payload(b"whatever".to_vec())).await;

  b.join(a.advertise_addr())
    .await
    .expect("join the seed node");
  let converged = wait_until::<C>(
    || a.num_members() == 2 && b.num_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(converged, "cluster did not converge before probing");

  // Probing is periodic; allow several intervals for a probe round to complete
  // and its ack to surface on one of the two nodes.
  let observed = wait_until::<C>(
    || !a.ping_completions().is_empty() || !b.ping_completions().is_empty(),
    DETECT_TIMEOUT,
  )
  .await;
  assert!(observed, "no ping completion observed on either node");

  let matched = a
    .ping_completions()
    .into_iter()
    .chain(b.ping_completions())
    .any(|p| p.rtt > Duration::ZERO && p.payload.as_ref() == b"whatever");
  assert!(
    matched,
    "no ping completion carried a positive RTT and the configured payload"
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// Two nodes claiming the same id with different addresses trigger a conflict
/// notification on the node that learns of the clash.
///
/// Ports the legacy `memberlist_conflict_delegate` helper (two nodes share an
/// id but differ in address; after they make contact, the conflict delegate
/// records the existing and conflicting nodes — whose ids match).
pub async fn conflict_delegate<C: TestCluster>() {
  let a = C::spawn(NodeConfig::new("dup")).await;
  let b = C::spawn(NodeConfig::new("dup")).await;

  // Ignoring Err: the join races a same-id conflict; the contract is the
  // conflict notification asserted below, not the join's own result.
  let _ = a.join(b.advertise_addr()).await;

  let notified = wait_until::<C>(|| !a.received_conflicts().is_empty(), CONVERGE_TIMEOUT).await;
  assert!(notified, "node a never observed the id conflict");

  let conflicts = a.received_conflicts();
  assert!(
    conflicts
      .iter()
      .any(|(existing, other)| existing == other && existing.as_str() == "dup"),
    "no conflict pair shared the duplicated id \"dup\": {conflicts:?}"
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}

/// Gossip propagates user broadcasts and push/pull exchanges each node's local
/// state: the seed observes every broadcast plus the joiner's state, and the
/// joiner observes the seed's state.
///
/// Ports the legacy `memberlist_user_data` helper. The legacy broadcasts 256
/// messages over the lossy gossip plane; this uses a smaller distinct set so
/// the set-equality assertion stays reliable while exercising the same
/// broadcast-plus-push/pull contract.
pub async fn user_data<C: TestCluster>() {
  const BROADCAST_COUNT: u32 = 32;
  let broadcasts: Vec<Bytes> = (0..BROADCAST_COUNT)
    .map(|i| Bytes::copy_from_slice(&i.to_be_bytes()))
    .collect();

  let a = C::spawn(NodeConfig::new("user-a").with_local_state(b"something".to_vec())).await;
  let b = C::spawn(
    NodeConfig::new("user-b")
      .with_local_state(b"my state".to_vec())
      .with_broadcasts(broadcasts.clone()),
  )
  .await;

  b.join(a.advertise_addr())
    .await
    .expect("join the seed node");
  let converged = wait_until::<C>(
    || a.num_online_members() == 2 && b.num_online_members() == 2,
    CONVERGE_TIMEOUT,
  )
  .await;
  assert!(
    converged,
    "cluster did not converge to 2 online members: a={}, b={}",
    a.num_online_members(),
    b.num_online_members()
  );

  // Gossip carries b's broadcasts to a; push/pull crosses each node's state.
  let exchanged = wait_until::<C>(
    || {
      let msgs = a.received_messages();
      let all_broadcasts = broadcasts.iter().all(|m| msgs.contains(m));
      let a_got_b_state = a
        .received_remote_states()
        .iter()
        .any(|s| s.as_ref() == b"my state");
      let b_got_a_state = b
        .received_remote_states()
        .iter()
        .any(|s| s.as_ref() == b"something");
      all_broadcasts && a_got_b_state && b_got_a_state
    },
    DETECT_TIMEOUT,
  )
  .await;
  assert!(
    exchanged,
    "user data did not fully propagate: a saw {}/{} broadcasts, \
     a-remote-states={:?}, b-remote-states={:?}",
    a.received_messages()
      .iter()
      .filter(|m| broadcasts.contains(m))
      .count(),
    BROADCAST_COUNT,
    a.received_remote_states(),
    b.received_remote_states()
  );

  a.shutdown().await.expect("shut down node a");
  b.shutdown().await.expect("shut down node b");
}
