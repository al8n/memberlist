//! Application-data-plane tests over TCP.
//!
//! These exercise the push commands (`queue_user_broadcast` /
//! `set_local_state` / `update_node_metadata`) and the receive-side
//! observation hooks (`notify_user_msg` / `merge_remote_state` /
//! `notify_update`) through two real driver tasks:
//!
//! - `queue_user_broadcast_disseminates_to_peer`: a user-broadcast queued on
//!   one node rides the periodic gossip scheduler and surfaces on the peer's
//!   `notify_user_msg`.
//! - `set_local_state_reaches_peer_merge_remote_state`: a local-state snapshot
//!   set before join rides the join push/pull (an explicit exchange,
//!   independent of the periodic scheduler) and fires `merge_remote_state` on
//!   the peer that receives the exchange.
//! - `rejected_merge_does_not_fire_merge_remote_state`: a `MergeDelegate` that
//!   rejects every merge must NOT leak the initiator's state to the
//!   application — `merge_remote_state` stays silent (push/pull-request arm).
//! - `rejected_merge_does_not_fire_on_reply_arm`: the symmetric guard on the
//!   push/pull-reply arm — a rejecting `MergeDelegate` on the join initiator
//!   must not leak the seed's reply snapshot to `merge_remote_state`.
//! - `post_join_metadata_update_disseminates`: a metadata update applied AFTER
//!   the cluster converges rides the periodic gossip scheduler and surfaces on
//!   the peer's `notify_update` carrying the new bytes.

#![cfg(feature = "tcp")]

use std::{
  net::SocketAddr,
  sync::{Arc, Mutex},
  time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::StreamExt;
use memberlist_compio::{
  Channel, ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved, Memberlist,
  MemberlistError, MemberlistOptions, MergeDelegate, NodeDelegate, Options, PingDelegate,
  RuntimeOptions, SocketAddrResolver, TcpTransport, TcpTransportOptions,
};
use memberlist_proto::{event::Event, typed::NodeState};
use smol_str::SmolStr;

/// Shared record of byte payloads a [`RecordingDelegate`] observed.
type Records = Arc<Mutex<Vec<Bytes>>>;

/// Shared record of `(node-id, meta-bytes)` pairs a [`RecordingDelegate`]
/// observed via `notify_update`.
type UpdateRecords = Arc<Mutex<Vec<(SmolStr, Bytes)>>>;

/// Observation delegate that records the payloads delivered to
/// `notify_user_msg` / `merge_remote_state` and the metadata updates delivered
/// to `notify_update` into shared vectors. Every other hook is a no-op
/// (mirroring `VoidDelegate`).
struct RecordingDelegate {
  user_msgs: Records,
  merges: Records,
  updates: UpdateRecords,
}

impl RecordingDelegate {
  fn new(user_msgs: Records, merges: Records, updates: UpdateRecords) -> Self {
    Self {
      user_msgs,
      merges,
      updates,
    }
  }
}

impl NodeDelegate for RecordingDelegate {
  async fn notify_user_msg(&self, msg: std::borrow::Cow<'_, [u8]>) {
    // Ignoring poisoning is not silent failure: a poisoned lock means a
    // prior test-thread panic already failed the test, so unwrap surfaces
    // that as the panic it is.
    self
      .user_msgs
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(msg.as_ref()));
  }

  async fn merge_remote_state(&self, buf: &[u8], _join: bool) {
    self
      .merges
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(buf));
  }
}

impl EventDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_update(&self, node: std::sync::Arc<NodeState<SmolStr, SocketAddr>>) {
    self.updates.lock().unwrap().push((
      node.id_ref().clone(),
      Bytes::copy_from_slice(node.meta_ref().as_bytes()),
    ));
  }
}

impl PingDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl ConflictDelegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl Delegate for RecordingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

/// A `Memberlist` carrying a [`RecordingDelegate`].
type RecordingMemberlist = memberlist_compio::Memberlist<SmolStr, SocketAddr>;

/// Machine [`MergeDelegate`] that rejects every join merge (`Send + Sync`),
/// installed via `Options::with_merge_delegate`.
struct RejectMerge;

impl MergeDelegate<SmolStr, SocketAddr> for RejectMerge {
  fn notify_merge(
    &self,
    _peers: memberlist_compio::MaybeOwned<'_, [NodeState<SmolStr, SocketAddr>]>,
  ) -> bool {
    false
  }
}

/// Build a plain `VoidDelegate`-backed seed bound to `127.0.0.1:0`.
async fn make_seed(id: &str) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  );
  Memberlist::new(
    opts,
    memberlist_compio::VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct seed")
}

/// Build a [`RecordingDelegate`]-backed node bound to `127.0.0.1:0`.
async fn make_recording_node(
  id: &str,
  user_msgs: Records,
  merges: Records,
  updates: UpdateRecords,
) -> RecordingMemberlist {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  );
  RecordingMemberlist::new(
    opts,
    RecordingDelegate::new(user_msgs, merges, updates),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct recording node")
}

/// Build a [`RecordingDelegate`]-backed node that also installs a
/// [`RejectMerge`] admission predicate.
async fn make_rejecting_recording_node(
  id: &str,
  user_msgs: Records,
  merges: Records,
  updates: UpdateRecords,
) -> RecordingMemberlist {
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_merge_delegate(RejectMerge);
  RecordingMemberlist::new(
    opts,
    RecordingDelegate::new(user_msgs, merges, updates),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct rejecting recording node")
}

async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

fn records_contain(records: &Records, needle: &[u8]) -> bool {
  records.lock().unwrap().iter().any(|b| b.as_ref() == needle)
}

/// True if `updates` recorded a `notify_update` for node `id` carrying `meta`.
fn updates_contain(updates: &UpdateRecords, id: &str, meta: &[u8]) -> bool {
  updates
    .lock()
    .unwrap()
    .iter()
    .any(|(node_id, node_meta)| node_id.as_str() == id && node_meta.as_ref() == meta)
}

/// A user-broadcast queued on the seed disseminates to the peer: the periodic
/// gossip scheduler drains the queued payload onto the wire and the peer's
/// `notify_user_msg` fires with the bytes.
///
/// This is end-to-end gossip dissemination — the command acks, then the
/// payload rides the periodic gossip path (armed at driver-loop entry) to the
/// peer. A generous window absorbs the gossip-interval stagger.
#[compio::test]
async fn queue_user_broadcast_disseminates_to_peer() {
  let seed = make_seed("ub-seed").await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let user_msgs: Records = Arc::new(Mutex::new(Vec::new()));
  let merges: Records = Arc::new(Mutex::new(Vec::new()));
  let updates: UpdateRecords = Arc::new(Mutex::new(Vec::new()));
  let peer = make_recording_node(
    "ub-peer",
    user_msgs.clone(),
    merges.clone(),
    updates.clone(),
  )
  .await;

  let contacted = peer
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join the seed");
  assert_eq!(contacted.len(), 1, "exactly one seed contacted");

  // The command must reach the endpoint and ack without error.
  seed
    .queue_user_broadcast(Bytes::from_static(b"hello"))
    .await
    .expect("queue user broadcast acks Ok");

  // Periodic gossip then carries the queued broadcast to the peer.
  let saw = wait_until(
    || records_contain(&user_msgs, b"hello"),
    Duration::from_secs(5),
  )
  .await;
  assert!(
    saw,
    "notify_user_msg did not observe the gossiped broadcast; recorded: {:?}",
    user_msgs.lock().unwrap()
  );

  seed.shutdown().await.expect("seed shutdown");
  peer.shutdown().await.expect("peer shutdown");
}

/// A local-state snapshot set on the seed BEFORE the join push/pull rides that
/// exchange; the peer that receives the inbound push/pull request admits the
/// merge and fires `merge_remote_state` with the snapshot bytes.
#[compio::test]
async fn set_local_state_reaches_peer_merge_remote_state() {
  let user_msgs: Records = Arc::new(Mutex::new(Vec::new()));
  let merges: Records = Arc::new(Mutex::new(Vec::new()));
  let updates: UpdateRecords = Arc::new(Mutex::new(Vec::new()));
  // The RECEIVER of the join push/pull request is the node observed: the
  // joiner initiates (outbound) and carries its snapshot in the request, so
  // the seed (inbound arm) is the one whose `merge_remote_state` fires.
  let seed = make_recording_node(
    "sls-seed",
    user_msgs.clone(),
    merges.clone(),
    updates.clone(),
  )
  .await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let joiner = make_seed("sls-joiner").await;

  // Set the joiner's local state BEFORE it joins so the very first push/pull
  // (the join exchange) carries it.
  joiner
    .set_local_state(Bytes::from_static(b"state-A"))
    .await
    .expect("set local state");

  let contacted = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join the seed");
  assert_eq!(contacted.len(), 1, "exactly one seed contacted");

  let saw = wait_until(
    || records_contain(&merges, b"state-A"),
    Duration::from_secs(5),
  )
  .await;
  assert!(
    saw,
    "merge_remote_state did not fire with the snapshot; recorded: {:?}",
    merges.lock().unwrap()
  );

  joiner.shutdown().await.expect("joiner shutdown");
  seed.shutdown().await.expect("seed shutdown");
}

/// Regression: a rejecting `MergeDelegate` on the receiver must NOT leak the
/// initiator's state to the application. The joiner sets a local-state
/// snapshot and joins; the seed's inbound push/pull-request arm consults the
/// `MergeDelegate`, which rejects, closing the stream BEFORE any
/// `RemoteStateReceived` is emitted. `merge_remote_state` must stay silent.
///
/// This exercises the inbound `PushPullRequestReceived` arm — the only arm
/// where rejection and a non-empty `user_data` coincide: the joiner is the
/// initiator carrying the state, and the seed is the rejecting receiver. A
/// rejected merge on that arm returns `StreamCommand::Close`, so the join
/// push/pull exchange terminates without success and `join` surfaces
/// `JoinFailed` — that failure is the rejection working, not a test bug, so
/// the join outcome is accepted either way. What the regression pins is the
/// absence of any `merge_remote_state` delivery for the rejected state.
#[compio::test]
async fn rejected_merge_does_not_fire_merge_remote_state() {
  let user_msgs: Records = Arc::new(Mutex::new(Vec::new()));
  let merges: Records = Arc::new(Mutex::new(Vec::new()));
  let updates: UpdateRecords = Arc::new(Mutex::new(Vec::new()));
  let seed = make_rejecting_recording_node(
    "rm-seed",
    user_msgs.clone(),
    merges.clone(),
    updates.clone(),
  )
  .await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let joiner = make_seed("rm-joiner").await;
  joiner
    .set_local_state(Bytes::from_static(b"secret"))
    .await
    .expect("set local state");

  // The rejecting inbound arm closes the stream, so the join exchange does
  // not succeed: `join` may report `JoinFailed`. Either outcome is
  // consistent with the rejection — the network contact happened, but the
  // merge (and thus the exchange) was refused. Ignoring Err: a failed join
  // is the expected effect of the rejection; the negative assertion below is
  // what proves no state leaked.
  let _ = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await;

  // Poll for a bounded window; the rejected merge must never surface the
  // joiner's state. A positive sighting at any point fails the regression.
  let leaked = wait_until(
    || records_contain(&merges, b"secret"),
    Duration::from_secs(2),
  )
  .await;
  assert!(
    !leaked,
    "rejected merge leaked the initiator's state to merge_remote_state: {:?}",
    merges.lock().unwrap()
  );

  joiner.shutdown().await.expect("joiner shutdown");
  seed.shutdown().await.expect("seed shutdown");
}

/// Regression (outbound push/pull-reply admission gate): when the join
/// INITIATOR runs a rejecting `MergeDelegate`, receiving the seed's reply on
/// the outbound arm must (1) terminalize the exchange as failed so `join`
/// surfaces `JoinFailed` — a rejected merge is NOT a successful join — and
/// (2) never surface the seed's reply snapshot through `merge_remote_state`.
///
/// The seed (plain `VoidDelegate` admission) merges the initiator's membership
/// from the inbound request and replies, so `seed.alive_count() == 2` is
/// positive proof the request/reply round-trip ran — the outbound arm was
/// exercised. The initiator rejects, so its join fails and its merge hook
/// stays silent.
#[compio::test]
async fn rejected_merge_does_not_fire_on_reply_arm() {
  let user_msgs: Records = Arc::new(Mutex::new(Vec::new()));
  let merges: Records = Arc::new(Mutex::new(Vec::new()));
  let updates: UpdateRecords = Arc::new(Mutex::new(Vec::new()));
  // The join INITIATOR is the observed node: it receives the seed's reply on
  // the outbound arm and rejects the merge.
  let joiner = make_rejecting_recording_node(
    "rr-joiner",
    user_msgs.clone(),
    merges.clone(),
    updates.clone(),
  )
  .await;

  // The seed carries a non-empty snapshot in its push/pull REPLY.
  let seed = make_seed("rr-seed").await;
  seed
    .set_local_state(Bytes::from_static(b"seed-secret"))
    .await
    .expect("set local state");
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  // The initiator rejects the seed's reply on the outbound arm, which
  // terminalizes the exchange as failed: with a single seed, `join` must
  // surface `JoinFailed` rather than a spurious success.
  let result = joiner
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await;
  assert!(
    matches!(result, Err((_, MemberlistError::JoinFailed(_)))),
    "a merge-rejected join must fail (the outbound exchange is terminalized), got {result:?}"
  );

  // The seed admits and merges the initiator, then replies — convergence on
  // the seed side proves the initiator received the reply (outbound arm ran).
  let round_tripped = wait_until(|| seed.alive_count() == 2, Duration::from_secs(5)).await;
  assert!(
    round_tripped,
    "seed did not learn the initiator — the reply round-trip never ran (alive={})",
    seed.alive_count()
  );

  // The rejecting initiator must never surface the seed's reply snapshot.
  let leaked = wait_until(
    || records_contain(&merges, b"seed-secret"),
    Duration::from_secs(2),
  )
  .await;
  assert!(
    !leaked,
    "rejected merge leaked the seed's reply state to merge_remote_state: {:?}",
    merges.lock().unwrap()
  );

  joiner.shutdown().await.expect("joiner shutdown");
  seed.shutdown().await.expect("seed shutdown");
}

/// A metadata update applied AFTER the cluster has converged disseminates to
/// the peer via the periodic gossip path: the seed bumps its metadata, and the
/// peer's `notify_update` fires carrying the new bytes.
///
/// This could not happen before the schedulers were armed — a post-join state
/// change only propagates through periodic gossip (the join push/pull is a
/// one-shot exchange that already completed). The update is applied only after
/// both nodes observe `alive_count() == 2`, so the propagation under test is
/// strictly the gossip path, not the join exchange.
#[compio::test]
async fn post_join_metadata_update_disseminates() {
  let seed = make_seed("mu-seed").await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  let user_msgs: Records = Arc::new(Mutex::new(Vec::new()));
  let merges: Records = Arc::new(Mutex::new(Vec::new()));
  let updates: UpdateRecords = Arc::new(Mutex::new(Vec::new()));
  let peer = make_recording_node(
    "mu-peer",
    user_msgs.clone(),
    merges.clone(),
    updates.clone(),
  )
  .await;

  let contacted = peer
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join the seed");
  assert_eq!(contacted.len(), 1, "exactly one seed contacted");

  // Wait for both nodes to converge before mutating, so the update can only
  // reach the peer through periodic gossip (not the join exchange).
  let converged = wait_until(
    || seed.alive_count() == 2 && peer.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge to 2 alive members: seed={}, peer={}",
    seed.alive_count(),
    peer.alive_count()
  );

  seed
    .update_node_metadata(b"meta-v2".to_vec())
    .await
    .expect("update node metadata acks Ok");

  let saw = wait_until(
    || updates_contain(&updates, "mu-seed", b"meta-v2"),
    Duration::from_secs(5),
  )
  .await;
  assert!(
    saw,
    "notify_update did not observe the gossiped metadata; recorded: {:?}",
    updates.lock().unwrap()
  );

  seed.shutdown().await.expect("seed shutdown");
  peer.shutdown().await.expect("peer shutdown");
}

/// `set_encryption_options` with an encryption policy whose AEAD backend is not
/// compiled into this build must be REJECTED (not stored), so the cluster never
/// silently breaks after a false `Ok`. This probe uses an AES-GCM key, so it is
/// only meaningful when the `aes-gcm` backend is ABSENT (gated below); in that
/// build AES is unsupported and the reconfiguration must surface
/// `MemberlistError::Encryption`. A disabled (no-keyring) policy is always
/// usable and must ack `Ok`.
#[cfg(not(feature = "aes-gcm"))]
#[compio::test]
async fn set_encryption_options_rejects_unsupported_algorithm() {
  use memberlist_proto::{EncryptionOptions, Keyring, SecretKey};

  let node = make_seed("enc-reject").await;

  // AES-GCM keyring: constructible without the `aes-gcm` feature (only the
  // `random_*` constructors are feature-gated), but `encrypt_gossip` /
  // reliable-stream encode would fail `UnsupportedAlgorithm` in this build.
  let unsupported =
    EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes128([0u8; 16])));
  let res = node.set_encryption_options(unsupported).await;
  assert!(
    matches!(res, Err(MemberlistError::Encryption(_))),
    "an unsupported encryption policy must be rejected with Encryption(_), got {res:?}"
  );

  // A disabled policy (no keyring) is always usable ⇒ Ok.
  let disabled = EncryptionOptions::new();
  node
    .set_encryption_options(disabled)
    .await
    .expect("a disabled encryption policy must be accepted");

  node.shutdown().await.expect("shutdown");
}

/// Encryption validation must probe EVERY keyring key, not just the primary.
/// Inbound frames are decrypted by trial-matching the variant of every key in
/// the ring, so a SUPPORTED primary paired with an UNSUPPORTED-algorithm
/// secondary (a key-rotation state) would silently drop every frame a peer
/// encrypted under that secondary. `set_encryption_options` must reject the
/// whole ring ATOMICALLY (live policy unchanged) when any key's algorithm is
/// unsupported in this build, while an all-supported ring is accepted.
///
/// Construction (deterministic only when exactly the AES-GCM backend is built
/// and ChaCha20-Poly1305 is NOT — hence the cfg gate): a pure `--features tcp`
/// build compiles in NO AEAD backend, so every real key is unsupported and the
/// primary-only probe already rejects, which cannot discriminate whether
/// secondaries are checked. Building `aes-gcm` (without
/// `chacha20-poly1305`) makes AES the SUPPORTED algorithm and
/// ChaCha20-Poly1305 the UNSUPPORTED one, so an AES primary + ChaCha secondary
/// isolates exactly the "primary ok, secondary bad" gap.
#[cfg(all(feature = "aes-gcm", not(feature = "chacha20-poly1305")))]
#[compio::test]
async fn encryption_rejected_for_unsupported_secondary_key() {
  use memberlist_proto::{EncryptionOptions, Keyring, SecretKey};

  let node = make_seed("enc-secondary").await;

  // AES primary (supported in this build) + ChaCha20-Poly1305 secondary
  // (unsupported in this build). The primary alone would pass; the ring as a
  // whole must be rejected because of the unsupported secondary.
  let ring = Keyring::with_secondaries(
    SecretKey::Aes256([7u8; 32]),
    [SecretKey::ChaCha20Poly1305([9u8; 32])],
  );
  let res = node
    .set_encryption_options(EncryptionOptions::new().with_keyring(ring))
    .await;
  assert!(
    matches!(res, Err(MemberlistError::Encryption(_))),
    "a ring with an unsupported secondary must be rejected with Encryption(_), got {res:?}"
  );

  // The live policy is unchanged: an all-supported (AES-only) ring is still
  // accepted, proving the rejected reconfig left the node in a usable state
  // rather than half-applying the bad ring.
  let all_ok = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([3u8; 32])));
  node
    .set_encryption_options(all_ok)
    .await
    .expect("an all-supported keyring must be accepted");

  node.shutdown().await.expect("shutdown");
}

/// `set_local_state` with a snapshot whose framed PushPull exceeds the
/// reliable-stream frame budget must be REJECTED (not stored), so the cluster
/// never silently fails to disseminate application state after a false `Ok`. A
/// reasonable snapshot must ack `Ok`.
#[compio::test]
async fn set_local_state_rejects_oversized_snapshot() {
  let node = make_seed("sls-reject").await;

  // 64 MiB snapshot — its framed PushPull is over the ~63 MiB budget
  // (`max_stream_frame_size` 64 MiB minus the 1 MiB membership-state reserve).
  let oversized = Bytes::from(vec![0u8; 64 * 1024 * 1024]);
  let res = node.set_local_state(oversized).await;
  assert!(
    matches!(
      res,
      Err(MemberlistError::Proto(
        memberlist_proto::Error::LocalStateExceedsFrame(..)
      ))
    ),
    "an oversized local-state snapshot must be rejected as LocalStateExceedsFrame, got {res:?}"
  );

  // A reasonable snapshot is accepted.
  node
    .set_local_state(Bytes::from_static(b"reasonable-state"))
    .await
    .expect("a small local-state snapshot must be accepted");

  node.shutdown().await.expect("shutdown");
}

/// A `notify_user_msg` that never returns, stalling the observation task on the
/// first user message it receives. Every other hook keeps the no-op default.
struct StallingDelegate;

impl NodeDelegate for StallingDelegate {
  async fn notify_user_msg(&self, _msg: std::borrow::Cow<'_, [u8]>) {
    // Block the observation task forever: once it picks up the first user
    // message it never drains another event, so the bounded observation
    // channel fills and the driver must drop (and count) the overflow.
    core::future::pending::<()>().await
  }

  async fn merge_remote_state(&self, _buf: &[u8], _join: bool) {
    // Same forever-stall for push/pull remote-state, so a flood of large
    // `RemoteStateReceived` payloads piles up against the byte backstop.
    core::future::pending::<()>().await
  }
}

impl EventDelegate for StallingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}
impl PingDelegate for StallingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}
impl ConflictDelegate for StallingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}
impl Delegate for StallingDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

type StallingMemberlist = memberlist_compio::Memberlist<SmolStr, SocketAddr>;

/// Regression for the bounded observation channel (the safe default): with the
/// observation task permanently stalled in `notify_user_msg`, a flood of
/// gossiped user messages fills the bounded channel; the driver drops the
/// newest events rather than growing without bound or stalling SWIM, and
/// `observation_dropped` advances. Proves the default caps the remote-driven
/// OOM class rather than only protecting opt-in callers.
#[compio::test]
async fn bounded_observation_channel_drops_and_counts_under_stalled_delegate() {
  let seed = make_seed("bc-seed").await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  // Small bounded observation channel + a delegate that stalls forever on the
  // first user message.
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("bc-node"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_runtime(RuntimeOptions::new().with_observation_channel(Channel::Bounded(4)));
  let node = StallingMemberlist::new(
    opts,
    StallingDelegate,
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct stalling node");

  let contacted = node
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join the seed");
  assert_eq!(contacted.len(), 1, "exactly one seed contacted");

  // Flood the node with user messages. The first wedges the observation task
  // forever; the rest pile into the bounded channel until it overflows.
  for i in 0..64u32 {
    seed
      .queue_user_broadcast(Bytes::from(format!("flood-{i}").into_bytes()))
      .await
      .expect("queue user broadcast acks Ok");
  }

  // The bounded channel drops (and counts) the overflow rather than growing
  // without limit.
  let dropped = wait_until(|| node.observation_dropped() > 0, Duration::from_secs(10)).await;
  assert!(
    dropped,
    "bounded observation channel never dropped under a stalled delegate + flood \
     (observation_dropped={})",
    node.observation_dropped()
  );
  // The drops are counted at the observation channel, NOT the EventStream: the
  // two counters are distinct, so a consumer can tell a (possibly unrecoverable)
  // delegate/app-data drop from a recoverable EventStream gap. A stalled
  // DELEGATE advances only `observation_dropped`.
  assert_eq!(
    node.events_dropped(),
    0,
    "a stalled delegate must not advance the EventStream drop counter (events_dropped={})",
    node.events_dropped()
  );

  seed.shutdown().await.expect("seed shutdown");
  node.shutdown().await.expect("node shutdown");
}

/// A FAST (no-op) delegate must NOT lose events from a single large-but-valid
/// drain burst, even on a small bounded observation channel. A node joins a
/// seed that already knows several members, so the join push/pull reply
/// surfaces a burst of `NodeJoined` events in one synchronous drain — more
/// than the channel capacity. Without the drain yielding to the observation
/// task, the bounded channel would overflow mid-burst and `observation_dropped`
/// would advance even for a no-op delegate; with the yield, the fast delegate
/// drains each chunk and nothing is dropped.
#[compio::test]
async fn bounded_observation_channel_does_not_drop_valid_burst_for_fast_delegate() {
  let seed = make_seed("burst-seed").await;
  let seed_addr = *seed.local_node().addr_ref();
  assert_ne!(seed_addr.port(), 0, "seed must have a concrete port");

  // Pre-populate the seed with three helpers so its membership (and thus the
  // push/pull reply to a later joiner) carries 4 members.
  let mut helpers = Vec::new();
  for i in 0..3u32 {
    let h = make_seed(&format!("burst-helper-{i}")).await;
    h.join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
      .await
      .expect("helper joins seed");
    helpers.push(h);
  }
  let converged = wait_until(|| seed.alive_count() == 4, Duration::from_secs(5)).await;
  assert!(
    converged,
    "seed did not learn its 3 helpers (alive={})",
    seed.alive_count()
  );

  // A no-op (fast) node with a SMALL bounded observation channel joins. Its
  // join push/pull reply carries the seed's 4 members, so 4 `NodeJoined`
  // events surface in a single drain — more than the channel capacity of 2.
  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("burst-node"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_runtime(RuntimeOptions::new().with_observation_channel(Channel::Bounded(2)));
  let node = Memberlist::new(
    opts,
    memberlist_compio::VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct burst node");

  node
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("burst node joins seed");
  let joined = wait_until(|| node.alive_count() == 5, Duration::from_secs(5)).await;
  assert!(
    joined,
    "burst node did not converge to 5 members (alive={})",
    node.alive_count()
  );

  // The fast delegate absorbed the whole burst via the drain's yield: nothing
  // was dropped at the observation channel, and the membership events fanned out
  // to the EventStream without overflowing it either.
  assert_eq!(
    node.observation_dropped(),
    0,
    "a no-op delegate must not lose events from a valid drain burst, but {} were dropped at the \
     observation channel",
    node.observation_dropped()
  );
  assert_eq!(
    node.events_dropped(),
    0,
    "the membership burst must not overflow the EventStream, but {} were dropped",
    node.events_dropped()
  );

  node.shutdown().await.expect("burst node shutdown");
  for h in helpers {
    h.shutdown().await.expect("helper shutdown");
  }
  seed.shutdown().await.expect("seed shutdown");
}

/// The observation byte backstop bounds memory by BYTES, not event count. A
/// node caps reliable frames at 256 KiB (so the payload byte budget is
/// `4 * 256 KiB = 1 MiB`) and stalls forever in `merge_remote_state`. Joining
/// eight seeds that each carry a ~224 KiB local-state snapshot surfaces eight
/// large `RemoteStateReceived` events; ~4 fit the byte budget and the rest are
/// dropped — with only ~8 events queued (far below the 1024-event count cap),
/// so the drop is byte-driven, which a count cap alone could never trigger.
#[compio::test]
async fn bounded_observation_channel_caps_payload_bytes_not_count() {
  // Each seed carries a ~224 KiB local-state snapshot (rides the join push/pull
  // as a large `RemoteStateReceived`; framed it stays under the node's 256 KiB
  // reliable-frame cap).
  let payload = Bytes::from(vec![0xd7_u8; 224 * 1024]);
  let mut seeds = Vec::new();
  let mut seed_addrs = Vec::new();
  for i in 0..8u32 {
    let s = make_seed(&format!("pb-seed-{i}")).await;
    s.set_local_state(payload.clone())
      .await
      .expect("seed sets local state");
    seed_addrs.push(MaybeResolved::Resolved(*s.local_node().addr_ref()));
    seeds.push(s);
  }

  let opts = Options::<TcpTransport<SmolStr, SocketAddr>>::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("pb-node"))
      .with_advertise_addr(MaybeResolved::Resolved("127.0.0.1:0".parse().unwrap())),
  )
  .with_memberlist(MemberlistOptions::new().with_max_stream_frame_size(256 * 1024));
  let node = StallingMemberlist::new(
    opts,
    StallingDelegate,
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct stalling node");

  // Join all eight seeds. The join push/pulls surface the large RemoteState
  // events; a merge-stalled delegate does not block join completion — the
  // exchange completes on the driver, ahead of the off-loop merge hook.
  node
    .join(&SocketAddrResolver, &seed_addrs)
    .await
    .expect("join the seeds");

  let dropped = wait_until(|| node.observation_dropped() > 0, Duration::from_secs(10)).await;
  assert!(
    dropped,
    "byte backstop did not drop large reliable payloads under a stalled delegate \
     (observation_dropped={}); only ~8 events queued, so this is byte-driven not count-driven",
    node.observation_dropped()
  );

  node.shutdown().await.expect("node shutdown");
  for s in seeds {
    s.shutdown().await.expect("seed shutdown");
  }
}

/// App-data events reach the delegate but are NOT fanned out to the
/// `EventStream`. `Event::UserPacket` / `Event::RemoteStateReceived` carry
/// arbitrary application payloads (up to the reliable-frame cap); routing them
/// through the bounded best-effort `events()` channel would let a slow
/// subscriber pile up large payloads, and a dropped one is unrecoverable from
/// the membership snapshot anyway. So the driver delivers them only to the
/// delegate hooks; the `EventStream` carries membership / control events only.
///
/// The node subscribes to `events()`, then joins a seed carrying local state
/// (fires `merge_remote_state` -> a `RemoteStateReceived` event) and receives a
/// gossiped user broadcast (fires `notify_user_msg` -> a `UserPacket` event).
/// The delegate records BOTH payloads; the stream yields the membership join
/// but NEITHER app-data event.
#[compio::test]
async fn app_data_reaches_delegate_but_not_eventstream() {
  let seed = make_seed("ad-seed").await;
  seed
    .set_local_state(Bytes::from_static(b"remote-state-x"))
    .await
    .expect("seed sets local state");
  let seed_addr = *seed.local_node().addr_ref();

  let user_msgs: Records = Arc::new(Mutex::new(Vec::new()));
  let merges: Records = Arc::new(Mutex::new(Vec::new()));
  let updates: UpdateRecords = Arc::new(Mutex::new(Vec::new()));
  let node = make_recording_node(
    "ad-node",
    user_msgs.clone(),
    merges.clone(),
    updates.clone(),
  )
  .await;

  // Subscribe BEFORE the join so the membership event is observable. A single
  // active subscriber receives every event — the receiver the `Memberlist`
  // holds internally is never polled, so flume's round-robin never steals.
  let mut stream = node.events();

  node
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("join the seed");

  seed
    .queue_user_broadcast(Bytes::from_static(b"user-packet-y"))
    .await
    .expect("queue user broadcast acks Ok");

  // Both app-data hooks must fire on the delegate: the join push/pull merge and
  // the gossiped broadcast. This is the proof the events were produced and
  // delivered — so their absence from the stream below is suppression, not a
  // race where they simply never happened.
  let delivered = wait_until(
    || records_contain(&merges, b"remote-state-x") && records_contain(&user_msgs, b"user-packet-y"),
    Duration::from_secs(6),
  )
  .await;
  assert!(
    delivered,
    "delegate did not record both app-data payloads (merges={:?}, user_msgs={:?})",
    merges.lock().unwrap(),
    user_msgs.lock().unwrap()
  );

  // Drain the EventStream: collect every buffered event (a 200ms idle gap marks
  // the queue drained). It must carry the membership join and NEITHER app-data
  // event.
  let mut saw_membership = false;
  let mut app_data: Vec<&str> = Vec::new();
  while let Ok(Some(ev)) = compio::time::timeout(Duration::from_millis(200), stream.next()).await {
    match ev {
      Event::NodeJoined(_) | Event::NodeUpdated(_) => saw_membership = true,
      Event::UserPacket(_) => app_data.push("UserPacket"),
      Event::RemoteStateReceived(_) => app_data.push("RemoteStateReceived"),
      _ => {}
    }
  }

  assert!(
    app_data.is_empty(),
    "app-data events must NOT reach the EventStream, but observed: {app_data:?}"
  );
  assert!(
    saw_membership,
    "EventStream should still carry membership events (NodeJoined); none observed"
  );

  seed.shutdown().await.expect("seed shutdown");
  node.shutdown().await.expect("node shutdown");
}
