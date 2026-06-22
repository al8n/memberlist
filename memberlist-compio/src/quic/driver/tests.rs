use std::{
  collections::{HashMap, HashSet},
  net::SocketAddr,
};

use memberlist_proto::Instant;

use super::{
  MemberlistError, PendingJoin, PendingLeave, PendingPing, PendingUserSend, dispatch_command,
  min_pending_join_deadline, min_pending_leave_deadline, observation_task, reap_pending_joins,
  reap_pending_leave,
};
use crate::{
  QuicEndpoint,
  command::{
    Command, JoinCmd, JoinKind, LeaveCmd, PingCmd, SendReliableCmd, SendUserCmd, SetAckPayloadCmd,
    SetLocalStateCmd, WaitForCompletionArgs,
  },
  delegate::VoidDelegate,
};
use core::time::Duration;

/// Build a standalone QUIC coordinator for dispatch-unit tests.
///
/// `QuicEndpoint::new` wraps the (sans-I/O) membership `Endpoint` with a
/// quinn-proto endpoint built from a fresh self-signed-localhost `QuicOptions`
/// — no socket is bound, so the handler arms can be exercised directly without
/// a live connection. The membership FSM starts `Running` after
/// `start_scheduling`, so `dispatch_command` takes the running branches; a
/// caller that wants the post-`leave()` `NotRunning` branches calls
/// `endpoint.leave(now)` first.
fn build_endpoint(id: &str, addr: SocketAddr, now: Instant) -> QuicEndpoint<smol_str::SmolStr> {
  use memberlist_proto::{EndpointOptions, endpoint::Endpoint};
  use rand::SeedableRng;

  let cfg = EndpointOptions::new(smol_str::SmolStr::new(id), addr);
  // Deterministic gossip RNG seeded off the port keeps the test reproducible;
  // these dispatch arms do not draw entropy, but `Endpoint::new` requires one.
  let rng = rand::rngs::StdRng::seed_from_u64(addr.port() as u64);
  let mut ep: Endpoint<smol_str::SmolStr, SocketAddr, rand::rngs::StdRng> = Endpoint::new(cfg, rng);
  ep.start_scheduling(now);

  let qc = test_quic_options();
  let mut seed = [0u8; 32];
  seed[..2].copy_from_slice(&addr.port().to_le_bytes());
  QuicEndpoint::<smol_str::SmolStr>::with_quinn_rng_seed(ep, qc, Some(seed))
}

/// A self-signed-localhost `QuicOptions` for dispatch-unit tests.
///
/// The dispatch arms under test (`NotRunning` gates, CIDR-skip, empty-payload
/// short-circuits, leave parking) never complete a handshake, so the crypto
/// material only has to be structurally valid. Mirrors the integration
/// harness's `build_quic_config` so the coordinator is built the same way the
/// driver builds it in production.
fn test_quic_options() -> memberlist_proto::QuicOptions {
  use std::sync::Arc;

  use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
  use rustls::RootCertStore;
  use rustls_pki_types::{CertificateDer, PrivateKeyDer};

  const ALPN: &[u8] = b"memberlist-quic";

  let ck =
    rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("rcgen self-sign");
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("add root cert");

  let provider = Arc::new(rustls::crypto::ring::default_provider());

  let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .expect("server single cert");
  rustls_server.alpn_protocols = vec![ALPN.to_vec()];
  let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
    .expect("QuicServerConfig");
  let server_cfg = ServerConfig::with_crypto(Arc::new(qsc));

  let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_root_certificates(roots)
    .with_no_client_auth();
  rustls_client.alpn_protocols = vec![ALPN.to_vec()];
  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
    .expect("QuicClientConfig");
  let client_cfg = ClientConfig::new(Arc::new(qcc));

  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &[0x5au8; 32]);
  let endpoint_cfg = EndpointConfig::new(Arc::new(hmac));

  let transport = TransportConfig::default();

  memberlist_proto::QuicOptions::new(
    endpoint_cfg,
    server_cfg,
    client_cfg,
    transport,
    "localhost",
    memberlist_proto::UnreliableTransport::Datagram,
  )
}

fn addr(port: u16) -> SocketAddr {
  ([127, 0, 0, 1], port).into()
}

/// Build an empty (allow-all default) CIDR filter — admits every address.
fn allow_all() -> super::CidrFilter {
  #[cfg(feature = "cidr")]
  {
    None
  }
  #[cfg(not(feature = "cidr"))]
  {
    Default::default()
  }
}

/// A block-all CIDR policy used to exercise the seed/destination-skip arms.
/// Only meaningful with the `cidr` feature; the no-`cidr` build's `cidr_blocks`
/// is a constant `false`, so the block arms are unreachable there.
#[cfg(feature = "cidr")]
fn block_all() -> super::CidrFilter {
  Some(memberlist_proto::CidrPolicy::block_all())
}

/// Drive one `dispatch_command` against a fresh detached state bundle.
///
/// Mirrors the per-iteration narrow-borrow call in `quic_driver_loop`: the
/// pending-tables/`next_pending_join_id`/`shutdown_reply` slots all start empty,
/// so the assertion sees exactly what this one command parked or replied.
#[allow(clippy::too_many_arguments)]
async fn dispatch_one(
  endpoint: &mut QuicEndpoint<smol_str::SmolStr>,
  cmd: Command<smol_str::SmolStr>,
  now: Instant,
  cidr: &super::CidrFilter,
) -> DispatchOutcome {
  let mut shutdown_reply = None;
  let mut pending_joins: HashMap<u64, PendingJoin> = HashMap::new();
  let mut next_pending_join_id: u64 = 0;
  let mut pending_leave: Option<PendingLeave> = None;
  let mut pending_pings: Vec<PendingPing> = Vec::new();
  let mut pending_user_sends: Vec<PendingUserSend> = Vec::new();

  dispatch_command::<smol_str::SmolStr, _>(
    endpoint,
    &mut shutdown_reply,
    &mut pending_joins,
    &mut next_pending_join_id,
    &mut pending_leave,
    &mut pending_pings,
    &mut pending_user_sends,
    Duration::from_secs(5),
    cidr,
    cmd,
    now,
  )
  .await;

  DispatchOutcome {
    pending_joins,
    pending_leave,
    pending_pings_len: pending_pings.len(),
    pending_user_sends_len: pending_user_sends.len(),
    shutdown_reply_set: shutdown_reply.is_some(),
  }
}

/// What a single dispatched command left behind in the driver's parking slots.
struct DispatchOutcome {
  pending_joins: HashMap<u64, PendingJoin>,
  pending_leave: Option<PendingLeave>,
  pending_pings_len: usize,
  pending_user_sends_len: usize,
  shutdown_reply_set: bool,
}

/// Assemble a `QuicDriverState` around a freshly-bound loopback UDP socket so
/// `drain_actions` can be exercised directly.
///
/// The snapshot cell is bootstrapped from the endpoint's own local node (the
/// membership map always carries it); `drain_actions` itself does not touch the
/// snapshot, so the bootstrap value only has to be structurally valid. The
/// returned `obs_rx` lets a test force the surfacing `try_send` down its
/// `Closed` (drop the receiver) or `Full` (saturate a tiny bounded channel)
/// branch. `commands` is a live-but-empty flume channel whose sender is held in
/// the returned tuple so the receiver does not see `Disconnected` mid-test.
/// The driver event type, parameterised for the dispatch-unit-test id/address.
type TestEvent = memberlist_proto::event::Event<smol_str::SmolStr, SocketAddr>;

/// A built `QuicDriverState` plus the two channel ends a test holds onto: the
/// observation receiver (drop ⇒ obs `Closed`; saturate ⇒ obs `Full`) and the
/// command sender (kept live so the driver's `commands` receiver does not see
/// `Disconnected`).
type BuiltState = (
  super::QuicDriverState<smol_str::SmolStr>,
  lochan::mpsc::Receiver<TestEvent>,
  flume::Sender<Command<smol_str::SmolStr>>,
);

fn build_state(
  endpoint: QuicEndpoint<smol_str::SmolStr>,
  udp_socket: compio::net::UdpSocket,
  obs_channel: crate::Channel,
) -> BuiltState {
  use std::{cell::Cell, rc::Rc};

  use memberlist_proto::{metrics::Metrics, typed::State};

  let (obs_tx, obs_rx) = match obs_channel {
    crate::Channel::Unbounded => lochan::mpsc::unbounded(),
    crate::Channel::Bounded(n) => lochan::mpsc::bounded(n),
  };
  let obs_payload_budget: Option<u64> = match obs_channel {
    crate::Channel::Bounded(_) => Some((endpoint.max_stream_frame_size() as u64).saturating_mul(4)),
    crate::Channel::Unbounded => None,
  };

  // Bootstrap the snapshot cell from the endpoint's local node entry.
  let ep_ref = endpoint.endpoint_ref();
  let local = ep_ref
    .member(ep_ref.local_id_ref())
    .expect("local node is always present");
  let alive = matches!(
    ep_ref
      .member_liveness(ep_ref.local_id_ref())
      .unwrap_or(State::Unknown(0)),
    State::Alive
  );
  let boot = crate::snapshot::MemberlistSnapshot::new(
    vec![local.clone()],
    local,
    usize::from(alive),
    ep_ref.num_members(),
    ep_ref.health_score(),
  );
  let snapshot: crate::snapshot::SnapshotCell<smol_str::SmolStr> =
    Rc::new(std::cell::RefCell::new(Rc::new(boot)));

  let (commands_tx, commands_rx) = flume::unbounded::<Command<smol_str::SmolStr>>();

  let state = super::QuicDriverState {
    endpoint,
    udp_socket,
    commands: commands_rx,
    label: None,
    obs_tx,
    observation_dropped: Rc::new(Cell::new(0)),
    obs_payload_bytes: Rc::new(Cell::new(0)),
    obs_payload_budget,
    snapshot,
    last_snapshot_version: 0,
    metrics: Rc::new(Cell::new(Metrics::default())),
    last_metrics: Metrics::default(),
    shutdown_flag: Rc::new(Cell::new(false)),
    driver_opts: crate::driver::options::RuntimeOptions::new(),
    cidr_policy: allow_all(),
    pending_joins: HashMap::new(),
    next_pending_join_id: 0,
    shutdown_reply: None,
    pending_leave: None,
    pending_pings: Vec::new(),
    pending_user_sends: Vec::new(),
  };
  (state, obs_rx, commands_tx)
}

/// Bind a loopback UDP socket on an OS-allocated port for a driver-state test.
async fn bind_udp() -> compio::net::UdpSocket {
  compio::net::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind loopback udp")
}

// ---- drain_actions: no-peer leave resolution + obs hand-off ----

/// A standalone (no live peers) `leave()` emits `Event::LeftCluster` on the very
/// next `poll_event`. `drain_actions` resolves the parked `pending_leave` to
/// `Ok(())` directly on the driver task and reports progress. Covers the
/// `LeftCluster ⇒ resolve_all(Ok)` arm in the events step.
#[compio::test]
async fn drain_actions_resolves_no_peer_leave() {
  let now = Instant::now();
  let mut endpoint = build_endpoint("solo", addr(17701), now);
  // No peers were ever added, so leave() emits LeftCluster immediately —
  // there is nothing to flush.
  endpoint.leave(now).expect("leave initiates");

  let udp = bind_udp().await;
  let (mut state, _obs_rx, _cmd_tx) = build_state(endpoint, udp, crate::Channel::Bounded(64));

  // Park a leave waiter as the dispatch arm would.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  state.pending_leave = Some(PendingLeave {
    repliers: vec![tx],
    deadline: now + Duration::from_secs(5),
  });

  let progressed = super::drain_actions::<smol_str::SmolStr, _>(&mut state).await;
  assert!(progressed, "draining the LeftCluster event is progress");
  assert!(
    state.pending_leave.is_none(),
    "the parked leave was resolved and cleared",
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "the no-peer leave resolves Ok once LeftCluster drains",
  );
}

/// When the observation channel has already been CLOSED (its receiver dropped,
/// as at driver teardown), the surfacing `try_send` returns `Closed` and
/// `drain_actions` silently drops the event — no `observation_dropped` increment
/// (a closed channel is teardown, not a delegate-too-slow gap). Covers the
/// `Err(TrySendError::Closed(_)) => {}` arm.
#[compio::test]
async fn drain_actions_obs_closed_drops_without_counting() {
  let now = Instant::now();
  let mut endpoint = build_endpoint("solo", addr(17702), now);
  endpoint.leave(now).expect("leave initiates");

  let udp = bind_udp().await;
  let (mut state, obs_rx, _cmd_tx) = build_state(endpoint, udp, crate::Channel::Bounded(64));
  // Close the observation channel up front: every surfacing try_send now sees
  // `Closed`.
  drop(obs_rx);

  let dropped_before = state.observation_dropped.get();
  let progressed = super::drain_actions::<smol_str::SmolStr, _>(&mut state).await;
  assert!(
    progressed,
    "the LeftCluster event still drives protocol progress"
  );
  assert_eq!(
    state.observation_dropped.get(),
    dropped_before,
    "a Closed obs channel is teardown, not a counted drop",
  );
}

// ---- dispatch_command: NotRunning gates (post-leave) ----

/// After `leave()` the coordinator is no longer `Running`, so every dispatch
/// arm that gates on `is_running()` rejects with `NotRunning` and parks
/// nothing. Covers the `else { Err(NotRunning) }` branches for
/// `UpdateNodeMetadata`, `SetLocalState`, `SetAckPayload`, `SendUser`,
/// `Ping`, and `SendReliable`.
#[compio::test]
async fn dispatch_rejects_commands_after_leave() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17601), now);
  // First leave on a Running node initiates the flush and returns Ok; the
  // endpoint is now `Leaving`, so `is_running()` is false for what follows.
  ep.leave(now).expect("first leave initiates");
  assert!(!ep.is_running(), "leave() leaves the node not-running");
  let cidr = allow_all();

  // UpdateNodeMetadata → NotRunning.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  dispatch_one(
    &mut ep,
    Command::UpdateNodeMetadata(crate::command::UpdateNodeMetadataCmd {
      meta: vec![1, 2, 3],
      reply: tx,
    }),
    now,
    &cidr,
  )
  .await;
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "UpdateNodeMetadata after leave ⇒ NotRunning",
  );

  // SetLocalState → NotRunning.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  dispatch_one(
    &mut ep,
    Command::SetLocalState(SetLocalStateCmd::new(bytes::Bytes::from_static(b"st"), tx)),
    now,
    &cidr,
  )
  .await;
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "SetLocalState after leave ⇒ NotRunning",
  );

  // SetAckPayload → NotRunning.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  dispatch_one(
    &mut ep,
    Command::SetAckPayload(SetAckPayloadCmd::new(bytes::Bytes::from_static(b"ack"), tx)),
    now,
    &cidr,
  )
  .await;
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "SetAckPayload after leave ⇒ NotRunning",
  );

  // SendUser → NotRunning (the running-gate fires before the CIDR check).
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  dispatch_one(
    &mut ep,
    Command::SendUser(SendUserCmd::new(
      addr(17699),
      vec![bytes::Bytes::from_static(b"hi")],
      tx,
    )),
    now,
    &cidr,
  )
  .await;
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "SendUser after leave ⇒ NotRunning",
  );

  // Ping → NotRunning, no waiter parked.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<Duration>>();
  let node = memberlist_proto::Node::new(smol_str::SmolStr::new("peer"), addr(17699));
  let out = dispatch_one(&mut ep, Command::Ping(PingCmd::new(node, tx)), now, &cidr).await;
  assert_eq!(
    out.pending_pings_len, 0,
    "no ping waiter parked when not running"
  );
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "Ping after leave ⇒ NotRunning",
  );

  // SendReliable → NotRunning, no waiter parked.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  let out = dispatch_one(
    &mut ep,
    Command::SendReliable(SendReliableCmd::new(
      addr(17699),
      vec![bytes::Bytes::from_static(b"data")],
      tx,
    )),
    now,
    &cidr,
  )
  .await;
  assert_eq!(
    out.pending_user_sends_len, 0,
    "no reliable-send waiter parked when not running"
  );
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "SendReliable after leave ⇒ NotRunning",
  );
}

/// A `Join` (both kinds) dispatched after `leave()` rejects with `NotRunning`
/// before any push/pull is enqueued, and parks no waiter.
#[compio::test]
async fn dispatch_join_rejected_after_leave() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17602), now);
  ep.leave(now).expect("first leave initiates");
  let cidr = allow_all();

  // Dispatch kind.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<usize>>();
  let out = dispatch_one(
    &mut ep,
    Command::Join(JoinCmd {
      addrs: vec![addr(17699)],
      kind: JoinKind::Dispatch,
      reply: tx,
    }),
    now,
    &cidr,
  )
  .await;
  assert!(out.pending_joins.is_empty(), "no join waiter parked");
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "Join(Dispatch) after leave ⇒ NotRunning",
  );

  // WaitForCompletion kind.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<usize>>();
  let out = dispatch_one(
    &mut ep,
    Command::Join(JoinCmd {
      addrs: vec![addr(17699)],
      kind: JoinKind::WaitForCompletion(WaitForCompletionArgs {
        deadline: now + Duration::from_secs(10),
      }),
      reply: tx,
    }),
    now,
    &cidr,
  )
  .await;
  assert!(out.pending_joins.is_empty(), "no join waiter parked");
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::NotRunning))),
    "Join(WaitForCompletion) after leave ⇒ NotRunning",
  );
}

// ---- dispatch_command: Shutdown stashes its reply ----

/// `Command::Shutdown` does not reply inline — it stashes the reply sender into
/// `shutdown_reply` so the post-loop cleanup can ack AFTER the UDP socket drops
/// (freeing the bound port before the caller's `shutdown().await` resumes).
/// Covers the `*shutdown_reply = Some(reply)` arm.
#[compio::test]
async fn dispatch_shutdown_stashes_reply_for_post_loop_ack() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17605), now);
  let cidr = allow_all();

  let (tx, _rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  // The other parking slots must stay empty — Shutdown only touches
  // `shutdown_reply`.
  let out = dispatch_one(
    &mut ep,
    Command::Shutdown(crate::command::ShutdownCmd { reply: tx }),
    now,
    &cidr,
  )
  .await;
  assert!(
    out.shutdown_reply_set,
    "Shutdown stashes its reply for the post-loop ack rather than replying inline",
  );
  assert!(out.pending_joins.is_empty(), "Shutdown parks no join");
  assert!(out.pending_leave.is_none(), "Shutdown parks no leave");
}

// ---- dispatch_command: Leave shared-operation semantics ----

/// A `Leave` that finds the node already-left is an idempotent no-op:
/// `was_running` is false, `endpoint.leave()` returns `Ok(())`, and the
/// `other` arm replies immediately rather than parking (parking would hang
/// on a `LeftCluster` that never re-fires). Covers the `other` reply arm.
#[compio::test]
async fn dispatch_leave_on_already_left_replies_immediately() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17603), now);
  // Drive into the not-running state first.
  ep.leave(now).expect("first leave initiates");
  assert!(!ep.is_running());
  let cidr = allow_all();

  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  let out = dispatch_one(&mut ep, Command::Leave(LeaveCmd { reply: tx }), now, &cidr).await;
  assert!(
    out.pending_leave.is_none(),
    "an idempotent no-op leave does not park a waiter",
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "leave on an already-left node replies Ok immediately",
  );
}

/// A `Leave` that finds the node `Running` PARKS a waiter (it returned Ok and
/// `was_running` was true) — the reply is withheld until `LeftCluster`. Covers
/// the `Ok(()) if was_running => park` arm.
#[compio::test]
async fn dispatch_leave_while_running_parks_waiter() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17604), now);
  assert!(ep.is_running(), "fresh endpoint is running");
  let cidr = allow_all();

  let (tx, mut rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  let out = dispatch_one(&mut ep, Command::Leave(LeaveCmd { reply: tx }), now, &cidr).await;
  let pl = out.pending_leave.expect("a running leave parks a waiter");
  assert_eq!(pl.repliers.len(), 1, "the initiator is the sole replier");
  assert_eq!(
    pl.deadline,
    now + Duration::from_secs(5),
    "the parked deadline is now + leave_timeout",
  );
  assert!(
    matches!(rx.try_recv(), Ok(None)),
    "no reply is sent while the leave is in flight",
  );
}

// ---- dispatch_command: CIDR transport-source filtering ----

/// A block-all CIDR policy makes `Join(Dispatch)` skip every seed: no
/// push/pull is started, and the reply is `Ok(0)` (zero seeds contacted).
/// Covers the `cidr_blocks(...) { continue }` skip in the Dispatch arm.
#[cfg(feature = "cidr")]
#[compio::test]
async fn dispatch_join_skips_cidr_blocked_seeds() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17611), now);
  let cidr = block_all();

  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<usize>>();
  let out = dispatch_one(
    &mut ep,
    Command::Join(JoinCmd {
      addrs: vec![addr(17699), addr(17700)],
      kind: JoinKind::Dispatch,
      reply: tx,
    }),
    now,
    &cidr,
  )
  .await;
  assert!(out.pending_joins.is_empty(), "Dispatch parks nothing");
  assert!(
    matches!(rx.await, Ok(Ok(0))),
    "every seed CIDR-blocked ⇒ zero contacted",
  );
}

/// A block-all CIDR policy makes `Join(WaitForCompletion)` block every seed,
/// leaving the `pending` set empty: the arm replies `JoinAllFailed` carrying
/// the original seed count immediately instead of parking. Covers the
/// `if pending.is_empty()` short-circuit in the WaitForCompletion arm.
#[cfg(feature = "cidr")]
#[compio::test]
async fn dispatch_join_wait_all_blocked_fails_immediately() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17612), now);
  let cidr = block_all();

  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<usize>>();
  let out = dispatch_one(
    &mut ep,
    Command::Join(JoinCmd {
      addrs: vec![addr(17699), addr(17700)],
      kind: JoinKind::WaitForCompletion(WaitForCompletionArgs {
        deadline: now + Duration::from_secs(10),
      }),
      reply: tx,
    }),
    now,
    &cidr,
  )
  .await;
  assert!(out.pending_joins.is_empty(), "no waiter parked");
  match rx.await {
    Ok(Err(MemberlistError::JoinAllFailed(e))) => {
      assert_eq!(e.requested(), 2, "carries the original seed count");
      assert_eq!(e.contacted(), 0);
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }
}

/// A block-all CIDR policy fails an unreliable `SendUser` to the blocked
/// destination with `SendFailed` (the destination is excluded before any
/// datagram is emitted). Covers the `cidr_blocks(...) ⇒ SendFailed` arm.
#[cfg(feature = "cidr")]
#[compio::test]
async fn dispatch_send_user_to_blocked_dest_fails() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17613), now);
  let cidr = block_all();

  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  dispatch_one(
    &mut ep,
    Command::SendUser(SendUserCmd::new(
      addr(17699),
      vec![bytes::Bytes::from_static(b"hi")],
      tx,
    )),
    now,
    &cidr,
  )
  .await;
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::SendFailed))),
    "SendUser to a CIDR-blocked destination ⇒ SendFailed",
  );
}

/// A block-all CIDR policy fails a reliable `SendReliable` to the blocked
/// destination with `SendFailed` without opening a QUIC stream or parking a
/// waiter. Covers the reliable-send `cidr_blocks(...) ⇒ SendFailed` early
/// return.
#[cfg(feature = "cidr")]
#[compio::test]
async fn dispatch_send_reliable_to_blocked_dest_fails() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17614), now);
  let cidr = block_all();

  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  let out = dispatch_one(
    &mut ep,
    Command::SendReliable(SendReliableCmd::new(
      addr(17699),
      vec![bytes::Bytes::from_static(b"data")],
      tx,
    )),
    now,
    &cidr,
  )
  .await;
  assert_eq!(
    out.pending_user_sends_len, 0,
    "no reliable-send waiter parked"
  );
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::SendFailed))),
    "SendReliable to a CIDR-blocked destination ⇒ SendFailed",
  );
}

// ---- dispatch_command: empty-payload short-circuits (running node) ----

/// A reliable send with zero payloads dispatches no exchange and replies
/// `Ok(())` immediately rather than parking a never-resolving waiter. Covers
/// the `if pending.is_empty() ⇒ Ok(())` short-circuit in the SendReliable arm.
#[compio::test]
async fn dispatch_send_reliable_empty_payloads_replies_ok() {
  let now = Instant::now();
  let mut ep = build_endpoint("a", addr(17621), now);
  assert!(ep.is_running());
  let cidr = allow_all();

  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  let out = dispatch_one(
    &mut ep,
    Command::SendReliable(SendReliableCmd::new(addr(17699), Vec::new(), tx)),
    now,
    &cidr,
  )
  .await;
  assert_eq!(
    out.pending_user_sends_len, 0,
    "an empty reliable send parks no waiter"
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "an empty reliable send resolves Ok immediately",
  );
}

// ---- observation_task: best-effort EventStream forward drop ----

/// The observation task forwards membership / control events to the bounded
/// `events_tx`; when that channel is FULL the forward `try_send` fails and the
/// task increments `events_dropped` instead of blocking. Covers the
/// `events_dropped.set(...)` Full branch.
#[compio::test]
async fn observation_task_counts_eventstream_forward_drops() {
  use std::{cell::Cell, rc::Rc};

  // A capacity-1 EventStream channel pre-filled so every forward sees `Full`.
  let (events_tx, _events_rx) =
    flume::bounded::<memberlist_proto::event::Event<smol_str::SmolStr, SocketAddr>>(1);
  events_tx
    .try_send(memberlist_proto::event::Event::LeftCluster)
    .expect("prime the single slot");

  // Hand-off channel the driver would feed; push two non-payload (membership /
  // control) events through it, then drop the sender so the task's recv loop
  // terminates after draining.
  let (obs_tx, obs_rx) =
    lochan::mpsc::unbounded::<memberlist_proto::event::Event<smol_str::SmolStr, SocketAddr>>();
  obs_tx
    .try_send(memberlist_proto::event::Event::LeftCluster)
    .expect("queue first control event");
  obs_tx
    .try_send(memberlist_proto::event::Event::LeftCluster)
    .expect("queue second control event");
  drop(obs_tx);

  let events_dropped = Rc::new(Cell::new(0u64));
  let obs_payload_bytes = Rc::new(Cell::new(0u64));
  observation_task::<smol_str::SmolStr, VoidDelegate<smol_str::SmolStr, SocketAddr>>(
    obs_rx,
    VoidDelegate::new(),
    events_tx,
    events_dropped.clone(),
    obs_payload_bytes.clone(),
  )
  .await;

  assert_eq!(
    events_dropped.get(),
    2,
    "both control events hit a full EventStream and were counted as drops",
  );
  assert_eq!(
    obs_payload_bytes.get(),
    0,
    "control events carry no payload, so the byte backstop is untouched",
  );
}

/// The observation task decrements the payload-byte backstop for each
/// payload-bearing event it dequeues, and does NOT forward those to the
/// EventStream (they went to the delegate only). Covers the
/// `obs_payload_bytes.set(get - b)` decrement + the `payload.is_some() =>
/// continue` skip.
#[compio::test]
async fn observation_task_releases_payload_budget_and_skips_forward() {
  use std::{cell::Cell, rc::Rc};

  let (events_tx, events_rx) =
    flume::bounded::<memberlist_proto::event::Event<smol_str::SmolStr, SocketAddr>>(8);

  let payload = bytes::Bytes::from_static(b"hello-app-data");
  let ev = memberlist_proto::event::Event::UserPacket(memberlist_proto::event::UserPacket::new(
    addr(17699),
    payload.clone(),
    memberlist_proto::event::Reliability::Unreliable,
  ));
  let weight = payload.len() as u64;

  let (obs_tx, obs_rx) =
    lochan::mpsc::unbounded::<memberlist_proto::event::Event<smol_str::SmolStr, SocketAddr>>();
  obs_tx.try_send(ev).expect("queue the payload event");
  drop(obs_tx);

  let events_dropped = Rc::new(Cell::new(0u64));
  // Pre-reserve the budget the enqueue side would have added, so the task's
  // dequeue decrement is observable.
  let obs_payload_bytes = Rc::new(Cell::new(weight));
  observation_task::<smol_str::SmolStr, VoidDelegate<smol_str::SmolStr, SocketAddr>>(
    obs_rx,
    VoidDelegate::new(),
    events_tx,
    events_dropped.clone(),
    obs_payload_bytes.clone(),
  )
  .await;

  assert_eq!(
    obs_payload_bytes.get(),
    0,
    "the dequeued payload event released its reserved bytes",
  );
  assert_eq!(events_dropped.get(), 0, "no forward attempted, no drop");
  assert!(
    events_rx.try_recv().is_err(),
    "app-data is NOT fanned out to the EventStream",
  );
}

/// `min_pending_join_deadline` returns the EARLIEST deadline across waiters,
/// and `None` when there are none. Folded into the driver's select timer so
/// the soonest-expiring synchronous join wakes the loop.
#[compio::test]
async fn min_pending_join_deadline_picks_the_earliest() {
  let mut joins: HashMap<u64, PendingJoin> = HashMap::new();
  assert_eq!(
    min_pending_join_deadline(&joins),
    None,
    "no waiters ⇒ no deadline",
  );

  let base = Instant::now();
  let earliest = base + Duration::from_secs(1);
  let latest = base + Duration::from_secs(5);
  for (key, deadline) in [(1u64, latest), (2u64, earliest)] {
    let (tx, _rx) = futures_channel::oneshot::channel();
    joins.insert(
      key,
      PendingJoin {
        pending: HashSet::new(),
        contacted: 0,
        requested: 1,
        deadline,
        reply: tx,
      },
    );
  }
  assert_eq!(
    min_pending_join_deadline(&joins),
    Some(earliest),
    "the soonest deadline wins",
  );
}

/// `min_pending_leave_deadline` surfaces the in-flight leave's deadline, or
/// `None` when no leave is parked.
#[compio::test]
async fn min_pending_leave_deadline_tracks_the_parked_leave() {
  assert_eq!(min_pending_leave_deadline(&None), None);
  let deadline = Instant::now() + Duration::from_secs(3);
  let pl = PendingLeave {
    repliers: Vec::new(),
    deadline,
  };
  assert_eq!(min_pending_leave_deadline(&Some(pl)), Some(deadline));
}

/// `PendingLeave::resolve_all` fans the single terminal outcome out to EVERY
/// joined replier (the initiator plus racing clones), and silently tolerates a
/// replier whose receiver was dropped.
#[compio::test]
async fn pending_leave_resolve_all_fans_out_to_every_replier() {
  let (tx_a, rx_a) = futures_channel::oneshot::channel::<super::Result<()>>();
  let (tx_b, rx_b) = futures_channel::oneshot::channel::<super::Result<()>>();
  // A third replier whose receiver is dropped up front — `resolve_all` must
  // not panic when its send fails.
  let (tx_c, rx_c) = futures_channel::oneshot::channel::<super::Result<()>>();
  drop(rx_c);

  let pl = PendingLeave {
    repliers: vec![tx_a, tx_b, tx_c],
    deadline: Instant::now(),
  };
  pl.resolve_all(|| Ok(())).await;

  assert!(
    matches!(rx_a.await, Ok(Ok(()))),
    "first replier resolved Ok"
  );
  assert!(
    matches!(rx_b.await, Ok(Ok(()))),
    "second replier resolved Ok"
  );
}

/// `reap_pending_leave` replies `LeaveTimeout` to every replier once the
/// deadline has elapsed and clears the slot; a not-yet-expired leave is left
/// parked untouched.
#[compio::test]
async fn reap_pending_leave_fires_only_after_the_deadline() {
  let now = Instant::now();

  // Not yet expired: the slot stays, no reply is sent.
  let (tx_live, mut rx_live) = futures_channel::oneshot::channel::<super::Result<()>>();
  let mut not_expired = Some(PendingLeave {
    repliers: vec![tx_live],
    deadline: now + Duration::from_secs(10),
  });
  reap_pending_leave(&mut not_expired, now).await;
  assert!(
    not_expired.is_some(),
    "a future-deadline leave is left parked"
  );
  assert!(
    matches!(rx_live.try_recv(), Ok(None)),
    "no reply is sent before the deadline",
  );

  // Expired: every replier gets LeaveTimeout and the slot is cleared.
  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<()>>();
  let mut expired = Some(PendingLeave {
    repliers: vec![tx],
    deadline: now - Duration::from_secs(1),
  });
  reap_pending_leave(&mut expired, now).await;
  assert!(expired.is_none(), "an expired leave clears its slot");
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::LeaveTimeout))),
    "an expired leave replies LeaveTimeout",
  );
}

/// `reap_pending_joins` resolves a waiter whose `pending` set is empty: zero
/// contacts ⇒ `JoinAllFailed(requested, 0)`, any contacts ⇒ `Ok(contacted)`.
/// (The empty-`pending` branch is the degenerate zero-exchange
/// `WaitForCompletion` AND the post-completion drain.)
#[compio::test]
async fn reap_pending_joins_resolves_empty_pending_waiters() {
  let now = Instant::now();
  let mut joins: HashMap<u64, PendingJoin> = HashMap::new();

  // Waiter 1: zero contacts ⇒ JoinAllFailed carrying the requested count.
  let (tx_fail, rx_fail) = futures_channel::oneshot::channel();
  joins.insert(
    1,
    PendingJoin {
      pending: HashSet::new(),
      contacted: 0,
      requested: 3,
      deadline: now + Duration::from_secs(60),
      reply: tx_fail,
    },
  );
  // Waiter 2: two contacts ⇒ Ok(2).
  let (tx_ok, rx_ok) = futures_channel::oneshot::channel();
  joins.insert(
    2,
    PendingJoin {
      pending: HashSet::new(),
      contacted: 2,
      requested: 2,
      deadline: now + Duration::from_secs(60),
      reply: tx_ok,
    },
  );

  reap_pending_joins(&mut joins, now).await;
  assert!(joins.is_empty(), "both empty-pending waiters are reaped");

  match rx_fail.await {
    Ok(Err(MemberlistError::JoinAllFailed(e))) => {
      assert_eq!(e.requested(), 3, "carries the requested seed count");
      assert_eq!(e.contacted(), 0);
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }
  assert!(
    matches!(rx_ok.await, Ok(Ok(2))),
    "a waiter with contacts resolves Ok(contacted)",
  );
}
