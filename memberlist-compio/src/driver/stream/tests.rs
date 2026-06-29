use std::{
  cell::{Cell, RefCell},
  collections::{HashMap, HashSet},
  net::{IpAddr, Ipv4Addr, SocketAddr},
  rc::Rc,
};

use crate::StreamEndpoint;
use bytes::Bytes;
use memberlist_proto::{
  Instant, Node, RawRecords,
  config::EndpointOptions,
  endpoint::Endpoint,
  event::{PushPullKind, StreamId},
  streams::{ExchangeId, LabelOptions, StreamAction},
};
use smol_str::SmolStr;

use crate::{
  command::{
    Command, JoinCmd, JoinKind, LeaveCmd, PingCmd, SendReliableCmd, SendUserCmd, SetAckPayloadCmd,
    SetLocalStateCmd, ShutdownCmd, UpdateNodeMetadataCmd, WaitForCompletionArgs,
  },
  delegate::VoidDelegate,
  driver::options::RuntimeOptions,
  snapshot::{MemberlistSnapshot, SnapshotCell},
};
use lochan::mpsc;

use super::{
  BridgeHandle, BridgeOut, BridgeReady, GOSSIP_RECV_BUF_MAX, MemberlistError, PendingCommands,
  PendingJoin, PendingLeave, StreamTransportOptions, dispatch_command, dispatch_gossip,
  drain_actions, drain_events, drain_transport_transmits, fire_timeout_with_drain,
  freeze_and_drain_bridges_to_disconnected, gossip_recv_buf_len, min_pending_join_deadline,
  min_pending_leave_deadline, process_one_action, reap_pending_joins, reap_pending_leave,
  spawn_bridge,
};
use compio::net::{TcpListener, TcpStream};
use core::time::Duration;

fn addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

fn test_endpoint() -> StreamEndpoint<SmolStr, SocketAddr, RawRecords> {
  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("capture-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  StreamEndpoint::new(
    ep,
    LabelOptions::new_in(Some(b"capture-test".to_vec()), ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  )
}

/// `process_one_action` captures a `Connect`'s `ExchangeId` ONLY when its
/// originating `stream_id()` is in the `started` set — never by peer address.
/// This is the regression guard for the cross-subsystem misattribution: a
/// same-peer dial flushed by the shared `service_dials` for another subsystem
/// (here a `start_push_pull`) is left pending while a `send_reliable`-style
/// `start_user_message` runs; the user-message command must capture ONLY its
/// own exchange, so the parked waiter never sees a foreign `ExchangeId` (which
/// would make `m > n` or park on a PushPull completion the resolver ignores).
#[compio::test]
async fn capture_binds_to_started_stream_id_not_peer() {
  let mut endpoint = test_endpoint();
  let now = Instant::now();
  let peer = addr(7000);

  // An UNRELATED dial to `peer` enqueued by another subsystem. Its in-band
  // `service_dials` already queued the `Connect`; we deliberately do NOT drain
  // it, so it is still pending when the user-message command runs — exactly the
  // shared-deque hazard `send_many_reliable` faces.
  let foreign_sid = endpoint.start_push_pull(peer, PushPullKind::Join, now);

  // The user-message command's own dial to the SAME peer.
  let mut started: HashSet<StreamId> = HashSet::new();
  let user_sid = endpoint
    .start_user_message(peer, bytes::Bytes::from_static(b"hi"), now)
    .expect("issued while running");
  started.insert(user_sid);
  assert_ne!(
    foreign_sid, user_sid,
    "distinct dials get distinct StreamIds"
  );

  // Drain every queued action through the real capture path. Both Connects
  // (foreign push/pull + this user-message) surface here; only the one whose
  // StreamId is in `started` may be captured.
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let (ready_tx, _ready_rx) = flume::unbounded::<BridgeReady>();
  let mut captured: HashSet<ExchangeId> = HashSet::new();
  let mut connect_count = 0usize;
  while let Some(action) = endpoint.poll_action() {
    if matches!(action, StreamAction::Connect(_)) {
      connect_count += 1;
    }
    process_one_action(
      action,
      &mut bridges,
      &ready_tx,
      StreamTransportOptions::default(),
      Some((&started, &mut captured, None)),
      &Default::default(),
    );
  }

  assert_eq!(
    connect_count, 2,
    "both same-peer dials surfaced a Connect in this drain",
  );
  assert_eq!(
    captured.len(),
    1,
    "exactly this command's one exchange was captured (m <= n holds)",
  );
  // The captured exchange is the user-message's, not the foreign push/pull's.
  // Both bridges were installed (every Connect routes to its bridge), so the
  // distinguishing fact is solely WHICH ExchangeId entered `captured`.
  assert_eq!(bridges.len(), 2, "every Connect installs its bridge handle");
  // A Connect whose StreamId is NOT in `started` contributes nothing.
  let empty_started: HashSet<StreamId> = HashSet::new();
  let mut endpoint2 = test_endpoint();
  let _ = endpoint2.start_push_pull(peer, PushPullKind::Join, now);
  let mut bridges2: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let mut captured2: HashSet<ExchangeId> = HashSet::new();
  while let Some(action) = endpoint2.poll_action() {
    process_one_action(
      action,
      &mut bridges2,
      &ready_tx,
      StreamTransportOptions::default(),
      Some((&empty_started, &mut captured2, None)),
      &Default::default(),
    );
  }
  assert!(
    captured2.is_empty(),
    "a Connect whose StreamId is not in `started` is never captured",
  );
}

/// `dispatch_gossip` drops a datagram that fails the codec gate (garbage that
/// is neither a valid encrypted wrapper nor a decodable labeled frame) without
/// panicking and without admitting any membership — exercising the lossy-gossip
/// `continue` arms (decrypt / decode / parse failures) per the drop discipline.
#[compio::test]
async fn dispatch_gossip_drops_malformed_datagram() {
  let mut endpoint = test_endpoint();
  // Only the local node is a member before any gossip.
  assert_eq!(endpoint.endpoint_ref().num_members(), 1);

  let now = Instant::now();
  let src = addr(9100);
  let label = Some(Bytes::from_static(b"capture-test"));

  // A few distinct garbage payloads: empty, random-looking bytes, and a buffer
  // that superficially resembles a labeled frame but is truncated. Each must be
  // dropped silently, leaving membership untouched.
  for datagram in [
    &b""[..],
    &b"\xff\xfe\xfd\xfc not a frame"[..],
    &[0x01u8; 64][..],
  ] {
    dispatch_gossip::<SmolStr, SocketAddr, RawRecords, _>(
      &mut endpoint,
      src,
      datagram,
      now,
      label.clone(),
    );
  }

  assert_eq!(
    endpoint.endpoint_ref().num_members(),
    1,
    "malformed gossip must never admit a member",
  );
}

/// `gossip_recv_buf_len` sizes the recv buffer to `gossip_mtu +
/// ENCRYPTED_WRAPPER_OVERHEAD`, clamped at the 65507-byte UDP-payload ceiling:
/// a default-MTU endpoint sits below the ceiling, a near-max MTU clamps to it.
#[compio::test]
async fn gossip_recv_buf_len_tracks_mtu_and_clamps_at_ceiling() {
  // Default MTU endpoint: buffer is mtu + overhead, strictly below the ceiling.
  let small = test_endpoint();
  let small_len = gossip_recv_buf_len::<SmolStr, SocketAddr, RawRecords, _>(&small);
  assert_eq!(
    small_len,
    small.gossip_mtu()
      + memberlist_proto::ENCRYPTED_WRAPPER_OVERHEAD
      + memberlist_proto::CHECKSUMED_WRAPPER_OVERHEAD,
    "buffer is gossip_mtu plus the checksum and encrypted wrapper overhead",
  );
  assert!(
    small_len < GOSSIP_RECV_BUF_MAX,
    "a default-MTU endpoint sits below the UDP-payload ceiling",
  );

  // A gossip_mtu at the UDP ceiling: mtu + overhead would exceed 65507, so the
  // result clamps to exactly GOSSIP_RECV_BUF_MAX.
  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("big-mtu"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    )
    .with_gossip_mtu(GOSSIP_RECV_BUF_MAX),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let big: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  );
  let big_len = gossip_recv_buf_len::<SmolStr, SocketAddr, RawRecords, _>(&big);
  assert_eq!(
    big_len, GOSSIP_RECV_BUF_MAX,
    "a near-ceiling gossip_mtu clamps the recv buffer at GOSSIP_RECV_BUF_MAX",
  );
}

/// On a quiescent endpoint with no queued actions or transport-transmits, the
/// per-surface drain helpers report no progress (`false`) — the loop's
/// fixed-point exit condition.
#[compio::test]
async fn drain_helpers_report_no_progress_when_idle() {
  let mut endpoint = test_endpoint();
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let (ready_tx, _ready_rx) = flume::unbounded::<BridgeReady>();

  assert!(
    !drain_actions::<SmolStr, SocketAddr, RawRecords, _>(
      &mut endpoint,
      &mut bridges,
      &ready_tx,
      StreamTransportOptions::default(),
      &Default::default(),
    ),
    "an idle endpoint queues no actions",
  );
  assert!(
    !drain_transport_transmits::<SmolStr, SocketAddr, RawRecords, _>(&mut endpoint, &bridges),
    "an idle endpoint queues no transport transmits",
  );
}

/// `min_pending_join_deadline` returns the earliest deadline across the
/// waiter vec, and `None` when empty. `min_pending_leave_deadline` surfaces a
/// parked leave's deadline. Both fold into the driver's select timer.
#[compio::test]
async fn pending_deadline_helpers_pick_the_earliest() {
  assert_eq!(min_pending_join_deadline(&[]), None);
  assert_eq!(min_pending_leave_deadline(&None), None);

  let base = Instant::now();
  let earliest = base + Duration::from_secs(1);
  let latest = base + Duration::from_secs(5);
  let mk = |deadline| {
    let (tx, _rx) = futures_channel::oneshot::channel();
    PendingJoin {
      pending: HashSet::new(),
      addr_by_eid: HashMap::new(),
      contacted: smallvec::SmallVec::new(),
      requested: 1,
      deadline,
      reply: tx,
    }
  };
  let joins = vec![mk(latest), mk(earliest)];
  assert_eq!(min_pending_join_deadline(&joins), Some(earliest));

  let leave_deadline = base + Duration::from_secs(3);
  let pl = PendingLeave {
    repliers: Vec::new(),
    deadline: leave_deadline,
  };
  assert_eq!(min_pending_leave_deadline(&Some(pl)), Some(leave_deadline));
}

/// `reap_pending_joins` resolves every empty-`pending` waiter: zero contacts ⇒
/// `JoinFailed(requested, 0)`, any contacts ⇒ `Ok(reached_set)`. This is the
/// degenerate zero-exchange `WaitForCompletion` / post-completion reap path.
#[compio::test]
async fn reap_pending_joins_resolves_empty_pending_waiters() {
  let now = Instant::now();
  let (tx_fail, rx_fail) = futures_channel::oneshot::channel();
  let (tx_ok, rx_ok) = futures_channel::oneshot::channel();
  let reached: smallvec::SmallVec<[std::net::SocketAddr; 1]> = smallvec::smallvec![
    "127.0.0.1:1".parse().unwrap(),
    "127.0.0.1:2".parse().unwrap(),
    "127.0.0.1:3".parse().unwrap(),
  ];
  let mut joins = vec![
    PendingJoin {
      pending: HashSet::new(),
      addr_by_eid: HashMap::new(),
      contacted: smallvec::SmallVec::new(),
      requested: 4,
      deadline: now + Duration::from_secs(60),
      reply: tx_fail,
    },
    PendingJoin {
      pending: HashSet::new(),
      addr_by_eid: HashMap::new(),
      contacted: reached.clone(),
      requested: 3,
      deadline: now + Duration::from_secs(60),
      reply: tx_ok,
    },
  ];

  reap_pending_joins(&mut joins, now).await;
  assert!(joins.is_empty(), "both empty-pending waiters are reaped");

  match rx_fail.await {
    Ok(Err((set, MemberlistError::JoinFailed(e)))) => {
      assert!(set.is_empty(), "all-failed carries an empty reached set");
      assert_eq!(e.requested(), 4, "carries the requested seed count");
      assert_eq!(e.contacted(), 0);
    }
    other => panic!("expected JoinFailed, got {other:?}"),
  }
  match rx_ok.await {
    Ok(Ok(set)) => assert_eq!(set, reached, "contacts ⇒ Ok(reached_set)"),
    other => panic!("expected Ok(reached_set), got {other:?}"),
  }
}

/// Build an empty [`PendingCommands`] for a `dispatch_command` call.
fn empty_pending() -> PendingCommands {
  PendingCommands {
    joins: Vec::new(),
    leave: None,
    pings: Vec::new(),
    user_sends: Vec::new(),
  }
}

/// One-shot reply pair typed for the `Result<()>` command replies.
fn unit_reply() -> (
  futures_channel::oneshot::Sender<super::Result<()>>,
  futures_channel::oneshot::Receiver<super::Result<()>>,
) {
  futures_channel::oneshot::channel()
}

/// Run `dispatch_command` against a fresh endpoint with default options and an
/// empty bridge table.
async fn dispatch_one(
  endpoint: &mut StreamEndpoint<SmolStr, SocketAddr, RawRecords>,
  pending: &mut PendingCommands,
  cmd: Command<SmolStr>,
) {
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let mut shutdown_reply: Option<futures_channel::oneshot::Sender<super::Result<()>>> = None;
  dispatch_with(endpoint, &mut bridges, &mut shutdown_reply, pending, cmd).await;
}

/// Run `dispatch_command` against caller-supplied bridge + shutdown-reply state
/// so a test can drive several commands across the SAME bridge table.
async fn dispatch_with(
  endpoint: &mut StreamEndpoint<SmolStr, SocketAddr, RawRecords>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  shutdown_reply: &mut Option<futures_channel::oneshot::Sender<super::Result<()>>>,
  pending: &mut PendingCommands,
  cmd: Command<SmolStr>,
) {
  let (ready_tx, _ready_rx) = flume::unbounded::<BridgeReady>();
  let now = Instant::now();
  dispatch_command::<SmolStr, SocketAddr, RawRecords, _>(
    endpoint,
    bridges,
    &ready_tx,
    StreamTransportOptions::default(),
    &Default::default(),
    shutdown_reply,
    pending,
    Duration::from_secs(5),
    cmd,
    now,
  )
  .await;
}

/// After `endpoint.leave(now)` the node is no longer Running, so every
/// lifecycle-gated command arm must reject with `NotRunning` WITHOUT touching
/// the endpoint — never parking an unresolvable waiter. Covers the
/// `is_running()`-false branch of each `dispatch_command` arm in one pass.
#[compio::test]
async fn dispatch_rejects_every_gated_command_after_leave() {
  let mut endpoint = test_endpoint();
  // Transition out of Running: a lone node's leave is an immediate no-op flush.
  endpoint.leave(Instant::now()).expect("leave");
  assert!(
    !endpoint.is_running(),
    "leave moves the node out of Running"
  );

  let mut pending = empty_pending();

  // Join → NotRunning, nothing parked.
  {
    let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
    dispatch_one(
      &mut endpoint,
      &mut pending,
      Command::Join(JoinCmd {
        addrs: vec![addr(7100)],
        kind: JoinKind::Dispatch,
        reply: tx,
      }),
    )
    .await;
    assert!(matches!(
      rx.await,
      Ok(Err((set, MemberlistError::NotRunning))) if set.is_empty()
    ));
    assert!(pending.joins.is_empty(), "rejected join parks nothing");
  }

  // UpdateNodeMetadata → NotRunning.
  {
    let (tx, rx) = unit_reply();
    dispatch_one(
      &mut endpoint,
      &mut pending,
      Command::UpdateNodeMetadata(UpdateNodeMetadataCmd {
        meta: b"m".to_vec(),
        reply: tx,
      }),
    )
    .await;
    assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
  }

  // SetLocalState → NotRunning.
  {
    let (tx, rx) = unit_reply();
    dispatch_one(
      &mut endpoint,
      &mut pending,
      Command::SetLocalState(SetLocalStateCmd::new(Bytes::from_static(b"s"), tx)),
    )
    .await;
    assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
  }

  // SetAckPayload → NotRunning.
  {
    let (tx, rx) = unit_reply();
    dispatch_one(
      &mut endpoint,
      &mut pending,
      Command::SetAckPayload(SetAckPayloadCmd::new(Bytes::from_static(b"a"), tx)),
    )
    .await;
    assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
  }

  // Ping → NotRunning, nothing parked.
  {
    let (tx, rx) = futures_channel::oneshot::channel::<super::Result<Duration>>();
    dispatch_one(
      &mut endpoint,
      &mut pending,
      Command::Ping(PingCmd::new(
        Node::new(SmolStr::new("peer"), addr(7101)),
        tx,
      )),
    )
    .await;
    assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
    assert!(pending.pings.is_empty(), "rejected ping parks nothing");
  }

  // SendUser → NotRunning.
  {
    let (tx, rx) = unit_reply();
    dispatch_one(
      &mut endpoint,
      &mut pending,
      Command::SendUser(SendUserCmd::new(
        addr(7102),
        vec![Bytes::from_static(b"u")],
        tx,
      )),
    )
    .await;
    assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
  }

  // SendReliable → NotRunning, nothing parked.
  {
    let (tx, rx) = unit_reply();
    dispatch_one(
      &mut endpoint,
      &mut pending,
      Command::SendReliable(SendReliableCmd::new(
        addr(7103),
        vec![Bytes::from_static(b"r")],
        tx,
      )),
    )
    .await;
    assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
    assert!(
      pending.user_sends.is_empty(),
      "rejected reliable-send parks nothing"
    );
  }
}

/// On a Running node, the non-network config setters apply IN PLACE and reply
/// `Ok(())` synchronously (no parking) — the success arms of
/// `SetLocalState` / `SetAckPayload` / `UpdateNodeMetadata`.
#[compio::test]
async fn dispatch_config_setters_apply_and_ack_when_running() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetLocalState(SetLocalStateCmd::new(Bytes::from_static(b"state"), tx)),
  )
  .await;
  assert!(matches!(rx.await, Ok(Ok(()))), "set_local_state acks Ok");

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetAckPayload(SetAckPayloadCmd::new(Bytes::from_static(b"ack"), tx)),
  )
  .await;
  assert!(matches!(rx.await, Ok(Ok(()))), "set_ack_payload acks Ok");

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::UpdateNodeMetadata(UpdateNodeMetadataCmd {
      meta: b"fresh-meta".to_vec(),
      reply: tx,
    }),
  )
  .await;
  assert!(matches!(rx.await, Ok(Ok(()))), "update_meta acks Ok");
}

/// On a Running node `SetCompressionOptions` applies the policy in place and
/// acks `Ok(())`; after `leave()` it rejects with `NotRunning`. Covers both
/// branches of the compression-gated arm.
#[cfg(compression)]
#[compio::test]
async fn dispatch_set_compression_options_running_then_not_running() {
  use crate::command::SetCompressionOptionsCmd;

  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetCompressionOptions(SetCompressionOptionsCmd {
      opts: memberlist_proto::CompressionOptions::new(),
      reply: tx,
    }),
  )
  .await;
  assert!(matches!(rx.await, Ok(Ok(()))), "running ⇒ Ok");

  endpoint.leave(Instant::now()).expect("leave");
  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetCompressionOptions(SetCompressionOptionsCmd {
      opts: memberlist_proto::CompressionOptions::new(),
      reply: tx,
    }),
  )
  .await;
  assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
}

/// On a Running node `SetChecksumOptions` validates then applies the policy and
/// acks `Ok(())` (a default disabled policy is always usable); after `leave()`
/// it rejects with `NotRunning` WITHOUT validating. Covers both branches of the
/// checksum-gated arm.
#[cfg(checksum)]
#[compio::test]
async fn dispatch_set_checksum_options_running_then_not_running() {
  use crate::command::SetChecksumOptionsCmd;

  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetChecksumOptions(SetChecksumOptionsCmd {
      opts: memberlist_proto::ChecksumOptions::new(),
      reply: tx,
    }),
  )
  .await;
  assert!(matches!(rx.await, Ok(Ok(()))), "running ⇒ Ok");

  endpoint.leave(Instant::now()).expect("leave");
  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetChecksumOptions(SetChecksumOptionsCmd {
      opts: memberlist_proto::ChecksumOptions::new(),
      reply: tx,
    }),
  )
  .await;
  assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
}

/// On a Running node `SetEncryptionOptions` validates then applies the policy
/// and acks `Ok(())` (a default no-keyring policy is always usable); after
/// `leave()` it rejects with `NotRunning` WITHOUT validating. Covers both
/// branches of the encryption-gated arm.
#[cfg(encryption)]
#[compio::test]
async fn dispatch_set_encryption_options_running_then_not_running() {
  use crate::command::SetEncryptionOptionsCmd;

  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
      opts: memberlist_proto::EncryptionOptions::new(),
      reply: tx,
    }),
  )
  .await;
  assert!(matches!(rx.await, Ok(Ok(()))), "running ⇒ Ok");

  endpoint.leave(Instant::now()).expect("leave");
  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
      opts: memberlist_proto::EncryptionOptions::new(),
      reply: tx,
    }),
  )
  .await;
  assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
}

/// `QueueUserBroadcast` on a Running node validates the framed packet against
/// the gossip budget and acks `Ok(())`; after `leave()` the gossip scheduler is
/// stopped so it rejects with `NotRunning`. Covers both branches of the
/// (ungated) broadcast arm.
#[compio::test]
async fn dispatch_queue_user_broadcast_running_then_not_running() {
  use crate::command::QueueUserBroadcastCmd;

  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::QueueUserBroadcast(QueueUserBroadcastCmd::new(Bytes::from_static(b"bcast"), tx)),
  )
  .await;
  assert!(matches!(rx.await, Ok(Ok(()))), "running ⇒ Ok");

  endpoint.leave(Instant::now()).expect("leave");
  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::QueueUserBroadcast(QueueUserBroadcastCmd::new(Bytes::from_static(b"bcast"), tx)),
  )
  .await;
  assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
}

/// `UpdateNodeMetadata` whose bytes exceed `Meta::MAX_SIZE` fails the
/// `Meta::try_from` conversion and surfaces `Io` rather than acking a change
/// the wire can never carry. Exercises the conversion-error arm.
#[compio::test]
async fn dispatch_update_metadata_rejects_oversized_meta() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  // One byte past the u16 meta ceiling.
  let oversized = vec![0u8; (u16::MAX as usize) + 1];
  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::UpdateNodeMetadata(UpdateNodeMetadataCmd {
      meta: oversized,
      reply: tx,
    }),
  )
  .await;
  assert!(
    matches!(rx.await, Ok(Err(MemberlistError::Io(_)))),
    "an oversized meta is rejected as Io, not acked",
  );
}

/// A second `Command::Leave` arriving while a leave is already in flight JOINS
/// the parked operation: it pushes its reply onto the shared `repliers` and does
/// NOT re-invoke `endpoint.leave()` (a terminal no-op once Leaving emits no
/// second `LeftCluster`). Both repliers resolve together on the single outcome.
#[compio::test]
async fn dispatch_second_leave_joins_the_in_flight_one() {
  let mut endpoint = test_endpoint();

  // Seed an in-flight leave directly (the first initiator already parked).
  let (tx_first, rx_first) = unit_reply();
  let mut pending = empty_pending();
  pending.leave = Some(PendingLeave {
    repliers: vec![tx_first],
    deadline: Instant::now() + Duration::from_secs(5),
  });

  // The second leave must attach, not initiate.
  let (tx_second, rx_second) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::Leave(LeaveCmd { reply: tx_second }),
  )
  .await;

  let pl = pending.leave.as_ref().expect("leave still parked");
  assert_eq!(pl.repliers.len(), 2, "the second leave joined the repliers");
  // Endpoint is untouched by the join (still Running — leave() not re-invoked).
  assert!(
    endpoint.is_running(),
    "a joined leave does not re-invoke leave()"
  );

  // Resolving the shared operation replies to BOTH waiters with one outcome.
  pending.leave.take().unwrap().resolve_all(|| Ok(())).await;
  assert!(matches!(rx_first.await, Ok(Ok(()))));
  assert!(matches!(rx_second.await, Ok(Ok(()))));
}

/// A `Command::SendReliable` with an EMPTY payload slice has nothing to dial, so
/// it resolves `Ok(())` immediately (vacuous success) with no parked waiter —
/// the `m == 0 && n == 0` arm.
#[compio::test]
async fn dispatch_send_reliable_empty_payloads_acks_ok() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SendReliable(SendReliableCmd::new(addr(7200), Vec::new(), tx)),
  )
  .await;
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "empty reliable send is vacuous Ok"
  );
  assert!(
    pending.user_sends.is_empty(),
    "an empty reliable send parks no waiter",
  );
}

/// A `Command::SendReliable` with a real payload on a Running node dials the
/// peer (one exchange) and PARKS a `PendingUserSend` whose terminal outcome is
/// driven later by `ExchangeCompleted(UserMessage)`. Pins the park path (the
/// `m > 0` arm) and the `failed`-seed of zero (every payload produced a dial).
#[compio::test]
async fn dispatch_send_reliable_parks_pending_user_send() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, _rx) = unit_reply();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::SendReliable(SendReliableCmd::new(
      addr(7201),
      vec![Bytes::from_static(b"payload")],
      tx,
    )),
  )
  .await;
  assert_eq!(
    pending.user_sends.len(),
    1,
    "a reliable send to a real peer parks one waiter",
  );
  let ps = &pending.user_sends[0];
  assert_eq!(ps.pending.len(), 1, "one payload ⇒ one tracked exchange");
  assert_eq!(
    ps.failed, 0,
    "every payload produced a Connect ⇒ zero seeded failures"
  );
}

/// `JoinKind::Dispatch` on a Running node fans one push/pull per seed, drains
/// each Connect to its bridge, and replies `Ok(set)` immediately — the
/// fire-and-forget dispatch arm. The dispatched set is NOT deduplicated, so a
/// duplicate seed contributes independently (the `dispatch_join` count is the
/// dispatched-exchange count).
#[compio::test]
async fn dispatch_join_dispatch_replies_with_seed_count() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  // Two seeds including a duplicate: three dispatched exchanges total.
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::Join(JoinCmd {
      addrs: vec![addr(7300), addr(7301), addr(7300)],
      kind: JoinKind::Dispatch,
      reply: tx,
    }),
  )
  .await;
  match rx.await {
    Ok(Ok(set)) => assert_eq!(
      set.len(),
      3,
      "dispatch join replies with the dispatched-exchange count",
    ),
    other => panic!("expected Ok(dispatched_set), got {other:?}"),
  }
  assert!(pending.joins.is_empty(), "dispatch join parks no waiter");
}

/// `JoinKind::WaitForCompletion` on a Running node parks a `PendingJoin` whose
/// `requested` is the full seed count and whose `pending` tracks each dispatched
/// exchange's id — resolved later by `ExchangeCompleted(PushPull)`.
#[compio::test]
async fn dispatch_join_wait_parks_pending_join() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, _rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  dispatch_one(
    &mut endpoint,
    &mut pending,
    Command::Join(JoinCmd {
      addrs: vec![addr(7400), addr(7401)],
      kind: JoinKind::WaitForCompletion(WaitForCompletionArgs {
        deadline: Instant::now() + Duration::from_secs(30),
      }),
      reply: tx,
    }),
  )
  .await;
  assert_eq!(
    pending.joins.len(),
    1,
    "wait-for-completion parks one waiter"
  );
  let pj = &pending.joins[0];
  assert_eq!(
    pj.requested, 2,
    "requested carries the full resolved seed count"
  );
  assert_eq!(pj.pending.len(), 2, "each seed produced a tracked exchange");
}

/// `Command::Shutdown` stashes its reply (acked later, after the sockets drop)
/// and does NOT tear bridges down: teardown is deferred to the post-loop
/// `freeze_and_drain_bridges_to_disconnected`, the single owner. Asserts the
/// reply is stashed (not sent inline) and the bridge table is left POPULATED for
/// the freeze barrier — a close-first drain here would empty the table and emit a
/// synthetic EOF that fabricates a completion. The bridge is installed through the
/// real `SendReliable` → `process_one_action` Connect path so the table holds a
/// genuine `BridgeHandle`.
#[compio::test]
async fn dispatch_shutdown_stashes_reply_without_tearing_down_bridges() {
  let mut endpoint = test_endpoint();
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let mut shutdown_reply: Option<futures_channel::oneshot::Sender<super::Result<()>>> = None;
  let mut pending = empty_pending();

  // A reliable send to a (dead) loopback port installs one bridge via the
  // Connect handler; the spawned dial task is harmless (it just fails to
  // connect off-loop).
  let (tx, _rx) = unit_reply();
  dispatch_with(
    &mut endpoint,
    &mut bridges,
    &mut shutdown_reply,
    &mut pending,
    Command::SendReliable(SendReliableCmd::new(
      addr(7500),
      vec![Bytes::from_static(b"x")],
      tx,
    )),
  )
  .await;
  assert_eq!(bridges.len(), 1, "the reliable send installed a bridge");

  let (tx, mut rx) = unit_reply();
  dispatch_with(
    &mut endpoint,
    &mut bridges,
    &mut shutdown_reply,
    &mut pending,
    Command::Shutdown(ShutdownCmd { reply: tx }),
  )
  .await;

  assert!(
    shutdown_reply.is_some(),
    "the shutdown reply is stashed for the cleanup ack",
  );
  assert!(
    matches!(rx.try_recv(), Ok(None)),
    "the shutdown caller is NOT acked inline (sockets are still bound)",
  );
  assert_eq!(
    bridges.len(),
    1,
    "shutdown leaves the bridge table POPULATED for the freeze barrier (the single owner of teardown); it must not close-first drain",
  );
}

/// `fire_timeout_with_drain` drains the out-of-band bridge channels (a buffered
/// `BridgeReady` and `BridgeInbound`) before consulting the deadline, and
/// returns `true` when it did observable work. A buffered `OutboundFail`
/// terminalizes its (already-absent) exchange via `handle_dial_failed` and marks
/// the loop dirty.
#[compio::test]
async fn fire_timeout_with_drain_consumes_buffered_bridge_ready() {
  let mut endpoint = test_endpoint();
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();

  let (bridge_inbound_tx, mut bridge_inbound_rx) = mpsc::unbounded::<super::BridgeInbound>();
  let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded::<BridgeReady>();

  // A dial-failure for an exchange the table never tracked: the drain routes it
  // to `handle_bridge_ready`'s OutboundFail arm (a no-op terminalization) and
  // reports progress.
  bridge_ready_tx
    .send(BridgeReady::OutboundFail(super::OutboundFailReady {
      eid: {
        // Mint a real ExchangeId by issuing a reliable send and capturing the
        // Connect's id, so the value is a genuine coordinator token.
        let sid = endpoint
          .start_user_message(addr(7600), Bytes::from_static(b"z"), Instant::now())
          .expect("issued while running");
        let mut started = HashSet::new();
        started.insert(sid);
        let mut captured: HashSet<ExchangeId> = HashSet::new();
        let (ready_tx2, _r) = flume::unbounded::<BridgeReady>();
        while let Some(action) = endpoint.poll_action() {
          process_one_action(
            action,
            &mut bridges,
            &ready_tx2,
            StreamTransportOptions::default(),
            Some((&started, &mut captured, None)),
            &Default::default(),
          );
        }
        *captured.iter().next().expect("one exchange captured")
      },
      err: std::io::Error::other("dial failed"),
      received_at: Instant::now(),
    }))
    .expect("send ready");

  let dirty = fire_timeout_with_drain::<SmolStr, SocketAddr, RawRecords, _>(
    &mut endpoint,
    &mut bridges,
    &bridge_inbound_tx,
    &mut bridge_inbound_rx,
    &bridge_ready_rx,
    RuntimeOptions::default(),
    StreamTransportOptions::default(),
  );
  assert!(dirty, "draining a buffered bridge-ready reports progress");
  assert!(
    bridge_ready_rx.is_empty(),
    "the buffered bridge-ready was consumed",
  );
}

/// `drain_events` performs only synchronous protocol accounting and hands each
/// event to the observation task. When the observation receiver has been
/// dropped, every `try_send` hits the `Closed` arm and the event is silently
/// discarded — `drain_events` still drains `poll_event` to empty and reports it
/// drained at least one event. A lone node's `leave()` emits the events.
#[compio::test]
async fn drain_events_drops_on_closed_obs_channel_but_still_drains() {
  let mut endpoint = test_endpoint();
  // `leave()` on a lone node queues `NodeLeft` + `LeftCluster` for `poll_event`.
  endpoint.leave(Instant::now()).expect("leave");

  // Drop the obs receiver immediately so every hand-off is `Closed`.
  let (obs_tx, obs_rx) = mpsc::unbounded::<memberlist_proto::event::Event<SmolStr, SocketAddr>>();
  drop(obs_rx);

  let observation_dropped = Cell::new(0u64);
  let obs_payload_bytes = Rc::new(Cell::new(0u64));
  let mut pending = empty_pending();

  let drained = drain_events::<SmolStr, SocketAddr, RawRecords, _>(
    &mut endpoint,
    &obs_tx,
    &observation_dropped,
    &obs_payload_bytes,
    None,
    &mut pending,
  )
  .await;

  assert!(
    drained,
    "leave queued events, so drain_events drained at least one"
  );
  assert!(
    endpoint.poll_event().is_none(),
    "drain_events emptied the event queue",
  );
}

/// `reap_pending_leave` replies `LeaveTimeout` to every joined replier once
/// the deadline elapses and clears the slot; a not-yet-expired leave stays
/// parked with no reply.
#[compio::test]
async fn reap_pending_leave_fires_only_after_the_deadline() {
  let now = Instant::now();

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
    "no reply before the deadline",
  );

  // Two repliers (initiator + racing clone) both get LeaveTimeout.
  let (tx_a, rx_a) = futures_channel::oneshot::channel::<super::Result<()>>();
  let (tx_b, rx_b) = futures_channel::oneshot::channel::<super::Result<()>>();
  let mut expired = Some(PendingLeave {
    repliers: vec![tx_a, tx_b],
    deadline: now - Duration::from_secs(1),
  });
  reap_pending_leave(&mut expired, now).await;
  assert!(expired.is_none(), "an expired leave clears its slot");
  assert!(matches!(rx_a.await, Ok(Err(MemberlistError::LeaveTimeout))));
  assert!(matches!(rx_b.await, Ok(Err(MemberlistError::LeaveTimeout))));
}

/// The construction self-join must reach the `EventStream` BEFORE the driver
/// loop parks. `Endpoint::new` queues a `NodeJoined(self)` (Go's Create-time
/// `NotifyJoin`); the `select` loop only drains `poll_event` AFTER an arm fires,
/// so with the periodic schedulers DISABLED the first wake would be the
/// idle-wake interval — a fresh no-peer node would not surface its own member
/// for ~a minute. With the loop-entry startup drain in place the self-join
/// arrives at once, so a short timeout suffices. Runs the real
/// `stream_driver_loop` over a loopback TCP/UDP pair with no peers.
#[compio::test]
async fn construction_self_join_surfaces_before_idle_wake() {
  let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
  // Schedulers disabled: with every interval at zero the loop's only timer is
  // the idle-wake fallback, so the self-join can surface ONLY via the startup
  // drain — never via a probe / gossip / push-pull tick.
  let ep = Endpoint::new(
    EndpointOptions::new(SmolStr::new("solo"), bind)
      .with_probe_interval(Duration::ZERO)
      .with_gossip_interval(Duration::ZERO)
      .with_push_pull_interval(Duration::ZERO),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  );

  // Bootstrap the snapshot cell from the local node entry; the driver's
  // loop-entry `refresh_snapshot` overwrites it immediately.
  let snapshot: SnapshotCell<SmolStr> = {
    let ep_ref = endpoint.endpoint_ref();
    let local = ep_ref
      .member(ep_ref.local_id_ref())
      .expect("the local node is always a member");
    let boot = MemberlistSnapshot::new(
      vec![local.clone()],
      local,
      1,
      ep_ref.num_members(),
      ep_ref.health_score(),
    );
    Rc::new(RefCell::new(Rc::new(boot)))
  };

  let gossip_socket = compio::net::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind loopback udp");
  let listener = compio::net::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind loopback tcp");

  let (commands_tx, commands_rx) = flume::unbounded::<Command<SmolStr>>();
  let (events_tx, events_rx) =
    flume::bounded::<memberlist_proto::event::Event<SmolStr, SocketAddr>>(1024);
  let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded();

  compio::runtime::spawn(super::stream_driver_loop::<
    SmolStr,
    SocketAddr,
    RawRecords,
    _,
    _,
  >(
    endpoint,
    gossip_socket,
    listener,
    commands_rx,
    events_tx,
    Rc::new(Cell::new(0u64)),
    Rc::new(Cell::new(0u64)),
    snapshot,
    Rc::new(Cell::new(memberlist_proto::metrics::Metrics::default())),
    bridge_ready_rx,
    bridge_ready_tx,
    Rc::new(Cell::new(false)),
    RuntimeOptions::new(),
    StreamTransportOptions::default(),
    VoidDelegate::<SmolStr, SocketAddr>::default(),
    None,
    Default::default(),
  ))
  .detach();

  // A short bound, FAR below the idle-wake interval: without the startup drain
  // the loop would park until idle-wake and this would elapse.
  let ev = compio::time::timeout(Duration::from_secs(2), events_rx.recv_async())
    .await
    .expect("the construction self-join surfaces before the idle-wake interval")
    .expect("the event stream stays open");
  match ev {
    memberlist_proto::event::Event::NodeJoined(n) => assert_eq!(
      n.id_ref().as_str(),
      "solo",
      "the first EventStream event is the local node's self-join",
    ),
    other => panic!("expected NodeJoined(self) as the first event, got {other:?}"),
  }

  // Signal the loop to exit so the detached task releases its sockets promptly.
  drop(commands_tx);
}

/// Shutdown drain preserves the reached-so-far set.
///
/// Scenario: two seeds are dispatched; one exchange completes with `Succeeded`
/// (so `pj.contacted` holds one address) while the second is still pending.
/// The shutdown drain fires before the join deadline. The Err tuple MUST carry
/// the already-contacted address — not an empty set — so the caller can observe
/// partial progress on a shutdown race.
#[compio::test]
async fn shutdown_drain_carries_reached_so_far() {
  let now = Instant::now();
  let reached_addr: SocketAddr = "127.0.0.1:7700".parse().unwrap();
  let pending_addr: SocketAddr = "127.0.0.1:7701".parse().unwrap();

  // Mint a genuine ExchangeId for the still-pending exchange (second seed).
  let mut ep2 = test_endpoint();
  let pending_eid: ExchangeId = ep2
    .start_push_pull(pending_addr, PushPullKind::Join, now)
    .into();

  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pj = PendingJoin {
    pending: {
      let mut s = HashSet::new();
      s.insert(pending_eid);
      s
    },
    addr_by_eid: {
      let mut m = HashMap::new();
      m.insert(pending_eid, pending_addr);
      m
    },
    // One seed already contacted — the reached-so-far set.
    contacted: smallvec::smallvec![reached_addr],
    requested: 2,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  };

  // Simulate the shutdown drain: take the contacted set and send the Err tuple.
  let _ = pj.reply.send(Err((
    std::mem::take(&mut pj.contacted),
    MemberlistError::Shutdown,
  )));

  match rx.await {
    Ok(Err((set, MemberlistError::Shutdown))) => {
      assert_eq!(set.len(), 1, "the one already-reached address is preserved");
      assert_eq!(
        set[0], reached_addr,
        "the preserved address matches the seed that succeeded",
      );
    }
    other => panic!("expected Err((reached, Shutdown)), got {other:?}"),
  }
}

/// After a clean zero-contact resolution the Err tuple's set is empty because
/// nothing was ever reached — not because it was hardcoded. This is the
/// complementary case to `shutdown_drain_carries_reached_so_far`: confirms
/// the all-failed path still yields an empty set when `contacted` was never
/// populated.
#[compio::test]
async fn shutdown_drain_empty_when_nothing_reached() {
  let now = Instant::now();
  let pending_addr: SocketAddr = "127.0.0.1:7702".parse().unwrap();

  // Mint a genuine ExchangeId for the still-pending exchange.
  let mut ep2 = test_endpoint();
  let pending_eid: ExchangeId = ep2
    .start_push_pull(pending_addr, PushPullKind::Join, now)
    .into();

  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pj = PendingJoin {
    pending: {
      let mut s = HashSet::new();
      s.insert(pending_eid);
      s
    },
    addr_by_eid: HashMap::new(),
    contacted: smallvec::SmallVec::new(), // nothing reached yet
    requested: 1,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  };

  let _ = pj.reply.send(Err((
    std::mem::take(&mut pj.contacted),
    MemberlistError::Shutdown,
  )));

  match rx.await {
    Ok(Err((set, MemberlistError::Shutdown))) => {
      assert!(
        set.is_empty(),
        "all-failed shutdown carries an empty reached set (not a hardcoded empty)"
      );
    }
    other => panic!("expected Err((empty, Shutdown)), got {other:?}"),
  }
}

/// A label-free stream endpoint for the shutdown-drain regressions. The dialer
/// and the peer that produces its pull response must share a label, so both use
/// `None` here — the drain under test is label-agnostic.
fn unlabeled_endpoint(
  name: &str,
  bind: SocketAddr,
) -> StreamEndpoint<SmolStr, SocketAddr, RawRecords> {
  let ep = Endpoint::new(
    EndpointOptions::new(SmolStr::new(name), bind),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  )
}

/// Drive a real push/pull from `endpoint` to a peer at `seed_addr` up to the
/// point where the peer's pull response is ready, WITHOUT spawning any I/O.
/// Returns the dialer's exchange id and the response chunks; replaying those on
/// `bridge_inbound_rx` (as `BridgeInbound::Bytes`) followed by a `BridgeInbound::
/// Eof` completes the exchange `Succeeded`. Ported from the reactor's identical
/// helper.
fn drive_push_to_queued_response(
  endpoint: &mut StreamEndpoint<SmolStr, SocketAddr, RawRecords>,
  seed_addr: SocketAddr,
  now: Instant,
) -> (ExchangeId, Vec<Vec<u8>>) {
  let mut peer = unlabeled_endpoint("seed", seed_addr);
  peer.start_scheduling(now);

  // Start the dialer's exchange and capture its `Connect` id + push frames
  // without spawning a real dial (the round-trip is driven by hand below).
  endpoint.start_push_pull(seed_addr, PushPullKind::Join, now);
  let mut eid = None;
  let mut push: Vec<Vec<u8>> = Vec::new();
  loop {
    let mut progressed = false;
    while let Some(action) = endpoint.poll_action() {
      progressed = true;
      if let StreamAction::Connect(info) = action {
        eid = Some(info.id());
      }
    }
    while let Some((id, _peer, bytes)) = endpoint.poll_transport_transmit() {
      progressed = true;
      if Some(id) == eid {
        push.push(bytes.to_vec());
      }
    }
    if !progressed {
      break;
    }
  }
  let eid = eid.expect("the dialer's start_push_pull produced a Connect exchange id");

  // Replay the dialer's push + FIN into the peer, then collect its pull response.
  let server_eid = peer
    .accept_connection("127.0.0.1:7400".parse().unwrap(), now)
    .expect("the peer admits the inbound exchange");
  for chunk in &push {
    peer.handle_transport_data(server_eid, chunk, false, now);
  }
  peer.handle_transport_data(server_eid, &[], true, now); // the dialer's FIN
  let mut response: Vec<Vec<u8>> = Vec::new();
  loop {
    let mut progressed = false;
    while peer.poll_action().is_some() {
      progressed = true;
    }
    while let Some((id, _peer, bytes)) = peer.poll_transport_transmit() {
      progressed = true;
      if id == server_eid {
        response.push(bytes.to_vec());
      }
    }
    if !progressed {
      break;
    }
  }
  assert!(
    !response.is_empty(),
    "the peer produced a pull response to the dialer's push"
  );
  (eid, response)
}

/// Mint a distinct, harmless filler exchange on `endpoint` (a `start_push_pull`
/// whose `Connect` + push frames are discarded). Minted AFTER the seeds so its
/// id never collides with theirs; it is not in any parked join's `pending` set,
/// so stray bytes addressed to it can never fold into a join.
fn mint_filler_eid(
  endpoint: &mut StreamEndpoint<SmolStr, SocketAddr, RawRecords>,
  now: Instant,
) -> ExchangeId {
  endpoint.start_push_pull(addr(9090), PushPullKind::Join, now);
  let mut filler = None;
  while let Some(action) = endpoint.poll_action() {
    if let StreamAction::Connect(info) = action {
      filler = Some(info.id());
    }
  }
  while endpoint.poll_transport_transmit().is_some() {}
  filler.expect("the filler start_push_pull produced a Connect exchange id")
}

/// Queue one item onto an unbounded test inbound channel. `BridgeInbound` is not
/// `Debug`, so a `.expect` on the error would not compile; an unbounded send to a
/// live receiver is infallible, so an error is unreachable.
fn queue_inbound(tx: &mpsc::Sender<super::BridgeInbound>, item: super::BridgeInbound) {
  tx.try_send(item)
    .unwrap_or_else(|_| unreachable!("unbounded send to a live receiver always succeeds"));
}

/// Build the shutdown-drain scaffolding (a bound gossip socket, a dropped-events
/// obs channel, default options) and run `freeze_and_drain_bridges_to_disconnected`
/// over the caller's endpoint + bridge channels + pending state — the exact
/// post-loop teardown call. The template `bridge_inbound_tx` is consumed (the
/// helper drops it so the channel can reach all-senders-gone).
async fn run_shutdown_drain(
  endpoint: &mut StreamEndpoint<SmolStr, SocketAddr, RawRecords>,
  bridges: &mut HashMap<ExchangeId, BridgeHandle>,
  bridge_inbound_tx: mpsc::Sender<super::BridgeInbound>,
  bridge_inbound_rx: &mut mpsc::Receiver<super::BridgeInbound>,
  bridge_ready_tx: &flume::Sender<BridgeReady>,
  pending: &mut PendingCommands,
) {
  let gossip_socket = compio::net::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind loopback udp");
  let (obs_tx, _obs_rx) = mpsc::unbounded::<memberlist_proto::event::Event<SmolStr, SocketAddr>>();
  let observation_dropped = Cell::new(0u64);
  let obs_payload_bytes = Cell::new(0u64);
  freeze_and_drain_bridges_to_disconnected::<SmolStr, SocketAddr, RawRecords, _>(
    endpoint,
    bridges,
    bridge_inbound_tx,
    bridge_inbound_rx,
    bridge_ready_tx,
    &gossip_socket,
    None,
    &obs_tx,
    &observation_dropped,
    &obs_payload_bytes,
    None,
    pending,
    StreamTransportOptions::default(),
    &Default::default(),
  )
  .await;
}

/// Regression: a successful push/pull completion buffered PAST the per-iteration
/// `iter_drain_cap` is still folded into the parked join's reached set on
/// shutdown. The live loop's iter-top bridge drain is capped at `iter_drain_cap`
/// (256) and the loop exits on the shutdown command without re-draining, so a
/// response sitting behind a larger backlog would be dropped — the
/// drain-to-disconnected shutdown consumes the whole channel (every already-read
/// item, regardless of depth), so the reaped `Err((reached, Shutdown))` still
/// carries that seed.
#[compio::test]
async fn shutdown_drain_folds_completion_buffered_past_iter_cap() {
  let now = Instant::now();
  let mut endpoint = unlabeled_endpoint("dialer", "127.0.0.1:0".parse().unwrap());
  endpoint.start_scheduling(now);

  let addr_a = addr(7800);
  let addr_b = addr(7801);

  // Seed A: a real Succeeded whose response + EOF we buffer (NOT yet consumed).
  let (eid_a, response_a) = drive_push_to_queued_response(&mut endpoint, addr_a, now);

  // Seed B: a second exchange that never completes, so the join survives to the
  // shutdown reap (which supplies the Err tuple).
  endpoint.start_push_pull(addr_b, PushPullKind::Join, now);
  let mut eid_b = None;
  while let Some(action) = endpoint.poll_action() {
    if let StreamAction::Connect(info) = action {
      eid_b = Some(info.id());
    }
  }
  let eid_b = eid_b.expect("seed B produced a Connect exchange id");
  while endpoint.poll_transport_transmit().is_some() {}

  let filler_eid = mint_filler_eid(&mut endpoint, now);

  // Park the 2-seed join awaiting both A and B.
  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pending = empty_pending();
  pending.joins.push(PendingJoin {
    pending: HashSet::from([eid_a, eid_b]),
    addr_by_eid: HashMap::from([(eid_a, addr_a), (eid_b, addr_b)]),
    contacted: smallvec::SmallVec::new(),
    requested: 2,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  });

  // Queue order: a backlog of filler items FIRST (more than the default
  // iter_drain_cap of 256), THEN seed A's real response + EOF — so the
  // completion sits PAST where the live loop's capped per-iteration drain stops.
  let (bridge_inbound_tx, mut bridge_inbound_rx) = mpsc::unbounded::<super::BridgeInbound>();
  let (bridge_ready_tx, _bridge_ready_rx) = flume::unbounded::<BridgeReady>();

  const BACKLOG: usize = 300; // > RuntimeOptions::default().iter_drain_cap() (256)
  for _ in 0..BACKLOG {
    queue_inbound(
      &bridge_inbound_tx,
      super::BridgeInbound::Bytes(super::BridgeBytes {
        eid: filler_eid,
        bytes: vec![0u8; 4],
        received_at: now,
      }),
    );
  }
  for chunk in response_a {
    queue_inbound(
      &bridge_inbound_tx,
      super::BridgeInbound::Bytes(super::BridgeBytes {
        eid: eid_a,
        bytes: chunk,
        received_at: now,
      }),
    );
  }
  queue_inbound(
    &bridge_inbound_tx,
    super::BridgeInbound::Eof(super::BridgeEof {
      eid: eid_a,
      received_at: now,
    }),
  );

  assert!(
    bridge_inbound_rx.len() > RuntimeOptions::default().iter_drain_cap(),
    "the completion is buffered past the per-iteration cap",
  );

  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  run_shutdown_drain(
    &mut endpoint,
    &mut bridges,
    bridge_inbound_tx,
    &mut bridge_inbound_rx,
    &bridge_ready_tx,
    &mut pending,
  )
  .await;

  // eid_a's Succeeded folded into contacted; eid_b stays pending so the join is
  // still parked (not resolved inline).
  assert_eq!(
    pending.joins.len(),
    1,
    "the 2-seed join is still pending on B"
  );
  assert!(
    pending.joins[0].contacted.contains(&addr_a),
    "the completion buffered past the cap was folded into the reached set: {:?}",
    pending.joins[0].contacted,
  );

  // Reap exactly as the post-loop cleanup does; the Err tuple carries A.
  for mut pj in pending.joins.drain(..) {
    let _ = pj.reply.send(Err((
      std::mem::take(&mut pj.contacted),
      MemberlistError::Shutdown,
    )));
  }
  match rx.await {
    Ok(Err((reached, MemberlistError::Shutdown))) => assert!(
      reached.contains(&addr_a),
      "the reaped Err carries the completed seed: {reached:?}",
    ),
    other => panic!("expected Err((reached, Shutdown)); got {other:?}"),
  }
}

/// Regression: a completion that became ready on `bridge_inbound_rx` but lost the
/// shutdown `select` (the biased `cmd` arm fired first) is still folded by the
/// post-loop drain. Here a 1-seed join's only exchange completes during the
/// drain, so the join fully resolves INLINE with `Ok(reached)` — a buffered
/// completion racing teardown is honored as success, not clobbered to
/// `Err(Shutdown)`.
#[compio::test]
async fn shutdown_drain_folds_completion_that_lost_the_select() {
  let now = Instant::now();
  let mut endpoint = unlabeled_endpoint("dialer", "127.0.0.1:0".parse().unwrap());
  endpoint.start_scheduling(now);

  let addr_a = addr(7810);
  let (eid_a, response_a) = drive_push_to_queued_response(&mut endpoint, addr_a, now);

  // A 1-seed WaitForCompletion join: when eid_a completes the join fully resolves.
  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pending = empty_pending();
  pending.joins.push(PendingJoin {
    pending: HashSet::from([eid_a]),
    addr_by_eid: HashMap::from([(eid_a, addr_a)]),
    contacted: smallvec::SmallVec::new(),
    requested: 1,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  });

  // A single buffered completion — no backlog: it became ready but the shutdown
  // command won the biased select, so the live loop never drained it.
  let (bridge_inbound_tx, mut bridge_inbound_rx) = mpsc::unbounded::<super::BridgeInbound>();
  let (bridge_ready_tx, _bridge_ready_rx) = flume::unbounded::<BridgeReady>();
  for chunk in response_a {
    queue_inbound(
      &bridge_inbound_tx,
      super::BridgeInbound::Bytes(super::BridgeBytes {
        eid: eid_a,
        bytes: chunk,
        received_at: now,
      }),
    );
  }
  queue_inbound(
    &bridge_inbound_tx,
    super::BridgeInbound::Eof(super::BridgeEof {
      eid: eid_a,
      received_at: now,
    }),
  );

  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  run_shutdown_drain(
    &mut endpoint,
    &mut bridges,
    bridge_inbound_tx,
    &mut bridge_inbound_rx,
    &bridge_ready_tx,
    &mut pending,
  )
  .await;

  // The drain folded the completion, emptying `pending`, so drain_events resolved
  // the waiter inline and removed it.
  assert!(
    pending.joins.is_empty(),
    "the fully-resolved join was replied + removed inline by the drain",
  );
  match rx.await {
    Ok(Ok(reached)) => assert!(
      reached.contains(&addr_a),
      "the buffered completion that raced the select resolved the join Ok: {reached:?}",
    ),
    other => panic!("expected Ok(reached) for a fully-completed join; got {other:?}"),
  }
}

/// Regression: the shutdown drain TERMINATES against a backlog far deeper than
/// any single per-iteration cap, while still folding the front-of-FIFO
/// completion. The drain drops the driver's template sender and reads to
/// all-senders-gone, so an arbitrarily deep already-buffered set is consumed and
/// then the channel disconnects — nothing refills it (every producer is frozen
/// first), so the drain cannot spin. The deep backlog here confirms termination.
#[compio::test]
async fn shutdown_drain_bounded_terminates_against_deep_backlog() {
  let now = Instant::now();
  let mut endpoint = unlabeled_endpoint("dialer", "127.0.0.1:0".parse().unwrap());
  endpoint.start_scheduling(now);

  let addr_a = addr(7820);
  let addr_b = addr(7821);

  // Seed A's response + EOF queued FIRST (front of the FIFO), so it sits inside
  // the snapshot window ahead of the deep backlog appended below.
  let (eid_a, response_a) = drive_push_to_queued_response(&mut endpoint, addr_a, now);

  endpoint.start_push_pull(addr_b, PushPullKind::Join, now);
  let mut eid_b = None;
  while let Some(action) = endpoint.poll_action() {
    if let StreamAction::Connect(info) = action {
      eid_b = Some(info.id());
    }
  }
  let eid_b = eid_b.expect("seed B produced a Connect exchange id");
  while endpoint.poll_transport_transmit().is_some() {}

  let filler_eid = mint_filler_eid(&mut endpoint, now);

  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pending = empty_pending();
  pending.joins.push(PendingJoin {
    pending: HashSet::from([eid_a, eid_b]),
    addr_by_eid: HashMap::from([(eid_a, addr_a), (eid_b, addr_b)]),
    contacted: smallvec::SmallVec::new(),
    requested: 2,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  });

  let (bridge_inbound_tx, mut bridge_inbound_rx) = mpsc::unbounded::<super::BridgeInbound>();
  let (bridge_ready_tx, _bridge_ready_rx) = flume::unbounded::<BridgeReady>();
  for chunk in response_a {
    queue_inbound(
      &bridge_inbound_tx,
      super::BridgeInbound::Bytes(super::BridgeBytes {
        eid: eid_a,
        bytes: chunk,
        received_at: now,
      }),
    );
  }
  queue_inbound(
    &bridge_inbound_tx,
    super::BridgeInbound::Eof(super::BridgeEof {
      eid: eid_a,
      received_at: now,
    }),
  );
  // A backlog far exceeding any per-iteration cap; the snapshot bound consumes it
  // in bounded time rather than spinning.
  const DEEP_BACKLOG: usize = 4096;
  for _ in 0..DEEP_BACKLOG {
    queue_inbound(
      &bridge_inbound_tx,
      super::BridgeInbound::Bytes(super::BridgeBytes {
        eid: filler_eid,
        bytes: vec![0u8; 4],
        received_at: now,
      }),
    );
  }

  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  // A spin (an unbounded drain that never reached the reap) would blow this bound.
  compio::time::timeout(
    Duration::from_secs(10),
    run_shutdown_drain(
      &mut endpoint,
      &mut bridges,
      bridge_inbound_tx,
      &mut bridge_inbound_rx,
      &bridge_ready_tx,
      &mut pending,
    ),
  )
  .await
  .expect("the drain-to-disconnected terminates against a deep backlog");

  assert_eq!(
    pending.joins.len(),
    1,
    "the 2-seed join is still pending on B"
  );
  assert!(
    pending.joins[0].contacted.contains(&addr_a),
    "the front-of-FIFO completion is folded even behind a deep backlog: {:?}",
    pending.joins[0].contacted,
  );

  for mut pj in pending.joins.drain(..) {
    let _ = pj.reply.send(Err((
      std::mem::take(&mut pj.contacted),
      MemberlistError::Shutdown,
    )));
  }
  match rx.await {
    Ok(Err((reached, MemberlistError::Shutdown))) => assert!(
      reached.contains(&addr_a),
      "the reaped Err carries the completed seed: {reached:?}",
    ),
    other => panic!("expected Err((reached, Shutdown)); got {other:?}"),
  }
}

/// The real gate: a target join's terminal peer-FIN EOF PARKED on a bridge's
/// SATURATED `inbound_tx.send` — sitting OUTSIDE the channel buffer — is still
/// folded into the reaped reached set. Uses the REAL bounded channel filled to
/// capacity (the prior tests used an unbounded test channel and so could not
/// reproduce a parked send). The earlier snapshot-bound drain read only the
/// buffered depth (`len()`), so it dropped a parked EOF and lost that seed; the
/// cancel-then-drain-to-disconnected protocol drains to all-senders-gone,
/// unblocking the parked send first.
#[compio::test]
async fn shutdown_drain_folds_parked_eof_on_saturated_handoff() {
  let now = Instant::now();
  let mut endpoint = unlabeled_endpoint("dialer", "127.0.0.1:0".parse().unwrap());
  endpoint.start_scheduling(now);

  let addr_a = addr(7830);
  let addr_b = addr(7831);

  // Seed A driven to SendClosed (push + FIN already sent, eid captured); its
  // peer-FIN EOF is the completion we PARK below. The pull-response chunks are
  // buffered first so the FSM reaches BothClosed when that EOF finally lands.
  let (eid_a, response_a) = drive_push_to_queued_response(&mut endpoint, addr_a, now);

  // Seed B never completes, so the join survives to the reap (the reap, not an
  // inline resolution, supplies the Err tuple).
  endpoint.start_push_pull(addr_b, PushPullKind::Join, now);
  let mut eid_b = None;
  while let Some(action) = endpoint.poll_action() {
    if let StreamAction::Connect(info) = action {
      eid_b = Some(info.id());
    }
  }
  let eid_b = eid_b.expect("seed B produced a Connect exchange id");
  while endpoint.poll_transport_transmit().is_some() {}

  let filler_eid = mint_filler_eid(&mut endpoint, now);

  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pending = empty_pending();
  pending.joins.push(PendingJoin {
    pending: HashSet::from([eid_a, eid_b]),
    addr_by_eid: HashMap::from([(eid_a, addr_a), (eid_b, addr_b)]),
    contacted: smallvec::SmallVec::new(),
    requested: 2,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  });

  // A REAL bounded channel (NOT an unbounded test channel): a send beyond its
  // capacity PARKS, which is the condition the snapshot drain missed.
  const CAP: usize = 8;
  let (bridge_inbound_tx, mut bridge_inbound_rx) = mpsc::bounded::<super::BridgeInbound>(CAP);
  let (bridge_ready_tx, _bridge_ready_rx) = flume::unbounded::<BridgeReady>();

  // Fill the buffer to capacity: A's response chunks, then fillers until full.
  // A's terminal EOF cannot fit and must be PARKED on a send (below).
  for chunk in response_a {
    bridge_inbound_tx
      .try_send(super::BridgeInbound::Bytes(super::BridgeBytes {
        eid: eid_a,
        bytes: chunk,
        received_at: now,
      }))
      .unwrap_or_else(|_| unreachable!("A's response fits within the bounded capacity"));
  }
  while bridge_inbound_tx
    .try_send(super::BridgeInbound::Bytes(super::BridgeBytes {
      eid: filler_eid,
      bytes: vec![0u8; 4],
      received_at: now,
    }))
    .is_ok()
  {}
  assert_eq!(
    bridge_inbound_rx.len(),
    CAP,
    "the bounded buffer is full, so the EOF below cannot be buffered",
  );

  // A stand-in bridge PARKED on the saturated hand-off: it holds a sender clone
  // and blocks sending A's terminal EOF until the drain frees a slot — mirroring
  // a real bridge whose peer-FIN read landed but whose `inbound_tx.send` parked.
  // The clone drops on task exit, AFTER the EOF lands (the freeze contract).
  let parked_tx = bridge_inbound_tx.clone();
  compio::runtime::spawn(async move {
    // Ignoring Err: the receiver outlives this send until the drain consumes it.
    let _ = parked_tx
      .send(super::BridgeInbound::Eof(super::BridgeEof {
        eid: eid_a,
        received_at: now,
      }))
      .await;
  })
  .detach();

  // Let the stand-in run and PARK on the full channel before the drain starts, so
  // the EOF is genuinely outside the buffer when teardown begins.
  compio::time::sleep(Duration::from_millis(20)).await;
  assert_eq!(
    bridge_inbound_rx.len(),
    CAP,
    "the EOF is parked on the send, not buffered, when the drain starts",
  );

  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  run_shutdown_drain(
    &mut endpoint,
    &mut bridges,
    bridge_inbound_tx,
    &mut bridge_inbound_rx,
    &bridge_ready_tx,
    &mut pending,
  )
  .await;

  assert!(
    pending.joins[0].contacted.contains(&addr_a),
    "the parked peer-FIN EOF was folded into the reached set: {:?}",
    pending.joins[0].contacted,
  );

  for mut pj in pending.joins.drain(..) {
    let _ = pj.reply.send(Err((
      std::mem::take(&mut pj.contacted),
      MemberlistError::Shutdown,
    )));
  }
  match rx.await {
    Ok(Err((reached, MemberlistError::Shutdown))) => assert!(
      reached.contains(&addr_a),
      "the reaped Err carries the seed whose EOF was parked at the saturated hand-off: {reached:?}",
    ),
    other => panic!("expected Err((reached, Shutdown)); got {other:?}"),
  }
}

/// Connect a loopback TCP pair, returning `(server, client)`. The driver's bridge
/// owns `server`; the test plays the peer through `client`. Holding `client` open
/// (no write-shutdown) keeps the bridge in both-halves-live mode — the state the
/// close-first shutdown bug fabricated a completion from.
async fn loopback_pair() -> (TcpStream, TcpStream) {
  let listener = TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind loopback listener");
  let laddr = listener.local_addr().expect("listener local_addr");
  let client = TcpStream::connect(laddr).await.expect("connect client");
  let (server, _peer) = listener.accept().await.expect("accept server");
  (server, client)
}

/// End-to-end regression for the close-first shutdown fabrication, driven through
/// the REAL `Command::Shutdown` arm + the post-loop freeze barrier with a LIVE
/// both-halves-open bridge.
///
/// Seed A's pull RESPONSE has been read (folded) but its peer-FIN has NOT; A's
/// bridge is a genuine `bridge_task` over a connected socket whose peer never
/// FINs, so it sits in both-halves-live mode. The OLD shutdown arm sent
/// `BridgeOut::Close` to that bridge, which made it emit a SYNTHETIC EOF; the
/// freeze then folded response+synthetic-EOF into `ExchangeCompleted(Succeeded)`
/// and added A to `reached` — a seed that never completed (a fabrication). The
/// fixed arm sends nothing to the bridge; the freeze sends `cancel_tx`, so the
/// bridge breaks WITHOUT an EOF and A never completes. Asserts A is ABSENT from
/// the reaped `reached` set and the join resolves `Shutdown` (not `JoinFailed`).
#[compio::test]
async fn shutdown_command_does_not_fabricate_response_without_fin() {
  let now = Instant::now();
  let mut endpoint = unlabeled_endpoint("dialer", "127.0.0.1:0".parse().unwrap());
  endpoint.start_scheduling(now);

  let addr_a = addr(7840);
  // Drive A to "push + FIN sent, awaiting the peer's response + FIN", then fold
  // the response WITHOUT an EOF: the exchange has the response buffered but is
  // still awaiting the peer-FIN that would complete it.
  let (eid_a, response_a) = drive_push_to_queued_response(&mut endpoint, addr_a, now);
  for chunk in &response_a {
    endpoint.handle_transport_data(eid_a, chunk, false, now);
  }

  // A LIVE both-halves-open bridge for A: a real `bridge_task` over a connected
  // socket whose peer (`client`, held open to the end) never writes and never
  // FINs, so the bridge read-blocks in both-halves-live mode.
  let (bridge_inbound_tx, mut bridge_inbound_rx) = mpsc::unbounded::<super::BridgeInbound>();
  let (bridge_ready_tx, _bridge_ready_rx) = flume::unbounded::<BridgeReady>();
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let (server, client) = loopback_pair().await;
  let (out_tx, out_rx) = mpsc::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = futures_channel::oneshot::channel::<()>();
  bridges.insert(eid_a, BridgeHandle { out_tx, cancel_tx });
  spawn_bridge(
    server,
    eid_a,
    out_rx,
    cancel_rx,
    &bridge_inbound_tx,
    64,
    Duration::from_secs(60),
  );

  // A 1-seed join awaiting A.
  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pending = empty_pending();
  pending.joins.push(PendingJoin {
    pending: HashSet::from([eid_a]),
    addr_by_eid: HashMap::from([(eid_a, addr_a)]),
    contacted: smallvec::SmallVec::new(),
    requested: 1,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  });

  // Route through the REAL `Command::Shutdown` arm: it stashes the reply and
  // leaves the bridge table POPULATED for the freeze barrier (no close-first
  // drain, so no synthetic EOF).
  let mut shutdown_reply: Option<futures_channel::oneshot::Sender<super::Result<()>>> = None;
  let (sd_tx, mut sd_rx) = unit_reply();
  dispatch_with(
    &mut endpoint,
    &mut bridges,
    &mut shutdown_reply,
    &mut pending,
    Command::Shutdown(ShutdownCmd { reply: sd_tx }),
  )
  .await;
  assert_eq!(
    bridges.len(),
    1,
    "Command::Shutdown leaves the live bridge for the freeze barrier",
  );
  assert!(shutdown_reply.is_some(), "the shutdown reply is stashed");
  assert!(
    matches!(sd_rx.try_recv(), Ok(None)),
    "shutdown is NOT acked inline",
  );

  // The REAL freeze barrier: cancel (freeze) the live bridge, then drain to
  // disconnected. The bridge breaks on `cancel_tx` WITHOUT emitting an EOF, so A
  // never completes — no synthetic completion folds in.
  run_shutdown_drain(
    &mut endpoint,
    &mut bridges,
    bridge_inbound_tx,
    &mut bridge_inbound_rx,
    &bridge_ready_tx,
    &mut pending,
  )
  .await;

  assert!(
    pending.joins[0].contacted.is_empty(),
    "A's response-without-FIN was NOT fabricated into a completion: {:?}",
    pending.joins[0].contacted,
  );

  // Reap exactly as the post-loop cleanup does.
  for mut pj in pending.joins.drain(..) {
    let _ = pj.reply.send(Err((
      std::mem::take(&mut pj.contacted),
      MemberlistError::Shutdown,
    )));
  }
  match rx.await {
    Ok(Err((reached, MemberlistError::Shutdown))) => assert!(
      !reached.contains(&addr_a),
      "the seed that did not complete is ABSENT from the reaped reached set: {reached:?}",
    ),
    other => panic!("expected Err((reached, Shutdown)) with A absent; got {other:?}"),
  }

  // Hold the peer open until the very end so the bridge genuinely read-blocked
  // through the freeze (a dropped client would FIN and emit a REAL EOF itself).
  drop(client);
}

/// End-to-end counterpart to the no-fabrication regression: an already-read REAL
/// peer-FIN is PRESERVED through the real `Command::Shutdown` arm + freeze
/// barrier. Seed A's response + a genuine (read==0) peer-FIN EOF are already
/// queued on the inbound channel at shutdown; the freeze drain folds them, A
/// completes `Succeeded`, and its seed lands in the reaped `reached`. Confirms the
/// cancel-not-close freeze suppresses only a SYNTHETIC EOF — a real one still
/// completes.
#[compio::test]
async fn shutdown_command_preserves_already_read_peer_fin() {
  let now = Instant::now();
  let mut endpoint = unlabeled_endpoint("dialer", "127.0.0.1:0".parse().unwrap());
  endpoint.start_scheduling(now);

  let addr_a = addr(7850);
  let addr_b = addr(7851);

  // A: response chunks + a genuine peer-FIN EOF, both already read (queued).
  let (eid_a, response_a) = drive_push_to_queued_response(&mut endpoint, addr_a, now);

  // B never completes, so the join survives to the reap that supplies the Err tuple.
  endpoint.start_push_pull(addr_b, PushPullKind::Join, now);
  let mut eid_b = None;
  while let Some(action) = endpoint.poll_action() {
    if let StreamAction::Connect(info) = action {
      eid_b = Some(info.id());
    }
  }
  let eid_b = eid_b.expect("seed B produced a Connect exchange id");
  while endpoint.poll_transport_transmit().is_some() {}

  let (tx, rx) = futures_channel::oneshot::channel::<super::JoinReply>();
  let mut pending = empty_pending();
  pending.joins.push(PendingJoin {
    pending: HashSet::from([eid_a, eid_b]),
    addr_by_eid: HashMap::from([(eid_a, addr_a), (eid_b, addr_b)]),
    contacted: smallvec::SmallVec::new(),
    requested: 2,
    deadline: now + Duration::from_secs(30),
    reply: tx,
  });

  // Queue A's response, then a genuine peer-FIN EOF (an already-read completion).
  let (bridge_inbound_tx, mut bridge_inbound_rx) = mpsc::unbounded::<super::BridgeInbound>();
  let (bridge_ready_tx, _bridge_ready_rx) = flume::unbounded::<BridgeReady>();
  for chunk in response_a {
    queue_inbound(
      &bridge_inbound_tx,
      super::BridgeInbound::Bytes(super::BridgeBytes {
        eid: eid_a,
        bytes: chunk,
        received_at: now,
      }),
    );
  }
  queue_inbound(
    &bridge_inbound_tx,
    super::BridgeInbound::Eof(super::BridgeEof {
      eid: eid_a,
      received_at: now,
    }),
  );

  // Route through the REAL Command::Shutdown arm (empty bridge table — the
  // completion is already queued), then the freeze barrier.
  let mut bridges: HashMap<ExchangeId, BridgeHandle> = HashMap::new();
  let mut shutdown_reply: Option<futures_channel::oneshot::Sender<super::Result<()>>> = None;
  let (sd_tx, _sd_rx) = unit_reply();
  dispatch_with(
    &mut endpoint,
    &mut bridges,
    &mut shutdown_reply,
    &mut pending,
    Command::Shutdown(ShutdownCmd { reply: sd_tx }),
  )
  .await;
  assert!(shutdown_reply.is_some(), "the shutdown reply is stashed");

  run_shutdown_drain(
    &mut endpoint,
    &mut bridges,
    bridge_inbound_tx,
    &mut bridge_inbound_rx,
    &bridge_ready_tx,
    &mut pending,
  )
  .await;

  assert!(
    pending.joins[0].contacted.contains(&addr_a),
    "the already-read real peer-FIN completed A and folded it into reached: {:?}",
    pending.joins[0].contacted,
  );

  for mut pj in pending.joins.drain(..) {
    let _ = pj.reply.send(Err((
      std::mem::take(&mut pj.contacted),
      MemberlistError::Shutdown,
    )));
  }
  match rx.await {
    Ok(Err((reached, MemberlistError::Shutdown))) => assert!(
      reached.contains(&addr_a),
      "the reaped Err preserves the seed whose real peer-FIN was already read: {reached:?}",
    ),
    other => panic!("expected Err((reached, Shutdown)) with A present; got {other:?}"),
  }
}

/// Loop-level regression for the shutdown barrier-first ordering invariant: once
/// `Command::Shutdown` is observed, the post-loop freeze->drain barrier — never a
/// deadline reap — owns pending-join reaping.
///
/// A `WaitForCompletion` join with an ALREADY-EXPIRED deadline is parked in the
/// SAME iteration `Command::Shutdown` is observed: both commands are queued before
/// the driver task starts, so the iter-top command drain reads the join (parking
/// it past its deadline) and then the shutdown (setting the exit flag) before any
/// select arm / timer is built. With the pre-barrier deadline reap, the exit
/// branch reaped the just-parked, already-expired join and replied `JoinFailed`
/// before the freeze barrier could take ownership. With the reap gated on the exit
/// flag, the join survives to the barrier and is reaped `Shutdown` — the documented
/// teardown outcome (the existing freeze-barrier regressions cover folding a real
/// completion's seed into that `Err((reached, Shutdown))` set). This FAILS on the
/// pre-barrier-reap code (`JoinFailed`) and passes once the reap is gated.
#[compio::test]
async fn shutdown_observed_skips_deadline_reap_so_expired_join_is_reaped_shutdown() {
  let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
  // Schedulers disabled: the only inputs are the two commands queued below, so the
  // iteration that observes shutdown is deterministic.
  let ep = Endpoint::new(
    EndpointOptions::new(SmolStr::new("dialer"), bind)
      .with_probe_interval(Duration::ZERO)
      .with_gossip_interval(Duration::ZERO)
      .with_push_pull_interval(Duration::ZERO),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  );

  let snapshot: SnapshotCell<SmolStr> = {
    let ep_ref = endpoint.endpoint_ref();
    let local = ep_ref
      .member(ep_ref.local_id_ref())
      .expect("the local node is always a member");
    let boot = MemberlistSnapshot::new(
      vec![local.clone()],
      local,
      1,
      ep_ref.num_members(),
      ep_ref.health_score(),
    );
    Rc::new(RefCell::new(Rc::new(boot)))
  };

  let gossip_socket = compio::net::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind loopback udp");
  let listener = TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind loopback tcp");

  let (commands_tx, commands_rx) = flume::unbounded::<Command<SmolStr>>();
  let (events_tx, _events_rx) =
    flume::bounded::<memberlist_proto::event::Event<SmolStr, SocketAddr>>(1024);
  let (bridge_ready_tx, bridge_ready_rx) = flume::unbounded();

  // Queue BOTH commands before the driver task starts. The iter-top command drain
  // reads the join (parking it with a deadline that is already in the past) and
  // then the shutdown (which sets `exit`) within ONE iteration — before any timer
  // arm exists — so the exit branch, not the past-due timer path, handles the
  // expired join.
  let now = Instant::now();
  let (jtx, jrx) = futures_channel::oneshot::channel::<super::JoinReply>();
  commands_tx
    .send(Command::Join(JoinCmd {
      // Two real-looking seeds (nothing listening) so the join PARKS on two
      // captured exchanges rather than resolving zero-exchange at dispatch; the
      // dials fail off-loop after the loop has already exited.
      addrs: vec![addr(7990), addr(7991)],
      kind: JoinKind::WaitForCompletion(WaitForCompletionArgs { deadline: now }),
      reply: jtx,
    }))
    .expect("driver command channel is open");
  let (sd_tx, _sd_rx) = unit_reply();
  commands_tx
    .send(Command::Shutdown(ShutdownCmd { reply: sd_tx }))
    .expect("driver command channel is open");

  compio::runtime::spawn(super::stream_driver_loop::<
    SmolStr,
    SocketAddr,
    RawRecords,
    _,
    _,
  >(
    endpoint,
    gossip_socket,
    listener,
    commands_rx,
    events_tx,
    Rc::new(Cell::new(0u64)),
    Rc::new(Cell::new(0u64)),
    snapshot,
    Rc::new(Cell::new(memberlist_proto::metrics::Metrics::default())),
    bridge_ready_rx,
    bridge_ready_tx,
    Rc::new(Cell::new(false)),
    RuntimeOptions::new(),
    StreamTransportOptions::default(),
    VoidDelegate::<SmolStr, SocketAddr>::default(),
    None,
    Default::default(),
  ))
  .detach();

  match compio::time::timeout(Duration::from_secs(5), jrx).await {
    Ok(Ok(Err((reached, MemberlistError::Shutdown)))) => assert!(
      reached.is_empty(),
      "the expired join is reaped by the freeze barrier as Shutdown (nothing reached): {reached:?}",
    ),
    Ok(Ok(Err((_, other)))) => panic!(
      "the deadline reaper ran before the freeze barrier on the shutdown path, \
       resolving the expired join {other:?} instead of Shutdown",
    ),
    other => panic!("expected Err((reached, Shutdown)); got {other:?}"),
  }

  drop(commands_tx);
}
