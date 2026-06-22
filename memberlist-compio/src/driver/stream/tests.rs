use std::{
  cell::Cell,
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
  driver::options::RuntimeOptions,
};
use lochan::mpsc;

use super::{
  BridgeHandle, BridgeReady, GOSSIP_RECV_BUF_MAX, MemberlistError, PendingCommands, PendingJoin,
  PendingLeave, StreamTransportOptions, dispatch_command, dispatch_gossip, drain_actions,
  drain_events, drain_transport_transmits, fire_timeout_with_drain, gossip_recv_buf_len,
  min_pending_join_deadline, min_pending_leave_deadline, process_one_action, reap_pending_joins,
  reap_pending_leave,
};
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
      Some((&started, &mut captured)),
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
      Some((&empty_started, &mut captured2)),
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
      contacted: 0,
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
/// `JoinAllFailed(requested, 0)`, any contacts ⇒ `Ok(contacted)`. This is the
/// degenerate zero-exchange `WaitForCompletion` / post-completion reap path.
#[compio::test]
async fn reap_pending_joins_resolves_empty_pending_waiters() {
  let now = Instant::now();
  let (tx_fail, rx_fail) = futures_channel::oneshot::channel();
  let (tx_ok, rx_ok) = futures_channel::oneshot::channel();
  let mut joins = vec![
    PendingJoin {
      pending: HashSet::new(),
      contacted: 0,
      requested: 4,
      deadline: now + Duration::from_secs(60),
      reply: tx_fail,
    },
    PendingJoin {
      pending: HashSet::new(),
      contacted: 3,
      requested: 3,
      deadline: now + Duration::from_secs(60),
      reply: tx_ok,
    },
  ];

  reap_pending_joins(&mut joins, now).await;
  assert!(joins.is_empty(), "both empty-pending waiters are reaped");

  match rx_fail.await {
    Ok(Err(MemberlistError::JoinAllFailed(e))) => {
      assert_eq!(e.requested(), 4, "carries the requested seed count");
      assert_eq!(e.contacted(), 0);
    }
    other => panic!("expected JoinAllFailed, got {other:?}"),
  }
  assert!(matches!(rx_ok.await, Ok(Ok(3))), "contacts ⇒ Ok(contacted)");
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
    let (tx, rx) = futures_channel::oneshot::channel::<super::Result<usize>>();
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
    assert!(matches!(rx.await, Ok(Err(MemberlistError::NotRunning))));
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
/// each Connect to its bridge, and replies `Ok(count)` immediately — the
/// fire-and-forget dispatch arm. Duplicate seeds count independently.
#[compio::test]
async fn dispatch_join_dispatch_replies_with_seed_count() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, rx) = futures_channel::oneshot::channel::<super::Result<usize>>();
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
  assert!(
    matches!(rx.await, Ok(Ok(3))),
    "dispatch join replies with the dispatched-exchange count",
  );
  assert!(pending.joins.is_empty(), "dispatch join parks no waiter");
}

/// `JoinKind::WaitForCompletion` on a Running node parks a `PendingJoin` whose
/// `requested` is the full seed count and whose `pending` tracks each dispatched
/// exchange's id — resolved later by `ExchangeCompleted(PushPull)`.
#[compio::test]
async fn dispatch_join_wait_parks_pending_join() {
  let mut endpoint = test_endpoint();
  let mut pending = empty_pending();

  let (tx, _rx) = futures_channel::oneshot::channel::<super::Result<usize>>();
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
/// and signals every live bridge to close, draining the bridge table. Asserts
/// the reply is NOT sent inline. The bridge is installed through the real
/// `SendReliable` → `process_one_action` Connect path so the drain operates on a
/// genuine `BridgeHandle`.
#[compio::test]
async fn dispatch_shutdown_stashes_reply_and_drains_bridges() {
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
  assert!(
    bridges.is_empty(),
    "every live bridge was drained on shutdown"
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
            Some((&started, &mut captured)),
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
