use std::{
  collections::{HashMap, HashSet},
  net::{IpAddr, Ipv4Addr, SocketAddr},
};

use bytes::Bytes;
use memberlist_proto::{
  Instant, RawRecords,
  config::EndpointOptions,
  endpoint::Endpoint,
  event::{PushPullKind, StreamId},
  streams::{ExchangeId, LabelOptions, StreamAction, StreamEndpoint},
};
use smol_str::SmolStr;

use super::{
  BridgeHandle, BridgeReady, GOSSIP_RECV_BUF_MAX, MemberlistError, PendingJoin, PendingLeave,
  StreamTransportOptions, dispatch_gossip, drain_actions, drain_transport_transmits,
  gossip_recv_buf_len, min_pending_join_deadline, min_pending_leave_deadline, process_one_action,
  reap_pending_joins, reap_pending_leave,
};

fn addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

fn test_endpoint() -> StreamEndpoint<SmolStr, SocketAddr, RawRecords> {
  let ep = Endpoint::new(EndpointOptions::new(
    SmolStr::new("capture-test"),
    "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
  ));
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
    dispatch_gossip::<SmolStr, SocketAddr, RawRecords>(
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
  let small_len = gossip_recv_buf_len::<SmolStr, SocketAddr, RawRecords>(&small);
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
  );
  let big: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  );
  let big_len = gossip_recv_buf_len::<SmolStr, SocketAddr, RawRecords>(&big);
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
    !drain_actions::<SmolStr, SocketAddr, RawRecords>(
      &mut endpoint,
      &mut bridges,
      &ready_tx,
      StreamTransportOptions::default(),
      &Default::default(),
    ),
    "an idle endpoint queues no actions",
  );
  assert!(
    !drain_transport_transmits::<SmolStr, SocketAddr, RawRecords>(&mut endpoint, &bridges),
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
  let earliest = base + core::time::Duration::from_secs(1);
  let latest = base + core::time::Duration::from_secs(5);
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

  let leave_deadline = base + core::time::Duration::from_secs(3);
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
      deadline: now + core::time::Duration::from_secs(60),
      reply: tx_fail,
    },
    PendingJoin {
      pending: HashSet::new(),
      contacted: 3,
      requested: 3,
      deadline: now + core::time::Duration::from_secs(60),
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

/// `reap_pending_leave` replies `LeaveTimeout` to every joined replier once
/// the deadline elapses and clears the slot; a not-yet-expired leave stays
/// parked with no reply.
#[compio::test]
async fn reap_pending_leave_fires_only_after_the_deadline() {
  let now = Instant::now();

  let (tx_live, mut rx_live) = futures_channel::oneshot::channel::<super::Result<()>>();
  let mut not_expired = Some(PendingLeave {
    repliers: vec![tx_live],
    deadline: now + core::time::Duration::from_secs(10),
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
    deadline: now - core::time::Duration::from_secs(1),
  });
  reap_pending_leave(&mut expired, now).await;
  assert!(expired.is_none(), "an expired leave clears its slot");
  assert!(matches!(rx_a.await, Ok(Err(MemberlistError::LeaveTimeout))));
  assert!(matches!(rx_b.await, Ok(Err(MemberlistError::LeaveTimeout))));
}
