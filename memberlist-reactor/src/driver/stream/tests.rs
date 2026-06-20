use agnostic::{
  Runtime, RuntimeLite,
  net::{Net, TcpListener, TcpStream},
  tokio::TokioRuntime,
};
use memberlist_proto::{
  Instant, RawRecords, config::EndpointOptions, endpoint::Endpoint, streams::LabelOptions,
};
use smol_str::SmolStr;

use super::*;

type TokioNet = <TokioRuntime as Runtime>::Net;
type TokioTcpStream = <TokioNet as Net>::TcpStream;

/// Mints a real [`ExchangeId`] via a minimal in-memory `StreamEndpoint`.
///
/// `accept_connection` allocates only in-memory bridge state (no I/O), and the
/// `ExchangeId` it returns is opaque to the bridge — it merely stamps it onto
/// `BridgeInbound` events. These tests assert on the bytes the bridge writes to
/// the wire, not on the stamped id, so any valid id suffices.
fn fresh_eid() -> ExchangeId {
  let cfg = EndpointOptions::new(
    SmolStr::new("bridge-test"),
    "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
  );
  let ep = Endpoint::new(cfg, crate::gossip_rng().expect("test: OS entropy"));
  let mut endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|addr| *addr),
  );
  endpoint
    .accept_connection("127.0.0.1:1".parse().unwrap(), Instant::now())
    .expect("test: connection admitted")
}

/// Connects a loopback TCP pair, returning `(server, client)`. The bridge owns
/// `server`; the test plays the peer through `client`, reading what the bridge
/// writes.
async fn loopback_pair() -> (TokioTcpStream, TokioTcpStream) {
  let listener = <TokioNet as Net>::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind loopback listener");
  let addr = listener.local_addr().expect("listener local_addr");
  let client = TokioTcpStream::connect(addr).await.expect("connect client");
  let (server, _peer_addr) = listener.accept().await.expect("accept server");
  (server, client)
}

/// A `Shared` whose only role here is to absorb the bridge's `wake_driver`
/// calls — these tests assert on the wire, not on driver wakeups.
fn test_shared() -> Arc<Shared<SmolStr>> {
  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("bridge-test"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  Arc::new(Shared::new(snapshot_of(&ep)))
}

fn capture_test_endpoint() -> StreamEndpoint<SmolStr, SocketAddr, RawRecords> {
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

use std::{
  sync::atomic::AtomicU64,
  task::{Context, Wake, Waker},
};

use memberlist_proto::{
  event::{Reliability, UserPacket},
  typed::{NodeState, State},
};
use std::sync::atomic::AtomicBool;

/// A waker that records, via a shared flag, whether it was woken. Built on the
/// safe `std::task::Wake` trait (the crate forbids `unsafe`). The driver-poll
/// tests pass one through a `Context` and only need it to be a valid, harmless
/// waker — its wake is a no-op flag flip.
struct FlagWaker(Arc<AtomicBool>);

impl Wake for FlagWaker {
  fn wake(self: Arc<Self>) {
    self.0.store(true, Ordering::SeqCst);
  }
  fn wake_by_ref(self: &Arc<Self>) {
    self.0.store(true, Ordering::SeqCst);
  }
}

fn flag_waker() -> Waker {
  Waker::from(Arc::new(FlagWaker(Arc::new(AtomicBool::new(false)))))
}

/// An application-data `Event::UserPacket` of exactly `len` payload bytes —
/// `observation_payload_bytes` reports `Some(len)`, so it drives the obs-channel
/// byte backstop. `account_event` treats it as a no-op (no pending state), so it
/// can be fed to `send_observation` without any join/leave/ping bookkeeping.
fn user_packet(len: usize) -> Event<SmolStr, SocketAddr> {
  Event::UserPacket(UserPacket::new(
    "127.0.0.1:2".parse::<SocketAddr>().unwrap(),
    Bytes::from(vec![0xABu8; len]),
    Reliability::Reliable,
  ))
}

/// A control event carrying no app-data (`observation_payload_bytes` is `None`)
/// and, with no parked leave/join/ping, a no-op for `account_event`. Used to
/// drive the obs-channel `Full`-and-recoverable drop arm.
fn control_event() -> Event<SmolStr, SocketAddr> {
  Event::NodeJoined(Arc::new(NodeState::new(
    SmolStr::new("ctl"),
    "127.0.0.1:3".parse::<SocketAddr>().unwrap(),
    State::Alive,
  )))
}

/// Builds a real `StreamDriver` with a bound gossip socket and a caller-supplied
/// observation channel, so the obs-backstop and shutdown branches can be driven
/// directly. The accept channel is wired but never fed.
///
/// `obs_cap` sizes the bounded obs channel (fill it to hit the `Full` arms);
/// `obs_budget` is the payload byte budget (small to hit the byte backstop).
/// Returns the driver, the obs receiver (drop it to hit the `Disconnected`
/// arms), and the shared payload-byte counter.
async fn build_driver(
  obs_cap: usize,
  obs_budget: Option<u64>,
) -> (
  StreamDriver<SmolStr, TokioRuntime, RawRecords>,
  Receiver<Event<SmolStr, SocketAddr>>,
  Arc<Shared<SmolStr>>,
  Arc<AtomicU64>,
) {
  build_driver_with(obs_cap, obs_budget, 8, |endpoint| {
    endpoint.start_scheduling(Instant::now());
  })
  .await
}

/// [`build_driver`] with a caller-chosen `transmit_batch` and a hook that runs
/// on the endpoint BEFORE the driver takes ownership. The hook also owns
/// scheduler startup ([`build_driver`] starts them; a wake-counting test leaves
/// them OFF so no staggered scheduler deadline can supply a wake the test means
/// to attribute to something else).
async fn build_driver_with(
  obs_cap: usize,
  obs_budget: Option<u64>,
  transmit_batch: usize,
  prep: impl FnOnce(&mut StreamEndpoint<SmolStr, SocketAddr, RawRecords>),
) -> (
  StreamDriver<SmolStr, TokioRuntime, RawRecords>,
  Receiver<Event<SmolStr, SocketAddr>>,
  Arc<Shared<SmolStr>>,
  Arc<AtomicU64>,
) {
  let socket = <TokioNet as Net>::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind gossip socket");
  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("drv"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let mut endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  );
  prep(&mut endpoint);
  let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));
  let obs_payload_bytes = Arc::new(AtomicU64::new(0));
  let (obs_tx, obs_rx) = flume::bounded(obs_cap);
  let (accepted_tx, accepted_rx) = flume::bounded(ACCEPT_CAP);
  let (accept_shutdown_tx, accept_shutdown_rx) = flume::bounded(1);
  // Spawn the real accept task over a bound listener so the shutdown branch's
  // join-on-exit behaves exactly as in production: dropping `accept_shutdown_tx`
  // cancels its `accept()`, the task exits, and `accept_join` resolves.
  let listener = <TokioNet as Net>::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind accept listener");
  let accept_join = TokioRuntime::spawn(accept_task::<SmolStr, _>(
    listener,
    accepted_tx,
    accept_shutdown_rx,
    shared.clone(),
  ));
  let driver = StreamDriver::<SmolStr, TokioRuntime, RawRecords>::new(
    endpoint,
    socket,
    shared.clone(),
    8,
    transmit_batch,
    obs_tx,
    obs_payload_bytes.clone(),
    obs_budget,
    accepted_rx,
    accept_shutdown_tx,
    accept_join,
    Duration::from_secs(60),
    None,
    #[cfg(feature = "cidr")]
    None,
    #[cfg(not(feature = "cidr"))]
    (),
  );
  (driver, obs_rx, shared, obs_payload_bytes)
}

/// The recv loop, when gated by a full raw-ingress buffer, skips
/// `poll_recv_from` entirely — so NO socket waker is registered that pass. A
/// `transmit_batch` larger than the cap then lets the ingress drain empty the
/// whole buffer without exhausting its budget, so without the gate's
/// self-wake flag the driver would park with readable datagrams queued in the
/// kernel until the idle timer. This drives exactly that shape and asserts
/// the gated pass self-wakes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recv_gate_self_wakes_after_draining_full_ingress_buffer() {
  let src: SocketAddr = "127.0.0.1:9".parse().unwrap();
  let (driver, _obs_rx, _shared, _bytes) =
    build_driver_with(16, None, MAX_BUFFERED_INGRESS * 2, |endpoint| {
      // Deliberately NO start_scheduling: with the schedulers off there is no
      // machine deadline whose elapse could fire the timer branch and supply
      // a wake this test would falsely attribute to the recv gate.
      //
      // Fill the raw buffer to the recv-gate cap so the first poll's recv loop
      // is gated before it ever touches the socket.
      let now = Instant::now();
      for _ in 0..MAX_BUFFERED_INGRESS {
        endpoint.handle_gossip(src, b"x", now);
      }
      assert_eq!(endpoint.pending_memberlist_ingress(), MAX_BUFFERED_INGRESS);
    })
    .await;

  struct CountWaker(AtomicU64);
  impl std::task::Wake for CountWaker {
    fn wake(self: Arc<Self>) {
      self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
      self.0.fetch_add(1, Ordering::SeqCst);
    }
  }
  let count = Arc::new(CountWaker(AtomicU64::new(0)));
  let waker = std::task::Waker::from(count.clone());
  let mut cx = Context::from_waker(&waker);

  // One pass: the gated recv loop registers no socket interest; the drain
  // then empties every buffered datagram under its oversized budget (so no
  // budget-exhaustion self-wake fires); the schedulers are off (no timer
  // wake). The gate flag is therefore the pass's ONLY wake source — assert
  // EXACTLY one wake, so an unrelated wake cannot mask a broken gate.
  let mut driver = core::pin::pin!(driver);
  assert!(driver.as_mut().poll(&mut cx).is_pending());
  assert_eq!(
    count.0.load(Ordering::SeqCst),
    1,
    "the gate flag must be the gated pass's only self-wake"
  );
}

/// `send_observation`'s byte backstop: when enqueuing a payload event would push
/// the queued payload bytes over budget, the event is DROPPED and counted —
/// never retained — because the count cap alone does not bound memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_byte_backstop_drops_oversized_payload() {
  // Budget of 4 bytes; an 8-byte payload alone exceeds it on the first event.
  let (mut driver, _obs_rx, shared, bytes) = build_driver(16, Some(4)).await;

  driver.send_observation(user_packet(8));

  assert_eq!(
    shared.observation_dropped(),
    1,
    "an over-budget payload event is dropped and counted"
  );
  assert_eq!(
    bytes.load(Ordering::Relaxed),
    0,
    "a dropped payload reserves no bytes"
  );
  assert!(
    driver.obs_overflow.is_empty(),
    "a byte-backstop drop never retains the event for retry"
  );
}

/// `send_observation` on a FULL obs channel RETAINS application data (still
/// byte-reserved) for a later retry, rather than dropping it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_full_channel_retains_app_data() {
  // Capacity-1 channel, ample byte budget. Fill the channel, then a second
  // payload event finds it full and is retained in the overflow.
  let (mut driver, _obs_rx, shared, bytes) = build_driver(1, Some(1 << 20)).await;

  driver.send_observation(user_packet(4)); // fills the capacity-1 channel
  assert!(
    driver.obs_overflow.is_empty(),
    "first event went to the channel"
  );
  driver.send_observation(user_packet(7)); // channel full → retained

  assert_eq!(
    driver.obs_overflow.len(),
    1,
    "app-data is retained for retry on a full channel, not dropped"
  );
  assert_eq!(
    shared.observation_dropped(),
    0,
    "a retained event is not counted as dropped"
  );
  assert_eq!(
    bytes.load(Ordering::Relaxed),
    4 + 7,
    "both the queued and the retained payload stay byte-reserved"
  );
}

/// `send_observation` on a FULL obs channel DROPS a recoverable control event
/// (no app-data) and counts it — only application data is worth retaining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_full_channel_drops_recoverable_control() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(1, Some(1 << 20)).await;

  driver.send_observation(control_event()); // fills the capacity-1 channel
  driver.send_observation(control_event()); // channel full → dropped + counted

  assert!(
    driver.obs_overflow.is_empty(),
    "a recoverable control event is never retained"
  );
  assert_eq!(
    shared.observation_dropped(),
    1,
    "the second control event found the channel full and was counted"
  );
}

/// `send_observation` with the obs task GONE (receiver dropped) rolls back the
/// reservation it made before the `try_send`, so the byte counter never leaks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_disconnected_rolls_back_reservation() {
  let (mut driver, obs_rx, shared, bytes) = build_driver(16, Some(1 << 20)).await;
  drop(obs_rx); // the obs task is gone → try_send returns Disconnected

  driver.send_observation(user_packet(9));

  assert_eq!(
    bytes.load(Ordering::Relaxed),
    0,
    "a Disconnected send rolls back the payload reservation"
  );
  assert!(
    driver.obs_overflow.is_empty(),
    "a Disconnected send retains nothing"
  );
  assert_eq!(
    shared.observation_dropped(),
    0,
    "a Disconnected (obs task gone) send is not a recoverable drop"
  );
}

/// `flush_obs_overflow` stops at the first `Full`, pushing the un-sendable event
/// back to the FRONT so retry order is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_overflow_stops_and_repushes_on_full() {
  // `_obs_rx` is held (never drained) so the channel stays connected-but-full.
  let (mut driver, _obs_rx, _shared, _bytes) = build_driver(1, Some(1 << 20)).await;
  // Fill the capacity-1 channel so the flush below cannot make progress.
  driver
    .obs_tx
    .try_send(control_event())
    .expect("seed the channel full");
  // Two events queued for retry; neither can be sent while the channel is full.
  driver.obs_overflow.push_back(control_event());
  driver.obs_overflow.push_back(control_event());

  driver.flush_obs_overflow();

  assert_eq!(
    driver.obs_overflow.len(),
    2,
    "flush stops at the first Full and re-pushes the event to the front"
  );
}

/// `flush_obs_overflow` with the obs task GONE reclaims each retained event's
/// reserved payload bytes (the obs task can no longer release them).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_overflow_disconnected_reclaims_bytes() {
  let (mut driver, obs_rx, _shared, bytes) = build_driver(16, Some(1 << 20)).await;
  // Stage a retained payload event AS IF previously reserved, then drop the
  // receiver so the flush sees Disconnected and reclaims the reservation.
  bytes.store(6, Ordering::Relaxed);
  driver.obs_overflow.push_back(user_packet(6));
  drop(obs_rx);

  driver.flush_obs_overflow();

  assert!(
    driver.obs_overflow.is_empty(),
    "a Disconnected flush drains the overflow"
  );
  assert_eq!(
    bytes.load(Ordering::Relaxed),
    0,
    "a Disconnected flush reclaims the retained payload's reserved bytes"
  );
}

/// Drives the driver through exactly one `Future::poll` with a harmless waker.
fn poll_once(driver: &mut StreamDriver<SmolStr, TokioRuntime, RawRecords>) -> Poll<()> {
  let waker = flag_waker();
  let mut cx = Context::from_waker(&waker);
  Pin::new(driver).poll(&mut cx)
}

/// Drives a shutting-down driver to `Poll::Ready`, polling it with the calling
/// task's real waker so the shutdown branch's awaited accept-task join handle
/// registers that waker and the runtime re-polls once the accept task exits (no
/// busy-loop, no fixed iteration bound). The one-time teardown — failing parked
/// waiters, closing the command queue — runs on the FIRST poll, so any parked
/// reply is already sent once this returns; the later polls only drain the
/// accept-task join.
async fn poll_to_ready(driver: &mut StreamDriver<SmolStr, TokioRuntime, RawRecords>) {
  futures_util::future::poll_fn(|cx| Pin::new(&mut *driver).poll(cx)).await;
}

/// On shutdown, a parked synchronous `WaitForCompletion` join is failed with
/// `Err(Shutdown)` (the `pending_joins.drain()` arm). Dispatching the wait-join
/// while still running parks it (its dial produces a `Connect`); the same poll's
/// shutdown branch then drains it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_join() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = oneshot::channel::<Result<usize, Error>>();
  shared.push_command(Command::Join(JoinCmd {
    addrs: vec!["127.0.0.1:9".parse::<SocketAddr>().unwrap()],
    wait: true,
    reply: tx,
  }));
  shared.begin_shutdown();
  poll_to_ready(&mut driver).await;
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked wait-join is failed with Shutdown on driver exit"
  );
}

/// On shutdown, a parked application-ping is failed with `Err(Shutdown)` (the
/// `pending_pings.drain()` arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_ping() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = oneshot::channel::<Result<Duration, Error>>();
  let node = memberlist_proto::Node::new(
    SmolStr::new("peer"),
    "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
  );
  shared.push_command(Command::Ping(PingCmd { node, reply: tx }));
  shared.begin_shutdown();
  poll_to_ready(&mut driver).await;
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked ping is failed with Shutdown on driver exit"
  );
}

/// On shutdown, a parked reliable directed send is failed with `Err(Shutdown)`
/// (the `pending_user_sends.drain()` arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_reliable_send() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = oneshot::channel::<Result<(), Error>>();
  shared.push_command(Command::SendReliable(SendReliableCmd {
    to: "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
    payloads: vec![Bytes::from_static(b"reliable")],
    reply: tx,
  }));
  shared.begin_shutdown();
  poll_to_ready(&mut driver).await;
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked reliable send is failed with Shutdown on driver exit"
  );
}

/// The shutdown branch's `close_and_drain` loop fails EVERY queued command
/// variant: a handle that pushed a command in the race window between the
/// poll's top-of-poll command drain and the shutdown `close_and_drain` gets a
/// reply (`Err(Shutdown)`, or `Ok(())` for `Shutdown`) instead of hanging.
///
/// That window — between the poll's top-of-poll dispatch and `close_and_drain`
/// — only exists mid-poll, so the commands must be enqueued concurrently. A
/// pusher thread bursts all variants the instant a barrier releases, while the
/// main task polls the already-`begin_shutdown()` driver. A variant whose
/// `close_and_drain` reply (`Shutdown`) differs from its dispatch reply proves
/// it reached `close_and_drain`. The window is racy, so each variant is
/// accumulated across attempts (rotating the push order so none is starved);
/// the bound fails loudly if any variant is never observed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_close_and_drain_fails_every_queued_command() {
  use std::{sync::Barrier, thread};

  // The five distinguishable variants: dispatched-while-running they reply
  // Ok / a non-Shutdown error, so a Shutdown reply on these proves the command
  // reached `close_and_drain` rather than the top-of-poll dispatch.
  const MAX_ATTEMPTS: usize = 20000;
  // The window is racy, so accumulate per-variant rather than demanding all
  // five in one attempt; the push order is rotated each attempt so no variant
  // is starved by always racing from the same position. The loop breaks as
  // soon as every variant has been observed replying Shutdown.
  let mut seen_join = false;
  let mut seen_user = false;
  let mut seen_comp = false;
  let mut seen_chk = false;
  let mut seen_enc = false;
  for attempt in 0..MAX_ATTEMPTS {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(64, Some(1 << 20)).await;
    shared.begin_shutdown();

    // Distinguishable replies.
    let (join_tx, join_rx) = oneshot::channel::<Result<usize, Error>>();
    let (user_tx, user_rx) = oneshot::channel::<Result<(), Error>>();
    let (comp_tx, comp_rx) = oneshot::channel::<Result<(), Error>>();
    let (chk_tx, chk_rx) = oneshot::channel::<Result<(), Error>>();
    let (enc_tx, enc_rx) = oneshot::channel::<Result<(), Error>>();
    // The remaining four arms (covered when the window is hit, but their
    // Shutdown / Ok reply is not uniquely attributable to this arm).
    let (leave_tx, _leave_rx) = oneshot::channel::<Result<(), Error>>();
    let (shutdown_tx, _shutdown_rx) = oneshot::channel::<Result<(), Error>>();
    let (ping_tx, _ping_rx) = oneshot::channel::<Result<Duration, Error>>();
    let (rel_tx, _rel_rx) = oneshot::channel::<Result<(), Error>>();

    let to = "127.0.0.1:9".parse::<SocketAddr>().unwrap();
    let node = memberlist_proto::Node::new(SmolStr::new("peer"), to);
    let mut cmds: Vec<Command<SmolStr>> = vec![
      Command::Join(JoinCmd {
        addrs: vec![to],
        wait: false,
        reply: join_tx,
      }),
      Command::SendUser(SendUserCmd {
        to,
        payloads: vec![Bytes::from_static(b"u")],
        reply: user_tx,
      }),
      Command::SetCompressionOptions(SetCompressionOptionsCmd {
        opts: memberlist_proto::CompressionOptions::new(),
        reply: comp_tx,
      }),
      Command::SetChecksumOptions(SetChecksumOptionsCmd {
        opts: memberlist_proto::ChecksumOptions::new(),
        reply: chk_tx,
      }),
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
        opts: memberlist_proto::EncryptionOptions::new(),
        reply: enc_tx,
      }),
      Command::Leave(LeaveCmd { reply: leave_tx }),
      Command::Shutdown(ShutdownCmd { reply: shutdown_tx }),
      Command::Ping(PingCmd {
        node,
        reply: ping_tx,
      }),
      Command::SendReliable(SendReliableCmd {
        to,
        payloads: vec![Bytes::from_static(b"r")],
        reply: rel_tx,
      }),
    ];
    // Rotate the push order so each variant races from a different position
    // across attempts rather than always last (which would starve it).
    let rotation = attempt % cmds.len();
    cmds.rotate_left(rotation);

    let barrier = Arc::new(Barrier::new(2));
    let pusher_barrier = barrier.clone();
    let pusher_shared = shared.clone();
    let pusher = thread::spawn(move || {
      pusher_barrier.wait();
      for cmd in cmds {
        // Ignoring bool: a push rejected after the driver closed the queue just
        // means this attempt missed the window; the outer loop retries.
        let _ = pusher_shared.push_command(cmd);
      }
    });

    barrier.wait();
    poll_to_ready(&mut driver).await;
    pusher.join().expect("pusher thread joins");

    // Record any variant that `close_and_drain` failed with Shutdown this
    // attempt (a Shutdown reply on these proves the command reached
    // `close_and_drain` rather than the top-of-poll dispatch).
    seen_join |= matches!(join_rx.await, Ok(Err(Error::Shutdown)));
    seen_user |= matches!(user_rx.await, Ok(Err(Error::Shutdown)));
    seen_comp |= matches!(comp_rx.await, Ok(Err(Error::Shutdown)));
    seen_chk |= matches!(chk_rx.await, Ok(Err(Error::Shutdown)));
    seen_enc |= matches!(enc_rx.await, Ok(Err(Error::Shutdown)));
    if seen_join && seen_user && seen_comp && seen_chk && seen_enc {
      break;
    }
  }
  assert!(
    seen_join && seen_user && seen_comp && seen_chk && seen_enc,
    "every queued command variant must reply Shutdown via close_and_drain within \
       {MAX_ATTEMPTS} attempts (join={seen_join} user={seen_user} comp={seen_comp} \
       chk={seen_chk} enc={seen_enc})"
  );
}

/// Two `Shutdown` commands queued before the SAME poll each get their own
/// `Ok(())` ack. The shutdown reply is a `Vec`, so the first caller is parked
/// alongside the second rather than overwritten — a single-slot reply would
/// drop the first sender (its receiver would observe a `Canceled` oneshot)
/// when the second `Shutdown` dispatched in the same drain. The ack fires only
/// after the gossip socket and the accept task's listener are released, which
/// `poll_to_ready` awaits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_acks_every_same_poll_caller() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;

  let (first_tx, first_rx) = oneshot::channel::<Result<(), Error>>();
  let (second_tx, second_rx) = oneshot::channel::<Result<(), Error>>();
  // Both land in one top-of-poll drain, so both dispatch (and park their reply)
  // before the shutdown branch acks.
  shared.push_command(Command::Shutdown(ShutdownCmd { reply: first_tx }));
  shared.push_command(Command::Shutdown(ShutdownCmd { reply: second_tx }));

  poll_to_ready(&mut driver).await;
  assert!(
    matches!(first_rx.await, Ok(Ok(()))),
    "the first same-poll shutdown caller is acked Ok, not dropped"
  );
  assert!(
    matches!(second_rx.await, Ok(Ok(()))),
    "the second same-poll shutdown caller is acked Ok"
  );
}

/// A second `Shutdown` racing the poll while one is already parked is itself
/// acked `Ok(())` — whether it is dispatched at the top of the poll or taken
/// by `close_and_drain` mid-poll — and the already-parked first caller is
/// STILL acked `Ok(())`. With a single-slot reply the second caller would
/// overwrite the first's parked sender (canceling its oneshot) when both land
/// in the same drain; the reply set holds every concurrent caller instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_acks_concurrent_callers() {
  use std::{sync::Barrier, thread};

  const MAX_ATTEMPTS: usize = 20000;
  // The first `Shutdown` is queued before the poll, so it is always drained and
  // parked. A pusher thread races a second `Shutdown` into the poll: it lands
  // either in the same top-of-poll drain as the first or in the window before
  // `close_and_drain`. The first caller's ack must survive that race in EVERY
  // attempt; the second's `Ok(())` is recorded when its push was accepted, to
  // confirm the concurrent path is actually exercised within the bound.
  let mut saw_second_ok = false;
  for _ in 0..MAX_ATTEMPTS {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(64, Some(1 << 20)).await;
    shared.begin_shutdown();

    let (first_tx, first_rx) = oneshot::channel::<Result<(), Error>>();
    let (second_tx, second_rx) = oneshot::channel::<Result<(), Error>>();
    // Queue the first caller before polling so it is always parked.
    shared.push_command(Command::Shutdown(ShutdownCmd { reply: first_tx }));

    let barrier = Arc::new(Barrier::new(2));
    let pusher_barrier = barrier.clone();
    let pusher_shared = shared.clone();
    // The push returns false if the poll already closed the queue; in that case
    // the second caller never enters and this attempt simply does not exercise
    // the concurrent path. Report whether it was accepted so the assertion can
    // ignore the receiver of a never-queued caller.
    let pusher = thread::spawn(move || -> bool {
      pusher_barrier.wait();
      pusher_shared.push_command(Command::Shutdown(ShutdownCmd { reply: second_tx }))
    });

    barrier.wait();
    poll_to_ready(&mut driver).await;
    let second_queued = pusher.join().expect("pusher thread joins");

    // The first, always-parked caller must be acked Ok regardless of how the
    // second raced — a single-slot reply would drop it on a same-drain overwrite.
    assert!(
      matches!(first_rx.await, Ok(Ok(()))),
      "the already-parked shutdown caller is acked Ok despite a concurrent shutdown"
    );
    // When the second push was accepted, its caller must also be acked Ok
    // (parked at dispatch or via close_and_drain), never left hanging.
    if second_queued {
      assert!(
        matches!(second_rx.await, Ok(Ok(()))),
        "an accepted concurrent shutdown caller is also acked Ok"
      );
      saw_second_ok = true;
    }
  }
  assert!(
    saw_second_ok,
    "a concurrent second shutdown must be accepted and acked Ok in some attempt within \
       {MAX_ATTEMPTS}"
  );
}

/// Shutdown completion promises only the bind address is free — it does NOT
/// await the connected-stream FD of an in-flight reliable exchange. The teardown
/// preempts every live bridge (`cancel_tx.send(())`) and then completes WITHOUT
/// blocking on the bridge task. A bridge that cannot exit on its own (parked,
/// e.g. stalled on the bounded inbound hand-off) must not wedge shutdown: were
/// the teardown to await the bridge instead, this test would time out.
///
/// A controllable proxy stands in for the bridge task. It first awaits the
/// preemption (proving the teardown SENT the cancel), reports it observed it,
/// then parks on a gate the test NEVER releases — so the proxy is provably still
/// alive when shutdown completes. The driver must reach `Poll::Ready` and fire
/// the completion latch promptly, with the proxy still parked. Every wait is
/// bounded, so a regression that re-introduces awaiting the bridge surfaces as a
/// timeout, never a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_preempts_live_bridge_without_awaiting_it() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  // The gossip socket's address, captured before teardown drops it, so the
  // post-shutdown rebind asserts the FD was actually released.
  let gossip_addr = driver
    .socket
    .as_ref()
    .expect("gossip socket is bound while running")
    .local_addr()
    .expect("gossip socket local_addr");

  // Install one live bridge whose task is a controllable proxy. `out_tx` is kept
  // on the handle (dropped by the teardown's `bridges.drain()`); `cancel_tx` is
  // the channel the teardown preempts on. The proxy is spawned independently —
  // the driver no longer stores a bridge join, so it cannot (and must not) await
  // this task.
  let (out_tx, _out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
  let (observed_cancel_tx, observed_cancel_rx) = oneshot::channel::<()>();
  // The gate is held by the test and NEVER released, so the proxy can only exit
  // if something cancels it — which nothing does. It is therefore provably still
  // alive at the instant shutdown completes.
  let (_gate_tx, gate_rx) = oneshot::channel::<()>();
  let exited = Arc::new(AtomicBool::new(false));
  let exited_in_task = exited.clone();
  let proxy = TokioRuntime::spawn(async move {
    // Wait for the teardown's explicit preemption; a teardown that merely dropped
    // the handle (no cancel) would leave this parked and never report below.
    let _ = cancel_rx.await;
    // Ignoring Err: the test holds the receiver until it has observed this.
    let _ = observed_cancel_tx.send(());
    // Park forever on the never-released gate: the proxy stays alive, so a
    // shutdown that completes while it is parked proves the driver did not await
    // it.
    let _ = gate_rx.await;
    exited_in_task.store(true, Ordering::SeqCst);
  });
  let eid = fresh_eid();
  driver
    .bridges
    .insert(eid, BridgeHandle { out_tx, cancel_tx });

  shared.begin_shutdown();

  // Drive shutdown to completion. The one-time teardown preempts the bridge and
  // then awaits ONLY the accept task; with no bridge await, this resolves even
  // though the proxy is still parked. The bound converts a regression (awaiting
  // the bridge) into a loud timeout rather than an indefinite hang.
  tokio::time::timeout(Duration::from_secs(5), poll_to_ready(&mut driver))
    .await
    .expect("shutdown completes without awaiting the live bridge task");

  // (b) The preemption actually reached the bridge.
  tokio::time::timeout(Duration::from_secs(5), observed_cancel_rx)
    .await
    .expect("the teardown sends the cancel preemption to the live bridge")
    .expect("the proxy reports it observed the preemption");
  // The proxy is still parked on the never-released gate: shutdown completed
  // strictly without waiting for it to exit.
  assert!(
    !exited.load(Ordering::SeqCst),
    "shutdown completes while the preempted bridge is still alive (not awaited)"
  );

  // (c) The bind address is free: an immediate rebind on the gossip address (no
  // sleep, no retry) must succeed, mirroring the post-`shutdown().await` rebind.
  <TokioNet as Net>::UdpSocket::bind(gossip_addr)
    .await
    .expect("the gossip socket FD is released, so its address rebinds immediately");

  // Abandon the deliberately-parked proxy; dropping a tokio `JoinHandle`
  // detaches the task, which the runtime reaps at test teardown.
  drop(proxy);
}

/// The accept task exits when its shutdown channel is dropped EVEN IF it is
/// blocked handing a freshly accepted connection to a full `accepted` channel.
/// The inner hand-off send is raced against the shutdown signal, so a full
/// queue at shutdown cannot wedge the task and leak the TCP listener FD. An
/// un-cancellable hand-off send would never observe the dropped shutdown
/// channel and this join would never resolve — the timeout converts that wedge
/// into a loud failure instead of an indefinite hang.
///
/// Driven on a single-threaded runtime so the cooperative schedule is
/// deterministic: the accept task is stepped (via `yield_now`) WHILE its
/// shutdown channel is still open, so it can only come to rest blocked on the
/// full-channel send (the connection is already accepted, the send cannot
/// progress, and the shutdown arm is not yet ready). Dropping the sender then
/// must wake exactly that inner select.
#[tokio::test(flavor = "current_thread")]
async fn accept_task_unwedges_from_full_queue_on_shutdown() {
  // Bind the listener the accept task will own.
  let listener = <TokioNet as Net>::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind accept listener");
  let addr = listener.local_addr().expect("listener local_addr");

  // A capacity-1 hand-off channel, pre-filled so the task's next send blocks.
  // The slot is occupied by a throwaway connected stream of the right type. The
  // receiver is held (never dropped, never drained) for the whole test: if it
  // were dropped, the send itself would error out and free the task, masking
  // the shutdown-race being tested.
  let (accepted_tx, _accepted_rx) = flume::bounded::<(TokioTcpStream, SocketAddr)>(1);
  let (filler, _filler_peer) = loopback_pair().await;
  accepted_tx
    .try_send((filler, "127.0.0.1:1".parse().unwrap()))
    .expect("pre-fill the capacity-1 hand-off channel");

  // An established inbound connection, so the task's `accept()` is immediately
  // ready and it proceeds straight into the (blocking) hand-off send.
  let _inbound = TokioTcpStream::connect(addr)
    .await
    .expect("inbound connect");

  let (shutdown_tx, shutdown_rx) = flume::bounded::<()>(1);
  let shared = test_shared();
  let join = TokioRuntime::spawn(accept_task::<SmolStr, _>(
    listener,
    accepted_tx,
    shutdown_rx,
    shared,
  ));

  // Step the task while its shutdown channel is still open. On this
  // single-threaded runtime that runs it until it parks — which it can only do
  // blocked on the full-channel send, since `accept()` is ready and consumed
  // and the shutdown arm is not yet signalled. (The receiver is never drained,
  // so the send can never complete on its own.)
  for _ in 0..16 {
    tokio::task::yield_now().await;
  }

  // Signal shutdown by dropping the sender — the ONLY thing that can free the
  // task here. The raced inner select observes the disconnect and breaks; an
  // un-cancellable send would ignore it and hang (the held receiver keeps the
  // full-channel send pending forever).
  drop(shutdown_tx);

  let exited = tokio::time::timeout(Duration::from_secs(5), join).await;
  assert!(
    exited.is_ok(),
    "the accept task exits on shutdown even with a full hand-off queue"
  );
}

/// On shutdown, an in-flight graceful leave's waiter(s) resolve with
/// `Err(Shutdown)` (the `pending_leave.take()` arm).
///
/// The shutdown branch itself calls `endpoint.leave()` then `drain_surfaces`,
/// and a no-peer leave emits `LeftCluster` within that same drain — which would
/// resolve a parked leave with `Ok(())` before the shutdown arm runs. To isolate
/// the shutdown arm, the endpoint is first driven to fully `Left` (a prior poll
/// consumes its `LeftCluster`), so the shutdown's own `leave()` is an idempotent
/// no-op emitting nothing; the freshly seeded `pending_leave` then survives the
/// drain and is failed with `Shutdown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_leave() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  // Drive the endpoint to Left first: a real leave whose LeftCluster is consumed
  // by this poll (resolving the dispatched waiter with Ok, then removed).
  let (warm_tx, _warm_rx) = oneshot::channel::<Result<(), Error>>();
  shared.push_command(Command::Leave(LeaveCmd { reply: warm_tx }));
  assert!(
    poll_once(&mut driver).is_pending(),
    "warm-up poll keeps running"
  );
  assert!(
    driver.pending_leave.is_none(),
    "the no-peer leave completed within the warm-up poll"
  );
  assert!(
    !driver.endpoint.is_running(),
    "the endpoint is now Left, so the shutdown leave() is a no-op"
  );

  // Now seed a fresh parked leave and shut down: the no-op leave() emits no
  // LeftCluster, so this waiter survives the drain and hits the shutdown arm.
  let (tx, rx) = oneshot::channel::<Result<(), Error>>();
  driver.pending_leave = Some(PendingLeave { repliers: vec![tx] });
  shared.begin_shutdown();
  poll_to_ready(&mut driver).await;
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked leave waiter is failed with Shutdown on driver exit"
  );
}

/// The reliable-send / join capture keys on the originating `StreamId`, NOT
/// the peer. Regression guard for the cross-subsystem misattribution: a
/// same-peer dial flushed by the shared `service_dials` for another subsystem
/// (here a `start_push_pull`) is left pending while a `send_reliable`-style
/// `start_user_message` runs; the command must capture ONLY its own exchange.
/// If capture matched by peer (the bug), the foreign push/pull's `ExchangeId`
/// would be captured too — `m > n` (`checked_sub` underflow) or a waiter parked
/// on a PushPull completion the `UserMessage` resolver ignores (leaked waiter).
#[test]
fn capture_binds_to_started_stream_id_not_peer() {
  let mut endpoint = capture_test_endpoint();
  let now = Instant::now();
  let peer = "127.0.0.1:7000".parse::<SocketAddr>().unwrap();

  // An UNRELATED same-peer dial enqueued by another subsystem; its Connect is
  // already queued by the in-band `service_dials` and deliberately left
  // undrained — the exact shared-deque hazard `send_many_reliable` faces.
  let foreign_sid = endpoint.start_push_pull(peer, PushPullKind::Join, now);

  // This command's own dial to the SAME peer; capture is keyed on `started`.
  let mut started: HashSet<StreamId> = HashSet::new();
  let user_sid = endpoint
    .start_user_message(peer, Bytes::from_static(b"hi"), now)
    .expect("issued while running");
  started.insert(user_sid);
  assert_ne!(
    foreign_sid, user_sid,
    "distinct dials get distinct StreamIds"
  );

  // Drain every queued action, replicating the dispatch loop's inline capture
  // predicate exactly. Both Connects surface; only the one in `started` is
  // captured.
  let mut captured: HashSet<ExchangeId> = HashSet::new();
  let mut connect_count = 0usize;
  let mut foreign_eid = None;
  let mut user_eid = None;
  while let Some(action) = endpoint.poll_action() {
    if let StreamAction::Connect(ref info) = action {
      connect_count += 1;
      if info.stream_id() == foreign_sid {
        foreign_eid = Some(info.id());
      }
      if info.stream_id() == user_sid {
        user_eid = Some(info.id());
      }
      if started.contains(&info.stream_id()) {
        captured.insert(info.id());
      }
    }
  }

  assert_eq!(
    connect_count, 2,
    "both same-peer dials surfaced a Connect in this drain",
  );
  let foreign_eid = foreign_eid.expect("foreign push/pull surfaced a Connect");
  let user_eid = user_eid.expect("user-message surfaced a Connect");
  assert_eq!(
    captured.len(),
    1,
    "exactly this command's one exchange was captured (m <= n holds)",
  );
  assert!(
    captured.contains(&user_eid),
    "the user-message exchange was captured",
  );
  assert!(
    !captured.contains(&foreign_eid),
    "the foreign same-peer push/pull exchange was NOT captured",
  );
}

/// A graceful close (handle drop) must flush every byte already queued in
/// `out_rx` before the bridge exits.
///
/// Reproduces the push/pull final-response shape: the peer half-closes its
/// write side (request EOF), so the bridge enters its read-EOF mode and stays
/// alive to write the reply. The reply is then queued and the whole handle
/// dropped — `out_tx` AND `cancel_tx` disconnect together. The peer must
/// receive the full reply, then EOF. If the cancel disconnect were treated as
/// an abort (the bug), `write_cancellable` would preempt the reply and the peer
/// would read `UnexpectedEof` / a truncated body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_close_drains_queued_bytes_before_exit() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
  let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();
  let shared = test_shared();

  let response = b"the-final-response-bytes".to_vec();

  // A long `close_timeout` so this graceful-drain test exercises the FIFO
  // drain, never the timeout backstop (the peer reads promptly here).
  let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    shared,
    Duration::from_secs(60),
  ));

  // The peer sends its request, then half-closes its write side. The bridge
  // reads the request, forwards it, then observes the FIN and enters read-EOF
  // mode (the half-open state where it writes the reply).
  client.write_all(b"request").await.expect("write request");
  client
    .shutdown(Shutdown::Write)
    .expect("half-close client write side");

  // Wait until the bridge has reported the request EOF: this proves it reached
  // read-EOF mode, the exact clean state the graceful close must drain from.
  loop {
    match inbound_rx
      .recv_async()
      .await
      .expect("bridge reports inbound")
    {
      BridgeInbound::Eof(_) | BridgeInbound::Error(_) => break,
      BridgeInbound::Data(_) => continue,
    }
  }

  // Queue the reply, then drop the whole handle: the graceful-Close shape —
  // `out_tx` and `cancel_tx` both disconnect with the reply already queued.
  out_tx
    .send(BridgeOut::Data(Bytes::from(response.clone())))
    .expect("queue response bytes");
  drop((out_tx, cancel_tx));

  // The peer must read the full reply before EOF.
  let mut got = vec![0u8; response.len()];
  client
    .read_exact(&mut got)
    .await
    .expect("peer reads the full response before EOF");
  assert_eq!(
    got, response,
    "graceful close must flush the queued response; a cancel disconnect must \
       not truncate the drain"
  );

  // After the reply the bridge exits and shuts the socket: the peer sees EOF.
  let tail = client
    .read_to_end(&mut Vec::new())
    .await
    .expect("read tail after response");
  assert_eq!(tail, 0, "no bytes after the response, only EOF");

  bridge.await.expect("bridge task exits cleanly");
}

/// An explicit abort (`cancel_tx.send(())`, a FAILED exchange's
/// `StreamAction::Abort`) must preempt and DISCARD the bytes still queued in
/// `out_rx` — stale bytes (possibly encoded under a superseded policy) never
/// reach the wire. Keeping `out_tx` alive proves the ONLY teardown signal is
/// the explicit abort, not a disconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_abort_preempts_and_discards() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
  let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();
  let shared = test_shared();

  let stale = b"stale-bytes-that-must-not-reach-the-wire".to_vec();

  // Queue stale bytes and pre-arm the explicit abort BEFORE the task runs, so
  // the bridge's first write race sees a ready cancel and breaks without
  // writing. Keep `out_tx` alive: the abort send is the sole teardown signal.
  out_tx
    .send(BridgeOut::Data(Bytes::from(stale)))
    .expect("queue stale bytes");
  cancel_tx.send(()).expect("signal explicit abort");
  let _out_tx_kept = out_tx;

  // A long `close_timeout`: the abort, not the backstop, must drive teardown.
  let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    shared,
    Duration::from_secs(60),
  ));

  // The peer must see EOF with NO bytes — the abort dropped the write side
  // without flushing the stale queue.
  let n = client
    .read_to_end(&mut Vec::new())
    .await
    .expect("read peer side to EOF");
  assert_eq!(n, 0, "explicit abort must discard queued bytes, got {n}");

  bridge.await.expect("bridge task exits cleanly");
}

/// A post-Close graceful drain whose peer STOPPED reading must be reclaimed by
/// `close_timeout` — it has NO remaining cancel path, so without the timeout
/// backstop the bridge's drain blocks FOREVER, leaking the detached task and
/// its socket. A fully stalled peer makes NO progress, so the idle
/// `close_timeout` fires and reclaims it.
///
/// Shape: the peer sends its request then half-closes (the bridge enters
/// read-EOF mode), an oversized reply is queued, and the whole handle is
/// dropped — the exact `StreamAction::Close` ordering. The peer is then held
/// but NEVER read, so its receive window collapses to zero and the drain
/// stalls once the kernel buffers fill. With no cancel path the ONLY teardown
/// is the `close_timeout` backstop.
///
/// The outer `close_timeout * 5` bound fails fast: without the backstop the
/// drain would block forever and trip it; with it the bridge reclaims within
/// ~`close_timeout`. A SHORT `close_timeout` keeps the test fast.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_close_drain_bounded_by_close_timeout_when_peer_stalls() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
  let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();
  let shared = test_shared();

  let close_timeout = Duration::from_millis(300);

  // A reply far larger than any kernel socket buffer: with the peer never
  // reading, its window collapses to zero and the drain blocks once the
  // send/recv buffers fill — it cannot complete unless the peer reads.
  let response = vec![0xCDu8; 16 * 1024 * 1024];

  let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    shared,
    close_timeout,
  ));

  // The peer sends its request then half-closes its write side, driving the
  // bridge into read-EOF mode (the half-open state from which a graceful Close
  // drains the queued reply).
  client.write_all(b"request").await.expect("write request");
  client
    .shutdown(Shutdown::Write)
    .expect("half-close client write side");

  // Wait until the bridge reports the request EOF: it is now in read-EOF mode,
  // the exact clean state from which a graceful Close drains.
  loop {
    match inbound_rx
      .recv_async()
      .await
      .expect("bridge reports inbound")
    {
      BridgeInbound::Eof(_) | BridgeInbound::Error(_) => break,
      BridgeInbound::Data(_) => continue,
    }
  }

  // Queue the oversized reply and drop the whole handle: the graceful-Close
  // shape (`out_tx` and `cancel_tx` both disconnect, the reply queued). No
  // cancel can ever be sent now — only `close_timeout` can reclaim the bridge.
  out_tx
    .send(BridgeOut::Data(Bytes::from(response)))
    .expect("queue oversized response");
  drop((out_tx, cancel_tx));

  // The peer is NEVER read: the drain stalls on a zero window. The bridge must
  // reclaim within ~`close_timeout`; the generous `close_timeout * 5` outer
  // bound fails fast — without the backstop the drain blocks forever and trips it.
  tokio::time::timeout(close_timeout * 5, bridge)
    .await
    .expect("bridge reclaims within ~close_timeout, not blocking forever on a stalled drain")
    .expect("bridge task exits cleanly");
}

/// A peer that reads SLOWLY but CONTINUOUSLY must receive the WHOLE reply, even
/// when the total drain outlasts `close_timeout`.
///
/// `close_timeout` is a NO-PROGRESS (idle) bound, not a cap on total write
/// duration. The peer here reads the oversized reply in chunks with a small
/// delay between each, so the writer is gated by the reader: the total drain
/// greatly EXCEEDS `close_timeout`, yet each chunk is read well within it, so no
/// single partial write ever goes idle for the full `close_timeout`.
///
/// A total-duration cap would instead race the ENTIRE write against a single
/// `R::sleep(close_timeout)`; a drain that takes longer than `close_timeout`
/// overall (this slow reader) would lose that race mid-transfer, the bridge
/// would tear down, and the socket would be RST — the peer's `read_exact` then
/// failing with `UnexpectedEof` on the truncated body. The idle bound instead
/// re-arms on each partial write, so progress keeps resetting it and the full
/// reply is delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_but_progressing_reader_is_not_timed_out() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
  let (inbound_tx, inbound_rx) = flume::unbounded::<BridgeInbound>();
  let shared = test_shared();

  // SHORT idle bound: the slow reader's TOTAL drain (below) runs several times
  // longer, while no single partial write ever goes idle this long.
  let close_timeout = Duration::from_millis(250);

  // A reply far larger than the kernel socket buffers, so the writer cannot
  // dump it all and finish instantly — it stays gated by the slow reader, so
  // the TOTAL drain wall-clock greatly outlasts `close_timeout`.
  let len = 16 * 1024 * 1024;
  let response: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();

  let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    shared,
    close_timeout,
  ));

  // The peer sends its request then half-closes its write side, driving the
  // bridge into read-EOF mode (the half-open state from which a graceful Close
  // drains the queued reply).
  client.write_all(b"request").await.expect("write request");
  client
    .shutdown(Shutdown::Write)
    .expect("half-close client write side");

  // Wait until the bridge reports the request EOF: it is now in read-EOF mode,
  // the exact clean state from which a graceful Close drains.
  loop {
    match inbound_rx
      .recv_async()
      .await
      .expect("bridge reports inbound")
    {
      BridgeInbound::Eof(_) | BridgeInbound::Error(_) => break,
      BridgeInbound::Data(_) => continue,
    }
  }

  // Queue the reply and drop the whole handle: the graceful-Close shape
  // (`out_tx` and `cancel_tx` both disconnect, the reply queued). No cancel can
  // ever be sent now — only the idle `close_timeout` could reclaim the bridge,
  // and it must NOT while the peer keeps reading.
  out_tx
    .send(BridgeOut::Data(Bytes::from(response.clone())))
    .expect("queue response bytes");
  drop((out_tx, cancel_tx));

  // Read the whole reply SLOWLY but CONTINUOUSLY: 2 MiB chunks with a 60ms gap
  // between reads. The chunk is large enough (~ the kernel buffer) that the
  // writer never gets more than one sleep ahead, so no single partial write
  // goes idle for `close_timeout`; yet with eight chunks the TOTAL drain runs
  // ~0.5s, several times the 250ms idle bound. A total-duration cap would lose
  // its race against the ~0.5s drain mid-transfer and RST, so this `read_exact`
  // would see `UnexpectedEof` on a truncated body; the idle bound re-arms on
  // each chunk, so the full body arrives.
  let chunk = 2 * 1024 * 1024;
  let mut got = vec![0u8; len];
  let mut off = 0;
  while off < len {
    let end = (off + chunk).min(len);
    client
      .read_exact(&mut got[off..end])
      .await
      .expect("slow reader receives the full reply, never a truncated body");
    off = end;
    tokio::time::sleep(Duration::from_millis(60)).await;
  }

  assert_eq!(
    got, response,
    "a slow-but-progressing reader must receive the exact reply, not a \
       truncated or corrupted body"
  );

  // After the full reply the bridge exits and shuts the socket: the peer sees
  // EOF.
  let tail = client
    .read_to_end(&mut Vec::new())
    .await
    .expect("read tail after reply");
  assert_eq!(tail, 0, "no bytes after the reply, only EOF");

  bridge.await.expect("bridge task exits cleanly");
}

/// `BridgeOut::ShutdownWrite` (the machine's `StreamAction::Shutdown`,
/// half-closing the write side after the send half retires) closes the bridge's
/// write half — the peer reads EOF on its read side — while the bridge stays
/// alive for the still-open read direction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_shutdown_write_half_closes_write_side() {
  let (server, mut client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
  let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();
  let shared = test_shared();

  let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    shared,
    Duration::from_secs(60),
  ));

  // Half-close the bridge's write side (FIN). The peer's read side sees EOF.
  out_tx
    .send(BridgeOut::ShutdownWrite)
    .expect("queue write half-close");
  let tail = client
    .read_to_end(&mut Vec::new())
    .await
    .expect("peer reads its read side to EOF after the bridge FIN");
  assert_eq!(tail, 0, "the write half-close delivers EOF with no bytes");

  // The bridge is still alive (read side open); dropping the handle tears it
  // down cleanly.
  drop((out_tx, cancel_tx));
  bridge.await.expect("bridge task exits cleanly");
}

/// A bridge write that fails (the peer dropped its whole socket, so the write
/// gets a broken pipe / connection reset) tears the bridge down rather than
/// spinning — `write_cancellable` returns the tear-down signal on a write error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_write_error_tears_down() {
  let (server, client) = loopback_pair().await;
  let eid = fresh_eid();
  let (out_tx, out_rx) = flume::unbounded::<BridgeOut>();
  let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
  let (inbound_tx, _inbound_rx) = flume::unbounded::<BridgeInbound>();
  let shared = test_shared();

  // Drop the peer entirely: its socket is gone (RST on subsequent writes).
  drop(client);

  let bridge = tokio::spawn(bridge_task::<SmolStr, TokioRuntime, TokioTcpStream>(
    server,
    eid,
    out_rx,
    cancel_rx,
    inbound_tx,
    shared,
    Duration::from_secs(60),
  ));

  // Keep the handle alive and keep queuing writes: the bridge's read side first
  // sees EOF (peer gone) and enters read-EOF mode, then a queued write to the
  // dead peer eventually errors, returning the tear-down signal. The bridge must
  // exit on its own (broken socket), NOT hang, even though the handle is held.
  let _out_tx_kept = out_tx.clone();
  let _cancel_kept = cancel_tx;
  for _ in 0..64 {
    // Ignoring Err: once the bridge tears down, out_rx disconnects; the test's
    // assertion is that the bridge EXITS, which the timeout below enforces.
    if out_tx
      .send(BridgeOut::Data(Bytes::from(vec![0u8; 64 * 1024])))
      .is_err()
    {
      break;
    }
  }

  tokio::time::timeout(Duration::from_secs(10), bridge)
    .await
    .expect("bridge tears down on a write error to a dropped peer, not hang")
    .expect("bridge task exits cleanly");
}

/// A `recv_batch` of 0 is clamped to 1 at construction. Without the clamp the
/// gossip-recv and inbound-transport loops would do no work yet still set `more`
/// (the `recv_n == recv_batch` self-wake arms), pegging the executor in a
/// busy-loop that never receives gossip or bridge data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recv_batch_zero_clamps_to_one() {
  let socket = <TokioNet as Net>::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind gossip socket");
  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("drv"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let endpoint: StreamEndpoint<SmolStr, SocketAddr, RawRecords> = StreamEndpoint::new(
    ep,
    LabelOptions::new_in(None, ()),
    Box::new(|_| None),
    Box::new(|a: &SocketAddr| *a),
  );
  let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));
  let obs_payload_bytes = Arc::new(AtomicU64::new(0));
  let (obs_tx, _obs_rx) = flume::bounded(16);
  let (accepted_tx, accepted_rx) = flume::bounded(ACCEPT_CAP);
  let (accept_shutdown_tx, accept_shutdown_rx) = flume::bounded(1);
  let listener = <TokioNet as Net>::TcpListener::bind("127.0.0.1:0")
    .await
    .expect("bind accept listener");
  let accept_join = TokioRuntime::spawn(accept_task::<SmolStr, _>(
    listener,
    accepted_tx,
    accept_shutdown_rx,
    shared.clone(),
  ));
  let driver = StreamDriver::<SmolStr, TokioRuntime, RawRecords>::new(
    endpoint,
    socket,
    shared,
    0, // recv_batch
    8, // transmit_batch
    obs_tx,
    obs_payload_bytes,
    None,
    accepted_rx,
    accept_shutdown_tx,
    accept_join,
    Duration::from_secs(60),
    None,
    #[cfg(feature = "cidr")]
    None,
    #[cfg(not(feature = "cidr"))]
    (),
  );
  assert_eq!(
    driver.recv_batch, 1,
    "a recv_batch of 0 must be clamped to 1 to avoid a receive busy-loop"
  );
}
