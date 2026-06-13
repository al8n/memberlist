use std::{
  sync::atomic::AtomicBool,
  task::{Context, Wake, Waker},
  thread,
};

use agnostic::{net::Net, tokio::TokioRuntime};
use memberlist_proto::{
  ChecksumOptions, CompressionOptions, EncryptionOptions, Node, QuicOptions, UnreliableTransport,
  config::EndpointOptions,
  endpoint::Endpoint,
  event::{Reliability, UserPacket},
  typed::{NodeState, State},
};
use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use smol_str::SmolStr;

use super::*;
use crate::command::{
  JoinCmd, LeaveCmd, PingCmd, SendReliableCmd, SendUserCmd, SetChecksumOptionsCmd,
  SetCompressionOptionsCmd, SetEncryptionOptionsCmd, ShutdownCmd,
};

type TokioNet = <TokioRuntime as Runtime>::Net;

const ALPN: &[u8] = b"memberlist-quic-cov";

/// A self-signed-and-self-trusted `QuicOptions` (QUIC-datagram unreliable
/// transport) for the in-process driver. Mirrors the smoke-test fixtures; these
/// driver tests never actually establish a connection, so a single self-trusted
/// identity suffices.
fn self_trusted_quic() -> QuicOptions {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen");
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");

  let provider = Arc::new(rustls::crypto::ring::default_provider());
  let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&rustls::version::TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .expect("server cert");
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
  QuicOptions::new(
    endpoint_cfg,
    server_cfg,
    client_cfg,
    transport,
    "localhost",
    UnreliableTransport::Datagram,
  )
}

/// A harmless waker (safe `std::task::Wake`, no `unsafe`).
fn flag_waker() -> Waker {
  struct W(Arc<AtomicBool>);
  impl Wake for W {
    fn wake(self: Arc<Self>) {
      self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
      self.0.store(true, Ordering::SeqCst);
    }
  }
  Waker::from(Arc::new(W(Arc::new(AtomicBool::new(false)))))
}

/// An app-data `UserPacket` of `len` payload bytes (drives the obs byte
/// backstop; a no-op for `account_event`).
fn user_packet(len: usize) -> Event<SmolStr, SocketAddr> {
  Event::UserPacket(UserPacket::new(
    "127.0.0.1:2".parse::<SocketAddr>().unwrap(),
    Bytes::from(vec![0xABu8; len]),
    Reliability::Reliable,
  ))
}

/// A control event carrying no app-data and (with no parked state) a no-op for
/// `account_event`.
fn control_event() -> Event<SmolStr, SocketAddr> {
  Event::NodeJoined(Arc::new(NodeState::new(
    SmolStr::new("ctl"),
    "127.0.0.1:3".parse::<SocketAddr>().unwrap(),
    State::Alive,
  )))
}

/// Builds a real `QuicDriver` over a bound gossip socket with a caller-supplied
/// observation channel, so the obs-backstop and shutdown branches can be driven
/// directly. Returns the driver, the obs receiver (drop it for `Disconnected`),
/// the shared state, and the payload-byte counter.
async fn build_driver(
  obs_cap: usize,
  obs_budget: Option<u64>,
) -> (
  QuicDriver<SmolStr, TokioRuntime>,
  flume::Receiver<Event<SmolStr, SocketAddr>>,
  Arc<Shared<SmolStr>>,
  Arc<AtomicU64>,
) {
  let socket = <TokioNet as Net>::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind gossip socket");
  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("qdrv"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let mut endpoint = QuicEndpoint::new(ep, self_trusted_quic());
  endpoint.start_scheduling(Instant::now());
  let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));
  let obs_payload_bytes = Arc::new(AtomicU64::new(0));
  let (obs_tx, obs_rx) = flume::bounded(obs_cap);
  let driver = QuicDriver::<SmolStr, TokioRuntime>::new(
    endpoint,
    socket,
    shared.clone(),
    8,
    8,
    obs_tx,
    obs_payload_bytes.clone(),
    obs_budget,
    None,
    #[cfg(feature = "cidr")]
    None,
    #[cfg(not(feature = "cidr"))]
    (),
  );
  (driver, obs_rx, shared, obs_payload_bytes)
}

/// Drives one `Future::poll` with a harmless waker.
fn poll_once(driver: &mut QuicDriver<SmolStr, TokioRuntime>) -> Poll<()> {
  let waker = flag_waker();
  let mut cx = Context::from_waker(&waker);
  Pin::new(driver).poll(&mut cx)
}

/// `send_observation`'s byte backstop drops (and counts) a payload event that
/// would push the queued payload bytes over budget; it is never retained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_byte_backstop_drops_oversized_payload() {
  let (mut driver, _obs_rx, shared, bytes) = build_driver(16, Some(4)).await;
  driver.send_observation(user_packet(8));
  assert_eq!(
    shared.observation_dropped(),
    1,
    "over-budget payload dropped + counted"
  );
  assert_eq!(
    bytes.load(Ordering::Relaxed),
    0,
    "a dropped payload reserves no bytes"
  );
  assert!(
    driver.obs_overflow.is_empty(),
    "a byte-backstop drop retains nothing"
  );
}

/// A FULL obs channel RETAINS application data (still byte-reserved) for retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_full_channel_retains_app_data() {
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
    "app-data retained on a full channel"
  );
  assert_eq!(
    shared.observation_dropped(),
    0,
    "a retained event is not a drop"
  );
  assert_eq!(
    bytes.load(Ordering::Relaxed),
    4 + 7,
    "both payloads stay byte-reserved"
  );
}

/// A FULL obs channel DROPS (and counts) a recoverable control event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_full_channel_drops_recoverable_control() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(1, Some(1 << 20)).await;
  driver.send_observation(control_event()); // fills the channel
  driver.send_observation(control_event()); // full → dropped + counted
  assert!(
    driver.obs_overflow.is_empty(),
    "a control event is never retained"
  );
  assert_eq!(
    shared.observation_dropped(),
    1,
    "the dropped control event is counted"
  );
}

/// With the obs task gone, `send_observation` rolls back its reservation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn obs_disconnected_rolls_back_reservation() {
  let (mut driver, obs_rx, shared, bytes) = build_driver(16, Some(1 << 20)).await;
  drop(obs_rx); // the only receiver is gone → the driver's sender sees Disconnected
  driver.send_observation(user_packet(9));
  assert_eq!(
    bytes.load(Ordering::Relaxed),
    0,
    "Disconnected rolls back the reservation"
  );
  assert!(
    driver.obs_overflow.is_empty(),
    "Disconnected retains nothing"
  );
  assert_eq!(
    shared.observation_dropped(),
    0,
    "Disconnected is not a recoverable drop"
  );
}

/// `flush_obs_overflow` stops at the first `Full`, re-pushing to the front.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_overflow_stops_and_repushes_on_full() {
  let (mut driver, _obs_rx, _shared, _bytes) = build_driver(1, Some(1 << 20)).await;
  driver
    .obs_tx
    .try_send(control_event())
    .expect("seed the channel full");
  driver.obs_overflow.push_back(control_event());
  driver.obs_overflow.push_back(control_event());
  driver.flush_obs_overflow();
  assert_eq!(
    driver.obs_overflow.len(),
    2,
    "flush stops at the first Full and re-pushes"
  );
}

/// `flush_obs_overflow` with the obs task gone reclaims retained payload bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_overflow_disconnected_reclaims_bytes() {
  let (mut driver, obs_rx, _shared, bytes) = build_driver(16, Some(1 << 20)).await;
  bytes.store(6, Ordering::Relaxed);
  driver.obs_overflow.push_back(user_packet(6));
  drop(obs_rx); // the only receiver is gone → the flush sees Disconnected
  driver.flush_obs_overflow();
  assert!(
    driver.obs_overflow.is_empty(),
    "a Disconnected flush drains the overflow"
  );
  assert_eq!(
    bytes.load(Ordering::Relaxed),
    0,
    "a Disconnected flush reclaims the bytes"
  );
}

/// On shutdown, a parked `WaitForCompletion` join is failed with `Shutdown`
/// (the `pending_joins.drain()` arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_join() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<usize, Error>>();
  shared.push_command(Command::Join(JoinCmd {
    addrs: vec!["127.0.0.1:9".parse::<SocketAddr>().unwrap()],
    wait: true,
    reply: tx,
  }));
  shared.begin_shutdown();
  assert!(poll_once(&mut driver).is_ready());
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked wait-join is failed with Shutdown on driver exit"
  );
}

/// On shutdown, a parked application-ping is failed with `Shutdown` (the
/// `pending_pings.drain()` arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_ping() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<Duration, Error>>();
  let node = Node::new(
    SmolStr::new("peer"),
    "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
  );
  shared.push_command(Command::Ping(PingCmd { node, reply: tx }));
  shared.begin_shutdown();
  assert!(poll_once(&mut driver).is_ready());
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked ping is failed with Shutdown on driver exit"
  );
}

/// On shutdown, a parked reliable directed send is failed with `Shutdown` (the
/// `pending_user_sends.drain()` arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_reliable_send() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  shared.push_command(Command::SendReliable(SendReliableCmd {
    to: "127.0.0.1:9".parse::<SocketAddr>().unwrap(),
    payloads: vec![Bytes::from_static(b"reliable")],
    reply: tx,
  }));
  shared.begin_shutdown();
  assert!(poll_once(&mut driver).is_ready());
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked reliable send is failed with Shutdown on driver exit"
  );
}

/// On shutdown, an in-flight graceful leave's waiter(s) resolve with `Shutdown`
/// (the `pending_leave.take()` arm). The endpoint is first driven to `Left` so
/// the shutdown's own no-op `leave()` emits no `LeftCluster` that would resolve
/// the seeded waiter early; it then survives the drain to the shutdown arm.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_fails_parked_leave() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (warm_tx, _warm_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  shared.push_command(Command::Leave(LeaveCmd { reply: warm_tx }));
  assert!(
    poll_once(&mut driver).is_pending(),
    "warm-up poll keeps running"
  );
  assert!(
    driver.pending_leave.is_none(),
    "the no-peer leave completed in the warm-up"
  );
  assert!(
    !driver.endpoint.is_running(),
    "the endpoint is Left; shutdown leave() is a no-op"
  );

  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  driver.pending_leave = Some(PendingLeave { repliers: vec![tx] });
  shared.begin_shutdown();
  assert!(poll_once(&mut driver).is_ready());
  assert!(
    matches!(rx.await, Ok(Err(Error::Shutdown))),
    "a parked leave waiter is failed with Shutdown on driver exit"
  );
}

/// The shutdown branch's `close_and_drain` fails EVERY queued command variant
/// with `Shutdown`. Every command type is queued while the driver still
/// accepts pushes, then shutdown begins; the next poll runs `close_and_drain`,
/// which takes the whole queue and replies `Shutdown` to each replier. The
/// five distinguishable variants (join, user, compression, checksum,
/// encryption) are checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_close_and_drain_fails_every_queued_command() {
  use std::sync::Barrier;

  const MAX_ATTEMPTS: usize = 20000;
  // Each command variant must, in SOME attempt, be pushed into the narrow
  // window between the poll's top-of-poll `drain_commands` and its
  // `close_and_drain`, so that `close_and_drain` (not normal dispatch) fails
  // it with `Shutdown`. The window is racy, so accumulate per-variant rather
  // than demanding all five in one attempt: every variant lands in it within
  // the bound, and the loop breaks as soon as all five have been observed.
  // The push order is rotated each attempt so no variant is starved by always
  // racing from the same position.
  let mut seen_join = false;
  let mut seen_user = false;
  let mut seen_comp = false;
  let mut seen_chk = false;
  let mut seen_enc = false;
  for attempt in 0..MAX_ATTEMPTS {
    let (mut driver, _obs_rx, shared, _bytes) = build_driver(64, Some(1 << 20)).await;
    shared.begin_shutdown();

    let (join_tx, join_rx) = futures_channel::oneshot::channel::<Result<usize, Error>>();
    let (user_tx, user_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (comp_tx, comp_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (chk_tx, chk_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (enc_tx, enc_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (leave_tx, _leave_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (shutdown_tx, _shutdown_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (ping_tx, _ping_rx) = futures_channel::oneshot::channel::<Result<Duration, Error>>();
    let (rel_tx, _rel_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();

    let to = "127.0.0.1:9".parse::<SocketAddr>().unwrap();
    let node = Node::new(SmolStr::new("peer"), to);
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
        opts: CompressionOptions::new(),
        reply: comp_tx,
      }),
      Command::SetChecksumOptions(SetChecksumOptionsCmd {
        opts: ChecksumOptions::new(),
        reply: chk_tx,
      }),
      Command::SetEncryptionOptions(SetEncryptionOptionsCmd {
        opts: EncryptionOptions::new(),
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
        // Ignoring bool: a push rejected after the queue closed just means
        // this attempt missed the window for that command; the outer loop
        // retries and another attempt will catch it.
        let _ = pusher_shared.push_command(cmd);
      }
    });

    barrier.wait();
    assert!(
      poll_once(&mut driver).is_ready(),
      "a shutdown poll returns Ready"
    );
    pusher.join().expect("pusher thread joins");

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
/// when the second `Shutdown` dispatched in the same drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_acks_every_same_poll_caller() {
  let (mut driver, _obs_rx, shared, _bytes) = build_driver(16, Some(1 << 20)).await;

  let (first_tx, first_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  let (second_tx, second_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  // Both land in one top-of-poll drain, so both dispatch (and park their reply)
  // before the shutdown branch acks.
  shared.push_command(Command::Shutdown(ShutdownCmd { reply: first_tx }));
  shared.push_command(Command::Shutdown(ShutdownCmd { reply: second_tx }));

  assert!(
    poll_once(&mut driver).is_ready(),
    "a shutdown poll returns Ready"
  );
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
  use std::sync::Barrier;

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

    let (first_tx, first_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    let (second_tx, second_rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
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
    assert!(
      poll_once(&mut driver).is_ready(),
      "a shutdown poll returns Ready"
    );
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
