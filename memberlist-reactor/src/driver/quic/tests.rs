use std::{
  sync::atomic::AtomicBool,
  task::{Context, Wake, Waker},
  thread,
};

use agnostic::{RuntimeLite, net::Net, tokio::TokioRuntime};
#[cfg(checksum)]
use memberlist_proto::ChecksumOptions;
#[cfg(compression)]
use memberlist_proto::CompressionOptions;
#[cfg(encryption)]
use memberlist_proto::EncryptionOptions;
use memberlist_proto::{
  Node, QuicOptions, UnreliableTransport,
  codec::{EncodeOptions, encode_outgoing},
  config::EndpointOptions,
  endpoint::Endpoint,
  event::{Reliability, UserPacket},
  typed::{Alive, Message, NodeState, State, Suspect},
};
use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use smol_str::SmolStr;

use super::*;
#[cfg(all(compression, checksum, encryption))]
use crate::command::SendUserCmd;
#[cfg(checksum)]
use crate::command::SetChecksumOptionsCmd;
#[cfg(compression)]
use crate::command::SetCompressionOptionsCmd;
#[cfg(encryption)]
use crate::command::SetEncryptionOptionsCmd;
use crate::command::{
  JoinCmd, LeaveCmd, PingCmd, QueueUserBroadcastCmd, SendReliableCmd, SetAckPayloadCmd,
  SetLocalStateCmd, ShutdownCmd, UpdateNodeMetadataCmd,
};
use rustls::version::TLS13;
use std::sync::Barrier;

type TokioNet = <TokioRuntime as Runtime>::Net;

const ALPN: &[u8] = b"memberlist-quic-cov";

/// A self-signed-and-self-trusted `QuicOptions` (QUIC-datagram unreliable
/// transport) for the in-process driver. Mirrors the smoke-test fixtures; these
/// driver tests never actually establish a connection, so a single self-trusted
/// identity suffices.
fn self_trusted_quic() -> QuicOptions {
  let hmac = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &[0x5au8; 32]);
  self_trusted_quic_with_endpoint_cfg(EndpointConfig::new(Arc::new(hmac)))
}

/// Like [`self_trusted_quic`] but with a caller-supplied `EndpointConfig`, so a
/// test can raise `max_udp_payload_size` above quinn's 1472 default (up to its
/// 65527 ceiling) to exercise `recv_buf_len`'s QUIC-size floor.
fn self_trusted_quic_with_endpoint_cfg(endpoint_cfg: EndpointConfig) -> QuicOptions {
  let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("rcgen");
  let cert = CertificateDer::from(ck.cert.der().to_vec());
  let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");

  let provider = Arc::new(rustls::crypto::ring::default_provider());
  let mut rustls_server = rustls::ServerConfig::builder_with_provider(provider.clone())
    .with_protocol_versions(&[&TLS13])
    .expect("TLS 1.3")
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .expect("server cert");
  rustls_server.alpn_protocols = vec![ALPN.to_vec()];
  let qsc = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
    .expect("QuicServerConfig");
  let server_cfg = ServerConfig::with_crypto(Arc::new(qsc));

  let mut rustls_client = rustls::ClientConfig::builder_with_provider(provider)
    .with_protocol_versions(&[&TLS13])
    .expect("TLS 1.3")
    .with_root_certificates(roots)
    .with_no_client_auth();
  rustls_client.alpn_protocols = vec![ALPN.to_vec()];
  let qcc = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
    .expect("QuicClientConfig");
  let client_cfg = ClientConfig::new(Arc::new(qcc));

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
  build_driver_with_quic(obs_cap, obs_budget, self_trusted_quic()).await
}

/// Like [`build_driver`] but with a caller-supplied `QuicOptions`, so a test can
/// set a low `max_pending_user_dials_per_peer` to exercise the per-peer reliable
/// user-message dial backlog ceiling cheaply.
async fn build_driver_with_quic(
  obs_cap: usize,
  obs_budget: Option<u64>,
  quic: QuicOptions,
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
  let mut endpoint = QuicEndpoint::new(ep, quic);
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

/// Build a standalone coordinator with an explicit gossip MTU (no socket), so
/// the recv-buffer sizing can be exercised at the validated `gossip_mtu` floor
/// and well above it.
fn endpoint_with_gossip_mtu(gossip_mtu: usize) -> QuicEndpoint<SmolStr, impl rand::Rng> {
  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("qdrv"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    )
    .with_gossip_mtu(gossip_mtu),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  QuicEndpoint::new(ep, self_trusted_quic())
}

/// At the validated `gossip_mtu` floor (512) the gossip datagram ceiling is far
/// below quinn's max recv UDP payload, so the recv buffer MUST floor at the QUIC
/// max — otherwise inbound QUIC packets (the >= 1200-byte Initial included) are
/// truncated and the handshake never completes. Reverting the `.max(...)` in
/// `recv_buf_len` drops this below the QUIC max and fails the test.
#[test]
fn recv_buf_len_floors_at_quic_max_for_small_gossip_mtu() {
  let ep = endpoint_with_gossip_mtu(512);
  let quic_max = ep.max_recv_udp_payload_size();
  assert!(quic_max >= 1200);
  assert!(
    recv_buf_len(&ep) >= quic_max,
    "recv buffer must accommodate the largest QUIC packet even at the gossip_mtu floor"
  );
}

/// A large `gossip_mtu` keeps the gossip datagram ceiling as the binding size —
/// the QUIC-max floor must not shrink it.
#[test]
fn recv_buf_len_gossip_wins_for_large_gossip_mtu() {
  let ep = endpoint_with_gossip_mtu(60_000);
  let got = recv_buf_len(&ep);
  assert!(got > ep.max_recv_udp_payload_size());
  assert_eq!(
    got,
    60_000 + ENCRYPTED_WRAPPER_OVERHEAD + CHECKSUMED_WRAPPER_OVERHEAD
  );
}

/// A custom `EndpointConfig` can raise `max_udp_payload_size` up to quinn's
/// 65527 ceiling (e.g. for IPv6 jumbograms), which sits ABOVE
/// `GOSSIP_RECV_BUF_MAX` (65507, the IPv4 UDP payload limit). At the default
/// gossip MTU the QUIC size is the binding term, and `recv_buf_len` must floor
/// at it WITHOUT the `GOSSIP_RECV_BUF_MAX` cap clamping it back down — the cap
/// applies only to the gossip-derived size. Applying `.min(GOSSIP_RECV_BUF_MAX)`
/// after the `.max(quic)` (the old clamp order) would wrongly truncate this to
/// 65507 and fail the assertion.
#[test]
fn recv_buf_len_floors_at_quic_max_above_gossip_cap() {
  let mut endpoint_cfg = EndpointConfig::new(Arc::new(ring::hmac::Key::new(
    ring::hmac::HMAC_SHA256,
    &[0x5au8; 32],
  )));
  endpoint_cfg
    .max_udp_payload_size(65_527)
    .expect("65527 is quinn's max_udp_payload_size ceiling");
  let quic = self_trusted_quic_with_endpoint_cfg(endpoint_cfg);

  let ep = Endpoint::new(
    EndpointOptions::new(
      SmolStr::new("qdrv"),
      "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let ep = QuicEndpoint::new(ep, quic);

  assert_eq!(ep.max_recv_udp_payload_size(), 65_527);
  let got = recv_buf_len(&ep);
  assert!(
    got >= 65_527,
    "recv buffer must floor at the QUIC receive size even above GOSSIP_RECV_BUF_MAX, got {got}"
  );
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
  let (tx, rx) = futures_channel::oneshot::channel::<crate::command::JoinReply>();
  shared.push_command(Command::Join(JoinCmd {
    addrs: vec!["127.0.0.1:9".parse::<SocketAddr>().unwrap()],
    wait: true,
    reply: tx,
  }));
  shared.begin_shutdown();
  assert!(poll_once(&mut driver).is_ready());
  assert!(
    matches!(rx.await, Ok(Err((set, Error::Shutdown))) if set.is_empty()),
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
#[cfg(all(compression, checksum, encryption))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_close_and_drain_fails_every_queued_command() {
  use Barrier;

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

    let (join_tx, join_rx) = futures_channel::oneshot::channel::<crate::command::JoinReply>();
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

    seen_join |= matches!(join_rx.await, Ok(Err((set, Error::Shutdown))) if set.is_empty());
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
  use Barrier;

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

/// `dispatch` of `SetChecksumOptions` on a RUNNING QUIC node validates the policy
/// and replies `Ok(())` — the success arm the post-leave `NotRunning` lifecycle
/// test never reaches. Checksumming is a gossip-plane concern; the reliable QUIC
/// bridge carries none.
#[cfg(feature = "crc32")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_set_checksum_running_replies_ok() {
  let (mut driver, _obs_rx, _shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  driver.dispatch(
    Command::SetChecksumOptions(SetChecksumOptionsCmd {
      opts: ChecksumOptions::new().with_algorithm(memberlist_proto::ChecksumAlgorithm::Crc32),
      reply: tx,
    }),
    Instant::now(),
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "a built-in checksum algorithm is accepted on a running QUIC node"
  );
}

/// `dispatch` of `UpdateNodeMetadata` on a RUNNING QUIC node builds the validated
/// `Meta` and replies `Ok(())` (the running success arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_update_metadata_running_replies_ok() {
  let (mut driver, _obs_rx, _shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  driver.dispatch(
    Command::UpdateNodeMetadata(UpdateNodeMetadataCmd {
      meta: b"role=web".to_vec(),
      reply: tx,
    }),
    Instant::now(),
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "an in-cap metadata update is applied on a running QUIC node"
  );
}

/// `dispatch` of `QueueUserBroadcast` on a RUNNING QUIC node queues the datagram
/// and replies `Ok(())` (the running success arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_queue_user_broadcast_running_replies_ok() {
  let (mut driver, _obs_rx, _shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  driver.dispatch(
    Command::QueueUserBroadcast(QueueUserBroadcastCmd {
      data: Bytes::from_static(b"hello-cluster"),
      reply: tx,
    }),
    Instant::now(),
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "an in-MTU user broadcast is queued on a running QUIC node"
  );
}

/// `dispatch` of `SetLocalState` on a RUNNING QUIC node stores the push/pull
/// snapshot and replies `Ok(())` (the running success arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_set_local_state_running_replies_ok() {
  let (mut driver, _obs_rx, _shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  driver.dispatch(
    Command::SetLocalState(SetLocalStateCmd {
      state: Bytes::from_static(b"app-snapshot"),
      reply: tx,
    }),
    Instant::now(),
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "an in-budget local-state snapshot is stored on a running QUIC node"
  );
}

/// `dispatch` of `SetAckPayload` on a RUNNING QUIC node stores the probe-ack
/// payload and replies `Ok(())` (the running success arm).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_set_ack_payload_running_replies_ok() {
  let (mut driver, _obs_rx, _shared, _bytes) = build_driver(16, Some(1 << 20)).await;
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  driver.dispatch(
    Command::SetAckPayload(SetAckPayloadCmd {
      payload: Bytes::from_static(b"ack-extra"),
      reply: tx,
    }),
    Instant::now(),
  );
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "an in-budget ack payload is stored on a running QUIC node"
  );
}

/// The per-peer reliable-backlog ceiling reports backpressure — it does NOT
/// panic the driver task. With a low cap, reliable sends to a never-answering
/// peer park until the backlog is full; the next single send is refused with
/// `UserDialBacklogFull` (carrying the peer and the cap), and the task survives
/// to service a follow-up command.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_send_over_cap_returns_backpressure_not_panic() {
  const CAP: usize = 2;
  let (mut driver, _obs_rx, shared, _bytes) = build_driver_with_quic(
    16,
    Some(1 << 20),
    self_trusted_quic().with_max_pending_user_dials_per_peer(CAP),
  )
  .await;
  let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();

  // Fill the peer's backlog to the cap with parked sends to a never-answering
  // peer. Queue them all, then a single poll drains the whole command queue.
  for _ in 0..CAP {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    shared.push_command(Command::SendReliable(SendReliableCmd {
      to: peer,
      payloads: vec![Bytes::from_static(b"park")],
      reply: tx,
    }));
  }
  assert!(
    poll_once(&mut driver).is_pending(),
    "parked sends keep the driver running"
  );
  assert!(
    !driver.endpoint.can_admit_user_dials(peer, 1),
    "the peer's reliable backlog is saturated at the cap"
  );

  // The (cap+1)th single send is refused as backpressure — not a panic.
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  shared.push_command(Command::SendReliable(SendReliableCmd {
    to: peer,
    payloads: vec![Bytes::from_static(b"overflow")],
    reply: tx,
  }));
  assert!(
    poll_once(&mut driver).is_pending(),
    "the driver survives the refusal"
  );
  match rx.await {
    Ok(Err(Error::UserDialBacklogFull(full))) => {
      assert_eq!(full.peer(), peer, "the error names the target peer");
      assert_eq!(full.limit(), CAP, "the error carries the configured cap");
    }
    other => panic!("expected UserDialBacklogFull backpressure, got {other:?}"),
  }

  // The task is still alive: a follow-up command is still serviced (an
  // empty-payload send replies `Ok` immediately), proving no panic.
  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  shared.push_command(Command::SendReliable(SendReliableCmd {
    to: peer,
    payloads: vec![],
    reply: tx,
  }));
  assert!(poll_once(&mut driver).is_pending());
  assert!(
    matches!(rx.await, Ok(Ok(()))),
    "a follow-up command is serviced after the refusal"
  );
}

/// A single `send_many`-style batch of (cap+1) payloads to a fresh peer is
/// refused ATOMICALLY: the whole batch's reply is the backpressure error and NO
/// exchange was started (the peer's backlog is unchanged, so a full-cap batch is
/// admissible again, and no waiter was registered).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_batch_over_cap_rejected_atomically() {
  const CAP: usize = 2;
  let (mut driver, _obs_rx, shared, _bytes) = build_driver_with_quic(
    16,
    Some(1 << 20),
    self_trusted_quic().with_max_pending_user_dials_per_peer(CAP),
  )
  .await;
  let peer: SocketAddr = "127.0.0.1:9".parse().unwrap();

  let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  let payloads: Vec<Bytes> = (0..=CAP).map(|_| Bytes::from_static(b"x")).collect();
  shared.push_command(Command::SendReliable(SendReliableCmd {
    to: peer,
    payloads,
    reply: tx,
  }));
  assert!(poll_once(&mut driver).is_pending());
  assert!(
    matches!(rx.await, Ok(Err(Error::UserDialBacklogFull(_)))),
    "an over-cap batch is refused as backpressure"
  );
  assert!(
    driver.endpoint.can_admit_user_dials(peer, CAP),
    "the atomic refusal started no exchange: the backlog is unchanged"
  );
  assert!(
    driver.pending_user_sends.is_empty(),
    "no waiter was registered for the refused batch"
  );
}

/// Repeated over-cap sends stay bounded and never panic, and the ceiling is
/// per-peer: a peer with headroom is admitted (parks) rather than refused, so a
/// send proceeds wherever reliable capacity is available.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reliable_over_cap_repeated_bounded_and_per_peer() {
  const CAP: usize = 2;
  let (mut driver, _obs_rx, shared, _bytes) = build_driver_with_quic(
    16,
    Some(1 << 20),
    self_trusted_quic().with_max_pending_user_dials_per_peer(CAP),
  )
  .await;
  let full_peer: SocketAddr = "127.0.0.1:9".parse().unwrap();

  for _ in 0..CAP {
    let (tx, _rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    shared.push_command(Command::SendReliable(SendReliableCmd {
      to: full_peer,
      payloads: vec![Bytes::from_static(b"p")],
      reply: tx,
    }));
  }
  assert!(poll_once(&mut driver).is_pending());
  assert!(
    !driver.endpoint.can_admit_user_dials(full_peer, 1),
    "the peer's backlog is saturated"
  );

  // Repeated over-cap sends all return bounded backpressure; the task never
  // panics (each poll stays pending).
  for _ in 0..5 {
    let (tx, rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
    shared.push_command(Command::SendReliable(SendReliableCmd {
      to: full_peer,
      payloads: vec![Bytes::from_static(b"q")],
      reply: tx,
    }));
    assert!(
      poll_once(&mut driver).is_pending(),
      "the driver stays alive under repeated over-cap load"
    );
    assert!(
      matches!(rx.await, Ok(Err(Error::UserDialBacklogFull(_)))),
      "each over-cap send is bounded backpressure"
    );
  }

  // Capacity is per-peer: a peer with headroom is admitted and parks, NOT
  // refused.
  let fresh_peer: SocketAddr = "127.0.0.1:10".parse().unwrap();
  assert!(
    driver.endpoint.can_admit_user_dials(fresh_peer, CAP),
    "a fresh peer has full reliable headroom"
  );
  let (tx, mut rx) = futures_channel::oneshot::channel::<Result<(), Error>>();
  shared.push_command(Command::SendReliable(SendReliableCmd {
    to: fresh_peer,
    payloads: vec![Bytes::from_static(b"r")],
    reply: tx,
  }));
  assert!(poll_once(&mut driver).is_pending());
  assert!(
    matches!(rx.try_recv(), Ok(None)),
    "a send to a peer with capacity is admitted and parks, not refused"
  );
}

/// Build a `QuicDriver` with an explicit `recv_batch` and a preparation hook run
/// on the endpoint before the driver is built. The periodic schedulers are left
/// OFF (no `start_scheduling`) so a staged suspicion timer is the machine's ONLY
/// deadline, and the suspicion timeout is shortened via `suspicion_mult = 1` and
/// the caller's `probe_interval` (its milliseconds are the suspicion timeout at a
/// two-node cluster's unit node-scale), so a test can sleep to the due instant
/// quickly.
async fn build_driver_with(
  obs_cap: usize,
  obs_budget: Option<u64>,
  recv_batch: usize,
  probe_interval: Duration,
  prep: impl FnOnce(&mut QuicEndpoint<SmolStr, rand::rngs::StdRng>),
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
    )
    .with_suspicion_mult(1)
    .with_probe_interval(probe_interval),
    crate::gossip_rng().expect("test: OS entropy"),
  );
  let mut endpoint = QuicEndpoint::new(ep, self_trusted_quic());
  prep(&mut endpoint);
  let shared = Arc::new(Shared::new(snapshot_of(endpoint.endpoint_ref())));
  let obs_payload_bytes = Arc::new(AtomicU64::new(0));
  let (obs_tx, obs_rx) = flume::bounded(obs_cap);
  let driver = QuicDriver::<SmolStr, TokioRuntime>::new(
    endpoint,
    socket,
    shared.clone(),
    recv_batch,
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

/// Stage a suspicion timer on a fresh peer: the machine's next deadline is its
/// suspicion timeout (a fixed multiple of the probe interval, no random
/// stagger), and its fire is OBSERVABLE as the peer turning `Dead` — so a test
/// can distinguish a deferred timeout from a fired one by member state rather
/// than by driver bookkeeping alone.
fn stage_suspicion(
  endpoint: &mut QuicEndpoint<SmolStr, rand::rngs::StdRng>,
  id: &str,
  peer_addr: SocketAddr,
) {
  let now = Instant::now();
  endpoint.handle_alive(
    peer_addr,
    Alive::new(1, Node::new(SmolStr::new(id), peer_addr)),
    now,
  );
  endpoint.handle_suspect(
    peer_addr,
    Suspect::new(1, SmolStr::new(id), SmolStr::new("qdrv")),
    now,
  );
}

/// The peer's gossip-tracked liveness, straight from the machine.
fn peer_state(driver: &QuicDriver<SmolStr, TokioRuntime>, id: &str) -> Option<State> {
  driver
    .endpoint
    .endpoint_ref()
    .member_liveness(&SmolStr::new(id))
}

/// The plain gossip-frame bytes of an `Alive(inc)` for `id`@`peer_addr`, encoded
/// through the PUBLIC codec exactly as the driver's outbound path emits gossip.
/// With no gossip transforms configured on the default endpoint the inbound
/// ingress path (`unwrap_transforms` / `decrypt_gossip`, then `decode_incoming`
/// and `parse_messages`) round-trips it back to the same `Alive`, so a real UDP
/// datagram carrying these bytes decodes and refutes the suspicion.
fn refuting_alive_datagram(id: &str, peer_addr: SocketAddr, inc: u32) -> Vec<u8> {
  let msg: Message<SmolStr, SocketAddr> =
    Message::Alive(Alive::new(inc, Node::new(SmolStr::new(id), peer_addr)));
  encode_outgoing(&msg, &EncodeOptions::new(None))
    .expect("a well-formed Alive encodes")
    .to_vec()
}

/// A DUE suspicion deadline must not fire while the gossip recv is capped: a
/// refuting `Alive(inc+1)` that arrived before the deadline can still be
/// buffered in the kernel socket behind a truncated batch, and firing past it
/// would declare a live peer `Dead` cluster-wide. The pump anchors the deferral
/// and self-wakes to drain instead; on the next poll the buffered `Alive`
/// decodes and refutes the suspicion, so the peer ends `Alive`, never `Dead`.
/// Deferral versus firing is read off the MACHINE (member state), so a
/// `handle_timeout` call that is silently dropped or relocated past the gate
/// fails this test rather than merely re-shuffling driver bookkeeping.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn due_timeout_defers_then_a_buffered_alive_refutes_the_suspicion() {
  let peer_addr: SocketAddr = "127.0.0.1:39990".parse().unwrap();
  // recv_batch == 1: every poll that reads a datagram is capped, so the two
  // preloaded datagrams surface one per poll.
  let (mut driver, _obs_rx, _shared, _bytes) =
    build_driver_with(64, None, 1, Duration::from_millis(40), |endpoint| {
      stage_suspicion(endpoint, "capped-peer", peer_addr);
    })
    .await;

  // Let the suspicion deadline fall due while the pump is unpolled — the machine
  // is passive, so the peer stays Suspect until a poll fires it.
  let due_at = driver
    .endpoint
    .poll_timeout()
    .expect("the staged suspicion arms a machine deadline");
  TokioRuntime::sleep(due_at.saturating_duration_since(Instant::now()) + Duration::from_millis(20))
    .await;

  // Preload the kernel FIFO: one garbage datagram (dropped by classification, but
  // still counts toward the batch) THEN the refuting Alive, from the same sender
  // so loopback delivers them in order.
  let gossip_addr = driver
    .socket
    .as_ref()
    .expect("gossip socket held")
    .local_addr()
    .expect("gossip local addr");
  let tx = <TokioNet as Net>::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind flood socket");
  tx.send_to(&[0x20u8; 24], gossip_addr)
    .await
    .expect("garbage send");
  tx.send_to(
    &refuting_alive_datagram("capped-peer", peer_addr, 2),
    gossip_addr,
  )
  .await
  .expect("alive send");
  // Let the kernel surface both datagrams to the receiving socket.
  TokioRuntime::sleep(Duration::from_millis(50)).await;

  // Poll 1 reads only the garbage (batch capped at 1) — the due deadline DEFERS.
  let _ = poll_once(&mut driver);
  assert!(
    driver.timeout_stall_since.is_some(),
    "a due deadline behind a capped recv batch must defer, not fire"
  );
  assert_eq!(
    peer_state(&driver, "capped-peer"),
    Some(State::Suspect),
    "the deferred fire must leave the machine untouched: the suspicion has not \
     completed while the recv batch is capped"
  );

  // Poll 2 (well inside the 5ms grace) reads the Alive, which decodes and refutes
  // the suspicion BEFORE the timeout region runs — the peer is now Alive, so the
  // deadline can never declare it Dead.
  let _ = poll_once(&mut driver);
  assert_eq!(
    peer_state(&driver, "capped-peer"),
    Some(State::Alive),
    "the buffered Alive refutes the suspicion the deferral kept open — the peer \
     is Alive, never Dead"
  );
}

/// The deferral is bounded: a sustained flood that keeps every poll capped
/// cannot suppress `handle_timeout` past the staleness grace — the due deadline
/// force-fires even with the backlog still standing, observable as the suspicion
/// completing (the peer turns `Dead`) with the socket still saturated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn due_timeout_force_fires_past_the_staleness_grace() {
  let peer_addr: SocketAddr = "127.0.0.1:39991".parse().unwrap();
  let (mut driver, _obs_rx, _shared, _bytes) =
    build_driver_with(64, None, 8, Duration::from_millis(40), |endpoint| {
      stage_suspicion(endpoint, "flooded-peer", peer_addr);
    })
    .await;

  let due_at = driver
    .endpoint
    .poll_timeout()
    .expect("the staged suspicion arms a machine deadline");
  TokioRuntime::sleep(due_at.saturating_duration_since(Instant::now()) + Duration::from_millis(20))
    .await;

  let gossip_addr = driver
    .socket
    .as_ref()
    .expect("gossip socket held")
    .local_addr()
    .expect("gossip local addr");
  let tx = <TokioNet as Net>::UdpSocket::bind("127.0.0.1:0")
    .await
    .expect("bind flood socket");

  // Anchor the deferral with a first capped poll: the due suspicion is held.
  for _ in 0..24 {
    tx.send_to(&[0x20u8; 24], gossip_addr)
      .await
      .expect("flood send");
  }
  TokioRuntime::sleep(Duration::from_millis(50)).await;
  let _ = poll_once(&mut driver);
  assert!(
    driver.timeout_stall_since.is_some(),
    "the flood must anchor a deferral first"
  );
  assert_eq!(
    peer_state(&driver, "flooded-peer"),
    Some(State::Suspect),
    "the anchored deferral must hold the due suspicion open"
  );

  // Keep the socket saturated past the grace, then poll: the due deadline must
  // force-fire despite the standing backlog.
  for _ in 0..24 {
    tx.send_to(&[0x20u8; 24], gossip_addr)
      .await
      .expect("flood send");
  }
  TokioRuntime::sleep(TIMEOUT_STALENESS_GRACE + Duration::from_millis(10)).await;
  let _ = poll_once(&mut driver);
  assert_eq!(
    peer_state(&driver, "flooded-peer"),
    Some(State::Dead),
    "a sustained flood must not suppress the deadline past the staleness grace: \
     the force-fire completes the suspicion"
  );
  assert!(
    driver.timeout_stall_since.is_none(),
    "the force-fire clears the deferral anchor"
  );
}

/// The armed-sleep arm is machine-neutral: when the sleep polls Ready, the pump
/// clears it and self-wakes — it never touches the endpoint. Entering that arm
/// requires a STABLE future target (a machine deadline below the idle wake), else
/// `arm_timer` replaces the staged sleep with a fresh one before it is ever
/// polled; a staged suspicion supplies one, and swapping the armed sleep for an
/// elapsed one while keeping `timer_deadline` drives the arm deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_sleep_is_cleared_without_touching_the_machine() {
  let peer_addr: SocketAddr = "127.0.0.1:39992".parse().unwrap();
  // A 1s probe interval puts the suspicion deadline ~1s out — comfortably beyond
  // this test's microsecond-scale polls and the 20ms swap wait, yet below the
  // idle wake set next.
  let (mut driver, _obs_rx, _shared, _bytes) =
    build_driver_with(64, None, 8, Duration::from_secs(1), |endpoint| {
      stage_suspicion(endpoint, "suspect-peer", peer_addr);
    })
    .await;
  // Keep the idle wake above the suspicion deadline so the machine deadline is
  // the arm target — a nearer idle target recomputes fresh every poll and would
  // replace the staged sleep.
  driver.idle_wake = Duration::from_secs(600);

  // Quiescent poll arms the sleep toward the machine deadline.
  let _ = poll_once(&mut driver);
  let armed = driver.timer_deadline;
  let machine_deadline = driver.endpoint.poll_timeout();
  assert_eq!(
    armed, machine_deadline,
    "with a machine deadline below the idle wake, the timer arms toward it"
  );
  let target = armed.expect("the quiescent poll arms the sleep");

  // Swap in an elapsed sleep, keeping the recorded deadline: the next poll
  // recomputes the SAME stable target, so `arm_timer` keeps this sleep and polls
  // it Ready — the exact shape of a deadline crossing between the fresh clock
  // sample and the timer poll. The wait lets the zero-duration sleep pass the
  // timer wheel's coarse elapse check while the suspicion deadline stays future.
  driver.timer = Some(Box::pin(TokioRuntime::sleep(Duration::ZERO)));
  TokioRuntime::sleep(Duration::from_millis(20)).await;
  assert!(
    Instant::now() < target,
    "precondition: the machine deadline is still in the future"
  );
  let _ = poll_once(&mut driver);

  assert!(
    driver.timer.is_none() && driver.timer_deadline.is_none(),
    "the ready sleep is consumed by the armed-sleep arm, not replaced"
  );
  assert_eq!(
    driver.endpoint.poll_timeout(),
    machine_deadline,
    "the armed-sleep arm must not fire the machine timer: the deadline is \
     untouched and fires later through the gated due branch"
  );
  assert!(
    driver.timeout_stall_since.is_none(),
    "clearing a ready sleep must not anchor a deferral"
  );
}

/// The inbound-quiescence gate is sound only if the machine timer can fire
/// nowhere else: every `handle_timeout` must route through the single gated
/// decision (fresh clock sample, backlog deferral, staleness grace). Pin that
/// structurally — the pump source carries exactly one call site AND it sits
/// inside the gated block, so the call can neither gain a sibling nor be
/// relocated to an arm (the ready-sleep arm in particular) that bypasses the
/// gate.
#[test]
fn the_pump_fires_timeouts_only_inside_the_gated_branch() {
  let src = include_str!("mod.rs");
  assert_eq!(
    src.matches(".handle_timeout(").count(),
    1,
    "a second handle_timeout call site can fire a due deadline past capped \
     inbound backlog holding a deadline-refuting input"
  );
  let gate = src
    .find("if !inbound_backlog || grace_elapsed")
    .expect("the gated due decision exists in the pump");
  // The gated block holds straight-line statements only, so its first `}` is its
  // closing brace; the sole call must sit between the condition and it.
  let gate_close = gate
    + src[gate..]
      .find('}')
      .expect("the gated due decision block closes");
  let call = src
    .find(".handle_timeout(")
    .expect("the single call site exists");
  assert!(
    gate < call && call < gate_close,
    "the sole handle_timeout call must sit inside the gated due decision — \
     anywhere else can fire past capped inbound backlog"
  );
}
