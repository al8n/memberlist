//! QUIC backend lifecycle tests — handle, commands, shutdown, fairness.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{
  net::SocketAddr,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use bytes::Bytes;
use compio::{buf::BufResult, net::UdpSocket};
use futures_util::StreamExt;
use memberlist_compio::{
  ConflictDelegate, Delegate, EventDelegate, FirstAddrResolver, MaybeResolved, Memberlist,
  MemberlistError, NodeDelegate, Options, PingDelegate, QuicOptions, QuicTransport,
  QuicTransportOptions, SocketAddrResolver, VoidDelegate,
};
use memberlist_proto::{event::Event, typed::NodeState};
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

/// Build a `Memberlist` on an OS-allocated loopback port (`127.0.0.1:0`).
/// The concrete bound address is read back from
/// [`Memberlist::advertise_address`] after construction, so no test
/// hard-codes a port — a hard-coded port would collide with a sibling test
/// binding the same value on another libtest thread in this binary.
///
/// The membership-input address type is `SocketAddr`, so the construction
/// resolver is the identity `SocketAddrResolver` (never invoked for a resolved
/// advertise).
async fn make_quic(id: &str, qcfg: QuicOptions) -> Memberlist<SmolStr, SocketAddr> {
  make_quic_at(id, loopback_addr(0), qcfg).await
}

/// Like [`make_quic`] but binds a caller-supplied address. Used by the rebind
/// test, which must re-bind the *same* address a prior node has released.
async fn make_quic_at(
  id: &str,
  addr: SocketAddr,
  qcfg: QuicOptions,
) -> Memberlist<SmolStr, SocketAddr> {
  let opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
    QuicTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_quic_config(qcfg),
  );
  Memberlist::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("bind quic memberlist")
}

async fn wait_until<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = std::time::Instant::now();
  while !predicate() {
    if start.elapsed() >= deadline {
      return false;
    }
    compio::time::sleep(Duration::from_millis(10)).await;
  }
  true
}

/// After `shutdown()` returns, the gossip UDP socket must be released
/// so a fresh memberlist can rebind the same `(advertise_addr, port)` tuple
/// immediately.
///
/// QUIC uses a single UDP socket (no separate TCP listener); the UDP port is
/// the sole address resource that must be released before `shutdown()` acks.
#[compio::test]
async fn rebind_after_shutdown_releases_udp_port() {
  let first = make_quic("first", support::self_trusted_quic_config()).await;
  let addr = first.advertise_address();
  first.shutdown().await.expect("first shutdown");

  // Same address — rebinding the just-released port must succeed.
  let second = make_quic_at("second", addr, support::self_trusted_quic_config()).await;
  second.shutdown().await.expect("second shutdown");
}

/// Same as the no-clone case, but a live clone outlives the
/// `shutdown.await` — the shutdown ack must still fire only after the
/// UDP socket drops, so the rebind on the same address succeeds even
/// though a clone is still holding the snapshot / event channels.
///
/// A clone keeps the `Memberlist` struct alive (the
/// `Arc<JoinHandle>` is shared), but the bound port must still be
/// free after `shutdown.await` returns. The shutdown ack is sent
/// only after the post-loop cleanup drops the UDP endpoint — independent
/// of any outstanding clones.
#[compio::test]
async fn rebind_after_shutdown_with_live_clone_works() {
  let first = make_quic("first-cloned", support::self_trusted_quic_config()).await;
  let addr = first.advertise_address();
  // Hold a clone past the shutdown — the inner Arc<JoinHandle> stays
  // alive but the driver task itself has exited.
  let _live_clone = first.clone();
  first.shutdown().await.expect("first shutdown");

  let second = make_quic_at("second-cloned", addr, support::self_trusted_quic_config()).await;
  second.shutdown().await.expect("second shutdown");
}

/// Continuous UDP-recv flood must not starve the past-due timer.
/// `recv` sits before `timer` in the driver's `select_biased!` so
/// that a buffered probe Ack resolves the deadline before
/// `handle_timeout` marks the peer suspect; without an additional
/// bound, a flooded recv queue would otherwise win every poll and
/// `handle_timeout` would never run — stale suspicion timers and probe
/// reapers would never fire.
///
/// The driver's bounded past-due preemption gives recv ONE shot per
/// past-due iteration: a buffered datagram is applied, then the
/// deadline is re-polled, then `handle_timeout` runs only if the
/// deadline is still past. This test pumps junk UDP datagrams at the
/// seed's gossip socket continuously while a legitimate join from a
/// separate node must still complete within bounded time.
#[compio::test]
async fn udp_flood_does_not_starve_timer_under_join() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_seed = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_joiner = support::build_quic_config(cert, key, roots);

  let seed = make_quic("udp-seed", qcfg_seed).await;
  let joiner = make_quic("udp-joiner", qcfg_joiner).await;
  let seed_addr = seed.advertise_address();

  // Continuously pump junk UDP datagrams at the seed's gossip socket
  // at ~200 pps. Slow enough that the seed's recv arm still hits
  // Pending between packets (so the timer-driven probe cycle can
  // fire), fast enough to demonstrate the past-due peek path runs
  // without disrupting convergence. The seed's machine rejects each
  // as a frame-decode error and drops it; the test goal is the
  // constant recv readiness, not the payload.
  let flood = compio::runtime::spawn(async move {
    // Ignoring Err: ephemeral source-port bind for the flood; any
    // failure stops this flooder (the test still exercises the
    // architecture even if the flood degrades).
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
      return;
    };
    let junk = vec![0u8; 64];
    loop {
      // Ignoring Err: per-datagram send is best-effort; a transient
      // failure does not stop the flood loop.
      let BufResult(_, b) = sender.send_to(junk.clone(), seed_addr).await;
      // Yield occasionally so the runtime can schedule the driver
      // task between bursts; without this the single-threaded compio
      // runtime might starve the driver entirely.
      drop(b);
      compio::time::sleep(Duration::from_millis(5)).await;
    }
  });

  // Legitimate join: must complete despite the UDP flood.
  let count = joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join under UDP flood");
  assert_eq!(count, 1);

  // Wider deadline than TCP: QUIC handshake latency.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(30),
  )
  .await;
  // Drop the flooder before the assertion so a failure does not leak it.
  drop(flood);
  assert!(
    converged,
    "join did not converge under UDP flood: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// A `join_with` whose resolver is slow (e.g. real DNS) passes the
/// shutdown-flag check at the top of the method but then awaits the
/// resolver before sending its Join command. If shutdown lands during
/// the resolver wait, the late send could race the cleanup drain.
/// The driver's post-drain `drop(commands)` closes the Receiver
/// immediately after the drain so any post-drain `send_async` returns
/// `SendError::Disconnected` — the clone surfaces
/// `MemberlistError::CommandSend` instead of hanging on a buffered
/// command whose reply Sender outlives the dropped Receiver.
///
/// The test uses a custom slow resolver (200ms sleep) and races it
/// against an immediate shutdown. Either error path is acceptable
/// (Shutdown if the flag was observed at re-check, CommandSend if
/// the send raced past the flag and hit the closed channel); the
/// test pins that the join_with returns within a bounded time, not
/// hangs.
#[compio::test]
async fn join_with_slow_resolver_does_not_hang_during_shutdown() {
  // Run a seed memberlist so the SocketAddrResolver target is real,
  // even though the join will be aborted by shutdown before it
  // converges.
  let seed = make_quic("slowres-seed", support::self_trusted_quic_config()).await;
  let seed_addr = seed.advertise_address();

  let m = make_quic("slowres", support::self_trusted_quic_config()).await;
  let m_for_join = m.clone();

  // Spawn the slow join on the same runtime. The seed is handed in
  // UNRESOLVED so the slow resolver (200ms sleep) is actually invoked —
  // a Resolved seed would skip resolution entirely and defeat the
  // slow-resolver-races-shutdown scenario this test pins.
  let join_task = compio::runtime::spawn(async move {
    let slow = SlowResolver;
    let start = std::time::Instant::now();
    let res = m_for_join
      .dispatch_join(&slow, &[MaybeResolved::Unresolved(seed_addr)])
      .await;
    (res, start.elapsed())
  });

  // Give the slow resolver a moment to start its sleep.
  compio::time::sleep(Duration::from_millis(50)).await;

  // Now shutdown — this should NOT cause the in-flight join_with to
  // hang. The cleanup drain replies Err(Shutdown) on any in-flight
  // command, and the post-drain `drop(commands)` closes the channel
  // so subsequent late sends fail fast.
  m.shutdown().await.expect("shutdown");

  // The join_with must complete within a bounded time — the slow
  // resolver sleeps 200ms; total budget is generous (1s).
  let (res, join_elapsed) = join_task.await.expect("join_task panicked");
  assert!(
    join_elapsed < Duration::from_secs(1),
    "join_with hung past shutdown: {join_elapsed:?}",
  );
  // Either error is acceptable — the test pins NO hang, not the
  // exact error variant.
  assert!(
    res.is_err(),
    "expected join_with to fail after shutdown, got {res:?}",
  );

  seed.shutdown().await.expect("seed shutdown");
}

/// Slow-resolver test fixture — sleeps 200ms before returning the
/// resolved addresses, simulating a slow DNS lookup.
struct SlowResolver;

impl memberlist_compio::Resolver for SlowResolver {
  type Address = SocketAddr;
  type Error = std::io::Error;

  async fn resolve(&self, addr: &Self::Address) -> Result<Vec<SocketAddr>, Self::Error> {
    compio::time::sleep(Duration::from_millis(200)).await;
    Ok(vec![*addr])
  }
}

/// A clone command issued during / after the driver's shutdown
/// cleanup must NOT hang on its reply-receiver. The driver's
/// post-loop cleanup sets the atomic `shutdown_flag` before draining
/// pending commands; every command-sending method on every
/// `Memberlist` clone reads the flag at entry and short-circuits with
/// `MemberlistError::Shutdown` when set. Commands that already
/// passed the flag check before the flip and were buffered in the
/// channel are drained by the post-loop cleanup and replied to with
/// the same Shutdown error.
#[compio::test]
async fn command_after_shutdown_returns_error_promptly() {
  let m = make_quic("post-shutdown", support::self_trusted_quic_config()).await;
  let m_clone = m.clone();

  // Shut down via the original. After shutdown returns, the
  // shutdown_flag is set across every clone.
  m.shutdown().await.expect("shutdown");

  // Now every command on the live clone must return Shutdown
  // immediately. Without the flag the calls would buffer in the
  // channel (the user-side `commands_tx` clones outlive the driver
  // task), the reply Sender would stay alive inside the channel,
  // and the clone's `.await` would hang.
  let start = std::time::Instant::now();
  let r_meta = m_clone
    .update_node_metadata(b"after-shutdown".to_vec())
    .await;
  let r_compr = m_clone
    .set_compression_options(memberlist_proto::CompressionOptions::new())
    .await;
  let r_enc = m_clone
    .set_encryption_options(memberlist_proto::EncryptionOptions::new())
    .await;
  let r_leave = m_clone.leave().await;
  let elapsed = start.elapsed();

  assert!(
    matches!(r_meta, Err(MemberlistError::Shutdown)),
    "update_node_metadata: expected Shutdown, got {r_meta:?}",
  );
  assert!(
    matches!(r_compr, Err(MemberlistError::Shutdown)),
    "set_compression_options: expected Shutdown, got {r_compr:?}",
  );
  assert!(
    matches!(r_enc, Err(MemberlistError::Shutdown)),
    "set_encryption_options: expected Shutdown, got {r_enc:?}",
  );
  assert!(
    matches!(r_leave, Err(MemberlistError::Shutdown)),
    "leave: expected Shutdown, got {r_leave:?}",
  );
  assert!(
    elapsed < Duration::from_millis(500),
    "post-shutdown commands did not return promptly: {elapsed:?}",
  );
}

/// Command flood must not starve network arms (recv / timer).
///
/// Many cloned `Memberlist` handles each issuing commands (e.g.
/// `update_node_metadata` in a tight loop) push messages onto the
/// command channel. If `cmd` were the highest-priority arm in the
/// driver's `select_biased!` a cmd flood would win every poll and
/// starve recv / timer — SWIM probe Acks would be dropped, timers
/// would not fire, new peers could not join.
///
/// The driver demotes `cmd` to MEDIUM priority (after recv / timer).
/// This test floods the seed with 200 concurrent `update_node_metadata`
/// calls from cloned handles while a separate joiner attempts to join.
/// The join must complete (proving the recv arm reaches the network)
/// and the cluster must stay alive (proving timer / recv are not starved).
#[compio::test]
async fn cmd_flood_does_not_starve_recv_or_timer_under_join() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_seed = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_joiner = support::build_quic_config(cert, key, roots);

  let seed = make_quic("cmdflood-seed", qcfg_seed).await;
  let joiner = make_quic("cmdflood-joiner", qcfg_joiner).await;
  let seed_addr = seed.advertise_address();

  // Spawn 200 concurrent cmd-pumping tasks, each holding a clone of
  // the seed handle and looping update_node_metadata calls. Each call
  // sends a SetMeta command through the seed's cmd channel.
  for i in 0..200u32 {
    let seed_clone = seed.clone();
    compio::runtime::spawn(async move {
      for _ in 0..10 {
        // Ignoring Err: a per-call failure does not stop the test;
        // the goal is the cumulative cmd-channel pressure.
        let _ = seed_clone
          .update_node_metadata(format!("meta-{i}").into_bytes())
          .await;
      }
    })
    .detach();
  }

  // Legitimate join through the seed's recv arm. Must complete
  // despite the command flood.
  let count = joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join under cmd flood");
  assert_eq!(count, 1);

  // Wider deadline than TCP: QUIC handshake latency.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "join did not converge under cmd flood: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// Quiet shutdown — no network activity, no concurrent commands.
/// The iter-top cmd fairness drain processes the Shutdown immediately
/// and the driver loop must honor `exit` BEFORE entering the main
/// select; otherwise the loop would block in `select_biased!` until
/// the next arm fires (up to `IDLE_WAKE_INTERVAL = 60s` on an idle
/// endpoint), and `shutdown.await` would hang for that full duration.
///
/// Asserts shutdown completes in << IDLE_WAKE_INTERVAL on a freshly
/// constructed memberlist with no peers and no traffic.
#[compio::test]
async fn quiet_shutdown_lands_without_waiting_for_select_wake() {
  let m = make_quic("quiet", support::self_trusted_quic_config()).await;

  // Let the driver settle into its main select wait.
  compio::time::sleep(Duration::from_millis(50)).await;

  let shutdown_start = std::time::Instant::now();
  m.shutdown().await.expect("shutdown");
  let elapsed = shutdown_start.elapsed();

  // 1s bound — comfortably below IDLE_WAKE_INTERVAL (60s) and below
  // any SWIM probe interval that might naturally wake the driver.
  assert!(
    elapsed < Duration::from_secs(1),
    "quiet shutdown waited for the select wake: {:?}",
    elapsed
  );
}

/// Concurrent shutdown from multiple cloned handles must terminate
/// cleanly: the first Shutdown command the driver dequeues is
/// terminal (the iter-top cmd drain breaks out of further command
/// processing on observing Shutdown), and the driver loop exits.
/// The second clone's `shutdown.await` resolves with either
/// `MemberlistError::CommandSend` (its send raced the driver loop
/// exit and found the receiver gone) or `MemberlistError::ReplyClosed`
/// (its send landed but the driver was already past the iter-top
/// drain and never processed it before exiting).
///
/// Either outcome is acceptable — both indicate "the cluster has
/// shut down". The test asserts that at least one shutdown returns
/// `Ok(())` AND that both calls return within a bounded time (no
/// hang).
#[compio::test]
async fn concurrent_shutdown_from_cloned_handles() {
  let m = make_quic("concurrent-shutdown", support::self_trusted_quic_config()).await;
  let m2 = m.clone();

  // Let the driver settle into its main select wait.
  compio::time::sleep(Duration::from_millis(50)).await;

  let start = std::time::Instant::now();
  let (r1, r2) = futures_util::future::join(m.shutdown(), m2.shutdown()).await;
  let elapsed = start.elapsed();

  assert!(
    r1.is_ok() || r2.is_ok(),
    "no shutdown succeeded: r1={:?} r2={:?}",
    r1,
    r2,
  );
  assert!(
    elapsed < Duration::from_secs(2),
    "concurrent shutdown hung: {:?}",
    elapsed
  );
}

/// Shutdown must complete promptly even under continuous network
/// flood. The driver's iter-top cmd fairness drain (cap
/// `CMD_FAIRNESS_BUDGET = 4` commands per iter via `try_recv`)
/// guarantees user commands make bounded progress regardless of
/// select-arm contention from recv.
///
/// Without the fairness drain a UDP flood would let the recv arm
/// preempt cmd indefinitely; `shutdown` would queue but never be
/// dispatched, and the caller's `shutdown.await` would hang.
#[compio::test]
async fn shutdown_lands_under_continuous_network_flood() {
  let seed = make_quic("flood-seed", support::self_trusted_quic_config()).await;
  let seed_addr = seed.advertise_address();

  // UDP flood at the gossip socket.
  let udp_flood = compio::runtime::spawn(async move {
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
      return;
    };
    let junk = vec![0u8; 64];
    loop {
      // Ignoring Err: per-datagram send is best-effort; a transient
      // failure does not stop the flood loop.
      let BufResult(_, _) = sender.send_to(junk.clone(), seed_addr).await;
      compio::time::sleep(Duration::from_micros(500)).await;
    }
  });

  // Give the flood a moment to ramp up.
  compio::time::sleep(Duration::from_millis(100)).await;

  // Shutdown must land promptly via the iter-top cmd fairness drain.
  // The cmd flood is just one Shutdown, but the UDP flood is
  // saturating recv — without the fairness drain the cmd arm would
  // not be reached for many iterations.
  let shutdown_start = std::time::Instant::now();
  seed.shutdown().await.expect("shutdown under network flood");
  let elapsed = shutdown_start.elapsed();
  // Drop the UDP flood task so it does not race the assertion.
  drop(udp_flood);

  assert!(
    elapsed < Duration::from_secs(2),
    "shutdown stalled under network flood: {:?}",
    elapsed
  );
}

/// io_uring-targeted valid-buffered-datagram regression for the
/// bounded past-due peek.
///
/// The SWIM probe cycle continuously generates a `slight-future
/// poll_timeout()` (the probe's cumulative deadline) AND a kernel-
/// buffered UDP datagram (the peer's Ack) immediately before that
/// deadline. On io_uring this is the precise race the bounded peek
/// must solve: a freshly-submitted `gossip_socket.recv_from(...)` SQE
/// is ALWAYS Pending on its first poll (the SQE has to be submitted,
/// then the runtime processes the completion on a later poll cycle),
/// so without the 1ms peek budget the main select_biased!'s `timer`
/// arm wins, `handle_timeout` fires, and the peer is wrongly
/// suspected. With the 1ms peek the runtime has time to harvest the
/// completion before the past-due branch falls through to
/// `handle_timeout`.
///
/// On polling backends (kqueue / epoll) the recv is Ready on first
/// poll so the peek timer never fires; the test passes either way.
/// The targeted value is the io_uring CI gate — without the bounded peek,
/// `alive_count` decays to 1 on one or both sides as successive Acks are
/// missed.
///
/// Holds the converged pair for ~2s (multiple sub-second probe
/// cycles) and asserts every iteration observes a steady
/// `(member=2, alive=2)` snapshot on both nodes.
#[compio::test]
async fn post_join_stays_alive_through_probe_cycles() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_seed = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_joiner = support::build_quic_config(cert, key, roots);

  let seed = make_quic("stable-seed", qcfg_seed).await;
  let joiner = make_quic("stable-joiner", qcfg_joiner).await;
  let seed_addr = seed.advertise_address();

  joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join");

  // Wider deadline than TCP: QUIC handshake latency.
  let converged = wait_until(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(converged, "initial convergence");

  // Hold for ~2s — covers multiple SWIM probe cycles at the default
  // (sub-second) probe interval. Every iteration verifies both sides
  // still observe the full cluster: any missed Ack would transition
  // the peer to Suspect and decrement alive_count.
  for _ in 0..20 {
    compio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
      seed.alive_count(),
      2,
      "seed marked joiner suspect (alive={}, member={})",
      seed.alive_count(),
      seed.member_count(),
    );
    assert_eq!(
      joiner.alive_count(),
      2,
      "joiner marked seed suspect (alive={}, member={})",
      joiner.alive_count(),
      joiner.member_count(),
    );
  }

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// Regression: `leave().await` immediately followed by `shutdown().await` on
/// the QUIC backend must still flush the direct `Dead`-self notice before the
/// socket is torn down, so the peer observes an intentional leave rather than a
/// probe-timeout failure. The QUIC loop's exit path drains every queued surface
/// before breaking (mirroring the stream driver); without that drain, a
/// `shutdown` racing into the same command-fairness pass as the preceding
/// `leave` would drop the queued `Dead`-self packet at socket teardown.
///
/// `NodeLeft` is the discriminating signal: the direct leave produces it in
/// well under a second, whereas probe-based failure detection would not mark
/// the peer dead until the suspicion timeout (~3s for a 2-node cluster). A 2s
/// observation window therefore passes only when the leave packet was flushed.
#[compio::test]
async fn quic_leave_then_shutdown_is_observed_by_peer() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make_quic("leaver-a", qcfg_a).await;
  let b = make_quic("watcher-b", qcfg_b).await;

  // Converge: B joins A; both observe two alive members.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  // Subscribe to B's event stream BEFORE A leaves so the NodeLeft is observable.
  let mut b_events = b.events();

  // A leaves, then immediately shuts down: the queued Dead-self transmit must
  // be flushed by the shutdown exit drain before the UDP socket drops.
  a.leave().await.expect("leave");
  a.shutdown().await.expect("shutdown");

  // B must observe A's intentional leave promptly — well before probe-based
  // detection could mark A dead.
  let saw_leave = compio::time::timeout(Duration::from_secs(2), async {
    while let Some(ev) = b_events.next().await {
      if let Event::NodeLeft(node) = ev
        && node.id_ref() == &SmolStr::new("leaver-a")
      {
        return true;
      }
    }
    false
  })
  .await
  .unwrap_or(false);

  assert!(
    saw_leave,
    "B did not observe A's intentional leave within 2s — the QUIC shutdown exit drain dropped the Dead-self packet"
  );

  // Ignoring Err: shutdown best-effort during test teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Observation delegate whose `notify_leave` sleeps ~3s; every other hook is a
/// no-op (mirroring `VoidDelegate`). Used to prove the QUIC driver flushes
/// protocol-critical transmits BEFORE awaiting observation hooks.
struct SlowLeaveDelegate;

impl NodeDelegate for SlowLeaveDelegate {}

impl EventDelegate for SlowLeaveDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_leave(&self, _node: Arc<NodeState<SmolStr, SocketAddr>>) {
    // A deliberately slow observation hook. If the driver dispatched events
    // ahead of flushing transmits, this sleep would stall the local node's
    // own `Dead`-self flush for its full duration.
    compio::time::sleep(Duration::from_secs(3)).await;
  }
}

impl PingDelegate for SlowLeaveDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl ConflictDelegate for SlowLeaveDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl Delegate for SlowLeaveDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

/// Regression: a slow observation delegate must NOT delay the leaving node's
/// direct `Dead`-self flush. `Endpoint::leave` emits `Event::NodeLeft(self)`
/// BEFORE it queues the direct `Dead`-self notices to live peers, and the
/// driver awaits `notify_leave` for that event. If the driver drained events
/// ahead of flushing transmits, a slow `notify_leave` (here ~3s) would hold the
/// `Dead`-self packets hostage and the peer would observe a probe-timeout
/// failure instead of an intentional leave.
///
/// The QUIC driver flushes the memberlist + raw-QUIC transmit surfaces BEFORE
/// dispatching events (mirroring the stream driver), so the `Dead`-self reaches
/// the wire promptly even while the leaver's own `notify_leave` sleeps. The
/// 1.5s `NodeLeft` window on the peer is the non-vacuity guarantee: with the
/// transmits-first ordering it passes in well under a second; with events
/// drained first the flush is delayed ~3s and the window fails.
#[compio::test]
async fn quic_slow_observation_delegate_does_not_delay_leave_flush() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  // Node A carries the slow-leave observation delegate; built inline like
  // `make_quic` but with `SlowLeaveDelegate` in place of `VoidDelegate`.
  let a: Memberlist<SmolStr, SocketAddr> = {
    let opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
      QuicTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(SmolStr::new("slowleave-a"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
        .with_quic_config(qcfg_a),
    );
    Memberlist::new(
      opts,
      SlowLeaveDelegate,
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await
    .expect("bind slow-leave node A")
  };

  // Node B is a normal VoidDelegate node.
  let b = make_quic("watcher-b", qcfg_b).await;

  // Converge: B joins A; both observe two alive members.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  // Subscribe to B's event stream BEFORE A leaves so the NodeLeft is observable.
  let mut b_events = b.events();

  // A leaves. `leave().await` returns once the `Dead`-self flush completes
  // (the machine's `LeftCluster`), which the two-pass event drain resolves in
  // pass A — BEFORE the slow `notify_leave(self)` runs in pass B. So the call
  // returns promptly despite the ~3s observation sleep, and the flush itself
  // (the memberlist + raw-QUIC transmit surfaces, drained ahead of the events
  // step) reaches the wire before that sleep.
  a.leave().await.expect("leave");

  // B must observe A's intentional leave within 1.5s — far below the ~3s the
  // leaver's own `notify_leave` sleeps. If the delegate dispatch were not
  // decoupled from the flush + completion accounting, the `Dead`-self would be
  // withheld for that full sleep and this window would fail.
  let saw_leave = compio::time::timeout(Duration::from_millis(1500), async {
    while let Some(ev) = b_events.next().await {
      if let Event::NodeLeft(node) = ev
        && node.id_ref() == &SmolStr::new("slowleave-a")
      {
        return true;
      }
    }
    false
  })
  .await
  .unwrap_or(false);

  assert!(
    saw_leave,
    "B did not observe A's intentional leave within 1.5s — a slow notify_leave stalled the QUIC Dead-self flush"
  );

  // Ignoring Err: best-effort teardown. B shuts down promptly; A's shutdown
  // may take up to ~3s while its notify_leave sleeps, so both are timeout-wrapped.
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
}

/// `leave()` honors the leave-completion contract: it returns `Ok(())`
/// only once the machine's `Event::LeftCluster` has fired, which the
/// machine withholds until the direct `Dead`-self notices `leave()`
/// queued for every live peer have been handed to the wire. QUIC parity
/// for the stream driver's `leave_completes_only_after_peer_is_notified`.
///
/// Once `a.leave().await` returns, the `Dead`-self has been SENT and B
/// observes `NodeLeft(A)` within a SHORT window — far below probe-based
/// failure detection (~3 s for a 2-node cluster), so the window passing
/// reflects the intentional leave reaching B, not A being reaped as
/// failed.
///
/// Non-vacuity caveat: on fast loopback this timing window alone does
/// NOT distinguish the parked-leave wiring from a hypothetical
/// return-when-queued `leave()` — the driver flushes the queued
/// `Dead`-self in its very next drain regardless of when the reply is
/// sent, so B observes the leave promptly either way. What it pins is
/// the contract itself (leave-return implies the notice is on the wire)
/// as a regression guard; the `LeftCluster`-withholding boundary is
/// unit-tested in `memberlist-proto`.
#[compio::test]
async fn leave_completes_only_after_peer_is_notified() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make_quic("leave-notify-a", qcfg_a).await;
  let b = make_quic("leave-notify-b", qcfg_b).await;

  // Converge: B joins A; both observe two alive members.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  // Subscribe to B's events BEFORE A leaves so the NodeLeft is observable.
  let mut b_events = b.events();

  // `leave().await` returning Ok now means the `Dead`-self flush
  // completed (the machine's `LeftCluster`).
  a.leave().await.expect("leave");

  // Therefore B must already have — or imminently receive — the leave.
  // A SHORT window proves the notice was on the wire by the time leave()
  // returned, not that probe-based detection eventually reaped A.
  let saw_leave = compio::time::timeout(Duration::from_millis(500), async {
    while let Some(ev) = b_events.next().await {
      if let Event::NodeLeft(node) = ev
        && node.id_ref() == &SmolStr::new("leave-notify-a")
      {
        return true;
      }
    }
    false
  })
  .await
  .unwrap_or(false);

  assert!(
    saw_leave,
    "B did not observe A's leave within 500ms of leave() returning — leave() resolved before the Dead-self was flushed"
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Observation delegate whose `notify_join` sleeps ~3s; every other hook is a
/// no-op (mirroring `VoidDelegate`). Used to prove join completion is decoupled
/// from the observation-delegate dispatch on the QUIC backend.
struct SlowJoinDelegate;

impl NodeDelegate for SlowJoinDelegate {}

impl EventDelegate for SlowJoinDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;

  async fn notify_join(&self, _node: Arc<NodeState<SmolStr, SocketAddr>>) {
    // A deliberately slow observation hook. If join completion were dispatched
    // behind the observation delegate, this sleep would stall `join().await`
    // for its full duration even though the join exchange already succeeded.
    compio::time::sleep(Duration::from_secs(3)).await;
  }
}

impl PingDelegate for SlowJoinDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl ConflictDelegate for SlowJoinDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl Delegate for SlowJoinDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

/// Regression: a slow observation `notify_join` must NOT delay `join()`'s
/// completion. On QUIC the join's terminal `ExchangeCompleted` arrives on a
/// SEPARATE inbound datagram AFTER the one that surfaces `NodeJoined`, so the
/// driver must keep servicing its recv arm (to receive that completion
/// datagram and fire the parked join reply) even while the joiner's own
/// `notify_join` runs. The driver dispatches observation hooks on a dedicated
/// task — never inline on the loop — so a slow `notify_join` cannot stall the
/// recv arm. The join's `Ok(contacted)` therefore fires the moment the
/// completion datagram lands, regardless of the ~3s observation sleep.
///
/// Strongly discriminating: with observation dispatch off the driver loop the
/// join returns in ~ms; dispatching the hook inline on the loop blocks the
/// driver in `notify_join` so it cannot receive the completion datagram, and
/// `join().await` waits out the full ~3s — the 1.5s window then fails (verified
/// empirically against an inline-dispatch driver).
#[compio::test]
async fn quic_slow_observation_delegate_does_not_delay_join_completion() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_seed = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_joiner = support::build_quic_config(cert, key, roots);

  // Node J carries the slow-join observation delegate; built inline like
  // `make_quic` but with `SlowJoinDelegate` in place of `VoidDelegate`.
  let joiner: Memberlist<SmolStr, SocketAddr> = {
    let opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
      QuicTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(SmolStr::new("slowjoin-j"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
        .with_quic_config(qcfg_joiner),
    );
    Memberlist::new(
      opts,
      SlowJoinDelegate,
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await
    .expect("bind slow-join joiner")
  };

  // The seed is a normal VoidDelegate node.
  let seed = make_quic("slowjoin-seed", qcfg_seed).await;

  // `join().await` must return Ok well within 1.5s even though the joiner's
  // own `notify_join` sleeps ~3s: the exchange completion resolves the waiter
  // on the driver task, ahead of the off-loop observation dispatch.
  let start = std::time::Instant::now();
  let joined = compio::time::timeout(
    Duration::from_millis(1500),
    joiner.join(
      &SocketAddrResolver,
      &[MaybeResolved::Resolved(seed.advertise_address())],
    ),
  )
  .await;
  let elapsed = start.elapsed();

  let contacted = joined
    .expect("join() did not return within 1.5s — a slow notify_join stalled join completion")
    .expect("join failed");
  assert_eq!(contacted.len(), 1, "exactly one seed contacted");
  assert!(
    elapsed < Duration::from_millis(1500),
    "join completion was delayed by the slow notify_join: {elapsed:?}",
  );

  // Ignoring Err: best-effort teardown. The joiner's shutdown may take up to
  // ~3s while its notify_join sleeps, so both are timeout-wrapped.
  let _ = compio::time::timeout(Duration::from_secs(5), seed.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), joiner.shutdown()).await;
}

/// Regression: two cloned `Memberlist` handles calling `leave()` concurrently
/// must BOTH return `Ok(())`. The QUIC driver treats leave as a single shared
/// in-flight operation — the first `Command::Leave` initiates the machine's
/// `leave()` and parks a `PendingLeave`; the second joins it by pushing its
/// reply onto the shared `repliers` list rather than re-invoking
/// `endpoint.leave()` (a terminal no-op once already `Leaving`). When the
/// machine's `Event::LeftCluster` fires, every joined replier is resolved
/// together, so neither caller hangs and neither sees a premature `Ok`.
///
/// The hazard this guards against: a second caller that snapshots
/// `was_running == false` and falls into the immediate-`Ok` branch returns
/// before the first leave's `Dead`-self is flushed. On fast loopback that
/// premature-`Ok` timing is not strongly discriminable, so this test guards the
/// in-flight-sharing logic against hang/regression and confirms a peer still
/// observes the leave: B must see `NodeLeft(A)` after both `leave()` futures
/// resolve.
#[compio::test]
async fn quic_concurrent_leave_from_cloned_handles_both_succeed() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make_quic("quic-concurrent-leave-a", qcfg_a).await;
  let b = make_quic("quic-concurrent-leave-b", qcfg_b).await;

  // Converge: B joins A; both observe two alive members.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  // Subscribe to B's events BEFORE A leaves so the NodeLeft is observable.
  let mut b_events = b.events();

  // Two clones of A both call leave() concurrently. Both must return Ok — one
  // initiates the in-flight leave, the other joins it; the single LeftCluster
  // resolves both.
  let a2 = a.clone();
  let (r1, r2) = futures_util::future::join(a.leave(), a2.leave()).await;
  r1.expect("first concurrent leave returned Err");
  r2.expect("second concurrent leave returned Err (premature-Ok regression or hang)");

  // The peer must observe A's intentional leave — proof the leave actually
  // reached the wire rather than both callers returning a premature Ok.
  let saw_leave = compio::time::timeout(Duration::from_millis(500), async {
    while let Some(ev) = b_events.next().await {
      if let Event::NodeLeft(node) = ev
        && node.id_ref() == &SmolStr::new("quic-concurrent-leave-a")
      {
        return true;
      }
    }
    false
  })
  .await
  .unwrap_or(false);
  assert!(
    saw_leave,
    "B did not observe A's leave within 500ms of both leave() calls returning"
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Observation delegate whose FIRST `notify_user_msg` sleeps ~2s, then records
/// every delivered payload into a shared vector. Every other hook is a no-op
/// (mirroring `VoidDelegate`). Used to prove the observation channel never
/// drops application data when the handler is slow.
struct SlowUserMsgDelegate {
  slept: Arc<AtomicBool>,
  user_msgs: Arc<Mutex<Vec<Bytes>>>,
}

impl NodeDelegate for SlowUserMsgDelegate {
  async fn notify_user_msg(&self, msg: std::borrow::Cow<'_, [u8]>) {
    // Stall the FIRST delivery ~2s to model a slow application handler. The
    // observation channel is unbounded, so the event is buffered (never
    // dropped) and this delivery still completes — just later. `swap` so only
    // the first call sleeps; subsequent re-gossips of the same payload record
    // promptly.
    if !self.slept.swap(true, Ordering::SeqCst) {
      compio::time::sleep(Duration::from_secs(2)).await;
    }
    // Ignoring poisoning is not silent failure: a poisoned lock means a prior
    // test-thread panic already failed the test, so unwrap surfaces that.
    self
      .user_msgs
      .lock()
      .unwrap()
      .push(Bytes::copy_from_slice(msg.as_ref()));
  }
}

impl EventDelegate for SlowUserMsgDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl PingDelegate for SlowUserMsgDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl ConflictDelegate for SlowUserMsgDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

impl Delegate for SlowUserMsgDelegate {
  type Id = SmolStr;
  type Address = SocketAddr;
}

/// The observation `Delegate` must observe received application data even when
/// its handler is slow — the driver buffers events for it across a brief stall.
///
/// `notify_user_msg` (from `Event::UserPacket`) and `merge_remote_state` (from
/// `Event::RemoteStateReceived`) carry application payloads that are NOT part of
/// the `MemberlistSnapshot`, so a dropped delegate event is unrecoverable from
/// `members()`. The driver only ever `try_send`s to the observation task (it
/// never awaits the hand-off), so a slow handler cannot stall SWIM. The
/// observation channel is bounded (`Channel::Bounded`, the default), but one
/// in-flight broadcast plus the trickle of membership events over a brief stall
/// stays far under the cap — so the slow first handler only DELAYS delivery, it
/// does not lose the payload. Here node B's first `notify_user_msg` sleeps ~2s;
/// A queues a user broadcast that periodic gossip carries to B. Despite the
/// slow first delivery, B's delegate EVENTUALLY records the payload within a
/// generous window.
///
/// Honest scope: this guards eventual delivery under handler latency for a
/// payload that fits the buffer — not the bounded channel's drop accounting
/// under sustained saturation, which the dedicated bounded-observation-channel
/// tests cover directly.
#[compio::test]
async fn quic_slow_user_msg_delegate_still_observes_broadcast() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  // Node A is a normal VoidDelegate node; it queues the broadcast.
  let a = make_quic("slowuser-a", qcfg_a).await;

  // Node B carries the slow-user-msg observation delegate; built inline like
  // `make_quic` but with `SlowUserMsgDelegate` in place of `VoidDelegate`.
  let slept = Arc::new(AtomicBool::new(false));
  let user_msgs: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(Vec::new()));
  let b: Memberlist<SmolStr, SocketAddr> = {
    let opts = Options::<QuicTransport<SmolStr, SocketAddr>>::new(
      QuicTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(SmolStr::new("slowuser-b"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0)))
        .with_quic_config(qcfg_b),
    );
    Memberlist::new(
      opts,
      SlowUserMsgDelegate {
        slept: slept.clone(),
        user_msgs: user_msgs.clone(),
      },
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await
    .expect("bind slow-user-msg node B")
  };

  // Converge: A joins B; both observe two alive members.
  a.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(b.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  // A queues a user broadcast; periodic gossip carries it to B.
  a.queue_user_broadcast(Bytes::from_static(b"slow-payload"))
    .await
    .expect("queue user broadcast acks Ok");

  // B's delegate must EVENTUALLY record the payload despite its first
  // notify_user_msg sleeping ~2s. The unbounded observation channel buffers
  // the event for the slow handler — it is never dropped. A generous 6s window
  // absorbs the gossip-interval stagger plus the 2s handler stall.
  let saw = wait_until(
    || {
      user_msgs
        .lock()
        .unwrap()
        .iter()
        .any(|b| b.as_ref() == b"slow-payload")
    },
    Duration::from_secs(6),
  )
  .await;
  assert!(
    saw,
    "slow notify_user_msg never recorded the gossiped broadcast (event lost?); recorded: {:?}",
    user_msgs.lock().unwrap()
  );

  // Ignoring Err: best-effort teardown. B's first notify_user_msg may still be
  // mid-sleep, so both are timeout-wrapped.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// After `leave()` completes, a data-plane mutation must be REJECTED rather
/// than falsely acked. `leave()` stops the periodic gossip scheduler that
/// drains `user_broadcasts`, so a broadcast enqueued afterward could never be
/// disseminated. The driver gates `queue_user_broadcast` on a running node and
/// replies `MemberlistError::NotRunning` instead of `Ok(())`.
///
/// `leave().await` returns only once the machine's `Event::LeftCluster` has
/// fired, so by the time it returns `is_running()` is false and the subsequent
/// command is gated. Strongly discriminating: without the gate the call returns
/// `Ok(())`. QUIC parity for the TCP test of the same name.
#[compio::test]
async fn queue_user_broadcast_after_leave_is_rejected() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make_quic("ub-after-leave-a", qcfg_a).await;
  let b = make_quic("ub-after-leave-b", qcfg_b).await;

  // Converge so A is a real running cluster member (the leave then has a live
  // peer to flush its `Dead`-self notice to).
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  // Leave; `.await` blocks until the flush completes (`LeftCluster`).
  a.leave().await.expect("leave");

  // The post-leave broadcast must be rejected, NOT falsely acked.
  let res = a
    .queue_user_broadcast(Bytes::from_static(b"after-leave"))
    .await;
  assert!(
    matches!(res, Err(MemberlistError::NotRunning)),
    "expected NotRunning after leave, got {res:?}",
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// QUIC mirror of the TCP `set_ack_payload_oversized_is_rejected`. An ack
/// payload too large to frame into a single gossip datagram must be REJECTED
/// as `AckPayloadExceedsMtu`, NOT falsely acked: acks ride one
/// UDP datagram on the gossip socket, so an over-budget payload makes every
/// probe reply silently fail to send and peers falsely suspect this node.
/// The driver validates at the machine setter (mirror-symmetric with the TCP
/// driver) and the payload is never stored. Strongly discriminating: without
/// the validation the call returns `Ok(())`.
#[compio::test]
async fn set_ack_payload_oversized_is_rejected() {
  let a = make_quic("ack-oversized-a", support::self_trusted_quic_config()).await;

  let res = a
    .set_ack_payload(Bytes::from(vec![0xab_u8; 1024 * 1024]))
    .await;
  assert!(
    matches!(
      res,
      Err(MemberlistError::Proto(
        memberlist_proto::Error::AckPayloadExceedsMtu(..)
      ))
    ),
    "expected AckPayloadExceedsMtu for a 1 MiB ack payload, got {res:?}",
  );

  // A reasonable payload on the SAME running node is still accepted.
  let ok = a.set_ack_payload(Bytes::from_static(b"small")).await;
  assert!(
    ok.is_ok(),
    "a small ack payload must still be accepted, got {ok:?}"
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
}

/// QUIC parity for the TCP `join_after_leave_is_rejected`. After `leave()`
/// completes, a `join` must be REJECTED with `MemberlistError::NotRunning`
/// rather than falsely reporting success: `leave()` is terminal (it stops the
/// periodic probe / gossip / push-pull schedulers and there is no rejoin path),
/// so a join enqueued afterward would resolve `Ok(contacted)` from the
/// push/pull's `ExchangeCompleted` yet leave the node Left and
/// non-participating. The QUIC driver gates `Command::Join` on a running node
/// BEFORE enqueuing any push/pull (mirror-symmetric with the stream driver) and
/// replies `NotRunning`. Both `join` (WaitForCompletion) and `dispatch_join`
/// (Dispatch) funnel through `Command::Join` and are covered. Strongly
/// discriminating: without the gate `join` returns `Ok(<reached set>)`.
#[compio::test]
async fn join_after_leave_is_rejected() {
  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_a = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_b = support::build_quic_config(cert, key, roots);

  let a = make_quic("join-after-leave-a", qcfg_a).await;
  let b = make_quic("join-after-leave-b", qcfg_b).await;
  let b_addr = b.advertise_address();

  // Converge so A is a real running member with a live peer to flush its
  // `Dead`-self notice to on leave.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = wait_until(
    || a.alive_count() == 2 && b.alive_count() == 2,
    Duration::from_secs(5),
  )
  .await;
  assert!(
    converged,
    "cluster did not converge: a={}, b={}",
    a.alive_count(),
    b.alive_count()
  );

  // Leave; `.await` blocks until the flush completes (`LeftCluster`), so A's
  // machine lifecycle is Left and `is_running()` is false by the time it
  // returns.
  a.leave().await.expect("leave");

  // The synchronous `join` (WaitForCompletion arm) must be rejected.
  let res = a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await;
  assert!(
    matches!(res, Err((_, MemberlistError::NotRunning))),
    "expected NotRunning from join after leave, got {res:?}",
  );

  // The fire-and-forget `dispatch_join` (Dispatch arm) shares the same
  // `Command::Join` gate and must also be rejected.
  let res_dispatch = a
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await;
  assert!(
    matches!(res_dispatch, Err(MemberlistError::NotRunning)),
    "expected NotRunning from dispatch_join after leave, got {res_dispatch:?}",
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}
