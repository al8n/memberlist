//! QUIC backend lifecycle tests — handle, commands, shutdown, fairness.

#![cfg(feature = "quic-rustls-ring")]

#[path = "support/quic.rs"]
mod support;

use std::{net::SocketAddr, time::Duration};

use compio::{buf::BufResult, net::UdpSocket};
use memberlist_compio::{MemberlistError, QuicMemberlist};
use memberlist_machine::config::EndpointConfig;
use rustls::RootCertStore;
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  format!("127.0.0.1:{port}").parse().expect("loopback")
}

fn make_config(id: &str, port: u16) -> EndpointConfig<SmolStr, SocketAddr> {
  EndpointConfig::new(SmolStr::new(id), loopback_addr(port))
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
  let first = QuicMemberlist::new(
    make_config("first", 7100),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("first bind");
  first.shutdown().await.expect("first shutdown");

  // Same port, same address — must succeed.
  let second = QuicMemberlist::new(
    make_config("second", 7100),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("rebind after shutdown");
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
  let first = QuicMemberlist::new(
    make_config("first-cloned", 7110),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("first bind");
  // Hold a clone past the shutdown — the inner Arc<JoinHandle> stays
  // alive but the driver task itself has exited.
  let _live_clone = first.clone();
  first.shutdown().await.expect("first shutdown");

  let second = QuicMemberlist::new(
    make_config("second-cloned", 7110),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("rebind after shutdown with live clone");
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
  let seed_port: u16 = 7120;
  let joiner_port: u16 = 7121;
  let seed_addr = loopback_addr(seed_port);

  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_seed = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_joiner = support::build_quic_config(cert, key, roots);

  let seed = QuicMemberlist::new(make_config("udp-seed", seed_port), qcfg_seed)
    .await
    .expect("seed construct");
  let joiner = QuicMemberlist::new(make_config("udp-joiner", joiner_port), qcfg_joiner)
    .await
    .expect("joiner construct");

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
    .dispatch_join_with(&memberlist_compio::SocketAddrResolver, &[seed_addr])
    .await
    .expect("join_with under UDP flood");
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
  let seed_addr = loopback_addr(7131);

  // Run a seed memberlist so the SocketAddrResolver target is real,
  // even though the join will be aborted by shutdown before it
  // converges.
  let seed = QuicMemberlist::new(
    make_config("slowres-seed", 7131),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("seed construct");

  let m = QuicMemberlist::new(
    make_config("slowres", 7130),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("construct");
  let m_for_join = m.clone();

  // Spawn the slow join on the same runtime.
  let join_task = compio::runtime::spawn(async move {
    let slow = SlowResolver;
    let start = std::time::Instant::now();
    let res = m_for_join.dispatch_join_with(&slow, &[seed_addr]).await;
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
  let m = QuicMemberlist::new(
    make_config("post-shutdown", 7140),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("construct");
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
    .set_compression_options(memberlist_wire::CompressionOptions::new())
    .await;
  let r_enc = m_clone
    .set_encryption_options(memberlist_wire::EncryptionOptions::new())
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
  let seed_port: u16 = 7150;
  let joiner_port: u16 = 7151;
  let seed_addr = loopback_addr(seed_port);

  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_seed = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_joiner = support::build_quic_config(cert, key, roots);

  let seed = QuicMemberlist::new(make_config("cmdflood-seed", seed_port), qcfg_seed)
    .await
    .expect("seed construct");
  let joiner = QuicMemberlist::new(make_config("cmdflood-joiner", joiner_port), qcfg_joiner)
    .await
    .expect("joiner construct");

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
    .dispatch_join_with(&memberlist_compio::SocketAddrResolver, &[seed_addr])
    .await
    .expect("join_with under cmd flood");
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
  let m = QuicMemberlist::new(
    make_config("quiet", 7160),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("construct");

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
  let m = QuicMemberlist::new(
    make_config("concurrent-shutdown", 7170),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("construct");
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
  let seed_addr = loopback_addr(7180);

  let seed = QuicMemberlist::new(
    make_config("flood-seed", 7180),
    support::self_trusted_quic_config(),
  )
  .await
  .expect("seed construct");

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
  let seed_port: u16 = 7190;
  let joiner_port: u16 = 7191;
  let seed_addr = loopback_addr(seed_port);

  let (cert, key) = support::generate_localhost_cert();
  let mut roots = RootCertStore::empty();
  roots.add(cert.clone()).expect("root");
  let qcfg_seed = support::build_quic_config(cert.clone(), key.clone_key(), roots.clone());
  let qcfg_joiner = support::build_quic_config(cert, key, roots);

  let seed = QuicMemberlist::new(make_config("stable-seed", seed_port), qcfg_seed)
    .await
    .expect("seed construct");
  let joiner = QuicMemberlist::new(make_config("stable-joiner", joiner_port), qcfg_joiner)
    .await
    .expect("joiner construct");

  joiner
    .dispatch_join_with(&memberlist_compio::SocketAddrResolver, &[seed_addr])
    .await
    .expect("join_with");

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
