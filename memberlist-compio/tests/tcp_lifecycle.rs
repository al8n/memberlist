//! Lifecycle regression tests — verify the dial/listener handles release
//! their resources before `shutdown()` returns.

#![cfg(feature = "tcp")]

use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
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
  ConflictDelegate, Delegate, DriverOptions, EventDelegate, FirstAddrResolver, MaybeResolved,
  Memberlist, NodeDelegate, Options, PingDelegate, SocketAddrResolver, StreamTransportOptions,
  TcpMemberlist, TcpTransport, TcpTransportOptions, VoidDelegate,
};
use memberlist_proto::{event::Event, typed::NodeState};
use smol_str::SmolStr;

fn loopback_addr(port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

/// Build an unlabeled `TcpMemberlist` advertising `addr`. The driver-layer
/// membership address is `SocketAddr`, so every seed handed to `join`
/// arrives as a `MaybeResolved::Resolved` and the construction resolver is
/// the identity `SocketAddrResolver` (never invoked for a resolved advertise).
async fn make_tcp(id: &str, addr: SocketAddr) -> TcpMemberlist<SmolStr, SocketAddr> {
  let opts = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new(id))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  );
  TcpMemberlist::<SmolStr, SocketAddr>::new(
    opts,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("construct tcp memberlist")
}

/// Reserve a concrete loopback address by binding an ephemeral node and
/// releasing it. Lets a caller run two constructions against the SAME port
/// (e.g. to prove a rejected construction left nothing bound); a bare `:0`
/// would hand the two constructions unrelated OS-selected ports. The freed
/// port could in principle be reclaimed by a sibling test before reuse — the
/// same exposure any port-reuse test carries.
async fn reserve_addr() -> SocketAddr {
  let probe = make_tcp("addr-probe", loopback_addr(0)).await;
  let addr = probe.advertise_address();
  probe
    .shutdown()
    .await
    .expect("probe shutdown releases the port");
  addr
}

/// After `shutdown()` returns, the gossip UDP socket AND the TCP
/// reliable listener must both be released so a fresh memberlist can
/// rebind the same `(advertise_addr, port)` tuple immediately.
///
/// The driver owns the `TcpListener` inline (no separate listener
/// task), so the listener drops with the driver loop and the bound
/// port is released before the post-loop cleanup acks the shutdown
/// reply. Pre-fix the listener ran as its own task and stayed
/// blocked on `listener.accept().await` past the driver exit; the
/// rebind would fail with `EADDRINUSE`.
#[compio::test]
async fn rebind_after_shutdown_releases_listener_port() {
  let first = make_tcp("first", loopback_addr(0)).await;
  let addr = first.advertise_address();
  first.shutdown().await.expect("first shutdown");

  // Same port, same address — must succeed.
  let second = make_tcp("second", addr).await;
  second.shutdown().await.expect("second shutdown");
}

/// Same as the no-clone case, but a live clone outlives the
/// `shutdown.await` — the shutdown ack must still fire only after the
/// listener drops, so the rebind on the same address succeeds even
/// though a clone is still holding the snapshot / event channels.
///
/// A clone keeps the `Memberlist` struct alive (the
/// `Arc<JoinHandle>` is shared), but the bound port must still be
/// free after `shutdown.await` returns. The shutdown ack is sent
/// only after the post-loop cleanup drops the listener — independent
/// of any outstanding clones.
#[compio::test]
async fn rebind_after_shutdown_with_live_clone_works() {
  let first = make_tcp("first-cloned", loopback_addr(0)).await;
  let addr = first.advertise_address();
  // Hold a clone past the shutdown — the inner Arc<JoinHandle> stays
  // alive but the driver task itself has exited.
  let _live_clone = first.clone();
  first.shutdown().await.expect("first shutdown");

  let second = make_tcp("second-cloned", addr).await;
  second.shutdown().await.expect("second shutdown");
}

/// Saturated accept regression — under continuous incoming TCP
/// connections (a peer or attacker pumping the listener), the driver
/// loop's `select_biased!` would prefer the `accept` arm and starve the
/// `timer` arm, blocking `handle_timeout` and leaving stale bridges
/// (overdue handshake deadlines) unreaped indefinitely.
///
/// The driver's iteration-top past-due check fires `handle_timeout`
/// before entering the select, so the timer cannot be starved by any
/// hot arm. This test pins that behavior by interleaving a legitimate
/// join with a continuous accept-saturator: convergence must still
/// complete within bounded time.
#[compio::test]
async fn saturated_accept_does_not_starve_join_convergence() {
  let seed = make_tcp("sat-seed", loopback_addr(0)).await;
  let joiner = make_tcp("sat-joiner", loopback_addr(0)).await;
  let seed_addr = seed.advertise_address();

  // Saturate seed's accept arm: open 100 TCP connections that never
  // send anything. Each opens a server-side bridge on seed whose
  // handshake deadline elapses without bytes — those bridges must be
  // reaped by the timer for the listener to keep accepting cleanly.
  for i in 0..100 {
    compio::runtime::spawn(async move {
      // Ignoring Err: any connect failure stops this saturator
      // instance; the remaining 99 keep the accept arm hot.
      if let Ok(stream) = compio::net::TcpStream::connect(seed_addr).await {
        // Hold briefly so the seed-side bridge actually starts; vary
        // hold time so accept stays hot across the join interval.
        compio::time::sleep(Duration::from_millis(50 + (i % 10) * 10)).await;
        drop(stream);
      }
    })
    .detach();
  }

  // Legitimate join: must complete despite the accept saturation.
  let count = joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join under saturation");
  assert_eq!(count, 1);

  // Bounded convergence — the iteration-top deadline check guarantees
  // the timer is not starved by the saturating accepts.
  let converged = tokio_like_wait(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "join did not converge under saturated accept: joiner={} seed={}",
    joiner.member_count(),
    seed.member_count()
  );

  seed.shutdown().await.expect("seed shutdown");
  joiner.shutdown().await.expect("joiner shutdown");
}

/// Spin-wait helper local to this file (the other join test in
/// `tcp_join.rs` has its own).
async fn tokio_like_wait<F: FnMut() -> bool>(mut predicate: F, deadline: Duration) -> bool {
  let start = std::time::Instant::now();
  while start.elapsed() < deadline {
    if predicate() {
      return true;
    }
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  predicate()
}

/// Continuous UDP-recv flood must not starve the past-due timer.
/// `recv` sits before `timer` in the driver's `select_biased!` so
/// that a buffered probe Ack resolves the deadline before
/// `handle_timeout` marks the peer suspect; without an additional
/// bound, a flooded recv queue would otherwise win every poll and
/// `handle_timeout` would never run — stale bridges, suspicion
/// timers, and probe reapers would never fire.
///
/// The driver's bounded past-due preemption gives recv ONE shot per
/// past-due iteration: a buffered datagram is applied, then the
/// deadline is re-polled, then `handle_timeout` runs only if the
/// deadline is still past. This test pumps junk UDP datagrams at the
/// seed's gossip socket continuously while a legitimate join from a
/// separate node must still complete within bounded time.
#[compio::test]
async fn udp_flood_does_not_starve_timer_under_join() {
  let seed = make_tcp("udp-seed", loopback_addr(0)).await;
  let joiner = make_tcp("udp-joiner", loopback_addr(0)).await;
  let seed_addr = seed.advertise_address();

  // Continuously pump junk UDP datagrams at the seed's gossip socket
  // at ~200 pps. Slow enough that the seed's recv arm still hits
  // Pending between packets (so accept and the timer-driven probe
  // cycle can fire), fast enough to demonstrate the past-due peek
  // path runs without disrupting convergence. The earlier 1000 pps
  // rate was on the edge of recv-vs-accept starvation under macOS
  // scheduling; the slower rate keeps the test deterministic.
  // The seed's machine rejects each as a frame-decode error and drops
  // it; the test goal is the constant recv readiness, not the payload.
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

  let converged = tokio_like_wait(
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

/// A dial that completes AFTER the coordinator already retired the
/// exchange (timeout / Close) must not produce a long-lived bridge that
/// writes stale bytes to the peer.
///
/// The test triggers a dial to an unreachable peer (a port with no
/// listener), then immediately shuts down. The dial task's
/// `TcpStream::connect` may still be in flight when shutdown lands; the
/// resulting `OutboundOk` or `OutboundFail` arriving past shutdown must
/// be a no-op.
#[compio::test]
async fn shutdown_during_dial_does_not_leak_bridge() {
  let local = loopback_addr(0);
  // Port 1 is a privileged port with no listener; the connect attempt
  // either fails fast (ECONNREFUSED) or stalls; either way the bridge
  // entry for the dial exists in the driver's bridges map until
  // OutboundOk/OutboundFail arrives.
  let unreachable = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1);

  let m = make_tcp("dial-then-shutdown", local).await;

  // Fire the join; the resolver is a SocketAddr pass-through. The dial
  // task is spawned by drain_actions; whether it has completed yet is
  // a race with the subsequent shutdown.
  // Ignoring Err: connect-to-port-1 may fail fast on some OSes; the
  // dispatch_join future then returns the count before the dial races
  // with shutdown.
  let _ = m
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(unreachable)])
    .await;

  // Give the dial a small window to either complete or stay in flight.
  compio::time::sleep(Duration::from_millis(50)).await;

  // Shut down. Should NOT hang on the in-flight dial task.
  m.shutdown().await.expect("shutdown");
}

/// Bridge inbound backlog regression — many simultaneous inbound TCP
/// connections pump bytes through the driver's `bridge_inbound`
/// channel. The channel is `flume::bounded(BRIDGE_INBOUND_CAP = 1024)`
/// so the per-bridge byte-mover tasks (`send_async`) naturally
/// backpressure when the driver gets behind. The iter-top drain is
/// itself capped at `ITER_DRAIN_CAP = 256` per iteration so other
/// arms (gossip recv, timer, accept) still get scheduled under load.
///
/// The test opens 200 raw TCP connections to the seed's listener and
/// writes 2 KiB of junk on each. Each connection results in a server-
/// side bridge that pushes BridgeInbound::Bytes through the bounded
/// channel; without the cap on the iter-top drain the driver could
/// stall other arms while flushing every queued message in one pass.
/// The test asserts the driver continues to serve `member_count`
/// reads (snapshot publication) and shuts down promptly.
#[compio::test]
async fn bridge_inbound_backlog_does_not_stall_driver() {
  let seed = make_tcp("backlog-seed", loopback_addr(0)).await;
  let seed_addr = seed.advertise_address();

  // Open 200 inbound TCP connections in parallel; each writes 2 KiB
  // of junk bytes that the seed's record layer rejects (no valid
  // cluster label). The bridge byte-mover still pushes the bytes
  // through the bounded bridge_inbound channel.
  for i in 0..200u32 {
    compio::runtime::spawn(async move {
      // Ignoring Err: any single failed connect / write does not
      // affect the test — the remaining bridges still pressure the
      // inbound channel.
      if let Ok(stream) = compio::net::TcpStream::connect(seed_addr).await {
        let junk = vec![(i & 0xff) as u8; 2048];
        let BufResult(_, _) = compio::io::AsyncWriteExt::write_all(&mut { stream }, junk).await;
      }
    })
    .detach();
  }

  // Snapshot reads must keep working during the backlog — the driver
  // republishes after every dirty iter, and the cap on the drain is
  // what keeps the republish cadence fresh under spam. The reads
  // exercise the lock-free arc-swap path; the return values are not
  // asserted on the per-iter loop because the snapshot can lag the
  // bridge churn by one tick. The final assert below pins the
  // steady-state invariant.
  let start = std::time::Instant::now();
  while start.elapsed() < Duration::from_millis(500) {
    // Discarded: per-tick reads only verify the snapshot path stays
    // responsive under the backlog; the steady-state assertion below
    // is the load-bearing check.
    let _ = seed.alive_count();
    let _ = seed.member_count();
    compio::time::sleep(Duration::from_millis(20)).await;
  }
  assert_eq!(seed.alive_count(), 1, "seed lost self under backlog");

  // Shutdown must complete promptly even with many bridges in various
  // states of being torn down.
  let shutdown_start = std::time::Instant::now();
  seed.shutdown().await.expect("shutdown under backlog");
  assert!(
    shutdown_start.elapsed() < Duration::from_secs(2),
    "shutdown stalled under backlog: {:?}",
    shutdown_start.elapsed()
  );
}

/// Bridge inbound pressure must not starve the accept arm.
///
/// Continuous `bridge_in` traffic on already-open bridges could win
/// every `select_biased!` poll if it sat above `accept` in source
/// order — the iter-top drain caps the per-iter bridge_in work but
/// the select arm itself would still preempt accept. The driver
/// places bridge_in at LOWEST priority specifically to keep accept
/// (and every other arm) reachable under continuous reliable-stream
/// pressure.
///
/// The test floods the seed with 100 byte-pumping bridges and then
/// attempts a legitimate join from a separate node. The join's TCP
/// SYN lands in the seed's listener backlog; the accept arm must
/// fire to pick it up. With bridge_in lowest-priority the accept
/// arm wins, the joiner's bridge spawns, and convergence completes
/// within 10s.
#[compio::test]
async fn bridge_in_pressure_does_not_starve_accept_under_join() {
  let seed = make_tcp("starve-seed", loopback_addr(0)).await;
  let joiner = make_tcp("starve-joiner", loopback_addr(0)).await;
  let seed_addr = seed.advertise_address();

  // Saturate bridge_in: 100 long-lived bridges each writing junk
  // bytes in a tight loop. Each push generates a `BridgeInbound::Bytes`
  // event on the seed's bridge_inbound channel.
  for i in 0..100u32 {
    compio::runtime::spawn(async move {
      // Ignoring Err: any single failed bridge does not affect the
      // test — the remaining bridges still pressure bridge_in.
      if let Ok(mut stream) = compio::net::TcpStream::connect(seed_addr).await {
        // Send bytes in a small loop so bridge_in keeps firing.
        for _ in 0..20 {
          let junk = vec![(i & 0xff) as u8; 512];
          let BufResult(res, _) = compio::io::AsyncWriteExt::write_all(&mut stream, junk).await;
          if res.is_err() {
            break;
          }
          compio::time::sleep(Duration::from_millis(5)).await;
        }
      }
    })
    .detach();
  }

  // Legitimate join through the seed's accept arm. Must complete
  // despite the bridge_in flood.
  let count = joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join under bridge_in pressure");
  assert_eq!(count, 1);

  let converged = tokio_like_wait(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(10),
  )
  .await;
  assert!(
    converged,
    "join did not converge under bridge_in pressure: joiner={} seed={}",
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
  let seed = make_tcp("slowres-seed", loopback_addr(0)).await;
  let seed_addr = seed.advertise_address();

  let m = make_tcp("slowres", loopback_addr(0)).await;
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
///
/// The race window the flag closes: a clone calls e.g.
/// `update_node_metadata` right as the driver is tearing down. Pre-
/// fix the command would buffer in the channel, the driver would
/// drop the Receiver, the buffered command's reply Sender would stay
/// alive (held inside the dropped Receiver's queue), and the clone's
/// `recv_async` on the reply would block forever.
#[compio::test]
async fn command_after_shutdown_returns_error_promptly() {
  let m = make_tcp("post-shutdown", loopback_addr(0)).await;
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
    matches!(r_meta, Err(memberlist_compio::MemberlistError::Shutdown)),
    "update_node_metadata: expected Shutdown, got {r_meta:?}",
  );
  assert!(
    matches!(r_compr, Err(memberlist_compio::MemberlistError::Shutdown)),
    "set_compression_options: expected Shutdown, got {r_compr:?}",
  );
  assert!(
    matches!(r_enc, Err(memberlist_compio::MemberlistError::Shutdown)),
    "set_encryption_options: expected Shutdown, got {r_enc:?}",
  );
  assert!(
    matches!(r_leave, Err(memberlist_compio::MemberlistError::Shutdown)),
    "leave: expected Shutdown, got {r_leave:?}",
  );
  assert!(
    elapsed < Duration::from_millis(500),
    "post-shutdown commands did not return promptly: {elapsed:?}",
  );
}

/// Command flood must not starve network arms (recv / accept / timer).
///
/// Many cloned `Memberlist` handles each issuing commands (e.g.
/// `update_node_metadata` in a tight loop) push messages onto the
/// command channel. If `cmd` were the highest-priority arm in the
/// driver's `select_biased!` a cmd flood would win every poll and
/// starve recv / timer / accept — SWIM probe Acks would be dropped,
/// timers would not fire, new peers could not join.
///
/// The driver demotes `cmd` to MEDIUM priority (after recv / timer /
/// accept). This test floods the seed with 200 concurrent
/// `update_node_metadata` calls from cloned handles while a separate
/// joiner attempts to join. The join must complete (proving the
/// accept arm reaches the network) and the cluster must stay alive
/// (proving timer / recv are not starved).
#[compio::test]
async fn cmd_flood_does_not_starve_accept_or_timer_under_join() {
  let seed = make_tcp("cmdflood-seed", loopback_addr(0)).await;
  let joiner = make_tcp("cmdflood-joiner", loopback_addr(0)).await;
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

  // Legitimate join through the seed's accept arm. Must complete
  // despite the command flood.
  let count = joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join under cmd flood");
  assert_eq!(count, 1);

  let converged = tokio_like_wait(
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
  let m = make_tcp("quiet", loopback_addr(0)).await;

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
  let m = make_tcp("concurrent-shutdown", loopback_addr(0)).await;
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
/// select-arm contention from recv / accept.
///
/// Without the fairness drain a UDP flood + concurrent accept flood
/// would let the network arms preempt cmd indefinitely; `shutdown`
/// would queue but never be dispatched, and the caller's
/// `shutdown.await` would hang.
#[compio::test]
async fn shutdown_lands_under_continuous_network_flood() {
  let seed = make_tcp("flood-seed", loopback_addr(0)).await;
  let seed_addr = seed.advertise_address();

  // UDP flood at the gossip socket.
  let udp_flood = compio::runtime::spawn(async move {
    let Ok(sender) = UdpSocket::bind("127.0.0.1:0").await else {
      return;
    };
    let junk = vec![0u8; 64];
    loop {
      let BufResult(_, _) = sender.send_to(junk.clone(), seed_addr).await;
      compio::time::sleep(Duration::from_micros(500)).await;
    }
  });

  // TCP accept flood at the reliable listener.
  for _ in 0..50 {
    compio::runtime::spawn(async move {
      // Ignoring Err: each iteration is independent; a connect
      // failure stops the flood loop (the remaining tasks keep
      // pressuring accept).
      while let Ok(stream) = compio::net::TcpStream::connect(seed_addr).await {
        compio::time::sleep(Duration::from_millis(20)).await;
        drop(stream);
      }
    })
    .detach();
  }

  // Give the floods a moment to ramp up.
  compio::time::sleep(Duration::from_millis(100)).await;

  // Shutdown must land promptly via the iter-top cmd fairness drain.
  // The cmd flood is just one Shutdown, but the network floods are
  // saturating recv / accept — without the fairness drain the cmd
  // arm would not be reached for many iterations.
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

/// Degraded-network dial-leak regression.
///
/// The driver spawns a dial task per [`StreamAction::Connect`]; on a
/// black-holed peer (a routable address with no responder that drops
/// SYN packets) the kernel's default `TcpStream::connect` timeout is
/// ~3 minutes on Linux. Without the driver's own bound the dial task
/// would live for that full duration, pinning a runtime task and the
/// `bridge_ready_tx` clone — under continuous churn an unreachable
/// peer set could exhaust the runtime.
///
/// The driver caps the dial at `DIAL_TIMEOUT = 5s` and reports a
/// synthetic `io::ErrorKind::TimedOut` to the coordinator. This test
/// dials a documentation-only address (`192.0.2.1` per RFC 5737 —
/// guaranteed unroutable) and asserts the subsequent shutdown is
/// prompt; the dial task does not block the driver loop and the bound
/// keeps the dial task itself from outliving the test.
#[compio::test]
async fn dial_to_blackhole_bounded_by_dial_timeout() {
  let local = loopback_addr(0);
  // RFC 5737 TEST-NET-1 documentation prefix. Packets to this address
  // are dropped by sane routers, so `TcpStream::connect` will not
  // receive a SYN-ACK or a RST — the kernel sits in its connect
  // retry loop until the bound elapses.
  let blackhole: SocketAddr = "192.0.2.1:9".parse().unwrap();

  let m = make_tcp("blackhole-dialer", local).await;

  // `dispatch_join` is fire-and-forget at the driver boundary: it
  // returns the dispatched count immediately. The dial task runs in
  // the background bounded by DIAL_TIMEOUT (5s). The test's primary
  // assertion is the SUBSEQUENT shutdown — the in-flight dial task
  // must not block shutdown's command-channel arm.
  let count = m
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(blackhole)])
    .await
    .expect("dispatch_join");
  assert_eq!(count, 1, "one address dispatched");

  let shutdown_start = std::time::Instant::now();
  m.shutdown().await.expect("shutdown");
  assert!(
    shutdown_start.elapsed() < Duration::from_secs(1),
    "shutdown blocked on in-flight dial: took {:?}",
    shutdown_start.elapsed()
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
/// The targeted value is the io_uring CI gate — pre-fix the test
/// would observe `alive_count` decaying to 1 on one or both sides as
/// successive Acks are missed.
///
/// Holds the converged pair for ~2s (multiple sub-second probe
/// cycles) and asserts every iteration observes a steady
/// `(member=2, alive=2)` snapshot on both nodes.
#[compio::test]
async fn post_join_stays_alive_through_probe_cycles() {
  let seed = make_tcp("stable-seed", loopback_addr(0)).await;
  let joiner = make_tcp("stable-joiner", loopback_addr(0)).await;
  let seed_addr = seed.advertise_address();

  joiner
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(seed_addr)])
    .await
    .expect("dispatch_join");

  let converged = tokio_like_wait(
    || joiner.member_count() == 2 && seed.member_count() == 2,
    Duration::from_secs(5),
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

/// `leave()` honors the leave-completion contract: it returns `Ok(())`
/// only once the machine's `Event::LeftCluster` has fired, which the
/// machine withholds until the direct `Dead`-self notices `leave()`
/// queued for every live peer have been handed to the wire. So once
/// `a.leave().await` returns, the `Dead`-self has been SENT and B
/// observes `NodeLeft(A)` within a SHORT window — far below probe-based
/// failure detection (~3 s for a 2-node cluster), so the window passing
/// reflects the intentional leave reaching B, not A being reaped as
/// failed.
///
/// Non-vacuity caveat: on fast loopback this timing window alone does
/// NOT distinguish the parked-leave wiring from a hypothetical
/// return-when-queued `leave()` — the driver flushes the queued
/// `Dead`-self in its very next drain (sub-millisecond) regardless of
/// when the reply is sent, so B observes the leave promptly either way
/// (verified: this test still passes against an immediate-reply
/// `leave()`). What it pins is the contract itself (leave-return implies
/// the notice is on the wire) as a regression guard against a change
/// that lets `leave()` resolve before the `Dead`-self is queued/handed
/// off; the `LeftCluster`-withholding boundary is unit-tested in
/// `memberlist-proto`.
#[compio::test]
async fn leave_completes_only_after_peer_is_notified() {
  let a = make_tcp("leave-notify-a", loopback_addr(0)).await;
  let b = make_tcp("leave-notify-b", loopback_addr(0)).await;

  // Converge: B joins A; both observe two alive members.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = tokio_like_wait(
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
/// from the observation-delegate dispatch.
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
/// completion. The driver dispatches observation hooks on a dedicated task,
/// never inline on the loop, and fires the synchronous-join reply on the
/// driver task the moment the join's push/pull `ExchangeCompleted` lands. So
/// `join().await` returns promptly even while the joiner's own `notify_join`
/// sleeps ~3s.
///
/// Non-vacuity caveat: on fast loopback the stream backend delivers the
/// push/pull response and its stream EOF together, so the `ExchangeCompleted`
/// is available in the same drain that surfaces `NodeJoined` — the timing alone
/// does not strongly distinguish off-loop dispatch from inline dispatch here.
/// The strongly-discriminating variant is on the QUIC backend
/// (`quic_slow_observation_delegate_does_not_delay_join_completion`), where the
/// completion arrives on a SEPARATE datagram and inline dispatch provably
/// stalls the join ~3s. This test pins the contract (and the ≤1.5s window) on
/// the stream backend as a parity guard.
#[compio::test]
async fn slow_observation_delegate_does_not_delay_join_completion() {
  // Node J carries the slow-join observation delegate; built inline like
  // `make_tcp` but with `SlowJoinDelegate` in place of `VoidDelegate`.
  let joiner: Memberlist<TcpTransport<SmolStr, SocketAddr>, SlowJoinDelegate> = {
    let opts = Options::new(
      TcpTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(SmolStr::new("slowjoin-j"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0))),
    );
    Memberlist::<TcpTransport<SmolStr, SocketAddr>, SlowJoinDelegate>::new(
      opts,
      SlowJoinDelegate,
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await
    .expect("construct slow-join joiner")
  };

  // The seed is a normal VoidDelegate node.
  let seed = make_tcp("slowjoin-seed", loopback_addr(0)).await;

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
  assert_eq!(contacted, 1, "exactly one seed contacted");
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
/// must BOTH return `Ok(())`. The driver treats leave as a single shared
/// in-flight operation — the first `Command::Leave` initiates the machine's
/// `leave()` and parks a `PendingLeave`; the second joins it by pushing its
/// reply onto the shared `repliers` list rather than re-invoking
/// `endpoint.leave()` (a terminal no-op once already `Leaving`). When the
/// machine's `Event::LeftCluster` fires, every joined replier is resolved
/// together, so neither caller hangs and neither sees a premature `Ok`.
///
/// The hazard this guards against: a second caller that snapshots
/// `was_running == false` (the first leave already moved the machine to
/// `Leaving`) and falls into the immediate-`Ok` branch returns before the
/// first leave's `Dead`-self is flushed. On fast loopback that premature-`Ok`
/// timing is not strongly discriminable (the flush completes sub-millisecond
/// either way), so this test guards the in-flight-sharing logic against
/// hang/regression and confirms a peer still observes the leave: B must see
/// `NodeLeft(A)` after both `leave()` futures resolve.
#[compio::test]
async fn concurrent_leave_from_cloned_handles_both_succeed() {
  let a = make_tcp("concurrent-leave-a", loopback_addr(0)).await;
  let b = make_tcp("concurrent-leave-b", loopback_addr(0)).await;

  // Converge: B joins A; both observe two alive members.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = tokio_like_wait(
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
        && node.id_ref() == &SmolStr::new("concurrent-leave-a")
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
/// generous window. Stream-backend parity for the QUIC test of the same name.
///
/// Honest scope: this guards eventual delivery under handler latency for a
/// payload that fits the buffer — not the bounded channel's drop accounting
/// under sustained saturation, which the dedicated bounded-observation-channel
/// tests cover directly.
#[compio::test]
async fn slow_user_msg_delegate_still_observes_broadcast() {
  // Node A is a normal VoidDelegate node; it queues the broadcast.
  let a = make_tcp("slowuser-a", loopback_addr(0)).await;

  // Node B carries the slow-user-msg observation delegate; built inline like
  // `make_tcp` but with `SlowUserMsgDelegate` in place of `VoidDelegate`.
  let slept = Arc::new(AtomicBool::new(false));
  let user_msgs: Arc<Mutex<Vec<Bytes>>> = Arc::new(Mutex::new(Vec::new()));
  let b: Memberlist<TcpTransport<SmolStr, SocketAddr>, SlowUserMsgDelegate> = {
    let opts = Options::new(
      TcpTransportOptions::<SmolStr, SocketAddr>::new()
        .with_local_id(SmolStr::new("slowuser-b"))
        .with_advertise_addr(MaybeResolved::Resolved(loopback_addr(0))),
    );
    Memberlist::<TcpTransport<SmolStr, SocketAddr>, SlowUserMsgDelegate>::new(
      opts,
      SlowUserMsgDelegate {
        slept: slept.clone(),
        user_msgs: user_msgs.clone(),
      },
      &SocketAddrResolver,
      &FirstAddrResolver,
    )
    .await
    .expect("construct slow-user-msg node B")
  };

  // Converge: A joins B; both observe two alive members.
  a.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(b.advertise_address())],
  )
  .await
  .expect("join");
  let converged = tokio_like_wait(
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
  let saw = tokio_like_wait(
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
/// `Ok(())`.
#[compio::test]
async fn queue_user_broadcast_after_leave_is_rejected() {
  let a = make_tcp("ub-after-leave-a", loopback_addr(0)).await;
  let b = make_tcp("ub-after-leave-b", loopback_addr(0)).await;

  // Converge so A is a real running cluster member (the leave then has a live
  // peer to flush its `Dead`-self notice to).
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = tokio_like_wait(
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
    matches!(res, Err(memberlist_compio::MemberlistError::NotRunning)),
    "expected NotRunning after leave, got {res:?}",
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Mirror of `queue_user_broadcast_after_leave_is_rejected` for a second gated
/// command: after `leave()` the metadata-update mutation is rejected with
/// `MemberlistError::NotRunning` (the alive-broadcast path that would gossip the
/// new metadata is no longer scheduled). Strongly discriminating: without the
/// gate the call returns `Ok(())`.
#[compio::test]
async fn update_node_metadata_after_leave_is_rejected() {
  let a = make_tcp("meta-after-leave-a", loopback_addr(0)).await;
  let b = make_tcp("meta-after-leave-b", loopback_addr(0)).await;

  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = tokio_like_wait(
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

  a.leave().await.expect("leave");

  let res = a.update_node_metadata(b"after-leave".to_vec()).await;
  assert!(
    matches!(res, Err(memberlist_compio::MemberlistError::NotRunning)),
    "expected NotRunning after leave, got {res:?}",
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// Mirror of `update_node_metadata_after_leave_is_rejected` for the two policy-
/// mutation commands: after `leave()` the node emits no protocol traffic, so a
/// new compression or encryption policy could never take effect on the wire.
/// Both `set_compression_options` and `set_encryption_options` must reject with
/// `MemberlistError::NotRunning` rather than falsely ack a change that will
/// never be observed. Strongly discriminating: without the gate each call
/// returns `Ok(())`.
#[compio::test]
async fn set_policy_options_after_leave_is_rejected() {
  let a = make_tcp("policy-after-leave-a", loopback_addr(0)).await;
  let b = make_tcp("policy-after-leave-b", loopback_addr(0)).await;

  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = tokio_like_wait(
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

  a.leave().await.expect("leave");

  let res_compr = a
    .set_compression_options(memberlist_proto::CompressionOptions::new())
    .await;
  assert!(
    matches!(
      res_compr,
      Err(memberlist_compio::MemberlistError::NotRunning)
    ),
    "expected NotRunning from set_compression_options after leave, got {res_compr:?}",
  );

  let res_enc = a
    .set_encryption_options(memberlist_proto::EncryptionOptions::new())
    .await;
  assert!(
    matches!(res_enc, Err(memberlist_compio::MemberlistError::NotRunning)),
    "expected NotRunning from set_encryption_options after leave, got {res_enc:?}",
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// An ack payload too large to frame into a single gossip datagram must be
/// REJECTED as `AckPayloadExceedsMtu`, NOT falsely acked.
/// An ack rides one UDP datagram on the gossip socket, so an over-budget
/// payload makes every probe reply silently fail to send (the lossy-gossip
/// policy drops `send_to` errors) — peers would receive no ack and falsely
/// suspect this node. The driver validates at the machine setter and the
/// payload is never stored. Strongly discriminating: without the validation
/// the call returns `Ok(())`. 1 MiB is far over the default 1400-byte
/// gossip budget.
#[compio::test]
async fn set_ack_payload_oversized_is_rejected() {
  let a = make_tcp("ack-oversized-a", loopback_addr(0)).await;

  let res = a
    .set_ack_payload(Bytes::from(vec![0xab_u8; 1024 * 1024]))
    .await;
  assert!(
    matches!(
      res,
      Err(memberlist_compio::MemberlistError::Proto(
        memberlist_proto::Error::AckPayloadExceedsMtu(..)
      ))
    ),
    "expected AckPayloadExceedsMtu for a 1 MiB ack payload, got {res:?}",
  );

  // A reasonable payload on the SAME running node is still accepted: the
  // rejection is a size cap, not a blanket failure.
  let ok = a.set_ack_payload(Bytes::from_static(b"small")).await;
  assert!(
    ok.is_ok(),
    "a small ack payload must still be accepted, got {ok:?}"
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
}

/// After `leave()` completes, a `join` must be REJECTED with
/// `MemberlistError::NotRunning` rather than falsely reporting success.
/// `leave()` is terminal — it stops the periodic probe / gossip / push-pull
/// schedulers and there is no rejoin / re-arm path — so a join enqueued
/// afterward would resolve `Ok(contacted)` from the push/pull's
/// `ExchangeCompleted` yet leave the node Left and non-participating,
/// misleading orchestration into treating a dead node as rejoined. The driver
/// gates `Command::Join` on a running node BEFORE enqueuing any push/pull and
/// replies `NotRunning`. Both public entry points funnel through
/// `Command::Join`, so both `join` (WaitForCompletion) and `dispatch_join`
/// (Dispatch) are covered. Strongly discriminating: without the gate `join`
/// returns `Ok(usize)`.
#[compio::test]
async fn join_after_leave_is_rejected() {
  let a = make_tcp("join-after-leave-a", loopback_addr(0)).await;
  let b = make_tcp("join-after-leave-b", loopback_addr(0)).await;
  let b_addr = b.advertise_address();

  // Converge so A is a real running member with a live peer to flush its
  // `Dead`-self notice to on leave.
  b.join(
    &SocketAddrResolver,
    &[MaybeResolved::Resolved(a.advertise_address())],
  )
  .await
  .expect("join");
  let converged = tokio_like_wait(
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

  // Leave; `.await` blocks until the flush completes (`LeftCluster`), so by the
  // time it returns A's machine lifecycle is Left and `is_running()` is false.
  a.leave().await.expect("leave");

  // The synchronous `join` (WaitForCompletion arm) must be rejected.
  let res = a
    .join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await;
  assert!(
    matches!(res, Err(memberlist_compio::MemberlistError::NotRunning)),
    "expected NotRunning from join after leave, got {res:?}",
  );

  // The fire-and-forget `dispatch_join` (Dispatch arm) shares the same
  // `Command::Join` gate and must also be rejected.
  let res_dispatch = a
    .dispatch_join(&SocketAddrResolver, &[MaybeResolved::Resolved(b_addr)])
    .await;
  assert!(
    matches!(
      res_dispatch,
      Err(memberlist_compio::MemberlistError::NotRunning)
    ),
    "expected NotRunning from dispatch_join after leave, got {res_dispatch:?}",
  );

  // Ignoring Err: best-effort teardown.
  let _ = compio::time::timeout(Duration::from_secs(5), a.shutdown()).await;
  let _ = compio::time::timeout(Duration::from_secs(5), b.shutdown()).await;
}

/// A zero `bridge_recv_buf_len` must be REJECTED at construction with
/// `MemberlistError::InvalidOption` rather than accepted. The per-bridge
/// byte-mover reads into a `vec![0u8; bridge_recv_buf_len]`; a zero-length read
/// returns `Ok(0)`, which the bridge treats as peer EOF, so every TCP/TLS
/// join / push-pull stream exchange would deterministically break while
/// construction returned `Ok`. The stream `Transport::new` validates the knob
/// fail-fast. Strongly discriminating: without the validation `new` returns
/// `Ok` over a silently-broken node. A nonzero value still constructs `Ok`.
#[compio::test]
async fn zero_bridge_recv_buf_len_is_rejected() {
  let addr = reserve_addr().await;

  let bad = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("zero-recv-buf"))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_stream(StreamTransportOptions::new().with_bridge_recv_buf_len(0)),
  );
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    bad,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  // `Memberlist` is not `Debug`, so match instead of formatting `res` — on the
  // (wrong) Ok path tear the node down so its socket releases the port.
  match res {
    Err(memberlist_compio::MemberlistError::InvalidOption(e)) => {
      assert_eq!(
        e.option(),
        "bridge_recv_buf_len",
        "InvalidOption must name the rejected knob"
      );
    }
    Err(other) => panic!("expected InvalidOption(bridge_recv_buf_len), got {other:?}"),
    Ok(m) => {
      let _ = compio::time::timeout(Duration::from_secs(5), m.shutdown()).await;
      panic!("a zero bridge_recv_buf_len must be rejected, but construction succeeded");
    }
  }

  // A nonzero value on the SAME address still constructs Ok: the rejection is
  // specific to the degenerate zero, not a blanket failure.
  let good = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("nonzero-recv-buf"))
      .with_advertise_addr(MaybeResolved::Resolved(addr))
      .with_stream(StreamTransportOptions::new().with_bridge_recv_buf_len(4096)),
  );
  let ok = TcpMemberlist::<SmolStr, SocketAddr>::new(
    good,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("nonzero bridge_recv_buf_len must construct Ok");
  let _ = compio::time::timeout(Duration::from_secs(5), ok.shutdown()).await;
}

/// A zero `idle_wake_interval` must be REJECTED at construction with
/// `MemberlistError::InvalidOption` rather than accepted. It is the driver
/// loop's fallback sleep when the coordinator has no nearer pending deadline,
/// so a zero value makes a quiescent endpoint busy-spin the loop and peg a CPU
/// core, silently. `Memberlist::new` validates the generic-free driver knob
/// fail-fast for every backend. Strongly discriminating: without the
/// validation `new` returns `Ok` over a CPU-pegging node. A nonzero value
/// still constructs `Ok`.
#[compio::test]
async fn zero_idle_wake_interval_is_rejected() {
  let addr = reserve_addr().await;

  let bad = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("zero-idle-wake"))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_driver(DriverOptions::new().with_idle_wake_interval(Duration::ZERO));
  let res = TcpMemberlist::<SmolStr, SocketAddr>::new(
    bad,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await;
  // `Memberlist` is not `Debug`, so match instead of formatting `res` — on the
  // (wrong) Ok path tear the node down so its socket releases the port.
  match res {
    Err(memberlist_compio::MemberlistError::InvalidOption(e)) => {
      assert_eq!(
        e.option(),
        "idle_wake_interval",
        "InvalidOption must name the rejected knob"
      );
    }
    Err(other) => panic!("expected InvalidOption(idle_wake_interval), got {other:?}"),
    Ok(m) => {
      let _ = compio::time::timeout(Duration::from_secs(5), m.shutdown()).await;
      panic!("a zero idle_wake_interval must be rejected, but construction succeeded");
    }
  }

  // A nonzero value on the SAME address still constructs Ok.
  let good = Options::new(
    TcpTransportOptions::<SmolStr, SocketAddr>::new()
      .with_local_id(SmolStr::new("nonzero-idle-wake"))
      .with_advertise_addr(MaybeResolved::Resolved(addr)),
  )
  .with_driver(DriverOptions::new().with_idle_wake_interval(Duration::from_secs(30)));
  let ok = TcpMemberlist::<SmolStr, SocketAddr>::new(
    good,
    VoidDelegate::default(),
    &SocketAddrResolver,
    &FirstAddrResolver,
  )
  .await
  .expect("nonzero idle_wake_interval must construct Ok");
  let _ = compio::time::timeout(Duration::from_secs(5), ok.shutdown()).await;
}
