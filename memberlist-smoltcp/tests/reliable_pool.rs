//! Reliable-plane pool recovery: the TCP listener must survive pool exhaustion.
//!
//! A seed runs with the minimum viable pool — one socket for the listener plus
//! one for dials/accepts. When an inbound push/pull is accepted, the listener
//! socket is consumed by the exchange; the listener must then be replenished
//! from the spare so the next inbound still has a socket. Once the spare is also
//! in use the pool is momentarily empty, and a freed socket reaped back to the
//! pool must re-establish a missing listener — otherwise the seed permanently
//! stops accepting inbound reliable connections.

mod harness;

use core::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  time::Duration,
};

use memberlist_proto::EndpointOptions;
use memberlist_smoltcp::{Memberlist, Options, TransformOptions};
use smol_str::SmolStr;

fn addr(ip: u8, port: u16) -> SocketAddr {
  SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, ip)), port)
}

/// A minimum-pool (`tcp_pool_size = 2`) seed must accept MORE THAN ONE inbound
/// reliable connection over its lifetime.
///
/// One socket is the listener and one is the spare. The first accept consumes
/// the listener, which must be replenished from the spare; once both are in
/// flight the pool is empty, and a socket reaped after an exchange completes
/// must back the listener again. The peer (`b`) is given a short push/pull
/// interval so it re-dials the seed periodically; each re-dial is a fresh
/// inbound connection the seed must accept. The observable is the seed's
/// accepted-inbound counter, NOT membership: gossip can converge a peer with no
/// TCP accept at all, so only a direct accept count witnesses that the listener
/// keeps being re-established.
///
/// If the listener were not replenished/re-established as its socket is consumed
/// and reaped, the counter would stall (later inbound dials silently dropped);
/// the listener-maintenance paths let it climb to >= 2.
#[test]
fn min_pool_seed_accepts_repeated_inbound() {
  // Each push/pull settles in a handful of zero-latency ticks; this only bounds
  // a wedged plane so the test fails loudly instead of hanging. The 50 ms
  // push/pull interval (below) means `b` re-dials many times within the budget.
  const BUDGET: u32 = 1500;

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // Seed A with the minimum pool (two sockets): one listener plus one spare for
  // accepts. A only ever ACCEPTS inbound reliable connections, which is the path
  // the listener-loss bug breaks — each accept consumes the listener, which must
  // be replenished/re-established as sockets free. A short push/pull interval is
  // harmless here and keeps A's scheduler from interfering.
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new().with_tcp_pool_size(2),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946))
      .with_rng_seed(1)
      .with_push_pull_interval(Duration::from_millis(50)),
    &mut da,
    now,
  );
  a.start(now);

  // Joiner B with the default pool (a listener plus dial sockets). Its short
  // push/pull interval makes it re-dial A repeatedly after the initial join, so
  // A must accept a SECOND (and further) inbound connection.
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946))
      .with_rng_seed(2)
      .with_push_pull_interval(Duration::from_millis(50)),
    &mut db,
    now,
  );
  b.start(now);
  b.join(&[addr(1, 7946)]);

  // Drive both until A has accepted at least two inbound connections (the first
  // join plus at least one periodic push/pull re-dial). Without listener
  // self-healing this never reaches 2: A's listener vanishes after the first
  // accept consumes the lone pooled socket.
  let mut accepted_twice = false;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    // Sanity: the initial join must converge (proves the first accept worked
    // and the plane is otherwise healthy).
    if a.accepted_inbound_count() >= 2 {
      accepted_twice = true;
      break;
    }
    clk.advance_ms(10);
  }

  assert!(
    a.num_members() >= 2,
    "the initial join must converge (a={})",
    a.num_members()
  );
  assert!(
    accepted_twice,
    "the single-socket seed accepted only {} inbound connection(s): its TCP \
     listener was not re-established after the first accept consumed the lone \
     pooled socket, so every later inbound dial is silently dropped",
    a.accepted_inbound_count()
  );
}

/// A dial the joiner could not back with a socket (pool exhausted) must be
/// PRESERVED and dialed once a socket frees — not dropped — so a viable seed
/// reached while a dead seed holds the only dial socket still converges.
///
/// The joiner B is given `tcp_pool_size = 2`: one socket is its listener,
/// leaving exactly ONE dial socket. A dead seed is joined first and claims that
/// socket; its dial never establishes (the address answers no SYN) and its
/// bridge elapses at `stream_timeout`, freeing the socket. The viable seed A is
/// joined many ticks later — while the dead dial still holds the lone socket —
/// so A's `Connect` finds the pool empty and is deferred to `pending_dial`. When
/// the dead dial times out and frees the socket, `drain_pending_dials` dials A,
/// and B converges (`num_members() == 2`).
///
/// The stagger between the two joins is deliberate: every dial started in one
/// tick shares the same `now + stream_timeout` deadline, so a viable seed joined
/// in the SAME tick as the dead one would have its (correctly buffered) intent
/// expire at the very tick the dead seed's socket frees, leaving no window to
/// dial. Joining A later gives its bridge a later deadline that outlives the
/// dead seed's socket-free event — exactly the real-world ordering where a join
/// intent must survive a transiently full pool. The fix under test is that the
/// intent is BUFFERED rather than lost; the stagger only makes the resulting
/// dial deterministic.
///
/// Without `pending_dial`, A's `Connect` is consumed and discarded the instant
/// the pool is found empty; the freed socket then has no queued intent to redial
/// and B never dials A nor converges within the budget.
#[test]
fn pool_exhaustion_defers_dial_to_viable_later_seed() {
  // The dead dial elapses at `stream_timeout` (1 000 ms) and frees the lone dial
  // socket; A — joined at 950 ms, so its bridge deadline (~1 950 ms) outlives the
  // free event with room to complete the push/pull — is dialed and converges
  // over the zero-latency link. 4 000 ticks × 10 ms = 40 s virtual time is ample
  // headroom; it only bounds a wedged plane so a lost dial fails loudly.
  const BUDGET: u32 = 4000;

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // Real seed A on the wire (10.0.0.1). It accepts B's eventual push/pull.
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut da,
    now,
  );
  a.start(now);

  // Joiner B with two pooled sockets: one becomes B's listener, leaving exactly
  // ONE dial socket. A short `stream_timeout` makes the dead dial elapse quickly
  // so the freed socket cycles to the viable seed within the virtual-time budget.
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new().with_tcp_pool_size(2),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946))
      .with_rng_seed(2)
      .with_stream_timeout(Duration::from_millis(1000)),
    &mut db,
    now,
  );
  b.start(now);

  // Dead seed first: 192.168.0.9 is OFF B's /24, so B has no route and emits no
  // SYN at all — the dial simply never establishes and elapses at
  // `stream_timeout`, holding the lone dial socket until then. (An off-link dead
  // address avoids putting SYN-retransmit noise on the shared paired-device wire
  // that would compete with A's real push/pull frames.)
  let dead = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 9)), 7946);
  b.join(&[dead]);

  let mut joined = false;
  for i in 0..BUDGET {
    // Join the viable seed A partway through the dead dial's lifetime — while
    // the dead dial still holds the only socket, so A's Connect (and its
    // same-tick push/pull request bytes and graceful FIN) are deferred. Joining
    // it later than the dead seed gives A's bridge a later deadline that
    // outlives the dead seed's socket-free event, leaving budget to complete the
    // push/pull once the freed socket finally backs the deferred dial.
    if i == 95 {
      b.join(&[addr(1, 7946)]);
    }
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if b.num_members() == 2 {
      joined = true;
      break;
    }
    clk.advance_ms(10);
  }

  assert!(
    joined,
    "B never converged: the viable seed's dial intent was lost when the pool \
     was exhausted by the dead seed holding the only dial socket (b={})",
    b.num_members()
  );
}

/// A FAILED reliable exchange must ABORT — reclaiming its socket immediately at
/// `stream_timeout` — rather than draining stale bytes in `closing` until the
/// much-later `close_timeout`.
///
/// B joins A over TCP; the push/pull initiator (B) half-closes its dial socket
/// when it finishes sending its request, emitting a FIN while STILL awaiting A's
/// reply (the exchange stays mapped — the half-close lifecycle). The loop stops
/// polling A the instant B's FIN goes out (`half_closed_count() > 0`), BEFORE A
/// could reply or ACK, so A has vanished mid-exchange and B's socket is wedged in
/// FinWait with undelivered request bytes still parked.
///
/// With the peer gone, B's bridge elapses at `stream_timeout`. That is a FAILED
/// terminal phase (an elapsed exchange deadline), so the machine now emits
/// `StreamAction::Abort` — NOT `Close` — and the driver's `abort_exchange`
/// hard-resets the socket (RST) and returns it straight to the pool. The stale
/// request bytes are discarded, not drained.
///
/// `close_timeout` is set MUCH larger than `stream_timeout`: if the failed
/// exchange were (wrongly) routed through the graceful `Close` → `teardown` path
/// it would park in `closing` and the socket would be held for the full
/// `close_timeout`. The assertion that the pool recovers shortly AFTER
/// `stream_timeout` — with `closing_count()` never rising — is what proves the
/// abort path fired promptly instead.
///
/// Fail-then-pass: before the machine change (or with the failed reap routed back
/// to `Close`), B's socket parks in `closing` and the pool stays depressed until
/// `close_timeout` (10x later), so the prompt-reclaim assertion fails.
#[test]
fn failed_exchange_aborts_and_reclaims_socket_at_stream_timeout() {
  const BUDGET: u32 = 4000;
  // close_timeout is 10x stream_timeout: a `closing` park (the graceful path)
  // would hold the socket until here, far past the prompt-abort window asserted
  // below — so reclaiming early can ONLY be the abort path.
  const CLOSE_TIMEOUT_MS: u64 = 4000;
  // Short stream timeout so B's bridge — which keeps the half-closed exchange
  // mapped while awaiting the vanished peer's reply — fails (elapsed deadline)
  // promptly within the budget.
  const STREAM_TIMEOUT_MS: u64 = 400;
  // The prompt-reclaim window: a few ticks past `stream_timeout`. An abort
  // reclaims at the timeout tick; this slack only covers the driver's own
  // poll cadence. It is FAR below `close_timeout`, so a `closing` park cannot
  // satisfy it.
  const ABORT_SLACK_MS: u64 = 200;

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut da,
    now,
  );
  // B uses the default pool plus a LARGE close timeout and a short stream timeout.
  // Record the pristine free count so we can assert it recovers to exactly that.
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new().with_close_timeout(Duration::from_millis(CLOSE_TIMEOUT_MS)),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946))
      .with_rng_seed(2)
      .with_stream_timeout(Duration::from_millis(STREAM_TIMEOUT_MS)),
    &mut db,
    now,
  );
  a.start(now);
  b.start(now);

  let free_at_construction = b.pool_free_count();

  // Phase 1 — B joins A over a real TCP push/pull and drives BOTH until B's dial
  // socket FIRST emits its graceful write-half FIN (`half_closed_count() > 0`).
  // The loop STOPS A the instant that happens: A was last polled at the top of
  // this same iteration, BEFORE B emitted its FIN at the end, so A has not seen —
  // and will never ACK or reply to — the FIN. B's socket is therefore wedged in
  // FinWait with a vanished peer, the exchange still mapped (the bridge awaits the
  // reply that never comes), and a socket held out of the pool.
  b.join(&[addr(1, 7946)]);
  let mut froze_at = None;
  for _ in 0..BUDGET {
    // Poll A then B every iteration until B half-closes; A is never polled again
    // afterward (the loop breaks), so it has not seen B's just-emitted FIN.
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if b.half_closed_count() > 0 {
      // The FIN is out and A has not replied: A is now frozen (the loop breaks).
      // Record the instant so the abort deadline can be checked against it.
      froze_at = Some(clk.now());
      break;
    }
    clk.advance_ms(10);
  }
  let froze_at = froze_at.expect("B never emitted a graceful FIN from the join exchange");
  // The socket backing the half-closed exchange is held out of the pool.
  assert!(
    b.pool_free_count() < free_at_construction,
    "expected the in-flight exchange's socket to be held out of the pool \
     (free={}, construction={})",
    b.pool_free_count(),
    free_at_construction
  );

  // Phase 2 — A has VANISHED (never polled again). Drive ONLY B. The bridge's
  // exchange deadline elapses at ~`stream_timeout` and FAILS; the machine emits
  // `StreamAction::Abort`, and the driver hard-resets the socket and reclaims it
  // straight to the pool. Assert the pool recovers — and `closing_count()` never
  // rises — WITHIN the prompt-abort window, well before `close_timeout`.
  //
  // The deadline by which the abort MUST have fired: the freeze instant plus the
  // stream timeout plus a small slack for the driver's poll cadence. This is far
  // below `close_timeout`, so a graceful `closing` park (which would hold the
  // socket until `close_timeout`) cannot satisfy it.
  let abort_by =
    froze_at + Duration::from_millis(STREAM_TIMEOUT_MS) + Duration::from_millis(ABORT_SLACK_MS);
  let mut reclaimed_promptly = false;
  let mut ever_parked_in_closing = false;
  for _ in 0..BUDGET {
    let _ = b.poll(clk.now(), &mut db);
    // The failed exchange must NEVER park in `closing` — an abort reclaims
    // straight to the pool. Witness that the socket is never held in the closing
    // set on the way to recovery.
    if b.closing_count() > 0 {
      ever_parked_in_closing = true;
    }
    if b.pool_free_count() >= free_at_construction {
      reclaimed_promptly = clk.now() <= abort_by;
      break;
    }
    clk.advance_ms(10);
  }
  assert!(
    !ever_parked_in_closing,
    "the FAILED exchange parked in `closing` instead of aborting: a timed-out \
     exchange must hard-reset its socket and reclaim it straight to the pool, \
     not drain stale bytes until close_timeout (closing={}, free={})",
    b.closing_count(),
    b.pool_free_count(),
  );
  assert!(
    reclaimed_promptly,
    "B's failed-exchange socket was not reclaimed promptly at stream_timeout: \
     it was held until {} (abort deadline was stream_timeout + slack), so the \
     failed reap did not abort — it drained in `closing` toward close_timeout \
     ({}ms, 10x stream_timeout). closing={}, free={} (construction had {}).",
    if clk.now() <= abort_by {
      "the abort window"
    } else {
      "past the abort window"
    },
    CLOSE_TIMEOUT_MS,
    b.closing_count(),
    b.pool_free_count(),
    free_at_construction,
  );
}

/// The `close_timeout` reclaim is honored under the REAL wake contract: a caller
/// that sleeps exactly to the instant `poll()` returns — never on a fixed tick —
/// must still wake in time to abort a FAILED reliable exchange whose peer
/// vanished, reclaiming its socket at `stream_timeout`.
///
/// This drives B as the documented super-loop intends: each iteration calls
/// `poll()`, then sleeps to EXACTLY the returned deadline when it is a future
/// instant (a single fixed step only when `poll()` returns `None` or a past/now
/// "poll again" signal). It is the precise behavior of a real embedded caller
/// that sleeps to the returned wakeup and no longer.
///
/// The deadline by which a vanished-peer exchange fails is the bridge's exchange
/// deadline (`stream_timeout`), enforced only when a `poll()` tick runs the
/// machine's `handle_timeout`. The failed reap emits `StreamAction::Abort`, and
/// the driver's `abort_exchange` hard-resets the socket and reclaims it straight
/// to the pool. If `stream_timeout` were NOT folded into `poll()`'s returned
/// wakeup, a deadline-driven caller would sleep right past it: its sleeps are
/// bounded only by the unrelated stack/machine timers `poll()` does return, so
/// the failed socket would be reclaimed late or not at all. The machine folds the
/// bridge deadline into its `poll_timeout`, which `poll()` surfaces — that is
/// what guarantees the caller wakes by ~`stream_timeout` to run the reap.
///
/// The assertion is tight: the socket must be reclaimed by a `poll()` taken at a
/// virtual time at or before `froze_at + stream_timeout + slack`. Driven only by
/// the wakeups `poll()` returns, that holds iff the bridge deadline is one of
/// them. A LARGE `close_timeout` (10x `stream_timeout`) ensures the assertion
/// cannot be satisfied by a graceful `closing` park: an abort reclaims promptly,
/// a park would hold the socket far past the window.
#[test]
fn failed_exchange_abort_honored_when_driven_by_returned_deadline() {
  const BUDGET: u32 = 4000;
  // close_timeout is 10x stream_timeout: a `closing` park would hold the socket
  // until here, far past the prompt-abort window asserted below.
  const CLOSE_TIMEOUT_MS: u64 = 4000;
  // Short stream timeout so B's bridge — which keeps the half-closed exchange
  // mapped while awaiting the vanished peer's reply — fails (elapsed deadline)
  // within the budget.
  const STREAM_TIMEOUT_MS: u64 = 400;
  // Slack past `stream_timeout` for the driver's poll cadence; FAR below
  // `close_timeout`, so a `closing` park cannot satisfy the deadline.
  const ABORT_SLACK_MS: u64 = 200;
  // Fallback step used only for a poll() result that is NOT a future sleep
  // target — `None`, or smoltcp's "poll again now" signal (a deadline at/before
  // `now`, e.g. a packet pending dispatch this instant). One millisecond is the
  // clock's finest unit: it lets such an instant make forward progress without
  // jumping the clock far enough to step over the bridge deadline a genuine sleep
  // would land on.
  const FALLBACK_STEP_MS: u64 = 1;

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("a"), addr(1, 7946)).with_rng_seed(1),
    &mut da,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new().with_close_timeout(Duration::from_millis(CLOSE_TIMEOUT_MS)),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946))
      .with_rng_seed(2)
      .with_stream_timeout(Duration::from_millis(STREAM_TIMEOUT_MS)),
    &mut db,
    now,
  );
  a.start(now);
  b.start(now);

  let free_at_construction = b.pool_free_count();

  // Phase 1 — B joins A over a real TCP push/pull. Both are driven (a small fixed
  // tick is fine here; the wake contract is exercised in phase 2) until B's dial
  // socket FIRST emits its graceful write-half FIN (`half_closed_count() > 0`),
  // at which point A is frozen — A was last polled at the top of this iteration,
  // BEFORE B emitted its FIN, so A never sees, ACKs, or replies to it: the peer
  // has vanished mid-exchange. B's bridge keeps the half-closed exchange mapped
  // and then FAILS at `stream_timeout`. Record the virtual instant of the freeze;
  // the abort deadline is that instant plus `stream_timeout` plus a small slack.
  b.join(&[addr(1, 7946)]);
  let mut froze_at = None;
  for _ in 0..BUDGET {
    // Poll A then B every iteration until B half-closes; A is never polled again
    // afterward (the loop breaks), so it has not seen B's just-emitted FIN.
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if b.half_closed_count() > 0 {
      froze_at = Some(clk.now());
      break;
    }
    clk.advance_ms(FALLBACK_STEP_MS);
  }
  let froze_at =
    froze_at.expect("B never half-closed the join exchange before the peer was frozen");
  // The instant by which the abort MUST have happened under the wake contract.
  // FAR below `froze_at + close_timeout`, so a graceful `closing` park cannot
  // satisfy it — only a prompt abort at the bridge deadline can.
  let abort_by =
    froze_at + Duration::from_millis(STREAM_TIMEOUT_MS) + Duration::from_millis(ABORT_SLACK_MS);

  // Phase 2 — A has VANISHED (never polled again). Drive ONLY B as the real
  // super-loop: poll, then sleep to EXACTLY poll()'s returned deadline. The failed
  // exchange's socket must be reclaimed by a poll taken no later than `abort_by`.
  // Driven only by the deadlines poll() actually returns, that holds iff the
  // bridge's exchange deadline is one of them — which is what folding the machine
  // `poll_timeout` into poll()'s wakeup guarantees. The socket must NEVER park in
  // `closing` (an abort reclaims straight to the pool).
  let mut reclaimed_in_time = false;
  let mut ever_parked_in_closing = false;
  for _ in 0..BUDGET {
    let next = b.poll(clk.now(), &mut db);

    if b.closing_count() > 0 {
      ever_parked_in_closing = true;
    }
    if b.pool_free_count() >= free_at_construction {
      // Reclaimed — and because we only ever slept to the FUTURE deadlines poll()
      // returned, the clock advanced past `abort_by` only if poll() actually
      // offered an instant at/after it. Folding the bridge deadline into the
      // wakeup makes the soonest such instant ~`stream_timeout`, so this poll runs
      // at-or-before `abort_by`.
      reclaimed_in_time = clk.now() <= abort_by;
      break;
    }

    // Honor the real wake contract:
    //
    // - A deadline strictly in the FUTURE is a sleep target — advance to exactly
    //   it (no clamp to `abort_by`; clamping would inject the very wake a missing
    //   fold omits and mask the defect).
    // - A deadline at or before `now` is smoltcp's "poll again immediately"
    //   signal (`PollAt::Now`, e.g. a FIN packet pending dispatch this instant),
    //   not a sleep target; a real caller re-polls without sleeping. Model the
    //   negligible wall time that tight re-poll consumes with one clock unit so a
    //   pending-dispatch instant cannot spin virtual time in place — exactly as a
    //   `None` (no-deadline) tick steps. Either way the clock only ever moves to
    //   instants poll() sanctioned, so reaching `abort_by` proves poll() offered
    //   an instant there.
    match next {
      Some(deadline) if deadline > clk.now() => clk.advance_to(deadline),
      _ => clk.advance_ms(FALLBACK_STEP_MS),
    }

    // Stop once virtual time has slept past the abort deadline without reclaiming:
    // under the wake contract a compliant driver would have woken AT `abort_by`
    // and aborted, so crossing it still holding the socket is the bug.
    if clk.now() > abort_by {
      break;
    }
  }

  assert!(
    !ever_parked_in_closing,
    "the FAILED exchange parked in `closing` instead of aborting: closing={}, \
     free={}. A timed-out exchange must hard-reset its socket and reclaim it \
     straight to the pool.",
    b.closing_count(),
    b.pool_free_count(),
  );
  assert!(
    reclaimed_in_time,
    "B's failed-exchange socket was not reclaimed by stream_timeout under the \
     returned-deadline wake contract: closing={}, free={} (construction had {}), \
     now_past_abort_by={}. The poll() wakeup omitted the bridge deadline, so a \
     caller sleeping only to the returned instant slept past stream_timeout.",
    b.closing_count(),
    b.pool_free_count(),
    free_at_construction,
    clk.now() > abort_by,
  );
}

/// A push/pull initiator must process its peer's reply that arrives AFTER the
/// initiator's own graceful write-half FIN (the normal ACK-before-reply TCP
/// ordering) — the exchange must stay mapped across the half-close, drain the
/// late reply + the peer's FIN, and complete over the reliable plane.
///
/// `StreamAction::Shutdown` closes only the local SEND half once the initiator
/// has finished sending its request; the bridge keeps READING for the reply.
/// smoltcp `close()` likewise closes only the transmit half — a FinWait socket
/// still receives — so the exchange must remain in `by_exchange` for the inbound
/// pump to deliver the peer's later reply bytes and its FIN (EOF). If the driver
/// instead detached the exchange when it emitted the FIN, the peer's reply (sent
/// in a segment AFTER its ACK of the request — standard TCP) would never be
/// pumped into the machine, and the exchange would time out at its bridge
/// deadline despite a valid response.
///
/// # Forcing the late reply deterministically
///
/// The paired device is zero-latency, so a reply would otherwise arrive in the
/// same tick as the ACK and mask the bug. The loop instead drives the two nodes
/// asymmetrically: it polls BOTH until the joiner B has emitted its FIN — at
/// which point the acceptor A has not yet replied (B is still a lone member) —
/// then PAUSES A for several ticks so A's reply is held back, landing strictly
/// AFTER B's FIN. Resuming A lets it send the reply + its FIN, which B must still
/// process across its half-closed socket. The FIN is detected via
/// `half_closed_count() > 0` (the fix keeps the exchange mapped here) OR
/// `closing_count() > 0` (the detach-on-Shutdown bug parks it instead) so the
/// delay is armed on either code path and the bug fails at the convergence check
/// rather than silently never delaying the reply.
///
/// # Why convergence proves the reliable path (not gossip)
///
/// Gossip, probe, and periodic push/pull are all disabled (hour-long intervals),
/// so the one-shot JOIN push/pull is the ONLY channel that can sync A's state
/// into B. B reaching `num_members() == 2` therefore proves B committed A's
/// reliable reply — impossible if the reply were dropped at the half-close — and
/// the socket returning to the pool proves the exchange COMPLETED rather than
/// timing out. On the detach-on-Shutdown bug B never pumps the reply, so with
/// gossip disabled it never converges and the exchange times out instead.
#[test]
fn delayed_reply_after_half_close_still_completes() {
  // The reliable round-trip settles in a handful of zero-latency ticks once A
  // resumes; this only bounds a wedged plane so the test fails loudly. A short
  // stream timeout keeps a (buggy-path) stalled exchange from masking a hang as
  // success — it must converge well before the bridge deadline, not after it.
  const BUDGET: u32 = 1500;
  const STREAM_TIMEOUT_MS: u64 = 600;
  // Ticks to withhold A after B's FIN, so A's reply lands strictly after it.
  const PAUSE_TICKS: u32 = 8;
  // Long enough to disable the periodic schedulers for the test's duration, so
  // only the one-shot join push/pull can converge B.
  const NEVER: Duration = Duration::from_secs(3600);

  let quiet = |id: &str, ip: u8| {
    EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
      .with_rng_seed(ip as u64)
      .with_gossip_interval(NEVER)
      .with_probe_interval(NEVER)
      .with_push_pull_interval(NEVER)
  };

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    quiet("a", 1),
    &mut da,
    now,
  );
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    quiet("b", 2).with_stream_timeout(Duration::from_millis(STREAM_TIMEOUT_MS)),
    &mut db,
    now,
  );
  a.start(now);
  b.start(now);

  let free_at_construction = b.pool_free_count();
  b.join(&[addr(1, 7946)]);

  let mut a_paused_for = 0u32;
  let mut withheld_reply = false;
  let mut converged = false;
  for _ in 0..BUDGET {
    // Poll A unless it is withheld to delay its reply past B's FIN.
    if a_paused_for == 0 {
      let _ = a.poll(clk.now(), &mut da);
    } else {
      a_paused_for -= 1;
    }
    let _ = b.poll(clk.now(), &mut db);

    // The instant B's FIN goes out — and A, last polled before it, has not yet
    // replied (B is still a lone member) — withhold A so its reply is delayed.
    // The FIN is observable on BOTH the fixed and the detach-on-Shutdown paths:
    // the fix keeps the exchange mapped (`half_closed_count() > 0`), the bug
    // detaches it into the closing set (`closing_count() > 0`). Triggering on
    // either makes the test detect the FIN regardless, so on the buggy path it
    // still pauses A and then fails at the convergence check (reply dropped)
    // rather than silently never arming the delay.
    if !withheld_reply && (b.half_closed_count() > 0 || b.closing_count() > 0) {
      assert_eq!(
        b.num_members(),
        1,
        "the acceptor must not have replied before the initiator's FIN — the test \
         would not exercise a post-FIN reply otherwise"
      );
      a_paused_for = PAUSE_TICKS;
      withheld_reply = true;
    }

    // B converges only by committing A's reliable reply (gossip is disabled).
    if b.num_members() == 2 {
      converged = true;
      break;
    }
    clk.advance_ms(10);
  }

  assert!(
    withheld_reply,
    "B never emitted its graceful FIN, so the post-FIN reply path was never \
     exercised (b_members={}, b_half_closed={}, b_closing={})",
    b.num_members(),
    b.half_closed_count(),
    b.closing_count()
  );
  assert!(
    converged,
    "B never converged: its peer's reply, delayed until after B's graceful FIN, \
     was dropped instead of pumped across the half-closed exchange (gossip is \
     disabled, so the reliable reply is the only sync channel) — the exchange \
     timed out at its bridge deadline despite a valid response (b_members={})",
    b.num_members()
  );

  // The exchange COMPLETED (peer reply + FIN committed) rather than timing out:
  // drive a few more ticks so B's terminal `Close` reclaims the socket, and
  // assert the pool fully recovers to its construction count.
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if b.pool_free_count() >= free_at_construction && b.half_closed_count() == 0 {
      break;
    }
    clk.advance_ms(10);
  }
  assert_eq!(
    b.pool_free_count(),
    free_at_construction,
    "B's reliable socket was not reclaimed after the exchange completed \
     (half_closed={}, closing={})",
    b.half_closed_count(),
    b.closing_count()
  );
}

/// The inbound listener has FIRST claim on a freed socket: when exactly one
/// socket frees while an inbound is ready to be accepted (or the listener was
/// just lost to an accept it could not yet replenish) AND a deferred outbound
/// dial is waiting, the listener is (re-)established from that socket BEFORE the
/// pending dial can take it. A node driven from a single spare socket must never
/// have its inbound listener starved by outbound dial intent.
///
/// # The race, constructed deterministically
///
/// Node `n` has `tcp_pool_size = 2`: one socket is its listener, leaving exactly
/// ONE dial socket. The race is assembled from three staggered events so that a
/// single contended poll sees one free socket, one waiting `PendingDial`, and a
/// listener that must be (re-)established:
///
/// 1. `n` dials a dead off-link seed `d1` at the start. `d1` is off `n`'s /24, so
///    `n` emits no SYN and the dial sits in SynSent holding the lone dial socket
///    until its bridge elapses at `stream_timeout`; the pool is now empty.
/// 2. Later (a deliberate stagger), `n` dials a second dead off-link seed `d2`.
///    The pool is empty, so this dial is deferred as a `PendingDial` with a
///    bridge deadline LATER than `d1`'s — so it is still waiting at the tick
///    `d1`'s socket frees, rather than expiring in the same tick (every dial
///    started in one tick shares one `now + stream_timeout` deadline).
/// 3. A real peer `p` dials `n` once, timed so its three-way handshake completes
///    right as `d1`'s socket frees — making `n`'s listener the contested
///    consumer of that one socket against the still-waiting `d2` `PendingDial`.
///
/// At the contended poll `d1`'s freed socket is the ONLY free socket. With the
/// listener-first order the listener claims it (re-established) and `d2` keeps
/// waiting. With the buggy order (pending dials drained before the listener is
/// accepted/replenished) `d2` steals it and the listener stays `None`, starving
/// inbound — every later inbound dial is then silently dropped.
///
/// # What is asserted
///
/// - The contended poll is witnessed directly at the tick `p`'s inbound is
///   accepted (`accepted_inbound_count` rises to 1): at that very poll the
///   listener is PRESENT and a `PendingDial` is STILL waiting. Because the accept
///   consumed the listener and the only socket free at that instant is the one
///   `d1` just released, the listener being present again while the dial still
///   waits proves that freed socket went to the listener, not the dial — and the
///   accept-replenish + rebalance are all listener-first within the tick, so the
///   re-establishment is not externally observable as an absent→present flip; the
///   load-bearing fact is that the dial did NOT steal the socket. Under the buggy
///   order the dial drains before the listener replenishes, so at that poll the
///   pending dial count would have dropped to zero and the listener stayed absent.
/// - Corroboration that inbound is genuinely alive afterwards: `p` dials `n` a
///   SECOND time and `n` accepts it (`accepted_inbound_count()` climbs past the
///   first accept). On the buggy order the listener is permanently gone after the
///   contended poll, so no second inbound is ever accepted.
#[test]
fn listener_keeps_first_claim_on_freed_socket_over_pending_dial() {
  // Zero-latency virtual time; the budget only bounds a wedged plane so a
  // regression fails loudly instead of hanging.
  const BUDGET: u32 = 400;
  // `d1`'s dead dial frees the lone dial socket at this deadline; the stagger and
  // `p`'s join are timed around it (see below).
  const STREAM_TIMEOUT_MS: u64 = 300;
  // Tick at which `n` dials the second dead seed, so its `PendingDial` deadline
  // (~`tick_d2 * 10ms + STREAM_TIMEOUT_MS`) outlives `d1`'s socket-free event.
  const D2_JOIN_TICK: u32 = 20;
  // Tick at which `p` dials `n`, timed so the inbound handshake settles as `d1`'s
  // socket frees, putting the listener and the pending dial in contention for it.
  const P_JOIN_TICK: u32 = 27;

  let (mut dn, mut dp) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // `n` is the contended node: pool size 2 (listener + one dial socket). Its own
  // periodic schedulers are disabled so the only sockets in play are the listener,
  // the dead dials, and the inbound from `p` — keeping the contended poll exact.
  let mut n: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new().with_tcp_pool_size(2),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("n"), addr(1, 7946))
      .with_rng_seed(1)
      .with_stream_timeout(Duration::from_millis(STREAM_TIMEOUT_MS))
      .with_push_pull_interval(Duration::from_secs(3600))
      .with_probe_interval(Duration::from_secs(3600))
      .with_gossip_interval(Duration::from_secs(3600)),
    &mut dn,
    now,
  );
  n.start(now);

  // `p` is a real peer on the wire. Its periodic schedulers are disabled so it
  // dials `n` only when explicitly told to (the timed `join`s below), making each
  // inbound on `n` a deliberate, placeable event.
  let mut p: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("p"), addr(2, 7946))
      .with_rng_seed(2)
      .with_push_pull_interval(Duration::from_secs(3600))
      .with_probe_interval(Duration::from_secs(3600))
      .with_gossip_interval(Duration::from_secs(3600)),
    &mut dp,
    now,
  );
  p.start(now);

  // Two dead off-link seeds (off `n`'s 10.0.0.0/24 — `n` has no route, emits no
  // SYN, so each dial simply sits until its bridge elapses at `stream_timeout`).
  let d1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 9)), 7946);
  let d2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 10)), 7946);

  // Event 1: `d1` takes the lone dial socket immediately.
  n.join(&[d1]);

  // Drive the race. The contended poll is the one where `p`'s inbound is accepted
  // (`accepted_inbound_count` first reaches 1): the accept consumes the listener
  // and the only socket free at that instant is the one `d1` just released. At
  // that poll the listener must be re-established AND the deferred `d2` dial must
  // still be waiting — proving the freed socket went to the listener, not the
  // dial. The replenish and rebalance are listener-first WITHIN the tick, so this
  // is not observable as an absent->present flip across polls; the load-bearing
  // fact is that `d2` did not steal the socket (it is still PendingDial).
  let mut listener_reclaimed_freed_socket = false;
  let mut first_accept_after_contention = 0u64;
  for i in 0..BUDGET {
    // Event 2: the staggered second dead dial becomes a waiting `PendingDial`.
    if i == D2_JOIN_TICK {
      n.join(&[d2]);
    }
    // Event 3: `p`'s single inbound, timed to settle as `d1`'s socket frees.
    if i == P_JOIN_TICK {
      p.join(&[addr(1, 7946)]);
    }

    let _ = n.poll(clk.now(), &mut dn);
    let _ = p.poll(clk.now(), &mut dp);

    // The contended poll: `p`'s inbound has just been accepted. The listener it
    // consumed must be present again (re-established from `d1`'s freed socket)
    // while `d2` is STILL deferred — the freed socket went to the listener, not
    // the dial. Under the buggy order the dial would have taken it: the pending
    // count would be zero here and the listener absent.
    if n.accepted_inbound_count() >= 1 {
      listener_reclaimed_freed_socket = n.listener_present() && n.pending_dial_count() >= 1;
      first_accept_after_contention = n.accepted_inbound_count();
      break;
    }
    clk.advance_ms(10);
  }

  assert!(
    listener_reclaimed_freed_socket,
    "the inbound listener did not reclaim the freed socket ahead of the waiting \
     pending dial at the contended accept: it was starved by deferred outbound \
     intent (the dial took the one free socket). listener_present={}, \
     pending_dial_count={}, accepted_inbound={}",
    n.listener_present(),
    n.pending_dial_count(),
    n.accepted_inbound_count(),
  );

  // Corroboration: with the listener re-established, a fresh inbound is still
  // accepted. `p` dials `n` a second time; `n`'s accept count must climb past the
  // accept it had at the contended poll. On the buggy order the listener is gone
  // for good here, so this never happens.
  p.join(&[addr(1, 7946)]);
  let mut accepted_again = false;
  for _ in 0..BUDGET {
    let _ = n.poll(clk.now(), &mut dn);
    let _ = p.poll(clk.now(), &mut dp);
    if n.accepted_inbound_count() > first_accept_after_contention {
      accepted_again = true;
      break;
    }
    clk.advance_ms(10);
  }
  assert!(
    accepted_again,
    "after the contended poll the listener accepted no further inbound \
     (accepted stuck at {}): the listener was not truly re-established, so \
     inbound reliable connections are starved",
    n.accepted_inbound_count(),
  );
}

/// A push/pull RESPONSE larger than the responder's TCP tx ring must be delivered
/// in FULL across the graceful close — the terminal FIN must not truncate the
/// bytes still buffered in the driver's per-exchange `out` queue.
///
/// The reliable plane models each exchange as a `Connection` whose `out` holds
/// outbound bytes pulled from the machine but not yet accepted by smoltcp's tx
/// ring: when a response exceeds `tcp_socket_tx_bytes`, `pump_outbound_reliable`
/// writes what fits and parks the remainder in `out`, flushing it across later
/// ticks as the peer ACKs and frees ring space. The acceptor then receives the
/// machine's `StreamAction::Close` for the completed exchange. If teardown FIN-ed
/// (or removed the `Connection`) on the spot, the bytes still in `out` would be
/// dropped and the FIN would close a TRUNCATED frame — the joiner would never
/// receive the full push/pull reply, so with gossip disabled it could not
/// converge. The fix drains `out` (and the unacknowledged tx ring) BEFORE the
/// terminal FIN, so the whole reply reaches the peer.
///
/// # Forcing an oversized response deterministically
///
/// The seed A is pre-seeded (`inject_alive`) with many extra peers, so its
/// push/pull reply carries a large membership snapshot, AND A's
/// `tcp_socket_tx_bytes` is set small so that snapshot cannot fit the tx ring in
/// one shot and MUST drain across ticks — the exact partial-write backpressure
/// that parks bytes in `out`. The joiner B then learns A plus every injected peer
/// from that one reply.
///
/// # Why convergence proves the full reliable reply (not gossip, not a prefix)
///
/// Gossip, probe, and periodic push/pull are all disabled (hour-long intervals)
/// on both nodes, so the one-shot JOIN push/pull is the ONLY channel that can
/// sync A's state into B. B is asserted to reach the FULL member count
/// (`1 self + A + all injected peers`): a truncated reply would fail to decode (a
/// partial frame is not a valid push/pull) and B would learn nothing from it,
/// leaving `num_members()` at 1. Only delivering every buffered byte lets B
/// commit the whole snapshot — so if the terminal `Close` dropped the response
/// remainder still parked in `out`, B would never receive the full reply and
/// never converge over the reliable plane.
#[test]
fn oversized_push_pull_response_is_not_truncated_by_close() {
  // The reliable round-trip settles in a handful of zero-latency ticks once the
  // response drains; this only bounds a wedged plane so the test fails loudly. A
  // short stream timeout keeps a stalled (buggy-path) exchange from masking a
  // hang as success — B must converge well before any bridge deadline.
  const BUDGET: u32 = 3000;
  const STREAM_TIMEOUT_MS: u64 = 2000;
  // Disable every periodic scheduler so the one-shot join push/pull is the only
  // channel that can sync A's state into B.
  const NEVER: Duration = Duration::from_secs(3600);
  // Peers injected into A on top of A itself. Sized so A's push/pull reply (one
  // `PushNodeState` per member) far exceeds the small tx ring below and must
  // drain across many ticks, exercising the parked-`out` remainder the close
  // would otherwise drop.
  const INJECTED_PEERS: usize = 60;
  // A's per-socket tx ring, deliberately tiny so the large reply cannot be
  // written in one `send_slice` and the remainder parks in `out`.
  const A_TX_RING: usize = 512;

  let quiet = |id: &str, ip: u8| {
    EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
      .with_rng_seed(ip as u64)
      .with_gossip_interval(NEVER)
      .with_probe_interval(NEVER)
      .with_push_pull_interval(NEVER)
  };

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // Seed A with a small tx ring so its oversized reply must drain across ticks.
  // The field is `pub`; set it on the owned Options (the builder exposes no tx
  // setter). A larger rx ring keeps A able to read B's inbound request in one go.
  let mut a_cfg = Options::new();
  a_cfg.tcp_socket_tx_bytes = A_TX_RING;
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    a_cfg,
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    quiet("a", 1),
    &mut da,
    now,
  );
  a.start(now);

  // Inject many synthetic Alive peers into A so its push/pull reply carries a
  // large membership snapshot. They are off the wire (never polled), but their
  // state still rides A's reply, inflating it past the tx ring. Use a /16 so the
  // 60 distinct host addresses do not collide with A's or B's /24.
  for i in 0..INJECTED_PEERS {
    let octet_hi = (i / 250) as u8;
    let octet_lo = (i % 250) as u8 + 1;
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, octet_hi, octet_lo)), 7946);
    a.inject_alive(SmolStr::new(std::format!("peer-{i}")), peer, now);
  }
  // A now knows itself plus every injected peer.
  let a_members_before = a.num_members();
  assert_eq!(
    a_members_before,
    1 + INJECTED_PEERS,
    "A should know itself plus all injected peers before the join"
  );

  // Joiner B: default sockets, schedulers disabled, short stream timeout. It
  // starts as a lone member and must learn A's whole snapshot from the one reply.
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    quiet("b", 2).with_stream_timeout(Duration::from_millis(STREAM_TIMEOUT_MS)),
    &mut db,
    now,
  );
  b.start(now);
  assert_eq!(b.num_members(), 1, "B starts as a lone member");

  b.join(&[addr(1, 7946)]);

  // Drive both until B has learned the FULL snapshot: itself + A + every injected
  // peer. A truncated reply decodes to nothing, so B would stay at 1.
  let expected = 1 + a_members_before; // B itself, plus A and all its peers.
  let mut converged = false;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if b.num_members() >= expected {
      converged = true;
      break;
    }
    clk.advance_ms(10);
  }

  assert!(
    converged,
    "B did not receive A's full push/pull reply: it converged to only {} of the \
     expected {} members. The oversized response ({}+ peers over a {}-byte tx \
     ring) was truncated when the graceful Close dropped the bytes still parked \
     in the exchange's `out` queue, so B never committed the full snapshot \
     (gossip is disabled — the reliable reply is the only sync channel).",
    b.num_members(),
    expected,
    INJECTED_PEERS,
    A_TX_RING,
  );
}

/// A graceful close whose drain takes LONGER than `close_timeout` in total but
/// makes progress every tick (a slow-but-continuously-reading peer) must NOT be
/// truncated: `close_timeout` is a NO-PROGRESS (idle) bound, not a total-drain
/// cap.
///
/// A replies with a large membership snapshot over a tiny tx ring, so the drain
/// spans many ticks; `close_timeout` is set FAR below the total drain time. B
/// reads continuously (every tick acks more), so the drain never stalls. With a
/// total-duration cap the `Closing` connection would be force-aborted partway
/// (truncating the reply, leaving B short of the full snapshot); with the idle
/// timeout the deadline re-arms on each ack and the close completes.
#[test]
fn slow_but_progressing_close_is_not_capped_by_close_timeout() {
  const BUDGET: u32 = 4000;
  const STREAM_TIMEOUT_MS: u64 = 4000;
  const NEVER: Duration = Duration::from_secs(3600);
  // A large snapshot over a tiny tx ring → a drain that spans far more ticks than
  // `close_timeout`.
  const INJECTED_PEERS: usize = 120;
  const A_TX_RING: usize = 256;
  // 50 ms = 5 ticks. The drain spans dozens of ticks, so a total cap would abort
  // mid-drain; the idle timeout re-arms every tick B acks.
  const CLOSE_TIMEOUT_MS: u64 = 50;
  const TICK_MS: u64 = 10;

  let quiet = |id: &str, ip: u8| {
    EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
      .with_rng_seed(ip as u64)
      .with_gossip_interval(NEVER)
      .with_probe_interval(NEVER)
      .with_push_pull_interval(NEVER)
  };

  let (mut da, mut db) = harness::link(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  let mut a_cfg = Options::new();
  a_cfg.tcp_socket_tx_bytes = A_TX_RING;
  a_cfg.close_timeout = Duration::from_millis(CLOSE_TIMEOUT_MS);
  let mut a: Memberlist<SmolStr, _> = Memberlist::new(
    a_cfg,
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    quiet("a", 1),
    &mut da,
    now,
  );
  a.start(now);

  for i in 0..INJECTED_PEERS {
    let octet_hi = (i / 250) as u8;
    let octet_lo = (i % 250) as u8 + 1;
    let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, octet_hi, octet_lo)), 7946);
    a.inject_alive(SmolStr::new(std::format!("peer-{i}")), peer, now);
  }
  let a_members_before = a.num_members();
  assert_eq!(a_members_before, 1 + INJECTED_PEERS);

  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    quiet("b", 2).with_stream_timeout(Duration::from_millis(STREAM_TIMEOUT_MS)),
    &mut db,
    now,
  );
  b.start(now);
  b.join(&[addr(1, 7946)]);

  let expected = 1 + a_members_before;
  let mut converged = false;
  let mut ticks = 0u32;
  for _ in 0..BUDGET {
    let _ = a.poll(clk.now(), &mut da);
    let _ = b.poll(clk.now(), &mut db);
    if b.num_members() >= expected {
      converged = true;
      break;
    }
    ticks += 1;
    clk.advance_ms(TICK_MS);
  }

  assert!(
    converged,
    "B converged to only {} of {} members: the graceful close was force-aborted \
     mid-drain because `close_timeout` ({} ms) was treated as a total-drain cap \
     rather than a no-progress idle bound — even though B read continuously and \
     the drain never stalled.",
    b.num_members(),
    expected,
    CLOSE_TIMEOUT_MS,
  );
  // The drain genuinely spanned far more than `close_timeout` (so a total cap
  // WOULD have fired): the total elapsed exceeds it by a wide margin.
  assert!(
    u64::from(ticks) * TICK_MS > CLOSE_TIMEOUT_MS * 3,
    "the drain finished too fast ({} ticks) to exercise the idle-vs-total \
     distinction; raise the snapshot size or lower close_timeout",
    ticks,
  );
}

/// A second outbound exchange deferred to `PendingDial` behind the only free dial
/// socket must be dialed the instant the FIRST exchange frees that socket — even
/// when the freeing happens LATE in the same `poll` (in the teardown that runs
/// after the machine tick) and the node is driven purely by the deadlines `poll`
/// returns.
///
/// # The bug this guards
///
/// The accept/replenish/dial rebalance runs EARLY in `poll`, before the machine
/// tick. But a socket is also returned to the pool LATE — by `teardown` (a clean
/// `Closed`/`TimeWait` exchange, or an abrupt abort) and by `flush_closing` (a
/// drain-deadline abort) — all of which run AFTER the early rebalance. If the
/// rebalance ran only early, a socket freed by the first exchange completing would
/// sit idle until the NEXT poll, and the returned wakeup would carry no term for
/// "a socket just freed and a dial is waiting". A caller that sleeps exactly to
/// the returned deadline would then sleep until the waiting exchange's OWN bridge
/// deadline — at which point the machine kills it before it ever got the socket.
/// Re-running the rebalance after the late frees services the freed socket
/// in-tick, so the deferred dial is issued immediately and its SYN egress is
/// driven by the (near-now) stack deadline instead.
///
/// # Construction
///
/// The hub node `b` has `tcp_pool_size = 2`: one socket is its listener, leaving
/// exactly ONE dial socket. It joins TWO reachable seeds `a1` and `a2` in a single
/// `join`, producing two `Connect` actions the same tick: the first claims the
/// lone dial socket (`Dialing`), the second finds the pool empty and is deferred
/// to `PendingDial`. Both seeds are real nodes wired to `b` through a broadcast
/// hub (each on the shared `/24`, reachable; the two seeds are NOT wired to each
/// other). With every periodic scheduler disabled, `b` can learn a seed ONLY via
/// its own direct push/pull to it — so reaching all three members proves BOTH
/// exchanges completed, the deferred one included.
///
/// # No masking near-term deadline after the free
///
/// Every periodic scheduler on all three nodes is disabled, so once the first
/// exchange frees `b`'s dial socket the only future deadline `b` returns is the
/// deferred exchange's own bridge kill at `stream_timeout`. That bound is kept far
/// below smoltcp's 10 s TimeWait close timer, so even if the freed socket lingered
/// in `TimeWait`, its timer (10 s) would NOT be the soonest wake — the deferred
/// bridge kill (`stream_timeout`) is. If the late frees were not rebalanced, a
/// deadline-driven caller would therefore advance straight to that kill and strand
/// the deferred dial, with no near-term stack timer to mask the gap by prompting an
/// early re-poll.
#[test]
fn late_freed_socket_services_deferred_dial_under_returned_deadline() {
  // Zero-latency virtual time; the budget only bounds a wedged plane so a
  // regression fails loudly instead of hanging.
  const BUDGET: u32 = 4000;
  // The deferred exchange's bridge elapses at this bound — the only future wake
  // `b` returns once the first exchange frees its socket. Kept well under
  // smoltcp's 10 s TimeWait close timer so that whether the freed socket lands in
  // `Closed` (no timer) or `TimeWait` (a 10 s timer), this 800 ms bridge kill is
  // strictly the soonest future deadline, so no near-term stack timer can mask the
  // stranding by prompting an early re-poll.
  const STREAM_TIMEOUT_MS: u64 = 800;
  // Fallback step for a poll result that is not a future sleep target (`None`, or
  // a past/now "poll again" signal). One millisecond is the clock's finest unit.
  const FALLBACK_STEP_MS: u64 = 1;

  let (mut dn, mut d1, mut d2) = harness::hub(1500);
  let mut clk = harness::Clock::new();
  let now = clk.now();

  // Hub joiner `b`: pool size 2 (listener + ONE dial socket). Its own periodic
  // schedulers are disabled so the only channel that can sync a seed into `b` is
  // `b`'s direct join push/pull to that seed.
  let mut b: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new().with_tcp_pool_size(2),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2))),
    TransformOptions::default(),
    EndpointOptions::new(SmolStr::new("b"), addr(2, 7946))
      .with_rng_seed(2)
      .with_stream_timeout(Duration::from_millis(STREAM_TIMEOUT_MS))
      .with_gossip_interval(Duration::from_secs(3600))
      .with_probe_interval(Duration::from_secs(3600))
      .with_push_pull_interval(Duration::from_secs(3600)),
    &mut dn,
    now,
  );
  b.start(now);

  // Two reachable seeds on the shared /24, all periodic schedulers disabled so the
  // only channel into `b` is `b`'s own direct join push/pull to each seed (and the
  // only sync out of `b` likewise), keeping the contended-socket scenario exact.
  let seed_cfg = |id: &str, ip: u8| {
    EndpointOptions::new(SmolStr::new(id), addr(ip, 7946))
      .with_rng_seed(ip as u64)
      .with_push_pull_interval(Duration::from_secs(3600))
      .with_probe_interval(Duration::from_secs(3600))
      .with_gossip_interval(Duration::from_secs(3600))
  };
  let mut a1: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))),
    TransformOptions::default(),
    seed_cfg("a1", 1),
    &mut d1,
    now,
  );
  a1.start(now);
  let mut a2: Memberlist<SmolStr, _> = Memberlist::new(
    Options::new(),
    harness::ip_iface(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3))),
    TransformOptions::default(),
    seed_cfg("a2", 3),
    &mut d2,
    now,
  );
  a2.start(now);

  // One `join` with BOTH seeds: two `Connect` actions the same tick. The first
  // claims the lone dial socket (`Dialing`); the second finds the pool empty and
  // defers to `PendingDial`. Whichever exchange frees its socket first must then
  // back the still-waiting second dial.
  b.join(&[addr(1, 7946), addr(3, 7946)]);

  // Witness that the second exchange was genuinely deferred at least once: the
  // single dial socket cannot back both at the join tick.
  let mut saw_pending_dial = false;

  // Drive all three nodes purely by the deadlines `poll` returns: advance the
  // clock to the soonest FUTURE instant any node asked to be woken at, never a
  // fixed tick. If a freed dial socket were not re-offered to the waiting dial
  // in-tick, `b`'s only future deadline after its first dial frees would be the
  // deferred exchange's bridge kill — the clock would advance straight to that
  // kill and the second seed would never be learned.
  let mut converged = false;
  for _ in 0..BUDGET {
    let nn = b.poll(clk.now(), &mut dn);
    let n1 = a1.poll(clk.now(), &mut d1);
    let n2 = a2.poll(clk.now(), &mut d2);

    if b.pending_dial_count() >= 1 {
      saw_pending_dial = true;
    }

    // `b` learns a seed only by completing its direct push/pull to it; all three
    // members means BOTH exchanges — the deferred one included — completed.
    if b.num_members() >= 3 {
      converged = true;
      break;
    }

    // Advance the shared clock the way a real multi-node system would: each node
    // independently sleeps to the deadline IT returned, an arriving packet wakes
    // its receiver, and the soonest such wake moves the clock. Concretely the loop
    // re-polls promptly (one clock unit, never a fixed exchange-pacing tick) while
    // EITHER a node returned a past/now instant (smoltcp's "poll again now" — work
    // pending this instant) OR a frame is sitting in some node's rx queue (a peer
    // reply that must wake its receiver); only when every node is idle AND no
    // packet is in flight does the clock sleep to the soonest future timer.
    //
    // Modeling packet arrival as an immediate wake is essential AND honest: it is
    // what a real link does, and it never injects a wake the drivers did not
    // sanction — the minimal step only stands in for "a packet just arrived" or
    // "poll again now", never for the missing socket-freed wake under test. Were
    // the loop to instead jump straight to the soonest returned timer, a reply
    // queued between polls would idle until that far instant, inflating every
    // zero-latency round trip and masking the defect behind transport stalls.
    let now_i = clk.now();
    let packet_in_flight = dn.inbound_pending() || d1.inbound_pending() || d2.inbound_pending();
    let any_immediate = [nn, n1, n2].into_iter().flatten().any(|d| d <= now_i);
    if packet_in_flight || any_immediate {
      clk.advance_ms(FALLBACK_STEP_MS);
    } else {
      match [nn, n1, n2].into_iter().flatten().min() {
        Some(deadline) => clk.advance_to(deadline),
        None => clk.advance_ms(FALLBACK_STEP_MS),
      }
    }
  }

  assert!(
    saw_pending_dial,
    "the second join exchange was never deferred to PendingDial — the single \
     dial socket should not have backed both seeds at the join tick, so the \
     contended-socket scenario was not exercised"
  );
  assert!(
    converged,
    "B learned only {} of 3 members: its second join exchange was stranded in \
     PendingDial when the first exchange freed the lone dial socket LATE in the \
     same poll (after the machine tick), and a deadline-driven caller then slept \
     until that stranded exchange's bridge deadline killed it — the freed socket \
     was never re-offered to the waiting dial in-tick",
    b.num_members(),
  );
}
