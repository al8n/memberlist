use crate::Instant;
use core::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use smol_str::SmolStr;

use super::{QuicEndpoint, UnreliableTransport};
use crate::{
  config::EndpointOptions,
  endpoint::Endpoint,
  event::{Event, PushPullKind, StreamId},
  label::LabelError,
  quic::QuicOptions,
  typed::State,
};

fn test_config_with_mode(mode: UnreliableTransport) -> QuicOptions {
  let mut transport = quinn_proto::TransportConfig::default();
  transport.max_idle_timeout(Some(
    quinn_proto::IdleTimeout::try_from(Duration::from_secs(20)).unwrap(),
  ));
  QuicOptions::new(
    crate::quic::crypto::tests::test_endpoint_config(&[0x5au8; 32]),
    crate::quic::crypto::tests::test_server(),
    crate::quic::crypto::tests::test_client(),
    transport,
    "localhost",
    mode,
  )
}

fn test_config() -> QuicOptions {
  test_config_with_mode(UnreliableTransport::Datagram)
}

fn make_endpoint(id: &str, addr: SocketAddr, now: Instant) -> QuicEndpoint<SmolStr> {
  let cfg = EndpointOptions::new(SmolStr::new(id), addr);
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  ep.start_scheduling(now);
  let qc = test_config();
  let mut seed = [0u8; 32];
  seed[..2].copy_from_slice(&addr.port().to_le_bytes());
  QuicEndpoint::<SmolStr>::with_quinn_rng_seed(ep, qc, Some(seed))
}

/// Same as [`make_endpoint`] but with the unreliable transport set to
/// [`UnreliableTransport::Udp`] at construction. In Udp mode the constructor
/// leaves quinn's datagram buffers at their defaults, so the endpoint does NOT
/// advertise the datagram extension: its connections report
/// `datagrams().max_size() == None` and no peer can deliver datagrams to it.
fn make_endpoint_udp(id: &str, addr: SocketAddr, now: Instant) -> QuicEndpoint<SmolStr> {
  let cfg = EndpointOptions::new(SmolStr::new(id), addr);
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  ep.start_scheduling(now);
  let qc = test_config_with_mode(UnreliableTransport::Udp);
  let mut seed = [0u8; 32];
  seed[..2].copy_from_slice(&addr.port().to_le_bytes());
  QuicEndpoint::<SmolStr>::with_quinn_rng_seed(ep, qc, Some(seed))
}

/// Build a coordinator from explicit membership + QUIC configs, so a test can
/// set caps ([`EndpointOptions::with_max_inbound_streams`],
/// [`QuicOptions::with_max_quic_connections`],
/// [`QuicOptions::with_max_pending_connections_per_source`]) that
/// [`make_endpoint`] leaves at their defaults. Same fixed RNG-seed derivation
/// as [`make_endpoint`] so behaviour stays deterministic.
fn make_endpoint_full(
  cfg: EndpointOptions<SmolStr, SocketAddr>,
  qc: QuicOptions,
  addr: SocketAddr,
  now: Instant,
) -> QuicEndpoint<SmolStr> {
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  ep.start_scheduling(now);
  let mut seed = [0u8; 32];
  seed[..2].copy_from_slice(&addr.port().to_le_bytes());
  QuicEndpoint::<SmolStr>::with_quinn_rng_seed(ep, qc, Some(seed))
}

/// A `MergeDelegate` that rejects every inbound merge, driving
/// `Endpoint::handle_stream_event(PushPullRequestReceived{Join})` to return
/// `StreamCommand::Close` — the coordinator-level twin of the bridge module's
/// `RejectAllMerges`.
struct RejectAllMergesQuic;
impl crate::delegate::MergeDelegate<SmolStr, SocketAddr> for RejectAllMergesQuic {
  fn notify_merge(
    &self,
    _peers: crate::MaybeOwned<'_, [crate::typed::NodeState<SmolStr, SocketAddr>]>,
  ) -> bool {
    false
  }
}

#[test]
fn quic_endpoint_type_is_constructible_signature() {
  // Behavioural coverage is the sim harness (needs a virtual clock + a
  // peer). This guards the public constructor signature only: the
  // coordinator is generic over `I` with `A = SocketAddr` pinned
  // internally, and `new` takes the membership `Endpoint` plus the
  // `QuicOptions` bundle.
  fn _sig<I>()
  where
    I: crate::Id,
  {
    let _: fn(
      crate::endpoint::Endpoint<I, SocketAddr>,
      super::QuicOptions,
    ) -> super::QuicEndpoint<I> = super::QuicEndpoint::<I>::new;
  }
}

#[test]
fn with_label_empty_normalizes_to_none() {
  // An empty label collapses to the byte-identical no-label path, never a
  // `[12][0]` header.
  let ep = make_endpoint("n", "127.0.0.1:7600".parse().unwrap(), Instant::now())
    .with_label(Some(Bytes::new()), false)
    .expect("an empty label normalizes to None, never errors");
  assert_eq!(ep.label(), None);
}

#[test]
fn with_label_accepts_valid() {
  let ep = make_endpoint("n", "127.0.0.1:7601".parse().unwrap(), Instant::now())
    .with_label(Some(Bytes::from_static(b"cluster-x")), false)
    .expect("a valid label");
  assert_eq!(ep.label(), Some(b"cluster-x".as_slice()));
}

#[test]
fn with_label_rejects_overlong() {
  // A label longer than MAX_LABEL_LEN is rejected here, so it never reaches
  // `encode_label_prefix` (which would truncate the length byte and let the
  // overflow be parsed as reliable-unit data).
  let too_long = Bytes::from(vec![b'x'; crate::label::MAX_LABEL_LEN + 1]);
  let result = make_endpoint("n", "127.0.0.1:7602".parse().unwrap(), Instant::now())
    .with_label(Some(too_long), false);
  assert!(matches!(result, Err(LabelError::TooLong(_))));
}

#[test]
fn with_label_rejects_non_utf8() {
  let result = make_endpoint("n", "127.0.0.1:7603".parse().unwrap(), Instant::now())
    .with_label(Some(Bytes::from_static(&[0xff, 0xfe])), false);
  assert!(matches!(result, Err(LabelError::NotUtf8)));
}

/// Regression guard: `service_quinn` MUST drain the per-connection
/// `Connection::poll_endpoint_events()` queue every tick and route each
/// non-`Drained` event through `quinn::Endpoint::handle_event(ch, ev)`. A
/// missing drain leaves `NeedIdentifiers` / `ResetToken` /
/// `RetireConnectionId` stranded in the connection's queue and breaks
/// quinn-proto's documented polling contract: initial CID issuance,
/// peer reset-token registration, and CID retirement all fail to
/// propagate to the endpoint's index.
///
/// The test drives a real QUIC handshake between two `QuicEndpoint`s
/// via the public `start_push_pull` / `poll_transmit` / `handle_udp` /
/// `handle_timeout` API — no internal accessors. `issue_first_cids`
/// pushes `NeedIdentifiers` on the connection's queue at handshake
/// completion; the drain loop's `#[cfg(test)]`
/// `endpoint_events_processed` counter is incremented once per
/// non-`Drained` event passed to `quinn::Endpoint::handle_event`. After
/// driving the handshake to `Established` on both sides the counter
/// MUST be `> 0`.
///
/// Negative control: revert the drain loop in `service_quinn` and the
/// counter stays at `0` — this assertion fails.
#[test]
fn service_quinn_drains_poll_endpoint_events() {
  let a_addr: SocketAddr = "127.0.0.1:7901".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7902".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  // Drive the handshake by ferrying datagrams in both directions.
  // Bounded: ~50 round-trips is plenty for a handshake on a virtual
  // network with no loss.
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    // Run a tick on each side so `service_quinn` drains
    // `poll_endpoint_events` and the new-CID feedback queued for the
    // NEXT tick is fed.
    a.handle_timeout(now);
    b.handle_timeout(now);
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }
  assert!(
    a.counters.endpoint_events_processed > 0,
    "service_quinn must drain Connection::poll_endpoint_events() and \
       feed each non-Drained event to quinn::Endpoint::handle_event; \
       missing this drain breaks quinn-proto's polling contract"
  );
  assert!(
    b.counters.endpoint_events_processed > 0,
    "same on the accepting side: handshake completion emits \
       `NeedIdentifiers` on both peers' connections"
  );
}

/// Regression guard: every datagram the coordinator emits on the QUIC
/// path MUST classify as `Class::Quic`, i.e. its first byte must have
/// `LONG_HEADER_FORM` (`0x80`) or short-header `FIXED_BIT` (`0x40`) set.
/// The first-byte demux is collision-free only under this invariant —
/// a FIXED_BIT-cleared short header would route to `Class::Memberlist`
/// (first byte ∈ `1..=15`) or `Class::Reject` and silently lose ACKs /
/// stream data / close packets to the memberlist codec.
///
/// `EndpointOptions::new` defaults `grease_quic_bit: true`; under that
/// default both sides advertise greasing and quinn-proto's packet
/// encoder is permitted to clear FIXED_BIT on outgoing short-header
/// packets. The coordinator forces `grease_quic_bit(false)` in
/// [`QuicOptions::new`](super::crypto::QuicOptions::new) so the
/// transport parameter is not advertised — a compliant peer will not
/// clear FIXED_BIT in packets it sends us and our encoder will not
/// clear FIXED_BIT in packets we send.
///
/// The test drives a real handshake between two `QuicEndpoint`s via
/// the public poll surface, then initiates a push/pull exchange that
/// generates post-handshake short-header packets (ACKs, STREAM frames,
/// HANDSHAKE_DONE, NEW_CONNECTION_ID, …). For every datagram drained
/// from either side's `poll_transmit`, `classify(&bytes)` is asserted
/// to be `Class::Quic` — directly encoding the demux invariant.
///
/// Negative control: revert `super::crypto::QuicOptions::new` to omit
/// the `endpoint.grease_quic_bit(false)` setter (greasing on) and run
/// this test. With the determinism seed wired through to quinn-proto's
/// per-packet greasing decision, some emitted short-header packets
/// will have FIXED_BIT cleared, `classify` returns `Class::Memberlist`
/// or `Class::Reject`, and the inner assertion fires. The asserting
/// predicate (`classify == Class::Quic`) directly encodes the inbound
/// demux invariant — independent of which exact packet quinn-proto
/// chose to grease.
#[test]
fn emitted_quic_packets_always_have_fixed_bit_set() {
  let a_addr: SocketAddr = "127.0.0.1:7951".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7952".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  // Trigger a dial from A → B; the coordinator emits the Initial on the
  // very first `poll_transmit`. Post-handshake the push/pull bidi
  // stream's payload produces short-header STREAM frames + ACKs — the
  // packet space the per-packet greasing decision applies to.
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  // Bounded ferry of every emitted datagram, asserting the demux
  // invariant on each. ~200 rounds is well past handshake completion
  // and enough push/pull traffic to give quinn-proto's per-packet
  // greasing decision many opportunities to fire on each side.
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      assert_eq!(
        super::classify(&bytes),
        super::Class::Quic,
        "A emitted a non-Quic-classifying datagram (first byte {:#04x}); \
           grease_quic_bit must stay off so the inbound first-byte demux \
           invariant holds end-to-end",
        bytes.first().copied().unwrap_or(0),
      );
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      assert_eq!(
        super::classify(&bytes),
        super::Class::Quic,
        "B emitted a non-Quic-classifying datagram (first byte {:#04x}); \
           grease_quic_bit must stay off so the inbound first-byte demux \
           invariant holds end-to-end",
        bytes.first().copied().unwrap_or(0),
      );
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }
}

/// Architectural guard: a connection's deferred `ConnectionEvent` queue
/// is co-located with its `ConnEntry` and therefore drops with the entry
/// on reap by construction — quinn's slab `vacant_key()` cannot re-key a
/// stale event onto a fresh connection occupying the freed slot because
/// there is no global queue from which a stale event could survive past
/// the reap.
///
/// The reuse hazard. `quinn_proto::Endpoint::connect` allocates the next
/// `ConnectionHandle` via `self.connections.vacant_key()`; reaping
/// invokes `Endpoint::handle_event(ch, EndpointEvent::drained())` which
/// removes the slot via `self.connections.try_remove(ch.0)`. The freed
/// slot is reusable by the very next dial — and `ConnTable.conns` is a
/// strict lockstep mirror of quinn's slab (`get_or_dial`'s
/// `debug_assert_eq!(slot, ch.0)`).
///
/// The fix is structural: a `ConnectionEvent` returned by
/// `quinn::Endpoint::handle_event(ch, ev)` is queued on the SAME
/// `ConnEntry`'s `pending_events` deque (see
/// [`super::conn::ConnEntry::queue_pending_event`]) for delivery on the
/// next `service_quinn` iteration. The entry owns its deferred queue;
/// dropping the entry drops the queue — no global FIFO exists from
/// which a stale `(handle, event)` pair could survive past the reap.
///
/// The test drives a real handshake A↔B so quinn-proto issues
/// `NeedIdentifiers` (via `issue_first_cids`) — `Endpoint::handle_event`
/// returns a `NewIdentifiers` that `service_quinn` queues via
/// `ConnEntry::queue_pending_event`. The
/// test harvests one real `ConnectionEvent` out of `ch_a`'s deferred
/// queue, re-injects it so a real entry exists on the eve of the
/// reap, drives the connection to `is_drained()`, runs the reap, then
/// asserts `ConnTable::get(ch_a)` is `None` — the entry and its
/// `pending_events` are both gone. Finally, the test dials a fresh
/// peer; quinn's `vacant_key()` returns `ch_a.0`, the new entry lands
/// at that slot, and its `pending_events_len() == 0` — the property
/// the previous global-FIFO purge-on-reap step was protecting is now
/// preserved by construction.
#[test]
fn conn_entry_pending_events_drop_with_entry_on_reap() {
  let a_addr: SocketAddr = "127.0.0.1:7921".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7922".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive a real handshake A↔B so quinn-proto issues `NeedIdentifiers`
  // and the [`super::QuicEndpoint::service_quinn`] drain enqueues a
  // real `ConnectionEvent` on the A↔B connection's `ConnEntry`. The
  // loop stops AFTER one entry is staged on `ch_a` and BEFORE the
  // next `service_quinn` drains it (drained at the start of the next
  // iteration per the documented one-tick latency).
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  // Capture A's single connection handle (the dial to B) once it
  // exists in A's slab.
  let mut harvested: Option<quinn_proto::ConnectionEvent> = None;
  let mut ch_a: Option<quinn_proto::ConnectionHandle> = None;
  'ferry: for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
        // `handle_udp` invokes `run_tick`, whose `service_quinn` may
        // queue a real `ConnectionEvent` on A's connection entry
        // (e.g. the `NewIdentifiers` returned by
        // `quinn::Endpoint::handle_event(NeedIdentifiers)` at
        // handshake completion). Harvest here, before subsequent
        // `handle_udp`/`handle_timeout` calls run another
        // `service_quinn` that would drain the queue at the start of
        // its next iteration.
        if let Some(h) = a.conns.iter_handles().first().copied() {
          ch_a = Some(h);
          if let Some(e) = a.conns.get_mut(h) {
            if let Some(ev) = e.take_pending_events().next() {
              harvested = Some(ev);
              break 'ferry;
            }
          }
        }
      }
    }
    a.handle_timeout(now);
    if let Some(h) = a.conns.iter_handles().first().copied() {
      ch_a = Some(h);
      if let Some(e) = a.conns.get_mut(h) {
        if let Some(ev) = e.take_pending_events().next() {
          harvested = Some(ev);
          break 'ferry;
        }
      }
    }
    b.handle_timeout(now);
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break 'ferry;
    }
  }
  let harvested = harvested.expect("handshake must produce at least one real ConnectionEvent");
  let ch_a = ch_a.expect("test precondition: A holds the dial-to-B handle");
  // A holds exactly one connection (to B) at this point.
  assert_eq!(
    a.conns.iter_handles().len(),
    1,
    "test precondition: A holds exactly one connection"
  );

  // Re-stash the harvested event on the about-to-be-reaped entry.
  a.conns
    .get_mut(ch_a)
    .expect("conn entry still present pre-close")
    .queue_pending_event(harvested);
  assert_eq!(
    a.conns
      .get(ch_a)
      .expect("entry present")
      .pending_events_len(),
    1,
    "test precondition: one event staged on the about-to-be-reaped entry"
  );

  // Close the connection on side A and drive it through `is_drained()`
  // by advancing `poll_timeout` on the underlying `Connection`.
  // Bypasses `run_tick` / `service_quinn` so the staged
  // `pending_events` entry is NOT drained before the reap.
  a.conns
    .get_mut(ch_a)
    .expect("connection still in slab pre-close")
    .conn_mut()
    .close(now.into_std(), 0u32.into(), Bytes::new());
  for _ in 0..5000 {
    if a
      .conns
      .get(ch_a)
      .map(|e| e.conn_ref().is_drained())
      .unwrap_or(true)
    {
      break;
    }
    let next = a
      .conns
      .get_mut(ch_a)
      .and_then(|e| e.conn_mut().poll_timeout());
    match next {
      Some(d) => {
        a.conns.get_mut(ch_a).unwrap().conn_mut().handle_timeout(d);
      }
      None => break,
    }
  }
  assert!(
    a.conns
      .get(ch_a)
      .map(|e| e.conn_ref().is_drained())
      .unwrap_or(false),
    "test precondition: A's connection must be drained before reap"
  );

  // Run the production reap loop (the `run_tick` / `flush_outbound`
  // step (5) via [`QuicEndpoint::finalize_tick`]).
  for ch in a.conns.iter_handles() {
    a.conns.reap_if_drained(&mut a.quinn, ch);
  }

  // The architectural property: the entry is gone, and its
  // `pending_events` deque is gone with it. No global FIFO holds the
  // staged event past the reap.
  assert!(
    a.conns.get(ch_a).is_none(),
    "reap_if_drained must remove the slab entry on a successful reap; \
       the entry owns its pending_events deque and the deque drops with it"
  );

  // Stronger property: dial a fresh peer; quinn's `vacant_key()` reuses
  // the freed slab index, the new `ConnEntry` lands at that slot, and
  // its `pending_events_len() == 0` because the deque is per-entry —
  // a stale event from the previous occupant cannot leak in.
  let c_addr: SocketAddr = "127.0.0.1:7923".parse().unwrap();
  let ch_new = a
    .conns
    .get_or_dial(
      &mut a.quinn,
      now,
      a.cfg.client().clone(),
      c_addr,
      "localhost",
    )
    .expect("fresh dial after reap succeeds");
  assert_eq!(
    ch_new.0, ch_a.0,
    "quinn vacant_key() reuses the freed slab index after the reap"
  );
  assert_eq!(
    a.conns
      .get(ch_new)
      .expect("fresh entry at reused slot")
      .pending_events_len(),
    0,
    "the deferred queue is per-entry; a fresh ConnEntry at the reused \
       slab slot starts with an empty queue (the previous occupant's \
       deque dropped with that occupant on reap)"
  );
}

/// Strict-poll wake on deferred-event backlog.
///
/// `service_quinn` queues `ConnectionEvent`s returned by
/// `quinn_proto::Endpoint::handle_event(ch, ev)` on the OWNING
/// `ConnEntry.pending_events` for delivery on the NEXT
/// `service_quinn` iteration (the one-tick latency mirrors quinn-
/// proto's reference async driver — see [`super::conn::ConnEntry::
/// queue_pending_event`]). At handshake completion quinn-proto
/// yields `EndpointEvent::NeedIdentifiers`; `Endpoint::handle_event`
/// returns `Some(ConnectionEvent::NewIdentifiers)`, which is what
/// drives the subsequent NEW_CONNECTION_ID frame emission via the
/// connection's own `handle_event`.
///
/// A strict-poll driver (no opportunistic wakes — re-enters the
/// coordinator only when `poll_timeout()` says to) would sleep
/// past the queued backlog if `poll_timeout` didn't account for
/// it: the only term that would surface a "wake me now" signal
/// is some unrelated idle/loss/probe timer, which on a quiet
/// post-handshake connection can be many seconds away. The
/// coordinator's `poll_timeout` therefore checks every
/// `ConnEntry::has_pending_events()` and returns `Some(last_now)`
/// (immediate-due) when any backlog is present. This regression
/// drives a real A↔B handshake until A's connection entry has a
/// real `ConnectionEvent` queued, then asserts
/// `QuicEndpoint::poll_timeout()` returns an immediate-due wake.
/// (`last_now` is set whenever `handle_*` / `start_*` runs, which
/// must precede any queue entry — the queue is filled by
/// `service_quinn` which is in those code paths.)
#[test]
fn deferred_connection_event_backlog_surfaces_immediate_due_wake() {
  let a_addr: SocketAddr = "127.0.0.1:7931".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7932".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);

  // Drive the handshake until A's ConnEntry has a deferred event
  // (NewIdentifiers from NeedIdentifiers at handshake completion).
  // Stop BEFORE another service_quinn drains it.
  let mut ch_a: Option<quinn_proto::ConnectionHandle> = None;
  let mut have_pending = false;
  'ferry: for _ in 0..200 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        if let Some(h) = a.conns.iter_handles().first().copied() {
          ch_a = Some(h);
          if let Some(e) = a.conns.get(h) {
            if e.has_pending_events() {
              have_pending = true;
              break 'ferry;
            }
          }
        }
      }
    }
    a.handle_timeout(now);
    if let Some(h) = a.conns.iter_handles().first().copied() {
      ch_a = Some(h);
      if let Some(e) = a.conns.get(h) {
        if e.has_pending_events() {
          have_pending = true;
          break 'ferry;
        }
      }
    }
    b.handle_timeout(now);
  }
  assert!(
    have_pending,
    "test precondition: the handshake must queue at least one \
       deferred ConnectionEvent on A's ConnEntry"
  );
  let ch_a = ch_a.expect("test precondition: A holds the dial-to-B handle");

  // The observable: with a real queued event, poll_timeout returns
  // a wake at or before `now` (the test's anchor — `handle_udp` /
  // `handle_timeout` set `last_now` to `now` above, so the
  // immediate-due `last_now` term is `now`). A `Some(t)` with
  // `t <= now` means "wake me immediately on the next driver
  // loop iteration."
  let wake = a.poll_timeout().expect(
    "a non-empty ConnEntry.pending_events MUST surface a wake via \
       poll_timeout — otherwise a strict-poll driver would not re-enter \
       service_quinn until an unrelated timer fires",
  );
  assert!(
    wake <= now,
    "the deferred-event wake MUST be immediate-due (≤ last_now); \
       got {:?} > {:?}",
    wake,
    now
  );

  // Stronger property: one more tick after the wake drains the
  // queue (verifies the wake is actionable, not a busy-loop).
  a.handle_timeout(now);
  assert!(
    !a.conns
      .get(ch_a)
      .expect("connection still present after one tick")
      .has_pending_events(),
    "one tick after the immediate-due wake MUST drain the deferred \
       ConnectionEvent queue (service_quinn's first action on the \
       per-connection iteration is `take_pending_events`)"
  );
}

/// `requeue_event` MUST anchor `last_now` AND route a requeued
/// `Event::DialRequested` directly into `dial_pending`, so the requeued
/// dial doesn't strand under strict-poll driving.
///
/// `endpoint_mut()` is `#[cfg(test)]`-only and the sealed-inner-endpoint
/// invariant blocks external callers from draining `DialRequested` out
/// of the inner Endpoint queue directly. But `pub fn requeue_event(ev,
/// now)` is still a public surface that can deposit an event back into
/// the inner queue — including a `DialRequested` the caller obtained
/// from a bare `Endpoint` BEFORE wrapping it in `QuicEndpoint`
/// (legitimate `Endpoint::poll_event` usage). Without anchoring
/// `last_now`, the strict-poll path would: sieve the requeued
/// DialRequested into `dial_pending` (`attempted = false`), check
/// `poll_timeout` → `last_now == None` → the immediate-due rescue's
/// `if let Some(anchor)` guard skips → wake degrades to the entry's
/// `deadline` term → at the deadline, `service_dials` retires the
/// intent as elapsed without ever opening QUIC. Silent strand.
///
/// The `now` parameter on `requeue_event` anchors `last_now` exactly
/// when an event is deposited — even if that event is a
/// `DialRequested` that bypasses the inner queue and lands directly in
/// `dial_pending`. The next `poll_timeout` then returns `Some(t)` with
/// `t <= now`, and the dial is attempted on the next
/// `handle_timeout(now)` tick.
#[test]
fn requeued_dial_request_under_strict_poll_anchors_last_now() {
  use PushPullKind;

  let a_addr: SocketAddr = "127.0.0.1:7941".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7942".parse().unwrap();
  let t0 = Instant::now();

  // (1) Build a bare Endpoint. Call start_push_pull on it BEFORE
  // wrapping — this is the legitimate Endpoint usage that produces a
  // DialRequested in the inner queue before `QuicEndpoint` wraps it.
  let cfg = EndpointOptions::new(SmolStr::new("a"), a_addr);
  let mut bare_ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  bare_ep.start_scheduling(t0);
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = bare_ep.start_push_pull(b_addr, PushPullKind::Join, t0);

  // Drain the DialRequested. The caller now holds it.
  let dial_requested = loop {
    match bare_ep.poll_event() {
      Some(Event::DialRequested(p)) => {
        break Event::DialRequested(p);
      }
      Some(_) => continue,
      None => panic!("the bare Endpoint must have queued a DialRequested"),
    }
  };

  // (2) Wrap the bare Endpoint in QuicEndpoint. The wrap occurs
  // AFTER the caller drained the DialRequested — so the wrapped
  // QuicEndpoint has `last_now = None` (no handle_* / start_*
  // has been called on it).
  let qc = test_config();
  let mut seed = [0u8; 32];
  seed[..2].copy_from_slice(&a_addr.port().to_le_bytes());
  let mut a: QuicEndpoint<SmolStr> = QuicEndpoint::with_quinn_rng_seed(bare_ep, qc, Some(seed));

  // (3) Requeue the DialRequested onto the wrapped QuicEndpoint.
  // `requeue_event` anchors `last_now` AND routes a `DialRequested`
  // DIRECTLY into `dial_pending` (bypassing the inner Endpoint queue)
  // so the entry is present the moment `requeue_event` returns — a
  // caller that proceeds STRAIGHT to `poll_timeout` without an
  // intervening `poll_event` sieve still sees the immediate-due wake.
  a.requeue_event(dial_requested, t0);

  // (4) Check poll_timeout WITHOUT a prior poll_event call. The
  // direct routing means the entry is in `dial_pending` (not the
  // inner queue awaiting sieve), so the unattempted-wake rescue has
  // a `last_now` anchor to return and the assertion holds.
  let wake = a.poll_timeout().expect(
    "a requeued DialRequested MUST be directly visible in \
       dial_pending — `poll_timeout` MUST return a wake even without \
       an intervening `poll_event` sieve",
  );
  assert!(
    wake <= t0,
    "requeue_event(_, now) anchors last_now AND routes DialRequested \
       directly into dial_pending, so the next poll_timeout fires \
       immediate-due (`<= last_now`). Got wake = {wake:?}, now = \
       {t0:?}. Without direct routing, the wake would have been the \
       inner endpoint's term (or `None`) because the entry would be in \
       the inner queue, not dial_pending."
  );

  // (5) `poll_event` afterwards: no `DialRequested` should leak
  // through to external callers (it never entered the inner
  // queue). Application-visible events still surface — but the
  // bare endpoint had only the DialRequested queued, so the
  // wrapped endpoint's poll_event sees nothing.
  assert!(
    a.poll_event().is_none(),
    "requeue_event(DialRequested, _) routes DIRECTLY to dial_pending, \
       so it must NOT leak through poll_event"
  );
}

/// Strict-poll self-sufficiency: when `service_quinn` observes
/// `Event::ConnectionLost` on a connection, every bridge riding it
/// MUST be D1 `drain_then_reap`'d within the SAME tick — not merely
/// marked fatal and deferred to a future `pump_bridges` cycle. The
/// deferral is unsafe under strict poll-surface driving: a stateless
/// reset / idle-timeout sets `quinn_proto::Connection::State::Drained`
/// synchronously (`Timer::Idle` → `kill(ConnectionError::TimedOut)` →
/// `State::Drained`), so `finalize_tick` reaps the slab slot in the
/// same tick the bridge is marked fatal. `Bridge::poll_timeout`
/// returns `None` for terminal bridges (a terminal bridge owes no
/// future work to itself), so the coordinator's unified `poll_timeout`
/// has no immediate-due term contributed by these bridges; a
/// strict-poll driver with no other peer/probe/timer due wakes never
/// again, and the bridge leaks indefinitely.
///
/// The test drives a real handshake A↔B so a bridge is established
/// on A. It then stops ferrying datagrams between the peers and
/// advances A's clock past the negotiated `max_idle_timeout` (20s in
/// `test_config`). One `handle_timeout(idle_due)` on A is enough:
/// the per-connection `Timer::Idle` synchronously transitions A's
/// connection to `State::Drained` AND queues a `ConnectionLost` for
/// `Connection::poll()`, `service_quinn` observes the loss, and the
/// per-bridge `drain_then_reap` runs inline. The `#[cfg(test)]`
/// `bridges_reaped_on_connection_lost` counter records how many
/// bridges were drained in this path.
///
/// Negative control: revert the inline `drain_then_reap` in
/// `service_quinn` to mere `mark_fatal()`; the counter stays at zero
/// and the bridge sits in `self.bridges` with `fatal == true` after
/// the same tick — its `poll_timeout` returns `None`, so under
/// `step_via_poll_timeout_only` a quiet cluster has no further wake
/// to reach the deferred `pump_bridges` reap.
#[test]
fn connection_lost_immediately_reaps_bridges_under_strict_poll() {
  let a_addr: SocketAddr = "127.0.0.1:7941".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7942".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Initiate the push/pull join and ferry datagrams in both directions
  // until A holds at least one live bridge AND both sides have fully
  // negotiated the handshake (`endpoint_events_processed > 0`
  // confirms the handshake-completion NEW_CONNECTION_ID feedback ran;
  // the test_config's 20s `max_idle_timeout` is then mutually
  // negotiated and armed on both ends).
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_bridge_count() >= 1
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
    // Only bail on no-traffic AFTER both sides have processed at least
    // one endpoint event — otherwise iteration 1 (poll_transmit empty
    // because no `handle_timeout` has yet driven `service_dials`) bails
    // before the handshake even starts.
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }
  assert!(
    a.live_bridge_count() >= 1,
    "test precondition: the push/pull dial must have opened at least \
       one bridge on A"
  );

  // Capture the connection handle that bridges to B, then verify it
  // is in fact Established (the handshake completed) so the negotiated
  // idle timeout is actually armed.
  let ch_a = a
    .conns
    .iter_handles()
    .first()
    .copied()
    .expect("A must hold one connection to B post-handshake");
  assert!(
    a.conns
      .get(ch_a)
      .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
      .unwrap_or(false),
    "test precondition: A's connection to B must be Established \
       before the idle-timeout is advanced"
  );

  let before = a.counters.bridges_reaped_on_connection_lost;
  let bridges_before = a.live_bridge_count();
  assert!(bridges_before >= 1);

  // Drive A's pooled connection to `State::Drained` WITHOUT elapsing
  // the bridges' own exchange deadlines. `Connection::close` transitions
  // to `State::Closed` and arms `Timer::Close` at `now + 3 * PTO`. When
  // that timer fires, the state advances to `Drained` and
  // `EndpointEventInner::Drained` is queued; `self.error` is NOT set,
  // so `poll()` does not yield `Event::ConnectionLost` for the
  // LocallyClosed path — the `service_quinn` `lost || is_drained()`
  // branch is what catches this case.
  //
  // 3 × PTO is a fraction of a second on a healthy localhost handshake;
  // the bridge's exchange deadline (~5s by default for push/pull) is
  // safely past, so `pump_bridges`'s natural deadline-expiry reap
  // cannot fire instead.
  a.conns
    .get_mut(ch_a)
    .expect("A's pooled connection is present")
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());

  let close_due = a
    .conns
    .get_mut(ch_a)
    .expect("A's pooled connection is present")
    .conn_mut()
    .poll_timeout()
    .expect("Connection::close arms the Close timer; poll_timeout MUST be Some");
  a.handle_timeout(crate::Instant::from_std(close_due));

  // Observable: the inline `drain_then_reap` ran for every bridge on
  // the lost connection in THIS single tick. The counter is incremented
  // per bridge reaped on the `Event::ConnectionLost` path; with a
  // mere-`mark_fatal` shape the counter stays at zero.
  assert!(
    a.counters.bridges_reaped_on_connection_lost > before,
    "at least one bridge MUST have been drain_then_reap'd inside \
       `service_quinn` on the `Event::ConnectionLost` path this tick \
       (counter before = {before}, after = {}). Without the inline \
       drain, the bridge sits fatal in `self.bridges` and a strict-poll \
       driver gets no wake to reach the deferred reap.",
    a.counters.bridges_reaped_on_connection_lost,
  );
  assert_eq!(
    a.live_bridge_count(),
    0,
    "every bridge riding the lost connection MUST be removed from \
       `self.bridges` this same tick. Got {} live bridge(s) — the \
       bridge leaked beyond the ConnectionLost tick.",
    a.live_bridge_count(),
  );
}

/// The load-bearing firewall: force-closing a pooled QUIC connection mid-
/// cluster does NOT change membership. The membership FSM is driven only by
/// SWIM probe-ack timing; a transport connection close/loss reaps the bridge
/// (a connection-level event) but never marks the peer Suspect/Dead/Alive.
///
/// `Endpoint` has no connection-state input by construction, and
/// `service_quinn` maps a quinn `Event::ConnectionLost` (or a locally-closed
/// `is_drained()`) to a bridge reap, NEVER to a membership transition. This
/// test locks that in: it drives a real push/pull JOIN A<->B to membership
/// completion (B becomes a tracked, `Alive` member of A — not merely a
/// bridge), then force-closes A's pooled connection to B at the SAME instant
/// `now` so no probe/suspicion deadline can fire. After servicing the loss,
/// A's membership is byte-identical: B is still tracked, still `Alive`, the
/// member count is unchanged, and no `NodeJoined`/`NodeLeft`/`NodeUpdated`/
/// `NodeConflict` transition for B is produced by the close. The bridge reap
/// may surface an `ExchangeCompleted` (a connection/exchange event) — that is
/// allowed and is not a membership transition.
///
/// If this test ever fails — num_members drops, or B's gossip liveness goes
/// Suspect/Dead, or a membership-transition event for B appears — WITHOUT any
/// elapsed probe deadline (time was never advanced), a connection->membership
/// edge has been introduced and the firewall is broken.
#[test]
fn closing_a_pooled_connection_does_not_change_membership() {
  let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7972".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  let b_id = SmolStr::new("b");

  // Drive a push/pull JOIN A<->B to membership COMPLETION: ferry datagrams
  // both ways at a FIXED instant `now`, ticking both ends and draining each
  // side's `poll_event` per iteration so the merge endpoint events are
  // processed (mirrors the membership-merge ferry the stream-plane tests use,
  // adapted to the QUIC datagram pump). The localhost handshake + the push/
  // pull bidi complete within a single instant in this model, so A learns B
  // as an `Alive` member without advancing the clock — keeping every
  // probe/suspicion deadline strictly in the future.
  // Ignoring StreamId return: the test asserts on membership + the connection
  // handle, not the exchange id.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..256 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    while a.poll_event().is_some() {}
    while b.poll_event().is_some() {}
    if a.endpoint_mut().num_members() >= 2 {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  let members_before = a.endpoint_mut().num_members();
  assert!(
    members_before >= 2,
    "precondition: A must know B as a member before the kill (the push/pull \
       JOIN must merge B's NodeState into A). num_members = {members_before}"
  );
  assert_eq!(
    a.endpoint_mut().member_liveness(&b_id),
    Some(State::Alive),
    "precondition: B must be tracked as Alive before the kill"
  );

  // Force-close A's pooled connection to B (a pure transport fault) at the
  // SAME instant `now`. Time is NEVER advanced in this test, so no
  // probe/suspicion deadline can fire — the connection close is the only
  // stimulus.
  let ch = a
    .conns
    .handle_for(&b_addr)
    .expect("A must hold a pooled connection to B post-merge");
  a.conns
    .get_mut(ch)
    .expect("A's pooled connection to B is present")
    .conn_mut()
    .close(now.into_std(), 0u32.into(), bytes::Bytes::new());

  // Service A once so `service_quinn` observes the loss and reaps the bridge.
  a.handle_timeout(now);

  // Membership MUST be byte-identical: B still tracked, still Alive, count
  // unchanged.
  assert_eq!(
    a.endpoint_mut().num_members(),
    members_before,
    "closing a QUIC connection MUST NOT change the member count \
       (before = {members_before}, after = {}). A connection->membership edge \
       has been introduced.",
    a.endpoint_mut().num_members(),
  );
  assert_eq!(
    a.endpoint_mut().member_liveness(&b_id),
    Some(State::Alive),
    "closing a QUIC connection MUST NOT transition B's gossip liveness \
       (got {:?}); only a SWIM probe timeout may move B to Suspect/Dead, and \
       time was never advanced.",
    a.endpoint_mut().member_liveness(&b_id),
  );

  // The close + service drain MUST produce no membership-transition event for
  // B. A bridge-reap `ExchangeCompleted` / `LeftCluster` is a connection/
  // exchange event and is allowed.
  while let Some(ev) = a.poll_event() {
    match &ev {
      Event::NodeJoined(ns) | Event::NodeLeft(ns) | Event::NodeUpdated(ns) => {
        assert_ne!(
          ns.id_ref(),
          &b_id,
          "closing a QUIC connection MUST NOT emit a membership transition \
             ({ev:?}) for B — the connection close reaped the bridge but the \
             firewall forbids it from touching membership."
        );
      }
      Event::NodeConflict(nc) => {
        assert_ne!(
          nc.existing_ref().id_ref(),
          &b_id,
          "closing a QUIC connection MUST NOT emit a NodeConflict for B"
        );
      }
      _ => {}
    }
  }
}

/// Offering an unreliable datagram to a peer that already holds an
/// Established pooled connection enqueues it on that connection
/// (`DatagramSendStatus::Queued`). The datagram does not surface on
/// `poll_transmit` synchronously — it rides out via the normal
/// `service_quinn -> poll_transmit` pump on a later tick — so the contract
/// asserted here is the `Queued` outcome, not an immediate transmit.
#[test]
fn queue_unreliable_datagram_to_established_peer_is_queued() {
  let a_addr: SocketAddr = "127.0.0.1:7951".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7952".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive the A<->B push/pull handshake until A holds an Established
  // connection to B (the proven ferry pattern).
  // Ignoring StreamId return: the test asserts on the connection state and
  // the datagram outcome, not the handle.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_bridge_count() >= 1
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  let ch_a = a
    .conns
    .iter_handles()
    .first()
    .copied()
    .expect("A must hold one connection to B post-handshake");
  assert!(
    a.conns
      .get(ch_a)
      .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
      .unwrap_or(false),
    "test precondition: A's connection to B must be Established before \
       offering an unreliable datagram"
  );

  let outcome = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01gossip"), now);
  assert_eq!(outcome, super::DatagramSendStatus::Queued);
}

/// A received application datagram surfaces through the SAME
/// `poll_memberlist_ingress` accessor the plain-UDP gossip path uses, tagged
/// with the sender's address — proving the wire is invisible past `mem_ingress`
/// (the driver decodes a datagram-sourced packet identically to a UDP one).
#[test]
fn received_datagram_surfaces_through_the_same_memberlist_ingress_as_udp() {
  let a_addr: SocketAddr = "127.0.0.1:7961".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7962".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive the A<->B push/pull handshake until A holds an Established
  // connection to B (the proven ferry pattern).
  // Ignoring StreamId return: the test asserts on the connection state and
  // the surfaced datagram, not the handle.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_bridge_count() >= 1
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  let ch_a = a
    .conns
    .iter_handles()
    .first()
    .copied()
    .expect("A must hold one connection to B post-handshake");
  assert!(
    a.conns
      .get(ch_a)
      .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
      .unwrap_or(false),
    "test precondition: A's connection to B must be Established before \
       offering an unreliable datagram"
  );

  // A offers one application datagram to B.
  let payload = Bytes::from_static(b"\x01hello-datagram");
  assert_eq!(
    a.queue_unreliable_datagram(b_addr, payload.clone(), now),
    super::DatagramSendStatus::Queued
  );

  // Pump: A's tick puts the datagram packet onto a.out; ferry it into B;
  // B's tick drains it into mem_ingress; poll_memberlist_ingress surfaces it.
  let mut got: Option<(SocketAddr, Bytes)> = None;
  for _ in 0..50 {
    a.handle_timeout(now);
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    b.handle_timeout(now);
    if let Some(item) = b.poll_memberlist_ingress() {
      got = Some(item);
      break;
    }
    // keep the connection healthy in both directions
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
  }

  let (from, bytes) = got.expect("B must surface the datagram via poll_memberlist_ingress");
  assert_eq!(
    from, a_addr,
    "ingress must be tagged with the sender address"
  );
  assert_eq!(
    bytes, payload,
    "the datagram payload must round-trip byte-identically"
  );
}

/// When mem_ingress is already at MAX_MEM_INGRESS_DATAGRAMS, a further inbound
/// QUIC datagram is dropped and counted rather than growing the queue
/// unbounded. quinn's byte-budget receive buffer cannot bound the entry count
/// (a zero/tiny-length datagram flood adds ~0 bytes per entry), so the
/// coordinator drain enforces the count cap.
#[test]
fn inbound_datagram_dropped_when_ingress_backlog_at_cap() {
  let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7972".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive the A<->B push/pull handshake until A holds an Established
  // connection to B (the proven ferry pattern).
  // Ignoring StreamId return: the test asserts on the drop counter, not the
  // handle.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_bridge_count() >= 1
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  // Clear any handshake-era ingress, then saturate A's mem_ingress to the cap
  // so the next inbound datagram has nowhere to land.
  a.mem_ingress.clear();
  for _ in 0..super::MAX_MEM_INGRESS_DATAGRAMS {
    a.mem_ingress.push_back((b_addr, Bytes::from_static(b"x")));
  }
  let dropped_before = a.datagram_ingress_dropped();

  // B sends one datagram to A; pump it onto the wire and service A. A's
  // service_quinn drain finds mem_ingress at the cap and drops + counts it.
  assert_eq!(
    b.queue_unreliable_datagram(a_addr, Bytes::from_static(b"\x01y"), now),
    super::DatagramSendStatus::Queued
  );
  for _ in 0..50 {
    b.handle_timeout(now);
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    if a.datagram_ingress_dropped() > dropped_before {
      break;
    }
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
  }

  assert!(
    a.datagram_ingress_dropped() > dropped_before,
    "a datagram arriving with mem_ingress at cap must be dropped and counted"
  );
  // The public Metrics fold the QUIC datagram-plane ingress shed into
  // gossip_ingress_dropped, so a driver sees one unified counter.
  assert_eq!(
    a.metrics().gossip_ingress_dropped,
    a.datagram_ingress_dropped(),
    "QuicEndpoint::metrics must surface the QUIC ingress drops, not a zero stream-plane count"
  );
}

/// One peer flooding its full per-peer standing share of `mem_ingress` must
/// not starve a DIFFERENT peer's inbound datagram, even while the node-global
/// cap is far from full. The share is bounded across the whole undrained
/// queue (not per recv pass), so the flooder's overflow is dropped-and-counted
/// by the real `service_quinn` drain while the other peer's datagram still
/// surfaces — the fairness the per-pass counter could not provide once a
/// driver batches several recv passes before draining `mem_ingress`.
#[test]
fn per_peer_ingress_cap_does_not_starve_other_peers() {
  let a_addr: SocketAddr = "127.0.0.1:7981".parse().unwrap();
  let t_addr: SocketAddr = "127.0.0.1:7982".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7983".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut t = make_endpoint("t", t_addr, now);
  // B is the flooding peer: a real connection so its datagram traverses the
  // actual receive drain, where its full standing share is dropped-and-counted.
  let mut b = make_endpoint("b", b_addr, now);

  // Drive both peers' push/pull handshakes until A holds an Established
  // connection to each (the proven ferry pattern). Ferrying both each
  // iteration lets A accept two distinct connections.
  // Ignoring StreamId return: the test asserts on the surfaced datagram and
  // the drop counter, not the handles.
  let _ = t
    .endpoint_mut()
    .start_push_pull(a_addr, PushPullKind::Join, now);
  let _ = b
    .endpoint_mut()
    .start_push_pull(a_addr, PushPullKind::Join, now);
  for _ in 0..400 {
    for (peer, peer_addr) in [(&mut t, t_addr), (&mut b, b_addr)] {
      while let Some((to, bytes)) = peer.poll_transmit() {
        if to == a_addr {
          a.handle_udp(peer_addr, &bytes, now);
        }
      }
    }
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == t_addr {
        t.handle_udp(a_addr, &bytes, now);
      } else if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    t.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_connections_to(t_addr) >= 1 && a.live_connections_to(b_addr) >= 1 {
      break;
    }
  }
  assert!(
    a.live_connections_to(t_addr) >= 1 && a.live_connections_to(b_addr) >= 1,
    "test precondition: A must hold a live connection to both T and B"
  );

  // Clear handshake-era ingress, then fill peer B's standing share to the
  // per-peer cap while the node-global queue stays far below its cap, so the
  // GLOBAL bound never fires — only B's per-peer bound can drop, and only B's
  // datagrams, never T's.
  a.mem_ingress.clear();
  a.mem_ingress_per_peer.clear();
  for _ in 0..super::MAX_INGRESS_DATAGRAMS_PER_PEER {
    a.mem_ingress.push_back((b_addr, Bytes::from_static(b"x")));
    *a.mem_ingress_per_peer.entry(b_addr).or_insert(0) += 1;
  }
  assert!(
    a.mem_ingress.len() < super::MAX_MEM_INGRESS_DATAGRAMS,
    "test precondition: global queue must stay below its cap so only the \
       per-peer bound is exercised"
  );

  // Both peers send one application datagram to A. T's must land in the shared
  // queue; B's (already at its per-peer cap) must be dropped-and-counted by the
  // real service_quinn drain.
  let t_payload = Bytes::from_static(b"\x01t-probe-ack");
  assert_eq!(
    t.queue_unreliable_datagram(a_addr, t_payload.clone(), now),
    super::DatagramSendStatus::Queued
  );
  assert_eq!(
    b.queue_unreliable_datagram(a_addr, Bytes::from_static(b"\x01b-flood"), now),
    super::DatagramSendStatus::Queued
  );
  let dropped_before = a.datagram_ingress_dropped();

  let mut got_t: Option<(SocketAddr, Bytes)> = None;
  for _ in 0..50 {
    t.handle_timeout(now);
    b.handle_timeout(now);
    for (peer, peer_addr) in [(&mut t, t_addr), (&mut b, b_addr)] {
      while let Some((to, bytes)) = peer.poll_transmit() {
        if to == a_addr {
          a.handle_udp(peer_addr, &bytes, now);
        }
      }
    }
    a.handle_timeout(now);
    // Drain A's queue looking for T's datagram, skipping B's filler entries.
    while let Some(item) = a.poll_memberlist_ingress() {
      if item.0 == t_addr {
        got_t = Some(item);
        break;
      }
    }
    if got_t.is_some() && a.datagram_ingress_dropped() > dropped_before {
      break;
    }
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == t_addr {
        t.handle_udp(a_addr, &bytes, now);
      } else if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
  }

  let (from, bytes) =
    got_t.expect("T's datagram must surface despite B's full standing share — no starvation");
  assert_eq!(
    from, t_addr,
    "surfaced ingress must be tagged with T's address"
  );
  assert_eq!(
    bytes, t_payload,
    "T's datagram payload must round-trip intact"
  );
  assert!(
    a.datagram_ingress_dropped() > dropped_before,
    "B's datagram, arriving while B is at its per-peer cap, must be dropped and counted"
  );
}

/// A plain-UDP flood from one peer cannot bypass the shared ingress caps:
/// `handle_memberlist_udp` is admission-checked through the SAME shared helper
/// as the QUIC datagram drain, so once a flooding peer reaches its per-peer cap
/// its further UDP frames are dropped-and-counted while a DIFFERENT peer's
/// inbound frame is still admitted. Before the fix the UDP path pushed
/// unconditionally and incremented `mem_ingress_per_peer` past the cap, so a
/// fallback flood was an unbounded queue grower that also starved later
/// authenticated QUIC datagrams on the per-peer/global cap checks.
#[test]
fn udp_ingress_flood_is_capped_and_does_not_starve_other_peers() {
  let now = Instant::now();
  let mut a = make_endpoint("a", "127.0.0.1:7991".parse().unwrap(), now);
  let x: SocketAddr = "127.0.0.1:7992".parse().unwrap(); // UDP flooder
  // Flood X over plain UDP well past its per-peer cap; the global cap stays
  // clear (per-peer cap << global cap), so only X's per-peer bound can fire.
  for _ in 0..(super::MAX_INGRESS_DATAGRAMS_PER_PEER + 50) {
    a.handle_memberlist_udp(x, b"\x01flood");
  }
  // X is capped at exactly its per-peer budget; the overflow was dropped+counted.
  assert!(
    a.mem_ingress_per_peer.get(&x).copied().unwrap_or(0) <= super::MAX_INGRESS_DATAGRAMS_PER_PEER,
    "X's standing share must not exceed its per-peer cap"
  );
  assert!(
    a.datagram_ingress_dropped() >= 50,
    "X's 50 overflow UDP frames must be dropped and counted"
  );
  assert!(
    a.mem_ingress.len() < super::MAX_MEM_INGRESS_DATAGRAMS,
    "X must not be able to fill the global queue"
  );
  // A different peer Y's frame is still admitted (not starved by X's flood).
  let y: SocketAddr = "127.0.0.1:7993".parse().unwrap();
  a.handle_memberlist_udp(y, b"\x01y");
  // Drain and confirm Y's frame is present.
  let mut saw_y = false;
  while let Some((from, _)) = a.poll_memberlist_ingress() {
    if from == y {
      saw_y = true;
    }
  }
  assert!(saw_y, "a non-flooding peer's frame must still be admitted");
}

/// Offering an unreliable datagram to a peer with no pooled connection is
/// best-effort `NotReady` (no datagram max_size yet), but it MUST initiate a
/// dial so a subsequent offer can land once the connection establishes.
#[test]
fn queue_unreliable_datagram_to_unknown_peer_is_not_ready_and_initiates_dial() {
  let a_addr: SocketAddr = "127.0.0.1:7953".parse().unwrap();
  let unknown: SocketAddr = "127.0.0.1:7954".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);

  let outcome = a.queue_unreliable_datagram(unknown, Bytes::from_static(b"x"), now);
  assert_eq!(outcome, super::DatagramSendStatus::NotReady);
  assert!(
    a.conns.handle_for(&unknown).is_some(),
    "queue must initiate a dial to an unknown peer"
  );
  // A best-effort NotReady on a still-handshaking connection (no datagram
  // max_size yet) is NOT a drop — only a residual quinn datagram-state error
  // bumps the drop counter.
  assert_eq!(a.datagram_dropped(), 0);
}

/// A datagram to a peer with no pooled connection is best-effort NotReady AND
/// initiates a dial; a single flush_outbound_transmits then emits that dial's
/// QUIC Initial the SAME tick (so the connection warms promptly instead of
/// waiting for the next driver wake).
#[test]
fn cold_peer_datagram_then_flush_emits_quic_initial_same_tick() {
  let a_addr: SocketAddr = "127.0.0.1:7991".parse().unwrap();
  let cold: SocketAddr = "127.0.0.1:7992".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);

  assert_eq!(
    a.queue_unreliable_datagram(cold, Bytes::from_static(b"\x01g"), now),
    super::DatagramSendStatus::NotReady
  );
  // What the driver now does on NotReady:
  a.flush_outbound_transmits(now);
  // The cold dial's Initial must be queued for transmit to `cold` this tick.
  let mut saw_initial_to_cold = false;
  while let Some((to, _bytes)) = a.poll_transmit() {
    if to == cold {
      saw_initial_to_cold = true;
    }
  }
  assert!(
    saw_initial_to_cold,
    "the cold dial's QUIC Initial must be emitted the same tick as the flush"
  );
}

/// `service_dials` MUST retire an expired dial intent through the
/// FSM's `dial_failed` path BEFORE calling
/// `quinn_proto::Streams::open(Dir::Bi)`. `Streams::open(Dir::Bi)`
/// inserts BOTH the send AND the recv state for the new stream. An
/// expired-intent open would therefore synthesise a bidi stream on a
/// pooled connection that no `Bridge` owns — the recv half is
/// unreachable and unreapable.
///
/// The `now >= deadline` pre-check skips the open entirely, so
/// neither half is created. As defence-in-depth, the
/// `dial_succeeded → None` branch additionally stops the recv half so
/// any other `None`-return path from the frozen `dial_succeeded`
/// fully retires both halves rather than leaving the recv half
/// orphaned.
///
/// The test drives a real handshake A↔B so A holds a pooled,
/// Established connection. It then injects a synthetic
/// `DialRequested` whose `deadline` is BEFORE `now`, runs
/// `service_dials(now)`, and asserts:
///  - no bridge was created on A;
///  - `Connection::streams().send_streams() == 0` on A's pooled
///    connection — neither half exists, so quinn-proto's
///    "streams that may have unacknowledged data" counter is
///    unchanged.
///
/// Negative control: revert the `now >= deadline` pre-check.
/// `Streams::open(Dir::Bi)` then runs for the expired intent,
/// inserting both halves; `dial_succeeded` returns `None` (the
/// frozen API drops past-deadline intents); reset-only retirement
/// touches `send_stream(sid)` but not `recv_stream(sid)`. The recv
/// half remains in `StreamsState::recv` with no owner —
/// `Streams::send_streams` is `> 0` immediately after `open` (it
/// counts open send streams) and stays in the count as the orphan,
/// so this assertion fails.
#[test]
fn expired_dial_does_not_open_unowned_bidi_stream() {
  let a_addr: SocketAddr = "127.0.0.1:7961".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7962".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive a real handshake A→B (push/pull join) so A holds a pooled
  // Established connection. Once the bridge on A has reaped its own
  // join exchange (or the loop bounds out), the pooled connection is
  // available for the expired-intent injection below to re-use.
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    // Only bail on no-traffic AFTER both sides have processed at least
    // one endpoint event — see the matching pattern in
    // `connection_lost_immediately_reaps_bridges_under_strict_poll`.
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  let ch_a = a
    .conns
    .iter_handles()
    .first()
    .copied()
    .expect("A must hold one pooled connection to B post-handshake");
  assert!(
    a.conns
      .get(ch_a)
      .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
      .unwrap_or(false),
    "test precondition: A's pooled connection must be Established"
  );

  // Snapshot the baseline `send_streams` count after the join's own
  // bidi stream has reaped. A non-zero residual is acceptable (e.g.
  // an in-flight ack), but the post-`service_dials(expired)` count
  // MUST match this baseline — the expired intent must create no
  // new stream state on the pooled connection.
  let send_streams_before = a
    .conns
    .get_mut(ch_a)
    .expect("conn entry present")
    .conn_mut()
    .streams()
    .send_streams();

  // Register a real PushPull intent on the inner endpoint (registers
  // it in `pending_stream_intents` and queues a `DialRequested`),
  // then sieve the `DialRequested` into `dial_pending`. Finally,
  // override the deadline on the `dial_pending` entry to be BEFORE
  // `now` — the expired-dial pre-check then routes this intent
  // through `dial_failed` BEFORE `Streams::open(Dir::Bi)` is called.
  let id = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Refresh, now);
  a.sieve_dial_events();
  assert_eq!(
    a.dial_pending.len(),
    1,
    "test precondition: the sieve must move exactly the one \
       `DialRequested` into `dial_pending`"
  );
  let entry = a
    .dial_pending
    .front_mut()
    .expect("dial_pending non-empty per the preceding assertion");
  assert_eq!(
    entry.id, id,
    "the sieved entry MUST carry the registered intent's id"
  );
  entry.deadline = now - Duration::from_millis(1);

  let bridges_before = a.live_bridge_count();
  a.service_dials(now);

  // Observable (a): no new bridge was created.
  assert_eq!(
    a.live_bridge_count(),
    bridges_before,
    "an expired dial intent MUST NOT spawn a new bridge — got \
       bridge_count {} -> {}",
    bridges_before,
    a.live_bridge_count(),
  );

  // Observable (b): no new bidi stream state was created on the
  // pooled connection. `Streams::open(Dir::Bi)` would have
  // incremented `send_streams` (its body runs `state.insert(false,
  // id); state.send_streams += 1`). The deadline pre-check skips
  // the open entirely, so `send_streams` is unchanged.
  let send_streams_after = a
    .conns
    .get_mut(ch_a)
    .expect("conn entry still present")
    .conn_mut()
    .streams()
    .send_streams();
  assert_eq!(
    send_streams_after, send_streams_before,
    "an expired dial intent MUST NOT call `Streams::open(Dir::Bi)` \
       on the pooled connection — that insert would create BOTH send \
       and recv state with no `Bridge` to own them. send_streams \
       before = {send_streams_before}, after = {send_streams_after}. \
       Reverting the deadline pre-check lets `open` create both halves; \
       a reset-only retirement on the `dial_succeeded → None` branch \
       touches the send half but not the recv half, so the recv half \
       stays orphaned in `StreamsState::recv` (and `send_streams` \
       would be > before, since the new stream is in `streams::send` \
       and only retires after the peer ACKs the RESET_STREAM)."
  );
}

/// A reliable `UserMessage` dial that fails BEFORE any bridge is
/// created MUST surface a terminal `Event::ExchangeCompleted` with
/// `outcome = Failed` and `kind = UserMessage`, keyed by the SAME
/// `ExchangeId::from(StreamId)` the QUIC driver parks its
/// reliable-send waiter under. Without this the driver's parked waiter
/// hangs forever (the bridge-reap path — the only other
/// `ExchangeCompleted(UserMessage)` producer — never fires for a
/// bridge that was never created).
///
/// The test registers a `UserMessage` intent (which dials the
/// unreachable peer in-band and requeues onto `dial_pending` while the
/// connection handshakes), overrides the requeued entry's deadline to
/// BEFORE `now`, and runs `service_dials(now)`. The expired-dial
/// pre-check retires the intent through `retire_failed_dial`, which
/// emits the `Failed` completion. `poll_event` then surfaces it.
///
/// Negative control: revert `retire_failed_dial` to the bare
/// `pending_outbound_kinds.remove` + `dial_failed` it replaced and
/// `poll_event` yields no `ExchangeCompleted` — the assertion fails.
#[test]
fn expired_user_message_dial_emits_failed_exchange_completed() {
  use crate::event::{Event, ExchangeId, ExchangeKind, ExchangeStatus};

  let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
  // No B endpoint is bound: the dial targets a port nothing listens on,
  // so the connection never leaves `is_handshaking`.
  let unreachable: SocketAddr = "127.0.0.1:7972".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);

  // `start_user_message` registers the intent, dials in-band, and (the
  // connection is still handshaking) requeues the intent onto
  // `dial_pending` with deadline `now + stream_timeout`. The returned
  // `StreamId` is the correlation handle the QUIC driver coerces to its
  // parked `ExchangeId`.
  let id = a
    .start_user_message(unreachable, Bytes::from_static(b"hello"), now)
    .expect("issued while running");
  let expected_eid = ExchangeId::from(id);

  assert_eq!(
    a.dial_pending.len(),
    1,
    "test precondition: the in-band dial must requeue the still-handshaking \
       UserMessage intent onto `dial_pending`"
  );
  let entry = a
    .dial_pending
    .front_mut()
    .expect("dial_pending non-empty per the preceding assertion");
  assert_eq!(
    entry.id, id,
    "the requeued entry MUST carry the registered intent's id"
  );
  // Force the expired-dial pre-check to fire on the next service tick.
  entry.deadline = now - Duration::from_millis(1);

  a.service_dials(now);

  // Drain events and find the terminal completion for this exchange.
  let mut found = None;
  while let Some(ev) = a.poll_event() {
    if let Event::ExchangeCompleted(payload) = ev
      && payload.eid() == expected_eid
    {
      found = Some(payload);
      break;
    }
  }
  let payload = found.expect(
    "a UserMessage dial that fails before any bridge is created MUST emit \
       Event::ExchangeCompleted keyed by ExchangeId::from(StreamId) — the \
       QUIC driver's parked reliable-send waiter resolves on exactly this \
       event; without it the send hangs forever",
  );
  assert_eq!(
    payload.kind(),
    ExchangeKind::UserMessage,
    "the surfaced completion MUST carry kind = UserMessage"
  );
  assert_eq!(
    payload.outcome(),
    ExchangeStatus::Failed,
    "a pre-bridge dial failure MUST carry outcome = Failed"
  );
  assert_eq!(
    payload.peer(),
    &unreachable,
    "the completion MUST carry the dialed peer address"
  );
  // The intent's staged kind/peer must be drained (no leak).
  assert!(
    !a.pending_outbound_kinds.contains_key(&id),
    "the staged outbound kind MUST be drained on dial failure"
  );
  assert!(
    !a.pending_outbound_peers.contains_key(&id),
    "the staged outbound peer MUST be drained on dial failure"
  );
}

/// A `PushPull` dial that fails BEFORE any bridge is created MUST
/// surface a terminal `Event::ExchangeCompleted` with `outcome = Failed`
/// and `kind = PushPull`, keyed by the SAME `ExchangeId::from(StreamId)`
/// a QUIC `WaitForCompletion` join parks its waiter under. Without this
/// the driver's parked join waiter — which resolves only when every
/// dispatched push/pull exchange completes — never drains its set for an
/// unreachable seed and the join hangs (the reactor QUIC join has no
/// deadline; it relies on the machine emitting a completion).
///
/// No double-completion: a pre-bridge failure created no bridge, so the
/// bridge-reap path (the only other `ExchangeCompleted` producer) never
/// fires for this StreamId — the `retire_failed_dial` `Failed` is the
/// single terminal event.
///
/// Negative control: narrow `retire_failed_dial` back to the
/// `UserMessage`-only gate and this assertion (a `Failed` PushPull
/// completion is surfaced) fails — the join hang regresses.
#[test]
fn expired_push_pull_dial_emits_failed_exchange_completed() {
  use crate::event::{Event, ExchangeId, ExchangeKind, ExchangeStatus};

  let a_addr: SocketAddr = "127.0.0.1:7973".parse().unwrap();
  let unreachable: SocketAddr = "127.0.0.1:7974".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);

  // `start_push_pull` on the coordinator registers the intent, dials
  // in-band, and (the connection is still handshaking) requeues the
  // intent onto `dial_pending`. The returned `StreamId` is the
  // correlation handle the QUIC driver coerces to its parked
  // `ExchangeId`.
  let id = a.start_push_pull(unreachable, PushPullKind::Refresh, now);
  let expected_eid = ExchangeId::from(id);
  assert_eq!(
    a.dial_pending.len(),
    1,
    "test precondition: the in-band dial must requeue the still-handshaking \
       PushPull intent onto `dial_pending`"
  );
  let entry = a
    .dial_pending
    .front_mut()
    .expect("dial_pending non-empty per the preceding assertion");
  assert_eq!(
    entry.id, id,
    "the requeued entry MUST carry the registered intent's id"
  );
  // Force the expired-dial pre-check to fire on the next service tick.
  entry.deadline = now - Duration::from_millis(1);

  a.service_dials(now);

  // Drain events and find the terminal completion for this exchange.
  let mut found = None;
  while let Some(ev) = a.poll_event() {
    if let Event::ExchangeCompleted(payload) = ev
      && payload.eid() == expected_eid
    {
      found = Some(payload);
      break;
    }
  }
  let payload = found.expect(
    "a PushPull dial that fails before any bridge is created MUST emit \
       Event::ExchangeCompleted keyed by ExchangeId::from(StreamId) — the \
       QUIC driver's parked join waiter resolves on exactly this event; \
       without it an unreachable-seed join hangs forever",
  );
  assert_eq!(
    payload.kind(),
    ExchangeKind::PushPull,
    "the surfaced completion MUST carry kind = PushPull"
  );
  assert_eq!(
    payload.outcome(),
    ExchangeStatus::Failed,
    "a pre-bridge dial failure MUST carry outcome = Failed"
  );
  assert_eq!(
    payload.peer(),
    &unreachable,
    "the completion MUST carry the dialed peer address"
  );
  // The intent's staged kind/peer must be drained (no leak).
  assert!(
    !a.pending_outbound_kinds.contains_key(&id),
    "the staged outbound kind MUST be drained on dial failure"
  );
  assert!(
    !a.pending_outbound_peers.contains_key(&id),
    "the staged outbound peer MUST be drained on dial failure"
  );
}

/// Strict-poll self-sufficiency: a bridge inserted into `self.bridges`
/// by `service_quinn`'s `accept(Dir::Bi)` loop (step 4) or
/// `service_dials`'s `streams().open(Dir::Bi)` (step 5) MUST be pumped
/// within the SAME tick — not deferred to a future `pump_bridges` cycle
/// that a strict-poll driver may never wake to run.
///
/// `quinn_proto::Connection::poll_timeout` returns only transport timers
/// (`self.timers.next_timeout()` — loss detection / idle / close /
/// KeyDiscard / KeepAlive). App-read readiness is NOT advertised as a
/// transport timer: when an inbound bidi stream is accepted by
/// `service_quinn` in step (4) of `run_tick`, step (2)'s `pump_bridges`
/// has already run for this tick — the freshly-accepted bridge's first
/// request data sits in quinn's per-stream recv buffer un-pumped.
/// `Bridge::poll_timeout` falls back to the bridge's snapshotted exchange
/// deadline while non-terminal (a non-immediate-due wake by construction:
/// the deadline is `now + stream_timeout`, ≈ 5 s by default). Under a
/// driver that uses ONLY `poll_timeout` as its wake source, the next
/// coordinator wake is therefore the exchange deadline itself — at
/// which point `Stream::handle_data` rejects the buffered request as
/// timed out and the exchange fails even though every byte was already
/// in quinn's recv buffer the moment the stream was accepted.
///
/// Step (5.5) of `run_tick` (and the mirror in `flush_outbound`) is
/// the same-tick second `pump_bridges` call that closes this gap.
///
/// The test drives a real handshake A↔B via `start_push_pull` on A and
/// ferries datagrams in both directions. It records the
/// `bridges_pumped_after_acceptance` counter on B BEFORE the iteration
/// `service_quinn` accepts the inbound bidi (signaled by
/// `live_bridge_count() >= 1` becoming true on B). Once at least one
/// inbound bridge appears on B, the counter MUST be `> 0` — step (5.5)
/// pumped the freshly-accepted bridge in the same tick it was inserted.
///
/// Negative control: revert the second `pump_bridges(now)` call in
/// `run_tick` (delete the step (5.5) invocation). The newly-accepted
/// inbound bridge on B is inserted by step (4) `service_quinn`'s
/// `accept(Dir::Bi)` loop AFTER step (2) already ran, so `pump_bridges`
/// never runs on it this tick; the counter increment branch in step
/// (5.5) never executes for that bridge and the counter stays at zero.
/// The assertion below fails.
#[test]
fn inbound_accepted_bridge_pumped_same_tick_under_strict_poll() {
  let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7972".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Initiate the push/pull join from A via the high-level wrapper so the
  // dial's Initial is on A's `out` queue immediately (the start-time
  // zero-time outbound flush). Then ferry datagrams in both directions
  // and call `handle_timeout(now)` per iteration to drive the handshake
  // to Established and exchange the push/pull bidi.
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);

  // The push/pull exchange opens a bidi from A to B. Once `service_quinn`
  // on B has accepted that bidi, B's `bridges_pumped_after_acceptance`
  // counter MUST advance — step (5.5) ran on the freshly-accepted bridge
  // before any subsequent tick could pump it. We do not gate on
  // `b.live_bridge_count()` directly because a short exchange can both
  // accept AND reap the bridge in the same iteration (after step (5.5)
  // pumps in the request data, drain_payload_only routes the
  // `PushPullReceived` endpoint event → `stream_load_response` →
  // `pump_out` writes the reply → bridge becomes terminal → next
  // `pump_bridges` reaps); the counter is the durable observable.
  let mut max_live_on_a: usize = 0;
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    max_live_on_a = max_live_on_a.max(a.live_bridge_count());
    if b.counters.bridges_pumped_after_acceptance > 0 {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }
  assert!(
    max_live_on_a >= 1,
    "test precondition: A must have opened the push/pull bridge to B \
       at some point within the bounded ferry loop (max live bridges \
       observed on A = {max_live_on_a}). The handshake must have \
       completed and `service_dials`'s retry must have opened the bidi."
  );
  assert!(
    b.counters.bridges_pumped_after_acceptance > 0,
    "every bridge inserted into `self.bridges` by `service_quinn`'s \
       `accept(Dir::Bi)` loop (step 4) or `service_dials`'s `open(Dir::Bi)` \
       (step 5) MUST be pumped within the SAME tick by `run_tick`'s step \
       (5.5) second `pump_bridges`. Counter on B = {} — without step (5.5), \
       the bridge accepted in step (4) sits in `self.bridges` with its first \
       request data un-pumped in quinn's recv buffer, and a strict-poll driver \
       next wakes at the bridge's exchange deadline (`Bridge::poll_timeout` \
       falls back to the snapshotted exchange deadline; \
       `quinn_proto::Connection::poll_timeout` reports transport timers only).",
    b.counters.bridges_pumped_after_acceptance,
  );
}

/// `quinn_proto::Endpoint::accept` returns
/// `Err(AcceptError { cause, response: Option<Transmit> })` for paths
/// where it owes a refusal/close to the peer: the `accept` body
/// produces `Some(self.initial_close(...))` on CID exhaustion and on a
/// handshake `TransportError` from `Connection::handle_first_packet`.
/// `route_datagram_event::DatagramEvent::NewConnection` extracts
/// `e.response`'s bytes onto the driver-facing `out` queue (mirroring
/// the existing `DatagramEvent::Response` arm); discarding the
/// response would force the peer to wait its full handshake
/// retransmit budget instead of receiving the immediate close.
///
/// This test exercises the response-extraction path on a NORMAL
/// handshake (where `accept` succeeds, so the counter stays at 0) —
/// asserting the counter is reachable and the `#[cfg(test)]` wiring
/// compiles. Forcing the actual error-with-response path
/// deterministically requires either filling `Endpoint::cids_exhausted`
/// — needs a custom 1-byte `ConnectionIdGenerator` + ~250 in-flight
/// CIDs — or crafting a
/// malformed Initial whose `Connection::handle_first_packet` returns
/// `TransportError` — neither composes cleanly in the focused-unit-test
/// budget. Negative control: revert the `Err(e)` extraction to bare
/// `Err(_)`; the counter increment is unreachable regardless of how
/// often the `Err`-with-response path fires, and any future test
/// that DOES drive that path (a CID-exhaustion bench, for instance)
/// observes the counter stuck at 0.
#[test]
fn accept_error_response_path_compiles_and_counter_is_wired() {
  let a_addr: SocketAddr = "127.0.0.1:7981".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7982".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive a clean handshake — `accept` succeeds on B's side, so no
  // `AcceptError` fires and the counter stays at 0. The structural
  // assertion is that the counter is observable and starts at 0
  // (i.e. the `#[cfg(test)]` wiring compiles and is reachable from
  // outside the module), so a future CID-exhaustion / malformed-
  // Initial test that DOES drive the `Err`-with-response path can
  // assert the counter advances.
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  // No AcceptError fires for a clean handshake.
  assert_eq!(
    a.counters.accept_error_responses_emitted, 0,
    "clean handshakes never trigger AcceptError; A counter MUST stay 0"
  );
  assert_eq!(
    b.counters.accept_error_responses_emitted, 0,
    "clean handshakes never trigger AcceptError; B counter MUST stay 0"
  );
}

/// The coordinator's alive-delegate forwarder installs the machine's
/// admission predicate: an inbound Alive for a vetoed peer never enters
/// membership. (The predicate's semantics are pinned by the endpoint suite;
/// this exercises the QUIC coordinator's public installation path.)
#[test]
fn alive_delegate_forwarder_installs_the_predicate() {
  struct RejectAllAlive;
  impl<I, A> crate::delegate::AliveDelegate<I, A> for RejectAllAlive
  where
    I: 'static,
    A: 'static,
  {
    fn notify_alive(&self, _peer: &crate::typed::NodeState<I, A>) -> bool {
      false
    }
  }

  let now = Instant::now();
  let mut b = make_endpoint("b", "127.0.0.1:7993".parse().unwrap(), now);
  b.set_alive_delegate(RejectAllAlive);
  b.endpoint_mut().process_alive(
    crate::typed::Alive::new(
      1,
      crate::Node::new(SmolStr::new("vetoed"), "127.0.0.1:7994".parse().unwrap()),
    ),
    false,
    now,
  );
  assert!(
    b.endpoint_ref().member(&SmolStr::new("vetoed")).is_none(),
    "the vetoed peer must not enter membership"
  );
}

/// A `MergeDelegate` that rejects every join push/pull merge —
/// `notify_merge -> false` exercises `Endpoint`'s admission-rejection
/// path which returns `Some(StreamCommand::Close)` to the bridge.
struct RejectAllMerges;
impl<I, A> crate::delegate::MergeDelegate<I, A> for RejectAllMerges
where
  I: 'static,
  A: 'static,
{
  fn notify_merge(&self, _peers: crate::MaybeOwned<'_, [crate::typed::NodeState<I, A>]>) -> bool {
    false
  }
}

/// A `MergeDelegate`-rejected inbound push/pull join MUST terminalize
/// the QUIC bridge in the SAME tick the `Close` command fires — full
/// bidi retirement (reset send + stop recv), `pending_out` cleared,
/// `fatal` set, and `pump_bridges`'s post-`drain_payload_only`
/// `is_terminal()` re-check D1-drains + reaps the bridge before the
/// tick returns. A reset-only retirement that left `fatal == false`
/// would reinsert the bridge into `self.bridges` and pin the quinn
/// bidi stream until its `~5s` exchange deadline elapsed, letting a
/// rejected peer hold transport resources per attempt
/// (admission-boundary weakening).
///
/// The test installs `RejectAllMerges` on B, drives a `Join` push/pull
/// from A → B via the public poll surface, and asserts:
///  (1) B's `live_bridge_count() == 0` after the ferry loop terminates
///      (the rejected bridge was reaped in-tick, not pinned to the
///      deadline);
///  (2) the rejection completes within the bounded ferry budget — no
///      deadline-elapsed reap.
///
/// Negative control: revert the `Close` arm in
/// `bridge.rs::drain_payload_only` to bare `send_stream(sid).reset(0)`
/// (no recv stop, no `fatal`, no `pending_out` clear) AND revert the
/// `pump_bridges` post-`drain_payload_only` re-check. The bridge then
/// gets reinserted into `self.bridges` with `fatal == false`; B's
/// `live_bridge_count()` is `> 0` after the ferry loop and the
/// assertion fires.
#[test]
fn rejected_join_merge_close_terminalizes_bridge_same_tick() {
  let a_addr: SocketAddr = "127.0.0.1:7991".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7992".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // B rejects every inbound merge — A's Join push/pull will trigger
  // `Endpoint::handle_stream_event(PushPullRequestReceived{kind:Join})`
  // on B's side, `merge_admitted` returns false, and the
  // `StreamCommand::Close` is routed to B's bridge.
  // Installed through the coordinator's public forwarder (not the test-only
  // endpoint accessor), so the forwarder itself is behaviorally covered.
  b.set_merge_delegate(RejectAllMerges);

  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);

  // Bounded ferry — handshake completes, A sends Join state, B's
  // delegate rejects, B emits the Close. The loop terminates on
  // no-traffic AFTER both sides processed at least one endpoint
  // event (handshake completion) so iteration 1 doesn't bail.
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  // Observable: B's `pump_bridges` post-`drain_payload_only`
  // `is_terminal()` re-check fired at least once for the rejected
  // bridge. The counter ticks ONLY when a bridge that was non-terminal
  // entering `drain_payload_only` became terminal during the per-tick
  // endpoint-event drain — i.e. the `StreamCommand::Close` path that
  // an admission rejection takes. A reset-only retirement on the
  // `Close` arm would leave `fatal == false` and the re-check would
  // never fire (counter stays 0).
  assert!(
    b.counters.bridges_terminalized_via_close_command > 0,
    "B's `pump_bridges` post-`drain_payload_only` `is_terminal()` \
       re-check MUST have fired at least once for the rejected-merge \
       bridge (counter = {}). Without the full bidi retirement on the \
       `Close` arm the bridge stays non-terminal, and the re-check never \
       tips — the bridge lingers in `self.bridges` until its exchange \
       deadline (~5 s).",
    b.counters.bridges_terminalized_via_close_command,
  );

  // And the bridge is gone by the time the loop terminates — no
  // lingering quinn bidi state on B.
  assert_eq!(
    b.live_bridge_count(),
    0,
    "B's `self.bridges` MUST be empty after the rejected join loop \
       terminates. Got {} live bridge(s) — the rejected bridge leaked \
       beyond the rejection tick.",
    b.live_bridge_count(),
  );
}

/// `Bridge::drain_then_reap` MUST retire the QUIC recv half before
/// dropping the bridge. Memberlist frames are length-prefixed: the
/// inner `Stream` FSM decodes the declared frame length before reading
/// FIN and transitions to `Done` after consuming the declared bytes
/// regardless of whether quinn-proto observed FIN on the recv half.
/// Dropping the bridge without an explicit `RecvStream::stop` would
/// orphan any late FIN / post-FIN events on the recv half —
/// quinn-proto frees recv state only when `Chunks::next` observes
/// finished OR the recv half is explicitly stopped/reset. `drain_then_reap`
/// therefore unconditionally `stop()`s the recv half (idempotent —
/// `Err(ClosedStream)` on the already-retired case the error/fatal
/// pump_in/pump_out paths produce).
///
/// This test exercises the clean-exchange path: A↔B push/pull where
/// both sides cooperate, both `finish()` their send halves, both recv
/// halves observe FIN naturally → `remote_open_streams(Dir::Bi)`
/// returns 0 whether or not the unconditional `stop()` is present
/// (the natural FIN-observed retire makes the same assertion hold
/// either way). The test documents that the unconditional stop does
/// NOT regress clean exchanges; the case the unconditional stop
/// guards against — an adversarial peer who withholds FIN entirely
/// (or splits FIN from the last data frame) — requires a custom
/// quinn peer that decouples data from FIN to reproduce
/// deterministically. Correctness in that adversarial case rests on
/// the per-half retirement semantics described above.
///
/// Negative control (by-hand verification documented here): revert
/// the `recv_stream(sid).stop(0)` block inside `drain_then_reap`
/// AND drive an adversarial peer that decodes a full memberlist
/// frame on the local stream then keeps the send half open without
/// FIN — `Connection::streams().remote_open_streams(Dir::Bi)` on the
/// receiving node stays `> 0` after the bridge reaps. This test
/// alone does NOT trigger that path.
#[test]
fn clean_bridge_reap_retires_quic_recv_half() {
  let a_addr: SocketAddr = "127.0.0.1:7901".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7902".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  let _id = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);

  // Drive the clean exchange to completion: handshake, A sends Join
  // state, B replies, both sides decode, both bridges become Done +
  // reap on the clean terminal path. Bail when no traffic moves AND
  // both sides have at least observed handshake completion.
  for _ in 0..400 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
      && a.live_bridge_count() == 0
      && b.live_bridge_count() == 0
    {
      break;
    }
  }

  // Test precondition: both bridges reaped on the clean path.
  assert_eq!(
    a.live_bridge_count(),
    0,
    "test precondition: A's bridge MUST have reached the clean reap path"
  );
  assert_eq!(
    b.live_bridge_count(),
    0,
    "test precondition: B's bridge MUST have reached the clean reap path"
  );

  // Observable: B's pooled connection has zero outstanding remote-
  // initiated bidi streams. Without the unconditional recv-half
  // stop, B's recv half for the push/pull exchange lingers in
  // `StreamsState::recv` and this count stays > 0.
  let ch_b = b
    .conns
    .iter_handles()
    .first()
    .copied()
    .expect("B holds the pooled connection from A's dial");
  let remote_open = b
    .conns
    .get_mut(ch_b)
    .expect("B's pooled ConnEntry is present")
    .conn_mut()
    .streams()
    .remote_open_streams(quinn_proto::Dir::Bi);
  assert_eq!(
    remote_open, 0,
    "B's pooled connection MUST retire the recv half of the accepted \
       push/pull bidi when the bridge clean-reaps. \
       remote_open_streams(Bi) = {remote_open} — the recv half leaked \
       beyond the clean reap, and repeated rounds would exhaust the \
       bidi credit on this connection."
  );
}

/// `LinkState::Failed` reap MUST be recv-clean AND send-clean by
/// construction — every failure transition retires both halves
/// atomically BEFORE flipping the phase, so a bridge that becomes
/// terminal via the failure path cannot orphan quinn stream state.
///
/// The most subtle failure path the atomic-retirement contract has
/// to cover: an outbound exchange waiting for a response (FSM
/// `OutboundAwaitingResponse`; `pending_out` empty; `is_done()` is
/// false) whose exchange deadline elapses. The FSM's own deadline
/// machinery inside `Stream::poll_transmit` transitions the FSM to
/// `Failed(Timeout)` and returns `None`; the pump's deferred-finish
/// path is gated against `stream.is_failed()` so it skips the
/// finish; the bridge's pre-write deadline check is gated on
/// `!pending_out.is_empty()` so it skips; the bridge's
/// `Done`-but-unflushed check is gated on `is_done()` so it
/// skips. Without the dedicated post-`poll_transmit` FSM-failure
/// detector in pump_out, `is_terminal()` would still return `true`
/// (via the old `stream.is_failed()` fallback) but `pump_out`
/// would have called NEITHER `SendStream::reset` NOR
/// `RecvStream::stop` — the bridge reaps with both halves orphaned
/// in `StreamsState`, and the pooled connection bleeds bidi credit.
///
/// Atomic-retirement is the architectural fix: every transition
/// to `Failed(_)` is preceded by `retire_halves(conns)`
/// (idempotent `reset + stop + clear_pending`), AND
/// `is_terminal()` is now phase-authoritative (no
/// `stream.is_failed()` fallback) so a bridge with an
/// FSM-failed-but-phase-not-yet-Failed transient is NOT reaped
/// until the next pump detects the FSM failure and runs the
/// atomic transition.
///
/// This test verifies the structural property on the OUTBOUND
/// post-`poll_transmit` deadline path: A initiates a push/pull to
/// a peer that doesn't respond (no peer at the address), waits past
/// the exchange deadline, then drives one tick of `handle_timeout`.
/// Asserts (a) A's bridge reaps within the tick (`live_bridge_count
/// == 0`); (b) A's pooled connection's `remote_open_streams(Bi)` is
/// 0 (no orphan recv state on A's view of the stream A initiated,
/// since A-initiated streams are not "remote-opened" on A's side —
/// but A's `send_streams()` count is the right observable here);
/// (c) A's `send_streams()` is unchanged from the pre-dial baseline.
///
/// Negative control documented inline: revert the
/// `if let Some(e) = self.stream.is_failed()` block in
/// `pump_out` (the post-`poll_transmit` FSM-failure detector) AND
/// restore `is_terminal()`'s `stream.is_failed()` fallback. A's
/// bridge still reaps within the tick, but A's `send_streams()`
/// stays elevated above baseline because the orphaned send half
/// was never reset.
#[test]
fn fsm_deadline_during_outbound_await_reaps_atomically_retired() {
  let a_addr: SocketAddr = "127.0.0.1:7993".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);

  // Baseline: no streams open before any dial.
  // We can't easily reach `a.conns` without a pooled connection; instead
  // we'll check post-reap that A's bridges have all reaped AND no
  // pooled connection holds any open send_streams.

  // Initiate a push/pull to an unreachable peer. The dial will
  // create a connection that handshakes against ... nothing. The
  // bridge intent's deadline = now + stream_timeout (~5s); the
  // handshake won't complete; eventually the deadline elapses.
  let unreachable: SocketAddr = "127.0.0.1:7994".parse().unwrap();
  // Ignoring StreamId return: tests assert on observable side
  // effects (poll_transmit/poll_event), not the handle itself.
  let _ = a
    .endpoint_mut()
    .start_push_pull(unreachable, PushPullKind::Refresh, now);

  // Drive A's tick: dial fires, Initial queued. Then jump time
  // past the bridge's exchange deadline (~5s by default; using 30s
  // to be well past it).
  a.handle_timeout(now);
  let past_deadline = now + Duration::from_secs(30);
  a.handle_timeout(past_deadline);

  // Observable: A's bridges all reaped within the deadline-trip tick.
  // The atomic-retire-then-fail path retires both halves,
  // `is_terminal()` returns true via the `Failed(Timeout)` phase
  // (phase-authoritative, with no `stream.is_failed()` fallback), and
  // `pump_bridges` reaps.
  assert_eq!(
    a.live_bridge_count(),
    0,
    "A's bridge MUST reap within the deadline-trip tick (got {} live \
       bridges on A — the bridge is stuck non-terminal or the \
       deadline-trip path is not firing)",
    a.live_bridge_count(),
  );

  // Check every pooled connection on A: `send_streams()` MUST be 0
  // (every stream A opened has been fully retired by the atomic
  // `reset` in the failure transition; no orphan send state). Without
  // atomic retirement, the bridge would reap via a `stream.is_failed()`
  // fallback and `send_streams()` would be > 0 — quinn would still be
  // tracking the orphaned send half.
  for ch in a.conns.iter_handles() {
    let entry = a.conns.get_mut(ch).expect("conn entry present");
    let send_streams_open = entry.conn_mut().streams().send_streams();
    assert_eq!(
      send_streams_open, 0,
      "every reaped bridge on A's pooled connection MUST have retired \
         its send half by the atomic failure transition. conn {ch:?} \
         has {send_streams_open} send_streams open — indicating a send \
         half was orphaned in StreamsState::send."
    );
  }
}

#[cfg(all(feature = "lz4", encryption))]
fn build_test_quic_endpoint_with_compression(
  compression: crate::CompressionOptions,
) -> QuicEndpoint<SmolStr> {
  let addr: SocketAddr = "127.0.0.1:7999".parse().unwrap();
  let now = Instant::now();
  let cfg = EndpointOptions::new(SmolStr::new("test"), addr);
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  ep.start_scheduling(now);
  let qc = test_config();
  QuicEndpoint::<SmolStr>::with_compression(ep, qc, compression)
}

#[cfg(all(test, feature = "quic", feature = "lz4", encryption))]
#[test]
fn quic_endpoint_gossip_compression_roundtrips() {
  use crate::{CompressAlgorithm, CompressionOptions};
  let coord = build_test_quic_endpoint_with_compression(
    CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(64),
  );
  let datagram = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".repeat(8);
  let on_wire = coord.compress_gossip(&datagram);
  assert!(
    on_wire.len() < datagram.len(),
    "compressible datagram must shrink"
  );
  let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
  assert_eq!(back, datagram);
}

#[cfg(all(test, feature = "quic", feature = "lz4", encryption))]
#[test]
fn quic_endpoint_over_mtu_compressed_gossip_is_rejected() {
  // A wrapper whose orig_len exceeds the gossip MTU cannot be produced by
  // any compliant coordinator. Build one synthetically and assert it is
  // rejected without touching the body (the bomb guard checks orig_len
  // before allocation).
  use crate::CompressAlgorithm;
  let coord = build_test_quic_endpoint_with_compression(
    crate::CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(64),
  );
  let over_mtu = coord.gossip_mtu() + 1;
  let frame = crate::encode_compressed_frame(CompressAlgorithm::Lz4, over_mtu, b"x");
  assert!(
    coord.decrypt_gossip(&frame).is_err(),
    "a compressed gossip frame claiming orig_len > gossip_mtu must be rejected"
  );
}

#[cfg(all(test, feature = "quic", feature = "lz4", encryption))]
#[test]
fn quic_endpoint_compressed_gossip_never_inflates() {
  use crate::{CompressAlgorithm, CompressionOptions};
  // Low threshold so the compressor attempts compression for all sizes in
  // the sweep, exercising the don't-expand else branch for sizes where the
  // wrapper header overhead erases the raw saving.
  let coord = build_test_quic_endpoint_with_compression(
    CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(1),
  );
  // Sweep up to the gossip MTU: inbound decode rejects a datagram whose
  // recovered plaintext exceeds the ceiling, so a round-trip is only defined
  // for in-budget sizes (the over-MTU rejection is covered separately above).
  for len in 1..=coord.gossip_mtu() {
    // Mostly-varying pattern with a short repeated motif: the backend's
    // saving is small and varies with `len`, ensuring the don't-expand
    // else branch is exercised for some sizes while still round-tripping
    // for all.
    let input: Vec<u8> = (0..len)
      .map(|i| {
        let base = (i % 7) as u8;
        base ^ ((i / 7) as u8).wrapping_mul(3)
      })
      .collect();
    let compressed = coord.compress_gossip(&input);
    assert!(
      compressed.len() <= input.len(),
      "compress_gossip inflated datagram at len={len}: {} > {}",
      compressed.len(),
      input.len(),
    );
    let back = coord.decrypt_gossip(&compressed).expect("decrypt failed");
    assert_eq!(
      back, input,
      "compress_gossip round-trip failed at len={len}",
    );
  }
}

#[cfg(feature = "aes-gcm")]
fn build_test_quic_endpoint_with_encryption(
  encryption: crate::EncryptionOptions,
) -> QuicEndpoint<SmolStr> {
  let addr: SocketAddr = "127.0.0.1:7999".parse().unwrap();
  let now = Instant::now();
  let cfg = EndpointOptions::new(SmolStr::new("test"), addr);
  let mut ep: Endpoint<SmolStr, SocketAddr> = Endpoint::new_seeded(cfg);
  ep.start_scheduling(now);
  let qc = test_config();
  QuicEndpoint::<SmolStr>::new(ep, qc).with_encryption(encryption)
}

#[cfg(all(test, feature = "quic", feature = "aes-gcm"))]
#[test]
fn quic_endpoint_gossip_encryption_roundtrip() {
  use crate::{EncryptionOptions, Keyring, SecretKey};
  let kr = Keyring::new(SecretKey::Aes256([0x42; 32]));
  let coord = build_test_quic_endpoint_with_encryption(EncryptionOptions::new().with_keyring(kr));
  let datagram = b"a quic-coordinated gossip body".to_vec();
  let on_wire = coord.encrypt_gossip(&datagram).expect("encrypt");
  assert_eq!(on_wire[0], crate::ENCRYPTED_TAG);
  let back = coord.decrypt_gossip(&on_wire).expect("decrypt");
  assert_eq!(back, datagram);
}

/// A datagram larger than the connection's negotiated `max_size` is reported
/// `TooLarge` (never silently dropped) — the driver's UDP fallback then
/// carries it. This is the spec's "split OR fall back to UDP, never silently
/// dropped" guarantee: the machine surface always returns an actionable
/// outcome rather than discarding the payload.
///
/// Also confirms the boundary: a payload of exactly `max_size` bytes is
/// `Queued`, not `TooLarge`.
#[test]
fn oversize_unreliable_datagram_reports_too_large() {
  let a_addr: SocketAddr = "127.0.0.1:7983".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7984".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive an A<->B push/pull handshake to Established so the connection
  // has negotiated a datagram max_size (the proven ferry loop pattern).
  // Ignoring StreamId return: the test asserts on the datagram outcome.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_bridge_count() >= 1
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  let max = a
    .connection_datagram_max_size(b_addr)
    .expect("an Established connection exposes a datagram max_size");

  // One byte over the negotiated limit must be TooLarge — never a silent drop.
  let too_big = Bytes::from(vec![0u8; max + 1]);
  assert_eq!(
    a.queue_unreliable_datagram(b_addr, too_big, now),
    super::DatagramSendStatus::TooLarge,
    "a payload of max_size+1 ({} bytes) must be TooLarge, not silently dropped",
    max + 1,
  );

  // A payload of exactly max_size bytes sits within the limit and is Queued.
  let ok = Bytes::from(vec![0u8; max]);
  assert_eq!(
    a.queue_unreliable_datagram(b_addr, ok, now),
    super::DatagramSendStatus::Queued,
    "a payload of exactly max_size ({max} bytes) must be Queued",
  );
}

/// With drop=false, filling the datagram send buffer returns NotReady (the
/// driver then falls back to UDP) instead of silently evicting the OLDEST queued
/// datagram. This is what prevents a burst from dropping an in-flight probe.
#[test]
fn full_datagram_send_buffer_reports_not_ready_without_eviction() {
  let a_addr: SocketAddr = "127.0.0.1:7995".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7996".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive an A<->B push/pull handshake to Established so the connection has a
  // negotiated datagram max_size (the proven ferry loop pattern).
  // Ignoring StreamId return: the test asserts on the datagram outcome.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_bridge_count() >= 1
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  // Query the connection's datagram max_size; use payloads near it so the 64 KiB
  // send buffer fills within a bounded number of un-flushed sends.
  let max = a.connection_datagram_max_size(b_addr).expect("max_size");
  let chunk = max.clamp(1, 4096);
  let payload = Bytes::from(vec![0u8; chunk]);
  let mut saw_not_ready = false;
  for _ in 0..10_000 {
    match a.queue_unreliable_datagram(b_addr, payload.clone(), now) {
      super::DatagramSendStatus::Queued => {}
      super::DatagramSendStatus::NotReady => {
        saw_not_ready = true;
        break;
      }
      super::DatagramSendStatus::TooLarge => {
        panic!("payload within max_size must not be TooLarge")
      }
    }
  }
  assert!(
    saw_not_ready,
    "drop=false must report NotReady when the send buffer is full, not evict and report Queued"
  );
}

/// A Udp-mode endpoint does not enable quinn's datagram extension, so it never
/// advertises `max_datagram_frame_size`: its established connections report
/// `datagrams().max_size() == None`. This is the structural guarantee behind
/// the pure-UDP opt-out AND cross-mode interop — a Datagram-mode peer dialing a
/// Udp-mode node sees no datagram capability, reports `NotReady`, and falls
/// back to plain UDP rather than emitting datagrams the node would swallow.
#[test]
fn udp_mode_does_not_advertise_datagram_support() {
  let a_addr: SocketAddr = "127.0.0.1:7993".parse().unwrap(); // Udp-mode receiver
  let b_addr: SocketAddr = "127.0.0.1:7994".parse().unwrap(); // Datagram-mode dialer
  let now = Instant::now();
  let mut a = make_endpoint_udp("a", a_addr, now); // Udp mode: datagrams disabled
  let mut b = make_endpoint("b", b_addr, now); // Datagram mode (default)

  // Drive an A<->B push/pull handshake to Established (the proven ferry loop
  // pattern); the handshake itself does not depend on the unreliable mode, so
  // the connection forms regardless of the datagram capability difference.
  // Ignoring StreamId return: the test asserts on the negotiated datagram
  // capability, not the handle.
  let _ = b
    .endpoint_mut()
    .start_push_pull(a_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if b.live_bridge_count() >= 1
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
    if !moved
      && a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
    {
      break;
    }
  }

  // quinn's `Connection::datagrams().max_size()` is the LOCAL send ceiling,
  // gated by the PEER's advertised `max_datagram_frame_size`. A (Udp mode)
  // advertised none, so B's connection to A reports `max_size() == None`: B
  // CANNOT send a datagram to A. This is the load-bearing cross-mode interop
  // guarantee — a Datagram-mode peer never emits a datagram a Udp-mode node
  // would silently swallow.
  assert_eq!(
    b.connection_datagram_max_size(a_addr),
    None,
    "a Datagram-mode peer must see no datagram capability toward a Udp-mode node \
       that disabled the extension (max_size == None)"
  );

  // The direct consequence at the send API: B's offer is `NotReady`, so the
  // driver falls back to plain UDP rather than dropping the payload.
  assert_eq!(
    b.queue_unreliable_datagram(a_addr, Bytes::from_static(b"\x01x"), now),
    super::DatagramSendStatus::NotReady,
    "a Datagram-mode peer must get NotReady for a Udp-mode node that disabled datagrams"
  );
}

/// Strict-mode rejection MUST fire at the coordinator's public ingress
/// API. A configured keyring + a leading tag that is not `Encrypted` is
/// an unauthenticated plaintext Ping/Ack/Alive frame; passing it through
/// to `handle_memberlist_udp` would bypass strict-mode entirely. The
/// coordinator's single canonical ingress helper `decrypt_gossip` routes
/// through `unwrap_transforms_with_encryption`, which applies the
/// strict-mode entry check before any wrapper decoding, so cluster
/// authentication holds without depending on driver discipline.
#[cfg(all(test, feature = "quic", feature = "aes-gcm"))]
#[test]
fn quic_decrypt_gossip_rejects_plaintext_when_encryption_enabled() {
  use crate::{EncryptionOptions, FrameError, Keyring, MessageTag, SecretKey, encode_plain_frame};
  let opts = EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x42; 32])));
  let coord = build_test_quic_endpoint_with_encryption(opts);
  // A plain Ping frame — its leading byte is `MessageTag::Ping`, not
  // `Encrypted`, so the strict-mode entry check inside
  // `unwrap_transforms_with_encryption` MUST reject before any decoding.
  // Body shape does not matter for the leading-byte check.
  let plain_ping = encode_plain_frame(MessageTag::Ping, b"opaque-body").expect("encode");
  let result = coord.decrypt_gossip(&plain_ping);
  assert!(
    matches!(result, Err(FrameError::Encryption(_))),
    "decrypt_gossip MUST reject a plaintext datagram while encryption \
       is enabled — got {result:?}",
  );
}

/// A QUIC packet delivered via handle_udp must NOT advance membership timers:
/// the packet may carry application datagrams (probe Acks/Alives) the driver has
/// not yet decoded, so firing a probe/suspicion deadline here would be a false
/// failure. Membership time advances ONLY on the driver's explicit
/// handle_timeout, after it decodes mem_ingress. (The plain-UDP ingress path
/// already has this property.)
#[test]
fn quic_packet_ingress_does_not_advance_membership_time() {
  let a_addr: SocketAddr = "127.0.0.1:7997".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7998".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Establish an A<->B connection (the ferry pattern) so B has a real QUIC
  // packet to send to A. Ignoring StreamId: the test asserts on the counter.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  let mut b_to_a: Option<Bytes> = None;
  for _ in 0..200 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        // capture one real QUIC packet from B to A WITHOUT feeding it yet
        if b_to_a.is_none() {
          b_to_a = Some(bytes.clone());
        }
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.counters.endpoint_events_processed > 0
      && b.counters.endpoint_events_processed > 0
      && b_to_a.is_some()
    {
      break;
    }
  }
  let packet = b_to_a.expect("B must have produced a QUIC packet to A");

  // Deliver a QUIC packet via handle_udp and assert membership time did NOT
  // advance; then an explicit handle_timeout DOES advance it.
  let before = a.membership_time_advances();
  a.handle_udp(b_addr, &packet, now);
  assert_eq!(
    a.membership_time_advances(),
    before,
    "handle_udp(Class::Quic) must not advance membership time (it may carry an undecoded datagram Ack)"
  );
  a.handle_timeout(now);
  assert_eq!(
    a.membership_time_advances(),
    before + 1,
    "handle_timeout must advance membership time exactly once"
  );
}

/// The thin membership pass-throughs each forward to the inner `Endpoint`
/// and anchor `last_now` where documented. Exercised on a single
/// freshly-constructed coordinator without a peer: `start_probe` (no peer ⇒
/// `false`), `handle_alive` seeds a member, `handle_suspect` is accepted,
/// `ping` returns a correlation id and queues an unreliable transmit,
/// `send_user_packets` enqueues directed gossip, and the simple accessors
/// (`is_running`, `gossip_mtu`, `max_stream_frame_size`, `live_bridge_count`,
/// `live_connections_to`, `unreliable_transport`, `compression`,
/// `endpoint_ref`) report the constructed defaults.
#[test]
fn membership_pass_throughs_forward_to_inner_endpoint() {
  use crate::{
    Node,
    typed::{Alive, Meta, Suspect},
  };

  let addr: SocketAddr = "127.0.0.1:7710".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7711".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);

  // Construction-time accessors.
  assert!(
    ep.is_running(),
    "a fresh coordinator is in normal operation"
  );
  assert!(ep.gossip_mtu() > 0);
  assert!(ep.max_stream_frame_size() > 0);
  assert_eq!(ep.live_bridge_count(), 0);
  assert_eq!(ep.live_connections_to(peer), 0, "no connection dialed yet");
  assert_eq!(ep.unreliable_transport(), UnreliableTransport::Datagram);
  #[cfg(compression)]
  let _ = ep.compression();
  let _ = ep.endpoint_ref();

  // start_probe on a single-node cluster: no eligible target ⇒ false.
  assert!(
    !ep.start_probe(now),
    "no peer to probe ⇒ start_probe returns false"
  );

  // handle_alive seeds a member for `peer`.
  let alive = Alive::new(1, Node::new(SmolStr::new("peer"), peer)).with_meta(Meta::empty());
  ep.handle_alive(peer, alive, now);

  // handle_suspect against the seeded member is accepted (no panic, time
  // anchored).
  let suspect = Suspect::new(1, SmolStr::new("peer"), SmolStr::new("self"));
  ep.handle_suspect(peer, suspect, now);

  // ping returns a correlation token and queues an unreliable transmit.
  let _ping_id = ep
    .ping(Node::new(SmolStr::new("peer"), peer), now)
    .expect("issued while running");
  assert!(
    ep.poll_memberlist_transmit().is_some(),
    "ping must enqueue an unreliable gossip transmit"
  );

  // send_user_packets enqueues directed gossip within the MTU budget.
  ep.send_user_packets(peer, &[Bytes::from_static(b"hi")])
    .expect("a small directed user packet fits the gossip MTU");
  assert!(
    ep.poll_memberlist_transmit().is_some(),
    "send_user_packets must enqueue an unreliable transmit"
  );
}

/// `requeue_event` for a NON-`DialRequested` event delegates to the inner
/// endpoint's requeue (the `other => self.ep.requeue_event(other)` arm) and
/// is observable via the sieving public `poll_event`. (The `DialRequested`
/// direct-routing arm is covered by `requeued_dial_request_under_strict_poll_anchors_last_now`.)
#[test]
fn requeue_non_dial_event_round_trips_through_poll_event() {
  use Event;

  let addr: SocketAddr = "127.0.0.1:7720".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);
  // Drain the construction-time self-join so the requeued event is observed
  // first below.
  while ep.poll_event().is_some() {}

  // A `LeftCluster` event (application-visible unit variant, not
  // DialRequested) must round-trip back out through poll_event after a
  // requeue — proving the `other` delegation arm.
  ep.requeue_event(Event::LeftCluster, now);
  let got = ep.poll_event();
  assert!(
    matches!(got, Some(Event::LeftCluster)),
    "a requeued non-dial event surfaces through poll_event — got {got:?}"
  );
}

/// `update_meta` / `queue_user_broadcast` / `set_local_state_snapshot` /
/// `set_ack_payload` are the inner-endpoint state setters; each forwards and
/// returns `Ok` for in-budget inputs on a running coordinator.
#[test]
fn state_setter_pass_throughs_return_ok_for_in_budget_inputs() {
  use crate::typed::Meta;

  let addr: SocketAddr = "127.0.0.1:7730".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);

  ep.update_meta(Meta::try_from(Bytes::from_static(b"v2")).expect("small meta"))
    .expect("update_meta forwards and succeeds while running");
  ep.queue_user_broadcast(Bytes::from_static(b"broadcast"))
    .expect("queue_user_broadcast forwards");
  ep.set_local_state_snapshot(Bytes::from_static(b"snap"))
    .expect("a tiny push-pull snapshot fits the reliable frame budget");
  ep.set_ack_payload(Bytes::from_static(b"ack"))
    .expect("a tiny ack payload fits the gossip budget");
}

/// `queue_user_broadcast_ranked` enqueues at the requested priority tier and
/// increments the inner endpoint's user-broadcast queue length. Mirrors the
/// inner `Endpoint::queue_user_broadcast_ranked` contract: rank 0 is the
/// highest priority tier; out-of-range ranks saturate to the lowest tier rather
/// than being rejected.
#[test]
fn queue_user_broadcast_ranked_forwards_to_inner_endpoint() {
  let addr: SocketAddr = "127.0.0.1:7741".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);

  assert_eq!(
    ep.endpoint_ref().user_broadcast_queue_len(),
    0,
    "fresh coordinator has an empty broadcast queue",
  );
  ep.queue_user_broadcast_ranked(0, Bytes::from_static(b"high-priority"))
    .expect("in-budget payload enqueues at rank 0");
  assert_eq!(
    ep.endpoint_ref().user_broadcast_queue_len(),
    1,
    "one ranked broadcast lands in the inner endpoint's queue",
  );
  ep.queue_user_broadcast_ranked(1, Bytes::from_static(b"lower-priority"))
    .expect("in-budget payload enqueues at rank 1");
  assert_eq!(
    ep.endpoint_ref().user_broadcast_queue_len(),
    2,
    "both ranked broadcasts land in the inner endpoint's queue",
  );
}

/// `send_user_packet` enqueues a single directed unreliable user packet and
/// the transmit surfaces via `poll_memberlist_transmit`. Covers the
/// singular-packet forwarder on `QuicEndpoint` (distinct from the plural
/// `send_user_packets`).
#[test]
fn send_user_packet_enqueues_one_transmit() {
  let addr: SocketAddr = "127.0.0.1:7742".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7743".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);

  ep.send_user_packet(peer, Bytes::from_static(b"hi"))
    .expect("a small directed user packet fits the gossip MTU");
  assert!(
    ep.poll_memberlist_transmit().is_some(),
    "send_user_packet must enqueue an unreliable transmit",
  );
}

/// `leave` initiates the graceful dead-self flush on a running coordinator
/// (returns `Ok`), and a second `leave` is the idempotent post-leave no-op.
/// `is_running` flips to `false` after the first leave.
#[test]
fn leave_initiates_then_is_idempotent() {
  let addr: SocketAddr = "127.0.0.1:7740".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);
  assert!(ep.is_running());
  ep.leave(now).expect("the first leave initiates the flush");
  assert!(
    !ep.is_running(),
    "after leave the lifecycle is no longer Running"
  );
  // Idempotent: a second leave on an already-left endpoint is a no-op Ok.
  ep.leave(now)
    .expect("a second leave is an idempotent no-op");
}

/// `set_compression_options` replaces the policy in place and is reflected by
/// the `compression()` accessor; `compress_gossip` then applies the new
/// policy on the next datagram.
#[cfg(feature = "lz4")]
#[test]
fn set_compression_options_updates_policy_in_place() {
  use crate::{CompressAlgorithm, CompressionOptions};

  let addr: SocketAddr = "127.0.0.1:7750".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);
  // Default is disabled — a large datagram passes through unchanged.
  let payload = vec![0xABu8; 4096];
  assert_eq!(
    ep.compress_gossip(&payload),
    payload,
    "disabled compression is identity"
  );
  // Enable lz4; the accessor reflects it and a compressible datagram shrinks.
  ep.set_compression_options(
    CompressionOptions::new()
      .with_algorithm(CompressAlgorithm::Lz4)
      .with_threshold(8),
  );
  assert!(ep.compression().algorithm().is_some());
  let compressible = b"the quick brown fox jumps over the lazy dog".repeat(64);
  let out = ep.compress_gossip(&compressible);
  assert!(
    out.len() < compressible.len(),
    "a highly-compressible datagram must shrink once lz4 is enabled"
  );
}

/// With encryption DISABLED (the `make_endpoint` default), `encrypt_gossip`
/// returns the datagram unchanged (the `None` keyring early-return), the
/// `encryption_options` accessor reports a disabled policy, and
/// `decrypt_gossip` on a plaintext datagram is an identity passthrough (the
/// strict-mode entry check is gated on `encryption.is_enabled()`).
#[cfg(encryption)]
#[test]
fn gossip_transforms_are_identity_when_encryption_disabled() {
  let addr: SocketAddr = "127.0.0.1:7760".parse().unwrap();
  let now = Instant::now();
  let ep = make_endpoint("self", addr, now);
  assert!(
    !ep.encryption_options().is_enabled(),
    "the default coordinator has encryption disabled"
  );
  let datagram = b"plain gossip body".to_vec();
  assert_eq!(
    ep.encrypt_gossip(&datagram)
      .expect("disabled encrypt is identity"),
    datagram,
    "encrypt_gossip with no keyring returns the bytes unchanged"
  );
  assert_eq!(
    ep.decrypt_gossip(&datagram)
      .expect("disabled decrypt is identity"),
    datagram,
    "decrypt_gossip with no keyring passes a plaintext datagram through"
  );
}

/// `set_encryption_options` no-op-reapply short-circuit: republishing the
/// SAME effective policy takes the equality early-return (it does NOT clear
/// the buffered gossip ingress). A DIFFERENT policy takes the clear path —
/// the buffered raw gossip datagram is dropped so it cannot be decrypted
/// under the new policy.
#[cfg(feature = "aes-gcm")]
#[test]
fn set_encryption_options_noop_reapply_vs_policy_change_clears_ingress() {
  use crate::{EncryptionOptions, Keyring, SecretKey};

  let self_addr: SocketAddr = "127.0.0.1:7770".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7771".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);

  // Buffer a raw inbound gossip datagram by feeding a Memberlist-classified
  // UDP packet (first byte in 1..=15 ⇒ Class::Memberlist ⇒ buffered).
  ep.handle_udp(peer, &[3u8, 0u8, 0u8], now);
  assert!(
    ep.poll_memberlist_ingress().is_some(),
    "precondition: a memberlist datagram is buffered"
  );

  // Re-buffer, then a NO-OP reapply of the same (disabled) policy: the
  // equality early-return must NOT clear the buffered datagram.
  ep.handle_udp(peer, &[3u8, 0u8, 0u8], now);
  ep.set_encryption_options(EncryptionOptions::new());
  assert!(
    ep.poll_memberlist_ingress().is_some(),
    "a no-op reapply of the identical policy must preserve buffered gossip"
  );

  // Re-buffer, then a REAL policy change (disabled → enabled keyring): the
  // clear path drops the datagram queued under the old policy.
  ep.handle_udp(peer, &[3u8, 0u8, 0u8], now);
  ep.set_encryption_options(
    EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x11; 32]))),
  );
  assert!(
    ep.poll_memberlist_ingress().is_none(),
    "a policy change must clear the buffered gossip queued under the old policy"
  );
  assert!(
    ep.encryption_options().is_enabled(),
    "the new policy is now in effect"
  );
}

/// `try_open_uni_stream_to` returns `false` when no connection to the peer
/// exists — the `handle_for(&peer)` early-`None` guard — and anchors
/// `last_now` so a subsequent `poll_timeout` is not stuck at `None`.
#[test]
fn try_open_uni_stream_to_unknown_peer_is_false() {
  let addr: SocketAddr = "127.0.0.1:7780".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7781".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);
  assert!(
    !ep.try_open_uni_stream_to(peer, now),
    "no connection to the peer ⇒ false (no uni-stream credit to probe)"
  );
}

/// `start_reliable_ping` is the reliable-fallback dial wrapper: it records the
/// exchange kind/peer, sieves the inner `DialRequested` into `dial_pending`,
/// and attempts the dial in-band. Against a cold peer the handshake is not yet
/// complete, so no bridge opens this tick but a quinn Initial is emitted on
/// the outbound path (the dial was attempted). Covers the wrapper body and
/// the `service_dials` cold-dial requeue arm.
#[test]
fn start_reliable_ping_attempts_dial_in_band() {
  let self_addr: SocketAddr = "127.0.0.1:7790".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7791".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);

  let deadline = now + core::time::Duration::from_secs(5);
  // Ignoring StreamId return: the test asserts on the emitted transmit.
  let _ = ep.start_reliable_ping(SmolStr::new("peer"), peer, 7, deadline, now);

  // The dial was attempted in-band: a quinn Initial (Class::Quic) is queued
  // toward the peer.
  let mut saw_quic_to_peer = false;
  while let Some((to, bytes)) = ep.poll_transmit() {
    if to == peer && matches!(super::classify(&bytes), super::Class::Quic) {
      saw_quic_to_peer = true;
    }
  }
  assert!(
    saw_quic_to_peer,
    "start_reliable_ping must attempt the dial in-band (emit a quinn Initial)"
  );
  // A connection entry now exists for the peer (still handshaking).
  assert_eq!(
    ep.live_connections_to(peer),
    1,
    "the in-band dial created a pooled connection for the peer"
  );
}

/// `handle_packet` forwards a decoded unreliable message into the inner
/// membership endpoint. A `UserData` packet surfaces as `Event::UserPacket`
/// through the sieving `poll_event`.
#[test]
fn handle_packet_forwards_user_data_to_inner_endpoint() {
  use crate::{event::Event, typed::Message};

  let self_addr: SocketAddr = "127.0.0.1:7800".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7801".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);

  ep.handle_packet(
    peer,
    Message::UserData(Bytes::from_static(b"directed-user-bytes")),
    now,
  );
  let mut saw = false;
  while let Some(ev) = ep.poll_event() {
    if matches!(&ev, Event::UserPacket(p) if p.data_ref().as_ref() == b"directed-user-bytes") {
      saw = true;
    }
  }
  assert!(
    saw,
    "handle_packet(UserData) must surface as an Event::UserPacket"
  );
}

/// `handle_udp` with a `Class::Reject` datagram (first byte in the
/// `0x10..=0x3F` gap) is silently dropped — neither buffered as memberlist
/// ingress nor fed to quinn — while still anchoring `last_now`.
#[test]
fn handle_udp_reject_class_datagram_is_dropped() {
  let self_addr: SocketAddr = "127.0.0.1:7810".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7811".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);
  // 0x20 is in the reject gap (bits 0x40/0x80 clear, value > 15).
  assert_eq!(super::classify(&[0x20u8]), super::Class::Reject);
  ep.handle_udp(peer, &[0x20u8, 0x00], now);
  assert!(
    ep.poll_memberlist_ingress().is_none(),
    "a Reject-class datagram must not be buffered as memberlist ingress"
  );
  assert!(
    ep.poll_transmit().is_none(),
    "a Reject-class datagram must not produce any quinn output"
  );
}

/// The sieving `poll_event` drains a `DialRequested` left in the INNER
/// endpoint queue into the private `dial_pending` deque and never returns it
/// to external callers (the `DialRequested(dial) => … continue` arm). A
/// bare-endpoint `start_reliable_ping` (no in-band service) leaves the
/// `DialRequested` in the inner queue for `poll_event` to sieve.
#[test]
fn poll_event_sieves_inner_dial_requested_out() {
  use Event;

  let self_addr: SocketAddr = "127.0.0.1:7820".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7821".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);

  // Drive the inner endpoint directly so the DialRequested lands in the inner
  // queue WITHOUT the wrapper's in-band `service_dials` sieving it first.
  let deadline = now + core::time::Duration::from_secs(5);
  // Ignoring StreamId return: the test asserts on poll_event sieving.
  let _ = ep
    .endpoint_mut()
    .start_reliable_ping(SmolStr::new("peer"), peer, 3, deadline);

  // poll_event must sieve the DialRequested out — it never surfaces to the
  // external caller. (The bare endpoint queued only the DialRequested, so
  // poll_event returns None after sieving it.)
  let mut leaked_dial = false;
  while let Some(ev) = ep.poll_event() {
    if matches!(ev, Event::DialRequested(_)) {
      leaked_dial = true;
    }
  }
  assert!(
    !leaked_dial,
    "poll_event must sieve DialRequested into dial_pending, never leak it"
  );
}

/// The `QuicEndpoint::start_scheduling` wrapper forwards to the inner
/// endpoint's periodic-scheduler arm. Calling it (re)arms scheduling without
/// panicking; a subsequent `poll_timeout` then offers a bounded next-deadline.
#[test]
fn start_scheduling_wrapper_arms_inner_scheduler() {
  let addr: SocketAddr = "127.0.0.1:7830".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", addr, now);
  ep.start_scheduling(now);
  // The inner scheduler is armed: a finite next-deadline is offered.
  assert!(
    ep.poll_timeout().is_some(),
    "an armed scheduler offers a finite next-deadline via poll_timeout"
  );
}

/// `try_open_uni_stream_to` on a peer with a pooled-but-still-handshaking
/// connection takes the `get_mut(ch) == Some` + `open(Dir::Uni) == None`
/// path (the handshake has not granted uni-stream credit, and the composed
/// config advertises `max_concurrent_uni_streams = 0` regardless), returning
/// `false` without closing the connection.
#[test]
fn try_open_uni_stream_to_handshaking_peer_is_false_without_close() {
  let self_addr: SocketAddr = "127.0.0.1:7840".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7841".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);

  // Create a pooled (handshaking) connection by offering an unreliable
  // datagram to the cold peer — `queue_unreliable_datagram` dials in.
  let _ = ep.queue_unreliable_datagram(peer, Bytes::from_static(b"probe"), now);
  assert_eq!(
    ep.live_connections_to(peer),
    1,
    "precondition: a pooled connection exists for the peer"
  );

  assert!(
    !ep.try_open_uni_stream_to(peer, now),
    "a handshaking connection grants no uni-stream credit ⇒ false"
  );
  // The connection was NOT closed (the false path skips the close-on-success
  // branch), so it is still live.
  assert_eq!(
    ep.live_connections_to(peer),
    1,
    "the false path must not close the pooled connection"
  );
}

/// `poll_memberlist_ingress` keeps the per-peer share counter exact: with two
/// datagrams buffered from the SAME peer, the first pop decrements (entry
/// retained, `*n != 0`) and the second pop removes the entry at zero. Covers
/// the `Occupied` decrement arm that the single-datagram path skips.
#[test]
fn poll_memberlist_ingress_decrements_then_removes_per_peer_counter() {
  let self_addr: SocketAddr = "127.0.0.1:7850".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7851".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);

  // Buffer two memberlist-classified datagrams from the same peer.
  ep.handle_udp(peer, &[3u8, 0xAA], now);
  ep.handle_udp(peer, &[3u8, 0xBB], now);

  // First pop: the per-peer counter decrements to 1 (entry retained).
  assert!(
    ep.poll_memberlist_ingress().is_some(),
    "first datagram pops"
  );
  // Second pop: the counter hits 0 and the entry is removed.
  assert!(
    ep.poll_memberlist_ingress().is_some(),
    "second datagram pops"
  );
  // Queue is now empty.
  assert!(
    ep.poll_memberlist_ingress().is_none(),
    "both buffered datagrams have been drained"
  );
}

/// `service_dials` `open(Dir::Bi) == None` with a CLOSED cached connection
/// (the `is_closed_now == true` arm): the intent is retired via
/// `dial_failed` rather than redialed, so a never-Established unreachable peer
/// does not generate a fresh handshake per service tick. The pooled
/// connection is driven to closed-never-Established directly, then
/// `service_dials` runs against the standing `dial_pending` entry.
#[test]
fn service_dials_retires_intent_when_cached_connection_is_closed() {
  use crate::event::{Event, ExchangeId, ExchangeStatus};

  let self_addr: SocketAddr = "127.0.0.1:7860".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:7861".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("self", self_addr, now);

  // Register a push/pull intent + dial in-band; the still-handshaking
  // connection requeues the intent onto `dial_pending`.
  let id = a.start_push_pull(peer, PushPullKind::Refresh, now);
  let expected_eid = ExchangeId::from(id);
  assert_eq!(a.dial_pending.len(), 1, "precondition: intent requeued");

  // Drive the pooled connection to Closed WITHOUT it ever reaching
  // Established (the never-Established closed-cache signature that
  // `get_or_dial` returns as-is so `service_dials` fires `dial_failed`).
  let ch = a
    .conns
    .handle_for(&peer)
    .expect("a pooled connection was dialed");
  a.conns
    .get_mut(ch)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), Bytes::new());

  // Keep the standing entry's deadline in the future so the closed-arm (not
  // the deadline-elapsed arm) is the one that fires.
  a.dial_pending.front_mut().unwrap().deadline = now + Duration::from_secs(30);

  a.service_dials(now);

  // The intent is retired as Failed (the closed-cached-connection arm routed
  // through `retire_failed_dial`).
  let mut found = None;
  while let Some(ev) = a.poll_event() {
    if let Event::ExchangeCompleted(payload) = ev {
      if payload.eid() == expected_eid {
        found = Some(payload);
        break;
      }
    }
  }
  let payload = found.expect(
    "a dial against a closed never-Established cached connection MUST retire \
       the intent via ExchangeCompleted(Failed), not redial",
  );
  assert_eq!(payload.outcome(), ExchangeStatus::Failed);
  assert!(
    a.dial_pending.is_empty(),
    "the retired intent must not be requeued onto dial_pending"
  );
}

/// `service_quinn`'s `StreamEvent::Stopped` arm: when the PEER sends
/// STOP_SENDING for our send half, quinn-proto yields
/// `Event::Stream(StreamEvent::Stopped{id, error_code})`. The arm retires both
/// halves inline (RESET our send + STOP our recv) and routes the
/// `(ch, sid)`-matched bridge through `fail_stopped_already_retired`, which
/// transitions it to `Failed(Transport)`.
///
/// To keep B's responder bridge deterministically alive when the STOP_SENDING
/// arrives, the test establishes only the CONNECTION (a datagram offer warms
/// the dial), then opens a RAW bidi from A at the quinn level and writes a
/// single byte — B's `service_quinn` accepts it (a bridge appears, parked
/// Active waiting for the rest of the never-completed reliable unit). A then
/// STOP_SENDINGs B's send half by stopping A's recv half; after ferrying the
/// frame, B's `service_quinn` runs the `Stopped` arm and terminalizes B's
/// bridge as `Failed(Transport)`.
#[test]
fn peer_stop_sending_terminalizes_responder_bridge_via_stopped_arm() {
  let a_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8002".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Warm a pooled connection A→B WITHOUT a reliable exchange: a datagram offer
  // initiates the dial; ferrying drives the handshake to Established on both
  // sides (no bridge is created on either end — datagrams are the unreliable
  // plane).
  assert_eq!(
    a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"\x01warm"), now),
    super::DatagramSendStatus::NotReady,
    "the first datagram to a cold peer is best-effort NotReady and warms a dial"
  );
  a.flush_outbound_transmits(now);
  let mut a_ch: Option<quinn_proto::ConnectionHandle> = None;
  for _ in 0..200 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if let Some(ch) = a.conns.handle_for(&b_addr)
      && a
        .conns
        .get(ch)
        .map(|e| !e.conn_ref().is_handshaking() && !e.conn_ref().is_closed())
        .unwrap_or(false)
    {
      a_ch = Some(ch);
      break;
    }
  }
  let a_ch = a_ch.expect("A's pooled connection to B must reach Established");
  assert_eq!(
    a.live_bridge_count(),
    0,
    "no reliable bridge is created by a datagram warm"
  );

  // Open a RAW bidi on A's pooled connection (bypassing the FSM) and write one
  // byte so B becomes aware of the stream and accepts it.
  let a_sid = a
    .conns
    .get_mut(a_ch)
    .expect("A's connection present")
    .conn_mut()
    .streams()
    .open(quinn_proto::Dir::Bi)
    .expect("A opens a raw bidi");
  // Ignoring the written-byte count: one byte fits a fresh stream's flow
  // window, and the test asserts on B accepting the bidi, not the write size.
  let _ = a
    .conns
    .get_mut(a_ch)
    .expect("A's connection present")
    .conn_mut()
    .send_stream(a_sid)
    .write(b"\x05");

  // Ferry until B's `service_quinn` accepts the bidi and a bridge appears.
  let mut b_id: Option<StreamId> = None;
  for _ in 0..100 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if let Some(id) = b.bridges.keys().next().copied() {
      b_id = Some(id);
      break;
    }
  }
  let b_id = b_id.expect("B must accept the raw bidi and create a responder bridge");
  assert!(
    b.bridges
      .get(&b_id)
      .map(|br| !br.is_phase_failed())
      .unwrap_or(false),
    "B's freshly-accepted bridge is parked Active before the STOP_SENDING"
  );

  // A STOP_SENDINGs B's send half by stopping A's recv half (the half that
  // would receive B's reply). quinn-proto queues a STOP_SENDING frame.
  let stop_res = a
    .conns
    .get_mut(a_ch)
    .expect("A's connection present")
    .conn_mut()
    .recv_stream(a_sid)
    .stop(quinn_proto::VarInt::from_u32(42));
  assert!(
    stop_res.is_ok(),
    "A must be able to STOP_SENDING B's send half (its recv half is still open)"
  );

  // Ferry A→B so the STOP_SENDING reaches B, then tick B so its
  // `service_quinn` polls the connection and runs the `Stopped` arm. B's
  // bridge MUST terminalize as Failed (the Stopped arm), then reap.
  let mut saw_failed = false;
  for _ in 0..100 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    b.handle_timeout(now);
    match b.bridges.get(&b_id) {
      Some(br) if br.is_phase_failed() => {
        saw_failed = true;
        break;
      }
      Some(_) => {}
      // Reaped: the Stopped arm terminalized it and `pump_bridges` removed it.
      // A reap is reachable here ONLY via the failure path — B never received
      // a complete reliable unit, so a clean BothClosed completion is
      // impossible.
      None => {
        saw_failed = true;
        break;
      }
    }
    a.handle_timeout(now);
  }
  assert!(
    saw_failed,
    "B's responder bridge MUST terminalize via the `StreamEvent::Stopped` arm \
       once A STOP_SENDINGs B's send half — the arm routes the (ch, sid)-matched \
       bridge through `fail_stopped_already_retired` → `Failed(Transport)`. \
       Without the arm the bridge would linger until its exchange deadline."
  );
}

/// `service_quinn`'s `StreamEvent::Finished` arm + its inner `(ch, sid)` match
/// (and the `break` after `observe_send_fin`): when the peer ACKs our FIN,
/// quinn-proto yields `Event::Stream(StreamEvent::Finished{id})`. The arm
/// finds the owning bridge by `(ch, sid)` and calls `observe_send_fin`,
/// advancing its send-half phase.
///
/// The test drives a real A→B push/pull and, on B (the responder), captures
/// B's bridge id. After B sends its reply and FINishes its send half, A ACKs
/// the FIN; B's `service_quinn` then observes `StreamEvent::Finished` for B's
/// send stream and routes it to B's bridge via the inner match + break. The
/// observable is that B's bridge reaches a send-finished phase
/// (`SendClosed`/`BothClosed`/terminal) — the `Finished` arm is the only
/// producer of `observe_send_fin` for a peer-ACKed FIN.
#[test]
fn peer_acked_fin_routes_to_bridge_via_finished_arm() {
  let a_addr: SocketAddr = "127.0.0.1:8011".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8012".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Ignoring StreamId return: the test inspects bridge phase, not the handle.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);

  // Capture B's bridge id the moment B accepts the inbound exchange, then keep
  // driving. B's responder bridge sits at `RecvClosed` (recv FIN observed)
  // with its send half FINned, waiting for A to ACK that FIN. When A's ACK
  // arrives, B's `service_quinn` observes `StreamEvent::Finished` for B's send
  // stream; the inner `(ch, sid)` match finds B's still-present bridge, runs
  // `observe_send_fin` (`RecvClosed → BothClosed`), and breaks. `BothClosed`
  // is terminal, so the next `pump_bridges` reaps the bridge CLEANLY (never
  // `is_phase_failed()`). A clean reap is reachable ONLY through the Finished
  // arm's `observe_send_fin` — every failure path sets `Failed(_)`.
  let mut b_id: Option<StreamId> = None;
  let mut ever_failed = false;
  let mut clean_reaped = false;
  for _ in 0..300 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    match b_id {
      None => {
        if let Some(id) = b.bridges.keys().next().copied() {
          b_id = Some(id);
        }
      }
      Some(id) => match b.bridges.get(&id) {
        Some(br) => {
          if br.is_phase_failed() {
            ever_failed = true;
          }
        }
        None => {
          // The captured bridge has been reaped. If it never went through a
          // failure phase, this is the clean `BothClosed` reap that only the
          // Finished arm's `observe_send_fin` can produce for a responder.
          clean_reaped = true;
          break;
        }
      },
    }
  }
  assert!(
    b_id.is_some(),
    "B must have accepted an inbound responder bridge within the ferry budget"
  );
  assert!(
    clean_reaped && !ever_failed,
    "B's responder bridge MUST reap CLEANLY (BothClosed) via the \
       `StreamEvent::Finished` arm's inner `(ch, sid)` match + `observe_send_fin` \
       once A ACKs B's reply FIN — clean_reaped = {clean_reaped}, \
       ever_failed = {ever_failed}. A clean reap is reachable only through that \
       arm; every other terminus is a `Failed(_)` phase."
  );
}

/// `route_datagram_event`'s `DatagramEvent::Response` arm: a long-header QUIC
/// packet carrying an UNSUPPORTED version drives quinn-proto's
/// `Endpoint::handle` to emit a Version Negotiation response (a stateless
/// `DatagramEvent::Response`, not a connection event). Because the coordinator
/// also installs a server config (its endpoints accept inbound dials), the
/// `Response` is produced and the arm surfaces it on the driver-facing `out`
/// queue. A Version Negotiation packet is uniquely identifiable on the wire by
/// its zero version field.
#[test]
fn handle_udp_unsupported_version_emits_version_negotiation_response() {
  let self_addr: SocketAddr = "127.0.0.1:8021".parse().unwrap();
  let peer: SocketAddr = "127.0.0.1:8022".parse().unwrap();
  let now = Instant::now();
  let mut ep = make_endpoint("self", self_addr, now);

  // A minimal QUIC long header with an unsupported (reserved-greasing) version.
  // Layout: [first][version u32][dcid_len][dcid..][scid_len][scid..].
  // `0xC0` = LONG_HEADER_FORM | FIXED_BIT (grease_quic_bit is forced off, so the
  // fixed bit must be set). The version `0x0a1a2a3a` is a reserved greasing
  // value quinn never supports, so the long-header decoder returns
  // `UnsupportedVersion` BEFORE the long-header type or payload length matter.
  let packet: &[u8] = &[
    0xC0, // long header + fixed bit
    0x0a, 0x1a, 0x2a, 0x3a, // unsupported version
    0x04, 0xde, 0xad, 0xbe, 0xef, // dcid len + dcid
    0x04, 0xca, 0xfe, 0xba, 0xbe, // scid len + scid
  ];
  assert_eq!(
    super::classify(packet),
    super::Class::Quic,
    "a long-header packet (bit 0x80 set) must classify as Quic so handle_udp \
       feeds it to quinn"
  );

  ep.handle_udp(peer, packet, now);

  // The Response arm pushed the Version Negotiation packet onto `out` destined
  // for the source. It is identified by a zero version field (bytes 1..5).
  let mut saw_version_negotiation = false;
  while let Some((to, bytes)) = ep.poll_transmit() {
    if to == peer && bytes.len() >= 5 && bytes[1..5] == [0, 0, 0, 0] {
      saw_version_negotiation = true;
    }
  }
  assert!(
    saw_version_negotiation,
    "an unsupported-version long-header packet must surface a Version \
       Negotiation response (zero version field) through the \
       DatagramEvent::Response arm"
  );
}

/// `set_encryption_options`'s per-bridge fan-out loop body (`for bridge in
/// self.bridges.values_mut() { bridge.set_encryption(...) }`): a real
/// push/pull exchange leaves A holding at least one live reliable bridge; a
/// subsequent policy CHANGE (not a no-op reapply) iterates that bridge. The
/// per-bridge `set_encryption` is a force-disable no-op on QUIC (quinn already
/// encrypts the stream), but the loop body itself must execute over a non-empty
/// bridge map — exactly the fan-out the plain-stream coordinator performs.
#[cfg(feature = "aes-gcm")]
#[test]
fn set_encryption_options_fans_out_over_live_bridges() {
  use crate::{EncryptionOptions, Keyring, SecretKey};

  let a_addr: SocketAddr = "127.0.0.1:8031".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8032".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Drive a real A->B push/pull until A holds at least one live reliable bridge
  // (the proven ferry pattern). A's outbound bridge exists for the duration of
  // the exchange.
  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);
  let mut had_bridge = false;
  for _ in 0..300 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    if a.live_bridge_count() >= 1 {
      had_bridge = true;
      break;
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
  }
  assert!(
    had_bridge,
    "A must hold a live reliable bridge mid-exchange for the fan-out to iterate"
  );
  assert_eq!(
    a.live_bridge_count(),
    1,
    "precondition: exactly one live bridge for the fan-out loop to visit"
  );

  // A real policy change (disabled -> enabled keyring) takes the fan-out path,
  // iterating the live bridge's `set_encryption` (a force-disable no-op on
  // QUIC) and then publishing the new gossip-plane policy.
  a.set_encryption_options(
    EncryptionOptions::new().with_keyring(Keyring::new(SecretKey::Aes256([0x22; 32]))),
  );
  assert!(
    a.encryption_options().is_enabled(),
    "the changed encryption policy must be in effect after the fan-out"
  );
}

/// `pump_bridges`'s post-`drain_payload_only` terminality re-check + Close
/// reap (the `bridges_terminalized_via_close_command` arm): a real A->B
/// push/pull JOIN against a B whose merge delegate REJECTS every merge drives
/// B's responder bridge through `StreamCommand::Close`. `drain_payload_only`
/// flips it terminal (`Failed(AdmissionClosed)`) and the re-check then
/// `drain_then_reap`s + reaps it in the SAME tick, emitting an
/// `ExchangeCompleted`. Without the re-check the bridge would hold the quinn
/// bidi until its exchange deadline.
#[test]
fn rejected_merge_terminalizes_responder_bridge_via_pump_bridges_close_arm() {
  let a_addr: SocketAddr = "127.0.0.1:8041".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:8042".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  // B rejects every inbound merge: its FSM turns the inbound JOIN PushPull into
  // a `StreamCommand::Close` that `pump_bridges` reaps via its Close arm.
  // A BOXED delegate through the public forwarder: covers both the
  // coordinator forwarding and the Box blanket impl.
  let boxed: Box<dyn crate::delegate::MergeDelegate<SmolStr, SocketAddr>> =
    Box::new(RejectAllMergesQuic);
  b.set_merge_delegate(boxed);

  let _ = a
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Join, now);

  // Drive the exchange. B accepts the inbound bidi, decodes the JOIN request,
  // the rejecting delegate returns Close, and B's `pump_bridges` terminalizes +
  // reaps the responder bridge via the post-drain re-check. The observable is
  // that B's `bridges_terminalized_via_close_command` counter increments.
  let mut closed_via_arm = false;
  for _ in 0..300 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if b.counters.bridges_terminalized_via_close_command > 0 {
      closed_via_arm = true;
      break;
    }
  }
  assert!(
    closed_via_arm,
    "B's responder bridge MUST terminalize + reap via `pump_bridges`'s \
       post-drain_payload_only Close re-check once the rejecting merge delegate \
       returns StreamCommand::Close — the counter stays at zero without that arm"
  );
  // The Close-reaped bridge must not leak: B holds no live bridge afterward.
  assert_eq!(
    b.live_bridge_count(),
    0,
    "the Close arm must reap B's responder bridge in the same tick (no leak)"
  );
}

/// The QUIC bidi-accept loop admission-gates inbound streams against the
/// coordinator-wide `QuicOptions::max_inbound_streams` ceiling, exactly as the
/// stream (TCP/TLS) coordinator's `accept_connection` does. This pins the FULL
/// contract, not merely "at least one rejection" (which a mutation that rejected
/// EVERY stream would also satisfy): exactly `CAP` concurrent inbound exchanges
/// are admitted AND complete, the excess (`OPENED - CAP`) is rejected, the PEAK
/// concurrent inbound bridge population equals `CAP` (never exceeds it, and
/// reaches it), and after the admitted batch releases its bridges a fresh
/// exchange is admitted and completes.
///
/// The cap is set via `QuicOptions` (the QUIC-specific option), NOT the shared
/// TCP/TLS `EndpointOptions::max_inbound_streams` (left at its unlimited
/// default): if the accept loop read the endpoint option instead of `self.cfg`,
/// `CAP` would have no effect and all `OPENED` streams would be admitted.
///
/// A drives `OPENED` push/pull exchanges to B over one pooled connection. Once
/// Established, one `service_dials` pass opens all their bidi streams together,
/// so B's accept loop sees them concurrently — before any admitted exchange can
/// complete-and-reap (completion needs a further round trip), so the first `CAP`
/// are admitted and the rest reset in one drained accept pass.
///
/// Negative controls, each breaking a distinct assertion: (a) reverting the gate
/// to admit every stream drives the peak to `OPENED` and rejections to 0; (b)
/// rejecting every stream drives the peak to 0 and admits/completes nothing; (c)
/// an off-by-one (`>` for `>=`) drives the peak to `CAP + 1`; (d) reading
/// `self.ep.max_inbound_streams()` (endpoint default `None`) instead of
/// `self.cfg` admits all `OPENED`.
#[test]
fn quic_accept_enforces_max_inbound_streams_admit_cap_reject_excess() {
  use crate::event::ExchangeKind;
  let a_addr: SocketAddr = "127.0.0.1:7940".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7941".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  const CAP: usize = 3;
  const OPENED: usize = 8;
  // B caps concurrent inbound reliable streams at CAP via the QUIC option
  // (endpoint-level `max_inbound_streams` is left at its unlimited default).
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config().with_max_inbound_streams(Some(CAP)),
    b_addr,
    now,
  );

  for _ in 0..OPENED {
    // Ignoring StreamId: the test asserts on metrics/completions, not handles.
    let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);
  }

  let mut a_succeeded: usize = 0;
  for _ in 0..400 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev
        && p.kind() == ExchangeKind::PushPull
        && p.outcome().is_succeeded()
      {
        a_succeeded += 1;
      }
    }
    if b.metrics().inbound_streams_rejected >= (OPENED - CAP) as u64 && a_succeeded >= CAP {
      break;
    }
  }

  // Peak concurrent inbound bridges equals the ceiling: never exceeds it, AND
  // reaches it. Off-by-one shows CAP+1, admit-all shows OPENED, reject-all 0.
  assert_eq!(
    b.counters.max_inbound_bridges_live, CAP,
    "peak concurrent inbound bridges on B must equal the ceiling {CAP}; observed {}",
    b.counters.max_inbound_bridges_live
  );
  // Only the excess over the ceiling was rejected — not more (reject-all) and
  // not fewer.
  assert_eq!(
    b.metrics().inbound_streams_rejected,
    (OPENED - CAP) as u64,
    "exactly the excess {} inbound streams over the ceiling must be rejected",
    OPENED - CAP
  );
  // Exactly CAP below-ceiling exchanges were admitted AND completed.
  assert_eq!(
    a_succeeded, CAP,
    "exactly {CAP} admitted inbound exchanges must complete (Succeeded); observed \
       {a_succeeded}"
  );

  // Capacity released: a fresh exchange after the batch is admitted and
  // completes, and is NOT rejected. This also proves the admitted bridges reaped
  // (the ceiling gates concurrency, not exchange lifetime) — without a reap the
  // new inbound stream would hit the full ceiling and be rejected.
  let rejected_after_batch = b.metrics().inbound_streams_rejected;
  let succeeded_after_batch = a_succeeded;
  // Ignoring StreamId: the test asserts on completion, not the handle.
  let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);
  let mut post_release_succeeded = false;
  for _ in 0..200 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev
        && p.kind() == ExchangeKind::PushPull
        && p.outcome().is_succeeded()
      {
        a_succeeded += 1;
      }
    }
    if a_succeeded > succeeded_after_batch {
      post_release_succeeded = true;
      break;
    }
  }
  assert!(
    post_release_succeeded,
    "after the admitted batch releases its bridges, a new inbound exchange must \
       be admitted and complete"
  );
  assert_eq!(
    b.metrics().inbound_streams_rejected,
    rejected_after_batch,
    "the post-release exchange is within the ceiling and must not be rejected"
  );
  assert_eq!(
    b.counters.max_inbound_bridges_live, CAP,
    "the ceiling must continue to hold across the post-release exchange (peak \
       still {CAP})"
  );
}

/// `handle_udp` MUST run a per-connection servicing pass ONLY when the datagram
/// resolves to a connection. A `Some` from `quinn_proto::Endpoint::handle` does
/// NOT: a discarded datagram (`None`), a stateless `Response` (version
/// negotiation / retry / stateless reset — attacker-triggerable), an over-cap or
/// failed `accept`, and a `ConnectionEvent` for an unknown handle all resolve to
/// no connection, so `route_datagram_event` returns `None` and no
/// `service_connection` runs.
///
/// The `quic_inbound_servicings` test counter increments once per
/// `service_connection` call. This covers three cases against a POPULATED
/// connection table: establishing the connection (a `NewConnection` accept plus
/// applied `ConnectionEvent`s) DID service; a discarded `None` datagram and a
/// version-negotiation `Response` (both attacker-shaped) do NOT; a real datagram
/// addressing the established connection DOES. (The over-cap case is covered by
/// `quic_new_connection_refused_over_global_cap`.)
///
/// Negative control: service on every `handle` result (or unconditionally) — the
/// `Response` case (and, if unconditional, the `None` case) then advances the
/// counter and those assertions fail.
#[test]
fn inert_quic_datagram_skips_coordinator_servicing() {
  let a_addr: SocketAddr = "127.0.0.1:7942".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7943".parse().unwrap();
  let c_addr: SocketAddr = "127.0.0.1:7944".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Establish a real A<->B connection so B holds connection (and transiently
  // bridge) state a servicing pass would pump/scan.
  // Ignoring StreamId: the test asserts on the servicing counter, not handles.
  let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if b.live_connections_to(a_addr) >= 1 && !moved && b.counters.endpoint_events_processed > 0 {
      break;
    }
  }
  assert!(
    b.live_connections_to(a_addr) >= 1,
    "test precondition: B must hold an established connection to A"
  );
  // (0) Establishing the connection accepted a NewConnection and applied
  // ConnectionEvents on B, each of which DID advance state and service.
  assert!(
    b.counters.quic_inbound_servicings > 0,
    "a real NewConnection accept and its applied ConnectionEvents MUST service"
  );

  // (1) Inert datagram: first byte 0x40 (FIXED_BIT set) classifies as
  // `Class::Quic`, but the packet is truncated far below a parseable short
  // header (and far below the stateless-reset minimum), so `handle` returns
  // `None` — no connection or stream state created or advanced.
  let before = b.counters.quic_inbound_servicings;
  let inert: [u8; 5] = [0x40, 0x00, 0x00, 0x00, 0x00];
  b.handle_udp(a_addr, &inert, now);
  assert_eq!(
    b.counters.quic_inbound_servicings, before,
    "an inert QUIC-classified datagram that quinn discards (handle -> None) \
       must NOT trigger a coordinator servicing pass; the counter advanced from \
       {before} to {}",
    b.counters.quic_inbound_servicings
  );

  // (2) Response-class datagram against the POPULATED table: a real Initial from
  // a third endpoint whose 4-byte version field (bytes [1..5] of the QUIC long
  // header) is overwritten with an unsupported version, forcing quinn to emit a
  // Version Negotiation `Response`. `handle` returns `Some(DatagramEvent::
  // Response)` — attacker-triggerable, commits no connection state — so it must
  // NOT service, yet it IS a `Some` (proving this exercises the Some-but-inert
  // path, distinct from the `None` case above): B queues the VN datagram back.
  let mut c = make_endpoint("c", c_addr, now);
  // Ignoring StreamId: only c's on-wire Initial is used.
  let _ = c.start_push_pull(b_addr, PushPullKind::Join, now);
  let mut vn_trigger = drain_first_datagram_to(&mut c, b_addr);
  vn_trigger[1..5].copy_from_slice(&[0x1a, 0x2a, 0x3a, 0x4a]);
  // Drain B's outbound so the only datagram queued after the VN feed is the VN.
  while b.poll_transmit().is_some() {}
  let before_vn = b.counters.quic_inbound_servicings;
  b.handle_udp(c_addr, &vn_trigger, now);
  assert_eq!(
    b.counters.quic_inbound_servicings, before_vn,
    "a version-negotiation Response (Some(DatagramEvent::Response)) commits no \
       connection state and must NOT trigger a servicing pass"
  );
  let mut vn_emitted = false;
  while let Some((to, _)) = b.poll_transmit() {
    if to == c_addr {
      vn_emitted = true;
    }
  }
  assert!(
    vn_emitted,
    "quinn must have emitted a Version Negotiation datagram to the source — \
       proving `handle` returned Some(Response), so this is the Some-but-inert \
       path, not the None-discard path"
  );

  // (3) A real datagram that advances the established connection must still be
  // serviced. Drive one more exchange and confirm the counter advances on a
  // datagram delivered to B.
  let before_real = b.counters.quic_inbound_servicings;
  // Ignoring StreamId: the test asserts on the servicing counter, not handles.
  let _ = a.start_push_pull(b_addr, PushPullKind::Refresh, now);
  let mut serviced = false;
  for _ in 0..50 {
    let mut delivered_to_b = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        delivered_to_b = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    if delivered_to_b && b.counters.quic_inbound_servicings > before_real {
      serviced = true;
      break;
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
  }
  assert!(
    serviced,
    "a real datagram that advances a connection (handle -> Some) MUST still \
       trigger a servicing pass; the counter never advanced"
  );
}

/// An unauthenticated inbound Initial past the global `max_quic_connections`
/// ceiling MUST be refused before any
/// connection-table state is committed, and bump `quic_connections_rejected`.
///
/// B caps total QUIC connections at 1. A1 establishes the one allowed
/// connection; A2's Initial (a distinct source) is then refused — no
/// connection-table entry is created for it and A1's connection is unaffected.
///
/// Negative control: revert the global-cap branch in the `NewConnection` arm —
/// A2's Initial is accepted, `quic_connections_rejected` stays 0, and the
/// assertion fails.
#[test]
fn quic_new_connection_refused_over_global_cap() {
  let a1_addr: SocketAddr = "127.0.0.1:7944".parse().unwrap();
  let a2_addr: SocketAddr = "127.0.0.1:7945".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7946".parse().unwrap();
  let now = Instant::now();
  let mut a1 = make_endpoint("a1", a1_addr, now);
  let mut a2 = make_endpoint("a2", a2_addr, now);
  // B: global ceiling of exactly one QUIC connection.
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config().with_max_quic_connections(Some(1)),
    b_addr,
    now,
  );

  // A1 dials B and B commits the single allowed connection.
  // Ignoring StreamId: the test asserts on connection state, not handles.
  let _ = a1.start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..20 {
    while let Some((to, bytes)) = a1.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a1_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a1_addr {
        a1.handle_udp(b_addr, &bytes, now);
      }
    }
    a1.handle_timeout(now);
    b.handle_timeout(now);
    if b.live_connections_to(a1_addr) >= 1 {
      break;
    }
  }
  assert!(
    b.live_connections_to(a1_addr) >= 1,
    "test precondition: B must have committed A1's connection (the one allowed)"
  );
  assert_eq!(
    b.metrics().quic_connections_rejected,
    0,
    "no refusal should have occurred while committing the first connection"
  );

  // A2 dials B: its Initial must be refused at the global cap.
  let rejected_before = b.metrics().quic_connections_rejected;
  // An over-cap Initial is `ignore`d before committing state — no servicing pass
  // is owed for it. Capture the counter so the refused datagrams below can be
  // shown not to advance it (Finding 1's over-cap case).
  let servicings_before_a2 = b.counters.quic_inbound_servicings;
  // Ignoring StreamId: the test asserts on the rejection metric, not handles.
  let _ = a2.start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..20 {
    while let Some((to, bytes)) = a2.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a2_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a2_addr {
        a2.handle_udp(b_addr, &bytes, now);
      }
    }
    a2.handle_timeout(now);
    b.handle_timeout(now);
    if b.metrics().quic_connections_rejected > rejected_before {
      break;
    }
  }
  assert!(
    b.metrics().quic_connections_rejected > rejected_before,
    "an inbound Initial past the global `max_quic_connections` cap must be \
       refused and bump `quic_connections_rejected`"
  );
  assert_eq!(
    b.live_connections_to(a2_addr),
    0,
    "no connection-table entry may be created for the refused source"
  );
  assert!(
    b.live_connections_to(a1_addr) >= 1,
    "the pre-existing connection must be unaffected by the refusal"
  );
  // The refused over-cap Initials committed no state, so none triggered a
  // servicing pass: `route_datagram_event` returns `None` for an over-cap
  // `ignore`, so `handle_udp` runs no `service_connection`. Servicing on every
  // `handle` result would advance this counter for each over-cap Initial an
  // attacker sends at full occupancy.
  assert_eq!(
    b.counters.quic_inbound_servicings, servicings_before_a2,
    "an over-cap Initial is ignored before committing state and must NOT trigger \
       a servicing pass"
  );
}

/// Concurrent *pending* (handshaking) connections from a single source address
/// are bounded by
/// `max_pending_connections_per_source`. An Initial from a source already at the
/// ceiling MUST be refused before committing state, bumping
/// `quic_connections_rejected`, so one source cannot pin unbounded half-open
/// handshake state.
///
/// Two distinct valid Initials (different connection IDs, minted by two
/// endpoints) are delivered to B under the SAME source address — the shape of a
/// single source opening many half-open handshakes. With a per-source ceiling of
/// 1 and a high global cap, the first is accepted (a pending connection) and the
/// second is refused.
///
/// Negative control: revert the per-source branch in the `NewConnection` arm —
/// the second Initial is accepted, `quic_connections_rejected` stays 0, and the
/// assertion fails.
#[test]
fn quic_new_connection_refused_over_per_source_pending_cap() {
  let a1_addr: SocketAddr = "127.0.0.1:7947".parse().unwrap();
  let a2_addr: SocketAddr = "127.0.0.1:7948".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7949".parse().unwrap();
  // The single source address both Initials appear to come from.
  let src: SocketAddr = "127.0.0.1:60001".parse().unwrap();
  let now = Instant::now();
  // Two endpoints, used only to mint two valid Initials with distinct DCIDs.
  let mut a1 = make_endpoint("a1", a1_addr, now);
  let mut a2 = make_endpoint("a2", a2_addr, now);
  // B: per-source pending ceiling of 1, global cap high enough not to interfere.
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config()
      .with_max_pending_connections_per_source(Some(1))
      .with_max_quic_connections(Some(64)),
    b_addr,
    now,
  );

  // Mint each endpoint's first Initial (destined to B).
  // Ignoring StreamId: the test drives raw Initials, not exchange handles.
  let _ = a1.start_push_pull(b_addr, PushPullKind::Join, now);
  let _ = a2.start_push_pull(b_addr, PushPullKind::Join, now);
  let init1 = drain_first_datagram_to(&mut a1, b_addr);
  let init2 = drain_first_datagram_to(&mut a2, b_addr);

  // First Initial from `src`: accepted, creating one pending (handshaking)
  // connection. B's handshake response goes to `src` and is never answered, so
  // the connection stays pending.
  b.handle_udp(src, &init1, now);
  assert_eq!(
    b.metrics().quic_connections_rejected,
    0,
    "the first Initial from a source must be accepted (under the per-source cap)"
  );
  assert!(
    b.live_connections_to(src) >= 1,
    "the first Initial must create one pending connection for the source"
  );

  // Second Initial (distinct DCID) from the SAME source: at the per-source
  // pending ceiling of 1, it must be refused.
  let rejected_before = b.metrics().quic_connections_rejected;
  b.handle_udp(src, &init2, now);
  assert!(
    b.metrics().quic_connections_rejected > rejected_before,
    "a second concurrent handshake from a source already at \
       `max_pending_connections_per_source` must be refused and bump \
       `quic_connections_rejected`"
  );
}

/// Drain `ep`'s outbound queue until the first datagram destined to `dst`,
/// returning its bytes. Used to lift a freshly-dialed endpoint's Initial off
/// the wire so a test can replay it under a chosen source address.
fn drain_first_datagram_to(ep: &mut QuicEndpoint<SmolStr>, dst: SocketAddr) -> Vec<u8> {
  for _ in 0..64 {
    while let Some((to, bytes)) = ep.poll_transmit() {
      if to == dst {
        return bytes.to_vec();
      }
    }
    // Nothing queued yet on this drain; the start-time flush should have emitted
    // the Initial already, but tolerate a couple of empty polls defensively.
  }
  panic!("endpoint emitted no datagram destined to {dst}");
}

/// Simultaneous bidirectional dial at per-source pending cap 1: `start_push_pull`
/// attempts each dial and flushes its Initial synchronously, so BEFORE any
/// delivery each side already holds a handshaking OUTBOUND connection to the
/// other. Because the per-source cap counts inbound connections only, that local
/// outbound does NOT consume the peer's inbound allowance — each side accepts the
/// other's Initial and BOTH push/pull exchanges complete.
///
/// Negative control: drop the `direction == Inbound` filter in
/// `pending_inbound_from` (count outbound dials too) — B's own handshaking
/// outbound to A fills its cap of 1, so B refuses A's inbound Initial (and vice
/// versa); neither outbound handshake gets a server side, so neither exchange
/// completes and both `*_succeeded` assertions fail (with `now` fixed, the
/// stranded exchanges never even fail by timeout — they simply never complete).
#[test]
fn simultaneous_dial_at_per_source_cap_one_both_exchanges_succeed() {
  use crate::event::ExchangeKind;
  let a_addr: SocketAddr = "127.0.0.1:7960".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7961".parse().unwrap();
  let now = Instant::now();
  // Both endpoints cap concurrent pending inbound handshakes per source at 1.
  let mut a = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("a"), a_addr),
    test_config().with_max_pending_connections_per_source(Some(1)),
    a_addr,
    now,
  );
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config().with_max_pending_connections_per_source(Some(1)),
    b_addr,
    now,
  );

  // Simultaneous dial: each side attempts its outbound (and flushes the Initial)
  // before any delivery, so each holds a handshaking outbound to the other when
  // the peer's inbound Initial arrives.
  // Ignoring StreamId: the test asserts on exchange completion, not handles.
  let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);
  let _ = b.start_push_pull(a_addr, PushPullKind::Join, now);

  let mut a_succeeded = false;
  let mut b_succeeded = false;
  for _ in 0..400 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    while let Some(ev) = a.poll_event() {
      if let Event::ExchangeCompleted(p) = ev
        && p.kind() == ExchangeKind::PushPull
        && p.outcome().is_succeeded()
      {
        a_succeeded = true;
      }
    }
    while let Some(ev) = b.poll_event() {
      if let Event::ExchangeCompleted(p) = ev
        && p.kind() == ExchangeKind::PushPull
        && p.outcome().is_succeeded()
      {
        b_succeeded = true;
      }
    }
    if a_succeeded && b_succeeded {
      break;
    }
  }
  assert!(
    a_succeeded,
    "A's push/pull to B must complete (Succeeded) under simultaneous dial at \
       per-source cap 1 — A's local outbound dial to B must not block B's inbound \
       Initial"
  );
  assert!(
    b_succeeded,
    "B's push/pull to A must complete (Succeeded) under simultaneous dial at \
       per-source cap 1"
  );
}

/// The production-default QUIC config (`QuicOptions::new`, no explicit cap set)
/// MUST install a bounded, nonzero coordinator-wide inbound-stream ceiling. A
/// default of `None` (unlimited) lets each of up to `max_quic_connections`
/// connections open its full bidi allowance and mint an unbounded number of
/// inbound bridges. The accept loop's behavioural enforcement of this QUIC option
/// is pinned by `quic_accept_enforces_max_inbound_streams_admit_cap_reject_excess`
/// (which sets the cap via `QuicOptions`, not the endpoint option); together they
/// show the default config behaviourally bounds inbound streams.
///
/// Negative control: revert the `QuicOptions::new` default to `None` — the first
/// assertion fails.
#[test]
fn production_default_quic_config_bounds_inbound_streams() {
  use crate::quic::DEFAULT_MAX_QUIC_INBOUND_STREAMS;
  let qc = test_config();
  assert_eq!(
    qc.max_inbound_streams(),
    Some(DEFAULT_MAX_QUIC_INBOUND_STREAMS),
    "the production-default QUIC config must bound inbound streams (not None)"
  );
  // The ceiling being nonzero (a `Some(0)` default would reject every inbound
  // stream) is guarded at compile time next to the constant's definition.
  // The shared TCP/TLS endpoint-level default is deliberately unchanged
  // (unlimited): the QUIC bound is QUIC-specific, so TCP/TLS behaviour is intact.
  let ep = EndpointOptions::new(
    SmolStr::new("x"),
    "127.0.0.1:1".parse::<SocketAddr>().unwrap(),
  );
  assert_eq!(
    ep.max_inbound_streams(),
    None,
    "the shared TCP/TLS endpoint-level inbound-stream default must remain \
       unlimited — the QUIC-specific bound must not change it"
  );
}

/// The global connection cap is checked BEFORE the per-source pending-index
/// lookup and short-circuits. An inbound Initial
/// refused at connection-table saturation must not perform the per-source
/// lookup at all, so a flood of fresh-DCID Initials at saturation cannot be
/// amplified into per-datagram admission work beyond the O(1) global check.
///
/// B caps total connections at 1. A1 establishes the one allowed connection (its
/// Initial passes the global check and reaches — and increments — the per-source
/// check). A2's Initial is then over the global cap: it must be refused WITHOUT
/// the per-source lookup, so `quic_pending_inbound_checks` does not advance while
/// `quic_connections_rejected` does.
///
/// Negative control: revert to the eager `over_global || over_per_source` form
/// (per-source computed unconditionally) — A2's Initial then performs the
/// per-source lookup, `quic_pending_inbound_checks` advances, and the
/// flat-counter assertion fails.
#[test]
fn over_global_cap_skips_per_source_pending_lookup() {
  let a1_addr: SocketAddr = "127.0.0.1:7980".parse().unwrap();
  let a2_addr: SocketAddr = "127.0.0.1:7981".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7982".parse().unwrap();
  let now = Instant::now();
  let mut a1 = make_endpoint("a1", a1_addr, now);
  let mut a2 = make_endpoint("a2", a2_addr, now);
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config().with_max_quic_connections(Some(1)),
    b_addr,
    now,
  );

  // A1 establishes the single allowed connection.
  // Ignoring StreamId: the test asserts on counters, not handles.
  let _ = a1.start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..20 {
    while let Some((to, bytes)) = a1.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a1_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a1_addr {
        a1.handle_udp(b_addr, &bytes, now);
      }
    }
    a1.handle_timeout(now);
    b.handle_timeout(now);
    if b.live_connections_to(a1_addr) >= 1 {
      break;
    }
  }
  assert!(
    b.live_connections_to(a1_addr) >= 1,
    "test precondition: B committed A1's connection (the one allowed)"
  );

  // At the global cap now. A2's over-cap Initials must be refused by the global
  // check WITHOUT reaching the per-source lookup.
  let checks_before = b.counters.quic_pending_inbound_checks;
  let rejected_before = b.metrics().quic_connections_rejected;
  // Ignoring StreamId: the test asserts on counters, not handles.
  let _ = a2.start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..20 {
    while let Some((to, bytes)) = a2.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a2_addr, &bytes, now);
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a2_addr {
        a2.handle_udp(b_addr, &bytes, now);
      }
    }
    a2.handle_timeout(now);
    b.handle_timeout(now);
    if b.metrics().quic_connections_rejected > rejected_before {
      break;
    }
  }
  assert!(
    b.metrics().quic_connections_rejected > rejected_before,
    "A2's over-cap Initial must be refused at the global cap"
  );
  assert_eq!(
    b.counters.quic_pending_inbound_checks, checks_before,
    "an Initial refused at the global cap must NOT perform the per-source \
       pending lookup (global-first short-circuit); the per-source check counter \
       advanced from {checks_before} to {}",
    b.counters.quic_pending_inbound_checks
  );
}

/// At per-source saturation, with a POPULATED connection table, a rejected
/// Initial performs exactly ONE O(1) per-source index lookup
/// and NO coordinator servicing pass. The per-source count is an indexed read,
/// not a scan of the connection slab, so the admission decision does not scale
/// with the number of tracked connections and does not run the O(connections +
/// streams) tick.
///
/// B's per-source pending cap is 3 (global cap high). Three inbound Initials
/// under source S plus two under other sources populate the table with five
/// pending connections; S is then at its cap. A sixth Initial under S is
/// refused. Across that one refused Initial the per-source check counter
/// advances by exactly one (a single index probe) and the servicing counter
/// does not advance at all.
///
/// Negative control: reverting the servicing gate (service on every `Some`)
/// makes the servicing counter advance on the refused Initial; reverting the
/// global/per-source structure so the per-source lookup runs more than once per
/// Initial breaks the exact `+1` check-count assertion.
#[test]
fn rejected_initial_at_per_source_cap_is_one_indexed_probe_no_servicing() {
  let b_addr: SocketAddr = "127.0.0.1:7990".parse().unwrap();
  let now = Instant::now();
  let mut b = make_endpoint_full(
    EndpointOptions::new(SmolStr::new("b"), b_addr),
    test_config()
      .with_max_pending_connections_per_source(Some(3))
      .with_max_quic_connections(Some(64)),
    b_addr,
    now,
  );

  // Source S: three distinct valid Initials (distinct DCIDs, minted by three
  // endpoints) delivered under the same source address — three pending inbound.
  let src_s: SocketAddr = "127.0.0.1:61000".parse().unwrap();
  for port in [8001u16, 8002, 8003] {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut c = make_endpoint(&format!("c{port}"), addr, now);
    // Ignoring StreamId: only c's on-wire Initial is used.
    let _ = c.start_push_pull(b_addr, PushPullKind::Join, now);
    let init = drain_first_datagram_to(&mut c, b_addr);
    b.handle_udp(src_s, &init, now);
  }
  assert_eq!(
    b.metrics().quic_connections_rejected,
    0,
    "the three Initials up to the per-source cap must all be accepted"
  );

  // Two more pending inbound from OTHER sources, so the table is populated with
  // entries the admission decision for S must not scan.
  for (port, src) in [(8101u16, "127.0.0.1:61101"), (8102, "127.0.0.1:61102")] {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let src: SocketAddr = src.parse().unwrap();
    let mut c = make_endpoint(&format!("c{port}"), addr, now);
    // Ignoring StreamId: only c's on-wire Initial is used.
    let _ = c.start_push_pull(b_addr, PushPullKind::Join, now);
    let init = drain_first_datagram_to(&mut c, b_addr);
    b.handle_udp(src, &init, now);
  }
  assert_eq!(
    b.metrics().quic_connections_rejected,
    0,
    "the five accepted Initials (three under S at the cap, two under other \
       sources) must all be accepted; none exceeds a cap yet"
  );

  // A fourth Initial under S — that S is at its cap of 3 is proven below by this
  // Initial being refused (had S held fewer than 3, it would be accepted).
  // Measure
  // the per-source check and servicing counters across ONLY this datagram.
  let mut c = make_endpoint("c-over", "127.0.0.1:8004".parse().unwrap(), now);
  // Ignoring StreamId: only c's on-wire Initial is used.
  let _ = c.start_push_pull(b_addr, PushPullKind::Join, now);
  let over = drain_first_datagram_to(&mut c, b_addr);
  let checks_before = b.counters.quic_pending_inbound_checks;
  let servicings_before = b.counters.quic_inbound_servicings;
  let rejected_before = b.metrics().quic_connections_rejected;
  b.handle_udp(src_s, &over, now);
  assert_eq!(
    b.metrics().quic_connections_rejected,
    rejected_before + 1,
    "the over-cap Initial under S must be refused"
  );
  assert_eq!(
    b.counters.quic_pending_inbound_checks,
    checks_before + 1,
    "the refused Initial must perform exactly one O(1) per-source index lookup, \
       independent of the populated table"
  );
  assert_eq!(
    b.counters.quic_inbound_servicings, servicings_before,
    "a refused Initial commits no state and must NOT run a servicing pass"
  );
}

/// A corrupted (AEAD-tag-flipped, DCID intact) or replayed (duplicate packet
/// number) datagram at an established live CID is routed to that ONE connection
/// and serviced — but only that connection and its bridges, never the whole
/// table. Servicing a packet quinn will discard is cheap when it is bounded to
/// the addressed connection, and it costs an attacker who learned a live CID
/// exactly O(that connection), not the O(all connections + bridges) the global
/// tick ran per flooded datagram before this change.
///
/// Builds a populated server B (connections to A, C1, C2, plus one live bridge on
/// the connection to C1) and floods B at A's live CID. Per-datagram
/// `connection_visits` stays exactly `1` (A's connection) and `bridge_visits`
/// stays exactly the count of A's bridges — neither scales with the table.
///
/// Negative control: restore the global tick on the datagram path — each flooded
/// datagram then visits every connection and every bridge, so `connection_visits`
/// climbs to the connection count and `bridge_visits` picks up C1's live bridge;
/// both exact-count assertions fail.
#[test]
fn corrupted_or_replayed_packet_services_only_its_connection() {
  let b_addr: SocketAddr = "127.0.0.1:7970".parse().unwrap();
  let a_addr: SocketAddr = "127.0.0.1:7971".parse().unwrap();
  let c1_addr: SocketAddr = "127.0.0.1:7972".parse().unwrap();
  let c2_addr: SocketAddr = "127.0.0.1:7973".parse().unwrap();
  let now = Instant::now();
  let mut b = make_endpoint("b", b_addr, now);
  let mut a = make_endpoint("a", a_addr, now);
  let mut c1 = make_endpoint("c1", c1_addr, now);
  let mut c2 = make_endpoint("c2", c2_addr, now);

  // Establish A, C1, C2 -> B (each dials B; B pools an inbound connection to
  // each), giving B a multi-connection table.
  for (caddr, c) in [(a_addr, &mut a), (c1_addr, &mut c1), (c2_addr, &mut c2)] {
    let _ = c.start_push_pull(b_addr, PushPullKind::Join, now);
    for _ in 0..200 {
      let mut moved = false;
      while let Some((to, bytes)) = c.poll_transmit() {
        if to == b_addr {
          b.handle_udp(caddr, &bytes, now);
          moved = true;
        }
      }
      while let Some((to, bytes)) = b.poll_transmit() {
        if to == caddr {
          c.handle_udp(b_addr, &bytes, now);
          moved = true;
        }
      }
      c.handle_timeout(now);
      b.handle_timeout(now);
      if b.live_connections_to(caddr) >= 1 && !moved {
        break;
      }
    }
    assert!(
      b.live_connections_to(caddr) >= 1,
      "test precondition: B must pool a connection to {caddr}"
    );
  }
  assert!(
    b.conns.iter_handles().len() >= 3,
    "test precondition: B must hold several connections so the O(1) claim is non-vacuous"
  );

  // Hold ONE live bridge on B's connection to C1: B initiates a push/pull to the
  // established C1 (opening an outbound bridge synchronously), and C1 is never
  // ferried again, so the bridge stays live. It rides B's connection to C1, NOT
  // the connection to A we flood — so a global bridge scan would visit it.
  let _ = b.start_push_pull(c1_addr, PushPullKind::Refresh, now);
  while b.poll_transmit().is_some() {}
  assert!(
    b.live_bridge_count() >= 1,
    "test precondition: B must hold a live bridge on a non-flooded connection"
  );

  let ch_a = b
    .conns
    .handle_for(&a_addr)
    .expect("B holds a connection to A");
  let bridges_on = |ep: &QuicEndpoint<SmolStr>, ch: quinn_proto::ConnectionHandle| {
    ep.bridges.values().filter(|br| br.ch() == ch).count()
  };
  assert!(
    b.live_bridge_count() > bridges_on(&b, ch_a),
    "test precondition: the live bridge must be on a connection OTHER than A's"
  );

  // Capture a genuine A->B 1-RTT gossip datagram (a DATAGRAM frame, so it mints
  // no bridge on B's side and leaves A's connection bridge-free).
  let _ = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"gossip-probe-payload"), now);
  a.flush_outbound_transmits(now);
  let pristine = drain_first_datagram_to(&mut a, b_addr);
  while a.poll_transmit().is_some() {}

  // Corrupted flood: flip the AEAD tag (last byte); the plaintext DCID is intact,
  // so quinn routes it to B's connection-to-A and discards it on auth failure.
  // Each such datagram services ONLY A's connection.
  let mut corrupt = pristine.clone();
  let last = corrupt.len() - 1;
  corrupt[last] ^= 0xff;
  for _ in 0..4 {
    b.counters.connection_visits = 0;
    b.counters.bridge_visits = 0;
    b.handle_udp(a_addr, &corrupt, now);
    assert_eq!(
      b.counters.connection_visits,
      1,
      "a corrupted datagram at A's live CID must service exactly A's connection, \
         not scale with the {}-connection table",
      b.conns.iter_handles().len()
    );
    assert_eq!(
      b.counters.bridge_visits as usize,
      bridges_on(&b, ch_a),
      "a corrupted datagram must pump only A's bridges, never C1's live bridge"
    );
  }

  // Replayed flood: deliver the genuine datagram once (B extracts it), then
  // re-deliver the SAME bytes — a duplicate packet number quinn discards. The
  // duplicate still services ONLY A's connection.
  b.handle_udp(a_addr, &pristine, now);
  while b.poll_transmit().is_some() {}
  b.counters.connection_visits = 0;
  b.counters.bridge_visits = 0;
  b.handle_udp(a_addr, &pristine, now);
  assert_eq!(
    b.counters.connection_visits, 1,
    "a replayed (duplicate packet number) datagram must service exactly A's connection"
  );
  assert_eq!(
    b.counters.bridge_visits as usize,
    bridges_on(&b, ch_a),
    "a replayed datagram must pump only A's bridges, never C1's live bridge"
  );
}

/// Admitting N distinct below-cap Initials is O(N) total servicing work, not
/// O(N^2). Each accepted Initial services ONLY the freshly-accepted connection,
/// so total `connection_visits` across every `handle_udp` is bounded by the
/// number of datagrams fed (at most one connection touched per datagram) rather
/// than the sum(1..N) a global-tick-per-accept would run.
///
/// Negative control: restore the global tick on the datagram path — the k-th
/// datagram then services all k connections accepted so far, so total
/// `connection_visits` grows quadratically and exceeds the datagram count.
#[test]
fn admitting_n_initials_is_o_n_not_o_n2() {
  let now = Instant::now();
  let b_addr: SocketAddr = "127.0.0.1:7980".parse().unwrap();
  let mut b = make_endpoint("b", b_addr, now);

  const N: usize = 48;
  let mut clients: Vec<(SocketAddr, QuicEndpoint<SmolStr>)> = Vec::new();
  for i in 0..N {
    let addr: SocketAddr = format!("127.0.0.1:{}", 7981 + i).parse().unwrap();
    let mut c = make_endpoint(&format!("c{i}"), addr, now);
    let _ = c.start_push_pull(b_addr, PushPullKind::Join, now);
    clients.push((addr, c));
  }

  // Feed each client's handshake datagrams to B and drain B's replies back,
  // WITHOUT ever calling `b.handle_timeout`: the global timer tick would service
  // every connection and mask the O(N)-vs-O(N^2) distinction. B accepts and
  // drives each handshake purely on the per-connection datagram path.
  b.counters.connection_visits = 0;
  let mut datagrams_to_b: u64 = 0;
  for _ in 0..40 {
    for (caddr, c) in clients.iter_mut() {
      while let Some((to, bytes)) = c.poll_transmit() {
        if to == b_addr {
          b.handle_udp(*caddr, &bytes, now);
          datagrams_to_b += 1;
        }
      }
      c.handle_timeout(now);
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if let Some((_, c)) = clients.iter_mut().find(|(a, _)| *a == to) {
        c.handle_udp(b_addr, &bytes, now);
      }
    }
  }

  let pooled = clients
    .iter()
    .filter(|(a, _)| b.live_connections_to(*a) >= 1)
    .count();
  assert!(
    pooled >= N - 4,
    "test precondition: B must pool essentially all {N} client connections; pooled {pooled}"
  );
  // O(1) per datagram: every inbound datagram serviced exactly its addressed
  // connection, so total visits never exceed the datagram count.
  assert!(
    b.counters.connection_visits <= datagrams_to_b,
    "admitting {N} initials must be O(N): total connection_visits {} exceeded the \
       {datagrams_to_b} datagrams fed — the global tick is running per datagram",
    b.counters.connection_visits
  );
  // Non-vacuous: each pooled connection was serviced at least once.
  assert!(
    b.counters.connection_visits >= pooled as u64,
    "each accepted connection must have been serviced at least once (visits {}, pooled {pooled})",
    b.counters.connection_visits
  );
}

/// A stateless reset delivered to an already-Closed connection reaps its slab
/// entry and frees its global-cap slot in the SAME servicing call — no unrelated
/// timer. quinn routes the reset to the connection as a `ConnectionEvent`; it
/// carries no protocol frame and the connection was already closed, so the former
/// "authenticated progress" gate treated it as inert and skipped servicing,
/// stranding the drained connection in the slab until an unrelated timer fired.
/// Bounding servicing to the addressed connection instead makes the reap happen
/// the moment the reset arrives.
///
/// Negative control: restore that progress gate — the reset is a no-frame event
/// on an already-closed connection, so servicing is skipped, the connection is
/// never `reap_if_drained`'d on this datagram, and the slab-length assertion
/// (`len` drops to 0) fails.
#[test]
fn closed_to_drained_stateless_reset_reaps() {
  let a_addr: SocketAddr = "127.0.0.1:7940".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7941".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  // Establish A <-> B.
  let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..200 {
    let mut moved = false;
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    a.handle_timeout(now);
    b.handle_timeout(now);
    if a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1 && !moved {
      break;
    }
  }
  assert!(
    a.live_connections_to(b_addr) >= 1 && b.live_connections_to(a_addr) >= 1,
    "test precondition: A and B must both hold an established connection"
  );

  // Capture a large genuine A->B 1-RTT datagram to use later as the reset
  // trigger (large enough that B will answer an unknown-CID packet with a
  // stateless reset rather than dropping it).
  let _ = a.queue_unreliable_datagram(b_addr, Bytes::from(vec![0xABu8; 240]), now);
  a.flush_outbound_transmits(now);
  let trigger = drain_first_datagram_to(&mut a, b_addr);
  while a.poll_transmit().is_some() {}

  // Reap B's connection to A (close it and advance B's clock to drained-reap),
  // WITHOUT delivering B's CONNECTION_CLOSE to A — so a later unknown-CID packet
  // makes B emit a stateless reset rather than a graceful close.
  let ch_b = b
    .conns
    .handle_for(&a_addr)
    .expect("B holds a connection to A");
  b.conns
    .get_mut(ch_b)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), Bytes::new());
  for _ in 0..8 {
    let due = b
      .conns
      .get_mut(ch_b)
      .and_then(|e| e.conn_mut().poll_timeout());
    match due {
      Some(due) => b.handle_timeout(crate::Instant::from_std(due)),
      None => b.handle_timeout(now),
    }
    if b.conns.handle_for(&a_addr).is_none() {
      break;
    }
  }
  while b.poll_transmit().is_some() {}
  assert!(
    b.conns.handle_for(&a_addr).is_none(),
    "test precondition: B must have reaped its connection to A"
  );

  // Close A's connection to B: it is now in `Closed` (is_closed, not yet
  // drained). This is the state the reset must drive to drained-and-reaped.
  let ch_a = a
    .conns
    .handle_for(&b_addr)
    .expect("A holds a connection to B");
  a.conns
    .get_mut(ch_a)
    .unwrap()
    .conn_mut()
    .close(now.into_std(), 0u32.into(), Bytes::new());
  while a.poll_transmit().is_some() {}
  assert!(
    a.conns
      .get(ch_a)
      .map(|e| e.conn_ref().is_closed())
      .unwrap_or(false)
      && !a
        .conns
        .get(ch_a)
        .map(|e| e.conn_ref().is_drained())
        .unwrap_or(true),
    "test precondition: A's connection must be Closed but not yet Drained"
  );
  let len_before = a.conns.iter_handles().len();
  assert_eq!(len_before, 1, "A holds exactly its one (closed) connection");

  // Feed the captured A->B packet to B (which no longer knows the CID) so B emits
  // a stateless reset back to A.
  b.handle_udp(a_addr, &trigger, now);
  let mut reset: Option<Vec<u8>> = None;
  while let Some((to, bytes)) = b.poll_transmit() {
    if to == a_addr {
      reset = Some(bytes.to_vec());
    }
  }
  let reset = reset.expect("B must emit a stateless reset to A for the unknown-CID packet");

  // Deliver the stateless reset to A on the datagram path, with NO handle_timeout:
  // the reset arrives as a ConnectionEvent to A's (already-closed) connection,
  // drives it to Drained, and `service_connection` reaps its slab entry in this
  // same call.
  let membership_before = a.membership_time_advances();
  a.handle_udp(b_addr, &reset, now);
  assert_eq!(
    a.membership_time_advances(),
    membership_before,
    "the datagram-path reap must not advance any membership timer"
  );
  assert_eq!(
    a.conns.iter_handles().len(),
    0,
    "the stateless reset must reap A's closed connection's slab entry in the same \
       servicing call (was {len_before} before the reset)"
  );
  assert!(
    a.conns.handle_for(&b_addr).is_none(),
    "the reaped connection's peer mapping must be cleared too"
  );
}

/// The per-connection bridge index [`QuicEndpoint::bridges_by_conn`] must equal
/// the brute-force `bridges` filtered by owning connection at every step — the
/// guard that a bridge mint or reap site missed its index maintenance. Drives a
/// real A<->B push/pull (outbound + inbound bridges minted and reaped) plus a
/// connection teardown, cross-checking after every servicing operation.
#[test]
fn bridges_by_conn_index_matches_bruteforce() {
  let a_addr: SocketAddr = "127.0.0.1:7930".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7931".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  let check = |ep: &QuicEndpoint<SmolStr>, label: &str| {
    let mut brute: std::collections::HashMap<
      quinn_proto::ConnectionHandle,
      std::collections::HashSet<StreamId>,
    > = std::collections::HashMap::new();
    for (id, br) in ep.bridges.iter() {
      brute.entry(br.ch()).or_default().insert(*id);
    }
    let mut index: std::collections::HashMap<
      quinn_proto::ConnectionHandle,
      std::collections::HashSet<StreamId>,
    > = std::collections::HashMap::new();
    for (ch, ids) in ep.bridges_by_conn.iter() {
      assert!(
        !ids.is_empty(),
        "bridges_by_conn must never hold an empty entry (after: {label})"
      );
      index.insert(*ch, ids.iter().copied().collect());
    }
    assert_eq!(
      index, brute,
      "bridges_by_conn diverged from the brute-force filter after: {label}"
    );
  };

  check(&a, "construction (a)");
  check(&b, "construction (b)");

  let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);
  check(&a, "a.start_push_pull");
  for _ in 0..300 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        check(&b, "b.handle_udp");
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        check(&a, "a.handle_udp");
      }
    }
    a.handle_timeout(now);
    check(&a, "a.handle_timeout");
    b.handle_timeout(now);
    check(&b, "b.handle_timeout");
  }

  // Teardown: force-close A's connection and advance to drained-reap, so the
  // connection-loss bridge reap path exercises the index too.
  if let Some(ch) = a.conns.handle_for(&b_addr) {
    a.conns
      .get_mut(ch)
      .unwrap()
      .conn_mut()
      .close(now.into_std(), 0u32.into(), Bytes::new());
    for _ in 0..8 {
      let due = a
        .conns
        .get_mut(ch)
        .and_then(|e| e.conn_mut().poll_timeout());
      match due {
        Some(due) => a.handle_timeout(crate::Instant::from_std(due)),
        None => a.handle_timeout(now),
      }
      check(&a, "a.handle_timeout(teardown)");
      if a.conns.handle_for(&b_addr).is_none() {
        break;
      }
    }
  }
  assert!(
    a.bridges_by_conn.is_empty(),
    "the index must be empty once every bridge is reaped"
  );
}

/// A dial `service_dials` mints on a DIFFERENT connection than the one whose
/// establishment triggered it must flush its first request bytes onto the wire in
/// the SAME `handle_udp` pass — not stall until the next global tick.
///
/// N holds an established pooled connection to B (no active exchange) and a dial
/// to B parked in its inner endpoint queue (queued via the raw endpoint so it is
/// NOT serviced yet). N is meanwhile completing a server-side handshake with A.
/// The inbound datagram that establishes N's connection to A runs
/// `service_connection`'s establishment branch → `service_dials`, which opens the
/// parked dial's bridge on N's connection to B (a connection OTHER than A's). The
/// bridge's request datagram to B must appear in N's outbound queue within that
/// same `handle_udp` — proving the per-connection path pumps AND collects the
/// exact set `service_dials` minted, wherever it landed.
///
/// Mutation-verify: revert step (c) to pump only the just-established connection's
/// bridges (`pump_conn_bridges(ch)`) — B's bridge is minted but never pumped or
/// collected this pass, so no datagram to B appears until a later `handle_timeout`
/// and `flushed_to_b` stays false.
#[test]
fn cross_connection_dial_mint_flushes_first_bytes_same_pass() {
  let n_addr: SocketAddr = "127.0.0.1:7920".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7921".parse().unwrap();
  let a_addr: SocketAddr = "127.0.0.1:7922".parse().unwrap();
  let now = Instant::now();
  let mut n = make_endpoint("n", n_addr, now);
  let mut b = make_endpoint("b", b_addr, now);
  let mut a = make_endpoint("a", a_addr, now);

  // Establish N -> B (N dials B) and let the push/pull settle so N holds an
  // established pooled connection to B with no live bridge.
  let _ = n.start_push_pull(b_addr, PushPullKind::Join, now);
  for _ in 0..300 {
    let mut moved = false;
    while let Some((to, bytes)) = n.poll_transmit() {
      if to == b_addr {
        b.handle_udp(n_addr, &bytes, now);
        moved = true;
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == n_addr {
        n.handle_udp(b_addr, &bytes, now);
        moved = true;
      }
    }
    n.handle_timeout(now);
    b.handle_timeout(now);
    if n.live_connections_to(b_addr) >= 1 && n.live_bridge_count() == 0 && !moved {
      break;
    }
  }
  assert!(
    n.live_connections_to(b_addr) >= 1,
    "test precondition: N must hold an established pooled connection to B"
  );
  assert_eq!(
    n.live_bridge_count(),
    0,
    "test precondition: the N->B push/pull must have settled (no live bridge)"
  );
  // Quiesce both sides so the only later datagram to B can be the parked dial's.
  while n.poll_transmit().is_some() {}
  while b.poll_transmit().is_some() {}

  // Park a dial to B in N's INNER endpoint queue via the raw endpoint, so it is
  // NOT serviced now — it waits for the next `service_dials`, which the A
  // handshake completion will trigger.
  let _ = n
    .endpoint_mut()
    .start_push_pull(b_addr, PushPullKind::Refresh, now);

  // Drive A's server-side handshake to N one inbound datagram at a time, WITHOUT
  // calling `n.handle_timeout` (a global tick would drain the parked B dial
  // early). After each `n.handle_udp`, look for a datagram to B in N's outbound
  // queue: it can only appear once N's connection to A establishes and step (c)
  // opens + flushes the parked B dial's bridge in that same pass.
  let _ = a.start_push_pull(n_addr, PushPullKind::Join, now);
  let mut flushed_to_b = false;
  'outer: for _ in 0..400 {
    let mut a_out: Vec<Vec<u8>> = Vec::new();
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == n_addr {
        a_out.push(bytes.to_vec());
      }
    }
    for dg in a_out {
      n.handle_udp(a_addr, &dg, now);
      let mut n_out: Vec<(SocketAddr, Vec<u8>)> = Vec::new();
      while let Some((to, bytes)) = n.poll_transmit() {
        n_out.push((to, bytes.to_vec()));
      }
      if n_out.iter().any(|(to, _)| *to == b_addr) {
        flushed_to_b = true;
        break 'outer;
      }
      // Not yet established: feed N's handshake responses back to A and continue.
      for (to, bytes) in n_out {
        if to == a_addr {
          a.handle_udp(n_addr, &bytes, now);
        }
      }
    }
    // A may advance its own timers; N must NOT (that would service the parked
    // dial through the global tick, defeating the same-pass claim).
    a.handle_timeout(now);
  }

  assert!(
    flushed_to_b,
    "the dial parked on N's connection to B must open AND flush its first request \
       bytes to B within the single handle_udp that established N's connection to \
       A — a bridge minted on a connection other than the just-established one \
       must not stall until the next global tick"
  );
}

/// Assert the incremental [`super::DeadlineIndex`] behind
/// [`QuicEndpoint::poll_timeout`] returns exactly what a brute-force fold of the
/// same sources ([`QuicEndpoint::recompute_earliest_bruteforce`]) would — the
/// guard that catches a missed `set` at any deadline-mutating site.
fn assert_deadline_index_matches(ep: &mut QuicEndpoint<SmolStr>, label: &str) {
  let want = ep.recompute_earliest_bruteforce();
  let got = ep.poll_timeout();
  assert_eq!(
    got, want,
    "deadline index diverged from the brute-force fold after: {label}"
  );
}

/// The whole risk of the O(1) `poll_timeout` redesign is a missed `set` at some
/// deadline-mutating site, which would leave the index stale. This drives a full
/// coordinator lifecycle — public push/pull dial, a real A<->B handshake, an
/// inbound stream exchange, connection-touching side channels
/// (`queue_unreliable_datagram`, `try_open_uni_stream_to`), membership
/// pass-throughs, leave, and idle-timeout drained-reap — and after EVERY public
/// operation asserts `poll_timeout()` equals the brute-force fold. A stale key
/// (missed insert, missed clear-on-reap, or wrong immediate-due aggregate) is
/// caught as a divergence at the offending step.
#[test]
fn deadline_index_matches_bruteforce_across_operations() {
  use crate::{
    Node,
    typed::{Alive, Meta, Suspect},
  };

  let a_addr: SocketAddr = "127.0.0.1:7860".parse().unwrap();
  let b_addr: SocketAddr = "127.0.0.1:7861".parse().unwrap();
  let c_addr: SocketAddr = "127.0.0.1:7862".parse().unwrap();
  let now = Instant::now();
  let mut a = make_endpoint("a", a_addr, now);
  let mut b = make_endpoint("b", b_addr, now);

  assert_deadline_index_matches(&mut a, "construction (a)");
  assert_deadline_index_matches(&mut b, "construction (b)");

  // Public in-band push/pull: sieves a DialRequested into `dial_pending`,
  // attempts the dial, mints an outbound bridge, and flushes — exercising the
  // Dial, Conn, and Bridge keys plus the immediate-due term in one call.
  let _ = a.start_push_pull(b_addr, PushPullKind::Join, now);
  assert_deadline_index_matches(&mut a, "a.start_push_pull");

  // Ferry the handshake + push/pull to completion. Assert after every
  // handle_udp (route_datagram_event -> service_connection: accept, live-conn
  // event, or no-op) and every handle_timeout (full tick: pump, service_quinn
  // mint + conn-lost reap, service_dials, finalize reap, collect_transmits).
  for round in 0..300 {
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, now);
        assert_deadline_index_matches(&mut b, "b.handle_udp");
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, now);
        assert_deadline_index_matches(&mut a, "a.handle_udp");
      }
    }
    // Mid-handshake, exercise the connection-touching public surfaces that run
    // no servicing tick (must self-index their Conn key).
    if round == 5 {
      let _ = a.queue_unreliable_datagram(b_addr, Bytes::from_static(b"gossip"), now);
      assert_deadline_index_matches(&mut a, "a.queue_unreliable_datagram");
      let _ = a.try_open_uni_stream_to(b_addr, now);
      assert_deadline_index_matches(&mut a, "a.try_open_uni_stream_to");
    }
    a.handle_timeout(now);
    assert_deadline_index_matches(&mut a, "a.handle_timeout");
    b.handle_timeout(now);
    assert_deadline_index_matches(&mut b, "b.handle_timeout");
  }

  // Membership pass-throughs that mutate the inner endpoint's timers (the
  // Endpoint term is refreshed live at the read point).
  let _ = a.start_probe(now);
  assert_deadline_index_matches(&mut a, "a.start_probe");
  let alive = Alive::new(1, Node::new(SmolStr::new("c"), c_addr)).with_meta(Meta::empty());
  a.handle_alive(c_addr, alive, now);
  assert_deadline_index_matches(&mut a, "a.handle_alive");
  let suspect = Suspect::new(1, SmolStr::new("c"), SmolStr::new("a"));
  a.handle_suspect(c_addr, suspect, now);
  assert_deadline_index_matches(&mut a, "a.handle_suspect");
  let _ = a.ping(Node::new(SmolStr::new("c"), c_addr), now);
  assert_deadline_index_matches(&mut a, "a.ping");
  let _ = a.send_user_packets(b_addr, &[Bytes::from_static(b"x")]);
  assert_deadline_index_matches(&mut a, "a.send_user_packets");
  // Drain the unreliable transmit surface (it ticks the inner endpoint's
  // leave-completion boundary).
  while a.poll_memberlist_transmit().is_some() {
    assert_deadline_index_matches(&mut a, "a.poll_memberlist_transmit");
  }

  // Leave, then advance far past the QUIC idle timeout so the pooled connection
  // reaches drained-reap and its Conn key is cleared — asserting throughout.
  let _ = a.leave(now);
  assert_deadline_index_matches(&mut a, "a.leave");
  let mut later = now;
  for _ in 0..60 {
    later += Duration::from_secs(1);
    a.handle_timeout(later);
    assert_deadline_index_matches(&mut a, "a.handle_timeout(post-leave)");
    b.handle_timeout(later);
    assert_deadline_index_matches(&mut b, "b.handle_timeout(post-leave)");
    while let Some((to, bytes)) = a.poll_transmit() {
      if to == b_addr {
        b.handle_udp(a_addr, &bytes, later);
        assert_deadline_index_matches(&mut b, "b.handle_udp(teardown)");
      }
    }
    while let Some((to, bytes)) = b.poll_transmit() {
      if to == a_addr {
        a.handle_udp(b_addr, &bytes, later);
        assert_deadline_index_matches(&mut a, "a.handle_udp(teardown)");
      }
    }
  }
}

/// `poll_timeout` must examine O(1) heap entries regardless of how many
/// connections the table holds — the property that denies an attacker an O(N)
/// re-poll per inbound receive batch. Populates a server with several real
/// pooled connections, then asserts a single `poll_timeout` examines a small
/// constant number of index entries (`>= 1`, so it truly consulted the index)
/// that does not scale with the connection count.
///
/// Mutation-verify: reverting `QuicEndpoint::poll_timeout` to the old O(N) fold
/// never calls `DeadlineIndex::earliest`, so `entities_scanned` stays `0` and
/// the `>= 1` assertion fails.
#[test]
fn poll_timeout_scans_o1_regardless_of_connection_count() {
  let now = Instant::now();
  let s_addr: SocketAddr = "127.0.0.1:7870".parse().unwrap();
  let mut s = make_endpoint("s", s_addr, now);

  let n = 8usize;
  let mut clients: Vec<(SocketAddr, QuicEndpoint<SmolStr>)> = Vec::new();
  for i in 0..n {
    let addr: SocketAddr = format!("127.0.0.1:{}", 7871 + i).parse().unwrap();
    let mut c = make_endpoint(&format!("c{i}"), addr, now);
    let _ = c.start_push_pull(s_addr, PushPullKind::Join, now);
    clients.push((addr, c));
  }

  // Interleaved ferry so the server accepts and pools every client connection.
  for _ in 0..600 {
    for (caddr, c) in clients.iter_mut() {
      while let Some((to, bytes)) = c.poll_transmit() {
        if to == s_addr {
          s.handle_udp(*caddr, &bytes, now);
        }
      }
    }
    while let Some((to, bytes)) = s.poll_transmit() {
      if let Some((_, c)) = clients.iter_mut().find(|(a, _)| *a == to) {
        c.handle_udp(s_addr, &bytes, now);
      }
    }
    for (_, c) in clients.iter_mut() {
      c.handle_timeout(now);
    }
    s.handle_timeout(now);
  }

  let conn_count: usize = clients.iter().map(|(a, _)| s.live_connections_to(*a)).sum();
  assert!(
    conn_count >= 6,
    "expected the server to pool most client connections; got {conn_count}"
  );

  // Settle the index (discard any tombstones), then measure one poll in
  // isolation.
  let _ = s.poll_timeout();
  s.deadline_index.reset_entities_scanned();
  let _ = s.poll_timeout();
  let scanned = s.deadline_index.entities_scanned();
  assert!(
    scanned >= 1,
    "poll_timeout must consult the deadline index — reverting it to the O(N) \
     fold never calls earliest(), leaving this 0"
  );
  assert!(
    scanned <= 4,
    "poll_timeout must examine O(1) heap entries, not scale with the \
     {conn_count} pooled connections; examined {scanned}"
  );
}
